"""Tests for the multi-replica offline write reconciliation API."""

import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def _isolated_history_db(tmp_path, monkeypatch):
    """Point the persistent store at a per-test SQLite file."""

    monkeypatch.setenv("HISTORY_DB_PATH", str(tmp_path / "history.db"))
    yield


def ev(replica, seq, key, put=None, delete=None, clock=None):
    e = {"replica_id": replica, "seq": seq, "key": key, "clock": clock or {}}
    if put is not None:
        e["put"] = put
    elif delete is not None:
        e["delete"] = delete
    else:
        e["delete"] = "delete"
    return e


def post(replica_ids, events):
    return client.post(
        "/api/resolve", json={"replica_ids": replica_ids, "events": events}
    )


def post_catchup(replica_ids, events, known_clock):
    return client.post(
        "/api/catchup",
        json={
            "replica_ids": replica_ids,
            "events": events,
            "known_clock": known_clock,
        },
    )


def post_resolve_conflict(replica_ids, events, key, writer, versions, **ops):
    body = {
        "replica_ids": replica_ids,
        "events": events,
        "key": key,
        "replica_id": writer,
        "versions": versions,
    }
    body.update(ops)
    return client.post("/api/resolve-conflict", json=body)


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_concurrent_puts_conflict_even_when_values_equal():
    r = post(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="x", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "k", put="x", clock={"r1": 1, "r2": 0}),
        ],
    )
    assert r.status_code == 200, r.text
    (item,) = r.json()["results"]
    assert item["state"] == "conflict"
    assert [e["replica_id"] for e in item["events"]] == ["r1", "r2"]


def test_results_sorted_by_key_and_put_shadows_older_event():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "z", put="a", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "a", delete="delete", clock={"r1": 0, "r2": 1}),
            ev("r1", 2, "a", put="kept", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    results = r.json()["results"]
    assert [x["key"] for x in results] == ["a", "z"]
    assert results[0]["state"] == "put"
    assert results[0]["value"] == "kept"
    assert results[0]["event"]["replica_id"] == "r1"
    assert results[0]["event"]["seq"] == 2


def test_unique_maximal_delete_is_reported_deleted():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "k", delete="delete", clock={"r1": 1, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    (item,) = r.json()["results"]
    assert item["state"] == "deleted"
    assert item["event"]["op"] == "delete"
    assert item["event"]["value"] is None


def test_events_may_arrive_out_of_order():
    events = [
        ev("r1", 3, "k", put="c", clock={"r1": 3, "r2": 1}),
        ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ev("r1", 2, "x", put="y", clock={"r1": 2, "r2": 1}),
        ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
    ]
    r = post(["r1", "r2"], events)
    assert r.status_code == 200, r.text
    k = [x for x in r.json()["results"] if x["key"] == "k"][0]
    assert k["state"] == "put"
    assert k["value"] == "c"


def test_empty_put_value_is_allowed():
    r = post(
        ["a", "b"],
        [ev("a", 1, "k", put="", clock={"a": 1, "b": 0})],
    )
    assert r.status_code == 200, r.text
    (item,) = r.json()["results"]
    assert item["state"] == "put"
    assert item["value"] == ""


def test_conflict_events_sorted_by_replica_then_seq():
    # Concurrent branches after common predecessor r2/1:
    # r1/2 and r2/2 are incomparable (each advances a different component).
    r = post(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "other", put="x", clock={"r1": 1, "r2": 0}),
            ev("r2", 2, "k", put="d", clock={"r1": 1, "r2": 2}),
            ev("r1", 2, "k", put="a", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    item = [x for x in r.json()["results"] if x["key"] == "k"][0]
    assert item["state"] == "conflict"
    assert [(e["replica_id"], e["seq"]) for e in item["events"]] == [
        ("r1", 2),
        ("r2", 2),
    ]


def test_three_replicas_and_eventless_replica():
    r = post(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0, "r3": 0}),
        ],
    )
    assert r.status_code == 200, r.text
    assert r.json()["results"][0]["event"]["clock"] == {"r1": 1, "r2": 0, "r3": 0}


# ---------------------------------------------------------------------------
# Validation failures -> 422
# ---------------------------------------------------------------------------


def assert_422(r, code=None):
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    if code is not None:
        assert any(e.get("code") == code for e in detail)
    return detail


def test_duplicate_replica_ids_rejected():
    r = post(["r1", "r1"], [ev("r1", 1, "k", put="v", clock={"r1": 0})])
    assert_422(r, "duplicate_replica_id")


def test_unknown_event_replica_rejected():
    r = post(
        ["r1", "r2"],
        [ev("r9", 1, "k", put="v", clock={"r1": 0, "r2": 0})],
    )
    assert_422(r, "unknown_replica_id")


def test_clock_missing_and_extra_components():
    r = post(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r3": 0})],
    )
    assert_422(r, "clock_replica_coverage")


def test_clock_negative_component():
    r = post(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": -1})],
    )
    assert r.status_code == 422


def test_self_component_must_equal_seq():
    r = post(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 2, "r2": 0})],
    )
    detail = assert_422(r, "clock_self_component")
    assert detail[0]["event_index"] == 0


def test_duplicate_seq_rejected():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r1", 1, "k", put="b", clock={"r1": 1, "r2": 0}),
        ],
    )
    assert_422(r, "duplicate_seq")


def test_gap_in_seq_rejected():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r1", 3, "k", put="b", clock={"r1": 3, "r2": 0}),
        ],
    )
    assert_422(r, "non_contiguous_seq")


def test_seq_must_start_at_one():
    r = post(
        ["r1", "r2"],
        [ev("r1", 2, "k", put="a", clock={"r1": 2, "r2": 0})],
    )
    assert_422(r, "non_contiguous_seq")


def test_clock_pointing_to_missing_event():
    # r1/1 claims to know r3/1, but no event was produced by r3.
    r = post(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0, "r3": 1}),
        ],
    )
    detail = assert_422(r, "clock_points_to_missing_event")
    assert detail[0]["event_index"] == 0


def test_other_replica_clock_equal_rejected():
    # Alias covered by test_equal_clock_events_are_causally_invalid:
    # re-verify the offending event index is reported.
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ],
    )
    detail = assert_422(r, "clock_causality")
    assert detail[0]["event_index"] == 0


def test_equal_clock_events_are_causally_invalid():
    # Cross-replica: two events with identical clocks cannot reference each
    # other (the referenced clock must be strictly smaller).
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ],
    )
    assert_422(r, "clock_causality")


def test_predecessor_clock_with_component_greater_rejected():
    # r1/1 knows r2/2 (clock r2=2), but r1/2 only knows r2/1 (r2=1):
    # the predecessor's clock is not <= the current one. Events arrive
    # out of order; the violation must still be pinpointed at r1/2.
    r = post(
        ["r1", "r2"],
        [
            ev("r2", 1, "x", put="x", clock={"r1": 0, "r2": 1}),
            ev("r2", 2, "x", put="x", clock={"r1": 0, "r2": 2}),
            ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 2}),
        ],
    )
    detail = assert_422(r, "clock_causality")
    assert detail[0]["event_index"] == 2


def test_other_component_referenced_clock_not_le_rejected():
    # Three replicas: r1/1 references r2/1 (r2 component = 1), but r2/1
    # already knows r3/1 while r1/1 does not -> referenced clock not <=.
    r = post(
        ["r1", "r2", "r3"],
        [
            ev("r3", 1, "z", put="z", clock={"r1": 0, "r2": 0, "r3": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1, "r3": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1, "r3": 0}),
        ],
    )
    detail = assert_422(r, "clock_causality")
    assert detail[0]["event_index"] == 2


def test_put_and_delete_together_rejected():
    r = post(
        ["r1", "r2"],
        [
            {
                "replica_id": "r1",
                "seq": 1,
                "key": "k",
                "put": "v",
                "delete": "delete",
                "clock": {"r1": 1, "r2": 0},
            }
        ],
    )
    assert r.status_code == 422


def test_delete_field_wrong_value_rejected():
    r = post(
        ["r1", "r2"],
        [
            {
                "replica_id": "r1",
                "seq": 1,
                "key": "k",
                "delete": "remove",
                "clock": {"r1": 1, "r2": 0},
            }
        ],
    )
    assert r.status_code == 422


def test_boundary_replica_and_event_counts():
    # 8 replicas, single event
    ids = [f"r{i}" for i in range(8)]
    r = post(ids, [ev("r0", 1, "k", put="v", clock={rid: 0 for rid in ids} | {"r0": 1})])
    assert r.status_code == 200, r.text

    # 1 replica -> schema error
    r = post(["only"], [ev("only", 1, "k", put="v", clock={"only": 1})])
    assert r.status_code == 422

    # 121 events (one replica with 121 seqs) -> schema error
    many = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0}) for n in range(1, 122)
    ]
    r = post(["r1", "r2"], many)
    assert r.status_code == 422


def test_empty_events_rejected():
    r = client.post(
        "/api/resolve", json={"replica_ids": ["r1", "r2"], "events": []}
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# /api/catchup
# ---------------------------------------------------------------------------


def test_catchup_schedules_by_dependency_rounds():
    # r1/1 -> r2/1 (knows r1/1) -> r1/2 (knows r2/1); target only has r1/1.
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
        ],
        {"r1": 1, "r2": 0},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    rounds = data["rounds"]
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r2", 1)],
        [("r1", 2)],
    ]
    # Operations, values and clocks are preserved verbatim.
    first = rounds[0][0]
    assert first["key"] == "k"
    assert first["op"] == "put"
    assert first["value"] == "b"
    assert first["clock"] == {"r1": 1, "r2": 1}
    assert data["progress"] == {"r1": 2, "r2": 1}


def test_catchup_round_includes_all_currently_ready_sorted():
    # Independent first events on both replicas are ready simultaneously;
    # second events wait for cross-replica knowledge.
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1}),
            ev("r1", 2, "k", put="a2", clock={"r1": 2, "r2": 1}),
            ev("r2", 2, "k", put="b2", clock={"r1": 1, "r2": 2}),
        ],
        {"r1": 0, "r2": 0},
    )
    assert r.status_code == 200, r.text
    rounds = r.json()["rounds"]
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r1", 1), ("r2", 1)],
        [("r1", 2), ("r2", 2)],
    ]


def test_catchup_concurrent_equal_value_events_are_both_transferred():
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="x", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "k", put="x", clock={"r1": 1, "r2": 0}),
        ],
        {"r1": 0, "r2": 0},
    )
    assert r.status_code == 200, r.text
    rounds = r.json()["rounds"]
    assert len(rounds) == 1
    assert [(e["replica_id"], e["seq"]) for e in rounds[0]] == [
        ("r1", 1),
        ("r2", 1),
    ]
    assert all(e["value"] == "x" for e in rounds[0])


def test_catchup_all_received_has_empty_rounds():
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ],
        {"r1": 1, "r2": 1},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["rounds"] == []
    assert data["progress"] == {"r1": 1, "r2": 1}


def test_catchup_delete_events_preserved():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", delete="delete", clock={"r1": 1, "r2": 0})],
        {"r1": 0, "r2": 0},
    )
    assert r.status_code == 200, r.text
    (only,) = r.json()["rounds"][0]
    assert only["op"] == "delete"
    assert only["value"] is None
    assert only["key"] == "k"


def test_catchup_progress_reports_eventless_replica_as_zero():
    r = post_catchup(
        ["r1", "r2", "r3"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0, "r3": 0})],
        {"r1": 0, "r2": 0, "r3": 0},
    )
    assert r.status_code == 200, r.text
    assert r.json()["progress"] == {"r1": 1, "r2": 0, "r3": 0}


def test_catchup_missing_known_clock_component():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
        {"r1": 0},
    )
    detail = assert_422(r, "known_clock_coverage")
    assert "event_index" not in detail[0]


def test_catchup_extra_known_clock_component():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
        {"r1": 0, "r2": 0, "r3": 0},
    )
    assert_422(r, "known_clock_coverage")


def test_catchup_known_clock_beyond_batch_max_seq():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
        {"r1": 1, "r2": 3},
    )
    assert_422(r, "known_clock_ahead_of_batch")


def test_catchup_received_event_clock_beyond_known_clock():
    # r2/1 is missing (known_clock r2 = 0), but the already-received r1/1
    # claims to know r2/1 -> the target's knowledge is inconsistent.
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
        ],
        {"r1": 1, "r2": 0},
    )
    detail = assert_422(r, "clock_exceeds_known_clock")
    assert detail[0]["event_index"] == 1


def test_catchup_negative_known_clock_component_is_422():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
        {"r1": -1, "r2": 0},
    )
    assert r.status_code == 422


def test_catchup_still_validates_batch_with_original_rules():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 2, "r2": 0})],
        {"r1": 0, "r2": 0},
    )
    assert_422(r, "clock_self_component")


def test_catchup_strictly_alternating_chain_takes_one_event_per_round():
    events = [
        ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
        ev("r2", 2, "k", put="d", clock={"r1": 2, "r2": 2}),
        ev("r1", 3, "k", put="e", clock={"r1": 3, "r2": 2}),
    ]
    r = post_catchup(["r1", "r2"], events, {"r1": 0, "r2": 0})
    assert r.status_code == 200, r.text
    rounds = r.json()["rounds"]
    # Dependencies must be present at the target or scheduled in an *earlier*
    # round, so the strictly alternating chain cannot batch events together.
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r1", 1)],
        [("r2", 1)],
        [("r1", 2)],
        [("r2", 2)],
        [("r1", 3)],
    ]
    assert r.json()["progress"] == {"r1": 3, "r2": 2}


# ---------------------------------------------------------------------------
# /api/resolve-conflict
# ---------------------------------------------------------------------------


def two_branch_conflict(put_a="alice", put_b="bob"):
    return [
        ev("r2", 1, "name", put=put_b, clock={"r1": 0, "r2": 1}),
        ev("r1", 1, "name", put=put_a, clock={"r1": 1, "r2": 0}),
    ]


def test_resolve_conflict_appends_dominating_put_and_returns_verdict():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    data = r.json()
    new_event = data["event"]
    assert new_event == {
        "replica_id": "r1",
        "seq": 2,
        "key": "name",
        "put": "chosen",
        "clock": {"r1": 2, "r2": 1},
    }
    assert data["result"] == {
        "key": "name",
        "state": "put",
        "value": "chosen",
        "event": {
            "replica_id": "r1",
            "seq": 2,
            "op": "put",
            "value": "chosen",
            "clock": {"r1": 2, "r2": 1},
        },
    }


def test_resolve_conflict_equal_value_conflict_is_also_resolved():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict("x", "x"),
        key="name",
        writer="r2",
        versions=[{"replica_id": "r2", "seq": 1}, {"replica_id": "r1", "seq": 1}],
        put="x",
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["event"]["seq"] == 2
    assert data["event"]["clock"] == {"r1": 1, "r2": 2}
    assert data["result"]["state"] == "put"
    assert data["result"]["event"]["seq"] == 2


def test_resolve_conflict_delete_op():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        delete="delete",
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["event"] == {
        "replica_id": "r1",
        "seq": 2,
        "key": "name",
        "delete": "delete",
        "clock": {"r1": 2, "r2": 1},
    }
    assert data["result"]["state"] == "deleted"
    assert data["result"]["event"]["op"] == "delete"
    assert data["result"]["event"]["value"] is None


def test_resolve_conflict_empty_put_allowed():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"]["put"] == ""
    assert r.json()["result"]["value"] == ""


def test_resolve_conflict_writer_with_no_prior_events_starts_at_seq_one():
    # r3 is eventless; it writes the resolution as r3/1 with an all-history
    # clock covering both branches.
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
        ],
        key="name",
        writer="r3",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"] == {
        "replica_id": "r3",
        "seq": 1,
        "key": "name",
        "put": "chosen",
        "clock": {"r1": 1, "r2": 1, "r3": 1},
    }


def test_resolve_conflict_clock_covers_writer_prior_history_beyond_branches():
    # r1/1 (key z) knows r3/1; r1/2 and r2/1 conflict on name but neither
    # branch has seen r3/1.  The resolution clock must still carry r3=1 from
    # the writer r1's own prior history.
    events = [
        ev("r3", 1, "z", put="z", clock={"r1": 0, "r2": 0, "r3": 1}),
        ev("r1", 1, "z", put="z2", clock={"r1": 1, "r2": 0, "r3": 1}),
        ev("r1", 2, "name", put="a", clock={"r1": 2, "r2": 0, "r3": 1}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        events,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 2}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"]["clock"] == {"r1": 3, "r2": 1, "r3": 1}


def test_resolve_conflict_event_replays_through_resolve():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    new_event = r.json()["event"]

    rr = post(["r1", "r2"], two_branch_conflict() + [new_event])
    assert rr.status_code == 200, rr.text
    (item,) = rr.json()["results"]
    assert item["state"] == "put"
    assert item["value"] == "chosen"
    assert item["event"]["seq"] == 2


def test_resolve_conflict_event_replays_through_catchup_by_dependency():
    # Eventless writer r3 resolves a 3-replica conflict; replaying onto a
    # target that knows nothing must transfer all three branches first, with
    # the resolution in a later round.
    branches = [
        ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        branches,
        key="name",
        writer="r3",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    resolution = r.json()["event"]

    cr = post_catchup(
        ["r1", "r2", "r3"],
        branches + [resolution],
        {"r1": 0, "r2": 0, "r3": 0},
    )
    assert cr.status_code == 200, cr.text
    rounds = cr.json()["rounds"]
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r1", 1), ("r2", 1)],
        [("r3", 1)],
    ]
    assert rounds[1][0]["value"] == "chosen"


def test_resolve_conflict_does_not_change_other_keys_verdicts():
    events = two_branch_conflict() + [
        ev("r1", 2, "other", put="keep", clock={"r1": 2, "r2": 1})
    ]
    # Note: r1/2 here dominates r2/1; it sits on a different key, unaffected.
    r = post_resolve_conflict(
        ["r1", "r2"],
        events,
        key="name",
        writer="r2",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        delete="delete",
    )
    assert r.status_code == 200, r.text
    resolution = r.json()["event"]

    rr = post(["r1", "r2"], events + [resolution])
    assert rr.status_code == 200, rr.text
    results = {x["key"]: x for x in rr.json()["results"]}
    assert results["name"]["state"] == "deleted"
    assert results["other"]["state"] == "put"
    assert results["other"]["value"] == "keep"


def test_resolve_conflict_versions_may_arrive_out_of_order():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r2", "seq": 1}, {"replica_id": "r1", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"]["clock"] == {"r1": 2, "r2": 1}


def test_resolve_conflict_unknown_writer_is_422():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r9",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert_422(r, "unknown_replica_id")


def test_resolve_conflict_version_with_unknown_replica_is_422():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r9", "seq": 1}],
        put="chosen",
    )
    assert_422(r, "unknown_replica_id")


def test_resolve_conflict_duplicate_versions_are_422():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r1", "seq": 1}],
        put="chosen",
    )
    assert_422(r, "duplicate_conflict_version")


def test_resolve_conflict_single_version_is_422():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 422, r.text


def test_resolve_conflict_missing_version_is_409():
    # Three pairwise-incomparable maximals; listing only two omits one.
    branches = [
        ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
        ev("r3", 1, "name", put="c", clock={"r1": 0, "r2": 0, "r3": 1}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        branches,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )

    # A one-entry list is a shape error (at least two required) -> 422.
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        branches,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 422, r.text

    # Two listed but one is not maximal (r3/1 lives on another key): 409.
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
            ev("r3", 1, "other", put="x", clock={"r1": 0, "r2": 0, "r3": 1}),
        ],
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r3", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )


def test_resolve_conflict_extra_version_is_409():
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
            ev("r3", 1, "other", put="x", clock={"r1": 0, "r2": 0, "r3": 1}),
        ],
        key="name",
        writer="r1",
        versions=[
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
            {"replica_id": "r3", "seq": 1},
        ],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )


def test_resolve_conflict_version_pointing_at_nonexistent_seq_is_409():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 7}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )


def test_resolve_conflict_key_already_resolved_is_409():
    # r1/2 causally dominates r2/1: name is a plain put, not a conflict.
    events = [
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1}),
        ev("r1", 1, "other", put="x", clock={"r1": 1, "r2": 0}),
        ev("r1", 2, "name", put="a", clock={"r1": 2, "r2": 1}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2"],
        events,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 2}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(e["code"] == "key_not_conflicted" for e in r.json()["detail"])


def test_resolve_conflict_unknown_key_is_409():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="missing",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(e["code"] == "key_not_conflicted" for e in r.json()["detail"])


def test_resolve_conflict_full_batch_is_422():
    # 120 valid events on r1 (seq 1..60) and r2 (seq 1..60), all key 'name';
    # the batch already contains a conflict and cannot take one more event.
    events = []
    for n in range(1, 61):
        c1 = {"r1": n, "r2": n - 1}
        c2 = {"r1": n - 1, "r2": n}
        events.append(ev("r1", n, "name", put="a", clock=c1))
        events.append(ev("r2", n, "name", put="b", clock=c2))
    assert len(events) == 120
    r = post_resolve_conflict(
        ["r1", "r2"],
        events,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 60}, {"replica_id": "r2", "seq": 60}],
        put="chosen",
    )
    assert r.status_code == 422, r.text
    assert any(e["code"] == "batch_full" for e in r.json()["detail"])


def test_resolve_conflict_still_validates_original_batch():
    bad = [
        ev("r1", 1, "name", put="a", clock={"r1": 2, "r2": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2"],
        bad,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert_422(r, "clock_self_component")


def test_resolve_conflict_requires_exactly_one_op():
    r = client.post(
        "/api/resolve-conflict",
        json={
            "replica_ids": ["r1", "r2"],
            "events": two_branch_conflict(),
            "key": "name",
            "replica_id": "r1",
            "versions": [
                {"replica_id": "r1", "seq": 1},
                {"replica_id": "r2", "seq": 1},
            ],
        },
    )
    assert r.status_code == 422

    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="x",
        delete="delete",
    )
    assert r.status_code == 422


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# Persistent histories
# ---------------------------------------------------------------------------


def hist_create(replica_ids, events):
    return client.post(
        "/api/histories",
        json={"replica_ids": replica_ids, "events": events},
    )


def hist_get(hid):
    return client.get(f"/api/histories/{hid}")


def hist_merge(hid, events):
    return client.post(
        f"/api/histories/{hid}/events", json={"events": events}
    )


def hist_commit(hid, key, writer, versions, **ops):
    body = {
        "key": key,
        "replica_id": writer,
        "versions": versions,
    }
    body.update(ops)
    return client.post(
        f"/api/histories/{hid}/resolve-conflict", json=body
    )


def create_conflict_history():
    r = hist_create(["r1", "r2"], two_branch_conflict())
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_history_create_returns_id_and_reads_back_sorted_batch_and_verdict():
    r = hist_create(
        ["r1", "r2"],
        [
            ev("r2", 1, "name", put="bob", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0}),
        ],
    )
    assert r.status_code == 201, r.text
    hid = r.json()["id"]
    assert isinstance(hid, str) and len(hid) >= 16

    got = hist_get(hid)
    assert got.status_code == 200, got.text
    data = got.json()
    assert data["id"] == hid
    assert data["replica_ids"] == ["r1", "r2"]
    # Events sorted by replica_id then seq regardless of input order.
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r2", 1),
    ]
    # The verdict is computed over that same committed state.
    (result,) = data["results"]
    assert result["state"] == "conflict"
    assert [e["seq"] for e in result["events"]] == [1, 1]


def test_history_create_invalid_batch_is_422_and_nothing_stored():
    r = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 2, "r2": 0})],
    )
    assert_422(r, "clock_self_component")

    # The bad batch never became a history (its id was never issued anyway,
    # and the DB still has no rows: reads of random ids keep returning 404).
    assert hist_get("deadbeef" * 4).status_code == 404


def test_history_unknown_id_is_404_on_every_endpoint():
    hid = "0" * 32
    assert hist_get(hid).status_code == 404
    assert (
        hist_merge(
            hid, [ev("r1", 1, "k", put="x", clock={"r1": 1, "r2": 0})]
        ).status_code
        == 404
    )
    assert (
        hist_commit(
            hid,
            key="k",
            writer="r1",
            versions=[
                {"replica_id": "r1", "seq": 1},
                {"replica_id": "r2", "seq": 1},
            ],
            put="x",
        ).status_code
        == 404
    )


def test_history_merge_appends_events_and_returns_full_state():
    hid = create_conflict_history()

    r = hist_merge(
        hid,
        [ev("r1", 2, "other", put="keep", clock={"r1": 2, "r2": 1})],
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    results = {x["key"]: x for x in data["results"]}
    assert results["name"]["state"] == "conflict"
    assert results["other"]["state"] == "put"

    # A subsequent read returns exactly what was committed.
    got = hist_get(hid).json()
    assert got["events"] == data["events"]
    assert got["results"] == data["results"]


def test_history_merge_identical_event_is_ignored():
    hid = create_conflict_history()
    same = two_branch_conflict()

    # Re-send both stored events verbatim: no new events, no error.
    r = hist_merge(hid, same)
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 2

    # Mix of identical + genuinely new in the same call.
    r = hist_merge(
        hid,
        [
            same[0],
            ev("r1", 2, "x", put="y", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 3


def test_history_merge_same_identity_different_content_is_409_and_atomic():
    hid = create_conflict_history()

    before = hist_get(hid).json()
    r = hist_merge(
        hid,
        [
            ev("r2", 1, "name", put="different", clock={"r1": 0, "r2": 1}),
            ev("r1", 2, "other", put="x", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "event_content_mismatch" for e in r.json()["detail"]
    )

    # Neither event (not even the valid second one) was written.
    after = hist_get(hid).json()
    assert after["events"] == before["events"]


def test_history_merge_failing_validation_is_422_and_atomic():
    # Valid seed on r1; incoming batch breaks contiguity on r2.
    r = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    )
    hid = r.json()["id"]

    before = hist_get(hid).json()
    r = hist_merge(
        hid,
        [ev("r2", 2, "k", put="b", clock={"r1": 1, "r2": 2})],
    )
    assert_422(r, "non_contiguous_seq")
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_enforces_120_event_limit():
    # 60 paired events -> r1 has 1..60 and r2 has 1..60 (120 total).
    seed = []
    for n in range(1, 61):
        seed.append(
            ev("r1", n, "k", put="a", clock={"r1": n, "r2": n - 1})
        )
        seed.append(
            ev("r2", n, "k", put="b", clock={"r1": n, "r2": n})
        )
    assert len(seed) == 120
    hid = hist_create(["r1", "r2"], seed).json()["id"]

    r = hist_merge(
        hid, [ev("r1", 61, "k", put="c", clock={"r1": 61, "r2": 60})]
    )
    assert_422(r, "batch_full")
    assert len(hist_get(hid).json()["events"]) == 120


def test_history_merge_unknown_replica_is_422():
    hid = create_conflict_history()
    r = hist_merge(
        hid, [ev("r9", 1, "k", put="x", clock={"r1": 0, "r2": 0})]
    )
    assert_422(r, "unknown_replica_id")


def test_history_commit_resolves_conflict_and_persists_event():
    hid = create_conflict_history()

    r = hist_commit(
        hid,
        key="name",
        writer="r1",
        versions=[
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
        ],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["id"] == hid
    assert data["event"] == {
        "replica_id": "r1",
        "seq": 2,
        "key": "name",
        "put": "chosen",
        "clock": {"r1": 2, "r2": 1},
    }
    assert data["result"]["state"] == "put"
    assert data["result"]["value"] == "chosen"

    # The committed history contains the resolution event and its verdict.
    got = hist_get(hid).json()
    assert [(e["replica_id"], e["seq"]) for e in got["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    assert got["results"][0]["state"] == "put"


def test_history_commit_then_stale_versions_get_409_and_nothing_written():
    hid = create_conflict_history()
    versions = [
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r2", "seq": 1},
    ]

    assert hist_commit(
        hid, key="name", writer="r1", versions=versions, put="chosen"
    ).status_code == 200
    state_after_first = hist_get(hid).json()

    # Replaying the same stale versions against the resolved key -> 409.
    r = hist_commit(
        hid, key="name", writer="r2", versions=versions, delete="delete"
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "key_not_conflicted" for e in r.json()["detail"]
    )

    # No r2/2 was allocated; the history is unchanged.
    assert hist_get(hid).json() == state_after_first


def test_history_commit_version_mismatch_is_409():
    branches = [
        ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
        ev("r3", 1, "name", put="c", clock={"r1": 0, "r2": 0, "r3": 1}),
    ]
    hid = hist_create(["r1", "r2", "r3"], branches).json()["id"]

    r = hist_commit(
        hid,
        key="name",
        writer="r1",
        versions=[
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
        ],
        put="chosen",
    )
    assert r.status_code == 409
    assert any(
        e["code"] == "conflict_versions_mismatch"
        for e in r.json()["detail"]
    )


def test_history_commit_writer_seq_continues_tail_under_concurrency():
    # Ten independent conflicting keys; resolve each from r1 concurrently.
    events = []
    for n in range(1, 11):
        events.append(
            ev("r1", n, f"k{n}", put=f"a{n}", clock={"r1": n, "r2": n - 1})
        )
        events.append(
            ev("r2", n, f"k{n}", put=f"b{n}", clock={"r1": n - 1, "r2": n})
        )
    hid = hist_create(["r1", "r2"], events).json()["id"]
    barrier = threading.Barrier(10)

    def resolve(n):
        c = TestClient(app)
        barrier.wait()
        return c.post(
            f"/api/histories/{hid}/resolve-conflict",
            json={
                "key": f"k{n}",
                "replica_id": "r1",
                "versions": [
                    {"replica_id": "r1", "seq": n},
                    {"replica_id": "r2", "seq": n},
                ],
                "put": f"chosen{n}",
            },
        )

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(resolve, range(1, 11)))

    assert all(r.status_code == 200 for r in results), [
        r.text for r in results if r.status_code != 200
    ]
    # Exactly one resolution per seq 11..20 -- no duplicated seq, none lost.
    got = hist_get(hid).json()
    r1_seqs = sorted(
        e["seq"] for e in got["events"] if e["replica_id"] == "r1"
    )
    assert r1_seqs == list(range(1, 21))
    assert len(got["events"]) == 30
    # All ten keys end resolved (single maximal put from r1).
    assert all(x["state"] == "put" for x in got["results"])


def test_history_concurrent_merges_lose_no_events_and_validate():
    # Seven writers each append their own independent replica tail, so the
    # only correctness under concurrency is that every append survives and
    # every writer's seq tail is contiguous/unique.
    replicas = ["r1", "r2", "r3", "r4", "r5", "r6", "r7"]
    seed = [
        ev("r1", 1, "k", put="start", clock={r: 0 for r in replicas} | {"r1": 1})
    ]
    hid = hist_create(replicas, seed).json()["id"]
    barrier = threading.Barrier(6)

    def append_tail(rid):
        c = TestClient(app)
        barrier.wait()
        # First event on this replica knows r1/1; later ones chain locally.
        c.post(
            f"/api/histories/{hid}/events",
            json={
                "events": [
                    ev(
                        rid,
                        1,
                        f"k-{rid}-1",
                        put="x",
                        clock={r: 0 for r in replicas} | {"r1": 1, rid: 1},
                    )
                ]
            },
        )
        for n in range(2, 7):
            r = c.post(
                f"/api/histories/{hid}/events",
                json={
                    "events": [
                        ev(
                            rid,
                            n,
                            f"k-{rid}-{n}",
                            put="x",
                            clock={r: 0 for r in replicas}
                            | {"r1": 1, rid: n},
                        )
                    ]
                },
            )
            assert r.status_code == 200, r.text

    with ThreadPoolExecutor(max_workers=7) as pool:
        list(pool.map(append_tail, replicas[1:]))

    got = hist_get(hid).json()
    assert len(got["events"]) == 1 + 6 * 6
    for rid in replicas[1:]:
        seqs = sorted(
            e["seq"] for e in got["events"] if e["replica_id"] == rid
        )
        assert seqs == [1, 2, 3, 4, 5, 6]


def test_history_concurrent_commits_to_same_conflict_one_wins():
    hid = create_conflict_history()
    versions = [
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r2", "seq": 1},
    ]
    barrier = threading.Barrier(2)

    def commit(writer):
        c = TestClient(app)
        barrier.wait()
        return c.post(
            f"/api/histories/{hid}/resolve-conflict",
            json={
                "key": "name",
                "replica_id": writer,
                "versions": versions,
                "put": f"from-{writer}",
            },
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(commit, ["r1", "r2"]))

    statuses = sorted(r.status_code for r in results)
    assert statuses == [200, 409], [r.text for r in results]
    got = hist_get(hid).json()
    # Exactly one resolution event was ever allocated.
    assert len(got["events"]) == 3
    resolution = [
        e for e in got["events"]
        if (e["replica_id"], e["seq"]) not in [("r1", 1), ("r2", 1)]
    ]
    assert len(resolution) == 1
    assert got["results"][0]["state"] == "put"


def test_history_survives_process_restart_with_same_sqlite_file(tmp_path):
    db = tmp_path / "history.db"
    env = dict(os.environ, HISTORY_DB_PATH=str(db), PYTHONPATH="/workspace")

    def run(code):
        return subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
        )

    # Process A: create a history, merge an event, commit a resolution.
    create = run(
        """
import json
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.post('/api/histories', json={
    'replica_ids': ['r1', 'r2'],
    'events': [
        {'replica_id': 'r1', 'seq': 1, 'key': 'name', 'put': 'alice',
         'clock': {'r1': 1, 'r2': 0}},
        {'replica_id': 'r2', 'seq': 1, 'key': 'name', 'put': 'bob',
         'clock': {'r1': 0, 'r2': 1}},
    ],
})
hid = r.json()['id']
c.post(f'/api/histories/{hid}/events', json={'events': [
    {'replica_id': 'r2', 'seq': 2, 'key': 'other', 'put': 'x',
     'clock': {'r1': 0, 'r2': 2}}
]})
c.post(f'/api/histories/{hid}/resolve-conflict', json={
    'key': 'name', 'replica_id': 'r1',
    'versions': [
        {'replica_id': 'r1', 'seq': 1},
        {'replica_id': 'r2', 'seq': 1},
    ], 'put': 'chosen'})
print(hid)
"""
    )
    assert create.returncode == 0, create.stderr
    hid = create.stdout.strip()

    # Process B (fresh import, same file): everything is still readable.
    read = run(
        f"""
import json
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.get('/api/histories/{hid}')
assert r.status_code == 200, r.text
data = r.json()
assert data['replica_ids'] == ['r1', 'r2']
assert [(e['replica_id'], e['seq']) for e in data['events']] == [
    ('r1', 1), ('r1', 2), ('r2', 1), ('r2', 2)
]
name = [x for x in data['results'] if x['key'] == 'name'][0]
assert name['state'] == 'put' and name['value'] == 'chosen'
print('ok')
"""
    )
    assert read.returncode == 0, read.stderr
    assert read.stdout.strip() == "ok"

