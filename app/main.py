"""HTTP API for reconciling multi-replica offline writes.

A batch of events (each carrying a per-replica sequence number and a vector
clock covering every replica) is validated against the causal history it
declares, and then reduced to the effective version of every key under the
vector-clock partial order.

Besides the resolution endpoint, a catch-up endpoint turns the same batch
into a directly replayable incremental transfer plan for a target replica:
given the target's ``known_clock`` (the highest seq contiguously received
from each source), missing events are scheduled into rounds of causally
satisfied dependencies.

A conflict-resolution endpoint turns a confirmed resolution of a still
conflicting key into a new, replayable event: it appends a put/delete event
to the writing replica, with a clock covering every conflicting version and
the writer's own prior history, and returns the new event together with the
key's post-append verdict.

The three endpoints above are stateless: request payloads are never persisted.
A second family of endpoints (/api/histories) gives multiple callers one
shared, persistent history created from a valid batch: callers then merge in
more offline events by ID (identical events are ignored, same
replica_id/seq with different content is 409) and commit conflict resolutions
by ID with only the original resolution parameters (key, writer, versions,
op) -- the stored versions are re-checked at commit time and stale lists get
409.  Histories live in a local SQLite database (HISTORY_DB_PATH, WAL) and
therefore survive container restarts.
"""

from __future__ import annotations

import json
import uuid
from typing import Annotated, Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StrictStr
from pydantic import model_validator

from . import store

app = FastAPI(
    title="Multi-Replica Offline Write Reconciler",
    version="1.0.0",
    description=(
        "Validate a batch of offline write/delete events from multiple "
        "replicas, resolve the effective (maximal) version of each key, and "
        "build replayable incremental catch-up plans for a target replica."
    ),
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class Event(BaseModel):
    """A single put/delete event observed on one replica."""

    model_config = ConfigDict(extra="forbid")

    replica_id: Annotated[
        str,
        Field(min_length=1, description="Non-empty ID of the originating replica."),
    ]
    seq: Annotated[
        int,
        Field(
            strict=True,
            ge=1,
            description="Per-replica sequence number, starting at 1.",
        ),
    ]
    key: Annotated[str, Field(min_length=1, description="Non-empty key.")]
    put: Optional[StrictStr] = Field(
        default=None, description="Set when the event is a put (empty string allowed)."
    )
    delete: Optional[StrictStr] = Field(
        default=None, description="Must equal 'delete' when the event is a deletion."
    )
    clock: dict[
        Annotated[str, Field(min_length=1)],
        Annotated[int, Field(strict=True, ge=0)],
    ] = Field(
        ...,
        description=(
            "Vector clock covering every replica ID; non-negative integer "
            "component per replica."
        ),
    )

    @model_validator(mode="after")
    def _exactly_one_op(self) -> "Event":
        has_put = self.put is not None
        has_delete = self.delete is not None
        if has_put == has_delete:
            raise ValueError(
                "exactly one of 'put' (string value) or 'delete' (\"delete\") "
                "must be provided"
            )
        if has_delete and self.delete != "delete":
            raise ValueError("the 'delete' field must equal the string \"delete\"")
        return self


class ResolveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    replica_ids: Annotated[
        list[Annotated[str, Field(min_length=1)]],
        Field(
            min_length=2,
            max_length=8,
            description="2-8 non-empty, unique replica IDs.",
        ),
    ]
    events: Annotated[
        list[Event],
        Field(min_length=1, max_length=120, description="1-120 events, any order."),
    ]


class CatchupRequest(ResolveRequest):
    known_clock: Annotated[
        dict[
            Annotated[str, Field(min_length=1)],
            Annotated[int, Field(strict=True, ge=0)],
        ],
        Field(
            ...,
            description=(
                "Per-source highest seq contiguously received by the target; "
                "one non-negative integer component per replica_id."
            ),
        ),
    ]


class ConflictVersion(BaseModel):
    """One of the currently maximal (conflicting) versions of a key."""

    model_config = ConfigDict(extra="forbid")

    replica_id: Annotated[
        str,
        Field(min_length=1, description="Replica that produced the version."),
    ]
    seq: Annotated[
        int,
        Field(
            strict=True,
            ge=1,
            description="Seq of the maximal event on that replica.",
        ),
    ]


class ConflictDecision(BaseModel):
    """The original conflict-resolution parameters, without any batch."""

    model_config = ConfigDict(extra="forbid")

    key: Annotated[str, Field(min_length=1, description="Conflicting key to resolve.")]
    replica_id: Annotated[
        str,
        Field(
            min_length=1,
            description="Replica that writes the resolution event; must be "
            "one of the history's replica_ids.",
        ),
    ]
    versions: Annotated[
        list[ConflictVersion],
        Field(
            min_length=2,
            description=(
                "The conflicting versions as seen when resolving: each with "
                "replica_id and seq; no duplicates, at least two."
            ),
        ),
    ]
    put: Optional[StrictStr] = Field(
        default=None, description="Set when the resolution is a put (empty string allowed)."
    )
    delete: Optional[StrictStr] = Field(
        default=None,
        description="Must equal 'delete' when the resolution is a deletion.",
    )

    @model_validator(mode="after")
    def _exactly_one_op(self) -> "ConflictDecision":
        has_put = self.put is not None
        has_delete = self.delete is not None
        if has_put == has_delete:
            raise ValueError(
                "exactly one of 'put' (string value) or 'delete' (\"delete\") "
                "must be provided"
            )
        if has_delete and self.delete != "delete":
            raise ValueError("the 'delete' field must equal the string \"delete\"")
        return self


class ResolveConflictRequest(ResolveRequest, ConflictDecision):
    """Original batch plus the confirmed resolution of one conflicting key."""


class MergeEventsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    events: Annotated[
        list[Event],
        Field(min_length=1, max_length=120, description="1-120 events to merge, any order."),
    ]


# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------


class BatchError(Exception):
    """A whole-batch semantic validation failure (HTTP 422)."""

    def __init__(self, errors: list[dict]):
        super().__init__("invalid event batch")
        self.errors = errors


class ConflictStateError(Exception):
    """The declared resolution no longer matches the batch state (HTTP 409)."""

    def __init__(self, errors: list[dict]):
        super().__init__("conflict resolution out of date")
        self.errors = errors


class HistoryNotFound(Exception):
    """No persistent history exists for the referenced ID (HTTP 404)."""

    def __init__(self, history_id: str):
        super().__init__(f"history {history_id!r} not found")
        self.history_id = history_id


def _err(code: str, message: str, event_index: int | None = None) -> dict:
    e: dict = {"code": code, "message": message}
    if event_index is not None:
        e["event_index"] = event_index
    return e


# ---------------------------------------------------------------------------
# Validation + resolution
# ---------------------------------------------------------------------------


def _clock_le(
    a: dict[str, int], b: dict[str, int], replicas: list[str]
) -> tuple[bool, bool]:
    """Return (<= under componentwise order, < under partial order).

    ``a <= b`` when every component of ``a`` is <= the corresponding
    component of ``b``; ``a < b`` when additionally some component is
    strictly smaller.
    """

    any_less = False
    for r in replicas:
        x, y = a[r], b[r]
        if x > y:
            return False, False
        if x < y:
            any_less = True
    return True, any_less


def validate_batch(
    replica_ids: list[str], events: list[Event]
) -> dict[tuple[str, int], Event]:
    """Validate the batch and index events by (replica_id, seq).

    Raises BatchError (-> 422) on any violation, pointing at event indices.
    """

    errors: list[dict] = []

    # Rule 1: replica IDs unique (cardinality/non-empty handled by schema).
    seen_replicas: set[str] = set()
    duplicate_replicas: set[str] = set()
    for rid in replica_ids:
        if rid in seen_replicas:
            duplicate_replicas.add(rid)
        seen_replicas.add(rid)
    for rid in sorted(duplicate_replicas):
        errors.append(
            _err("duplicate_replica_id", f"replica_id {rid!r} appears more than once")
        )

    # Per-replica structural checks, one pass.
    seen: dict[tuple[str, int], Event] = {}
    seen_index: dict[tuple[str, int], int] = {}
    seqs_by_replica: dict[str, list[int]] = {rid: [] for rid in replica_ids}

    for i, ev in enumerate(events):
        bad = False

        if ev.replica_id not in seen_replicas:
            errors.append(
                _err(
                    "unknown_replica_id",
                    f"event replica_id {ev.replica_id!r} is not listed in replica_ids",
                    i,
                )
            )
            bad = True
        else:
            seqs_by_replica[ev.replica_id].append(ev.seq)

        # Rule 3: clock covers exactly all replicas, non-negative integers.
        clock_keys = set(ev.clock)
        if clock_keys != seen_replicas:
            missing = sorted(seen_replicas - clock_keys)
            extra = sorted(clock_keys - seen_replicas)
            parts = []
            if missing:
                parts.append(f"missing components for {missing}")
            if extra:
                parts.append(f"unexpected components {extra}")
            errors.append(
                _err(
                    "clock_replica_coverage",
                    "clock must cover exactly all replica_ids: " + "; ".join(parts),
                    i,
                )
            )
            bad = True

        # Rule 4a: per-replica seq unique.
        key = (ev.replica_id, ev.seq)
        if key in seen:
            errors.append(
                _err(
                    "duplicate_seq",
                    f"replica {ev.replica_id!r} already has an event with seq {ev.seq} "
                    f"(event index {seen_index[key]})",
                    i,
                )
            )
            bad = True
        else:
            seen[key] = ev
            seen_index[key] = i

    # Rule 4b: per-replica seq must be a contiguous 1..N range.
    valid_seqs: dict[str, set[int]] = {}
    for rid, seqs in seqs_by_replica.items():
        if not seqs:
            continue
        uniq = set(seqs)
        valid_seqs[rid] = uniq
        expected = set(range(1, max(uniq) + 1))
        if len(uniq) != len(seqs) or uniq != expected:
            gaps = sorted(expected - uniq)
            errors.append(
                _err(
                    "non_contiguous_seq",
                    f"seqs for replica {rid!r} must be unique and start at 1 with no "
                    f"gaps; got sorted seqs {sorted(seqs)} (missing {gaps})",
                )
            )

    if errors:
        raise BatchError(errors)

    # From here the structure is sound; replica/seq/clock lookups are safe.

    # Rule 5: the clock's own component must equal this event's seq.
    for i, ev in enumerate(events):
        if ev.clock[ev.replica_id] != ev.seq:
            raise BatchError(
                [
                    _err(
                        "clock_self_component",
                        f"clock[{ev.replica_id!r}] = {ev.clock[ev.replica_id]} "
                        f"must equal seq = {ev.seq}",
                        i,
                    )
                ]
            )

    # Rules 6 + 7: referenced history must exist and its clocks must be
    # strictly smaller (componentwise <= with at least one strict <).
    for i, ev in enumerate(events):
        referenced: list[Event] = []

        # The immediately previous event on this replica.
        if ev.seq > 1:
            referenced.append(seen[(ev.replica_id, ev.seq - 1)])

        # For every other replica, the latest event its component points at;
        # component 0 means "nothing known".
        for rid in replica_ids:
            if rid == ev.replica_id:
                continue
            n = ev.clock[rid]
            if n > 0:
                if n not in valid_seqs.get(rid, set()):
                    raise BatchError(
                        [
                            _err(
                                "clock_points_to_missing_event",
                                f"clock[{rid!r}] = {n}, but replica {rid!r} has no "
                                f"event with seq {n}",
                                i,
                            )
                        ]
                    )
                referenced.append(seen[(rid, n)])

        for ref in referenced:
            le, any_less = _clock_le(ref.clock, ev.clock, replica_ids)
            if not le or not any_less:
                ref_idx = seen_index[(ref.replica_id, ref.seq)]
                kind = "not <= current clock" if not le else "equal to current clock"
                raise BatchError(
                    [
                        _err(
                            "clock_causality",
                            f"referenced event (index {ref_idx}, replica "
                            f"{ref.replica_id!r}, seq {ref.seq}) clock {ref.clock} is "
                            f"{kind} {ev.clock}; it must be componentwise <= with at "
                            f"least one strictly smaller component",
                            i,
                        )
                    ]
                )

    return seen


def _maximal_events(group: list[Event], replica_ids: list[str]) -> list[Event]:
    """Maximal events of one key under the vector-clock partial order.

    Maximal = no *other* event in the group is strictly greater.
    """

    maximals: list[Event] = []
    for ev in group:
        dominated = False
        for other in group:
            if other is ev:
                continue
            le, any_less = _clock_le(ev.clock, other.clock, replica_ids)
            if le and any_less:
                dominated = True
                break
        if not dominated:
            maximals.append(ev)

    maximals.sort(key=lambda e: (e.replica_id, e.seq))
    return maximals


def _event_shape(ev: Event, replica_ids: list[str]) -> dict:
    """Verdict-shaped event projection used in resolve responses."""

    return {
        "replica_id": ev.replica_id,
        "seq": ev.seq,
        "op": "put" if ev.put is not None else "delete",
        "value": ev.put,
        "clock": {rid: ev.clock[rid] for rid in replica_ids},
    }


def _event_wire(ev: Event) -> dict:
    """Event in request wire format (exactly one of put/delete present).

    The returned object can be appended to a batch's ``events`` and replayed
    through /api/resolve and /api/catchup unchanged.
    """

    out = {
        "replica_id": ev.replica_id,
        "seq": ev.seq,
        "key": ev.key,
    }
    if ev.put is not None:
        out["put"] = ev.put
    else:
        out["delete"] = "delete"
    out["clock"] = dict(ev.clock)
    return out


def _canonical_wire(ev: Event, replica_ids: list[str]) -> dict:
    """Wire form with clock keys in the history's replica_ids order."""

    out = _event_wire(ev)
    out["clock"] = {rid: ev.clock[rid] for rid in replica_ids}
    return out


def _wire_model(raw: dict, replica_ids: list[str]) -> Event:
    """Parse a stored wire event back into an Event (clock order irrelevant)."""

    payload = dict(raw)
    payload["clock"] = {rid: raw["clock"][rid] for rid in replica_ids}
    return Event.model_validate(payload)


def _load_row(row: object) -> tuple[str, list[str], list[Event]]:
    replica_ids: list[str] = json.loads(row["replica_ids"])
    events = [_wire_model(raw, replica_ids) for raw in json.loads(row["events"])]
    return row["id"], replica_ids, events


def _canonical_events(
    events: list[Event], replica_ids: list[str]
) -> list[dict]:
    """Canonical persisted/returned order: replica_id then seq."""

    return [
        _canonical_wire(ev, replica_ids)
        for ev in sorted(events, key=lambda e: (e.replica_id, e.seq))
    ]


def _history_state(
    history_id: str, replica_ids: list[str], events: list[Event]
) -> dict:
    """Full original-format batch plus the verdict over the same events."""

    ordered = sorted(events, key=lambda e: (e.replica_id, e.seq))
    return {
        "id": history_id,
        "replica_ids": list(replica_ids),
        "events": [_canonical_wire(ev, replica_ids) for ev in ordered],
        "results": resolve(ordered, replica_ids),
    }


def _batch_full_error() -> BatchError:
    return BatchError(
        [
            _err(
                "batch_full",
                "the history already contains 120 events; the 120-event "
                "limit leaves no room to merge or append more events",
            )
        ]
    )


def _merge_events(
    replica_ids: list[str],
    current: list[Event],
    incoming: list[Event],
) -> list[Event]:
    """Merge incoming events by (replica_id, seq) into the stored history.

    Identical events (same identity and same full content) are ignored; the
    same (replica_id, seq) with different content is a 409.  The merged list
    is then checked against the original batch validation and the 120-event
    cap (422).  Either the whole merge applies or nothing does.
    """

    stored = {(ev.replica_id, ev.seq): ev for ev in current}

    merged = list(current)
    for i, ev in enumerate(incoming):
        prior = stored.get((ev.replica_id, ev.seq))
        if prior is not None:
            if _canonical_wire(prior, replica_ids) != _canonical_wire(
                ev, replica_ids
            ):
                raise ConflictStateError(
                    [
                        {
                            "code": "event_content_mismatch",
                            "message": (
                                f"event with replica_id {ev.replica_id!r}, "
                                f"seq {ev.seq} already exists in this history "
                                "with different content; stored "
                                f"{_event_wire(prior)!r}, incoming "
                                f"{_event_wire(ev)!r}"
                            ),
                            "event_index": i,
                        }
                    ]
                )
            # Same identity, identical content -> ignore.
            continue
        stored[(ev.replica_id, ev.seq)] = ev
        merged.append(ev)

    if len(merged) > 120:
        raise _batch_full_error()

    # Re-run the original validation on the whole merged history.
    validate_batch(replica_ids, merged)
    return merged


def _key_verdict(key: str, group: list[Event], replica_ids: list[str]) -> dict:
    """Reduce the events touching one key to its effective-version verdict."""

    maximals = _maximal_events(group, replica_ids)

    if len(maximals) == 1:
        m = maximals[0]
        if m.put is not None:
            return {
                "key": key,
                "state": "put",
                "value": m.put,
                "event": _event_shape(m, replica_ids),
            }
        return {"key": key, "state": "deleted", "event": _event_shape(m, replica_ids)}

    return {
        "key": key,
        "state": "conflict",
        "events": [_event_shape(m, replica_ids) for m in maximals],
    }


def resolve(events: list[Event], replica_ids: list[str]) -> list[dict]:
    """Compute maximal events per key under the vector-clock partial order."""

    by_key: dict[str, list[Event]] = {}
    for ev in events:
        by_key.setdefault(ev.key, []).append(ev)

    return [
        _key_verdict(key, by_key[key], replica_ids)
        for key in sorted(by_key)
    ]


# ---------------------------------------------------------------------------
# Conflict resolution event
# ---------------------------------------------------------------------------


def build_resolution_event(
    replica_ids: list[str],
    events: list[Event],
    seen: dict[tuple[str, int], Event],
    key: str,
    writer: str,
    versions: list[ConflictVersion],
    put: Optional[str],
    delete: Optional[str],
) -> tuple[Event, dict]:
    """Build the replayable event that records a confirmed conflict resolution.

    Structural checks (unknown writer, duplicate versions, full batch) raise
    BatchError (422).  When the declared conflict no longer matches the batch
    -- the key is not conflicting, or the version list is stale -- raises
    ConflictStateError (409) and generates no event.

    Returns the new event and the key's verdict over the appended batch.
    """

    replica_set = set(replica_ids)

    # Structural 422s first.
    if writer not in replica_set:
        raise BatchError(
            [
                _err(
                    "unknown_replica_id",
                    f"writing replica_id {writer!r} is not listed in replica_ids",
                )
            ]
        )

    dupes: set[tuple[str, int]] = set()
    seen_versions: set[tuple[str, int]] = set()
    for v in versions:
        ident = (v.replica_id, v.seq)
        if ident in seen_versions:
            dupes.add(ident)
        seen_versions.add(ident)
    if dupes:
        raise BatchError(
            [
                _err(
                    "duplicate_conflict_version",
                    "conflict version list must contain no duplicates; "
                    f"repeated entries: {sorted(dupes)}",
                )
            ]
        )

    if len(events) >= 120:
        raise BatchError(
            [
                _err(
                    "batch_full",
                    "the original batch already contains 120 events; no room "
                    "to append the resolution event (batch limit is 120)",
                )
            ]
        )

    for v in versions:
        if v.replica_id not in replica_set:
            raise BatchError(
                [
                    _err(
                        "unknown_replica_id",
                        f"version replica_id {v.replica_id!r} is not listed in "
                        "replica_ids",
                    )
                ]
            )

    # 409s: the declared conflict must still be live and exactly observed.
    group = [ev for ev in events if ev.key == key]
    if not group:
        raise ConflictStateError(
            [
                {
                    "code": "key_not_conflicted",
                    "message": f"key {key!r} has no events in the batch",
                }
            ]
        )

    maximals = _maximal_events(group, replica_ids)
    if len(maximals) < 2:
        raise ConflictStateError(
            [
                {
                    "code": "key_not_conflicted",
                    "message": f"key {key!r} is no longer in conflict",
                }
            ]
        )

    current = {(m.replica_id, m.seq) for m in maximals}
    if seen_versions != current:
        raise ConflictStateError(
            [
                {
                    "code": "conflict_versions_mismatch",
                    "message": (
                        "the supplied version list is stale or incomplete: it "
                        f"must equal exactly all current maximal events of "
                        f"{key!r}; supplied {sorted(seen_versions)}, current "
                        f"maximals {sorted(current)}"
                    ),
                }
            ]
        )

    # The new seq continues the writer's tail (1 when it has no events yet).
    writer_max = max(
        (ev.seq for ev in events if ev.replica_id == writer), default=0
    )
    new_seq = writer_max + 1

    # Clock = componentwise maximum of every conflicting version's clock and
    # the writer's prior history, then advance the writer's own component to
    # the new seq.
    clock = {rid: 0 for rid in replica_ids}
    for m in maximals:
        for rid in replica_ids:
            if m.clock[rid] > clock[rid]:
                clock[rid] = m.clock[rid]
    if writer_max > 0:
        prior = seen[(writer, writer_max)]
        for rid in replica_ids:
            if prior.clock[rid] > clock[rid]:
                clock[rid] = prior.clock[rid]
    clock[writer] = new_seq

    new_event = Event(
        replica_id=writer,
        seq=new_seq,
        key=key,
        put=put,
        delete=delete,
        clock=clock,
    )

    appended = events + [new_event]

    # The construction guarantees this, but re-run the original validation on
    # the extended batch so the returned event is replayable by construction.
    validate_batch(replica_ids, appended)

    verdict = _key_verdict(
        key, [ev for ev in appended if ev.key == key], replica_ids
    )
    return new_event, verdict


# ---------------------------------------------------------------------------
# Incremental catch-up plan
# ---------------------------------------------------------------------------


def _max_seq_by_replica(
    replica_ids: list[str], events: list[Event]
) -> dict[str, int]:
    max_seq: dict[str, int] = {rid: 0 for rid in replica_ids}
    for ev in events:
        if ev.seq > max_seq[ev.replica_id]:
            max_seq[ev.replica_id] = ev.seq
    return max_seq


def validate_known_clock(
    replica_ids: list[str],
    known_clock: dict[str, int],
    max_seq: dict[str, int],
) -> None:
    """Validate known_clock against replicas and the batch.

    It must cover exactly all replicas with non-negative integer components,
    and no component may exceed that source's highest seq in this batch.
    Raises BatchError (-> 422) otherwise.
    """

    replica_set = set(replica_ids)
    clock_keys = set(known_clock)

    errors: list[dict] = []
    missing = sorted(replica_set - clock_keys)
    if missing:
        errors.append(
            _err(
                "known_clock_coverage",
                f"known_clock must cover exactly all replica_ids; missing "
                f"components for {missing}",
            )
        )
    extra = sorted(clock_keys - replica_set)
    if extra:
        errors.append(
            _err(
                "known_clock_coverage",
                f"known_clock must cover exactly all replica_ids; unexpected "
                f"components {extra}",
            )
        )
    if errors:
        raise BatchError(errors)

    over: list[str] = []
    for rid in replica_ids:
        if known_clock[rid] > max_seq[rid]:
            over.append(
                f"{rid!r}: known_clock {known_clock[rid]} > highest batch seq "
                f"{max_seq[rid]}"
            )
    if over:
        raise BatchError(
            [
                _err(
                    "known_clock_ahead_of_batch",
                    "known_clock component exceeds that source's highest seq "
                    "in the batch; "
                    + "; ".join(over),
                )
            ]
        )


def validate_received_events(
    replica_ids: list[str],
    events: list[Event],
    known_clock: dict[str, int],
) -> None:
    """Reject batches where an already-received event claims newer history.

    An event whose own seq is at or below its source's known_clock entry has
    already been received by the target; every component of its clock must
    then be at or below the corresponding known_clock entry.
    """

    for i, ev in enumerate(events):
        if ev.seq > known_clock[ev.replica_id]:
            continue
        beyond = {
            rid: ev.clock[rid]
            for rid in replica_ids
            if ev.clock[rid] > known_clock[rid]
        }
        if beyond:
            raise BatchError(
                [
                    _err(
                        "clock_exceeds_known_clock",
                        f"event already received (replica {ev.replica_id!r}, "
                        f"seq {ev.seq} <= known_clock {known_clock[ev.replica_id]}) "
                        f"but its clock {ev.clock} exceeds known_clock; "
                        f"components beyond target knowledge: {beyond}",
                        i,
                    )
                ]
            )


def plan_catchup(
    replica_ids: list[str],
    events: list[Event],
    known_clock: dict[str, int],
    max_seq: dict[str, int],
) -> dict:
    """Schedule missing events into replay rounds.

    A missing event is eligible once its previous event on the same replica
    and, for every other replica, the event its clock component points at
    are all either already at the target (covered by known_clock) or were
    scheduled in an earlier round.
    """

    remaining = {
        (ev.replica_id, ev.seq): ev
        for ev in events
        if ev.seq > known_clock[ev.replica_id]
    }

    rounds: list[list[dict]] = []
    received = dict(known_clock)

    while remaining:
        ready: list[Event] = []
        for (rid, n), ev in remaining.items():
            # Previous event on the same replica must be present.
            if n > 1 and n - 1 > received[rid]:
                continue

            # Events on other replicas pointed at by this clock component.
            blocked = any(
                other != rid
                and ev.clock[other] > received[other]
                for other in replica_ids
            )
            if blocked:
                continue

            ready.append(ev)

        if not ready:
            # Defensive: unreachable for a batch that passed causal
            # validation, reported rather than looping forever.
            raise BatchError(
                [
                    _err(
                        "catchup_unschedulable",
                        "missing events form a dependency cycle and cannot be "
                        "scheduled into rounds",
                    )
                ]
            )

        ready.sort(key=lambda e: (e.replica_id, e.seq))
        round_events: list[dict] = []
        for ev in ready:
            round_events.append(
                {
                    "replica_id": ev.replica_id,
                    "seq": ev.seq,
                    "key": ev.key,
                    "op": "put" if ev.put is not None else "delete",
                    "value": ev.put,
                    "clock": {rid: ev.clock[rid] for rid in replica_ids},
                }
            )
            # The local component is the highest seq observed from this source.
            received[ev.replica_id] = ev.seq
            del remaining[(ev.replica_id, ev.seq)]
        rounds.append(round_events)

    return {"rounds": rounds, "progress": {rid: max_seq[rid] for rid in replica_ids}}


# ---------------------------------------------------------------------------
# Routes / error handling
# ---------------------------------------------------------------------------


@app.exception_handler(BatchError)
async def batch_error_handler(request: Request, exc: BatchError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors},
    )


@app.exception_handler(ConflictStateError)
async def conflict_state_error_handler(
    request: Request, exc: ConflictStateError
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": exc.errors},
    )


@app.exception_handler(HistoryNotFound)
async def history_not_found_handler(
    request: Request, exc: HistoryNotFound
) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={
            "detail": [
                {
                    "code": "history_not_found",
                    "message": f"no history exists with id {exc.history_id!r}",
                }
            ]
        },
    )


@app.exception_handler(RequestValidationError)
async def request_validation_error_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    # Keep FastAPI's 422 shape, but drop the non-JSON-serializable 'ctx'
    # payload (it holds the original ValueError object).
    safe_errors: list[dict] = []
    for e in exc.errors():
        safe = {k: v for k, v in e.items() if k != "ctx"}
        safe_errors.append(safe)
    return JSONResponse(status_code=422, content={"detail": safe_errors})


@app.get("/health", tags=["meta"])
async def health() -> dict:
    return {"status": "ok"}


@app.post("/api/resolve", tags=["resolve"], response_model=None)
async def resolve_batch(body: ResolveRequest) -> dict:
    """Validate the batch and return the effective version of each key."""

    validate_batch(body.replica_ids, body.events)
    return {"results": resolve(body.events, body.replica_ids)}


@app.post("/api/resolve-conflict", tags=["resolve"], response_model=None)
async def resolve_conflict(body: ResolveConflictRequest) -> dict:
    """Record a confirmed conflict resolution as a replayable event.

    The original batch is validated with the same rules as /api/resolve; the
    supplied conflict versions must then be exactly the key's current maximal
    events (>= 2, no duplicates).  The new event continues the writing
    replica's seq tail with a clock covering the conflicting versions and the
    writer's prior history.
    """

    seen = validate_batch(body.replica_ids, body.events)
    new_event, verdict = build_resolution_event(
        replica_ids=body.replica_ids,
        events=body.events,
        seen=seen,
        key=body.key,
        writer=body.replica_id,
        versions=body.versions,
        put=body.put,
        delete=body.delete,
    )
    return {"event": _event_wire(new_event), "result": verdict}


@app.post("/api/catchup", tags=["catchup"], response_model=None)
async def catchup_batch(body: CatchupRequest) -> dict:
    """Build a replayable incremental transfer plan for a target replica."""

    validate_batch(body.replica_ids, body.events)
    max_seq = _max_seq_by_replica(body.replica_ids, body.events)
    validate_known_clock(body.replica_ids, body.known_clock, max_seq)
    validate_received_events(body.replica_ids, body.events, body.known_clock)
    return plan_catchup(
        body.replica_ids, body.events, body.known_clock, max_seq
    )


# ---------------------------------------------------------------------------
# Persistent histories (/api/histories)
#
# Every mutation runs inside one BEGIN IMMEDIATE transaction (see store.py),
# which serialises all read-modify-write sequences: merges and conflict
# commits can neither lose events nor allocate the same seq twice, and a
# commit is always evaluated against -- and written back to -- the current
# committed state.  Any validation/conflict failure rolls the transaction
# back, so each write is all-or-nothing.
# ---------------------------------------------------------------------------


@app.post("/api/histories", tags=["histories"], response_model=None, status_code=201)
def create_history(body: ResolveRequest) -> dict:
    """Validate a batch and persist it as a new history, returning its ID."""

    validate_batch(body.replica_ids, body.events)
    history_id = uuid.uuid4().hex
    with store.write_transaction() as conn:
        store.insert_history(
            conn,
            history_id,
            body.replica_ids,
            _canonical_events(body.events, body.replica_ids),
        )
    return {"id": history_id}


@app.get("/api/histories/{history_id}", tags=["histories"], response_model=None)
def read_history(history_id: str) -> dict:
    """Return the stored batch (original wire format) and its verdicts.

    Events are sorted by replica_id then seq; both the batch and ``results``
    come from one committed snapshot.  Unknown IDs get 404.
    """

    row = store.get_history(history_id)
    if row is None:
        raise HistoryNotFound(history_id)
    hid, replica_ids, events = _load_row(row)
    return _history_state(hid, replica_ids, events)


@app.post(
    "/api/histories/{history_id}/events",
    tags=["histories"],
    response_model=None,
)
def merge_history_events(history_id: str, body: MergeEventsRequest) -> dict:
    """Merge offline events into a history and return the merged state.

    Merge key is (replica_id, seq): identical events are ignored and the same
    identity with different content is 409.  The merged history must still
    satisfy the original validation and the 120-event limit, otherwise the
    whole merge is rejected with 422 and nothing is written.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, events = _load_row(row)
        merged = _merge_events(replica_ids, events, body.events)
        store.update_events(
            conn, hid, _canonical_events(merged, replica_ids)
        )
        return _history_state(hid, replica_ids, merged)


@app.post(
    "/api/histories/{history_id}/resolve-conflict",
    tags=["histories"],
    response_model=None,
)
def commit_history_conflict(history_id: str, body: ConflictDecision) -> dict:
    """Commit a confirmed conflict resolution into a stored history.

    Only the original resolution parameters are supplied (key, writing
    replica_id, versions, put/delete); the batch itself is read from the
    history under a serialised write lock.  ``versions`` must equal the
    key's current maximal events -- a stale list gets 409 and no event is
    generated.  The new event continues the writer's seq tail and is
    persisted together with the whole history atomically.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, events = _load_row(row)
        new_event, verdict = build_resolution_event(
            replica_ids=replica_ids,
            events=events,
            seen={(ev.replica_id, ev.seq): ev for ev in events},
            key=body.key,
            writer=body.replica_id,
            versions=body.versions,
            put=body.put,
            delete=body.delete,
        )
        merged = events + [new_event]
        store.update_events(
            conn, hid, _canonical_events(merged, replica_ids)
        )
        return {"id": hid, "event": _canonical_wire(new_event, replica_ids),
                "result": verdict}
