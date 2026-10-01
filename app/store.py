"""SQLite persistence for mergeable offline-event histories.

Each history is one table row holding the fixed ``replica_ids`` and the full
event list (canonical JSON).  Every request opens a short-lived connection:

* writes take a ``BEGIN IMMEDIATE`` transaction, which acquires the database
  reserved lock up front.  Concurrent writers therefore serialise, and a
  read-modify-write sequence (merge / conflict commit) can never interleave
  with another one -- no lost events, no double-allocated seqs, no stale
  ``versions`` overwriting a conflict;
* readers run on autocommit snapshot reads and never block on writers (WAL).
  A history row stores ``replica_ids`` and ``events`` together, so a single
  SELECT always yields both from the same committed state.

The database location comes from ``HISTORY_DB_PATH`` so tests and containers
can point it at an isolated file.  The schema is created lazily on the first
write, which keeps read-only/stateless usage from materialising a database
file.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from typing import Iterator

DEFAULT_DB_PATH = "history.db"
_BUSY_TIMEOUT_MS = 30_000

_SCHEMA = """
CREATE TABLE IF NOT EXISTS histories (
    id          TEXT PRIMARY KEY,
    replica_ids TEXT NOT NULL,
    events      TEXT NOT NULL
);
"""


def db_path() -> str:
    return os.environ.get("HISTORY_DB_PATH", DEFAULT_DB_PATH)


def _connect(*, for_write: bool) -> sqlite3.Connection:
    path = db_path()
    if for_write:
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)

    conn = sqlite3.connect(path, timeout=_BUSY_TIMEOUT_MS / 1000, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    if for_write:
        # WAL is a persistent file property; applying it per connection is
        # cheap and makes reads/writes concurrent.
        if path != ":memory:":
            conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(_SCHEMA)
        conn.execute("BEGIN IMMEDIATE")
    return conn


@contextlib.contextmanager
def write_transaction() -> Iterator[sqlite3.Connection]:
    """Serialised (BEGIN IMMEDIATE) write transaction, all-or-nothing."""

    conn = _connect(for_write=True)
    try:
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_history(history_id: str) -> sqlite3.Row | None:
    """Committed snapshot of one history, or None when it does not exist.

    Uses a fresh autocommit connection so the caller observes the most recent
    committed state (equivalent to a process restart reopening the file).
    """

    try:
        conn = _connect(for_write=False)
    except sqlite3.OperationalError:
        return None
    try:
        try:
            cursor = conn.execute(
                "SELECT id, replica_ids, events FROM histories WHERE id = ?",
                (history_id,),
            )
        except sqlite3.OperationalError:
            # No writer has created the schema yet -> no history can exist.
            return None
        return cursor.fetchone()
    finally:
        conn.close()


def insert_history(
    conn: sqlite3.Connection,
    history_id: str,
    replica_ids: list[str],
    events: list[dict],
) -> None:
    import json

    conn.execute(
        "INSERT INTO histories (id, replica_ids, events) VALUES (?, ?, ?)",
        (
            history_id,
            json.dumps(replica_ids, ensure_ascii=False),
            json.dumps(events, ensure_ascii=False),
        ),
    )


def select_for_update(
    conn: sqlite3.Connection, history_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT id, replica_ids, events FROM histories WHERE id = ?",
        (history_id,),
    ).fetchone()


def update_events(
    conn: sqlite3.Connection, history_id: str, events: list[dict]
) -> None:
    import json

    conn.execute(
        "UPDATE histories SET events = ? WHERE id = ?",
        (json.dumps(events, ensure_ascii=False), history_id),
    )
