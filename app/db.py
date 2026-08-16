"""Storage layer. SQLite locally, Postgres in production.

Everything that matters is committed before we acknowledge anything. A restart
mid-drain resumes exactly where it left off, because no in-memory structure
holds state that isn't also a row here.

Single connection + a lock, on both backends. The rate governor and the outbox
claim are only correct with one writer, so the connection pool is deliberately
a pool of one -- see FAILURES.md #2.

The two backends exist because no free host still offers a persistent disk.
SQLite keeps the tests fast and dependency-free; Postgres is what actually
survives a restart on a free web service. SQL is written once, in SQLite
dialect, and translated where the dialects disagree -- which is only in three
places: placeholders, auto-increment columns, and the float type.
"""
import os
import sqlite3
import threading
import time
from contextlib import contextmanager

from . import config

# --- dm_tasks states --------------------------------------------------------
QUEUED = "queued"        # waiting for its turn at the rate governor
IN_FLIGHT = "in_flight"  # an HTTP send is currently outstanding
ACCEPTED = "accepted"    # API returned 202; delivery not yet confirmed
DELIVERED = "delivered"  # API confirmed delivered. This, and only this, is "sent".
FAILED = "failed"        # gave up
CANCELLED = "cancelled"  # comment was deleted before we sent

# States that mean "this DM is still owed to someone".
OPEN_STATES = (QUEUED, IN_FLIGHT, ACCEPTED)

# --- match decisions --------------------------------------------------------
D_CREATED = "created"
D_DUPLICATE = "duplicate"
D_SUPPRESSED_DELETED = "suppressed_deleted"

SCHEMA = """
CREATE TABLE IF NOT EXISTS rules (
    rule_id      TEXT PRIMARY KEY,
    keyword      TEXT NOT NULL,
    keyword_lc   TEXT NOT NULL,
    dm_message   TEXT NOT NULL,
    created_at   REAL NOT NULL
);

-- One row per distinct event_id. Redeliveries bump delivery_count and reset
-- processed, so the matcher sees each delivery as its own pass.
CREATE TABLE IF NOT EXISTS events (
    event_id       TEXT PRIMARY KEY,
    event_type     TEXT NOT NULL,
    sent_at        TEXT,
    first_seen_at  REAL NOT NULL,
    last_seen_at   REAL NOT NULL,
    delivery_count INTEGER NOT NULL DEFAULT 1,
    processed_pass INTEGER NOT NULL DEFAULT 0,
    payload        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_unprocessed
    ON events (processed_pass, delivery_count);

CREATE TABLE IF NOT EXISTS comments (
    comment_id  TEXT PRIMARY KEY,
    post_id     TEXT,
    user_id     TEXT,
    username    TEXT,
    text        TEXT,
    created_at  TEXT,
    deleted_at  REAL
);

-- comment.deleted that arrived before (or without) its comment.created.
CREATE TABLE IF NOT EXISTS tombstones (
    comment_id  TEXT PRIMARY KEY,
    deleted_at  REAL NOT NULL
);

-- The outbox. dedupe_key is the whole "never DM the same user twice for the
-- same rule" guarantee: it is a PRIMARY KEY, so the check-then-act race cannot
-- happen no matter how concurrent the arrival.
CREATE TABLE IF NOT EXISTS dm_tasks (
    dedupe_key       TEXT PRIMARY KEY,
    rule_id          TEXT NOT NULL,
    user_id          TEXT NOT NULL,
    username         TEXT,
    comment_id       TEXT,
    message          TEXT NOT NULL,
    state            TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    resend_count     INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  REAL NOT NULL DEFAULT 0,
    dm_id            TEXT,
    idempotency_key  TEXT NOT NULL,
    last_error       TEXT,
    last_checked_at  REAL NOT NULL DEFAULT 0,
    accepted_at      REAL,
    source_ref       TEXT NOT NULL,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    terminal_at      REAL
);
CREATE INDEX IF NOT EXISTS idx_tasks_due   ON dm_tasks (state, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_tasks_check ON dm_tasks (state, last_checked_at);
CREATE INDEX IF NOT EXISTS idx_tasks_cmt   ON dm_tasks (comment_id);

-- One row per (event delivery x rule) evaluation. Replay-safe by PK, so the
-- matcher can re-run after a crash without inflating any number.
CREATE TABLE IF NOT EXISTS match_decisions (
    event_id    TEXT NOT NULL,
    pass_no     INTEGER NOT NULL,
    rule_id     TEXT NOT NULL,
    user_id     TEXT,
    dedupe_key  TEXT,
    decision    TEXT NOT NULL,
    created_at  REAL NOT NULL,
    PRIMARY KEY (event_id, pass_no, rule_id)
);
CREATE INDEX IF NOT EXISTS idx_decisions ON match_decisions (decision);

-- Append-only trace of every state transition, for /dm/{key} and for debugging
-- a 500-event run after the fact.
CREATE TABLE IF NOT EXISTS dm_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key  TEXT NOT NULL,
    ts          REAL NOT NULL,
    from_state  TEXT,
    to_state    TEXT,
    detail      TEXT
);
CREATE INDEX IF NOT EXISTS idx_dm_events ON dm_events (dedupe_key, id);

-- Every outbound send request, for the rolling-window backstop.
CREATE TABLE IF NOT EXISTS send_log (
    id  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_send_log ON send_log (ts);

-- Things that should never happen (a 429, a signature failure). Surfaced in
-- /stats?verbose=1 so we notice rather than quietly absorbing them.
CREATE TABLE IF NOT EXISTS invariants (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      REAL NOT NULL,
    kind    TEXT NOT NULL,
    detail  TEXT
);
CREATE INDEX IF NOT EXISTS idx_invariants ON invariants (kind);

-- Simulation runs we started, so /audit can find them again after a restart.
CREATE TABLE IF NOT EXISTS sim_runs (
    run_id      TEXT PRIMARY KEY,
    started_at  REAL NOT NULL,
    count       INTEGER,
    duration    INTEGER
);
"""

_conn = None
_lock = threading.RLock()


def _pg_schema() -> str:
    """The same DDL, in Postgres dialect."""
    return (SCHEMA
            .replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
            .replace("REAL", "DOUBLE PRECISION"))


class _PGConn:
    """Wraps a psycopg connection so it speaks the sqlite3 API this code uses.

    Two adaptations: `?` placeholders become `%s`, and `.execute()` returns a
    cursor whose `.fetchall()` yields name-addressable rows. Everything else --
    ON CONFLICT, RETURNING, rowcount -- is already common to both dialects,
    which is why the port touched almost no query.
    """

    def __init__(self, dsn: str):
        import psycopg
        from psycopg.rows import dict_row
        self._psycopg = psycopg
        self._dsn = dsn
        self._raw = psycopg.connect(dsn, row_factory=dict_row, autocommit=False)

    def _reconnect(self) -> None:
        # Neon drops idle connections, and a 30-minute drain has long idle gaps.
        try:
            self._raw.close()
        except Exception:
            pass
        self._raw = self._psycopg.connect(
            self._dsn, row_factory=self._psycopg.rows.dict_row, autocommit=False)

    def execute(self, sql: str, params: tuple = ()):
        translated = sql.replace("?", "%s") if params else sql
        try:
            cur = self._raw.cursor()
            cur.execute(translated, params or None)
            return cur
        except (self._psycopg.OperationalError, self._psycopg.InterfaceError):
            self._reconnect()
            cur = self._raw.cursor()
            cur.execute(translated, params or None)
            return cur

    def commit(self):
        self._raw.commit()

    def rollback(self):
        try:
            self._raw.rollback()
        except Exception:
            self._reconnect()

    def close(self):
        self._raw.close()


def connect():
    global _conn
    if _conn is not None:
        return _conn

    if config.use_postgres():
        conn = _PGConn(config.DATABASE_URL)
        conn.execute(_pg_schema())
        conn.commit()
        _conn = conn
        return conn

    directory = os.path.dirname(os.path.abspath(config.DB_PATH))
    os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # FULL, not NORMAL. In WAL mode NORMAL is durable against process death but
    # can lose the last few commits if the host loses power mid-fsync -- and an
    # event lost before it becomes a task is lost *silently*, because nothing
    # downstream knows it existed. The cost is an fsync per commit, which at a
    # ceiling of 10 DMs/min is free. Buying durability with throughput we have
    # no use for is the easiest trade in this codebase.
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    conn.commit()
    _conn = conn
    return conn


@contextmanager
def tx():
    """Serialised write transaction."""
    conn = connect()
    with _lock:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def query(sql: str, params: tuple = ()) -> list:
    conn = connect()
    with _lock:
        rows = conn.execute(sql, params).fetchall()
        if config.use_postgres():
            return rows
        return rows


def query_one(sql: str, params: tuple = ()):
    rows = query(sql, params)
    return rows[0] if rows else None


def first_value(row, default=0):
    """First column of a row, whichever driver produced it.

    sqlite3.Row indexes positionally; psycopg's dict_row does not. Every
    single-column aggregate in this codebase goes through here.
    """
    if row is None:
        return default
    value = row[0] if isinstance(row, sqlite3.Row) else next(iter(row.values()))
    return default if value is None else value


def scalar(sql: str, params: tuple = (), default=0):
    return first_value(query_one(sql, params), default)


def trace(conn, dedupe_key: str, from_state: str | None,
          to_state: str | None, detail: str = "") -> None:
    """Append a transition to the audit trail. Caller owns the transaction."""
    conn.execute(
        "INSERT INTO dm_events (dedupe_key, ts, from_state, to_state, detail)"
        " VALUES (?, ?, ?, ?, ?)",
        (dedupe_key, time.time(), from_state, to_state, detail[:2000]),
    )


def record_invariant(kind: str, detail: str = "") -> None:
    with tx() as conn:
        conn.execute(
            "INSERT INTO invariants (ts, kind, detail) VALUES (?, ?, ?)",
            (time.time(), kind, detail[:2000]),
        )


def reset_for_tests() -> None:
    """Drop the cached connection so a test can point the DB somewhere else."""
    global _conn
    with _lock:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
        _conn = None


def drop_all() -> None:
    """Wipe every table. Used to give a Postgres test run a clean slate."""
    tables = ["sim_runs", "invariants", "send_log", "dm_events", "match_decisions",
              "dm_tasks", "tombstones", "comments", "events", "rules"]
    with tx() as conn:
        for table in tables:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
    reset_for_tests()
