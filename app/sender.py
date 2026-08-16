"""The outbox worker: claim one task, wait for a rate-limit slot, send it.

Throughput here is fixed by PseudoGram at 10 sends per rolling 60s, so there is
nothing to win by going fast. Everything is optimised for never sending twice
and never losing one instead.
"""
import asyncio
import random
import time

import httpx

from . import config, db

_client: httpx.AsyncClient | None = None
_last_send_at: float = 0.0


def client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            base_url=config.BASE_URL,
            timeout=config.HTTP_TIMEOUT_SECONDS,
            headers={"X-API-Key": config.API_KEY},
        )
    return _client


async def aclose() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


# --- recovery ---------------------------------------------------------------

def recover_in_flight() -> int:
    """Return tasks abandoned mid-send by a crash to the queue.

    Re-sending is safe: the task keeps its Idempotency-Key, so if PseudoGram
    did receive the original request we get the original dm_id back instead of
    a second DM. This is the entire reason we bother with idempotency keys.
    """
    now = time.time()
    with db.tx() as conn:
        rows = conn.execute(
            "SELECT dedupe_key FROM dm_tasks WHERE state = ?", (db.IN_FLIGHT,)
        ).fetchall()
        for row in rows:
            conn.execute(
                "UPDATE dm_tasks SET state = ?, next_attempt_at = ?, updated_at = ?"
                " WHERE dedupe_key = ?",
                (db.QUEUED, now, now, row["dedupe_key"]),
            )
            db.trace(conn, row["dedupe_key"], db.IN_FLIGHT, db.QUEUED,
                     "recovered after restart; same idempotency key")
    return len(rows)


def prime_pacing() -> None:
    """Carry the send cadence across a restart, so a reboot can't burst."""
    global _last_send_at
    _last_send_at = db.scalar("SELECT COALESCE(MAX(ts), 0) FROM send_log", (), 0.0)


# --- rate governor ----------------------------------------------------------

def _window_wait(now: float) -> float:
    """Seconds until the rolling window has room, per our own send log."""
    effective_max = config.RATE_LIMIT_MAX - config.RATE_LIMIT_HEADROOM
    cutoff = now - config.RATE_LIMIT_WINDOW
    n = db.scalar("SELECT COUNT(*) FROM send_log WHERE ts > ?", (cutoff,))
    if n < effective_max:
        return 0.0
    # Wait for enough of the oldest in-window sends to age out.
    offset = n - effective_max
    row = db.query_one(
        "SELECT ts FROM send_log WHERE ts > ? ORDER BY ts LIMIT 1 OFFSET ?",
        (cutoff, offset),
    )
    if row is None:
        return 0.0
    return max(0.0, row["ts"] + config.RATE_LIMIT_WINDOW - now + 0.05)


async def await_slot(stop: asyncio.Event) -> bool:
    """Block until it is safe to issue a send. False if we're shutting down."""
    while not stop.is_set():
        now = time.time()
        pace_wait = (_last_send_at + config.SEND_INTERVAL_SECONDS) - now
        wait = max(pace_wait, _window_wait(now))
        if wait <= 0:
            return True
        try:
            await asyncio.wait_for(stop.wait(), timeout=min(wait, 5.0))
        except asyncio.TimeoutError:
            continue
    return False


# --- claiming ---------------------------------------------------------------

def has_due_task() -> bool:
    return db.scalar(
        "SELECT COUNT(*) FROM (SELECT 1 FROM dm_tasks WHERE state = ? AND"
        " next_attempt_at <= ? LIMIT 1)",
        (db.QUEUED, time.time()),
    ) > 0


def claim_next() -> dict | None:
    """Atomically take the oldest due task. Single writer, so no lost updates.

    Called only once a rate-limit slot is already in hand, so a task spends
    almost no time in `in_flight` -- which keeps the window where a
    comment.deleted can't cancel it down to the length of one HTTP request.
    """
    now = time.time()
    with db.tx() as conn:
        row = conn.execute(
            """
            UPDATE dm_tasks SET state = ?, updated_at = ?
            WHERE dedupe_key = (
                SELECT dedupe_key FROM dm_tasks
                WHERE state = ? AND next_attempt_at <= ?
                ORDER BY next_attempt_at, created_at LIMIT 1
            )
            RETURNING *
            """,
            (db.IN_FLIGHT, now, db.QUEUED, now),
        ).fetchone()
        if row is None:
            return None
        task = dict(row)
        db.trace(conn, task["dedupe_key"], db.QUEUED, db.IN_FLIGHT,
                 f"attempt {task['attempts'] + 1}")
    return task


def _backoff(attempts: int) -> float:
    raw = min(config.BACKOFF_BASE_SECONDS * (2 ** attempts), config.BACKOFF_CAP_SECONDS)
    return raw * (0.5 + random.random() * 0.5)  # jitter, so retries don't convoy


# --- outcome handling -------------------------------------------------------

def _accept(task: dict, dm_id: str) -> None:
    now = time.time()
    with db.tx() as conn:
        conn.execute(
            "UPDATE dm_tasks SET state = ?, dm_id = ?, accepted_at = ?, updated_at = ?,"
            " attempts = attempts + 1, last_error = NULL WHERE dedupe_key = ?",
            (db.ACCEPTED, dm_id, now, now, task["dedupe_key"]),
        )
        db.trace(conn, task["dedupe_key"], db.IN_FLIGHT, db.ACCEPTED, f"202 dm_id={dm_id}")


def _retry_later(task: dict, reason: str, delay: float = None,
                 count_attempt: bool = True) -> None:
    now = time.time()
    attempts = task["attempts"] + (1 if count_attempt else 0)
    if count_attempt and attempts >= config.MAX_SEND_ATTEMPTS:
        _give_up(task, f"{reason} (after {attempts} attempts)")
        return
    delay = _backoff(attempts) if delay is None else delay
    with db.tx() as conn:
        conn.execute(
            "UPDATE dm_tasks SET state = ?, attempts = ?, next_attempt_at = ?,"
            " updated_at = ?, last_error = ? WHERE dedupe_key = ?",
            (db.QUEUED, attempts, now + delay, now, reason[:500], task["dedupe_key"]),
        )
        db.trace(conn, task["dedupe_key"], db.IN_FLIGHT, db.QUEUED,
                 f"{reason}; retry in {delay:.1f}s")


def _give_up(task: dict, reason: str) -> None:
    now = time.time()
    with db.tx() as conn:
        conn.execute(
            "UPDATE dm_tasks SET state = ?, updated_at = ?, terminal_at = ?,"
            " last_error = ?, attempts = attempts + 1 WHERE dedupe_key = ?",
            (db.FAILED, now, now, reason[:500], task["dedupe_key"]),
        )
        db.trace(conn, task["dedupe_key"], db.IN_FLIGHT, db.FAILED, reason)


def _log_send(ts: float) -> None:
    """Record rate-limit usage *before* the request leaves.

    If we crash between this row and the HTTP call we will have over-counted
    our own usage by one. Over-counting costs 6 seconds of throughput;
    under-counting costs a 429 and a breached rate limit.
    """
    with db.tx() as conn:
        conn.execute("INSERT INTO send_log (ts) VALUES (?)", (ts,))


async def send_one(task: dict) -> None:
    global _last_send_at
    _last_send_at = time.time()
    await asyncio.to_thread(_log_send, _last_send_at)
    body = {
        "recipient_user_id": task["user_id"],
        "message": task["message"],
        "comment_id": task["comment_id"],
    }
    try:
        resp = await client().post(
            "/v1/dm/send", json=body,
            headers={"Idempotency-Key": task["idempotency_key"]},
        )
    except Exception as exc:
        # We do not know whether it arrived. The idempotency key makes finding
        # out unnecessary: retrying returns the original dm_id if it did.
        _retry_later(task, f"transport error: {exc!r}")
        return

    if resp.status_code in (200, 201, 202):
        try:
            dm_id = resp.json().get("dm_id")
        except Exception:
            dm_id = None
        if dm_id:
            _accept(task, dm_id)
        else:
            _retry_later(task, f"accepted without dm_id: {resp.text[:200]}")
        return

    if resp.status_code == 429:
        # Our governor is supposed to make this impossible. Record it loudly.
        retry_after = float(resp.headers.get("Retry-After", 5) or 5)
        db.record_invariant(
            "rate_limited",
            f"429 with {db.scalar('SELECT COUNT(*) FROM send_log WHERE ts > ?', (time.time() - 60,))} sends in last 60s",
        )
        _retry_later(task, "rate limited", delay=retry_after + 1.0, count_attempt=False)
        return

    if 400 <= resp.status_code < 500 and resp.status_code not in (408, 425):
        # Client error: the request itself is wrong, so retrying cannot help and
        # burning five more rate-limit slots on it would starve DMs that could
        # still succeed. 408 and 425 are the exceptions -- those are timing, not
        # content, and are worth another go.
        #
        # Deliberately a range rather than `== 400`. The docs promise 400 for a
        # malformed payload; the live API actually returns 422. Probing it found
        # that, and a hardcoded 400 would have quietly retried every validation
        # error six times.
        _give_up(task, f"http {resp.status_code} (client error): {resp.text[:300]}")
        return

    _retry_later(task, f"http {resp.status_code}: {resp.text[:200]}")


async def sender_loop(stop: asyncio.Event) -> None:
    while not stop.is_set():
        if not await asyncio.to_thread(has_due_task):
            try:
                await asyncio.wait_for(stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            continue
        # Wait for the slot first, claim second. A task waiting out the pacing
        # interval stays `queued`, so a comment.deleted can still cancel it.
        if not await await_slot(stop):
            return
        task = await asyncio.to_thread(claim_next)
        if task is None:
            continue
        try:
            await send_one(task)
        except Exception as exc:
            db.record_invariant("sender_error", repr(exc))
            await asyncio.to_thread(_retry_later, task, f"sender crash: {exc!r}")
