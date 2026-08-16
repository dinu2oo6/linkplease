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

# Any 64-bit constant; it only has to be the same in every process.
_SLOT_LOCK_KEY = 8123407771


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


def reserve_slot() -> bool:
    """Atomically claim one slot in the rolling window. True if we got it.

    Counting and then sending is not atomic, and that is not theoretical: Render
    starts a new instance before stopping the old one, so a deploy briefly runs
    two sender loops. Both passed the count check in the same instant and we
    took two 429s in production -- exactly the failure this file's FAILURES.md
    entry predicted, arriving on schedule.

    The whole reservation now happens inside one transaction holding a Postgres
    advisory lock, so concurrent instances serialise here. The counting and the
    write of the row that proves the slot was taken are no longer separable.
    SQLite needs no lock: db.tx() is already a single global writer.

    The row is written *before* the request goes out. Crashing between the two
    over-counts our own usage by one, costing 6 seconds of throughput;
    under-counting would cost a breached rate limit.
    """
    now = time.time()
    effective_max = config.RATE_LIMIT_MAX - config.RATE_LIMIT_HEADROOM
    with db.tx() as conn:
        if config.use_postgres():
            conn.execute("SELECT pg_advisory_xact_lock(?)", (_SLOT_LOCK_KEY,))
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM send_log WHERE ts > ?",
            (now - config.RATE_LIMIT_WINDOW,),
        ).fetchone()
        used = db.first_value(row, 0)
        if used >= effective_max:
            return False
        conn.execute("INSERT INTO send_log (ts) VALUES (?)", (now,))
    return True


async def await_slot(stop: asyncio.Event) -> bool:
    """Block until this process has actually reserved a send slot.

    Local pacing keeps one instance evenly spaced; the reservation above is what
    makes the limit hold when there is more than one.
    """
    while not stop.is_set():
        now = time.time()
        pace_wait = (_last_send_at + config.SEND_INTERVAL_SECONDS) - now
        if pace_wait > 0:
            try:
                await asyncio.wait_for(stop.wait(), timeout=min(pace_wait, 5.0))
                return False
            except asyncio.TimeoutError:
                continue

        if await asyncio.to_thread(reserve_slot):
            return True

        # Someone else holds the slots. Wait for the oldest one to age out.
        wait = await asyncio.to_thread(_window_wait, time.time())
        try:
            await asyncio.wait_for(stop.wait(), timeout=min(max(wait, 0.5), 5.0))
            return False
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


async def send_one(task: dict) -> None:
    # The send_log row was already written by reserve_slot(), inside the
    # transaction that decided we were allowed to send at all.
    global _last_send_at
    _last_send_at = time.time()
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
