"""Part C: a 202 is a promise, not a delivery.

PseudoGram accepts a DM and then fails roughly 15% of them, and the only way to
find out is to ask. Status reads don't count against the rate limit, so we ask
about every accepted DM until it reaches a terminal state.

The non-obvious part is the resend. A DM that came back `failed` cannot be
retried under its original Idempotency-Key -- that key is bound to the dead
dm_id and would just hand us the same corpse back. So a resend deliberately
mints a fresh key (`<dedupe_key>:r1`, `:r2`, ...), which means a resend is the
one path in this system that can genuinely produce a second DM to a real human
if PseudoGram's `failed` status was itself a lie. That trade -- risk a rare
duplicate rather than accept a silent loss -- is the one we chose. It's in
FAILURES.md.
"""
import asyncio
import time

from . import config, db
from .sender import client


def _mark_delivered(task_key: str, detail: str) -> None:
    now = time.time()
    with db.tx() as conn:
        conn.execute(
            "UPDATE dm_tasks SET state = ?, updated_at = ?, terminal_at = ?,"
            " last_checked_at = ? WHERE dedupe_key = ? AND state = ?",
            (db.DELIVERED, now, now, now, task_key, db.ACCEPTED),
        )
        db.trace(conn, task_key, db.ACCEPTED, db.DELIVERED, detail)


def _schedule_resend(task: dict, reason: str) -> None:
    now = time.time()
    key = task["dedupe_key"]
    if task["resend_count"] >= config.MAX_RESENDS:
        with db.tx() as conn:
            conn.execute(
                "UPDATE dm_tasks SET state = ?, updated_at = ?, terminal_at = ?,"
                " last_checked_at = ?, last_error = ? WHERE dedupe_key = ? AND state = ?",
                (db.FAILED, now, now, now,
                 f"{reason}; {task['resend_count']} resends exhausted", key, db.ACCEPTED),
            )
            db.trace(conn, key, db.ACCEPTED, db.FAILED,
                     f"{reason}; giving up after {task['resend_count']} resends")
        return

    resend_no = task["resend_count"] + 1
    with db.tx() as conn:
        conn.execute(
            """
            UPDATE dm_tasks SET state = ?, resend_count = ?, idempotency_key = ?,
                   attempts = 0, next_attempt_at = ?, updated_at = ?,
                   last_checked_at = ?, last_error = ?, dm_id = NULL, accepted_at = NULL
            WHERE dedupe_key = ? AND state = ?
            """,
            (db.QUEUED, resend_no, f"{key}:r{resend_no}", now, now, now,
             reason[:500], key, db.ACCEPTED),
        )
        db.trace(conn, key, db.ACCEPTED, db.QUEUED,
                 f"{reason}; resend #{resend_no} (dm_id {task['dm_id']} abandoned, fresh key)")


def _touch(task_key: str) -> None:
    with db.tx() as conn:
        conn.execute(
            "UPDATE dm_tasks SET last_checked_at = ? WHERE dedupe_key = ?",
            (time.time(), task_key),
        )


async def check_one(task: dict) -> None:
    dm_id = task["dm_id"]
    if not dm_id:
        await asyncio.to_thread(_schedule_resend, task, "accepted without a dm_id")
        return
    try:
        resp = await client().get(f"/v1/dm/{dm_id}")
    except Exception as exc:
        # A read failure tells us nothing about the DM. Try again next sweep.
        await asyncio.to_thread(_touch, task["dedupe_key"])
        db.record_invariant("status_read_error", repr(exc))
        return

    if resp.status_code != 200:
        await asyncio.to_thread(_touch, task["dedupe_key"])
        return

    try:
        status = (resp.json() or {}).get("status")
    except Exception:
        await asyncio.to_thread(_touch, task["dedupe_key"])
        return

    if status == "delivered":
        await asyncio.to_thread(_mark_delivered, task["dedupe_key"], f"{dm_id} delivered")
    elif status == "failed":
        await asyncio.to_thread(
            _schedule_resend, task, f"{dm_id} reported failed after acceptance"
        )
    else:
        # Still queued on their side. Give it a while, then assume it's stuck.
        age = time.time() - (task["accepted_at"] or time.time())
        if age > config.RECONCILE_STUCK_SECONDS:
            await asyncio.to_thread(
                _schedule_resend, task, f"{dm_id} stuck in queued for {age:.0f}s"
            )
        else:
            await asyncio.to_thread(_touch, task["dedupe_key"])


def _due_for_check(limit: int = 40) -> list[dict]:
    rows = db.query(
        "SELECT * FROM dm_tasks WHERE state = ? ORDER BY last_checked_at LIMIT ?",
        (db.ACCEPTED, limit),
    )
    return [dict(r) for r in rows]


async def reconcile_once() -> int:
    tasks = await asyncio.to_thread(_due_for_check)
    if not tasks:
        return 0
    # Reads are free, but stay polite: 8 at a time.
    for i in range(0, len(tasks), 8):
        await asyncio.gather(*(check_one(t) for t in tasks[i:i + 8]))
    return len(tasks)


async def reconciler_loop(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await reconcile_once()
        except Exception as exc:
            db.record_invariant("reconciler_error", repr(exc))
        try:
            await asyncio.wait_for(stop.wait(), timeout=config.RECONCILE_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass
