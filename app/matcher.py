"""Turn stored events into outbox rows.

Runs as a background task. Reads events whose processed_pass is behind their
delivery_count, evaluates every rule against them, and writes at most one
dm_task per (rule, user).

Crash safety: the decision rows and the processed_pass bump commit in the same
transaction, so re-running after a crash reproduces the same numbers rather
than inflating them.
"""
import asyncio
import hashlib
import json
import time
import uuid

from . import config, db


def dedupe_key(rule_id: str, user_id: str) -> str:
    """Identity of a DM obligation: this rule, this human, forever.

    user_id and not username -- usernames change, and the brief says so.
    """
    return hashlib.sha256(f"{rule_id}:{user_id}".encode("utf-8")).hexdigest()[:32]


def create_rule(keyword: str, dm_message: str) -> dict:
    rule_id = f"rule_{uuid.uuid4().hex[:12]}"
    now = time.time()
    with db.tx() as conn:
        conn.execute(
            "INSERT INTO rules (rule_id, keyword, keyword_lc, dm_message, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (rule_id, keyword, keyword.strip().lower(), dm_message, now),
        )
    return {"rule_id": rule_id, "keyword": keyword, "dm_message": dm_message}


def _matching_rules(conn, text: str) -> list:
    """Case-insensitive substring match, anywhere in the comment."""
    if not text:
        return []
    lowered = text.lower()
    rules = conn.execute("SELECT * FROM rules ORDER BY created_at").fetchall()
    return [r for r in rules if r["keyword_lc"] and r["keyword_lc"] in lowered]


def _handle_created(conn, event_id: str, pass_no: int, payload: dict) -> None:
    data = payload.get("data") or {}
    comment_id = data.get("comment_id")
    sender = data.get("from") or {}
    user_id = sender.get("user_id")
    text = data.get("text") or ""

    if comment_id:
        conn.execute(
            """
            INSERT INTO comments (comment_id, post_id, user_id, username, text, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(comment_id) DO UPDATE SET
                post_id  = COALESCE(excluded.post_id, comments.post_id),
                user_id  = COALESCE(excluded.user_id, comments.user_id),
                username = COALESCE(excluded.username, comments.username),
                text     = COALESCE(excluded.text, comments.text)
            """,
            (comment_id, data.get("post_id"), user_id, sender.get("username"),
             text, data.get("created_at")),
        )

    if not user_id:
        db.trace(conn, f"event:{event_id}", None, None, "no user_id in payload")
        return

    # A delete for this comment may already have arrived -- events are not
    # ordered. If so, we owe nobody a DM.
    deleted = conn.execute(
        "SELECT 1 FROM tombstones WHERE comment_id = ?", (comment_id,)
    ).fetchone()
    if deleted and comment_id:
        # The delete landed before the comment row existed, so the UPDATE in
        # _handle_deleted matched nothing. Stamp it now, or the comment reads as
        # live forever and every view of it is wrong.
        row = conn.execute(
            "SELECT deleted_at FROM tombstones WHERE comment_id = ?", (comment_id,)
        ).fetchone()
        conn.execute(
            "UPDATE comments SET deleted_at = ? WHERE comment_id = ? AND deleted_at IS NULL",
            (row["deleted_at"] if row else time.time(), comment_id),
        )

    now = time.time()
    source_ref = f"{event_id}#{pass_no}"

    for rule in _matching_rules(conn, text):
        key = dedupe_key(rule["rule_id"], user_id)

        if deleted:
            decision = db.D_SUPPRESSED_DELETED
        else:
            cur = conn.execute(
                """
                INSERT INTO dm_tasks (dedupe_key, rule_id, user_id, username, comment_id,
                                      message, state, idempotency_key, source_ref,
                                      next_attempt_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dedupe_key) DO NOTHING
                """,
                (key, rule["rule_id"], user_id, sender.get("username"), comment_id,
                 rule["dm_message"], db.QUEUED, key, source_ref, now, now, now),
            )
            if cur.rowcount == 1:
                decision = db.D_CREATED
                db.trace(conn, key, None, db.QUEUED,
                         f"matched rule {rule['rule_id']} from {source_ref}")
            else:
                # Already owed. Was it this exact delivery that created it (a
                # crash replay of our own work), or a genuine second trigger?
                existing = conn.execute(
                    "SELECT source_ref FROM dm_tasks WHERE dedupe_key = ?", (key,)
                ).fetchone()
                if existing and existing["source_ref"] == source_ref:
                    decision = db.D_CREATED
                else:
                    decision = db.D_DUPLICATE

        conn.execute(
            """
            INSERT INTO match_decisions (event_id, pass_no, rule_id, user_id,
                                         dedupe_key, decision, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id, pass_no, rule_id) DO NOTHING
            """,
            (event_id, pass_no, rule["rule_id"], user_id, key, decision, now),
        )


def _handle_deleted(conn, payload: dict) -> None:
    """A comment vanished. Anything we haven't sent yet, we no longer owe.

    Only `queued` tasks are cancelled. An `in_flight` task has an HTTP request
    outstanding that may already have been accepted, and `accepted`/`delivered`
    are past the point of no return -- we record the delete but leave the DM
    alone rather than lie about it.
    """
    data = payload.get("data") or {}
    comment_id = data.get("comment_id")
    if not comment_id:
        return
    now = time.time()

    conn.execute(
        "INSERT INTO tombstones (comment_id, deleted_at) VALUES (?, ?)"
        " ON CONFLICT(comment_id) DO NOTHING",
        (comment_id, now),
    )
    conn.execute(
        "UPDATE comments SET deleted_at = ? WHERE comment_id = ? AND deleted_at IS NULL",
        (now, comment_id),
    )

    doomed = conn.execute(
        "SELECT dedupe_key FROM dm_tasks WHERE comment_id = ? AND state = ?",
        (comment_id, db.QUEUED),
    ).fetchall()
    for row in doomed:
        conn.execute(
            "UPDATE dm_tasks SET state = ?, updated_at = ?, terminal_at = ?,"
            " last_error = 'comment deleted before send' WHERE dedupe_key = ? AND state = ?",
            (db.CANCELLED, now, now, row["dedupe_key"], db.QUEUED),
        )
        db.trace(conn, row["dedupe_key"], db.QUEUED, db.CANCELLED,
                 f"comment {comment_id} deleted")


def process_pending(limit: int = None) -> int:
    """Process every event delivery we haven't yet matched. Returns count."""
    limit = limit or config.MATCH_BATCH_SIZE
    rows = db.query(
        "SELECT event_id, delivery_count, processed_pass, payload FROM events"
        " WHERE processed_pass < delivery_count ORDER BY first_seen_at LIMIT ?",
        (limit,),
    )
    done = 0
    for row in rows:
        payload = json.loads(row["payload"])
        # Catch up one pass at a time; a redelivery that arrived while we were
        # busy gets its own pass on the next sweep.
        pass_no = row["processed_pass"] + 1
        with db.tx() as conn:
            event_type = payload.get("event_type", "")
            if event_type == "comment.created":
                _handle_created(conn, row["event_id"], pass_no, payload)
            elif event_type == "comment.deleted":
                _handle_deleted(conn, payload)
            conn.execute(
                "UPDATE events SET processed_pass = ? WHERE event_id = ? AND processed_pass = ?",
                (pass_no, row["event_id"], pass_no - 1),
            )
        done += 1
    return done


async def matcher_loop(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            worked = await asyncio.to_thread(process_pending)
        except Exception as exc:  # never let the loop die
            db.record_invariant("matcher_error", repr(exc))
            worked = 0
        # Busy while there's a backlog, otherwise idle politely.
        delay = 0 if worked >= config.MATCH_BATCH_SIZE else config.MATCH_INTERVAL_SECONDS
        try:
            await asyncio.wait_for(stop.wait(), timeout=max(delay, 0.01))
        except asyncio.TimeoutError:
            pass
