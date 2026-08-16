"""Human-readable view of the pipeline: one comment becoming one DM.

The stats endpoint answers "how many". This answers "what happened to Arjun",
which is the question anyone watching a demo actually has. Every row here is
joined from the same ledger the graded numbers are counted from -- there is no
separate bookkeeping for the pretty view.
"""
import json

from . import db

_STATE_LABEL = {
    db.QUEUED: "waiting for a rate-limit slot",
    db.IN_FLIGHT: "sending now",
    db.ACCEPTED: "accepted — confirming delivery",
    db.DELIVERED: "delivered",
    db.FAILED: "gave up",
    db.CANCELLED: "cancelled — comment was deleted",
}


def recent(limit: int = 25) -> dict:
    """The most recently active DM obligations, newest first."""
    tasks = db.query(
        """
        SELECT t.dedupe_key, t.user_id, t.username, t.state, t.attempts,
               t.resend_count, t.dm_id, t.last_error, t.created_at, t.updated_at,
               t.comment_id, r.keyword, c.text AS comment_text
        FROM dm_tasks t
        LEFT JOIN rules r    ON r.rule_id = t.rule_id
        LEFT JOIN comments c ON c.comment_id = t.comment_id
        ORDER BY t.updated_at DESC
        LIMIT ?
        """,
        (min(limit, 100),),
    )

    out = []
    for task in tasks:
        row = dict(task)
        trace = db.query(
            "SELECT ts, from_state, to_state, detail FROM dm_events"
            " WHERE dedupe_key = ? ORDER BY id", (row["dedupe_key"],)
        )
        row["label"] = _STATE_LABEL.get(row["state"], row["state"])
        row["trace"] = [dict(t) for t in trace]
        row["seconds_to_resolve"] = (
            round(row["updated_at"] - row["created_at"], 1)
            if row["state"] in (db.DELIVERED, db.FAILED, db.CANCELLED) else None
        )
        out.append(row)
    return {"activity": out}


def recent_blocked(limit: int = 15) -> dict:
    """Duplicates we refused to send, with the comment that triggered them.

    The most counter-intuitive number on the dashboard is the one for DMs we
    deliberately did *not* send, so it deserves to be inspectable.
    """
    rows = db.query(
        """
        SELECT m.event_id, m.user_id, m.created_at, r.keyword, e.payload
        FROM match_decisions m
        LEFT JOIN rules r  ON r.rule_id = m.rule_id
        LEFT JOIN events e ON e.event_id = m.event_id
        WHERE m.decision = ?
        ORDER BY m.created_at DESC
        LIMIT ?
        """,
        (db.D_DUPLICATE, min(limit, 100)),
    )
    blocked = []
    for row in rows:
        payload = row["payload"]
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        data = (payload or {}).get("data") or {}
        blocked.append({
            "user_id": row["user_id"],
            "username": (data.get("from") or {}).get("username"),
            "text": data.get("text"),
            "keyword": row["keyword"],
            "at": row["created_at"],
            "reason": "already owed this DM for this rule",
        })
    return {"blocked": blocked}
