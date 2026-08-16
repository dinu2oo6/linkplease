"""What each person actually received, from their side.

The stats say 95 delivered. This says "Priya got the price list at 14:03, after
one retry." That is what makes the system legible to someone who isn't reading
the schema, and it is the same rows the graded numbers are counted from.
"""
from . import db

_STATUS = {
    db.DELIVERED: ("delivered", "Message delivered"),
    db.ACCEPTED: ("sending", "Sent — waiting for delivery confirmation"),
    db.QUEUED: ("pending", "Queued — waiting for a rate-limit slot"),
    db.IN_FLIGHT: ("sending", "Sending now"),
    db.FAILED: ("failed", "Could not be delivered after retries"),
    db.CANCELLED: ("cancelled", "Not sent — the comment was deleted"),
}


def inboxes(limit: int = 40, state: str | None = None) -> dict:
    """One entry per person, newest activity first."""
    sql = """
        SELECT t.dedupe_key, t.user_id, t.state, t.message, t.attempts,
               t.resend_count, t.dm_id, t.created_at, t.updated_at, t.last_error,
               COALESCE(t.username, a.username) AS username,
               c.text AS comment_text, r.keyword
        FROM dm_tasks t
        LEFT JOIN demo_accounts a ON a.user_id = t.user_id
        LEFT JOIN comments c      ON c.comment_id = t.comment_id
        LEFT JOIN rules r         ON r.rule_id = t.rule_id
    """
    params: tuple = ()
    if state:
        sql += " WHERE t.state = ?"
        params = (state,)
    sql += " ORDER BY t.updated_at DESC LIMIT ?"
    params = params + (min(limit, 300),)

    out = []
    for row in db.query(sql, params):
        status, label = _STATUS.get(row["state"], ("pending", row["state"]))
        out.append({
            "user_id": row["user_id"],
            "username": row["username"] or row["user_id"],
            "comment": row["comment_text"],
            "keyword": row["keyword"],
            "message": row["message"],
            "status": status,
            "status_label": label,
            "attempts": row["attempts"],
            "resends": row["resend_count"],
            "dm_id": row["dm_id"],
            "sent_at": row["updated_at"],
            "waited_seconds": round(row["updated_at"] - row["created_at"], 1),
            "error": row["last_error"],
            "key": row["dedupe_key"],
        })

    counts = {r["state"]: r["n"] for r in db.query(
        "SELECT state, COUNT(*) AS n FROM dm_tasks GROUP BY state")}
    return {"inboxes": out, "counts": counts,
            "people": db.scalar("SELECT COUNT(DISTINCT user_id) FROM dm_tasks")}
