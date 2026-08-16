"""Per-person view: everything someone said, and everything they got back.

The stats page answers "how many". This answers "did Arjun get his price list,
and how long did it take". It is the view that makes the system legible to
someone who will never read the schema -- and it is assembled from the same
rows the graded numbers are counted from, so it cannot flatter them.
"""
import time

from . import db

_STATUS = {
    db.DELIVERED: ("delivered", "Delivered"),
    db.ACCEPTED: ("sending", "Sent, confirming delivery"),
    db.QUEUED: ("pending", "Queued for sending"),
    db.IN_FLIGHT: ("sending", "Sending now"),
    db.FAILED: ("failed", "Failed after retries"),
    db.CANCELLED: ("cancelled", "Cancelled, comment deleted"),
}


def people(limit: int = 60, offset: int = 0, only: str | None = None,
           search: str | None = None) -> dict:
    """Everyone who commented, newest activity first.

    `only` filters to delivered / pending / failed / none (commented but never
    triggered a rule), which is how you show a recruiter a specific case on
    demand instead of scrolling.
    """
    rows = db.query(
        """
        SELECT c.user_id,
               MAX(COALESCE(c.username, '')) AS username,
               COUNT(*)                       AS comment_count,
               MAX(c.created_at)              AS last_comment_at
        FROM comments c
        WHERE c.user_id IS NOT NULL
        GROUP BY c.user_id
        """
    )
    index = {r["user_id"]: dict(r) for r in rows}

    # Comments, oldest first, so each person reads as a conversation.
    for row in db.query(
        "SELECT comment_id, user_id, username, text, created_at, deleted_at"
        " FROM comments WHERE user_id IS NOT NULL ORDER BY comment_id"
    ):
        person = index.get(row["user_id"])
        if person is None:
            continue
        person.setdefault("comments", []).append({
            "comment_id": row["comment_id"],
            "text": row["text"],
            "deleted": row["deleted_at"] is not None,
        })
        if not person.get("username") and row["username"]:
            person["username"] = row["username"]

    for row in db.query(
        """SELECT t.user_id, t.state, t.message, t.dm_id, t.attempts,
                  t.resend_count, t.created_at, t.updated_at, t.last_error,
                  r.keyword
           FROM dm_tasks t LEFT JOIN rules r ON r.rule_id = t.rule_id"""
    ):
        person = index.setdefault(row["user_id"], {
            "user_id": row["user_id"], "username": "", "comment_count": 0,
            "last_comment_at": None})
        status, label = _STATUS.get(row["state"], ("pending", row["state"]))
        person.setdefault("dms", []).append({
            "message": row["message"],
            "keyword": row["keyword"],
            "state": row["state"],
            "status": status,
            "status_label": label,
            "dm_id": row["dm_id"],
            "attempts": row["attempts"],
            "resends": row["resend_count"],
            "seconds": round((row["updated_at"] or 0) - (row["created_at"] or 0), 1),
            "error": row["last_error"],
        })

    for row in db.query(
        "SELECT user_id, COUNT(*) AS n FROM match_decisions"
        " WHERE decision = ? GROUP BY user_id", (db.D_DUPLICATE,)
    ):
        if row["user_id"] in index:
            index[row["user_id"]]["duplicates_blocked"] = row["n"]

    for row in db.query(
        "SELECT user_id, COUNT(*) AS n FROM match_decisions"
        " WHERE decision = ? GROUP BY user_id", (db.D_SUPPRESSED_DELETED,)
    ):
        if row["user_id"] in index:
            index[row["user_id"]]["suppressed"] = row["n"]

    everyone = []
    for person in index.values():
        person.setdefault("comments", [])
        person.setdefault("dms", [])
        person.setdefault("duplicates_blocked", 0)
        person.setdefault("suppressed", 0)
        person["username"] = person.get("username") or person["user_id"]
        dms = person["dms"]
        person["outcome"] = (
            "delivered" if any(d["state"] == db.DELIVERED for d in dms)
            else "failed" if any(d["state"] == db.FAILED for d in dms)
            else "cancelled" if dms and all(d["state"] == db.CANCELLED for d in dms)
            else "pending" if dms
            # Matched a rule, but the comment was already deleted when we
            # evaluated it. Reporting this as "no keyword matched" would be a
            # plain lie about someone whose comment did match.
            else "suppressed" if person["suppressed"]
            else "none")
        everyone.append(person)

    if only and only != "all":
        everyone = [p for p in everyone if p["outcome"] == only]
    if search:
        needle = search.lower()
        everyone = [p for p in everyone
                    if needle in p["username"].lower()
                    or any(needle in (c["text"] or "").lower() for c in p["comments"])]

    everyone.sort(key=lambda p: (p["last_comment_at"] or ""), reverse=True)
    total = len(everyone)
    page = everyone[offset:offset + min(limit, 200)]
    return {"people": page, "total": total, "offset": offset}


def analytics() -> dict:
    """Live numbers for the accounts page, all derived at request time."""
    now = time.time()
    commenters = db.scalar(
        "SELECT COUNT(DISTINCT user_id) FROM comments WHERE user_id IS NOT NULL")
    comments = db.scalar("SELECT COUNT(*) FROM comments")
    triggered = db.scalar("SELECT COUNT(DISTINCT user_id) FROM dm_tasks")
    delivered = db.scalar("SELECT COUNT(*) FROM dm_tasks WHERE state = ?", (db.DELIVERED,))
    pending = db.scalar("SELECT COUNT(*) FROM dm_tasks WHERE state IN (?,?,?)",
                        db.OPEN_STATES)
    failed = db.scalar("SELECT COUNT(*) FROM dm_tasks WHERE state = ?", (db.FAILED,))
    cancelled = db.scalar("SELECT COUNT(*) FROM dm_tasks WHERE state = ?", (db.CANCELLED,))

    times = [r["s"] for r in db.query(
        "SELECT (updated_at - created_at) AS s FROM dm_tasks WHERE state = ?"
        " ORDER BY (updated_at - created_at)", (db.DELIVERED,))]
    median = times[len(times) // 2] if times else None
    slowest = times[-1] if times else None

    # Sends per minute for the last 20 minutes, for the throughput sparkline.
    buckets = []
    for i in range(19, -1, -1):
        start = now - (i + 1) * 60
        buckets.append(db.scalar(
            "SELECT COUNT(*) FROM send_log WHERE ts >= ? AND ts < ?",
            (start, start + 60)))

    top = [dict(r) for r in db.query(
        "SELECT text, COUNT(*) AS n FROM comments WHERE text IS NOT NULL"
        " GROUP BY text ORDER BY n DESC LIMIT 6")]

    resolved = delivered + failed + cancelled
    return {
        "commenters": commenters,
        "comments": comments,
        "triggered_a_rule": triggered,
        "delivered": delivered,
        "pending": pending,
        "failed": failed,
        "cancelled": cancelled,
        "duplicates_blocked": db.scalar(
            "SELECT COUNT(*) FROM match_decisions WHERE decision = ?", (db.D_DUPLICATE,)),
        "delivery_rate": round(delivered / resolved * 100, 1) if resolved else None,
        "median_seconds_to_deliver": round(median, 1) if median is not None else None,
        "slowest_seconds": round(slowest, 1) if slowest is not None else None,
        "sends_per_minute": buckets,
        "top_comments": top,
        "generated_at": now,
    }
