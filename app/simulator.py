"""A controllable crowd of commenters, for demonstrating the system end to end.

PseudoGram's own simulator fires a fixed script at us. This one lets you choose
the shape of the traffic -- how many accounts, how many comments, what fraction
mention a keyword, how many arrive twice, how many get deleted -- which is what
you need to show a specific guarantee on demand rather than hoping the random
script happens to exercise it.

Every comment is signed and pushed through the real `/webhook` endpoint over
HTTP. Nothing here reaches into the pipeline's internals, so a flood exercises
signature verification, batched ingest, matching, dedupe, the rate governor and
reconciliation exactly as production traffic does. If the demo holds, the
system holds.
"""
import asyncio
import hashlib
import hmac
import json
import os
import random
import time
import uuid

import httpx

from . import config, db, webhook

FIRST = ["arjun", "priya", "rahul", "ananya", "vikram", "meera", "karan", "diya",
         "rohan", "sneha", "aditya", "kavya", "nikhil", "isha", "varun", "tara",
         "dev", "riya", "sameer", "neha", "yash", "aisha", "kabir", "zoya"]
LAST = ["shoots", "creates", "films", "clicks", "studio", "media", "official",
        "daily", "hq", "world", "vibes", "diaries"]

MATCHING = [
    "PRICE please 🙏", "what's the pricing?", "how much does this cost? PRICE",
    "Price list?", "send me the PRICE", "pricing please", "PRICE dm me",
    "price pls 🙏", "interested — what's the price", "PRICING info please",
]
NON_MATCHING = [
    "love this!!", "🔥🔥🔥", "wow just wow", "amazing content", "following since day 1",
    "tag me in the next one", "where do you shoot this", "this is so good",
    "need this in my life", "can you do a tutorial", "first!", "❤️",
]


def create_accounts(count: int) -> list[dict]:
    """Add `count` synthetic commenters, reusing any that already exist."""
    existing = db.scalar("SELECT COUNT(*) FROM demo_accounts")
    now = time.time()
    made = []
    with db.tx() as conn:
        for i in range(count):
            n = existing + i
            username = f"{random.choice(FIRST)}.{random.choice(LAST)}{n}"
            user_id = f"usr_{uuid.uuid4().hex[:10]}"
            conn.execute(
                "INSERT INTO demo_accounts (user_id, username, created_at)"
                " VALUES (?, ?, ?) ON CONFLICT(user_id) DO NOTHING",
                (user_id, username, now),
            )
            made.append({"user_id": user_id, "username": username})
    return made


def list_accounts(limit: int = 500) -> list[dict]:
    return [dict(r) for r in db.query(
        "SELECT user_id, username, created_at FROM demo_accounts"
        " ORDER BY created_at, user_id LIMIT ?", (min(limit, 2000),))]


def _sign(raw: bytes) -> str:
    secrets = webhook.candidate_secrets()
    if not secrets:
        return ""
    return "sha256=" + hmac.new(secrets[0], raw, hashlib.sha256).hexdigest()


def _comment_event(account: dict, text: str) -> dict:
    now = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    return {
        "event_id": f"evt_{uuid.uuid4().hex[:14]}",
        "event_type": "comment.created",
        "sent_at": now,
        "data": {
            "comment_id": f"cmt_{uuid.uuid4().hex[:10]}",
            "post_id": "post_demo",
            "text": text,
            "created_at": now,
            "from": {"user_id": account["user_id"], "username": account["username"]},
        },
    }


def _delete_event(comment_id: str) -> dict:
    now = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    return {
        "event_id": f"evt_{uuid.uuid4().hex[:14]}",
        "event_type": "comment.deleted",
        "sent_at": now,
        "data": {"comment_id": comment_id},
    }


def build_traffic(accounts: list[dict], comments: int, pct_matching: int,
                  pct_duplicate: int, pct_delete: int) -> list[dict]:
    """Assemble the delivery list, then shuffle it.

    Shuffling is the point: it makes deletes overtake their own comments and
    redeliveries land far from the original, which is exactly the ordering the
    real platform produces and the case most implementations get wrong.
    """
    events = []
    for _ in range(comments):
        account = random.choice(accounts)
        matching = random.randint(1, 100) <= pct_matching
        text = random.choice(MATCHING if matching else NON_MATCHING)
        events.append(_comment_event(account, text))

    deletes = [
        _delete_event(e["data"]["comment_id"])
        for e in random.sample(events, max(0, len(events) * pct_delete // 100))
    ]
    deliveries = events + deletes
    duplicates = [dict(e) for e in
                  random.sample(deliveries, max(0, len(deliveries) * pct_duplicate // 100))]
    deliveries += duplicates
    random.shuffle(deliveries)
    return deliveries


async def _deliver(client: httpx.AsyncClient, url: str, event: dict,
                   sem: asyncio.Semaphore, forge: bool = False) -> None:
    raw = json.dumps(event, separators=(",", ":")).encode()
    signature = ("sha256=" + uuid.uuid4().hex + uuid.uuid4().hex) if forge else _sign(raw)
    headers = {"Content-Type": "application/json", "X-PseudoGram-Signature": signature}
    async with sem:
        try:
            await client.post(url, content=raw, headers=headers)
        except Exception as exc:
            db.record_invariant("demo_delivery_failed", repr(exc))


async def run_flood(run_id: str, deliveries: list[dict], duration: float,
                    forgeries: int = 0) -> None:
    """Deliver the traffic, mixed with a handful of forged requests.

    The forgeries are the point of including them: a signature checkpoint that
    has never rejected anything is decoration, not evidence. These are properly
    formed events with a garbage HMAC, so every one of them must come back 401
    and none may reach the ledger.
    """
    url = "http://127.0.0.1:%s/webhook" % os.getenv("PORT", "8080")
    forged = [_comment_event({"user_id": f"usr_attacker_{i}",
                              "username": f"attacker{i}"}, "PRICE please")
              for i in range(forgeries)]
    plan = [(e, False) for e in deliveries] + [(e, True) for e in forged]
    random.shuffle(plan)

    gap = duration / max(len(plan), 1)
    sem = asyncio.Semaphore(20)
    tasks = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for event, forge in plan:
            tasks.append(asyncio.create_task(_deliver(client, url, event, sem, forge)))
            await asyncio.sleep(gap)
        await asyncio.gather(*tasks, return_exceptions=True)

    with db.tx() as conn:
        conn.execute(
            "UPDATE demo_runs SET finished_at = ?, status = 'complete' WHERE run_id = ?",
            (time.time(), run_id),
        )


def prepare_flood(accounts: int, comments: int, duration: float, pct_matching: int,
                  pct_duplicate: int, pct_delete: int) -> tuple[dict, list[dict]]:
    """Create accounts if needed and build the traffic. Does not deliver.

    Scheduling is left to the caller because this runs in a worker thread, where
    there is no event loop to attach a task to.
    """
    have = list_accounts()
    if len(have) < accounts:
        create_accounts(accounts - len(have))
        have = list_accounts()
    pool = have[:accounts] if accounts else have
    if not pool:
        return {"error": "no accounts"}, []

    deliveries = build_traffic(pool, comments, pct_matching, pct_duplicate, pct_delete)
    n_created = sum(1 for e in deliveries if e["event_type"] == "comment.created")
    n_deleted = len(deliveries) - n_created
    distinct = len({e["event_id"] for e in deliveries})
    matching_count = sum(
        1 for e in deliveries
        if e["event_type"] == "comment.created"
        and any(k in (e["data"]["text"] or "").lower() for k in _keywords())
    )

    # A few forged requests, so "we reject forgeries" is demonstrated rather
    # than asserted. Always at least one, capped so it stays a garnish.
    forgeries = max(1, min(10, len(deliveries) // 25))

    run_id = f"demo_{uuid.uuid4().hex[:10]}"
    with db.tx() as conn:
        conn.execute(
            """INSERT INTO demo_runs (run_id, started_at, accounts, comments,
                                      duplicates, deletes, matching, duration, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'running')""",
            (run_id, time.time(), len(pool), len(deliveries),
             len(deliveries) - distinct, n_deleted, matching_count, duration),
        )

    info = {
        "run_id": run_id,
        "accounts": len(pool),
        "deliveries": len(deliveries),
        "distinct_events": distinct,
        "redeliveries": len(deliveries) - distinct,
        "deletes": n_deleted,
        "matching_comments": matching_count,
        "forged_requests": forgeries,
        "duration_seconds": duration,
    }
    return info, deliveries


def _keywords() -> list[str]:
    return [r["keyword_lc"] for r in db.query("SELECT keyword_lc FROM rules")]


def latest_run() -> dict | None:
    row = db.query_one("SELECT * FROM demo_runs ORDER BY started_at DESC LIMIT 1")
    return dict(row) if row else None
