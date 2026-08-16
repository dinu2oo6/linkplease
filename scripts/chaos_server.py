"""A local, hostile PseudoGram clone.

Same failure modes as the real thing, same shapes, no rate limit to burn:

  * 20% of /v1/dm/send return 500
  * 10 sends per rolling 60s, then 429 with Retry-After
  * 202 means accepted; ~15% of accepted DMs resolve to `failed` a few seconds
    later, the rest to `delivered`
  * Idempotency-Key returns the original dm_id instead of sending again
  * simulations redeliver ~8% of events, out of order, HMAC-signed

It also keeps a send ledger, which is the bit the real API only exposes to the
graders: `GET /_ledger` shows every recipient we sent to and how many times, so
a duplicate DM is impossible to hide from ourselves.

    python scripts/chaos_server.py            # listens on :8899
"""
import argparse
import asyncio
import hashlib
import hmac
import json
import random
import time
import uuid

import httpx
import uvicorn
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

SECRET = "chaos-test-key"

FAIL_RATE = 0.20          # /v1/dm/send returns 500 before creating anything
AMBIGUOUS_RATE = 0.10     # DM created, then a 500 -- the caller cannot tell
SILENT_FAIL_RATE = 0.15   # accepted, then quietly fails
DUPLICATE_RATE = 0.08     # webhook redeliveries
RATE_LIMIT = 10
RATE_WINDOW = 60.0

app = FastAPI(title="ChaosGram")

dms: dict[str, dict] = {}
idempotency: dict[str, str] = {}
send_times: list[float] = []
ledger: list[dict] = []
runs: dict[str, dict] = {}

KEYWORDS = ["PRICE", "price please", "LINK", "info", "COST"]
FILLER = ["🙏", "pls", "how much?", "dm me", "interested", "!!", "need this"]


@app.post("/v1/dm/send")
async def send_dm(request: Request,
                  idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")):
    body = await request.json()

    if idempotency_key and idempotency_key in idempotency:
        existing = idempotency[idempotency_key]
        ledger.append({"ts": time.time(), "recipient": body.get("recipient_user_id"),
                       "message": body.get("message", ""),
                       "dm_id": existing, "deduped": True, "key": idempotency_key})
        return JSONResponse({"dm_id": existing, "status": dms[existing]["status"]},
                            status_code=202)

    now = time.time()
    while send_times and send_times[0] < now - RATE_WINDOW:
        send_times.pop(0)
    if len(send_times) >= RATE_LIMIT:
        retry_after = int(send_times[0] + RATE_WINDOW - now) + 1
        return JSONResponse({"error": "rate_limited"}, status_code=429,
                            headers={"Retry-After": str(retry_after)})

    if not body.get("recipient_user_id") or not body.get("message"):
        # 422, not the documented 400 -- this is what the live API actually
        # returns for a malformed payload (FastAPI validation). Matching the
        # docs here instead of reality is how I missed it the first time.
        return JSONResponse(
            {"detail": [{"type": "missing", "loc": ["body", "recipient_user_id"],
                         "msg": "Field required"}]}, status_code=422)

    send_times.append(now)

    if random.random() < FAIL_RATE:
        return JSONResponse({"error": "internal_error"}, status_code=500)

    dm_id = f"dm_{uuid.uuid4().hex[:8]}"

    if random.random() < AMBIGUOUS_RATE:
        # The nastiest real failure: we accepted the DM and created it, then
        # the response never made it back. The caller has no idea whether it
        # landed. This is the ONLY case an Idempotency-Key actually saves you
        # from, so a clone that never does this cannot prove the key works.
        fate = "failed" if random.random() < SILENT_FAIL_RATE else "delivered"
        dms[dm_id] = {"dm_id": dm_id, "status": "queued", "fate": fate,
                      "recipient_user_id": body.get("recipient_user_id"),
                      "resolve_at": now + random.uniform(1.0, 6.0)}
        if idempotency_key:
            idempotency[idempotency_key] = dm_id
        ledger.append({"ts": now, "recipient": body.get("recipient_user_id"),
                       "message": body.get("message", ""),
                       "dm_id": dm_id, "deduped": False, "key": idempotency_key,
                       "ambiguous": True})
        return JSONResponse({"error": "internal_error"}, status_code=500)

    fate = "failed" if random.random() < SILENT_FAIL_RATE else "delivered"
    dms[dm_id] = {"dm_id": dm_id, "status": "queued", "fate": fate,
                  "recipient_user_id": body.get("recipient_user_id"),
                  "resolve_at": now + random.uniform(1.0, 6.0)}
    if idempotency_key:
        idempotency[idempotency_key] = dm_id
    ledger.append({"ts": now, "recipient": body.get("recipient_user_id"),
                   "message": body.get("message", ""),
                   "dm_id": dm_id, "deduped": False, "key": idempotency_key})
    return JSONResponse({"dm_id": dm_id, "status": "queued"}, status_code=202)


@app.get("/v1/dm/{dm_id}")
async def get_dm(dm_id: str):
    dm = dms.get(dm_id)
    if dm is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    if dm["status"] == "queued" and time.time() >= dm["resolve_at"]:
        dm["status"] = dm["fate"]
    return {"dm_id": dm_id, "status": dm["status"],
            "recipient_user_id": dm["recipient_user_id"],
            "updated_at": time.time()}


def _make_event(i: int) -> dict:
    user_id = f"usr_{i % 140:04d}"          # ~140 distinct humans
    keyword = random.choice(KEYWORDS + [""])
    text = f"{keyword} {random.choice(FILLER)}".strip() or "nice post"
    return {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "event_type": "comment.created",
        "sent_at": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "data": {
            "comment_id": f"cmt_{uuid.uuid4().hex[:8]}",
            "post_id": "post_44de1b",
            "text": text,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            "from": {"user_id": user_id, "username": f"user{i % 140}"},
        },
    }


async def _fire(run_id: str, webhook_url: str, count: int, duration: int, deletes: int):
    events = [_make_event(i) for i in range(count)]

    # Some comments get deleted afterwards; a few of those deletes are sent
    # *before* their create, which is the ordering case that matters.
    deleted = []
    for event in random.sample(events, min(deletes, len(events))):
        deleted.append({
            "event_id": f"evt_{uuid.uuid4().hex[:12]}",
            "event_type": "comment.deleted",
            "sent_at": event["sent_at"],
            "data": {"comment_id": event["data"]["comment_id"]},
        })

    deliveries = events + deleted
    duplicates = [dict(e) for e in random.sample(
        deliveries, int(len(deliveries) * DUPLICATE_RATE))]
    deliveries = deliveries + duplicates
    random.shuffle(deliveries)

    runs[run_id] = {"run_id": run_id, "events": deliveries,
                    "distinct": len({e["event_id"] for e in deliveries}),
                    "duplicates": len(duplicates)}

    gap = duration / max(len(deliveries), 1)
    async with httpx.AsyncClient(timeout=10.0) as client:
        for event in deliveries:
            raw = json.dumps(event, separators=(",", ":")).encode()
            sig = hmac.new(SECRET.encode(), raw, hashlib.sha256).hexdigest()
            try:
                await client.post(webhook_url, content=raw, headers={
                    "Content-Type": "application/json",
                    "X-PseudoGram-Signature": f"sha256={sig}",
                })
            except Exception:
                pass
            await asyncio.sleep(gap)


@app.post("/v1/simulate/start")
async def simulate_start(request: Request):
    body = await request.json()
    run_id = f"run_{uuid.uuid4().hex[:10]}"
    count = int(body.get("count", 500))
    asyncio.create_task(_fire(
        run_id, body["webhook_url"], count,
        int(body.get("duration_seconds", 10)), int(body.get("deletes", count * 0.05)),
    ))
    return {"run_id": run_id, "count": count}


@app.get("/v1/simulate/{run_id}/truth")
async def simulate_truth(run_id: str):
    run = runs.get(run_id)
    if run is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    return run


@app.get("/_ledger")
async def get_ledger():
    """Server-side truth: who did we actually deliver a DM to, and how often?

    The unit that matters is (recipient, message), not recipient alone. One
    human legitimately gets several DMs if they tripped several rules -- the
    guarantee is one DM per rule per person, and each rule has its own message.
    Grouping by recipient alone conflates those and reports false duplicates.

    Two counts, because they mean different things:

      accepted_twice   the same (recipient, message) accepted more than once.
                       Not necessarily a bug: a resend after a confirmed
                       `failed` is correct behaviour.
      delivered_twice  the same (recipient, message) that will actually reach
                       the human more than once. THIS is a real duplicate DM,
                       and it is the number that matters.
    """
    accepted: dict[tuple[str, str], int] = {}
    delivered: dict[tuple[str, str], int] = {}
    for row in ledger:
        if row["deduped"]:
            continue
        pair = (row["recipient"], row.get("message", "")[:40])
        accepted[pair] = accepted.get(pair, 0) + 1
        dm = dms.get(row["dm_id"])
        if dm and dm["fate"] == "delivered":
            delivered[pair] = delivered.get(pair, 0) + 1
    fmt = lambda d: {f"{r} | {m}": n for (r, m), n in d.items() if n > 1}
    return {
        "requests": len(ledger),
        "deduped_by_idempotency_key": sum(1 for r in ledger if r["deduped"]),
        "distinct_recipients": len({r for r, _ in accepted}),
        "distinct_recipient_message_pairs": len(accepted),
        "accepted_twice": fmt(accepted),
        "DUPLICATE_DMS_ACTUALLY_DELIVERED": fmt(delivered),
        "max_sends_in_any_60s": _max_window(),
    }


def _max_window() -> int:
    times = sorted(r["ts"] for r in ledger if not r["deduped"])
    best = 0
    for i, t in enumerate(times):
        j = i
        while j < len(times) and times[j] < t + RATE_WINDOW:
            j += 1
        best = max(best, j - i)
    return best


@app.post("/_reset")
async def reset():
    dms.clear(); idempotency.clear(); send_times.clear(); ledger.clear(); runs.clear()
    return {"ok": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8899)
    args = parser.parse_args()
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")
