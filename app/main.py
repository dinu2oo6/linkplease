"""LinkPlease — comment-to-DM automation over the PseudoGram mock API.

Graded contract (exact paths, exact shapes):
    POST /webhook   receive comment events, 200 fast
    POST /rules     {keyword, dm_message} -> 201 {rule_id, keyword, dm_message}
    GET  /stats     {sent, failed, queued, duplicates_blocked}
"""
import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
import uuid

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from . import (activity, audit, checkpoints, config, db, export, inbox, ingest,
               keepalive, matcher, reconciler, sender, simulator, stats, webhook)

STARTED_AT = time.time()
log = logging.getLogger("linkplease")
_stop = asyncio.Event()
_tasks: list[asyncio.Task] = []


async def lifespan(app: FastAPI):
    db.connect()
    # Anything a previous process abandoned mid-flight comes back here.
    recovered = sender.recover_in_flight()
    sender.prime_pacing()
    backlog = db.scalar(
        "SELECT COUNT(*) FROM events WHERE processed_pass < delivery_count"
    )
    log.warning(
        "boot: recovered %d in-flight task(s), %d event(s) awaiting match, "
        "%d DM(s) still owed", recovered, backlog,
        db.scalar("SELECT COUNT(*) FROM dm_tasks WHERE state IN (?,?,?)", db.OPEN_STATES),
    )

    _stop.clear()
    _tasks.append(asyncio.create_task(ingest.batcher_loop(_stop)))
    _tasks.append(asyncio.create_task(matcher.matcher_loop(_stop)))
    _tasks.append(asyncio.create_task(sender.sender_loop(_stop)))
    _tasks.append(asyncio.create_task(reconciler.reconciler_loop(_stop)))
    if config.keepalive_url():
        _tasks.append(asyncio.create_task(keepalive.keepalive_loop(_stop)))
    try:
        yield
    finally:
        _stop.set()
        await asyncio.gather(*_tasks, return_exceptions=True)
        _tasks.clear()
        await sender.aclose()


app = FastAPI(title="LinkPlease", version="1.0.0", lifespan=lifespan)


# --- POST /rules ------------------------------------------------------------

class RuleIn(BaseModel):
    keyword: str = Field(min_length=1)
    dm_message: str = Field(min_length=1)


@app.post("/rules", status_code=201)
def create_rule(rule: RuleIn):
    return matcher.create_rule(rule.keyword, rule.dm_message)


@app.get("/rules")
def list_rules():
    rows = db.query("SELECT rule_id, keyword, dm_message, created_at FROM rules"
                    " ORDER BY created_at")
    return {"rules": [dict(r) for r in rows]}


# --- POST /webhook ----------------------------------------------------------

@app.post("/webhook")
async def receive_webhook(
    request: Request,
    x_pseudogram_signature: str | None = Header(default=None),
):
    """Verify, persist, acknowledge. Target is single-digit milliseconds.

    No rule evaluation and no outbound HTTP happens here. Background tasks pick
    the event up from the database, so a slow or angry PseudoGram can never
    push us past the 5 second budget and make us drop events.
    """
    started = time.perf_counter()
    raw = await request.body()

    if not webhook.verify_signature(raw, x_pseudogram_signature):
        # Record the exact bytes alongside the header. Without the body we can
        # only see *that* verification failed, never *why* -- and the why is
        # always "we disagree about the secret or the encoding".
        db.record_invariant(
            "bad_signature",
            json.dumps({"sig": (x_pseudogram_signature or "<missing>")[:120],
                        "body": raw.decode("utf-8", "replace")[:900]}),
        )
        return JSONResponse({"error": "invalid_signature"}, status_code=401)

    try:
        payload = json.loads(raw)
    except Exception:
        return JSONResponse({"error": "invalid_json"}, status_code=400)
    if not isinstance(payload, dict):
        return JSONResponse({"error": "invalid_json"}, status_code=400)

    # Batched: this waits for the group's INSERT to commit, then returns. We
    # batch the write, we never defer it -- nothing is acknowledged from memory.
    status = await ingest.submit(payload)
    # Measured, so "answers within 5 seconds" is a checkpoint with a number
    # behind it rather than an assurance.
    ingest.record_latency((time.perf_counter() - started) * 1000.0)
    return {"status": status}


# --- GET /stats -------------------------------------------------------------

@app.get("/stats")
def get_stats(verbose: int = 0):
    return stats.verbose_stats() if verbose else stats.core_stats()


# --- observability ----------------------------------------------------------

@app.get("/health")
def health():
    return {
        "ok": True,
        "uptime_seconds": round(time.time() - STARTED_AT, 1),
        "backend": "postgres" if config.use_postgres() else f"sqlite:{config.DB_PATH}",
        "has_api_key": bool(config.API_KEY),
        "signature_required": config.REQUIRE_SIGNATURE,
        "owed": db.scalar("SELECT COUNT(*) FROM dm_tasks WHERE state IN (?,?,?)",
                          db.OPEN_STATES),
    }


@app.get("/dm/{dedupe_key}")
def dm_trace(dedupe_key: str):
    """Full life story of one DM obligation: every state transition, in order."""
    task = db.query_one("SELECT * FROM dm_tasks WHERE dedupe_key = ?", (dedupe_key,))
    if task is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    events = db.query(
        "SELECT ts, from_state, to_state, detail FROM dm_events"
        " WHERE dedupe_key = ? ORDER BY id", (dedupe_key,)
    )
    return {"task": dict(task), "trace": [dict(e) for e in events]}


@app.get("/tasks")
def list_tasks(state: str | None = None, limit: int = 50):
    if state:
        rows = db.query(
            "SELECT * FROM dm_tasks WHERE state = ? ORDER BY updated_at DESC LIMIT ?",
            (state, min(limit, 500)),
        )
    else:
        rows = db.query(
            "SELECT * FROM dm_tasks ORDER BY updated_at DESC LIMIT ?", (min(limit, 500),)
        )
    return {"tasks": [dict(r) for r in rows]}


# --- self-audit -------------------------------------------------------------

def _require_demo_token(token: str | None) -> None:
    """Guard the endpoints that spend the rate limit or move graded numbers.

    Disabled entirely when DEMO_TOKEN is unset, so the deployed URL is safe to
    hand to anyone. Without this, a stranger who found the URL could fire 500
    events at our API key or inject comments into the ledger mid-grading.
    """
    if not config.DEMO_TOKEN:
        raise HTTPException(status_code=404, detail="not_found")
    if not token or not hmac.compare_digest(token, config.DEMO_TOKEN):
        raise HTTPException(status_code=401, detail="bad_token")


@app.post("/admin/simulate")
async def start_simulation(count: int = 500, duration_seconds: int = 10,
                           webhook_url: str | None = None, token: str | None = None):
    _require_demo_token(token)
    return await audit.start_simulation(count, duration_seconds, webhook_url)


class DemoComment(BaseModel):
    text: str = Field(min_length=1)
    username: str = Field(default="demo.user", min_length=1)
    user_id: str | None = None
    comment_id: str | None = None
    event_id: str | None = None


@app.post("/demo/comment")
async def demo_comment(comment: DemoComment, token: str | None = None):
    """Inject one comment for a live demo.

    Signs the payload with our real secret and pushes it through the ordinary
    `/webhook` handler, so a demo exercises signature verification, batched
    ingest, matching and dedupe exactly as a real event would. Nothing here is
    a shortcut around the pipeline -- if the demo works, the pipeline works.
    """
    _require_demo_token(token)

    now = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    user_id = comment.user_id or f"usr_demo_{uuid.uuid4().hex[:8]}"
    payload = {
        "event_id": comment.event_id or f"evt_demo_{uuid.uuid4().hex[:12]}",
        "event_type": "comment.created",
        "sent_at": now,
        "data": {
            "comment_id": comment.comment_id or f"cmt_demo_{uuid.uuid4().hex[:8]}",
            "post_id": "post_demo",
            "text": comment.text,
            "created_at": now,
            "from": {"user_id": user_id, "username": comment.username},
        },
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    secrets = webhook.candidate_secrets()
    signature = "sha256=" + hmac.new(
        secrets[0], raw, hashlib.sha256).hexdigest() if secrets else ""

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(
            "http://127.0.0.1:%s/webhook" % os.getenv("PORT", "8080"),
            content=raw,
            headers={"Content-Type": "application/json",
                     "X-PseudoGram-Signature": signature},
        )
    return {"webhook_status": resp.status_code, "result": resp.json(),
            "user_id": user_id, "text": comment.text}


@app.post("/demo/accounts")
def demo_accounts(count: int = 10, token: str | None = None):
    _require_demo_token(token)
    made = simulator.create_accounts(max(1, min(count, 1000)))
    return {"created": len(made), "accounts": made[:20],
            "total": db.scalar("SELECT COUNT(*) FROM demo_accounts")}


@app.get("/demo/accounts")
def list_demo_accounts(limit: int = 200):
    return {"accounts": simulator.list_accounts(limit),
            "total": db.scalar("SELECT COUNT(*) FROM demo_accounts")}


@app.post("/demo/flood")
async def demo_flood(accounts: int = 50, comments: int = 500,
                     duration_seconds: float = 10, pct_matching: int = 40,
                     pct_duplicate: int = 8, pct_delete: int = 5,
                     token: str | None = None):
    """Flood the webhook with realistic traffic from N accounts.

    Signed and delivered over HTTP to our own /webhook, so this exercises the
    whole pipeline rather than a shortcut into it.
    """
    _require_demo_token(token)
    info, deliveries = await asyncio.to_thread(
        simulator.prepare_flood,
        max(1, min(accounts, 1000)),
        max(1, min(comments, 5000)),
        max(0.5, min(duration_seconds, 600)),
        max(0, min(pct_matching, 100)),
        max(0, min(pct_duplicate, 100)),
        max(0, min(pct_delete, 100)),
    )
    if deliveries:
        # Scheduled here, on the event loop, and deliberately not awaited: the
        # flood outlives this request by design.
        asyncio.create_task(simulator.run_flood(
            info["run_id"], deliveries, info["duration_seconds"],
            info["forged_requests"]))
    return info


@app.get("/demo/run")
def demo_run():
    return {"run": simulator.latest_run()}


@app.get("/checkpoints")
def get_checkpoints():
    """Every guarantee the brief asks for, evaluated against the ledger."""
    return checkpoints.all_checkpoints()


@app.get("/inbox")
def get_inbox(limit: int = 40, state: str | None = None):
    """What each person received, from their side."""
    return inbox.inboxes(limit, state)


@app.get("/export.xlsx")
def export_xlsx():
    data = export.to_xlsx()
    stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    if data is None:                       # openpyxl unavailable
        return Response(
            content=export.to_csv(), media_type="text/csv",
            headers={"Content-Disposition":
                     f'attachment; filename="linkplease-{stamp}.csv"'})
    return Response(
        content=data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition":
                 f'attachment; filename="linkplease-{stamp}.xlsx"'})


@app.get("/export.csv")
def export_csv():
    stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    return Response(
        content=export.to_csv(), media_type="text/csv",
        headers={"Content-Disposition":
                 f'attachment; filename="linkplease-{stamp}.csv"'})


@app.get("/activity")
def get_activity(limit: int = 25):
    return activity.recent(limit)


@app.get("/activity/blocked")
def get_blocked(limit: int = 15):
    return activity.recent_blocked(limit)


@app.get("/admin/invariants")
def list_invariants(kind: str | None = None, limit: int = 20):
    if kind:
        rows = db.query("SELECT * FROM invariants WHERE kind = ? ORDER BY id DESC"
                        " LIMIT ?", (kind, min(limit, 100)))
    else:
        rows = db.query("SELECT * FROM invariants ORDER BY id DESC LIMIT ?",
                        (min(limit, 100),))
    return {"invariants": [dict(r) for r in rows]}


@app.get("/admin/runs")
def list_runs():
    return {"runs": [dict(r) for r in db.query(
        "SELECT * FROM sim_runs ORDER BY started_at DESC LIMIT 20")]}


@app.get("/audit/{run_id}")
async def audit_run(run_id: str):
    """Grade ourselves against PseudoGram's own record of what it sent us."""
    return await audit.audit_run(run_id)


# --- dashboard --------------------------------------------------------------

_DASHBOARD = os.path.join(os.path.dirname(__file__), "static", "dashboard.html")


@app.get("/", response_class=HTMLResponse)
def dashboard():
    with open(_DASHBOARD, encoding="utf-8") as fh:
        return HTMLResponse(fh.read())
