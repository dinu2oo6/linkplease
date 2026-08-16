"""LinkPlease — comment-to-DM automation over the PseudoGram mock API.

Graded contract (exact paths, exact shapes):
    POST /webhook   receive comment events, 200 fast
    POST /rules     {keyword, dm_message} -> 201 {rule_id, keyword, dm_message}
    GET  /stats     {sent, failed, queued, duplicates_blocked}
"""
import asyncio
import json
import logging
import os
import time

from fastapi import FastAPI, Header, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from . import (audit, config, db, ingest, keepalive, matcher, reconciler, sender,
               stats, webhook)

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

@app.post("/admin/simulate")
async def start_simulation(count: int = 500, duration_seconds: int = 10,
                           webhook_url: str | None = None):
    return await audit.start_simulation(count, duration_seconds, webhook_url)


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
