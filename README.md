# LinkPlease

Comment-to-DM automation over the PseudoGram mock API. Someone comments `PRICE`,
they get the price list — once, ever, no matter how hostile the network is.

Parts A + B + C.

---

## The graded contract

| Route | Behaviour |
|---|---|
| `POST /webhook` | Verifies the HMAC signature, writes the event, returns `200`. Median ~3 ms; no rule evaluation and no outbound HTTP on this path. |
| `POST /rules` | `{keyword, dm_message}` → `201 {rule_id, keyword, dm_message}`. Matching is case-insensitive, anywhere in the text. |
| `GET /stats` | `{sent, failed, queued, duplicates_blocked}` — four `COUNT(*)`s over the ledger, computed at request time. |

Also available: `GET /` (live dashboard), `GET /health`, `GET /rules`, `GET /tasks`,
`GET /dm/{dedupe_key}` (full state-transition trace for one DM),
`GET /stats?verbose=1`, `POST /admin/simulate`, `GET /audit/{run_id}`.

---

## How it works

```
POST /webhook ──▶ verify HMAC ──▶ INSERT OR IGNORE events ──▶ 200
                                          │
                       [matcher] ─────────┘  reads unprocessed events
                                          │
                                          ▼
                                  dm_tasks (durable outbox)
                                          │
                       [sender] ──────────┤  one send / 6.1s, Idempotency-Key
                                          │
                       [reconciler] ──────┘  polls GET /v1/dm/{id}, free reads
```

One FastAPI process, one SQLite file in WAL mode, three background asyncio tasks.
Nothing is held in memory that isn't also a row on disk, so `kill -9` mid-drain
loses nothing — the three loops re-scan the database on boot and carry on.

### The five decisions that matter

**1. Deduplication is a `PRIMARY KEY`, not an `if` statement.**
`dm_tasks.dedupe_key = sha256(rule_id:user_id)`. The matcher does
`INSERT ... ON CONFLICT DO NOTHING` and reads `rowcount`. The race the brief
calls out by name — two identical events passing a check before either writes —
has no window to happen in, because there is no check. Fifty concurrent
identical deliveries produce exactly one DM and 49 blocked duplicates
(`tests/test_webhook_and_matching.py`).

Identity is `user_id`, never `username`.

**2. Event-level dedupe is an optimisation; it is not the correctness mechanism.**
A redelivered `event_id` is *not* discarded at the door. It bumps
`delivery_count`, and the matcher evaluates it again as its own pass. It then
lands on the unique constraint like any other repeat and is counted honestly as
a blocked duplicate. This is why `duplicates_blocked` covers both PseudoGram's
~8% redelivery *and* the same person commenting five times, with no
double-counting and no separate bookkeeping.

**3. Every send carries an `Idempotency-Key`.**
Set to the `dedupe_key`. So a retry after a timeout, a 500, or a crash mid-flight
returns the *original* `dm_id` instead of sending a second DM. This is what makes
"no DM is silently lost" compatible with "never DM the same person twice": we can
retry aggressively precisely because retrying is free of consequence.

**4. `sent` means delivered, not accepted.**
A `202` is an acceptance and ~15% of them end up `failed`. Counting those as sent
is the single easiest way to inflate the graded number, so `sent` counts only
`state='delivered'` — confirmed by polling `GET /v1/dm/{dm_id}`. Accepted-but-
unconfirmed DMs sit in `queued`, which reads as "we still owe this person".
Our numbers therefore look *lower* than a naive implementation's mid-drain. That
is the point.

**5. The rate limiter paces rather than bursts.**
10 sends per rolling 60s is the binding constraint on the entire system, so
throughput is not something we can win. We send one every 6.1s (≈9.8/min) instead
of 10 back-to-back followed by a 60s stall. Even pacing means clock skew against
PseudoGram's window can't push us over, and a rolling-window count over
`send_log` backstops it. Any `429` we ever receive is recorded as an invariant
violation and shown in `/stats?verbose=1`; the expected count is zero.

Consequence worth stating plainly: **500 events produce ~300 unique DMs, which is
~30 minutes of draining.** A large `queued` right after a run is correct
behaviour, not a backlog bug.

### Reconciliation, and the one place we chose duplicates over loss

The reconciler polls every accepted DM until it reaches a terminal status.
Status reads don't count against the rate limit, so it polls hard.

On `failed`, it resends — under a **new** idempotency key (`<dedupe_key>:r1`).
It has to: the original key is bound to the dead `dm_id` and would return the
same corpse forever. That makes a resend the one path in this system that can
genuinely produce a second DM to a real human, if PseudoGram's `failed` status
was itself wrong. We chose that over accepting a silent loss. It's the first
entry in [FAILURES.md](FAILURES.md).

### `comment.deleted`

| When it arrives | What happens |
|---|---|
| Before the DM is sent | Task → `cancelled`. Excluded from all four headline numbers. |
| Before the `comment.created` (out of order) | Tombstone written; the matcher never creates the obligation. |
| After the DM is accepted or delivered | Recorded, DM left alone. We don't rewrite history. |

`cancelled` and `suppressed_deleted` are real suppressions, but folding them into
`duplicates_blocked` would pad a graded field, so they appear only under
`?verbose=1`.

---

## Running it

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env          # add your PSEUDOGRAM_API_KEY
.venv/bin/uvicorn app.main:app --port 8000
```

Get a key:

```bash
python scripts/apply_and_keygen.py --name "..." --email you@example.com \
    --phone "+91..." --linkedin https://linkedin.com/in/you
```

## Testing it

```bash
.venv/bin/python -m pytest          # 52 tests, ~2s
```

Covers the concurrent-dedupe race, forged and mismatched signatures, delete
before create, `400` never retried, `429` not consuming an attempt, a `500` that
secretly created the DM anyway, resend under a fresh key, crash recovery of
in-flight tasks, and the governor holding under a compressed-clock run.

### The chaos server

`scripts/chaos_server.py` is a local PseudoGram clone with the same failure modes
— 20% 500s, 10/60s rate limit, 15% silent post-acceptance failures, 8%
redelivery, reordering, idempotency keys. It lets us run 500-event simulations as
often as we like without burning the real rate limit, and it exposes `GET
/_ledger`, which the real API only shows the graders: every recipient we sent to,
and whether any human got two DMs.

```bash
python scripts/chaos_server.py &
PSEUDOGRAM_API_KEY=chaos-test-key PSEUDOGRAM_BASE_URL=http://127.0.0.1:8899 \
  PUBLIC_WEBHOOK_URL=http://127.0.0.1:8000/webhook \
  .venv/bin/uvicorn app.main:app --port 8000 &
python scripts/run_sim.py --target http://127.0.0.1:8000 --count 500 --local
```

### Self-audit

`GET /audit/{run_id}` fetches PseudoGram's own truth file for a run, replays it
through our rules, and diffs the result against our ledger:

- events they sent that never reached us,
- DMs we should have created and didn't,
- DMs we created that the truth doesn't justify,
- where our counts land inside the expected band.

It reports a **band** rather than a single expected number, because deletes race
sends: a comment deleted before its DM goes out owes nothing, while the same
comment deleted a second later correctly keeps its DM, and the truth file records
what was sent rather than the interleaving against our own send clock. The three
checks above are exact regardless of timing; the counts are bounded.

Every number in FAILURES.md that has a digit in it came from here.

---

## Deployment

Render free web service + Neon Postgres. One worker, deliberately — two would
each believe they owned 10 sends per minute and breach the limit
([FAILURES.md](FAILURES.md) #2).

**Why not SQLite in production?** The design wants a persistent disk, and no
free host still offers one — Render's disks are paid, Fly and Railway want a
card. So the ledger moved to managed Postgres, which is the same durability
guarantee bought a different way. The storage layer speaks both: SQLite for
local dev and the test suite, Postgres when `DATABASE_URL` is set. Only three
things differ between the dialects (placeholders, auto-increment, float type),
all handled in `app/db.py`.

```bash
# Neon: create a project, copy the connection string
# Render: New > Web Service > from this repo, runtime Docker, plan Free
#   env: PSEUDOGRAM_API_KEY, PSEUDOGRAM_EMAIL, DATABASE_URL, PUBLIC_WEBHOOK_URL
```

`render.yaml` declares the service; secrets are `sync: false` so they're set in
the dashboard, never committed.

**The free-tier catch:** Render suspends a service after ~15 minutes with no
*inbound* traffic, and background loops don't count — so a 30-minute drain would
be suspended halfway. `app/keepalive.py` calls our own `/health` every 10
minutes, which leaves the machine and comes back through Render's router as
inbound traffic. It keeps the service awake; it cannot wake it up. That's a
workaround for a hosting constraint, not engineering, and it's documented as
such.

`fly.toml` is kept in the repo: it's the deployment this was designed for, with
a real persistent volume and no idle suspension, and it's a two-command deploy
for anyone who has a card on Fly.
