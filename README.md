# LinkPlease

Someone comments `PRICE` on a post, they get DMed a price list — once, ever, over
an API that fails 20% of the time, redelivers events, reorders them, lies about
delivery, and allows 10 sends a minute.

Parts A + B + C.

> **The model:** a durable order book of people we owe a message to, worked by one
> clerk who is only allowed to send 10 DMs per minute.

Nothing is held in memory that isn't also a row on disk, so `kill -9` mid-drain
loses nothing. Throughput is fixed by the rate limit, so the only things worth
competing on are **not losing anything** and **not lying about the numbers**.

---

## Verified

Live 500-event run, drained to completion, audited against PseudoGram's own truth
file:

| | |
|---|---|
| Deliveries they attempted → we acknowledged | 531 → **531** |
| Recipients they expected → we DMed | 97 → **97** (0 missed, 0 invented) |
| Failed / queued at the end | 0 / 0 |
| Peak sends in any rolling 60s | **9** against a ceiling of 10 |
| Deploys during the drain | **4** — nothing lost, nothing double-sent |

68 tests, passing on SQLite and against real Postgres.
[FAILURES.md](FAILURES.md) documents 19 ways this can still go wrong.

---

## The graded contract

| Route | Behaviour |
|---|---|
| `POST /webhook` | Verify HMAC, store, return `200`. ~200ms; no matching or outbound HTTP on this path. |
| `POST /rules` | `{keyword, dm_message}` → `201 {rule_id, keyword, dm_message}`. Case-insensitive, matches anywhere. |
| `GET /stats` | `{sent, failed, queued, duplicates_blocked}` — four `COUNT(*)`s over the ledger, computed per request. |

Also: `/` (console), `/accounts`, `/checkpoints`, `/verify`, `/inbox`, `/people`,
`/analytics`, `/activity`, `/audit/{run_id}`, `/dm/{key}`, `/export.xlsx`,
`/health`, `/stats?verbose=1`.

---

## How it works

```
POST /webhook ──▶ verify HMAC ──▶ batched INSERT ──▶ 200
                                        │
                     [matcher] ─────────┘  reads unprocessed events
                                        │
                                        ▼
                                dm_tasks (durable outbox)
                                        │
                     [sender] ──────────┤  reserve slot, then send
                                        │
                     [reconciler] ──────┘  poll until confirmed
```

One process, one Postgres connection, four background tasks. All three loops
re-scan the database on boot, so a restart resumes exactly where it stopped.

### The decisions that matter

**Deduplication is a primary key, not an `if`.**
`dm_tasks.dedupe_key = sha256(rule_id:user_id)`. The matcher does
`INSERT ... ON CONFLICT DO NOTHING` and reads `rowcount`. The race the brief warns
about — two identical events both passing a check before either writes — has no
window to occur in, because there is no check. 50 concurrent identical deliveries
produce exactly one DM and 49 blocked duplicates.

Identity is `user_id`, never `username`.

**Event-level dedupe is an optimisation, not the correctness mechanism.**
A redelivered `event_id` is *not* discarded at the door. It increments
`delivery_count`, and the matcher evaluates it again as its own pass, where it
lands on the unique constraint like any other repeat. That is why
`duplicates_blocked` covers both redelivery and repeat commenters with one
mechanism and no double-counting.

**Every send carries an `Idempotency-Key`.**
Set to the `dedupe_key`. A retry after a timeout, a 500, or a crash mid-flight
returns the *original* `dm_id` instead of sending twice. This is what makes "never
lose a DM" compatible with "never send two" — retrying is free of consequence, so
we can be paranoid about loss.

**`sent` means delivered, not accepted.**
A `202` is an acceptance and ~15% of them fail afterwards. `sent` counts only
`state='delivered'`, confirmed by polling. Accepted-but-unconfirmed DMs sit in
`queued`. Our numbers read *lower* than a naive implementation's mid-drain, which
is the point.

**The governor paces, and reserves before sending.**
One send every 6.1s (~9.8/min) rather than 10 back-to-back then a 60s stall — even
pacing means clock skew against their server can't tip us over. The slot is
reserved by counting the window and writing the row that claims it inside one
transaction holding `pg_advisory_xact_lock`, so counting and claiming cannot be
separated even across instances. Any `429` is recorded as an invariant violation;
expected count is zero.

**Consequence worth stating:** 500 events produce ~100 unique DMs, which is ~10
minutes of draining. A large `queued` right after a run is the rate limit, not a
backlog bug.

### Reconciliation, and the one place we chose duplicates over loss

The reconciler polls every accepted DM until terminal. Status reads don't count
against the rate limit, so it polls hard.

On `failed`, it resends under a **new** idempotency key — it has to, since the
original is bound to the dead `dm_id`. That makes a resend the one path that can
genuinely produce a second DM if their `failed` status was wrong. We chose a rare
duplicate over a silent loss; it's [FAILURES.md](FAILURES.md) #1.

### `comment.deleted`

| Arrives | Result |
|---|---|
| Before the DM is sent | Task → `cancelled`, excluded from all four headline numbers |
| Before the `comment.created` | Tombstone written; the obligation is never created |
| After acceptance or delivery | Recorded, DM left alone — we don't rewrite history |

Cancellations and suppressions are real, but folding them into
`duplicates_blocked` would pad a graded field, so they appear under
`?verbose=1` only.

---

## The demo console

`/` is a working console, not a status page. Two tabs.

**Console** splits the two things people conflate:

1. **Real run** — PseudoGram fires signed events at our webhook; we call *their*
   `/v1/dm/send` and poll *their* `/v1/dm/{id}`. **Compare against their truth**
   then shows their record beside ours. The only check whose evidence comes from
   outside this system.
2. **Local simulator** — a controllable crowd (how many accounts, comments, what
   fraction match, how many redelivered, how many deleted) for showing a specific
   guarantee on demand. Comments are signed and delivered over HTTP to our own
   `/webhook`, so it exercises the whole pipeline rather than a shortcut into it.

**Accounts** shows every person, their comments, the DM they got, delivery status
and the PseudoGram `dm_id`, filterable by outcome — plus delivery rate, median time
to deliver, and a sends-per-minute sparkline against the ceiling.

### Checkpoints

Each guarantee from the brief, evaluated as a live query, reported **PASS / FAIL /
PENDING**.

`PENDING` is load-bearing: a guarantee nothing has exercised is not a pass. The
signature checkpoint stays `PENDING` while verification is merely *enabled* — it
passes only once something forged has actually bounced. So the flood mixes in
forged requests, and the checkpoint additionally asserts none reached the ledger.

`/export.xlsx` writes summary, checkpoints, DM log and blocked duplicates as four
sheets, read from the ledger at request time so it cannot disagree with `/stats`.

Endpoints that spend the rate limit (`/demo/*`, `/admin/simulate`) require
`DEMO_TOKEN` and return `404` when it's unset. Read-only views stay open.

---

## Running it

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env          # add PSEUDOGRAM_API_KEY and PSEUDOGRAM_EMAIL
.venv/bin/uvicorn app.main:app --port 8000
```

Get a key: `python scripts/apply_and_keygen.py --name "..." --email you@example.com
--phone "+91..." --linkedin https://linkedin.com/in/you`

> **The documented HMAC secret is wrong.** The brief says to sign with your API
> key; real signatures verify against the base64 half of the key *decoded*, which
> is your account email. Implementing Part B exactly as written rejects every event
> with `401` and reports four honest zeroes.
> `webhook.candidate_secrets()` accepts either.

## Testing it

```bash
.venv/bin/python -m pytest                                    # 68 tests, SQLite
TEST_DATABASE_URL="postgresql://.../linkplease_test" pytest   # and real Postgres
```

The Postgres run **must** use a different database from the deployed one — the
suite truncates every table before every test, and `conftest.py` refuses to start
if the names match. That guard exists because I once pointed it at production
mid-run and destroyed the ledger ([FAILURES.md](FAILURES.md) #14).

Covered: the concurrent-dedupe race, forged and mismatched signatures, signatures
signed with the decoded key prefix, delete-before-create, `4xx` never retried,
`429` not consuming an attempt, a `500` that secretly created the DM anyway,
resend under a fresh key, crash recovery of in-flight tasks, same-batch
redelivery, 16-way concurrent slot reservation, and the alarm that fires when
every request is being rejected.

### The chaos server

`scripts/chaos_server.py` clones PseudoGram's failure modes — 20% 500s, 10/60s
rate limit, 15% post-acceptance failures, 10% *ambiguous* failures (DM created,
then a 500), 8% redelivery, reordering, idempotency keys. It runs 500-event
simulations without burning the real rate limit, and exposes `/_ledger`: every
recipient we sent to, and whether any human received the same message twice.

```bash
python scripts/chaos_server.py &
PSEUDOGRAM_API_KEY=chaos-test-key PSEUDOGRAM_BASE_URL=http://127.0.0.1:8899 \
  PUBLIC_WEBHOOK_URL=http://127.0.0.1:8000/webhook \
  .venv/bin/uvicorn app.main:app --port 8000 &
python scripts/run_sim.py --target http://127.0.0.1:8000 --count 500 --local
```

---

## Deployment

Render free web service + Neon Postgres, one instance.

**Why not SQLite in production?** The design wants a persistent disk and no free
host still offers one. The ledger moved to managed Postgres — same durability
guarantee, bought differently. `app/db.py` speaks both: SQLite for local dev and
tests, Postgres when `DATABASE_URL` is set. Only placeholders, auto-increment and
the float type differ.

**One worker, deliberately.** Two would each believe they owned 10 sends a minute.
The slot reservation now makes that safe, but the pacing clock is still
per-process ([FAILURES.md](FAILURES.md) #13).

**Idle suspension.** Render stops a free service after ~15 minutes without inbound
traffic, and background work doesn't count — so a 10-minute drain would be
suspended halfway. `app/keepalive.py` calls our own `/health` every 10 minutes so
the request re-enters as inbound traffic. It keeps the service awake; it cannot
wake it. That's a workaround for a hosting constraint, not engineering.

`fly.toml` is kept as the deployment this was designed for — real persistent
volume, no idle suspension, two commands.
