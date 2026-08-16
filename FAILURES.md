# FAILURES.md

Every way this system can still lose a DM, send a duplicate, or report a wrong
number. Ordered roughly by how likely I think each one is to actually bite.

Numbers marked *(chaos)* come from runs against `scripts/chaos_server.py`, my
local clone of PseudoGram's failure modes. Numbers marked *(live)* come from
real 500-event simulations against the deployed app.

---

### 1. A resend after a `failed` status can deliver a second DM to a real human

This is the sharpest edge in the system and I put it there on purpose.

When `GET /v1/dm/{id}` reports `failed`, I resend. I cannot reuse the original
`Idempotency-Key` — that key is bound to the dead `dm_id`, and PseudoGram would
hand me back the same failed record forever. So a resend mints a fresh key
(`<dedupe_key>:r1`). If PseudoGram's `failed` status was itself wrong, or if the
DM was actually delivered and only the status record failed, that person gets
the message twice.

I chose a rare duplicate over a silent loss. For a price list that is the right
trade; for something transactional it would not be.

**Conditions:** any DM whose status reads `failed`, plus any DM stuck in `queued`
upstream for more than 180s. Capped at 3 resends per DM, so the worst case is 4
copies of one message.

*(chaos)* In a 500-event run this fired **54 times across 313 DMs** — 46 of the
313 needed at least one resend, or 15%, which is exactly the clone's silent-
failure rate. So this isn't a rare path I'm hedging about: on this workload it's
one DM in seven, and every one of them is a coin-flip I've chosen to resolve in
favour of sending again.

### 2. Two application processes would breach the rate limit

The rolling-window check reads `send_log` from the database, so two processes
would at least see each other's sends — but the check and the send are not
atomic, and the 6.1s pacing clock is per-process and in memory. Two workers would
each believe they owned a fresh cadence, and both could pass the window check in
the same instant.

The Dockerfile pins `--workers 1` and the service runs one instance. This is a
constraint I've written down, not one the code enforces. Anyone who scales it to
2 instances gets 429s within a minute.

**Fix:** the outbox claim already uses `UPDATE ... WHERE key = (SELECT ... LIMIT 1)
RETURNING *`, which is safe under concurrency; it's the governor that isn't. It
would need `SELECT ... FOR UPDATE SKIP LOCKED` and a token bucket held in a row
rather than in a module global. Now that production is on Postgres this is
genuinely available — I didn't do it because one process comfortably saturates a
10/min limit, and untested concurrency code is worse than a documented
single-writer constraint.

### 3. A crash between PseudoGram receiving a send and my writing the `dm_id` is only safe if their idempotency cache outlives my downtime

The sequence is: mark `in_flight` → POST → write `dm_id`. If the process dies
between the POST arriving at PseudoGram and my writing the response, the task is
recovered on boot as `queued` and resent under **the same** idempotency key. If
their cache still holds that key, I get the original `dm_id` back and nothing is
duplicated. Verified in `tests/test_sending.py::test_transport_error_retries_under_the_same_idempotency_key`
and via `kill -9` against the chaos server.

**What I don't know:** PseudoGram's idempotency cache TTL. My chaos clone never
expires keys, so my test proves my logic, not their behaviour. If their TTL is
shorter than my restart time, that DM goes out twice and neither my numbers nor
theirs would show it as a duplicate — it would look like one send from each of
two attempts.

### 4. A `comment.deleted` arriving while the send is in flight is ignored

Deletes cancel tasks in state `queued` only. Once a task is claimed it is
`in_flight` for the duration of one HTTP request (up to the 15s timeout), and a
delete landing in that window does not stop the DM. I deliberately shrank this
window by waiting for the rate-limit slot *before* claiming the task rather than
after — the task sits `queued` and cancellable through the whole 6.1s pacing
wait, and is only `in_flight` for the request itself.

**Conditions:** delete arrives within the ~50–300ms of an in-flight POST.
*(chaos)* not observed in a 567-delivery run, but the window is real.

### 5. Deleting the *first* comment cancels a DM that a later comment still justifies

A task records the `comment_id` that created it. If someone comments "PRICE"
twice and deletes only the first, the delete cancels the pending DM even though
their second comment is still live and still matches. They get nothing.

The inverse also holds: deleting the second comment does nothing, which is
correct. I picked per-comment cancellation because tracking "does any live
comment still justify this obligation" means re-deriving the whole match set on
every delete, and the wrong-direction case is rarer than the right one.

### 6. Rules created after an event arrives never apply to it

Events are matched once per delivery and then marked processed. Create a rule at
10:05 and the comment that arrived at 10:04 is never re-evaluated, so that person
never gets the DM. There is no backfill.

This is the correct behaviour for a live system and the wrong behaviour for
anyone who sets up their rules after pointing the webhook at us. `run_sim.py`
creates rules before starting a simulation for exactly this reason.

### 7. Terminal failures are terminal — there is no requeue

After 6 send attempts or 3 resends a DM is `failed` and nothing ever retries it.
If PseudoGram is down for ten minutes, everything that cycles through its
attempts in that window is permanently lost, and `/stats` will honestly report
them as `failed` forever. There is no admin endpoint to push them back into the
queue, which is the first thing I'd add.

### 8. `duplicates_blocked` is my definition of "duplicate", which may not be theirs

I count one per match evaluation that did not create a new obligation — covering
both redelivered `event_id`s and the same person commenting again. A duplicate
event that matches *no rule* is not counted, because no DM was ever going to be
sent, so nothing was blocked.

If PseudoGram's truth file counts every redelivered event regardless of whether
it matched, my number is lower than theirs.

*(chaos)* 126 blocked across 567 deliveries, of which 42 were redeliveries.

There is a second, subtler reason this number can't be checked exactly, and I
only found it by building the checker. **The expected count is a band, not a
number, because deletes race sends.** A comment deleted before its DM goes out
should produce nothing; the same comment deleted a second later correctly keeps
its DM. The truth file records what was sent, not the interleaving against my
own send clock, so no single expected value exists. For the 500-event run the
band was 307–315 unique DMs and 121–133 blocked duplicates; I landed at 313 and
126, strictly inside both — which is exactly where you'd expect to land when
some deletes win the race and some lose it.

I wrote the audit twice before getting this right. The first version ignored
deletes and reported 2 missing DMs; the second excluded every deleted comment
and reported 6 unjustified ones. Both were the ruler being wrong, and both
looked exactly like a bug in the system. `GET /audit/{run_id}` now reports the
band plus the three checks that *are* exact regardless of timing: no event lost,
no pair with a surviving comment left un-DMed, no DM invented from nothing.

### 9. `sent` lags reality by up to one reconcile interval, always downward

A DM PseudoGram has already delivered counts as `queued` until my reconciler
polls it (every 5s, 40 at a time). Under a large backlog that lag grows: 300
accepted DMs take ~40s to sweep. `sent` therefore understates and never
overstates, which is the direction I want to be wrong in.

### 10. Durability is only as good as the filesystem underneath it

This one started as a real defect and I fixed it while writing this file. I had
`PRAGMA synchronous=NORMAL`, which in WAL mode is durable against process death —
`kill -9` mid-drain conserves `sent + queued` exactly *(chaos: 20 before, 20
after)* — but not against the host losing power mid-fsync. A few seconds of
ingested events could vanish, and since they'd vanish *before* becoming tasks,
nothing in `/stats` would know they had existed. Silent loss, not visible loss.
It's now `synchronous=FULL`; an fsync per commit costs nothing at 10 DMs/min.

That reasoning still governs the SQLite path, which is what local development and
the whole test suite run on.

In production the question moved rather than disappeared. The ledger is now Neon
Postgres, so durability is Neon's `fsync` and Neon's replication rather than
mine, and I have verified neither. I've swapped a risk I could see and reason
about for one I have to take on trust — which is the usual trade when you move
onto managed infrastructure, and worth naming rather than treating as a
resolution.

### 11. The test suite and production run on different databases

Tests run against SQLite. Production runs against Postgres. That is a real gap:
54 passing tests prove the logic on a dialect the deployed system doesn't use.

The port was small — placeholders, auto-increment columns, the float type — and
every query that carries weight (`ON CONFLICT DO NOTHING`, `UPDATE ... RETURNING`,
`rowcount`) is common to both. But "small" is not "none", and the two behaviours
I'd least like to differ are exactly the two the whole design rests on: whether
`ON CONFLICT DO NOTHING` reports `rowcount = 0`, and whether
`UPDATE ... WHERE key = (SELECT ... LIMIT 1) RETURNING *` claims exactly one row
under concurrency.

I run the suite against the real Neon database before deploying, which closes
most of this. What it doesn't close: the tests exercise one writer, and Postgres
under genuine concurrency has isolation semantics SQLite simply doesn't have.

### 12. Render's free tier suspends the service, and the fix is a hack

Render stops a free service after ~15 minutes with no inbound HTTP. Background
work doesn't count, so a 30-minute drain — which by definition receives no
webhooks while it drains — gets suspended around the halfway mark.

`app/keepalive.py` works around it by calling our own public `/health` every 10
minutes, so the request re-enters through the router as inbound traffic. Two
honest limits:

- **It keeps the service awake; it cannot wake it up.** Once suspended, the
  keep-alive loop is suspended too. Only an outside request revives it, and then
  the boot path resumes the drain.
- A cold start is ~50 seconds. Any webhook arriving in that window is lost
  before it reaches my ledger, so **nothing in `/stats` would know it existed** —
  the same silent-loss shape as #14.

On a host with a persistent disk and no idle suspension this file doesn't exist.
That's what `fly.toml` is for.

### 13. Neon drops idle connections, and the ledger is one connection

The Postgres connection is a pool of one, because the rate governor and the
outbox claim are only correct with a single writer. Neon closes idle
connections, and a drain paced at one send per 6.1 seconds has plenty of idle.

`_PGConn` catches `OperationalError`/`InterfaceError` and reconnects once. If the
drop happens mid-transaction, that transaction is lost and retried by the loop
that owned it — which is safe for every write in this system, because they're
all either idempotent or replay-guarded. If the reconnect itself fails, the
error propagates and the loop records an invariant and continues; it does not
crash, but it also does not make progress until Neon comes back.

Neon's free tier also suspends a project after ~5 minutes idle. Wake-up is
~500ms, which the driver absorbs as a slow query.

### 14. One database, no backups

The entire ledger is one Neon project on the free tier. If it's lost, every
pending DM and every stat goes with it. No backups, no replica, nothing in the
design guards against it.

### 15. If my API key is rotated, every webhook is rejected and the events are gone

The key is the HMAC secret. A rotated key means every signature fails, `/webhook`
returns 401, and PseudoGram records a delivered-and-rejected response. Those
events are never retried and never appear in my ledger, so my numbers would look
*perfect* while I received nothing. A silent, invisible total failure.

### 16. `/rules` and `/admin/simulate` have no authentication

Anyone who finds the deployed URL can create rules that DM strangers on my key's
behalf, or start a 500-event simulation against my rate limit. For an assignment
with an unlisted URL this is a considered omission; in production it's a hole.

### 17. The live API's contract differs from its documentation, and I only found the differences I went looking for

I built the retry policy from the spec, then probed the real endpoint with my key
before deploying. Three things did not match:

| Documented | Actually |
|---|---|
| `202 Accepted` on success | **`200`** |
| `400 {"error": "invalid_request"}` for a bad payload | **`422`** with a FastAPI `detail` array |
| `{"error": "..."}` error bodies | `{"detail": "..."}` |

The second one was a live bug in my code. I treated `400` as terminal-no-retry
and everything else as retryable, so a genuinely malformed payload would have
been retried six times, burning six rate-limit slots per bad DM on something that
could never succeed. It is now "any 4xx except 408/425 is terminal", which is the
rule I should have written in the first place — a status-code allowlist built
from prose is a guess.

**What this implies is the actual failure mode:** there are almost certainly
other divergences I haven't hit, because I only probed the paths I thought to
probe. Anything the API does that I didn't test, I have handled according to a
document that has already been wrong three times out of three.

### 18. The tables grow forever

`events`, `dm_events`, `match_decisions` and `send_log` are never pruned. At
assignment scale this is a few MB. At "millions a month" the governor's
`COUNT(*) WHERE ts > now-60` over an unbounded `send_log` degrades, and the
volume fills. There is no retention policy.

---

## What I checked, and what I found

*(chaos)* 500-event / 10-second run, drained to completion — which took ~32
minutes, because 313 DMs at 10 per 60s is what the rate limit costs.

| | |
|---|---|
| Deliveries received | 567 (525 distinct, 42 redeliveries, 25 deletes) |
| Events lost or unprocessed | **0** |
| Rule matches evaluated | 448 |
| DM obligations created | 313 — 304 delivered, 9 cancelled by a delete |
| Still `failed` at the end | **0** |
| Duplicates blocked | 126 |
| Send requests issued | 433 (313 obligations + 5xx retries + 54 resends) |
| Peak sends in any rolling 60s | **9**, against a ceiling of 10 |
| `429`s received | **0** |
| Invariant violations of any kind | **0** |
| Duplicate DMs delivered | **0** |

`313 = 304 + 9` reconciles exactly. Audit verdict against the truth file: no
event lost, no DM missing, no DM invented, both counts inside the timing band.

`kill -9` mid-drain and restart: `sent + queued` conserved exactly (20 → 20), the
drain resumed, and the recovered in-flight task kept its idempotency key.

### Two things I got wrong while measuring, both in the ruler

The duplicate-DM count took two attempts. My first ledger grouped sends by
recipient alone and flagged two users as double-DMed. They hadn't been — they'd
tripped two *different* rules, and one DM per rule per person is the actual
guarantee. The unit is `(recipient, message)`, not recipient.

The audit took three; details in #8.

Both times the broken tool looked exactly like a broken system, and the instinct
was to go and fix the system. A checker that reports false positives is worse
than no checker.

### A third measurement bug: I was testing idempotency against nothing

For most of this build **the runs did not prove the idempotency key did anything
at all**, and I didn't notice because the number that would have told me was
zero and I read zero as good news.

The clone returned every 500 *before* creating the DM — the one case where
retrying is trivially safe. So across 433 send requests, the key deduplicated
exactly 0 of them. I had a passing unit test and a confident README paragraph
about a mechanism that had never once fired under load.

The clone now creates the DM and *then* fails, 10% of the time, which is the
ambiguous case the key exists for. Re-run:

| | |
|---|---|
| Send requests | 116 |
| Ambiguous 500s (DM created, response lost) | 9 |
| Times the key returned the original `dm_id` instead of sending again | **9** |
| Duplicate DMs delivered | **0** |
| `(recipient, message)` pairs accepted twice | 7 — all resends after a confirmed `failed`; none delivered twice |

That is the mechanism working against the failure it was built for. It is also
the third time on this project that a green number meant my test was blind
rather than my system correct.

### What none of this proves

- **PseudoGram's idempotency cache TTL (#3).** My clone never expires keys, so
  the run above proves my logic, not their behaviour. If their TTL is shorter
  than my restart time, the guarantee quietly stops holding and neither ledger
  would show it.
- **That their definition of a blocked duplicate matches mine (#8).**

Those are the two numbers I'd expect a grader's server-side log to disagree with
mine on.
