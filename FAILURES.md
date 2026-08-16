# FAILURES.md

Every way this system can still lose a DM, send a duplicate, or report a wrong
number, with the conditions that trigger it.

Numbers marked *(live)* come from real runs against PseudoGram and the deployed
app. *(local)* means the chaos clone in `scripts/chaos_server.py`.

---

## What was actually measured

*(live)* 500-event run, drained to completion, audited against their truth file:

| | |
|---|---|
| Deliveries they attempted → we acknowledged | 531 → **531** |
| Recipients they expected → we DMed | 97 → **97** (0 missed, 0 invented) |
| Failed / queued at the end | 0 / 0 |
| Peak sends in any rolling 60s | **9** against a ceiling of 10 |
| `429`s received | **0** |
| Deploys during the drain | **4** — nothing lost, nothing double-sent |

*(local)* 567 deliveries incl. 42 redeliveries and 25 deletes: 313 obligations →
304 delivered + 9 cancelled by a delete, 126 duplicates blocked, 0 duplicate DMs
delivered. `kill -9` mid-drain: `sent + queued` conserved exactly (20 → 20).

*(local)* Idempotency under ambiguous failure — the clone creates the DM, then
returns a 500, so the caller cannot know whether it landed: 9 such failures → the
key returned the original `dm_id` 9 times → **0 duplicates**.

---

## Failures that will bite in production

### 1. A resend after a `failed` status can deliver a second DM

When `GET /v1/dm/{id}` reports `failed`, I resend under a **new** idempotency key.
I have to: the original key is bound to the dead `dm_id` and would return the same
failed record forever.

If that `failed` status was itself wrong — or the DM was delivered and only the
status record failed — the person gets the message twice.

**Trigger:** any DM reported `failed` after acceptance, or stuck `queued` upstream
past 180s. **Frequency:** *(local)* 54 resends across 313 DMs — 46 of 313 needed
at least one, matching the clone's 15% silent-failure rate. So roughly **one DM in
seven** takes this path. **Cap:** 3 resends, so worst case is 4 copies.

I chose a rare duplicate over a silent loss. Right for a price list, wrong for
anything transactional.

### 2. A crash between the send and the `dm_id` write depends on their cache TTL

Sequence: mark `in_flight` → POST → write `dm_id`. Die in the middle and the task
is recovered as `queued` and resent under the *same* key, so PseudoGram returns
the original `dm_id` and nothing duplicates.

**That only holds while their idempotency cache still has the key.** I don't know
its TTL. If it expires during my downtime, the DM goes out twice and neither
ledger shows a duplicate — it looks like one send from each of two attempts.

### 3. A `comment.deleted` arriving mid-send is ignored

Deletes cancel tasks in state `queued` only. A claimed task is `in_flight` for the
duration of one HTTP request, and a delete landing in that window doesn't stop it.

I shrank the window by reserving the rate-limit slot *before* claiming the task,
so a task stays cancellable through the whole 6.1s pacing wait and is `in_flight`
only for the request itself — roughly 50–300ms. *(local)* not observed in 567
deliveries, but the window is real.

### 4. Deleting the *first* comment cancels a DM a later comment still justifies

A task records the `comment_id` that created it. Comment "PRICE" twice, delete
only the first, and the pending DM is cancelled even though the second comment is
live and still matches. That person gets nothing.

Tracking "does any live comment still justify this obligation" means re-deriving
the match set on every delete. I judged the wrong-direction case rarer than the
right one and took the simpler rule.

### 5. Terminal failures are terminal

After 6 attempts or 3 resends a DM is `failed` and nothing retries it. If
PseudoGram is down for ten minutes, everything that exhausts its attempts in that
window is permanently lost. `/stats` reports them honestly as `failed` forever.
There is no requeue endpoint — first thing I'd add.

### 6. Rules created after an event arrives never apply to it

Events are matched once per delivery, then marked processed. Create a rule at
10:05 and the comment from 10:04 is never re-evaluated. No backfill. Correct for a
live system, wrong for anyone who configures rules after pointing the webhook at
us.

### 7. A correct matcher with the wrong keyword looks identical to a broken one

*(live)* An audit reported 96 expected recipients against 91 DMed. All five had
commented **"pricing please"** while the rule keyword was `PRICE` — and `price` is
not a substring of `pricing`. The matcher was correct to the character; the rule
was too narrow.

Every internal number was self-consistent while this was happening. **A system
that never matches an event cannot tell you it should have.** Only comparing
against their expected recipient set exposed it.

The deployed rule is now the stem `pric`, which matches all 96 with no false
positives. It is still tuned to observed traffic: "priceless" would match and get
a price list. Keyword quality is a product decision the system cannot validate.

---

## Reporting: where the numbers can be wrong

### 8. `duplicates_blocked` is my definition, which may not be theirs

I count one per match evaluation that did not create a new obligation — covering
both redelivered events and repeat commenters. A duplicate event matching *no*
rule isn't counted, because no DM was ever going to be sent.

If their truth counts every redelivered event regardless of whether it matched, my
number is lower than theirs.

### 9. `sent` lags reality, always downward

A DM they've already delivered counts as `queued` until the reconciler polls it
(every 5s, 40 at a time). Under a large backlog that lag grows — 300 accepted DMs
take ~40s to sweep. It understates, never overstates, which is the direction I
want to be wrong in.

### 10. The live API's contract differs from its documentation

| Documented | Actual |
|---|---|
| `202` on success | `200` |
| `400` for a malformed payload | `422` |
| `{"error": ...}` | `{"detail": ...}` |

The second was a live bug: I treated only `400` as terminal, so validation errors
were retried six times each, burning six rate-limit slots per bad DM on something
that could never succeed. Now any 4xx except 408/425 is terminal.

**The real risk is what I didn't probe.** Three of three documented behaviours
were wrong, and anything I haven't tested is handled per a document with that
track record.

### 11. Signature verification fails silently and looks like success

The brief says the HMAC secret is your API key. It isn't — it's the base64 half of
the key, decoded, which is the account email. Implemented as documented, it
rejected **29 of 29** live webhooks with 401.

The failure shape is the dangerous part: every event is rejected before reaching
the ledger, so `/stats` reports four honest zeroes for a system receiving nothing.
Nothing distinguishes that from being idle. **Skipping signature verification
entirely passes by accident; implementing it correctly scores zero.**

Fixed by accepting any secret derivable from our own key, and `/stats?verbose=1`
now reports `REJECTING_ALL_TRAFFIC` when requests are rejected and none accepted.

**Still open:** the alarm catches total rejection, not partial — one request
getting through silences it. And a system rejecting 100% of events while nobody is
sending any looks healthy, and always will.

### 12. A rotated API key silently discards everything

The key is the HMAC secret. Rotate it and every signature fails, `/webhook`
returns 401, and PseudoGram records a delivered-and-rejected response. Those events
are never retried and never appear in my ledger. My numbers would look perfect
while I received nothing — same shape as #11.

---

## Infrastructure and operational

### 13. Two processes could breach the rate limit — and did

*(live)* Two real `429`s, logged as `429 with 10 sends in last 60s` against a
governor that caps at 9. Cause: **Render starts the replacement instance before
stopping the old one**, so every deploy briefly runs two sender loops. Counting
and then sending isn't atomic, so both passed the check in the same instant.

I had documented this failure and then walked into it, having filed it under "only
if someone scales up" without noticing a rolling deploy is exactly that for thirty
seconds.

**Fixed:** `sender.reserve_slot()` counts the window and writes the row claiming
the slot inside one transaction holding `pg_advisory_xact_lock`. Verified with 16
concurrent threads against real Postgres. *(live)* a later deploy mid-drain
produced **0** 429s.

**Still true:** the 6.1s pacing clock is per-process and in memory. Two instances
can no longer exceed the window but will bunch sends toward the start of it.
Pacing is now a politeness optimisation, not the safety mechanism.

### 14. I destroyed the production ledger with my own test suite

Mid-drain, at 87 of 97 DMs delivered, I ran the suite with `TEST_DATABASE_URL` set
to the production connection string. Every test truncates every table. The entire
run's ledger was gone in about a second and `/stats` went from `{sent: 87}` to four
zeroes.

Nothing about that was the system failing. **The ledger is only as safe as the
least careful command run against it.** During grading it would have zeroed the
submission, and `/stats` would have looked perfectly healthy doing it.

**Fixed:** `tests/conftest.py` refuses to start if the test database name matches
the deployed one; test runs use a separate database.

**Still wrong:** the guard compares database *names*. Point it at a different
project with a differently-named database and it will happily truncate that. No
backups, no undo. A real system wouldn't have production credentials in a
developer shell at all.

### 15. The test suite and production run on different databases

68 tests pass on SQLite; production is Postgres. The port was small — placeholders,
auto-increment, float type — and every load-bearing query (`ON CONFLICT DO NOTHING`,
`UPDATE ... RETURNING`, `rowcount`) is common to both. I run the full suite against
real Postgres before deploying, which closes most of it.

What it doesn't close: the tests exercise one writer, and Postgres under genuine
concurrency has isolation semantics SQLite doesn't have.

### 16. Free-tier idle suspension, and the workaround's limits

Render stops a free service after ~15 minutes without inbound HTTP. Background work
doesn't count, so a 30-minute drain — which by definition receives no webhooks
while it drains — would be suspended halfway.

`app/keepalive.py` calls our own `/health` every 10 minutes so the request
re-enters through the router as inbound traffic.

- **It keeps the service awake; it cannot wake it.** Once suspended, the keep-alive
  loop is suspended too.
- A cold start is ~50s. Any webhook arriving in that window is lost before reaching
  the ledger, so nothing in `/stats` knows it existed.

### 17. One Postgres connection, one database, no backups

The connection pool is deliberately a pool of one, because the outbox claim and the
governor are only correct with a single writer. Neon drops idle connections and a
6.1s send cadence has plenty of idle; `_PGConn` reconnects once on
`OperationalError`/`InterfaceError`. If a drop happens mid-transaction that
transaction is lost and retried by the loop that owned it, which is safe because
every write is idempotent or replay-guarded.

The whole ledger is one Neon project on the free tier. If it's lost, every pending
DM and every stat goes with it.

### 18. `/rules` has no authentication

Anyone who finds the URL can create rules that DM strangers using this API key. The
demo and simulation endpoints are token-gated and return 404 when `DEMO_TOKEN` is
unset, but `/rules` must stay open for the grading script. Considered omission for
an unlisted URL; a hole in production.

### 19. Nothing is ever pruned

`events`, `dm_events`, `match_decisions`, `send_log` and `webhook_timing` grow
forever. At this scale that's a few MB. At "millions a month" the governor's
`COUNT(*) WHERE ts > now-60` over an unbounded `send_log` degrades and the database
fills. No retention policy.

---

## Three times a green number meant the ruler was broken

Worth separating, because in each case the *measurement* was wrong and it looked
exactly like a working system — or a broken one.

1. **The duplicate-DM check grouped by recipient alone**, so two users who tripped
   two different rules were reported as double-DMed. They weren't; one DM per rule
   per person is the guarantee. The unit is `(recipient, message)`.

2. **The audit compared one run's truth against the entire ledger**, reporting all
   146 earlier recipients as DMs the truth didn't justify. Now scoped to work
   created after the run started.

3. **The idempotency test never exercised idempotency.** The clone returned every
   500 *before* creating the DM — the one case where retrying is trivially safe —
   so across 433 send requests the key deduplicated exactly 0 of them, and I read
   that zero as good news. The clone now creates the DM and *then* fails.

Every real bug on this project was found by comparing against PseudoGram's truth,
never by my own tests. My tests were green throughout.

---

## What none of this proves

- **PseudoGram's idempotency cache TTL** (#2). My clone never expires keys, so the
  measured result proves my logic, not their behaviour.
- **That their definition of a blocked duplicate matches mine** (#8).
- **Anything about their API I didn't think to probe** (#10).
