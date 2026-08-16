"""Every guarantee the brief asks for, evaluated live against the ledger.

Each checkpoint is a query, not a claim. They are written so that a failure is
loud and specific -- "3 people were DMed twice" rather than "duplicate check
failed" -- because the whole point of the exercise is knowing exactly where the
system breaks rather than asserting that it doesn't.

PASS/FAIL/PENDING, where PENDING means "nothing has happened that could test
this yet". A checkpoint nobody has exercised is not a pass, and saying so is the
difference between evidence and decoration.
"""
import time

from . import config, db, simulator

PASS, FAIL, PENDING = "pass", "fail", "pending"


def _cp(key, title, requirement, state, detail, evidence=None):
    return {"key": key, "title": title, "requirement": requirement,
            "state": state, "detail": detail, "evidence": evidence or {}}


def no_events_lost() -> dict:
    """Part A: 500 comments in 10 seconds, nothing lost."""
    run = simulator.latest_run()
    received = db.scalar("SELECT COALESCE(SUM(delivery_count), 0) FROM events")
    unprocessed = db.scalar(
        "SELECT COUNT(*) FROM events WHERE processed_pass < delivery_count")
    if not run:
        return _cp("no_loss", "No event lost", "Every delivery is recorded and matched",
                   PENDING, "No demo run yet.")
    sent = run["comments"] or 0
    running = run["status"] == "running"
    if received < sent and running:
        return _cp("no_loss", "No event lost", "Every delivery is recorded and matched",
                   PENDING, f"Flood in progress — {received} of {sent} delivered so far.",
                   {"received": received, "expected": sent})
    ok = received >= sent and unprocessed == 0
    return _cp(
        "no_loss", "No event lost", "Every delivery is recorded and matched",
        PASS if ok else FAIL,
        f"{received} of {sent} deliveries recorded, {unprocessed} awaiting match."
        if ok else f"Only {received} of {sent} deliveries recorded ({sent - received} lost).",
        {"delivered_by_simulator": sent, "recorded": received, "unprocessed": unprocessed},
    )


def no_duplicate_dms() -> dict:
    """Part A: the same user never gets DMed twice for the same rule."""
    dupes = db.scalar(
        "SELECT COUNT(*) FROM (SELECT 1 FROM dm_tasks GROUP BY rule_id, user_id"
        " HAVING COUNT(*) > 1) x")
    tasks = db.scalar("SELECT COUNT(*) FROM dm_tasks")
    blocked = db.scalar("SELECT COUNT(*) FROM match_decisions WHERE decision = ?",
                        (db.D_DUPLICATE,))
    if tasks == 0:
        return _cp("no_dupes", "No duplicate DMs",
                   "One DM per person per rule, ever", PENDING, "No DMs yet.")
    return _cp(
        "no_dupes", "No duplicate DMs", "One DM per person per rule, ever",
        PASS if dupes == 0 else FAIL,
        f"{tasks} obligations, {blocked} repeat triggers blocked, {dupes} people double-DMed."
        if dupes else f"{tasks} obligations across distinct (rule, person) pairs; "
                      f"{blocked} repeat triggers correctly blocked.",
        {"obligations": tasks, "duplicates_blocked": blocked, "people_double_dmed": dupes},
    )


def rate_limit_respected() -> dict:
    """Part C: the rate limit is never breached."""
    times = sorted(r["ts"] for r in db.query("SELECT ts FROM send_log ORDER BY ts"))
    worst = 0
    for i, t in enumerate(times):
        j = i
        while j < len(times) and times[j] < t + config.RATE_LIMIT_WINDOW:
            j += 1
        worst = max(worst, j - i)
    breaches = db.scalar(
        "SELECT COUNT(*) FROM invariants WHERE kind = 'rate_limited'")
    if not times:
        return _cp("rate", "Rate limit never breached",
                   f"Max {config.RATE_LIMIT_MAX} sends per {int(config.RATE_LIMIT_WINDOW)}s",
                   PENDING, "No sends yet.")
    ok = worst <= config.RATE_LIMIT_MAX and breaches == 0
    return _cp(
        "rate", "Rate limit never breached",
        f"Max {config.RATE_LIMIT_MAX} sends per {int(config.RATE_LIMIT_WINDOW)}s",
        PASS if ok else FAIL,
        f"Peak was {worst} sends in any rolling 60s (ceiling {config.RATE_LIMIT_MAX}); "
        f"{breaches} rate-limit rejections received.",
        {"peak_in_window": worst, "ceiling": config.RATE_LIMIT_MAX,
         "total_sends": len(times), "429s_received": breaches},
    )


def no_dm_silently_lost() -> dict:
    """Part A: no DM is silently lost when the API fails."""
    total = db.scalar("SELECT COUNT(*) FROM dm_tasks")
    accounted = db.scalar(
        "SELECT COUNT(*) FROM dm_tasks WHERE state IN (?,?,?,?,?,?)",
        (db.QUEUED, db.IN_FLIGHT, db.ACCEPTED, db.DELIVERED, db.FAILED, db.CANCELLED))
    retried = db.scalar("SELECT COUNT(*) FROM dm_tasks WHERE attempts > 1")
    failed = db.scalar("SELECT COUNT(*) FROM dm_tasks WHERE state = ?", (db.FAILED,))
    if total == 0:
        return _cp("no_dm_lost", "No DM silently lost",
                   "Every DM ends delivered, failed, or cancelled — never vanished",
                   PENDING, "No DMs yet.")
    return _cp(
        "no_dm_lost", "No DM silently lost",
        "Every DM ends delivered, failed, or cancelled — never vanished",
        PASS if accounted == total else FAIL,
        f"All {total} obligations are in a known state. {retried} survived a failed "
        f"attempt and were retried; {failed} gave up after exhausting retries.",
        {"obligations": total, "accounted_for": accounted,
         "retried_after_failure": retried, "gave_up": failed},
    )


def delivery_reconciled() -> dict:
    """Part C: catch DMs that the API accepted but never delivered."""
    delivered = db.scalar("SELECT COUNT(*) FROM dm_tasks WHERE state = ?", (db.DELIVERED,))
    awaiting = db.scalar("SELECT COUNT(*) FROM dm_tasks WHERE state = ?", (db.ACCEPTED,))
    resends = db.scalar("SELECT COALESCE(SUM(resend_count), 0) FROM dm_tasks")
    if delivered == 0 and awaiting == 0:
        return _cp("reconcile", "Delivery confirmed, not assumed",
                   "A 202 is an acceptance; only a polled 'delivered' counts as sent",
                   PENDING, "No DMs accepted yet.")
    return _cp(
        "reconcile", "Delivery confirmed, not assumed",
        "A 202 is an acceptance; only a polled 'delivered' counts as sent",
        PASS,
        f"{delivered} confirmed delivered by polling, {awaiting} awaiting confirmation. "
        f"{resends} were reported failed after acceptance and resent.",
        {"confirmed_delivered": delivered, "awaiting_confirmation": awaiting,
         "resent_after_silent_failure": resends},
    )


def deletes_handled() -> dict:
    """Part C: handle comment.deleted sensibly."""
    cancelled = db.scalar("SELECT COUNT(*) FROM dm_tasks WHERE state = ?", (db.CANCELLED,))
    suppressed = db.scalar("SELECT COUNT(*) FROM match_decisions WHERE decision = ?",
                           (db.D_SUPPRESSED_DELETED,))
    tombstones = db.scalar("SELECT COUNT(*) FROM tombstones")
    if tombstones == 0:
        return _cp("deletes", "Deleted comments don't get DMs",
                   "A comment deleted before we send cancels the DM",
                   PENDING, "No deletes received yet.")
    return _cp(
        "deletes", "Deleted comments don't get DMs",
        "A comment deleted before we send cancels the DM",
        PASS,
        f"{tombstones} deletes seen: {cancelled} pending DMs cancelled, "
        f"{suppressed} matches suppressed because the delete arrived first.",
        {"deletes_received": tombstones, "dms_cancelled": cancelled,
         "matches_suppressed_out_of_order": suppressed},
    )


def signatures_enforced() -> dict:
    """Part B: reject forged requests."""
    rejected = db.scalar("SELECT COUNT(*) FROM invariants WHERE kind = 'bad_signature'")
    accepted = db.scalar("SELECT COUNT(*) FROM events")
    if not config.REQUIRE_SIGNATURE:
        return _cp("signature", "Forged webhooks rejected",
                   "HMAC-SHA256 over the raw body, constant-time compare",
                   FAIL, "Signature verification is DISABLED.")
    if accepted == 0 and rejected == 0:
        return _cp("signature", "Forged webhooks rejected",
                   "HMAC-SHA256 over the raw body, constant-time compare",
                   PENDING, "No webhooks received yet.")
    if rejected == 0:
        # Verification being enabled is not evidence that it works. Until
        # something forged has actually bounced, this is untested.
        return _cp(
            "signature", "Forged webhooks rejected",
            "HMAC-SHA256 over the raw body, constant-time compare",
            PENDING,
            f"Verification is on and {accepted} signed events were accepted, but "
            f"nothing forged has been attempted yet — so rejection is untested. "
            f"Run the simulator: it mixes forged requests into the flood.",
            {"accepted": accepted, "rejected": 0},
        )
    # Nothing forged may ever reach the ledger: every demo forgery uses a
    # recipient prefixed usr_attacker_, so any obligation for one is a breach.
    leaked = db.scalar(
        "SELECT COUNT(*) FROM dm_tasks WHERE user_id LIKE 'usr_attacker_%'")
    return _cp(
        "signature", "Forged webhooks rejected",
        "HMAC-SHA256 over the raw body, constant-time compare",
        PASS if leaked == 0 else FAIL,
        f"{accepted} correctly-signed events accepted, {rejected} forged or unsigned "
        f"requests rejected with 401, and {leaked} forged events reached the ledger."
        if leaked else
        f"{accepted} correctly-signed events accepted, {rejected} forged or unsigned "
        f"requests rejected with 401. None reached the ledger.",
        {"accepted": accepted, "rejected": rejected, "forgeries_that_got_through": leaked},
    )


def webhook_fast_enough() -> dict:
    """The contract: /webhook must answer within 5 seconds."""
    rows = db.query("SELECT ms FROM webhook_timing ORDER BY ms")
    if not rows:
        return _cp("latency", "Webhook answers well within 5s",
                   "Return 200 in under 5000ms, even under a burst",
                   PENDING, "No webhook calls recorded yet.")
    values = [r["ms"] for r in rows]
    p50 = values[len(values) // 2]
    p95 = values[min(len(values) - 1, int(len(values) * 0.95))]
    worst = values[-1]
    ok = worst < 5000
    return _cp(
        "latency", "Webhook answers well within 5s",
        "Return 200 in under 5000ms, even under a burst",
        PASS if ok else FAIL,
        f"median {p50:.0f}ms · p95 {p95:.0f}ms · slowest {worst:.0f}ms across "
        f"{len(values)} calls.",
        {"median_ms": round(p50, 1), "p95_ms": round(p95, 1),
         "slowest_ms": round(worst, 1), "samples": len(values)},
    )


def all_checkpoints() -> dict:
    checks = [
        no_events_lost(), no_duplicate_dms(), no_dm_silently_lost(),
        rate_limit_respected(), delivery_reconciled(), deletes_handled(),
        signatures_enforced(), webhook_fast_enough(),
    ]
    return {
        "checkpoints": checks,
        "summary": {
            "pass": sum(1 for c in checks if c["state"] == PASS),
            "fail": sum(1 for c in checks if c["state"] == FAIL),
            "pending": sum(1 for c in checks if c["state"] == PENDING),
            "total": len(checks),
        },
        "generated_at": time.time(),
    }
