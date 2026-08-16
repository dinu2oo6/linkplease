"""Part A (no DM silently lost) + Part C (reconciliation) + the rate governor."""
import time

from app import config, db, matcher, reconciler, sender, stats, webhook
from tests.conftest import FakeClient, FakeResponse, make_event


def one_queued_task(text="PRICE", user_id="usr_1", comment_id=None) -> dict:
    matcher.create_rule("PRICE", "here is the list")
    webhook.ingest(make_event(text=text, user_id=user_id, comment_id=comment_id))
    matcher.process_pending()
    return dict(db.query_one("SELECT * FROM dm_tasks"))


def task_row() -> dict:
    return dict(db.query_one("SELECT * FROM dm_tasks"))


# --- happy path -------------------------------------------------------------

async def test_202_moves_task_to_accepted_not_sent(fake_client):
    """A 202 is an acceptance. It must not count as `sent`."""
    fc = fake_client(FakeClient([FakeResponse(202, {"dm_id": "dm_1", "status": "queued"})]))
    task = one_queued_task()
    await sender.send_one(task)

    row = task_row()
    assert row["state"] == db.ACCEPTED
    assert row["dm_id"] == "dm_1"
    assert stats.core_stats() == {"sent": 0, "failed": 0, "queued": 1,
                                  "duplicates_blocked": 0}
    assert fc.posts[0]["json"] == {
        "recipient_user_id": "usr_1", "message": "here is the list",
        "comment_id": row["comment_id"],
    }


async def test_send_carries_an_idempotency_key_equal_to_the_dedupe_key(fake_client):
    fc = fake_client(FakeClient())
    task = one_queued_task()
    await sender.send_one(task)
    assert fc.posts[0]["headers"]["Idempotency-Key"] == task["dedupe_key"]


# --- failures ---------------------------------------------------------------

async def test_400_is_terminal_and_never_retried(fake_client):
    """Retrying a malformed payload burns rate-limit slots that other DMs need."""
    fc = fake_client(FakeClient([FakeResponse(400, {"error": "invalid_request"})]))
    task = one_queued_task()
    await sender.send_one(task)

    assert task_row()["state"] == db.FAILED
    assert stats.core_stats()["failed"] == 1
    assert not sender.has_due_task()
    assert len(fc.posts) == 1


async def test_422_is_terminal_too_because_the_live_api_uses_it_not_400(fake_client):
    """The docs promise 400 for a malformed payload. The real API sends 422.

    Probing the live API found this. A hardcoded `== 400` retried every
    validation error six times, wasting six rate-limit slots per bad DM.
    """
    fc = fake_client(FakeClient([FakeResponse(422, {"detail": [{"msg": "Field required"}]})]))
    task = one_queued_task()
    await sender.send_one(task)

    assert task_row()["state"] == db.FAILED
    assert len(fc.posts) == 1


async def test_unknown_4xx_is_terminal_but_408_and_425_are_retried(fake_client):
    """Client errors can't be fixed by repetition; timing errors can."""
    for code, expected in [(403, db.FAILED), (404, db.FAILED), (413, db.FAILED),
                           (408, db.QUEUED), (425, db.QUEUED)]:
        db.reset_for_tests()
        db.connect()
        fake_client(FakeClient([FakeResponse(code, {})]))
        await sender.send_one(one_queued_task())
        assert task_row()["state"] == expected, f"http {code}"


async def test_500_is_retried_with_backoff_and_stays_queued(fake_client):
    fake_client(FakeClient([FakeResponse(500, {"error": "internal_error"})]))
    task = one_queued_task()
    await sender.send_one(task)

    row = task_row()
    assert row["state"] == db.QUEUED
    assert row["attempts"] == 1
    assert row["next_attempt_at"] > time.time()
    assert stats.core_stats()["queued"] == 1   # still owed, never lost


async def test_repeated_500s_eventually_give_up_exactly_once(fake_client):
    fake_client(FakeClient([FakeResponse(500, {}) for _ in range(20)]))
    one_queued_task()
    for _ in range(config.MAX_SEND_ATTEMPTS):
        row = task_row()
        row["next_attempt_at"] = 0
        await sender.send_one(row)

    assert task_row()["state"] == db.FAILED
    assert stats.core_stats() == {"sent": 0, "failed": 1, "queued": 0,
                                  "duplicates_blocked": 0}


async def test_transport_error_retries_under_the_same_idempotency_key(fake_client):
    """We don't know if it arrived. The key makes finding out unnecessary."""
    fc = fake_client(FakeClient([
        TimeoutError("connection reset"),
        FakeResponse(202, {"dm_id": "dm_original"}),
    ]))
    task = one_queued_task()
    await sender.send_one(task)
    assert task_row()["state"] == db.QUEUED

    await sender.send_one(task_row())
    assert task_row()["dm_id"] == "dm_original"
    assert fc.posts[0]["headers"]["Idempotency-Key"] == \
           fc.posts[1]["headers"]["Idempotency-Key"]


async def test_a_500_that_secretly_created_the_dm_does_not_double_send(fake_client):
    """The ambiguous failure: PseudoGram made the DM, then the response died.

    We cannot tell this apart from a 500 that created nothing, and we must not
    have to. Retrying under the same key returns the original dm_id, so the
    human gets exactly one message either way.
    """
    fc = fake_client(FakeClient([
        FakeResponse(500, {"error": "internal_error"}),   # DM was created anyway
        FakeResponse(202, {"dm_id": "dm_created_before_the_500"}),
    ]))
    task = one_queued_task()
    await sender.send_one(task)
    assert task_row()["state"] == db.QUEUED

    await sender.send_one(task_row())
    row = task_row()
    assert row["dm_id"] == "dm_created_before_the_500"
    assert row["state"] == db.ACCEPTED
    assert fc.posts[0]["headers"]["Idempotency-Key"] == \
           fc.posts[1]["headers"]["Idempotency-Key"] == task["dedupe_key"]
    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 1


async def test_429_does_not_consume_an_attempt_and_is_flagged(fake_client):
    """The governor should make this impossible, so we record it loudly."""
    fake_client(FakeClient([FakeResponse(429, {"error": "rate_limited"},
                                         headers={"Retry-After": "30"})]))
    task = one_queued_task()
    await sender.send_one(task)

    row = task_row()
    assert row["state"] == db.QUEUED
    assert row["attempts"] == 0                       # not the DM's fault
    assert row["next_attempt_at"] >= time.time() + 29  # Retry-After honoured
    assert db.scalar("SELECT COUNT(*) FROM invariants WHERE kind='rate_limited'") == 1


async def test_202_without_a_dm_id_is_retried_not_counted(fake_client):
    fake_client(FakeClient([FakeResponse(202, {"status": "queued"})]))
    task = one_queued_task()
    await sender.send_one(task)
    assert task_row()["state"] == db.QUEUED
    assert stats.core_stats()["sent"] == 0


# --- crash recovery ---------------------------------------------------------

async def test_in_flight_tasks_are_recovered_after_a_crash(fake_client):
    fake_client(FakeClient())
    one_queued_task()
    sender.claim_next()                       # now in_flight
    assert task_row()["state"] == db.IN_FLIGHT

    assert sender.recover_in_flight() == 1    # simulate process restart
    row = task_row()
    assert row["state"] == db.QUEUED
    assert row["idempotency_key"] == row["dedupe_key"]   # key survives, so a
                                                         # resend cannot double
    assert stats.core_stats()["queued"] == 1


def test_claiming_is_exclusive():
    one_queued_task()
    assert sender.claim_next() is not None
    assert sender.claim_next() is None        # nobody else can take it


def test_a_task_waiting_on_backoff_is_not_due_yet():
    one_queued_task()
    with db.tx() as conn:
        conn.execute("UPDATE dm_tasks SET next_attempt_at = ?", (time.time() + 60,))
    assert not sender.has_due_task()
    assert sender.claim_next() is None
    assert stats.core_stats()["queued"] == 1   # still counted as owed


# --- rate governor ----------------------------------------------------------

def test_governor_reports_no_wait_when_the_window_is_empty():
    assert sender._window_wait(time.time()) == 0.0


def test_governor_blocks_before_reaching_the_real_ceiling():
    """We stop at 9 sends per 60s, not 10, so clock skew can't breach it."""
    now = time.time()
    ceiling = config.RATE_LIMIT_MAX - config.RATE_LIMIT_HEADROOM
    with db.tx() as conn:
        for i in range(ceiling - 1):
            conn.execute("INSERT INTO send_log (ts) VALUES (?)", (now - i,))
    assert sender._window_wait(now) == 0.0     # 8 in the window: room for one

    with db.tx() as conn:
        conn.execute("INSERT INTO send_log (ts) VALUES (?)", (now,))
    assert sender._window_wait(now) > 0        # 9 in the window: stop


def test_governor_waits_exactly_until_the_oldest_send_ages_out():
    now = time.time()
    ceiling = config.RATE_LIMIT_MAX - config.RATE_LIMIT_HEADROOM
    oldest = now - 50
    with db.tx() as conn:
        conn.execute("INSERT INTO send_log (ts) VALUES (?)", (oldest,))
        for i in range(ceiling - 1):
            conn.execute("INSERT INTO send_log (ts) VALUES (?)", (now - i * 0.1,))
    wait = sender._window_wait(now)
    assert 9.9 < wait < 10.2                   # 60 - 50, plus a hair


def test_pacing_survives_a_restart(monkeypatch):
    """A reboot must not let us burst past the limit."""
    now = time.time()
    with db.tx() as conn:
        conn.execute("INSERT INTO send_log (ts) VALUES (?)", (now,))
    sender._last_send_at = 0.0                 # fresh process
    sender.prime_pacing()
    assert sender._last_send_at == now


async def test_governor_never_exceeds_the_limit_over_a_simulated_run(fake_client):
    """Drive 40 sends through the real governor on a compressed clock."""
    fake_client(FakeClient())
    matcher.create_rule("PRICE", "list")
    for i in range(40):
        webhook.ingest(make_event(text="PRICE", user_id=f"usr_{i}"))
    matcher.process_pending()

    # 1s window / 10 max keeps the test fast without changing the logic.
    monkey_window, monkey_interval = 1.0, 0.101
    config.RATE_LIMIT_WINDOW = monkey_window
    config.SEND_INTERVAL_SECONDS = monkey_interval
    try:
        import asyncio
        stop = asyncio.Event()
        task = asyncio.create_task(sender.sender_loop(stop))
        await asyncio.sleep(2.0)
        stop.set()
        await asyncio.wait_for(task, timeout=5)

        times = sorted(r["ts"] for r in db.query("SELECT ts FROM send_log"))
        assert len(times) >= 5, "the loop should have made progress"
        worst = max(
            sum(1 for t in times if start <= t < start + monkey_window)
            for start in times
        )
        assert worst <= config.RATE_LIMIT_MAX - config.RATE_LIMIT_HEADROOM
        assert db.scalar("SELECT COUNT(*) FROM invariants WHERE kind='rate_limited'") == 0
    finally:
        config.RATE_LIMIT_WINDOW = 60.0
        config.SEND_INTERVAL_SECONDS = 6.1


# --- Part C: reconciliation -------------------------------------------------

async def accept_one(fake_client, dm_id="dm_1"):
    fc = fake_client(FakeClient([FakeResponse(202, {"dm_id": dm_id})]))
    await sender.send_one(one_queued_task())
    return fc


async def test_delivered_status_is_what_finally_counts_as_sent(fake_client):
    await accept_one(fake_client)
    fake_client(FakeClient(get_responses={"dm_1": FakeResponse(200, {"status": "delivered"})}))
    await reconciler.reconcile_once()

    assert task_row()["state"] == db.DELIVERED
    assert stats.core_stats() == {"sent": 1, "failed": 0, "queued": 0,
                                  "duplicates_blocked": 0}


async def test_a_dm_that_fails_after_acceptance_is_resent_with_a_fresh_key(fake_client):
    """The ~15% silent failure. The original key is bound to the dead dm_id."""
    await accept_one(fake_client)
    fake_client(FakeClient(get_responses={"dm_1": FakeResponse(200, {"status": "failed"})}))
    await reconciler.reconcile_once()

    row = task_row()
    assert row["state"] == db.QUEUED
    assert row["resend_count"] == 1
    assert row["idempotency_key"] == f"{row['dedupe_key']}:r1"
    assert row["dm_id"] is None
    assert stats.core_stats()["queued"] == 1     # still owed, not lost


async def test_still_queued_upstream_is_left_alone_until_it_looks_stuck(fake_client):
    await accept_one(fake_client)
    fake_client(FakeClient(get_responses={"dm_1": FakeResponse(200, {"status": "queued"})}))
    await reconciler.reconcile_once()

    row = task_row()
    assert row["state"] == db.ACCEPTED
    assert row["last_checked_at"] > 0


async def test_a_dm_stuck_upstream_forever_is_eventually_resent(fake_client):
    await accept_one(fake_client)
    with db.tx() as conn:
        conn.execute("UPDATE dm_tasks SET accepted_at = ?",
                     (time.time() - config.RECONCILE_STUCK_SECONDS - 1,))
    fake_client(FakeClient(get_responses={"dm_1": FakeResponse(200, {"status": "queued"})}))
    await reconciler.reconcile_once()

    assert task_row()["state"] == db.QUEUED
    assert task_row()["resend_count"] == 1


async def test_resends_are_capped_then_reported_as_failed(fake_client):
    await accept_one(fake_client)
    with db.tx() as conn:
        conn.execute("UPDATE dm_tasks SET resend_count = ?", (config.MAX_RESENDS,))
    fake_client(FakeClient(get_responses={"dm_1": FakeResponse(200, {"status": "failed"})}))
    await reconciler.reconcile_once()

    assert task_row()["state"] == db.FAILED
    assert stats.core_stats() == {"sent": 0, "failed": 1, "queued": 0,
                                  "duplicates_blocked": 0}


async def test_a_failed_status_read_changes_nothing(fake_client):
    """Not knowing is not the same as knowing it failed."""
    await accept_one(fake_client)

    class Broken(FakeClient):
        async def get(self, url):
            raise ConnectionError("upstream down")

    fake_client(Broken())
    await reconciler.reconcile_once()

    assert task_row()["state"] == db.ACCEPTED
    assert stats.core_stats()["queued"] == 1


# --- ledger arithmetic ------------------------------------------------------

def test_every_task_lands_in_exactly_one_headline_bucket():
    matcher.create_rule("PRICE", "list")
    for i in range(6):
        webhook.ingest(make_event(text="PRICE", user_id=f"usr_{i}"))
    matcher.process_pending()
    with db.tx() as conn:
        for i, state in enumerate([db.DELIVERED, db.DELIVERED, db.FAILED,
                                   db.QUEUED, db.ACCEPTED, db.CANCELLED]):
            conn.execute("UPDATE dm_tasks SET state = ? WHERE user_id = ?",
                         (state, f"usr_{i}"))

    core = stats.core_stats()
    assert core["sent"] == 2 and core["failed"] == 1 and core["queued"] == 2
    total = db.scalar("SELECT COUNT(*) FROM dm_tasks")
    cancelled = stats.verbose_stats()["detail"]["cancelled_by_delete"]
    assert core["sent"] + core["failed"] + core["queued"] + cancelled == total
