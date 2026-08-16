"""Part A + Part B: ingest, signature, matching, dedupe."""
import concurrent.futures
import json

from fastapi.testclient import TestClient

from app import config, db, matcher, stats, webhook
from tests.conftest import make_event, raw_of, sign


def client():
    from app.main import app
    return TestClient(app)


# --- Part B: signatures -----------------------------------------------------

def test_valid_signature_is_accepted():
    event = make_event()
    raw = raw_of(event)
    assert webhook.verify_signature(raw, sign(raw))


def test_forged_signature_is_rejected():
    raw = raw_of(make_event())
    assert not webhook.verify_signature(raw, "sha256=" + "0" * 64)


def test_missing_signature_is_rejected():
    raw = raw_of(make_event())
    assert not webhook.verify_signature(raw, None)


def test_signature_of_a_different_body_is_rejected():
    """The signature must cover *this* body, not merely be a valid signature."""
    other = raw_of(make_event(text="LINK"))
    raw = raw_of(make_event(text="PRICE"))
    assert not webhook.verify_signature(raw, sign(other))


def test_signature_computed_over_raw_bytes_not_reserialised_json():
    """Whitespace the sender used must not change the verdict."""
    event = make_event()
    raw = json.dumps(event, indent=4).encode()  # different bytes, same object
    assert webhook.verify_signature(raw, sign(raw))
    assert not webhook.verify_signature(
        json.dumps(event, separators=(",", ":")).encode(), sign(raw)
    )


def test_webhook_route_rejects_forgery_and_writes_nothing():
    with client() as c:
        resp = c.post("/webhook", content=raw_of(make_event()),
                      headers={"X-PseudoGram-Signature": "sha256=" + "a" * 64})
    assert resp.status_code == 401
    assert db.scalar("SELECT COUNT(*) FROM events") == 0


def test_webhook_route_accepts_signed_event():
    raw = raw_of(make_event())
    with client() as c:
        resp = c.post("/webhook", content=raw,
                      headers={"X-PseudoGram-Signature": sign(raw)})
    assert resp.status_code == 200
    assert resp.json()["status"] == "accepted"


# --- contract ---------------------------------------------------------------

def test_rules_contract_shape():
    with client() as c:
        resp = c.post("/rules", json={"keyword": "PRICE", "dm_message": "list"})
    assert resp.status_code == 201
    body = resp.json()
    assert set(body) == {"rule_id", "keyword", "dm_message"}
    assert isinstance(body["rule_id"], str) and body["rule_id"]
    assert body["keyword"] == "PRICE"


def test_stats_contract_shape():
    with client() as c:
        body = c.get("/stats").json()
    assert set(body) == {"sent", "failed", "queued", "duplicates_blocked"}
    assert all(isinstance(v, int) for v in body.values())


# --- matching ---------------------------------------------------------------

def test_keyword_matches_case_insensitively_anywhere_in_text():
    matcher.create_rule("PRICE", "here you go")
    for text in ["PRICE", "price", "what's the PrIcE?", "hey price pls 🙏",
                 "aPRICEb"]:
        webhook.ingest(make_event(text=text, user_id=f"usr_{text}"))
    matcher.process_pending()
    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 5


def test_non_matching_text_creates_nothing():
    matcher.create_rule("PRICE", "here you go")
    webhook.ingest(make_event(text="great post!"))
    matcher.process_pending()
    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 0


def test_two_rules_both_match_the_same_comment():
    matcher.create_rule("PRICE", "prices")
    matcher.create_rule("LINK", "links")
    webhook.ingest(make_event(text="PRICE and LINK please"))
    matcher.process_pending()
    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 2


def test_identity_is_user_id_not_username():
    """Same human, renamed account, must not get a second DM."""
    matcher.create_rule("PRICE", "list")
    first = make_event(text="PRICE", user_id="usr_7")
    second = make_event(text="PRICE", user_id="usr_7")
    second["data"]["from"]["username"] = "totally.different.handle"
    webhook.ingest(first)
    webhook.ingest(second)
    matcher.process_pending()
    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 1
    assert stats.core_stats()["duplicates_blocked"] == 1


def test_different_users_same_keyword_each_get_one():
    matcher.create_rule("PRICE", "list")
    for i in range(5):
        webhook.ingest(make_event(text="PRICE", user_id=f"usr_{i}"))
    matcher.process_pending()
    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 5
    assert stats.core_stats()["duplicates_blocked"] == 0


def test_same_user_different_rules_gets_both():
    matcher.create_rule("PRICE", "prices")
    matcher.create_rule("LINK", "links")
    webhook.ingest(make_event(text="PRICE", user_id="usr_1"))
    webhook.ingest(make_event(text="LINK", user_id="usr_1"))
    matcher.process_pending()
    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 2


# --- duplicates -------------------------------------------------------------

def test_repeat_commenter_is_blocked_once_per_extra_comment():
    matcher.create_rule("PRICE", "list")
    for _ in range(4):
        webhook.ingest(make_event(text="PRICE", user_id="usr_1"))
    matcher.process_pending()
    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 1
    assert stats.core_stats()["duplicates_blocked"] == 3


def test_redelivered_event_id_is_counted_as_a_blocked_duplicate():
    """The 8% redelivery case. Same event_id five times -> one DM, four blocks."""
    matcher.create_rule("PRICE", "list")
    event = make_event(text="PRICE", user_id="usr_1", event_id="evt_same")
    for _ in range(5):
        webhook.ingest(event)
        matcher.process_pending()
    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 1
    assert stats.core_stats()["duplicates_blocked"] == 4
    assert db.scalar("SELECT delivery_count FROM events WHERE event_id='evt_same'") == 5


def test_redeliveries_arriving_before_matching_are_still_counted():
    """All five land while the matcher is asleep; it must catch up pass by pass."""
    matcher.create_rule("PRICE", "list")
    event = make_event(text="PRICE", user_id="usr_1", event_id="evt_same")
    for _ in range(5):
        webhook.ingest(event)
    for _ in range(6):
        matcher.process_pending()
    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 1
    assert stats.core_stats()["duplicates_blocked"] == 4


def test_matcher_is_idempotent_under_replay():
    """Re-running the matcher must not invent duplicates. Crash-replay safety."""
    matcher.create_rule("PRICE", "list")
    webhook.ingest(make_event(text="PRICE", user_id="usr_1"))
    matcher.process_pending()
    before = stats.core_stats()
    for _ in range(5):
        matcher.process_pending()
    assert stats.core_stats() == before


def test_fifty_concurrent_identical_deliveries_produce_exactly_one_dm():
    """The race the brief calls out by name.

    Fifty threads POST a matching comment for the same user at once. The
    guarantee is a PRIMARY KEY, not a check-then-act, so there is no window in
    which two of them can both decide to send.
    """
    matcher.create_rule("PRICE", "list")
    events = [make_event(text="PRICE please", user_id="usr_hot") for _ in range(50)]

    with client() as c:
        def post(event):
            raw = raw_of(event)
            return c.post("/webhook", content=raw,
                          headers={"X-PseudoGram-Signature": sign(raw)}).status_code

        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as pool:
            codes = list(pool.map(post, events))

    assert set(codes) == {200}
    while db.scalar("SELECT COUNT(*) FROM events WHERE processed_pass < delivery_count"):
        matcher.process_pending()

    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 1
    assert stats.core_stats()["duplicates_blocked"] == 49


# --- ordering ---------------------------------------------------------------

def test_out_of_order_arrival_does_not_affect_the_outcome():
    """sent_at is recorded but never used for a decision."""
    matcher.create_rule("PRICE", "list")
    old = make_event(text="PRICE", user_id="usr_1")
    old["sent_at"] = "2026-08-15T00:00:00Z"
    new = make_event(text="PRICE", user_id="usr_1")
    new["sent_at"] = "2020-01-01T00:00:00Z"   # older stamp, arrives second
    webhook.ingest(old)
    webhook.ingest(new)
    matcher.process_pending()
    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 1


# --- comment.deleted --------------------------------------------------------

def test_delete_before_send_cancels_the_dm():
    matcher.create_rule("PRICE", "list")
    created = make_event(text="PRICE", user_id="usr_1", comment_id="cmt_x")
    webhook.ingest(created)
    matcher.process_pending()
    assert db.query_one("SELECT state FROM dm_tasks")["state"] == db.QUEUED

    webhook.ingest(make_event(comment_id="cmt_x", event_type="comment.deleted"))
    matcher.process_pending()
    assert db.query_one("SELECT state FROM dm_tasks")["state"] == db.CANCELLED
    assert stats.core_stats()["queued"] == 0


def test_delete_arriving_before_create_suppresses_the_dm():
    """Out-of-order delete. We must never create the obligation at all."""
    matcher.create_rule("PRICE", "list")
    webhook.ingest(make_event(comment_id="cmt_y", event_type="comment.deleted"))
    matcher.process_pending()
    webhook.ingest(make_event(text="PRICE", user_id="usr_1", comment_id="cmt_y"))
    matcher.process_pending()

    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 0
    assert stats.verbose_stats()["detail"]["suppressed_deleted"] == 1
    # Suppression is not a duplicate; it must not pad the graded field.
    assert stats.core_stats()["duplicates_blocked"] == 0


def test_delete_after_delivery_does_not_rewrite_history():
    matcher.create_rule("PRICE", "list")
    webhook.ingest(make_event(text="PRICE", user_id="usr_1", comment_id="cmt_z"))
    matcher.process_pending()
    with db.tx() as conn:
        conn.execute("UPDATE dm_tasks SET state = ?", (db.DELIVERED,))

    webhook.ingest(make_event(comment_id="cmt_z", event_type="comment.deleted"))
    matcher.process_pending()

    assert db.query_one("SELECT state FROM dm_tasks")["state"] == db.DELIVERED
    assert stats.core_stats()["sent"] == 1


def test_cancelled_dms_are_excluded_from_every_headline_number():
    matcher.create_rule("PRICE", "list")
    webhook.ingest(make_event(text="PRICE", user_id="usr_1", comment_id="cmt_c"))
    matcher.process_pending()
    webhook.ingest(make_event(comment_id="cmt_c", event_type="comment.deleted"))
    matcher.process_pending()

    core = stats.core_stats()
    assert core == {"sent": 0, "failed": 0, "queued": 0, "duplicates_blocked": 0}
    assert stats.verbose_stats()["detail"]["cancelled_by_delete"] == 1


# --- malformed input --------------------------------------------------------

def test_event_without_id_is_recorded_but_not_stored():
    assert webhook.ingest({"event_type": "comment.created", "data": {}}) == "ignored"
    assert db.scalar("SELECT COUNT(*) FROM events") == 0
    assert db.scalar("SELECT COUNT(*) FROM invariants WHERE kind='event_missing_id'") == 1


def test_comment_without_user_id_creates_no_task():
    matcher.create_rule("PRICE", "list")
    event = make_event(text="PRICE")
    event["data"]["from"] = {}
    webhook.ingest(event)
    matcher.process_pending()
    assert db.scalar("SELECT COUNT(*) FROM dm_tasks") == 0


def test_non_json_body_is_rejected_without_a_500():
    body = b"this is not json"
    with client() as c:
        resp = c.post("/webhook", content=body,
                      headers={"X-PseudoGram-Signature": sign(body)})
    assert resp.status_code == 400
