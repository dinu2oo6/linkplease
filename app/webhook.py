"""Inbound webhook: verify, persist, acknowledge. Nothing else.

The whole point of this module is that it does as little as possible. Rule
matching, sending and reconciliation all happen on background tasks reading
from the database, so a slow PseudoGram can never make us drop an event.
"""
import hashlib
import hmac
import json
import time

from . import config, db


def verify_signature(raw_body: bytes, header_value: str | None) -> bool:
    """HMAC-SHA256 of the raw request body, keyed with our API key.

    The header looks like `sha256=<hex>`. We compare in constant time, and we
    never fall back to "no header means fine" when signatures are required.
    """
    if not config.REQUIRE_SIGNATURE:
        return True
    if not header_value or not config.API_KEY:
        return False
    candidate = header_value.strip()
    if candidate.lower().startswith("sha256="):
        candidate = candidate[7:]
    expected = hmac.new(
        config.API_KEY.encode("utf-8"), raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, candidate.lower())


def ingest(payload: dict) -> str:
    """Durably record one webhook delivery.

    Returns "accepted" for a first sighting, "redelivered" for a repeat.

    A repeat does not overwrite anything and does not short-circuit matching:
    it increments delivery_count, which the matcher treats as another pass.
    That is deliberate. Event-level dedupe here is an optimisation; the actual
    no-double-DM guarantee lives on the dm_tasks primary key. Running the
    redelivery through the matcher anyway is what lets us count it honestly as
    a blocked duplicate instead of silently discarding it.
    """
    event_id = payload.get("event_id")
    if not event_id:
        db.record_invariant("event_missing_id", json.dumps(payload)[:500])
        return "ignored"

    now = time.time()
    with db.tx() as conn:
        cur = conn.execute(
            """
            INSERT INTO events (event_id, event_type, sent_at, first_seen_at,
                                last_seen_at, delivery_count, processed_pass, payload)
            VALUES (?, ?, ?, ?, ?, 1, 0, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                last_seen_at   = excluded.last_seen_at,
                delivery_count = events.delivery_count + 1
            RETURNING delivery_count
            """,
            (
                event_id,
                payload.get("event_type", ""),
                payload.get("sent_at"),
                now,
                now,
                json.dumps(payload, separators=(",", ":")),
            ),
        )
        delivery_count = db.first_value(cur.fetchone(), 1)

    return "accepted" if delivery_count == 1 else "redelivered"
