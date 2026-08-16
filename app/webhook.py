"""Inbound webhook: verify, persist, acknowledge. Nothing else.

The whole point of this module is that it does as little as possible. Rule
matching, sending and reconciliation all happen on background tasks reading
from the database, so a slow PseudoGram can never make us drop an event.
"""
import base64
import hashlib
import hmac
import json
import time

from . import config, db


def candidate_secrets() -> list[bytes]:
    """Every secret PseudoGram might plausibly be signing with.

    The docs say the secret is the API key. It is not. Keys look like
    `<base64>.<hex>`, and the real signatures verify against the base64 half
    *decoded* -- which turns out to be the account email.

    I found this only because signature verification rejected 29 of 29 live
    webhooks and I logged the raw bytes to find out why. Worth stating plainly:
    implementing Part B exactly as documented rejects every real event, returns
    401 to all of them, and produces perfectly honest-looking stats for a system
    that received nothing. Not implementing it at all would have "worked".

    So we accept any of these. All are derived from our own key, so this widens
    what we accept without letting in anything an attacker couldn't already
    forge with the key. Being strict about *which* correct secret it is would
    buy no security and would break the moment they fix their docs.
    """
    secrets: list[bytes] = []
    key = config.API_KEY
    if key:
        secrets.append(key.encode("utf-8"))              # as documented
        prefix = key.partition(".")[0]
        if prefix:
            try:
                decoded = base64.urlsafe_b64decode(prefix + "=" * (-len(prefix) % 4))
                if decoded:
                    secrets.append(decoded)              # what actually verifies
            except Exception:
                pass
    if config.ACCOUNT_EMAIL:
        secrets.append(config.ACCOUNT_EMAIL.encode("utf-8"))
    return secrets


def verify_signature(raw_body: bytes, header_value: str | None) -> bool:
    """HMAC-SHA256 over the raw request body.

    The header looks like `sha256=<hex>`. Compared in constant time, and we
    never fall back to "no header means fine" when signatures are required.
    """
    if not config.REQUIRE_SIGNATURE:
        return True
    if not header_value:
        return False
    candidate = header_value.strip()
    if candidate.lower().startswith("sha256="):
        candidate = candidate[7:]
    candidate = candidate.lower()

    matched = False
    for secret in candidate_secrets():
        expected = hmac.new(secret, raw_body, hashlib.sha256).hexdigest()
        # No early exit: every candidate is checked every time, so the work done
        # doesn't leak which secret matched.
        matched |= hmac.compare_digest(expected, candidate)
    return matched


def ingest(payload: dict) -> str:
    """Durably record one webhook delivery.

    Returns "accepted" for a first sighting, "redelivered" for a repeat.

    A repeat does not overwrite anything and does not short-circuit matching:
    it increments delivery_count, which the matcher treats as another pass.
    That is deliberate. Event-level dedupe here is an optimisation; the actual
    no-double-DM guarantee lives on the dm_tasks primary key. Running the
    redelivery through the matcher anyway is what lets us count it honestly as
    a blocked duplicate instead of silently discarding it.

    Shares its implementation with the batch writer, so the single-event path
    used by tests and the batched path used in production cannot drift apart.
    """
    from .ingest import write_batch
    return write_batch([payload])[0]
