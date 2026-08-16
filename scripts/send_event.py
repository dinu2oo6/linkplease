"""Post a single signed webhook event. Used for hand-driving edge cases.

    python scripts/send_event.py created --user usr_x --text "PRICE pls" --comment cmt_1
    python scripts/send_event.py deleted --comment cmt_1
    python scripts/send_event.py created --user usr_x --text "PRICE" --event-id evt_fixed
"""
import argparse
import hashlib
import hmac
import json
import os
import time
import uuid

import httpx


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=["created", "deleted"])
    parser.add_argument("--url", default=os.getenv("TARGET", "http://127.0.0.1:8000/webhook"))
    parser.add_argument("--secret", default=os.getenv("PSEUDOGRAM_API_KEY", "chaos-test-key"))
    parser.add_argument("--user", default="usr_manual")
    parser.add_argument("--username", default="manual.tester")
    parser.add_argument("--text", default="PRICE please")
    parser.add_argument("--comment", default=None)
    parser.add_argument("--event-id", default=None)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--unsigned", action="store_true")
    args = parser.parse_args()

    comment_id = args.comment or f"cmt_{uuid.uuid4().hex[:8]}"
    event_id = args.event_id or f"evt_{uuid.uuid4().hex[:12]}"
    now = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())

    if args.kind == "created":
        data = {"comment_id": comment_id, "post_id": "post_manual", "text": args.text,
                "created_at": now,
                "from": {"user_id": args.user, "username": args.username}}
    else:
        data = {"comment_id": comment_id}

    payload = {"event_id": event_id, "event_type": f"comment.{args.kind}",
               "sent_at": now, "data": data}
    raw = json.dumps(payload, separators=(",", ":")).encode()
    headers = {"Content-Type": "application/json"}
    if not args.unsigned:
        headers["X-PseudoGram-Signature"] = "sha256=" + hmac.new(
            args.secret.encode(), raw, hashlib.sha256).hexdigest()

    for _ in range(args.repeat):
        resp = httpx.post(args.url, content=raw, headers=headers, timeout=10.0)
        print(resp.status_code, resp.text, "|", event_id, comment_id)


if __name__ == "__main__":
    main()
