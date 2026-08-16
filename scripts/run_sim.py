"""Fire a simulation at a running LinkPlease, watch it drain, then self-grade.

    # against the local chaos server
    python scripts/chaos_server.py &
    PSEUDOGRAM_API_KEY=chaos-test-key PSEUDOGRAM_BASE_URL=http://127.0.0.1:8899 \
        uvicorn app.main:app --port 8000 &
    python scripts/run_sim.py --target http://127.0.0.1:8000 --count 500 --local

    # against the deployed app and the real PseudoGram
    python scripts/run_sim.py --target https://your-app.fly.dev --count 500

Draining 500 events takes ~25 minutes at 10 sends/60s. That is the rate limit,
not us being slow, and --wait tells you how long to watch.
"""
import argparse
import json
import time

import httpx

DEFAULT_RULES = [
    {"keyword": "PRICE", "dm_message": "Here's the price list: linkplease.co/pricing"},
    {"keyword": "LINK", "dm_message": "Here's the link: linkplease.co"},
    {"keyword": "COST", "dm_message": "Costs start at 999/mo."},
    {"keyword": "info", "dm_message": "Full details: linkplease.co/info"},
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:8000")
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument("--wait", type=int, default=2400, help="seconds to watch")
    parser.add_argument("--local", action="store_true",
                        help="also print the chaos server's send ledger")
    parser.add_argument("--chaos", default="http://127.0.0.1:8899")
    parser.add_argument("--no-rules", action="store_true")
    args = parser.parse_args()

    target = args.target.rstrip("/")
    with httpx.Client(timeout=60.0) as client:
        if not args.no_rules:
            existing = client.get(f"{target}/rules").json()["rules"]
            if not existing:
                for rule in DEFAULT_RULES:
                    client.post(f"{target}/rules", json=rule)
                print(f"created {len(DEFAULT_RULES)} rules")
            else:
                print(f"reusing {len(existing)} existing rules")

        resp = client.post(
            f"{target}/admin/simulate",
            params={"count": args.count, "duration_seconds": args.duration},
        )
        body = resp.json()
        print(json.dumps(body, indent=2))
        run_id = (body.get("body") or {}).get("run_id")
        if not run_id:
            raise SystemExit("no run_id -- is PUBLIC_WEBHOOK_URL set on the target?")

        started = time.time()
        last = None
        while time.time() - started < args.wait:
            time.sleep(5)
            stats = client.get(f"{target}/stats").json()
            if stats != last:
                elapsed = int(time.time() - started)
                print(f"  t+{elapsed:>4}s  sent={stats['sent']:<4} "
                      f"queued={stats['queued']:<4} failed={stats['failed']:<3} "
                      f"dupes_blocked={stats['duplicates_blocked']}")
                last = stats
            if stats["queued"] == 0 and elapsed > 20:
                print("  drained.")
                break

        print("\n=== self-audit against PseudoGram's truth ===")
        audit = client.get(f"{target}/audit/{run_id}", timeout=120.0).json()
        print(json.dumps(audit, indent=2))

        if args.local:
            print("\n=== chaos server ledger (server-side truth) ===")
            print(json.dumps(client.get(f"{args.chaos}/_ledger").json(), indent=2))


if __name__ == "__main__":
    main()
