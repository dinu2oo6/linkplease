"""Step 0: apply for a PseudoGram key, then fetch it.

    python scripts/apply_and_keygen.py \
        --name "Your Name" --email you@example.com \
        --phone "+91..." --linkedin https://linkedin.com/in/you

Prints the api_key and the .env line to paste. If you have already applied,
pass --keygen-only.
"""
import argparse
import json

import httpx

BASE = "https://pseudogram-api.onrender.com"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", required=True)
    parser.add_argument("--name")
    parser.add_argument("--phone")
    parser.add_argument("--linkedin")
    parser.add_argument("--whatsapp")
    parser.add_argument("--base", default=BASE)
    parser.add_argument("--keygen-only", action="store_true")
    args = parser.parse_args()

    with httpx.Client(base_url=args.base, timeout=60.0) as client:
        if not args.keygen_only:
            missing = [f for f in ("name", "phone", "linkedin") if not getattr(args, f)]
            if missing:
                raise SystemExit(f"--{' --'.join(missing)} required to apply")
            body = {"name": args.name, "email": args.email, "phone": args.phone,
                    "linkedin_url": args.linkedin}
            if args.whatsapp:
                body["whatsapp"] = args.whatsapp
            resp = client.post("/v1/apply", json=body)
            print(f"apply  -> {resp.status_code} {resp.text[:300]}")

        resp = client.post("/v1/keygen", json={"email": args.email})
        print(f"keygen -> {resp.status_code}")
        if resp.status_code != 200:
            print(resp.text[:500])
            raise SystemExit(1)
        payload = resp.json()
        print(json.dumps(payload, indent=2))
        print("\nPaste into .env:")
        print(f"PSEUDOGRAM_API_KEY={payload['api_key']}")
        print(f"PSEUDOGRAM_EMAIL={args.email}")


if __name__ == "__main__":
    main()
