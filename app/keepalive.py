"""Keep a free-tier web service from spinning down mid-drain.

Render suspends a free service after ~15 minutes with no *inbound* HTTP. Our
background loops don't count -- so a 30-minute drain, which by definition
receives no webhooks while it works, would be suspended roughly halfway through.

The ledger is in Postgres and the boot path recovers everything, so a suspension
costs time rather than correctness. But it costs a lot of time: the drain only
resumes when someone next hits the URL.

So we call our own public URL every few minutes. The request leaves the machine,
comes back through the platform's router, and counts as inbound traffic.

Limits worth being honest about, both in FAILURES.md:
  * This keeps the service awake; it cannot wake it up. Once suspended, only an
    external request revives it, and this loop is suspended too.
  * It's a workaround for a hosting constraint, not engineering. On a host with
    a persistent disk and no idle suspension, none of this file exists.
"""
import asyncio

import httpx

from . import config, db


async def keepalive_loop(stop: asyncio.Event) -> None:
    url = config.keepalive_url()
    if not url:
        return
    async with httpx.AsyncClient(timeout=20.0) as client:
        while not stop.is_set():
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=config.KEEPALIVE_INTERVAL_SECONDS)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await client.get(url)
            except Exception as exc:
                db.record_invariant("keepalive_failed", repr(exc))
