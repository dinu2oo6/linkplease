"""Batched webhook ingest.

Moving the ledger from a local SQLite file to a network Postgres turned the
webhook's single INSERT from ~0.1ms into a network round-trip. Serialised
through one connection, 500 events arriving in 10 seconds would queue behind
each other: at 5ms the last event waits 2.5s, at 10ms it blows the 5 second
budget and PseudoGram starts recording dropped deliveries.

So requests are grouped: everything that arrives within a ~15ms window is
written as ONE multi-row INSERT, and every waiting request is released when that
statement commits. 500 events become roughly a dozen round-trips instead of 500.

The durability guarantee is unchanged, which is the whole point. A request still
does not receive its 200 until its row is committed -- we batch the write, we do
not defer it. Nothing is ever acknowledged from memory.

Same-batch redelivery is handled explicitly: Postgres refuses to let one
ON CONFLICT DO UPDATE touch a row twice, so duplicate event_ids inside a window
are collapsed into a single row whose delivery_count increments by the number of
copies, and each caller is told its own position in that sequence.
"""
import asyncio
import json
import time

from . import config, db

_queue: asyncio.Queue | None = None

_INSERT_HEAD = """
INSERT INTO events (event_id, event_type, sent_at, first_seen_at,
                    last_seen_at, delivery_count, processed_pass, payload)
VALUES """
_INSERT_TAIL = """
ON CONFLICT(event_id) DO UPDATE SET
    last_seen_at   = excluded.last_seen_at,
    delivery_count = events.delivery_count + excluded.delivery_count
RETURNING event_id, delivery_count
"""


def write_batch(payloads: list[dict]) -> list[str]:
    """Persist a batch. Returns "accepted"/"redelivered"/"ignored" per payload.

    One statement for the whole batch, on both backends.
    """
    results: list[str | None] = [None] * len(payloads)
    groups: dict[str, dict] = {}

    for i, payload in enumerate(payloads):
        event_id = payload.get("event_id")
        if not event_id:
            results[i] = "ignored"
            continue
        group = groups.setdefault(event_id, {"payload": payload, "indices": []})
        group["indices"].append(i)

    if groups:
        now = time.time()
        rows, params = [], []
        for event_id, group in groups.items():
            payload = group["payload"]
            rows.append("(?, ?, ?, ?, ?, ?, 0, ?)")
            params.extend([
                event_id,
                payload.get("event_type", ""),
                payload.get("sent_at"),
                now,
                now,
                len(group["indices"]),          # this batch's copies
                json.dumps(payload, separators=(",", ":")),
            ])

        sql = _INSERT_HEAD + ", ".join(rows) + _INSERT_TAIL
        with db.tx() as conn:
            returned = conn.execute(sql, tuple(params)).fetchall()

        finals = {r["event_id"]: r["delivery_count"] for r in returned}
        for event_id, group in groups.items():
            count = len(group["indices"])
            final = finals.get(event_id, count)
            # The k-th copy in this batch landed at delivery number base+k.
            base = final - count + 1
            for k, index in enumerate(group["indices"]):
                results[index] = "accepted" if base + k == 1 else "redelivered"

    # Recorded outside the batch transaction so a malformed payload can never
    # roll back a batch of good ones.
    for i, payload in enumerate(payloads):
        if results[i] == "ignored":
            db.record_invariant("event_missing_id", json.dumps(payload)[:500])

    return [r or "ignored" for r in results]


_latency_buffer: list[float] = []


def record_latency(ms: float) -> None:
    """Buffer webhook acknowledgement times, flushed in batches.

    Writing a row per request would double the database work on the hot path --
    the exact thing the batching above exists to avoid. Buffered in memory and
    flushed every 50 samples: losing a few timing rows on a crash costs nothing,
    since they are diagnostics, not the ledger.
    """
    _latency_buffer.append(ms)
    if len(_latency_buffer) < 50:
        return
    batch, _latency_buffer[:] = list(_latency_buffer), []
    now = time.time()
    try:
        with db.tx() as conn:
            for value in batch:
                conn.execute("INSERT INTO webhook_timing (ts, ms) VALUES (?, ?)",
                             (now, value))
    except Exception:
        pass


def flush_latency() -> None:
    if not _latency_buffer:
        return
    batch, _latency_buffer[:] = list(_latency_buffer), []
    now = time.time()
    try:
        with db.tx() as conn:
            for value in batch:
                conn.execute("INSERT INTO webhook_timing (ts, ms) VALUES (?, ?)",
                             (now, value))
    except Exception:
        pass


async def submit(payload: dict) -> str:
    """Hand one event to the batch writer and wait for its commit."""
    if _queue is None:                     # batcher not running (tests)
        return await asyncio.to_thread(lambda: write_batch([payload])[0])
    future: asyncio.Future = asyncio.get_running_loop().create_future()
    await _queue.put((payload, future))
    return await future


async def batcher_loop(stop: asyncio.Event) -> None:
    global _queue
    _queue = asyncio.Queue()
    try:
        while not stop.is_set():
            try:
                first = await asyncio.wait_for(_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            batch = [first]
            # Let a moment's worth of arrivals accumulate. Under a burst this
            # fills instantly; when idle it costs one event ~15ms.
            deadline = time.monotonic() + config.INGEST_BATCH_WINDOW
            while len(batch) < config.INGEST_BATCH_MAX:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(_queue.get(), timeout=remaining))
                except asyncio.TimeoutError:
                    break

            payloads = [p for p, _ in batch]
            try:
                results = await asyncio.to_thread(write_batch, payloads)
            except Exception as exc:
                db.record_invariant("ingest_batch_error", repr(exc))
                # Fail the requests rather than 200 something we didn't store.
                for _, future in batch:
                    if not future.done():
                        future.set_exception(exc)
                continue

            for (_, future), result in zip(batch, results):
                if not future.done():
                    future.set_result(result)

            # Idle moment: get the buffered timings on disk.
            if _queue is not None and _queue.empty():
                await asyncio.to_thread(flush_latency)
    finally:
        _queue = None
