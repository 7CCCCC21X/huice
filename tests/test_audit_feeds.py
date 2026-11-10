import asyncio, sys, time
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m


# FEED-429-QUEUED: a 429 on one orderbook read starts the cooldown; the reads still queued on the gate (behind the
# PREDICT_PARALLEL in flight) must not go out into it, and refusing them must not push the cooldown further out
async def feed_429_queued() -> None:
    feed = m.PredictFeed(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}))
    feed.gate = asyncio.Semaphore(4)
    sent: list[str] = []
    hit: list[float] = []

    async def fake_blocking(fn, url, payload, timeout, headers):
        sent.append(url)
        first = len(sent) == 1
        await asyncio.sleep(0.05)
        if first:
            hit.append(time.monotonic())
            raise m.RemoteError("HTTP 429: Too Many Requests", 0)
        return b"{}"

    real = m.predict_blocking
    m.predict_blocking = fake_blocking
    try:
        results = await asyncio.gather(*(feed.fetch(f"{m.PREDICT_REST}/markets/{i}/orderbook") for i in range(12)),
                                       return_exceptions=True)
    finally:
        m.predict_blocking = real
    assert len(sent) == 4, (len(sent), sent)  # only the 4 already in flight; the 8 queued are refused
    errors = [r for r in results if isinstance(r, m.RemoteError)]
    assert len(errors) == 9 and sum("429" in str(e) for e in errors) == 1, results
    refused = [e for e in errors if "429" not in str(e)]
    assert all(e.retry_after > 0 and "冷却" in str(e) for e in refused), refused
    assert sum(r == {} for r in results) == 3, results
    # the cooldown is the 429's PREDICT_BACKOFF_SECONDS, not extended by the refusals
    assert abs(feed.blocked_until - (hit[0] + m.PREDICT_BACKOFF_SECONDS)) < 0.5, (feed.blocked_until - hit[0])
    assert feed.calls == 4, feed.calls  # /diag counts the requests sent, not the refused ones
    assert len(feed.limited) == 1


asyncio.run(feed_429_queued())

print("AUDIT_FEEDS_OK")
