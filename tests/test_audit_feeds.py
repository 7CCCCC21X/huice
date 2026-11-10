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


# FEED-CACHE-GROWTH: a 5/15-minute window card gets a new slug and market id every window. PredictFeed.refresh pruned
# its slug tables but never category_raw (the whole REST category answer per slug), market_meta, fees, book_keys or
# meta_errors (per market id): one entry per window, for the life of the process
async def feed_cache_growth() -> None:
    import json
    feed = m.PredictFeed(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "PREDICT_GRAPHQL": "off"}))
    listing: dict[str, dict] = {}

    def market(mid: int, slug: str) -> dict:
        return {"id": mid, "conditionId": "0x" + f"{mid:064x}", "title": f"BTC Up or Down {slug[-10:]}", "question": f"BTC up {mid}?",
                "description": "rules " * 50, "status": "REGISTERED", "tradingStatus": "OPEN", "feeRateBps": 200,
                "createdAt": "2026-11-10T00:00:00Z",
                "outcomes": [{"name": "Up", "indexSet": 1}, {"name": "Down", "indexSet": 2}]}

    async def answer(url):
        path = url.removeprefix(m.PREDICT_REST)
        if path.startswith("/categories/"):
            slug = path.removeprefix("/categories/")
            if slug not in listing:
                raise m.RemoteError("HTTP 404: not found", 0)
            return {"success": True, "data": {"slug": slug, "markets": [listing[slug]]}}
        if path.endswith("/orderbook"):
            return {"success": True, "data": {"bids": [[0.48, 100]], "asks": [[0.52, 100]]}}
        mid = int(path.removeprefix("/markets/"))
        found = next((v for v in listing.values() if v["id"] == mid), None)
        if found is None:
            raise m.RemoteError("HTTP 404: not found", 0)
        return {"success": True, "data": found}

    async def fake_fetch(url, payload=None):
        return json.loads(json.dumps(await answer(url)))
    feed.fetch = fake_fetch

    def age(table: dict, seconds: float) -> None:  # as if every entry had last been refreshed that long ago
        for k, v in list(table.items()):
            table[k] = (v[0], v[1] - seconds)

    rollovers = 30
    for i in range(rollovers):
        slug = f"btc-updown-15m-{1793761200 + 900 * i}"
        listing[slug] = market(10_000 + i, slug)
        await feed.refresh({"BTC-15M": slug}, force=True)
        await asyncio.sleep(0)
        if i < rollovers - 1:
            listing.pop(slug)
    current_slug, current_id = slug, str(10_000 + rollovers - 1)
    assert feed.books["BTC-15M"].market_id == current_id, feed.books
    assert set(feed.category_raw) <= {current_slug}, len(feed.category_raw)  # only the window the card follows
    assert set(feed.markets) == {current_slug}, len(feed.markets)
    # the past windows' details and fees stay until nothing has refreshed them for a day...
    assert current_id in feed.market_meta and current_id in feed.fees
    # ...then go, with their orderbook keys and details failures; the current market's survive even when as old
    feed.meta_errors["9999"] = "HTTP 404"  # a failure of a market no card follows
    age(feed.market_meta, m.PREDICT_META_KEEP_SECONDS + 1)
    age(feed.fees, m.PREDICT_META_KEEP_SECONDS + 1)
    age(feed.markets, 0)
    feed.blocked_until = 0
    await feed.refresh({"BTC-15M": current_slug}, force=True)
    assert set(feed.market_meta) == {current_id}, len(feed.market_meta)
    assert set(feed.fees) == {current_id}, len(feed.fees)
    assert set(feed.book_keys) == {current_id}, len(feed.book_keys)
    assert feed.meta_errors == {}, feed.meta_errors
    assert feed.books["BTC-15M"].market_id == current_id and feed.books["BTC-15M"].bid


asyncio.run(feed_cache_growth())

print("AUDIT_FEEDS_OK")
