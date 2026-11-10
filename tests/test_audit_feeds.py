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


# FEED-SYNC-CLOCK: a failed clock re-sync (the /time read refused during a Binance cooldown) must not abort the cycle
# when the clock synced before and the pushed stream rows are fresh; with no sync yet it still aborts
async def feed_sync_clock() -> None:
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "ADMIN_USER_ID": "42", "SYMBOLS": "UNITREEUSDT",
                             "BASELINE_MODE": "manual", "AUCTION_ALERT": "off"})

    class TG:
        async def send(self, *a, **k):
            pass

    def build():
        store = m.Store(":memory:")
        market = m.Binance(cfg)

        class FreshStream:
            def rows(self, symbols):
                return {s: {"symbol": s, "price": "76", "time": market.now_ms()} for s in symbols}

            def seed(self, rows):
                pass

        bot = m.Bot(cfg, store, market, TG())
        market.stream = FreshStream()
        store.put(f"manual:UNITREEUSDT:{m.beijing_day()}", {"value": "75", "valid_date": m.beijing_day()})
        return bot, market

    real = m.http_json

    async def no_http(*a, **k):
        raise AssertionError("no request may go out during the cooldown")

    m.http_json = no_http
    try:
        # synced 601 s ago (re-sync due) and a 418 cooldown running: the stream rows still serve
        bot, market = build()
        market.offset_ms = 37
        market.clock_checked = time.monotonic() - 601
        market.blocked_until = time.monotonic() + 120
        await bot.one_cycle()
        snap = bot.snapshots["UNITREEUSDT"]
        assert "quote" in snap and "error" not in snap, snap
        assert market.offset_ms == 37 and time.monotonic() - market.clock_checked > 600  # old offset kept; retried next cycle
        # never synced (offset 0 = raw local clock): the failure still aborts the cycle
        bot, market = build()
        market.blocked_until = time.monotonic() + 120
        await bot.one_cycle()
        assert bot.snapshots["UNITREEUSDT"] == {"error": "币安接口冷却中，暂停请求"}, bot.snapshots
        # the last good sync over an hour old: it aborts too
        bot, market = build()
        market.clock_checked = time.monotonic() - 3601
        market.blocked_until = time.monotonic() + 120
        await bot.one_cycle()
        assert bot.snapshots["UNITREEUSDT"] == {"error": "币安接口冷却中，暂停请求"}, bot.snapshots
    finally:
        m.http_json = real


asyncio.run(feed_sync_clock())


# FEED-SPOT-PREV-ZERO: one cash-index source answering a 前收 of 0 (an empty field kept as 0) made spot_problem raise
# in percent(), and pick_quote ran problem() outside its try: the whole IndexFutures.refresh aborted, Sina (good
# figure) was never asked and self.quote stayed stale. The zero prev must be refused and the next source tried.
async def feed_spot_prev_zero() -> None:
    import datetime as dt, json
    D = m.D

    def ms(d: int, h: int, mi: int) -> int:
        return int(dt.datetime(2026, 9, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)

    now = ms(23, 10, 30)
    asked: list[str] = []

    async def fake_get(url, timeout=15, headers=None):
        asked.append(url)
        if "etnet" in url:
            raise m.RemoteError("HTTP 502")
        if "134.HSI_M" in url:
            return json.dumps({"data": {"f43": 24822.0, "f60": 25055.0, "f86": now // 1000, "f58": "恒指主连"}}).encode()
        if "100.HSI" in url:  # the cash index with an empty 前收 written as 0
            return json.dumps({"data": {"f43": 24834.12, "f60": 0, "f86": now // 1000}}).encode()
        if "rt_hkHSI" in url:
            return ('var hq_str_rt_hkHSI="HSI,恒生指数,25064.84,25087.75,25072.25,24811.85,24834.12,'
                    + ",".join(["0"] * 10) + ',2026/09/23,10:30:00";').encode("gbk")
        raise m.RemoteError("HTTP 404")

    real_get, real_health = m.http_get, m.SOURCE_HEALTH
    m.http_get, m.SOURCE_HEALTH = fake_get, m.SourceHealth()
    try:
        # (a) the refresh round survives the zero 前收 and takes Sina's cash index
        h = m.IndexFutures(True, frozenset())
        h.dated_close = lambda day: D("25087.75") if day == dt.date(2026, 9, 22) else None
        await h.refresh(now, force=True)
        assert any("rt_hkHSI" in url for url in asked), asked
        assert h.quote is not None and "新浪" in h.quote.spot_source, h.quote
        assert h.quote.spot_prev == D("25087.75") and h.spot_problem(h.quote, now) == "", h.quote
        # (b) a stored quote with a zero 前收 is refused (not raised on) once the dated close is known
        zero = m.FuturesQuote("恒指", D("24822"), D("25055"), None, None, None, now, "东方财富",
                              spot=D("24834.12"), spot_source="东方财富", spot_prev=D(0), spot_ms=now)
        why = h.spot_problem(zero, now)
        assert why and "前收" in why, why
        # (c) a check that raises passes the source over instead of aborting pick_quote
        sources = [("东方财富", "https://push2.eastmoney.com/api/qt/stock/get?secid=100.HSI", {})]

        def boom(quote):
            raise ValueError("数值必须是正的有限数字")

        best, notes, error = await m.pick_quote(sources, lambda name, raw: zero, boom)
        assert best is zero and error and "数值必须是正的有限数字" in notes[0], (best, notes, error)
    finally:
        m.http_get, m.SOURCE_HEALTH = real_get, real_health


asyncio.run(feed_spot_prev_zero())

print("AUDIT_FEEDS_OK")
