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


# FEED-POOL-POST: a POST (an order or a cancel, idle limit 3 s) that finds the newest pooled connection too old gets a
# fresh connection, but must not close the others that a GET may still reuse (idle limit 25 s): only those past it go
def feed_pool_post() -> None:
    closed: list[int] = []

    class FakeConn:
        made = 0

        def __init__(self, *args, **kwargs):
            FakeConn.made += 1
            self.sock = None
            self.n = 100 + FakeConn.made

        def close(self):
            closed.append(self.n)

    class Pooled(FakeConn):
        def __init__(self, n):
            self.sock, self.n = None, n

    host = "api.predict.fun"
    real_conn = m.HTTPS_CONNECTION
    m.HTTPS_CONNECTION = FakeConn
    try:
        m._POOL.pop((host, 443), None)
        now = time.monotonic()
        m._POOL[(host, 443)] = [(Pooled(0), now - 60)] + [(Pooled(i), now - 5) for i in range(1, 12)]
        conn, reused = m._pool_take(host, 443, 10, m.KEEPALIVE_POST_IDLE_SECONDS)
        assert not reused and isinstance(conn, FakeConn) and conn.n > 100, (conn, reused)
        rows = m._POOL[(host, 443)]
        assert len(rows) == 11, len(rows)
        assert closed == [0], closed
        assert [c.n for c, _ in rows] == list(range(1, 12)), [c.n for c, _ in rows]  # oldest first, as before
        conn, reused = m._pool_take(host, 443, 10, m.KEEPALIVE_IDLE_SECONDS)
        assert reused and conn.n == 11 and len(m._POOL[(host, 443)]) == 10, (conn.n, reused)
        # a GET miss (every pooled connection past 25 s) still drops them all, as before
        m._POOL[(host, 443)] = [(Pooled(i), now - 40) for i in range(20, 23)]
        conn, reused = m._pool_take(host, 443, 10, m.KEEPALIVE_IDLE_SECONDS)
        assert not reused and not m._POOL[(host, 443)] and closed[-3:] == [20, 21, 22], closed
    finally:
        m.HTTPS_CONNECTION = real_conn
        m._POOL.pop((host, 443), None)


feed_pool_post()


# FEED-ETNET-CLOSED-BLOCK: etnet prints no time and a finished session's block is dated from the calendar, so while the
# market is shut a page left on an older session (Friday's, read Tuesday 08:00) passed as "the session that ended last"
# and, with Monday's dated close and a 16:10 anchor known, priced a warn-free after-hours card from Friday's futures
def feed_etnet_closed_block() -> None:
    import datetime as dt
    D = m.D

    def ms(d, h, mi):
        return int(dt.datetime(2026, 9, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)

    page = """<div><h3>恒生指數期貨(09/2026)</h3><span>日市</span><div>&#9660;24,522 -169 (-0.68%) 高水12</div>
<div>最高: 24,643 最低: 24,265 前收市: 24,691 開市: 24,630</div><img alt="chart"></div>
<div><h3>恒生指數期貨(09/2026)</h3><span>夜市</span><div>&#9660;24,501 -21 (-0.09%) 低水9</div>
<div>最高: 24,582 最低: 24,451 前收市: 24,522 開市: 24,528</div><img alt="chart"></div>
<div>未平倉 (到期日：29/09/2026)</div>
<div>恒生指數現貨 &#9660;24,510.09 -251.04 (-1.01%) 最高: 24,537.14 最低: 24,275.56 前收市: 24,761.13 開市: 24,523.50</div>""".encode()
    now = ms(29, 8, 0)  # Tuesday 08:00: shut; the session that ended last is Monday's night (Tue 03:00)
    clock = {"now": now}

    class _M:
        def now_ms(self):
            return clock["now"]

    bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "HK0625USDT", "KOSPI_INDEX": "off",
                                   "SSE_INDEX": "off"}), m.Store(":memory:"), _M(), None)
    hk = bot.hsi.holidays
    bot.hsi_daily.daily[dt.date(2026, 9, 24)] = D("24761.13")
    bot.hsi_daily.daily[dt.date(2026, 9, 25)] = D("24510.09")
    # control: the same page on Saturday 18:30 is the current one (its cash 前收市 is the 09-24 close)
    sat = ms(26, 18, 30)
    qs = bot.hsi.dated(m.parse_etnet_futures(page, sat, hk), sat)
    assert bot.hsi.futures_problem(qs, sat) == "", bot.hsi.futures_problem(qs, sat)
    # Friday's page read on Tuesday 08:00: dated Tue 03:00 by the calendar, but its cash index is Friday's
    q = bot.hsi.dated(m.parse_etnet_futures(page, now, hk), now)
    assert q.quoted_ms == ms(29, 3, 0), m.stamp(q.quoted_ms)
    why = bot.hsi.futures_problem(q, now)
    assert why and "etnet" in why and "24,761.13" in why, why
    # Monday closed 25,300 and the futures printed 25,310 at 16:10 (Eastmoney anchor, contract ""): the card must not
    # be priced warn-free from Friday's 24,501
    bot.hsi.quote = q
    bot.hsi.families = {}
    bot.hsi_daily.daily[dt.date(2026, 9, 28)] = D("25300")
    bot.store.put("anchor:HSI", [ms(28, 16, 10), "25310", "16:10 现货收市时", "", ms(28, 16, 10) + 3000, ms(28, 16, 10)])
    o = bot.hsi_odds(now)
    assert isinstance(o, str) or o.warn or "24,501" not in o.proxy_note, o


feed_etnet_closed_block()


# FEED-HEARTBEAT-FRESH: PredictStream.touch() restarted, at every heartbeat (every 15 s), the age of every book ever
# pushed on the connection, whether or not its topic still brought frames. A topic gone quiet on a live connection (a
# subscription the server dropped, a market paused) whose REST reads fail as well (a 429 cool-down, "Market is closed")
# then kept its last levels at age 0 for ever: the stale guard (PREDICT_STALE_MS), the express lane's skip and the
# 盘口断流 withdrawal never tripped. A topic that keeps re-sending its book (the server does every second or so), and one
# quiet for less than PREDICT_WS_SILENT_SECONDS, must still be restarted by the heartbeat.
async def feed_heartbeat_fresh() -> None:
    import json
    D, real_time, real_blocking = m.D, m.time, m.predict_blocking

    class Clock:  # main's wall and monotonic clocks moved together: three minutes of a connection in no time
        def __init__(self):
            self.wall, self.mono = float(int(real_time.time())), float(int(real_time.monotonic()))

        def time(self):
            return self.wall

        def monotonic(self):
            return self.mono

        def advance(self, seconds):
            self.wall += seconds
            self.mono += seconds

        def __getattr__(self, name):
            return getattr(real_time, name)

    class Ws:
        def __init__(self):
            self.sent = []

        async def send(self, text):
            self.sent.append(json.loads(text))

    def frame(mid: str, bid: str, ask: str) -> str:  # the topic's book; the same levels again are a re-send
        return json.dumps({"type": "M", "topic": "predictOrderbook/" + mid,
                           "data": {"version": 3, "bids": [[bid, "100"]], "asks": [[ask, "100"]]}})

    beat = lambda t: json.dumps({"type": "M", "topic": "heartbeat", "data": t})
    rest: list[str] = []

    async def closed(fn, url, *args):  # every REST request fails (the transport under the real fetch)
        rest.append(url)
        raise m.RemoteError("HTTP 400: Market is closed")

    clock = Clock()
    now = lambda: int(clock.wall * 1000)
    m.time, m.predict_blocking = clock, closed
    try:
        # (1) two single markets on one live connection, their REST reads failing: HSI's topic brings its snapshot and
        # goes quiet, KOSPI's re-sends its book every second; the real hot round every 10 s, the heartbeat every 15 s
        feed = m.PredictFeed(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "PREDICT_API_KEY": "k1"}))
        targets: dict[str, str] = {}
        for key, mid in (("HSI", "7"), ("KOSPI", "9")):
            slug = f"{key.lower()}-up-or-down-on-november-10-2026"
            market = {"id": mid, "conditionId": "c" + mid, "title": f"{key} up?"}
            feed.books[key] = m.PredictBook(key, slug, mid, market["title"], ((D("0.80"), D("100")),), ((D("0.85"), D("100")),), now(), 200)
            feed.markets[slug], feed.fees[mid], feed.strikes[slug] = (market, clock.mono), (200, clock.mono), (None, clock.mono)
            targets[key] = slug
        stream, ws = feed.stream, Ws()
        stream.ws, stream.connected_at, stream.last_frame = ws, clock.mono, clock.mono
        stream.subscribed = {"7": ("HSI", False), "9": ("KOSPI", False)}
        await stream.handle(ws, frame("7", "0.81", "0.84"))
        await stream.handle(ws, frame("9", "0.81", "0.84"))
        assert stream.live == {"7", "9"} and feed.books["HSI"].bids == ((D("0.81"), D("100")),)
        first_stale = None
        for t in range(1, 181):
            clock.advance(1)
            await stream.handle(ws, frame("9", "0.81", "0.84"))
            if t % 15 == 0:
                stream.last_frame = clock.mono
                await stream.handle(ws, beat(t))
                if t == 30:  # quiet for less than PREDICT_WS_SILENT_SECONDS: restarted by the heartbeat as before
                    assert feed.books["HSI"].fetched_ms == now(), (now() - feed.books["HSI"].fetched_ms) / 1000
            if t % 10 == 0:
                await feed.refresh(targets, force=True, hot=set(targets))
                if feed.cold_task is not None:
                    await feed.cold_task
            if first_stale is None and feed.books["HSI"].stale(now()):
                first_stale = t
        hsi, kospi = feed.books["HSI"], feed.books["KOSPI"]
        assert rest and "Market is closed" in feed.errors.get("HSI", ""), (len(rest), feed.errors)
        assert stream.heartbeats == 12 and hsi.bids == ((D("0.81"), D("100")),), (stream.heartbeats, hsi)
        assert hsi.stale(now()), \
            f"a book whose topic went quiet three minutes ago is re-stamped by every heartbeat: age {(now() - hsi.fetched_ms) / 1000:.0f} s"
        # it ages from its last restart: stale at most PREDICT_WS_SILENT_SECONDS + PREDICT_STALE_MS after its last frame
        assert first_stale is not None and first_stale <= m.PREDICT_WS_SILENT_SECONDS + m.PREDICT_STALE_MS / 1000 + 1, first_stale
        assert kospi.fetched_ms == now() and not kospi.stale(now()), "a topic that re-sends its book stays fresh"

        # (2) a ladder's levels, the same: level 41's topic goes quiet, level 42's re-sends; nothing else reads them
        feed = m.PredictFeed(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "PREDICT_API_KEY": "k1"}))
        key, slug = "BTC-HIT-11", "what-price-will-bitcoin-hit-in-november-2026"
        feed.ladder_keys.add(key)
        feed.ladder_rows[key] = {
            mid: m.LadderRow(D(target), mid, f"BTC ↑ ${target}",
                             m.PredictBook(key, slug, mid, "BTC", ((D("0.10"), D("50")),), ((D("0.20"), D("50")),), now(), 200))
            for mid, target in (("41", "90000"), ("42", "95000"))}
        feed.ladder_publish(key)
        stream, ws = feed.stream, Ws()
        stream.ws, stream.connected_at, stream.last_frame = ws, clock.mono, clock.mono
        stream.subscribed = {"41": (key, True), "42": (key, True)}
        await stream.handle(ws, frame("41", "0.12", "0.19"))
        await stream.handle(ws, frame("42", "0.12", "0.19"))
        assert stream.live == {"41", "42"}
        for t in range(1, 181):
            clock.advance(1)
            await stream.handle(ws, frame("42", "0.12", "0.19"))
            if t % 15 == 0:
                await stream.handle(ws, beat(t))
        rows, published = feed.ladder_rows[key], {row.market_id: row for row in feed.ladders[key]}
        assert rows["41"].book.stale(now()) and published["41"].book.stale(now()), \
            f"a ladder level whose topic went quiet is re-stamped by every heartbeat: age {(now() - rows['41'].book.fetched_ms) / 1000:.0f} s"
        assert rows["41"].book.bids == ((D("0.12"), D("100")),)  # the levels are kept, only their age is honest
        assert not rows["42"].book.stale(now()) and not published["42"].book.stale(now())
        # a market let go is forgotten with the rest of its state (a new connection starts with none, run)
        stream.want({"42": (key, True)})
        await stream.sync(ws)
        assert ws.sent[-1]["method"] == "unsubscribe" and "41" not in stream.heard and "41" not in stream.live and "42" in stream.heard, stream.heard
    finally:
        m.time, m.predict_blocking = real_time, real_blocking


asyncio.run(feed_heartbeat_fresh())

print("AUDIT_FEEDS_OK")
