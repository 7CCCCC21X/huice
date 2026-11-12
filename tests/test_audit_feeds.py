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


# FEED-FETCHED-MS: a book's fetched_ms is its age — a REST answer's landing, restarted by every unchanged re-send and by
# the heartbeat — but own_on_book (1.138.1: a book read before Predict's answer shows none of our order) and
# Bot.sim_own_resting (1.104.6: a book read before a cancel's confirmation may still show the order) took it for the time
# the levels were read. A REST read asked before Predict's answer and landing after it, or the old levels pushed again
# after it, then had our own 100 taken out of a 买1 that did not hold them — the real order just placed withdrawn as
# "买1 不足", a floor order placed and withdrawn, the order placed again at the back of the queue — or the cancelled
# order's ghost counted as somebody's 买1, and the next real order rested on 20 shares of others. A book now carries the
# time its levels were taken beside its age; and a deep-bid order is not moved up on a book that may show the order
# itself uncounted (a push can beat the HTTP answer: it then chased itself a cent at a time).
# The review of that fix found the same order moved DOWN off itself: fewer shares than SIM_MAKER_MIN_BID under an ask a
# cent away, read as somebody's bid with no room in front, "joined the valid 买1 below" — withdrawn, placed where it was
# and withdrawn again, 24 times a minute, the time the levels were taken no longer restarted by the re-sends. And the
# other side of the same doubt, found while replaying that: the order that takes a cancelled deep-bid order's place was
# put back at its price beside somebody's bid there (taken for the ghost on a book pushed before the cancel's
# confirmation), pushed off the front on the next book, withdrawn, and put back again — 8 orders and 8 cancels in 30 s.
# On a book that cannot tell our own order from somebody's bid at its price, a deep-bid order is now neither moved nor
# placed onto that bid.
async def feed_fetched_ms() -> None:
    import datetime as dt, json
    import live as L
    D, real_time, WEI = m.D, m.time, 10 ** 18
    BJ = lambda mo, d, h, mi=0: int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
    SLUG = "hang-seng-index-up-or-down-on-october-5-2026"
    SETTLE = {"key": "HSI", "target": "2026-10-05", "line": 24600.0, "close_ms": BJ(10, 5, 16, 10)}
    ASKS = [["0.58", "400"]]
    lv = lambda rows: tuple((D(p), D(q)) for p, q in rows)

    class Clock:  # main's and live's wall clock, in the test's hands; the rest stays the real module's. One clock for
        #           everything: answered_at being on the Binance-offset clock is another finding (SIM-OWN-ON-BOOK-CLOCK)
        def __init__(self, ms):
            self.ms, self.skew = ms, 0.0

        def time(self):
            return (self.ms + 0.5) / 1000  # int(time.time() * 1000) is self.ms exactly

        def monotonic(self):  # the real one, moved on with the wall clock where a round trip is timed on it (pass_ms)
            return real_time.monotonic() + self.skew

        def pass_ms(self, ms):  # time passes on both clocks
            self.ms, self.skew = self.ms + ms, self.skew + ms / 1000

        def __getattr__(self, name):
            return getattr(real_time, name)

    class FM:  # the bot's market clock: the same one
        config = None

        def now_ms(self):
            return clock.ms

    class Server:  # Predict's book of the market: the others' bids and our own open orders
        def __init__(self, others, asks=None):
            self.others, self.ours, self.asks = dict(others), {}, asks or ASKS

        def levels(self):
            rows = dict(self.others)
            for price, shares in self.ours.values():
                rows[price] = rows.get(price, 0.0) + shares
            return {"bids": [[f"{p:.2f}", f"{q:g}"] for p, q in sorted(rows.items(), reverse=True) if q > 0], "asks": self.asks}

    class Api:  # Predict as the real trader sees it (tests/test_trading.py's FakeApi, cut down): an order is on Predict's
        #         book, or off it, as its request arrives; an order is answered at once, a cancel confirmed 100 ms later
        def __init__(self, server):
            self.jwt, self.jwt_exp, self.auth_error, self.lookup = "t", 0, "", ""
            self.markets, self.open, self.closed, self.calls, self.positions_rows = {}, {}, {}, [], []
            self.next_id, self.server, self.on_create, self.on_remove, self.on_removed = 100, server, None, None, None

        def hook(self, name):  # a hook runs once, or every time when it says so (keep)
            hook = getattr(self, name)
            if hook is not None and not getattr(hook, "keep", False):
                setattr(self, name, None)
            return hook

        async def ensure_auth(self): pass

        async def market(self, mid): return self.markets[str(mid)]

        async def create_order(self, body):
            self.next_id += 1
            oid, order = str(self.next_id), body["data"]["order"]
            price, shares = round(int(order["makerAmount"]) / int(order["takerAmount"]), 2), int(order["takerAmount"]) / WEI
            self.calls.append(("create", price))
            self.server.ours[oid] = (price, shares)
            self.open[oid] = {"id": oid, "status": "OPEN", "amount": order["takerAmount"], "amountFilled": "0",
                              "strategy": body["data"]["strategy"], "order": {**order}}
            if (hook := self.hook("on_create")) is not None:  # what else happens while the order's answer is on its way back
                await hook()
            return {"order_id": oid, "hash": order["hash"], "code": None}

        async def remove_orders(self, ids):
            if (hook := self.hook("on_remove")) is not None:  # what else happens as the cancel goes out, before it reaches Predict
                await hook()
            self.calls.append(("remove", list(ids)))
            removed = [i for i in ids if i in self.open]
            for i in removed:
                self.server.ours.pop(i, None)
                self.closed[i] = {**self.open.pop(i), "status": "CANCELLED"}
            if (hook := self.hook("on_removed")) is not None:  # ...and while the cancel's confirmation is on its way back
                await hook()
            clock.ms += 100
            return {"removed": removed, "noop": [i for i in ids if i not in removed]}

        async def orders(self, status="OPEN", **params):
            rows = list(self.open.values()) if status == "OPEN" else [r for r in self.closed.values() if r["status"] == status]
            return [dict(r) for r in rows]

        async def order(self, oid, hash_=""):
            row = self.open.get(oid) or self.closed.get(oid)
            return dict(row) if row is not None else None

        async def positions(self): return self.positions_rows

    class Chain:
        async def account_owner(self, account): return "0x8fd379246834eac74B8419FfdA202CF8051F7A03"  # the test key's address
        async def usdt_balance(self, owner=None): return 1000 * WEI
        async def bnb_balance(self, owner=None): return 10 ** 17
        async def allowance(self, key, owner=None): return 10 ** 30
        async def redeem(self, *a): return "0xredeem"

    def single_feed(feed=None):  # HSI (market 101) with its market, fee and strike in hand: a read is the orderbook request alone
        feed = feed or m.PredictFeed(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}))
        mono = real_time.monotonic()
        feed.markets[SLUG] = ({"id": "101", "conditionId": "c", "title": "HSI up?"}, mono)
        feed.fees["101"], feed.strikes[SLUG] = (200, mono), (None, mono)
        return feed

    def card(book, fair=0.70):
        return m.SimMarket(SLUG, "恒生指数", "close", "HSI", fair, book, 0.03, "", ("涨", "跌"), SETTLE,
                           {"basis": {"fair_up": fair, "ref": 24600.0}, "sources": [{"what": "恒指期货", "source": "etnet"}], "proxy": None})

    def slow(feed, levels):
        """The feed's requests from here on: Predict takes the snapshot (``levels()``) as the request reaches it — at
        once — and the answer is on its way until ``go`` is set. (taken, go)."""
        taken, go = asyncio.Event(), asyncio.Event()

        async def fetch(url, payload=None):
            snapshot = json.loads(json.dumps(levels()))
            taken.set()
            await go.wait()
            return {"data": snapshot}
        feed.fetch = fetch
        return taken, go

    async def looked(feed):  # the express looks a stored book started, to their end
        while feed.express_tasks:
            await asyncio.gather(*list(feed.express_tasks))
            await asyncio.sleep(0)

    async def ask(feed, levels):
        """A REST read of HSI goes out (the real refresh_one) and Predict takes its snapshot; landed by land()."""
        taken, go = slow(feed, levels)
        read = asyncio.create_task(feed.refresh_one("HSI", SLUG))
        await taken.wait()
        return read, go

    async def land(feed, read, go, lane=True):
        """The read's answer lands now: its book is stored, and the express lane looks at once (unless ``lane`` is off)."""
        hook = feed.on_update
        feed.on_update = hook if lane else None
        try:
            go.set()
            assert await read == 1, feed.errors
        finally:
            feed.on_update = hook
        await looked(feed)
        return feed.books["HSI"]

    async def rest_read(feed, levels, during=None, lands_ms=0, lane=True):
        """One whole REST read: asked now, ``during`` runs while its answer is on its way, landing ``lands_ms`` after that."""
        read, go = await ask(feed, levels)
        if during is not None:
            await during()
        clock.ms += lands_ms
        return await land(feed, read, go, lane)

    def resting(price, order_id):  # a real resting 跟买1 of 100 shares, as the record holds it
        return {"maker": True, "side": "up", "price": price, "order": 100.0, "shares": 0.0, "market": SLUG, "status": "resting",
                "live": {"state": "open", "order_id": order_id}}

    async def rest_across_the_answer():
        """A REST read asked before Predict answered the order, landing after it: the levels of before the order."""
        feed = single_feed()
        before = {"bids": [["0.55", "150"], ["0.53", "80"]], "asks": ASKS}  # the 买1 our 100 are about to join: 150 of others
        order, asked = resting(0.55, "7"), clock.ms

        async def answered():  # Predict answers the order 400 ms after the read went out
            clock.ms += 400
            order["live"].update(sent_at=clock.ms - 300, answered_at=clock.ms)
        bk = await rest_read(feed, lambda: before, answered, 600)
        assert bk.fetched_ms == asked + 1000 > order["live"]["answered_at"] and not bk.stale(clock.ms)  # its age: its landing's, as before
        assert m.own_on_book(order, bk) == 0.0, \
            "a REST read asked before Predict's answer holds the levels of before the order, whenever it lands"
        assert m.maker_level(bk, "up", 100, m.own_on_book(order, bk), 0.55) == (0.55, 150.0)
        # the same levels pushed again after the answer, and the heartbeat: the age restarts, the levels are no newer
        stream = feed.stream
        stream.subscribed = {"101": ("HSI", False)}
        clock.ms += 200
        stream.book("101", {"version": 3, **before})
        bk = feed.books["HSI"]
        assert stream.repeated == 1 and bk.fetched_ms == clock.ms and m.own_on_book(order, bk) == 0.0, \
            "the old levels pushed again are not a book read after the answer"
        # the read asked after the answer shows the order: 100 of the 250 are ours again, 150 of others still make the 买1
        clock.ms += 50
        bk = await rest_read(feed, lambda: {"bids": [["0.55", "250"], ["0.53", "80"]], "asks": ASKS}, None, 300)
        assert m.own_on_book(order, bk) == 100.0 and m.maker_level(bk, "up", 100, 100.0, 0.55) == (0.55, 150.0)

    async def pushes_across_the_answer():
        """A pushed market: the push of before the order, then re-sends and heartbeats after Predict's answer."""
        feed = single_feed()
        feed.books["HSI"] = m.PredictBook("HSI", SLUG, "101", "HSI up?", lv([["0.55", "150"], ["0.53", "80"]]), lv(ASKS), clock.ms, 200)
        stream = feed.stream
        stream.subscribed = {"101": ("HSI", False)}
        clock.ms += 100
        levels = {"bids": [["0.55", "150"], ["0.53", "90"]], "asks": ASKS}
        stream.book("101", {"version": 3, **levels})  # a changed push: 150 of others at 55¢
        assert stream.books == 1 and "101" in stream.live
        clock.ms += 100
        order = resting(0.55, "8")
        order["live"].update(sent_at=clock.ms, answered_at=clock.ms + 300)
        clock.ms += 500  # 200 ms after Predict's answer: the server's re-send of the same levels, then its heartbeat
        stream.book("101", {"version": 3, **levels})
        clock.ms += 100
        stream.touch()
        bk = feed.books["HSI"]
        assert stream.repeated == 1 and bk.fetched_ms == clock.ms > order["live"]["answered_at"] and not bk.stale(clock.ms)  # its age: as before
        assert m.own_on_book(order, bk) == 0.0, "a re-send and a heartbeat restart the book's age, not the time its levels were taken"
        assert m.maker_level(bk, "up", 100, m.own_on_book(order, bk), 0.55) == (0.55, 150.0)
        clock.ms += 100
        stream.book("101", {"version": 4, "bids": [["0.55", "250"], ["0.53", "90"]], "asks": ASKS})  # the push that shows the order
        assert m.own_on_book(order, feed.books["HSI"]) == 100.0
        # a book that carries no content time of its own (built by hand) keeps its old stamp for it when pushed again
        feed = single_feed()
        feed.books["HSI"] = m.PredictBook("HSI", SLUG, "101", "HSI up?", lv(levels["bids"]), lv(ASKS), clock.ms, 200)
        feed.stream.subscribed = {"101": ("HSI", False)}
        order = resting(0.55, "9")
        order["live"].update(sent_at=clock.ms + 100, answered_at=clock.ms + 400)
        clock.ms += 600
        feed.stream.book("101", {"version": 1, **levels})
        assert feed.stream.repeated == 1 and feed.books["HSI"].fetched_ms == clock.ms and m.own_on_book(order, feed.books["HSI"]) == 0.0

    async def ladder_level():
        """A ladder level's book (ladder_row_read, and its topic's re-send): the same."""
        key, slug = "BTC-CAP", "what-market-cap-will-x-hit"
        market = {"id": "41", "conditionId": "c41", "title": "$200M", "question": ""}
        feed = m.PredictFeed(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}))
        feed.market_meta["41"] = ({"fee_bps": 200, "status": "REGISTERED"}, real_time.monotonic())
        before = {"bids": [["0.55", "150"]], "asks": ASKS}
        taken, go = slow(feed, lambda: before)
        asked = clock.ms
        read = asyncio.create_task(feed.ladder_row_read(key, slug, market))
        await taken.wait()
        clock.ms += 400
        order = {**resting(0.55, "5"), "market": f"{slug}#41"}
        order["live"].update(sent_at=clock.ms - 300, answered_at=clock.ms)
        clock.ms += 600
        go.set()
        row, _ = await read
        assert row.book.fetched_ms == asked + 1000 and m.own_on_book(order, row.book) == 0.0, \
            "a ladder level's REST read asked before Predict's answer"
        feed.ladder_rows[key] = {"41": row}
        feed.stream.subscribed = {"41": (key, True)}
        clock.ms += 200
        feed.stream.book("41", {"version": 2, **before})
        again = feed.ladder_rows[key]["41"].book
        assert feed.stream.repeated == 1 and again.fetched_ms == clock.ms and m.own_on_book(order, again) == 0.0

    async def rest_across_the_cancel():
        """The 1.104.6 ghost: a REST read asked before Predict confirmed the cancel, landing after it, still shows the order."""
        feed = single_feed()
        gone = {"t": {**resting(0.55, "7"), "status": "cancelled", "live": {"state": "done", "order_id": "7"}}}
        with_ghost = {"bids": [["0.55", "120"], ["0.53", "80"]], "asks": ASKS}  # 20 of others and our 100, cancelled a moment later

        async def confirmed():  # Predict confirms the cancel 100 ms after the read went out
            clock.ms += 100
            gone["t"]["live"]["cancel_done_ms"] = clock.ms
        bk = await rest_read(feed, lambda: with_ghost, confirmed, 300)
        own, price = m.Bot.sim_own_resting(gone, SLUG, "up", bk)
        assert bk.fetched_ms > gone["t"]["live"]["cancel_done_ms"] and (own, price) == (100.0, 0.55), \
            f"a REST read asked before the cancel's confirmation may still show the order, whenever it lands: {(own, price)}"
        assert m.maker_level(bk, "up", 100, own, price) is None  # 20 of others: no 买1 to join
        feed.stream.subscribed = {"101": ("HSI", False)}
        clock.ms += 200
        feed.stream.book("101", {"version": 2, **with_ghost})  # the same levels pushed again: still the ghost's
        assert feed.stream.repeated == 1 and m.Bot.sim_own_resting(gone, SLUG, "up", feed.books["HSI"]) == (100.0, 0.55)
        clock.ms += 50  # a read asked after the confirmation: the order is off the book, nothing of it is taken out
        bk = await rest_read(feed, lambda: {"bids": [["0.55", "150"], ["0.53", "80"]], "asks": ASKS}, None, 300)
        assert m.Bot.sim_own_resting(gone, SLUG, "up", bk) == (0.0, None) and m.maker_level(bk, "up", 100, 0.0, None) == (0.55, 150.0)

    PAPER = {"TELEGRAM_BOT_TOKEN": "1:x", "SIM_WAYS": "maker", "SIM_MAKER_POINTS": "off", "SIM_MAKER_SESSION": "off",
             "SIM_MAKER_EXIT_CENTS": "0", "SIM_MAKER_DEEP_CENTS": "10"}

    def deep_order(bot, price, at, **more):  # a real deep-bid order sent at ``at`` and answered 400 ms later
        return {"market": SLUG, "side": "up", "maker": True, "price": price, "order": 100.0, "shares": 0.0, "status": "resting",
                "deep": True, "settle": SETTLE, "kind": "close", "key": "HSI", "version": bot.sim_version(),
                "live": {"state": "open", "order_id": "7", "sent_at": at, "answered_at": at + 400}, **more}

    async def deep_bid_is_not_its_own_front():
        """A deep-bid order on a book that may show the order itself (taken before Predict's answer): never moved up, and
        not moved at all while that book shows a bid at its price."""
        bot = m.Bot(m.Config.from_env({**PAPER, "SIM_MAKER_FOLLOW_SETTLE_SECONDS": "0"}), m.Store(":memory:"), FM(), None)
        T = clock.ms

        def why(trade, bids, at, fair=0.70, bot=bot, ask="0.60"):  # the re-quote reason on a book whose levels were taken at ``at``
            book = m.PredictBook("HSI", SLUG, "101", "t", lv(bids), lv([[ask, "300"]]), at, None)
            return bot.sim_requote_reason(trade, card(book, fair), T + 450)
        deep = deep_order(bot, 0.31, T)             # sent at T, answered at T + 400
        ours = [["0.31", "100"], ["0.30", "500"]]   # our 100 at 31¢, a cent over the others' 30¢
        topped = [["0.33", "50"], *ours]            # ...and somebody's 50 at 33¢ in front of them
        assert why(deep, ours, T + 300) == "", "pushed 100 ms before Predict's answer: the bid 'in front' is the order itself"
        assert why({**deep, "live": {"state": "placing", "sent_at": T}}, ours, T + 100) == "", "the request still in flight"
        assert why(deep, topped, T + 300) == ""     # held on such a book whatever it shows
        assert why(deep, ours, T + 410) == ""       # a book taken after the answer: the 100 are ours, nothing is in front
        assert why(deep, topped, T + 410) == "买1 被顶到 33.0¢，改挂 34.0¢"  # ...and a real bid in front is followed, as before
        # a move down on a book with no bid at the order's price goes as before: the order is not on that book, nothing on
        # it can be mistaken for it (dust under SIM_MAKER_FRONT_MIN at its price is nobody's bid to the rule either)
        assert why(deep, [["0.25", "500"]], T + 300) == "买1 移到 25.0¢，改挂 26.0¢"
        assert why(deep, [["0.31", "0.5"], ["0.25", "500"]], T + 300) == "买1 移到 25.0¢，改挂 26.0¢"
        # ...but not off its own bid (the review's finding). 50 shares — fewer than SIM_MAKER_MIN_BID: a smaller SIM_SHARES, an
        # order a limit cut — with the best ask a cent over them: taken for somebody's, they leave "no room in front", and
        # the order was moved down to join the valid 买1 under it (placed again where it was, and withdrawn again)
        small = {**deep, "order": 50.0}
        own50 = [["0.31", "50"], ["0.30", "500"]]
        assert why(small, own50, T + 300, ask="0.32") == "", "a deep-bid order moved down off its own bid (pushed before Predict's answer)"
        assert why({**small, "live": {"state": "placing", "sent_at": T}}, own50, T + 100, ask="0.32") == "", "...the request still in flight"
        assert why({**small, "order": 64.0}, [["0.31", "64"], ["0.30", "500"]], T + 300, ask="0.32") == ""  # (cut by LIVE_MAX_ORDER_USD=20)
        assert why(small, own50, T + 410, ask="0.32") == ""  # a book taken after the answer: the 50 are ours, a cent over the others
        left = [["0.31", "50"], ["0.25", "500"]]    # the others gone from 30¢ to 25¢, our 50 where they were
        assert why(small, left, T + 300, ask="0.32") == "", "moved on a book that cannot tell the order from somebody's bid"
        assert why(small, left, T + 410, ask="0.32") == "买1 移到 25.0¢，改挂 26.0¢"  # ...the book that can: followed down, as before
        unsent = [["0.33", "50"], ["0.30", "500"]]  # an order not sent yet is on no book: judged as before (the send-time re-check)
        assert why(deep, unsent, T + 300) == ""     # (sent and answered: never up on a book taken before the answer, its bid on it or not)
        assert why({**deep, "live": {"state": "pending"}}, unsent, T + 300) == "买1 被顶到 33.0¢，改挂 34.0¢"
        assert why({k: v for k, v in deep.items() if k != "live"}, unsent, T + 300) == "买1 被顶到 33.0¢，改挂 34.0¢"  # paper: unchanged
        # a sweep order (SIM_MAKER_DEEP_ONLY) the same: "the bid under the threshold" may be the order itself. The front at
        # 50¢ (no edge there at a fair price of 55¢), our 100 at 20¢, the others' 19¢
        sbot = m.Bot(m.Config.from_env({**PAPER, "SIM_MAKER_DEEP_ONLY": "on", "SIM_MAKER_FOLLOW_SETTLE_SECONDS": "0"}),
                     m.Store(":memory:"), FM(), None)
        sweep = deep_order(sbot, 0.20, T, sweep=True)
        under = [["0.50", "300"], ["0.20", "100"], ["0.19", "500"]]
        assert why(sweep, under, T + 300, 0.55, sbot) == "", "a sweep order re-quoted over itself"
        assert why(sweep, under, T + 410, 0.55, sbot) == ""
        assert why(sweep, [under[0], ["0.22", "50"], *under[1:]], T + 410, 0.55, sbot) == "下面的买单变了，击穿单改挂 23.0¢"
        # ...and not re-placed where it is as "the front" either: the best bid, an ask a cent over it — its own 100 read as the 买1
        alone = [["0.20", "100"], ["0.19", "500"]]
        assert why(sweep, alone, T + 300, 0.55, sbot, ask="0.21") == "", "a sweep order withdrawn to be placed 'over' its own bid"
        assert why(sweep, alone, T + 410, 0.55, sbot, ask="0.21") == "盘口离公平价远了，击穿单改挂到买1 之上 1¢（20.0¢）"  # the others' 19¢: as before

    async def deep_bid_through_the_settle():
        """The real stream and the default follow settle (2 s): our deep-bid order's own push beats Predict's answer, the
        server re-sends it every second — and the look at the settle's end still does not move the order over itself."""
        bot = m.Bot(m.Config.from_env(PAPER), m.Store(":memory:"), FM(), None)
        feed, stream = bot.predict, bot.predict.stream
        feed.on_update = None  # the looks below are made by hand
        feed.books["HSI"] = m.PredictBook("HSI", SLUG, "101", "t", lv([["0.30", "500"]]), lv([["0.60", "300"]]), clock.ms, None)
        stream.subscribed = {"101": ("HSI", False)}
        deep = deep_order(bot, 0.31, clock.ms)
        ours = {"bids": [["0.31", "100"], ["0.30", "500"]], "asks": [["0.60", "300"]]}
        clock.ms += 150
        stream.book("101", {"version": 2, **ours})  # the order on the book, pushed 250 ms before the HTTP answer
        clock.ms += 270
        assert bot.sim_requote_reason(deep, card(feed.books["HSI"]), clock.ms) == ""
        for _ in range(2):
            clock.ms += 1000
            stream.book("101", {"version": 2, **ours})
        clock.ms += 250  # 2.25 s after the first look: past the settle it would have started
        assert stream.repeated == 2 and feed.books["HSI"].fetched_ms > deep["live"]["answered_at"]
        assert bot.sim_requote_reason(deep, card(feed.books["HSI"]), clock.ms) == "", "the settle's look moved the order over its own push"
        # somebody's 50 at 33¢: a changed push, taken after the answer — followed once it has held the settle
        stream.book("101", {"version": 3, "bids": [["0.33", "50"], *ours["bids"]], "asks": ours["asks"]})
        assert m.own_on_book(deep, feed.books["HSI"]) == 100.0 and bot.sim_requote_reason(deep, card(feed.books["HSI"]), clock.ms) == ""
        clock.ms += 2100
        assert bot.sim_requote_reason(deep, card(feed.books["HSI"]), clock.ms) == "买1 被顶到 33.0¢，改挂 34.0¢"
        # the same with 50 shares under SIM_MAKER_MIN_BID 100 and the best ask a cent over them: the order's own push read as
        # "somebody's 31¢, no room in front" moved it DOWN to the 30¢ level — a move the re-sends used to end inside the settle
        # (the order's shares counted again), and which the levels' own time, never restarted, let through at its end
        bot = m.Bot(m.Config.from_env({**PAPER, "SIM_SHARES": "50"}), m.Store(":memory:"), FM(), None)
        feed, stream = bot.predict, bot.predict.stream
        feed.on_update = None
        tight = [["0.32", "300"]]
        feed.books["HSI"] = m.PredictBook("HSI", SLUG, "101", "t", lv([["0.30", "500"]]), lv(tight), clock.ms, None)
        stream.subscribed = {"101": ("HSI", False)}
        small = deep_order(bot, 0.31, clock.ms, order=50.0)
        ours = {"bids": [["0.31", "50"], ["0.30", "500"]], "asks": tight}
        clock.ms += 150
        stream.book("101", {"version": 2, **ours})  # pushed 250 ms before the HTTP answer
        clock.ms += 270
        assert bot.sim_requote_reason(small, card(feed.books["HSI"]), clock.ms) == "" and not bot.sim_follow_since
        for _ in range(2):
            clock.ms += 1000
            stream.book("101", {"version": 2, **ours})
        clock.ms += 250
        assert stream.repeated == 2 and m.own_on_book(small, feed.books["HSI"]) == 0.0 and feed.books["HSI"].fetched_ms > small["live"]["answered_at"]
        assert bot.sim_requote_reason(small, card(feed.books["HSI"]), clock.ms) == "", "the settle's look moved the order down off its own push"
        # the others gone from 30¢ to 25¢: a changed push, taken after the answer — the order follows them down once that has held
        stream.book("101", {"version": 3, "bids": [["0.31", "50"], ["0.25", "500"]], "asks": tight})
        assert m.own_on_book(small, feed.books["HSI"]) == 50.0 and bot.sim_requote_reason(small, card(feed.books["HSI"]), clock.ms) == ""
        clock.ms += 2100
        assert bot.sim_requote_reason(small, card(feed.books["HSI"]), clock.ms) == "买1 移到 25.0¢，改挂 26.0¢"

    async def deep_bid_not_onto_its_ghost():
        """The other side of the same doubt: the deep-bid order that takes a cancelled one's place. On a book taken before
        the cancel's confirmation and heard of since its request went out, the bid at the cancelled order's price is its
        ghost or somebody's — not placed onto."""
        bot = m.Bot(m.Config.from_env({**PAPER, "SIM_MAKER_FLOOR_SHARES": "0"}), m.Store(":memory:"), FM(), None)
        T, tid = clock.ms, f"{SLUG}|up|挂"
        # a real deep-bid order of 100 at 31¢: its cancel sent at T − 140, confirmed at T (the books' clock: cancel_done_ms)
        gone = {"market": SLUG, "side": "up", "maker": True, "price": 0.31, "order": 100.0, "shares": 0.0, "status": "cancelled", "deep": True,
                "settle": SETTLE, "kind": "close", "key": "HSI", "version": bot.sim_version(),
                "live": {"state": "done", "order_id": "7", "sent_at": T - 9000, "answered_at": T - 8600,
                         "cancel_sent_at": T - 140, "cancel_done_at": T, "cancel_done_ms": T}}

        def placed(bids, at, trades=None, fair=0.70, bot=bot, heard=None):
            """What the 涨 side gets on a book whose levels were taken at ``at`` (and last heard of at ``heard``)."""
            book = m.PredictBook("HSI", SLUG, "101", "t", lv(bids), lv(ASKS), heard or at, None, content_ms=at)
            notes: dict = {}
            bot.sim_maker_reason(card(book, fair), m.TradeMap(trades or {f"{tid}#1": gone}), T + 50, notes=notes)
            seen = notes["sides"]["up"]
            return (round(seen["cand"].price, 2), seen["cand"].front) if "cand" in seen else seen["why"]
        beside = [["0.31", "100"], ["0.30", "500"]]  # 100 at the cancelled order's 31¢: its ghost — or somebody's, the order off the book
        doubt = "刚撤的单（31.0¢）可能还在这份盘口上"
        assert doubt in str(placed(beside, T - 60)), \
            f"a deep-bid order placed onto a bid it cannot tell from its own ghost (a push in the cancel's flight): {placed(beside, T - 60)}"
        assert placed(beside, T + 10) == (0.32, True)  # a book taken after the confirmation: somebody's 100, a cent over them
        # a book last heard of before the cancel went out shows the order for certain: its 100 taken out, its place used at once
        # (a REST-polled market's earlier read: the same order placed again waits for no later one) — until that book is
        # heard of again (the same levels pushed again, a REST answer asked earlier landing in the cancel's flight)
        assert placed(beside, T - 500) == (0.31, True)
        assert doubt in str(placed(beside, T - 500, heard=T - 60)), "...landed, or pushed again, while the cancel was in flight"
        ahead = {**gone, "live": {**gone["live"], "cancel_sent_at": T + 5000 - 140, "cancel_done_at": T + 5000}}  # (the market clock 5 s ahead)
        assert placed(beside, T - 500, {f"{tid}#1": ahead}) == (0.31, True) and doubt in str(placed(beside, T - 60, {f"{tid}#1": ahead}))
        bare = {**gone, "live": {k: v for k, v in gone["live"].items() if k not in ("cancel_sent_at", "cancel_done_at")}}
        assert doubt in str(placed(beside, T - 500, {f"{tid}#1": bare}))  # a record without the cancel's times: in doubt
        assert placed([["0.30", "500"]], T - 60) == (0.31, True)  # nobody at its price on the book of the flight: placed at once, as before
        assert placed([["0.31", "0.5"], ["0.30", "500"]], T - 60) == (0.31, True)  # (dust there is nobody's bid to the rule)
        assert placed([["0.31", "250"], ["0.30", "500"]], T - 60) == (0.32, True)  # 150 besides the ghost's 100: somebody is there either way
        assert placed([["0.33", "50"], *beside], T - 60) == (0.34, True)  # a bid above: the place does not hang on the 31¢ level
        # an order resting in the slot is not a ghost: its own shares at its price are its own (the card's "可挂" beside it)
        resting = {**gone, "status": "resting", "live": {"state": "open", "order_id": "8", "sent_at": T - 9000, "answered_at": T - 8600}}
        assert placed(beside, T - 60, {tid: resting}) == (0.31, True)
        # an order that JOINS a level is placed as before: the ghost's shares taken out, the others' 150 at 55¢ are a 买1 either way
        jbot = m.Bot(m.Config.from_env({k: v for k, v in PAPER.items() if k != "SIM_MAKER_DEEP_CENTS"}), m.Store(":memory:"), FM(), None)
        joined = {f"{tid}#1": {**gone, "price": 0.55, "deep": False, "version": jbot.sim_version()}}
        assert placed([["0.55", "250"], ["0.53", "80"]], T - 60, joined, bot=jbot) == (0.55, False)
        # a sweep order (SIM_MAKER_DEEP_ONLY) the same, a cent over "the bid under the threshold": the front at 50¢ (no edge
        # there at a fair price of 55¢), 100 at the cancelled sweep order's 20¢, the others' 19¢
        sbot = m.Bot(m.Config.from_env({**PAPER, "SIM_MAKER_DEEP_ONLY": "on", "SIM_MAKER_FLOOR_SHARES": "0"}), m.Store(":memory:"), FM(), None)
        swept = {f"{tid}#1": {**gone, "price": 0.20, "sweep": True, "version": sbot.sim_version()}}
        under = [["0.50", "300"], ["0.20", "100"], ["0.19", "500"]]
        assert "刚撤的单（20.0¢）" in str(placed(under, T - 60, swept, 0.55, sbot)), "a sweep order placed onto a bid it cannot tell from its ghost"
        assert placed(under, T + 10, swept, 0.55, sbot) == (0.21, True)
        assert placed([under[0], under[2]], T - 60, swept, 0.55, sbot) == (0.20, True)

    # the real trader as tests/test_trading.py sets it up (the key is the SDK test suite's, never a real one), makers only;
    # SIM_MAKER_MIN_BID (100), SIM_SHARES (100), SIM_MAKER_FOLLOW_SETTLE_SECONDS (2) and the floor orders at their defaults
    LIVE = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
            "SIM_WAYS": "maker", "SIM_MARKETS": "all", "LIVE": "on", "PREDICT_PRIVATE_KEY": "0x" + "a" * 64, "PREDICT_API_KEY": "k",
            "SIM_MAKER_EXIT_CENTS": "0", "SIM_MAKER_SESSION": "off", "SIM_MAKER_POINTS": "off", "PREDICT_WS": "off",
            "LIVE_MAX_ORDER_USD": "100", "LIVE_MAX_OPEN_USD": "250", "LIVE_MAX_DAILY_LOSS_USD": "50"}

    async def live_bot(others, asks=None, **env):  # ...on the real feed: a REST-polled market, the card's fair price 70¢
        server = Server(others, asks)
        bot = L.LiveBot(m.Config.from_env({**LIVE, **env}), m.Store(":memory:"), FM(), None)
        bot.live.api, bot.live.chain = Api(server), Chain()
        # the lane's own timers (a follow's settle, a slot's cooldown) run on the event loop's clock, not on the test's: off, and
        # the lane looks in their place where a part lets time pass (lane, live_on)
        bot.sim_follow_wake = bot.sim_cooling_wake = lambda *args, **kwargs: None
        bot.live.api.markets["101"] = {"id": 101, "status": "REGISTERED", "isNegRisk": False, "isYieldBearing": False, "feeRateBps": 200,
                                       "conditionId": "0x" + "ab" * 32, "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": "1011"},
                                                                                    {"name": "Down", "indexSet": 2, "onChainId": "1012"}]}
        feed = single_feed(bot.predict)
        bot.sim_markets = lambda now_ms, only=None: [card(feed.books["HSI"])] if "HSI" in feed.books else []
        await bot.live_prepare()
        await bot.live_prefetch(clock.ms)
        assert bot.live.ready, bot.live.ready_error
        return bot, server

    async def step(bot):
        bot.sim_ran = -1e9
        await bot.sim_step(clock.ms)

    async def live_order_not_withdrawn():
        """End to end, the defaults: a REST read in flight across the order's answer no longer withdraws the order."""
        bot, server = await live_bot({0.55: 150.0, 0.53: 80.0})
        api, feed, tid = bot.live.api, bot.predict, f"{SLUG}|up|挂"
        await rest_read(feed, server.levels, lane=False)  # 买1 55¢ × 150 of others

        async def placed():  # 50 ms after the read went out the step joins the 55¢ level with a real order, answered at once
            clock.ms += 50
            await step(bot)
            assert api.calls == [("create", 0.55)] and bot.sim_trades()[tid]["live"]["state"] == "open", api.calls
        clock.ms += 1000
        bk = await rest_read(feed, server.levels, placed, 300)  # ...and lands 300 ms after Predict's answer: the lane looks at once
        rec = bot.sim_trades()[tid]
        assert api.calls == [("create", 0.55)] and rec["status"] == "resting" and rec["price"] == 0.55 and rec["live"]["state"] == "open", \
            f"the order just placed was withdrawn on a book that cannot show it: {api.calls}"
        assert float(bk.bids[0][1]) == 150 and bk.fetched_ms > rec["live"]["answered_at"]  # (the levels of before the order, landed after it)
        clock.ms += 50  # the read asked after the answer: 250 at 55¢, 100 of them ours — counted again; 150 of others keep the 买1
        bk = await rest_read(feed, server.levels, None, 300)
        assert float(bk.bids[0][1]) == 250 and m.own_on_book(rec, bk) == 100.0 and api.calls == [("create", 0.55)]

    async def live_ghost_not_joined():
        """End to end: a REST read in flight across a cancel's confirmation shows the cancelled order — not a 买1 to join."""
        bot, server = await live_bot({0.55: 300.0, 0.53: 80.0}, SIM_MAKER_FLOOR_SHARES="0", SIM_MAKER_FOLLOW_SETTLE_SECONDS="0")
        api, feed, tid = bot.live.api, bot.predict, f"{SLUG}|up|挂"
        await rest_read(feed, server.levels, lane=False)
        clock.ms += 50
        await step(bot)  # our 100 join the 300 at 55¢
        assert api.calls == [("create", 0.55)]
        clock.ms += 1000
        server.others[0.55] = 20.0  # the others leave: 20 and our 100
        await rest_read(feed, server.levels, None, 300)
        flying = []

        async def cancel_goes_out():  # the market's next REST read goes out as the cancel does: Predict still has the order
            flying.append(await ask(feed, server.levels))
        api.on_remove = cancel_goes_out
        clock.ms += 1000
        await step(bot)  # "买1 不足": withdrawn, the cancel confirmed, the slot looked at again — nothing to join
        trades = bot.sim_trades()
        assert api.calls == [("create", 0.55), ("remove", ["101"])] and tid not in trades, api.calls
        done = trades[f"{tid}#1"]["live"]["cancel_done_ms"]
        assert done == clock.ms
        clock.ms += 300
        bk = await land(feed, *flying[0])  # the read lands 300 ms after the confirmation: the lane looks at once
        assert float(bk.bids[0][1]) == 120 and bk.fetched_ms > done  # 20 of others and the ghost, stamped after the confirmation
        await step(bot)  # ...and so does the step
        assert api.calls == [("create", 0.55), ("remove", ["101"])] and tid not in bot.sim_trades(), \
            f"a real order joined its own ghost — 20 shares of others under SIM_MAKER_MIN_BID 100: {api.calls}"
        server.others[0.55] = 150.0  # others back at 55¢, on a read asked after the cancel: joined again
        clock.ms += 1000
        await rest_read(feed, server.levels, None, 300)
        await step(bot)
        assert api.calls == [("create", 0.55), ("remove", ["101"]), ("create", 0.55)], api.calls
        assert bot.sim_trades()[tid]["status"] == "resting"

    async def live_deep_bid_holds():
        """End to end, the deep-bid mode on a pushed market: the push that shows the new order beats Predict's HTTP answer."""
        bot, server = await live_bot({0.30: 500.0}, SIM_MAKER_DEEP_CENTS="10", SIM_MAKER_FLOOR_SHARES="0",
                                     SIM_MAKER_FOLLOW_SETTLE_SECONDS="0")
        api, feed, tid = bot.live.api, bot.predict, f"{SLUG}|up|挂"
        await rest_read(feed, server.levels, lane=False)  # 30¢ × 500 of others under a 58¢ ask: a cent in front has the edge
        feed.stream.subscribed = {"101": ("HSI", False)}

        async def pushed():  # the book with the order on it arrives 100 ms after the request, 200 ms before its HTTP answer
            clock.pass_ms(100)
            feed.stream.book("101", {"version": 2, **server.levels()})
            await looked(feed)  # the lane looks at once, the request still in flight
            clock.pass_ms(200)
        api.on_create = pushed
        clock.ms += 50
        await step(bot)
        rec = bot.sim_trades()[tid]
        assert api.calls == [("create", 0.31)] and rec["deep"] and rec["status"] == "resting" and rec["live"]["state"] == "open", api.calls
        assert feed.books["HSI"].bids[0] == (D("0.31"), D("100")) and feed.books["HSI"].fetched_ms < rec["live"]["answered_at"]
        clock.ms += 50
        await step(bot)  # the look after the answer, on that book: the 100 at 31¢ "in front" are the order itself
        assert api.calls == [("create", 0.31)] and bot.sim_trades()[tid] is rec and rec["status"] == "resting", \
            f"a deep-bid order withdrawn to be placed over its own push: {api.calls}"
        server.others[0.33] = 50.0  # somebody's 50 at 33¢, pushed after the answer: a real bid in front, followed as before
        clock.ms += 1000
        feed.stream.book("101", {"version": 3, **server.levels()})
        await looked(feed)
        await step(bot)
        assert api.calls == [("create", 0.31), ("remove", ["101"]), ("create", 0.34)], api.calls

    async def lane(bot):  # the express lane's look at the market now, as a follow's settle or a slot's cooldown starts it by its timer
        bot.predict.updated("HSI")
        await looked(bot.predict)

    def pushing(feed, server):
        """Predict's stream for the market: push() sends its book as it stands now (the real PredictStream applies it when
        the levels changed, restarts its age when not); push.sent is what it sent last."""
        feed.stream.subscribed = {"101": ("HSI", False)}
        version = [1]

        def push():
            version[0], push.sent = version[0] + 1, server.levels()
            feed.stream.book("101", {"version": version[0], **push.sent})
        push.sent = server.levels()  # (the book the feed holds: the REST read's)
        return push

    async def live_on(bot, server, seconds, push=None):
        """``seconds`` of the market's life, a tenth of a second at a time: the stream sends the book when it has changed and
        again every second (``push``; a REST-polled market has none), the express lane looks when a follow's settle or a
        slot's cooldown has run out (in the place of the bot's own timers: live_bot), the step runs every five."""
        feed, settle = bot.predict, bot.config.sim_maker_follow_settle * 1000
        for tick in range(1, seconds * 10 + 1):
            clock.pass_ms(100)
            if push is not None and (server.levels() != push.sent or tick % 10 == 0):
                push()
                await looked(feed)
            cooled = [tid for tid, (at, _) in bot.sim_express_withdrawn.items() if clock.monotonic() - at >= bot.SIM_EXPRESS_COOLDOWN_SECONDS + 0.1]
            if cooled or any(clock.ms - since >= settle + 100 for _, since in bot.sim_follow_since.values()):
                await lane(bot)
                for tid in cooled:
                    bot.sim_express_withdrawn.pop(tid, None)
            if tick % 50 == 0:
                await step(bot)

    SMALL = {"SIM_MAKER_DEEP_CENTS": "10", "SIM_MAKER_FLOOR_SHARES": "0", "SIM_SHARES": "50"}  # the settle (2 s) and SIM_MAKER_MIN_BID (100) at their defaults

    async def live_small_deep_bid_holds():
        """End to end, a pushed market, the review's finding: a deep-bid order of 50 shares under an ask a cent away, every
        order's own push beating its HTTP answer — one order, no cancel, where there were 24 of each a minute."""
        bot, server = await live_bot({0.30: 500.0}, asks=[["0.32", "300"]], **SMALL)
        api, feed, tid = bot.live.api, bot.predict, f"{SLUG}|up|挂"
        await rest_read(feed, server.levels, lane=False)  # 30¢ × 500 of others under a 32¢ ask: a cent in front, 31¢, has the edge
        push = pushing(feed, server)

        async def pushed():  # the book with the order on it arrives 100 ms after the request, 200 ms before its HTTP answer
            clock.pass_ms(100)
            busy = bool(feed.express_tasks)  # (an order sent from inside an express look cannot wait for that look's end)
            push()
            if not busy:
                await looked(feed)
            clock.pass_ms(200)
        pushed.keep = True
        api.on_create = pushed
        clock.ms += 50
        await step(bot)
        rec = bot.sim_trades()[tid]
        assert api.calls == [("create", 0.31)] and rec["order"] == 50.0 and rec["deep"] and rec["live"]["state"] == "open", api.calls
        assert feed.books["HSI"].bids[0] == (D("0.31"), D("50")) and m.own_unsure(rec, feed.books["HSI"])  # its own push, taken before the answer
        await live_on(bot, server, 12, push)  # the re-sends every second, the settle's timer, the cooldown's, two steps
        assert api.calls == [("create", 0.31)] and bot.sim_trades()[tid] is rec and rec["status"] == "resting", \
            f"a deep-bid order under SIM_MAKER_MIN_BID shares withdrawn and placed again, over and over, off its own push: {api.calls}"
        assert m.own_unsure(rec, feed.books["HSI"]) and feed.books["HSI"].fetched_ms > rec["live"]["answered_at"] and not bot.sim_follow_since
        # the others' 30¢ gone to 25¢: a changed push, its levels taken after the answer — the order is told from the others
        # again and follows them down once that has held the settle (the cancel, then at once the order a cent over 25¢)
        server.others = {0.25: 500.0}
        await live_on(bot, server, 3, push)
        assert api.calls == [("create", 0.31), ("remove", ["101"]), ("create", 0.26)], api.calls
        await live_on(bot, server, 6, push)  # ...and that one, its own push before its answer again, rests
        assert api.calls == [("create", 0.31), ("remove", ["101"]), ("create", 0.26)] and bot.sim_trades()[tid]["status"] == "resting", api.calls

    async def live_small_deep_bid_rest():
        """The same on a REST-polled market: a read asked while the order's request is in flight is served after the order
        went in, and lands after Predict's answer — its levels taken before the answer, the order's own 50 on them."""
        bot, server = await live_bot({0.30: 500.0}, asks=[["0.32", "300"]], **SMALL)
        api, feed, tid = bot.live.api, bot.predict, f"{SLUG}|up|挂"
        await rest_read(feed, server.levels, lane=False)
        flying = []

        async def in_flight():  # 100 ms after the order's request reached Predict a REST read of the market is served; the answer 200 ms later
            clock.pass_ms(100)
            flying.append(await ask(feed, server.levels))
            clock.pass_ms(200)
        api.on_create = in_flight
        clock.ms += 50
        await step(bot)
        rec = bot.sim_trades()[tid]
        assert api.calls == [("create", 0.31)] and rec["live"]["state"] == "open", api.calls
        clock.pass_ms(300)
        bk = await land(feed, *flying[0])  # the read lands 300 ms after the answer: the lane looks at once
        assert bk.bids[0] == (D("0.31"), D("50")) and bk.fetched_ms > rec["live"]["answered_at"] > m.book_content_ms(bk) and m.own_unsure(rec, bk)
        await live_on(bot, server, 8)  # the settle's timer and a step; the next read of the market is ten seconds away
        assert api.calls == [("create", 0.31)] and bot.sim_trades()[tid] is rec and rec["status"] == "resting", \
            f"a deep-bid order withdrawn off a REST read that already showed it: {api.calls}"
        bk = await rest_read(feed, server.levels, None, 300)  # the read asked after the answer: the 50 are ours, a cent over the others
        assert m.own_on_book(rec, bk) == 50.0
        await live_on(bot, server, 6)
        assert api.calls == [("create", 0.31)] and rec["status"] == "resting", api.calls

    async def live_same_price_on_an_earlier_read():
        """...and what the wait does not touch, end to end on a REST-polled market: a setting changes, the deep-bid order is
        withdrawn to be placed again at its price. The read in hand, landed before the cancel went out, shows the cancelled
        order for certain: its place is used at once — no waiting for the next read."""
        bot, server = await live_bot({0.30: 500.0}, SIM_MAKER_DEEP_CENTS="10", SIM_MAKER_FLOOR_SHARES="0")
        api, feed, tid = bot.live.api, bot.predict, f"{SLUG}|up|挂"
        await rest_read(feed, server.levels, lane=False)
        clock.ms += 50
        await step(bot)
        clock.ms += 1000
        bk = await rest_read(feed, server.levels, None, 300)  # the read asked after the answer: our 100 at 31¢, a cent over the others
        assert api.calls == [("create", 0.31)] and bk.bids[0] == (D("0.31"), D("100")) and m.own_on_book(bot.sim_trades()[tid], bk) == 100.0
        clock.ms += 2000
        bot.config = m.dataclasses.replace(bot.config, sim_shares=80.0)  # SIM_SHARES 100 → 80 (the control page)
        await step(bot)
        trades = bot.sim_trades()
        assert trades[f"{tid}#1"]["withdrawn"]["why"] == "每次份数已改为 80 份，按新份数重挂" and feed.books["HSI"] is bk  # (no read since)
        assert api.calls == [("create", 0.31), ("remove", ["101"]), ("create", 0.31)] and trades[tid]["order"] == 80.0, \
            f"the order's own place, on a read that shows the cancelled order for certain, was not used at once: {api.calls}"

    async def live_deep_bid_not_onto_its_ghost():
        """End to end, a pushed market, the placement's side of it: somebody joins our deep-bid order's price with as many
        shares, the order is withdrawn to go a cent over them — and the push of its cancel beats the confirmation."""
        bot, server = await live_bot({0.30: 500.0}, SIM_MAKER_DEEP_CENTS="10", SIM_MAKER_FLOOR_SHARES="0")
        api, feed, tid = bot.live.api, bot.predict, f"{SLUG}|up|挂"
        await rest_read(feed, server.levels, lane=False)  # 30¢ × 500 of others under a 58¢ ask
        push = pushing(feed, server)

        async def gone():  # the book without the order arrives 40 ms after the cancel reached Predict, 100 ms before its confirmation
            clock.pass_ms(40)
            push()
        gone.keep = True
        api.on_removed = gone
        clock.ms += 50
        await step(bot)  # our 100 at 31¢, answered at once; their push follows (its levels taken after the answer)
        await live_on(bot, server, 2, push)
        rec = bot.sim_trades()[tid]
        assert api.calls == [("create", 0.31)] and m.own_on_book(rec, feed.books["HSI"]) == 100.0
        server.others[0.31] = 100.0  # somebody's 100 beside ours
        await live_on(bot, server, 3, push)  # ...held the settle: the order is withdrawn to go over them
        trades = bot.sim_trades()
        assert api.calls == [("create", 0.31), ("remove", ["101"])] and tid not in trades, \
            f"placed back beside somebody's bid, taken for its own ghost: {api.calls}"
        done = trades[f"{tid}#1"]["live"]["cancel_done_ms"]
        bk = feed.books["HSI"]
        assert trades[f"{tid}#1"]["withdrawn"]["why"] == "买1 被顶到 31.0¢，改挂 32.0¢" and bk.bids[0] == (D("0.31"), D("100")) and m.book_content_ms(bk) < done
        await live_on(bot, server, 9, push)  # the cooldown's end, the re-sends, a step: that book cannot tell — the side waits
        assert api.calls == [("create", 0.31), ("remove", ["101"])] and tid not in bot.sim_trades(), \
            f"placed back beside somebody's bid, withdrawn, and again: {api.calls}"
        why = bot.sim_why(card(feed.books["HSI"]), bot.sim_trades(), clock.ms)["maker"]["sides"][0]["text"]
        assert "刚撤的单（31.0¢）可能还在这份盘口上" in why, why  # ...and the card says what for
        await rest_read(feed, server.levels, None, 200)  # the cold round's read, asked after the confirmation: the 100 are somebody's
        assert api.calls == [("create", 0.31), ("remove", ["101"]), ("create", 0.32)], api.calls
        await live_on(bot, server, 6, push)
        assert api.calls == [("create", 0.31), ("remove", ["101"]), ("create", 0.32)] and bot.sim_trades()[tid]["status"] == "resting", api.calls

    clock = Clock(BJ(10, 5, 10, 0))
    m.time = L.time = clock
    try:
        for part in (rest_across_the_answer, pushes_across_the_answer, ladder_level, rest_across_the_cancel,
                     deep_bid_is_not_its_own_front, deep_bid_through_the_settle, deep_bid_not_onto_its_ghost, live_order_not_withdrawn,
                     live_ghost_not_joined, live_deep_bid_holds, live_small_deep_bid_holds, live_small_deep_bid_rest,
                     live_same_price_on_an_earlier_read, live_deep_bid_not_onto_its_ghost):
            await part()
    finally:
        m.time = L.time = real_time


asyncio.run(feed_fetched_ms())


# FEED-CLOSE-BOOK-ORIENT: a daily index / stock card ("close") took Predict's book — which prices the market's FIRST
# outcome — for 涨 just as it came, while the real order's token and the result go by the outcome NAMES (every other
# kind turns its book by the names). No market has been listed Down (or No) first so far. On one, replayed here, the
# card was priced on the other outcome's book: model and market agreeing at 68¢ for 涨 read as a 38¢ edge, the paper
# trader bought "涨" at 30¢, and real BUYs of the Up token went out at 30¢ and 33¢ — nothing refused them. The card now
# holds such a book (by the names the feed reads with the fee), and a real order is refused on the trader's own read
# of the names whatever the feed knows.
async def feed_close_book_orient() -> None:
    import datetime as dt, math, statistics
    import live as L
    D, WEI, real_time = m.D, 10 ** 18, m.time
    BJ = lambda mo, d, h, mi=0: int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
    NOW, TARGET = BJ(10, 5, 10, 0), dt.date(2026, 10, 5)
    SLUG = m.predict_slug("hang-seng-index", TARGET)
    NOTE, HELD, REFUSED = "按名称第一个结果不是涨", "盘口方向未确认", "结果名称无法确认方向，不下真实单"

    def listing(names):  # Predict's market 101: indexSet 1 is the outcome its orderbook prices (token 1011), whatever the JSON's order
        return {"id": 101, "status": "REGISTERED", "isNegRisk": False, "isYieldBearing": False, "feeRateBps": 200,
                "conditionId": "0x" + "ab" * 32, "title": "HSI Up or Down", "question": "HSI up?",
                "outcomes": [{"name": names[1], "indexSet": 2, "onChainId": "1012"}, {"name": names[0], "indexSet": 1, "onChainId": "1011"}]}

    class Api:  # Predict as the real trader sees it (tests/test_trading.py's FakeApi, cut down): every order sent is kept
        def __init__(self, market):
            self.jwt, self.jwt_exp, self.auth_error, self.lookup = "t", 0, "", ""
            self.markets, self.open, self.sent, self.positions_rows, self.next_id = {"101": market}, {}, [], [], 100

        async def ensure_auth(self): pass

        async def market(self, mid): return self.markets[str(mid)]

        async def create_order(self, body):
            self.next_id += 1
            oid, data = str(self.next_id), body["data"]
            self.sent.append((data["order"]["tokenId"], data["order"]["side"], round(int(data["pricePerShare"]) / WEI, 2)))
            self.open[oid] = {"id": oid, "status": "OPEN", "amount": data["order"]["takerAmount"], "amountFilled": "0",
                              "strategy": data["strategy"], "order": {**data["order"]}}
            return {"order_id": oid, "hash": data["order"]["hash"], "code": None}

        async def remove_orders(self, ids):
            removed = [i for i in ids if self.open.pop(i, None) is not None]
            return {"removed": removed, "noop": [i for i in ids if i not in removed]}

        async def orders(self, status="OPEN", **params): return [dict(r) for r in self.open.values()] if status == "OPEN" else []

        async def order(self, oid, hash_=""): return dict(self.open[oid]) if oid in self.open else None

        async def positions(self): return self.positions_rows

    class Chain:
        async def account_owner(self, account): return "0x8fd379246834eac74B8419FfdA202CF8051F7A03"  # the test key's address
        async def usdt_balance(self, owner=None): return 1000 * WEI
        async def bnb_balance(self, owner=None): return 10 ** 17
        async def allowance(self, key, owner=None): return 10 ** 30

    class FM:  # the bot's market clock: the test's time
        config = None

        def now_ms(self): return NOW

    class Clock:  # main's and live's wall clock at the test's time too (as feed_fetched_ms): the books read below are stamped NOW
        def time(self): return (NOW + 0.5) / 1000

        def __getattr__(self, name): return getattr(real_time, name)

    # the model: 涨 68¢ (the cash index in session, a third of a percent over the reference close)
    spot = D(str(round(24600 * math.exp(statistics.NormalDist().inv_cdf(0.68) * 0.01 * math.sqrt(0.5)), 2)))
    odds = m.close_odds("恒生指数", D("24600"), spot, 0.01, 0.5, TARGET, D("0.01"), "10-02 收盘", "恒指现货（盘中直接用现货）", "σ")
    assert abs(odds.fair_up - 0.68) < 0.002, odds.fair_up

    ENV = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
           "SIM_WAYS": "both", "SIM_MARKETS": "close", "SIM_MAKER_MIN_BID": "0", "SIM_MAKER_EXIT_CENTS": "0", "SIM_MAKER_SESSION": "off",
           "SIM_MAKER_POINTS": "off", "SIM_MAKER_SPREAD_CENTS": "10", "SIM_TAKER_MAX_SHARES": "0", "SIM_TAKER_SWEEP_SHARES": "0",
           "SIM_MAKER_FOLLOW_SETTLE_SECONDS": "0", "PREDICT_GRAPHQL": "off", "PREDICT_WS": "off"}
    LIVE = {"LIVE": "on", "PREDICT_PRIVATE_KEY": "0x" + "a" * 64, "PREDICT_API_KEY": "k",  # the SDK test suite's key, never a real one
            "LIVE_MAX_ORDER_USD": "100", "LIVE_MAX_OPEN_USD": "250", "LIVE_MAX_DAILY_LOSS_USD": "50"}

    async def world(names, live=True, details=True):
        """The bot on Predict's HSI market listed with ``names`` (first = the outcome the book prices), whose book shows
        30¢ / 33¢: read by the real feed round (the category, the market's details with the fee, the orderbook; with
        ``details`` off the details read fails, as a best-effort read may), the card priced by the real Bot.sim_markets
        (only the model's odds are given), the real trader on a fake API and chain. (bot, feed, api, the feed's switch)."""
        market, switch, asked = listing(names), {"details": details}, []
        if live:
            bot = L.LiveBot(m.Config.from_env({**ENV, **LIVE}), m.Store(":memory:"), FM(), None)
            bot.live.api, bot.live.chain = Api(market), Chain()
        else:
            bot = m.Bot(m.Config.from_env(ENV), m.Store(":memory:"), FM(), None)
        bot.odds_items = lambda now_ms, only=None: [("恒生指数", odds)]
        feed = bot.predict

        async def fetch(url, payload=None):
            path = url.removeprefix(m.PREDICT_REST)
            asked.append(path)
            if path == f"/categories/{SLUG}":
                return {"success": True, "data": {"slug": SLUG, "markets": [market]}}
            if path == "/markets/101/orderbook":
                return {"success": True, "data": {"bids": [["0.30", "500"]], "asks": [["0.33", "500"]]}}
            if path == "/markets/101" and switch["details"]:
                return {"success": True, "data": market}
            raise m.RemoteError(f"HTTP 500: {path}", 0)
        feed.fetch = fetch
        lane, feed.on_update = feed.on_update, None  # the looks below are made by hand: the express lane's, then the step's
        assert await feed.refresh({"HSI": SLUG}, force=True) is not False and not feed.errors, feed.errors
        feed.on_update = lane
        assert asked == [f"/categories/{SLUG}", "/markets/101", "/markets/101/orderbook"], asked  # no request for the names alone
        raw = feed.books["HSI"]
        assert (float(raw.bid[0]), float(raw.ask[0]), raw.fetched_ms, raw.fee_bps) == (0.30, 0.33, NOW, 200 if details else None), raw
        assert SLUG not in feed.want_info
        if live:
            await bot.live_prepare()
            await bot.live_prefetch(NOW)
            assert bot.live.ready, bot.live.ready_error
        return bot, feed, getattr(bot.live.api if live else None, "sent", None), switch

    async def look(bot):  # a book in (the express lane), then the trader's step
        await bot.sim_express("HSI")
        bot.sim_ran = -1e9
        await bot.sim_step(NOW)

    def card(bot):
        return [mk for mk in bot.sim_markets(NOW) if mk.kind == "close"]

    def shown(bot):  # what the card says of the book everywhere it is shown: the page, /book, /prob, the auction reminder, /calib's market price
        return (bot.predict_payload("恒生指数", odds, NOW), bot.cmd_book(None).text, bot.cmd_prob(None).text,
                bot.auction_text("收盘竞价", [("恒生指数", odds)], NOW), bot.predict_mid("HSI", SLUG, NOW))

    def outs(*names):  # a market's outcomes as the trader holds them (parse_market: in index order)
        return [{"name": name, "index_set": i + 1, "token": f"101{i + 1}"} for i, name in enumerate(names)]

    async def listed_up_first(details):
        """The listings there are (Up first), the feed having read the names or not: nothing changes — the card is priced,
        the orders go out for the Up token."""
        bot, feed, sent, _ = await world(("Up", "Down"), details=details)
        raw = feed.books["HSI"]
        assert (feed.market_meta["101"][0]["outcomes"] == ["Up", "Down"]) if details else ("101" not in feed.market_meta)
        assert bot.close_book("HSI", SLUG) == (raw, "") == bot.close_book("HSI") and bot.close_book("HSI", "another-day") == (None, "")
        (mk,) = card(bot)
        assert mk.book is raw and bot.live_room({}, mk, "up", 0.33, 100) == "" and bot.live_room({}, mk, "down", 0.70, 100) == ""
        payload, book_text, prob_text, auction, mid = shown(bot)
        assert payload["bids"] == [[0.30, 500.0]] and payload["error"] == "" and any(e["best"] for e in payload["edges"])
        assert "Predict 涨 买1 30.0¢" in book_text and "Predict 涨 买1 30.0¢" in prob_text and "Predict 买1 30.0¢" in auction
        assert "👉" in book_text and "👉" in auction and mid == 0.315
        await look(bot)
        assert sorted(sent) == [("1011", 0, 0.30), ("1011", 0, 0.33)], sent  # 挂涨 at the 买1, 吃涨 at the 卖1: BUYs of Up
        assert {t["label"] for t in bot.sim_trades().values()} == {"挂涨", "吃涨"}

    async def listed_down_first(names):
        """Listed Down (No) first, the feed having read the names: the book is the other outcome's — the card is held."""
        bot, feed, sent, _ = await world(names)
        assert feed.market_meta["101"][0]["outcomes"] == list(names)  # read with the fee, before the book was stored
        assert card(bot) == [], f"a book that prices the {names[0]} outcome was taken for 涨"
        payload, book_text, prob_text, auction, mid = shown(bot)
        why = payload["error"]
        assert HELD in why and NOTE in why and "、".join(names) in why, payload
        assert payload == {"url": m.predict_url(SLUG, bot.config.predict_ref), "error": why}, payload  # no depth, no edges: the page says why
        for text in (book_text, prob_text):
            assert f"Predict 盘口暂缺：{why}" in text and "Predict 涨 买1" not in text and "👉" not in text, text
        assert "Predict 买1" not in auction and "👉" not in auction and "模型 涨" in auction, auction
        assert mid is None, f"/calib would score the model against the other outcome's price: {mid}"
        await look(bot)
        assert sent == [] and bot.sim_trades() == {}, (sent, list(bot.sim_trades()))
        assert bot.close_book("HSI", SLUG) == (None, why) == bot.close_book("HSI") and bot.close_book("HSI", "another-day") == (None, "")

    async def names_unread_by_the_feed():
        """The feed's details read failed (best effort: the book is read all the same): the card is as before, and the
        real order is refused on the trader's own read of the market — until the feed's next read brings the names."""
        bot, feed, sent, switch = await world(("Down", "Up"), details=False)
        raw = feed.books["HSI"]
        assert "101" not in feed.market_meta and bot.close_book("HSI", SLUG) == (raw, "")
        (mk,) = card(bot)
        assert [o["name"] for o in bot.live.markets["101"].outcomes] == ["Down", "Up"]  # the trader's own read
        for side, price in (("up", 0.33), ("down", 0.70)):
            why = bot.live_room({}, mk, side, price, 100)
            assert REFUSED in why and NOTE in why, f"a real order priced on the other outcome's book was let through: {why!r}"
        await look(bot)
        assert sent == [], f"real orders for the Up token at the Down outcome's prices: {sent}"
        assert bot.sim_trades() == {}  # a refused decision is no paper position either
        blocks = bot.sim_blocks()
        assert {b["label"] for b in blocks} == {"挂涨", "吃涨"} and all(REFUSED in b["why"] and NOTE in b["why"] for b in blocks), blocks
        switch["details"] = True  # PREDICT_META_SECONDS later the details are read: the card is held from then on
        feed.fees["101"] = (feed.fees["101"][0], time.monotonic() - m.PREDICT_META_SECONDS - 1)
        feed.on_update = None
        assert await feed.refresh({"HSI": SLUG}, force=True) is not False
        assert feed.market_meta["101"][0]["outcomes"] == ["Down", "Up"] and card(bot) == [] and bot.close_book("HSI", SLUG)[0] is None

    async def on_paper(names, traded):
        """LIVE=off: no paper position at the other outcome's prices either (it would be settled by the name)."""
        bot, feed, _, _ = await world(names, live=False)
        await look(bot)
        assert {t["label"] for t in bot.sim_trades().values()} == traded, list(bot.sim_trades())

    # the trader's choice of the token: by the name, as before — and noted on a daily card whose book is the other outcome's
    up_first, down_first, no_first = outs("Up", "Down"), outs("Down", "Up"), outs("No", "Yes")
    for pair in (down_first, no_first):
        (up, up_note), (down, down_note) = L.outcome_for_side("close", "up", pair), L.outcome_for_side("close", "down", pair)
        assert up is pair[1] and down is pair[0] and NOTE in up_note and NOTE in down_note, (up_note, down_note)
        for kind in ("updown", "ladder", "range", "flip"):  # their books are turned by the names (the pre-open's pairs are bought at fixed prices)
            assert L.outcome_for_side(kind, "up", pair) == (pair[1], "") and L.outcome_for_side(kind, "down", pair) == (pair[0], "")
    assert L.outcome_for_side("close", "up", up_first) == (up_first[0], "") and L.outcome_for_side("close", "down", up_first) == (up_first[1], "")
    odd = outs("A", "B")  # names that say neither way: the order of the outcomes, noted as before (never a real order)
    assert L.outcome_for_side("close", "up", odd) == (odd[0], "按结果顺序推定（结果名称：a、b）")
    # the card's reading of the names is the trader's: held exactly where a real order would be refused for it
    assert all(m.second_outcome_up(pair) for pair in (["Down", "Up"], ["No", "Yes"], [" DOWN ", "up"], ["下跌", "上涨"], ["Down", "Flat"], ["Flat", "Up"]))
    assert not any(m.second_outcome_up(pair) for pair in (["Up", "Down"], ["Yes", "No"], ["Up", "Flat"], ["Flat", "Down"], ["A", "B"],
                                                          ["Up", "Up"], ["Down", "Down"], [], ["Down"], ["Down", "Up", "Flat"], None))
    words = ["Up", "Down", "Yes", "No", "涨", "跌", "上涨", "下跌", "Higher", "Lower", "Above", "Below", " up ", "DOWN", "Flat", "A", ""]
    for first in words:
        for second in words:
            noted = [NOTE in L.outcome_for_side("close", side, outs(first, second))[1] for side in ("up", "down")]
            assert noted == [m.second_outcome_up([first, second])] * 2, (first, second, noted)
    assert m.OUTCOME_UP_WORDS == L.UP_WORDS and m.OUTCOME_DOWN_WORDS == L.DOWN_WORDS

    m.time = L.time = Clock()
    try:
        await listed_up_first(True)
        await listed_up_first(False)
        await listed_down_first(("Down", "Up"))
        await listed_down_first(("No", "Yes"))
        await names_unread_by_the_feed()
        await on_paper(("Up", "Down"), {"挂涨", "吃涨"})
        await on_paper(("Down", "Up"), set())
    finally:
        m.time = L.time = real_time


asyncio.run(feed_close_book_orient())


# FEED-MARK-VS-LAST: after hours a contract card's fair price is "exchange close × Binance now ÷ Binance at the close".
# The quote is the last trade while that is fresh (120 s) and Binance's MARK price once it is not; the anchor was the
# closing minute's trade candle, its kind dropped. So a thin contract's mark was divided by a trade, and the gap between
# the two kinds at the close read as a move of the stock: the fair price jumped the moment the last trade turned 120 s
# old, nothing having traded, and the paper trader (the real one follows it) bought on the jump. A quote on the mark is
# now compared with the mark candle of that same minute, read beside the trade candle; until it is read the estimate is
# flagged and nothing is traded on it. (A closing minute without a trade has no trade candle: a fresh trade is still
# compared with its mark candle, as before, the pair now named — see Bot.contract_anchor.)
async def feed_mark_vs_last() -> None:
    import datetime as dt
    D = m.D
    SYM = "HK0625USDT"
    TITLE = f"{m.NAMES[SYM]}｜{SYM}"
    BJ = lambda day, hour, minute: int(dt.datetime(2026, 9, day, hour, minute, tzinfo=m.BEIJING).timestamp() * 1000)
    CLOSE, NOW = BJ(23, 16, 10), BJ(23, 20, 0)  # a Wednesday: the HK close, and an evening hour after it
    TRADES, MARKS = "/fapi/v1/klines", "/fapi/v1/markPriceKlines"

    class Binance(m.Binance):
        """The real class (price_at's choice of candle, its cache); only the REST rows are made up: the closing minute's
        trade candle (close, volume) and mark candle (None: the read fails)."""
        def __init__(self, config, trade=("38.02", "120"), mark="38.10"):
            super().__init__(config)
            self.now, self.trade, self.mark, self.asked = NOW, trade, mark, []

        def now_ms(self): return self.now

        async def sync_clock(self): pass

        async def get(self, path, **p):
            self.asked.append((path, p.get("startTime")))
            start = p["startTime"]
            if path == TRADES and p["interval"] == "1m":
                return [[start, "38.00", "38.05", "37.98", self.trade[0], self.trade[1], start + 59_999]]
            if path == MARKS and self.mark is not None:
                return [[start, "38.05", "38.12", "38.04", self.mark, "0", start + 59_999]]
            raise m.RemoteError(f"HTTP 500: {path}")

    async def world(**candles):
        """The bot under its default settings on the evening of 09-23: the stock closed at 38.00 HKD at 16:10, the anchors
        read by the real refresh_odds_inputs → Binance.price_at."""
        cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "ADMIN_USER_ID": "42", "STATE_DB": ":memory:"})
        bot = m.Bot(cfg, m.Store(":memory:"), Binance(cfg, **candles), None)
        bot.stocks.closes[SYM] = m.StockMarket.baseline(cfg.tickers[SYM], m.STOCK_MARKETS["hk"], "腾讯", dt.date(2026, 9, 23),
                                                        D("38.00"), D("38.10"))
        for symbol in cfg.symbols:
            bot.vols.refreshed[symbol] = 1e18  # no daily-bar read here: σ is the configured prior
        await bot.refresh_odds_inputs(NOW)
        return bot, bot.market

    def quote(bot, last_age_s, mark, last="38.02", at=NOW):
        """Binance's row as the stream pushes it: the last trade that old, the mark price of this second."""
        bot.market.now = at
        row = {"symbol": SYM, "price": last, "time": at - last_age_s * 1000, "markPrice": mark, "markTime": at, "indexPrice": "37.80"}
        if isinstance((bot.snapshots.get(SYM) or {}).get("quote"), m.Quote):
            bot.price_pushed("futures", SYM, row)  # between two samples: the push replaces the quote
        else:
            bot.snapshots[SYM] = {"quote": m.Quote.parse(row, SYM, at, bot.config.max_age)}
        return bot.snapshots[SYM]["quote"]

    def card(bot, at=NOW):
        odds = dict(bot.odds_items(at))[TITLE]
        assert isinstance(odds, m.CloseOdds), odds
        return odds

    # --- an ordinary evening: the closing minute traded (38.02); Binance's mark stood at 38.10 then, 0.21% above ---------
    bot, market = await world()
    assert bot.anchors[SYM] == (CLOSE, D("38.02"))  # the trade candle, as before
    looked: list = []
    bot.priced = lambda keys: looked.append(sorted(keys))
    q = quote(bot, 60, "38.10")
    fresh = card(bot)
    assert q.source == "last" and fresh.effective == D("38") and abs(fresh.fair_up - 0.5) < 1e-4 and not fresh.warn, fresh
    assert fresh.proxy_note == "币安 38.02 / 收盘时刻 38.02 → +0.000%", fresh.proxy_note  # a trade against a trade reads as ever
    # 70 s on nothing has traded and nothing has moved — the last trade and the mark are where they were at the close —
    # but the last trade is now 130 s old, so the quote is the mark: 38.10 against the 38.10 of the close is no move
    # (it was divided by the 38.02 trade: +0.21%, 涨 52.8¢)
    q = quote(bot, 130, "38.10", at=NOW + 70_000)
    quiet = card(bot, NOW + 70_000)
    assert q.source == "mark" and q.price == D("38.10") and looked == [[SYM]], (q, looked)  # (the trader still looks: the quote's kind changed)
    assert quiet.effective == D("38") and quiet.fair_up == fresh.fair_up and not quiet.warn, (quiet.effective, quiet.fair_up, quiet.warn)
    assert quiet.proxy_note == "币安 标记价 38.1 / 收盘时刻 标记价 38.1 → +0.000%", quiet.proxy_note
    # the mark really moves: its own move since the close, +0.079%, not +0.289% (涨 51.0¢, was 53.8¢)
    q = quote(bot, 131, "38.13", at=NOW + 71_000)
    moved = card(bot, NOW + 71_000)
    assert moved.effective == D("38.00") * D("38.13") / D("38.10") and 0.509 < moved.fair_up < 0.512 and not moved.warn, (moved.effective, moved.fair_up)
    assert moved.proxy_note == "币安 标记价 38.13 / 收盘时刻 标记价 38.1 → +0.079%", moved.proxy_note
    assert bot.odds_row(moved) in bot.context_lines(SYM, NOW + 71_000, q.price, q.source)  # an alert's odds line is that card's
    bot.snapshots[SYM]["baseline"] = m.Baseline(D("38.02"), "k", "币安合约成交价@港交所收盘时刻", NOW + 86_400_000, CLOSE)
    assert bot.odds_row(moved) in bot.status("1:0")  # ...so is /status's
    bot.record_predictions(NOW + 71_000)  # ...and so is the snapshot /calib scores
    saved = bot.store.get(f"pred:{SYM}:{NOW + 71_000}")
    assert abs(saved["up_raw"] - moved.fair_up) < 1e-12 and saved["proxy"] == moved.proxy_note and not saved["warn"], saved
    # the next trade: the quote is a trade again, compared with the trade candle
    q = quote(bot, 1, "38.13", last="38.05", at=NOW + 80_000)
    traded = card(bot, NOW + 80_000)
    assert q.source == "last" and traded.effective == D("38.00") * D("38.05") / D("38.02") and not traded.warn, traded
    # the anchors: both candles of the closing minute, one request each, never again for that close
    assert bot.anchor_marks[SYM] == (CLOSE, D("38.10")) and bot.anchor_kinds[SYM] == (CLOSE, "成交价")
    assert market.asked == [(TRADES, CLOSE - 60_000), (MARKS, CLOSE - 60_000)], market.asked
    bot.anchor_tries.clear()
    await bot.refresh_odds_inputs(NOW + 90_000)
    assert len(market.asked) == 2, market.asked

    # --- the paper trader under the defaults (takers, after hours, 10¢ bar), 涨 offered at 42¢ × 300 --------------------
    async def trader(last_age_s, mark_now, **candles):
        """One look of the paper trader with that quote: (the card's odds, its market as the trader sees it, the trades)."""
        bot, market = await world(**candles)
        quote(bot, last_age_s, mark_now)
        odds = card(bot)
        slug = m.predict_slug(bot.config.predict_slugs[SYM], odds.target)
        bot.predict.books[SYM] = m.PredictBook(SYM, slug, "901", "t", ((D("0.38"), D("100")),), ((D("0.42"), D("300")),), NOW)

        async def details(mid):
            return {"outcomes": ["Up", "Down"], "status": "REGISTERED", "resolved": None}
        bot.predict.market_details = details
        (mk,) = [x for x in bot.sim_markets(NOW) if x.key == SYM]
        bot.sim_ran = -1e9
        await bot.sim_step(NOW)
        return odds, mk, sorted((t["label"], t["status"], t["shares"]) for t in bot.sim_trades().values())

    odds, mk, trades = await trader(60, "38.13")
    assert abs(odds.fair_up - 0.5) < 1e-4 and not mk.hold and trades == [], (odds.fair_up, trades)
    # the last trade turns 120 s old, the mark 0.29% above it as it nearly was at the close: no chance has opened
    # (the 53.8¢ of mark ÷ trade cleared the bar: 300 shares of 涨 were bought at 42¢)
    odds, mk, trades = await trader(200, "38.13")
    assert 0.509 < odds.fair_up < 0.512 and not mk.hold and trades == [], (odds.fair_up, mk.hold, trades)
    # a move of the mark itself is traded as before (+1.05% since the close: 涨 64¢)
    odds, mk, trades = await trader(200, "38.50")
    assert odds.fair_up > 0.62 and not mk.hold and trades == [("吃涨", "filled", 300.0)], (odds.fair_up, mk.hold, trades)
    assert mk.evidence["proxy"] == {"proxy": f"币安 {SYM} 合约", "price": 38.5, "quoted_ms": NOW, "anchor": 38.10, "anchor_ms": CLOSE,
                                    "anchor_note": "收盘时刻的币安标记价", "approx": False}, mk.evidence["proxy"]
    assert mk.evidence["sources"][-1]["type"] == "标记价", mk.evidence["sources"]
    # the mark candle of the close not read (the request failed): the trade candle still gives an estimate — the 53.8¢
    # — but it is flagged and nothing is bought on it
    odds, mk, trades = await trader(200, "38.13", mark=None)
    assert 0.535 < odds.fair_up < 0.54 and "口径不同" in odds.warn and mk.hold == odds.warn and trades == [], (odds.fair_up, mk.hold, trades)
    assert mk.evidence["proxy"]["approx"] and mk.evidence["proxy"]["anchor_note"] == "收盘时刻的币安成交价（现价是标记价，口径不同）", mk.evidence["proxy"]

    # --- the mark candle cannot be read: a mark quote waits for it (flagged meanwhile), and it alone is asked again ------
    bot, market = await world(mark=None)
    assert bot.anchors[SYM] == (CLOSE, D("38.02")) and SYM not in bot.anchor_marks
    quote(bot, 60, "38.13")
    assert not card(bot).warn  # a trade against the trade candle needs no mark
    quote(bot, 130, "38.13", at=NOW + 70_000)
    waiting = card(bot, NOW + 70_000)
    assert "口径不同" in waiting.warn and "尚未取到" in waiting.warn, waiting.warn
    assert waiting.proxy_note == "币安 标记价 38.13 / 收盘时刻 成交价 38.02 → +0.289%", waiting.proxy_note
    market.mark = "38.10"
    await bot.refresh_odds_inputs(NOW + 70_000)  # within the minute: not asked yet (retry_ok)
    assert SYM not in bot.anchor_marks and len(market.asked) == 2, market.asked
    bot.anchor_tries.clear()                     # a minute later: the mark candle alone
    await bot.refresh_odds_inputs(NOW + 70_000)
    assert bot.anchor_marks[SYM] == (CLOSE, D("38.10")) and [path for path, _ in market.asked] == [TRADES, MARKS, MARKS], market.asked
    assert not card(bot, NOW + 70_000).warn and card(bot, NOW + 70_000).effective == D("38.00") * D("38.13") / D("38.10")

    # --- the closing minute did not trade: its mark candle is the anchor (price_at's fallback), no second request for it ---
    bot, market = await world(trade=("38.02", "0"))
    assert bot.anchors[SYM] == bot.anchor_marks[SYM] == (CLOSE, D("38.10")) and bot.anchor_kinds[SYM] == (CLOSE, "标记价")
    assert market.asked == [(TRADES, CLOSE - 60_000), (MARKS, CLOSE - 60_000)], market.asked
    q = quote(bot, 200, "38.10")
    marked = card(bot)
    assert q.source == "mark" and marked.effective == D("38") and not marked.warn, marked  # the mark against the mark, as before
    assert marked.proxy_note == "币安 标记价 38.1 / 收盘时刻 标记价 38.1 → +0.000%", marked.proxy_note
    # a fresh trade there has no trade candle to be compared with: left as it was — against that mark candle, and not
    # held (that would silence the card for two minutes after every trade of such a night) — but the pair is named and
    # a trade's record says its anchor is of the other kind
    q = quote(bot, 5, "38.10", at=NOW + 1_000)
    other = card(bot, NOW + 1_000)
    assert q.source == "last" and other.effective == D("38.00") * D("38.02") / D("38.10") and not other.warn, other
    assert other.proxy_note == "币安 最新成交 38.02 / 收盘时刻 标记价 38.1 → -0.210%", other.proxy_note
    assert bot.close_evidence(TITLE, other, NOW + 1_000)["proxy"] == {
        "proxy": f"币安 {SYM} 合约", "price": 38.02, "quoted_ms": NOW + 1_000 - 5_000, "anchor": 38.10, "anchor_ms": CLOSE,
        "anchor_note": "收盘时刻的币安标记价（现价是最新成交价，口径不同）", "approx": True}

    # --- Binance.price_at: the kinds asked, each cached beside the other; a new instant drops the old one's -------------
    b = Binance(bot.config)
    assert await b.price_at(SYM, CLOSE) == (D("38.02"), "成交价") and await b.price_at(SYM, CLOSE, ("标记价",)) == (D("38.10"), "标记价")
    assert await b.price_at(SYM, CLOSE) == (D("38.02"), "成交价") and await b.price_at(SYM, CLOSE, ("标记价",)) == (D("38.10"), "标记价")
    assert [path for path, _ in b.asked] == [TRADES, MARKS] and {k[1] for k in b.at_cache} == {CLOSE}, (b.asked, b.at_cache)
    await b.price_at(SYM, CLOSE + 60_000)
    assert {k[1] for k in b.at_cache} == {CLOSE + 60_000}, b.at_cache
    b.mark = None
    try:
        await b.price_at(SYM, CLOSE + 60_000, ("标记价",))
        assert False, "a mark candle that cannot be read is no price"
    except m.RemoteError:
        pass


asyncio.run(feed_mark_vs_last())


# FEED-HSI-FUTURES-STALE: after the cash close the HSI card is priced from the futures alone (close × futures now ÷ futures
# at 16:10), and while a futures session trades a quote stood for the market for ten minutes without a change
# (futures_problem: LIVE_STALE_MS, a single stock's allowance; the cash index feeds: three). etnet's running block prints
# no time and is dated by when its values were first seen. So (a) a page already frozen when the bot started was dated
# "now", whatever its age, and (b) a page that froze priced the card for ten more minutes — both with nothing said, the
# default after-hours taker (10¢ bar) buying on it. The estimate now stays on the card with the reason and nothing is
# suggested from it (CloseOdds.warn → SimMarket.hold): until the block is seen to change after a start, and from three
# minutes without a change. Which source is used, the card's figures and futures_problem's ten minutes are as they were.
async def feed_hsi_futures_stale() -> None:
    import datetime as dt
    D = m.D

    def at(hour: int, minute: int, second: int = 0) -> int:  # Wednesday 2026-09-23: the night session runs 17:15–03:00
        return int(dt.datetime(2026, 9, 23, hour, minute, second, tzinfo=m.BEIJING).timestamp() * 1000)

    CLOSE = at(16, 10)
    feed = {"last": 24846}

    def page() -> bytes:
        """etnet in the night session: the running 夜市 block (opened from the day block's last) carries no time."""
        last = feed["last"]
        return (f"""<div><h3>恒生指數期貨(10/2026)</h3><span>日市</span><div>&#9650;24,610 +90 (+0.37%) 高水15</div>
<div>最高: 24,650 最低: 24,500 前收市: 24,520 開市: 24,530</div></div>
<div><h3>恒生指數期貨(10/2026)</h3><span>夜市</span><div>&#9650;{last:,} +{last - 24610} (+0.96%) 高水{last - 24595}</div>
<div>最高: 24,900 最低: 24,600 前收市: 24,610 開市: 24,615</div></div>
<div>恒生指數現貨 &#9650;24,595.00 +84.91 (+0.35%) 前收市: 24,510.09 開市: 24,520</div>""").encode()

    async def fake_get(url, timeout=15, headers=None):
        if "etnet" in url:
            return page()
        raise m.RemoteError("HTTP 502")  # Eastmoney and the Sina CFD are down: etnet alone prices the card

    class Clock:
        now = 0

        def now_ms(self):
            return self.now

    def start() -> "m.Bot":
        """The bot as it starts that evening under its default settings (takers, after hours, 10¢ bar): the HSI closed at
        24,595, the futures printed 24,600 at 16:10 (the exact anchor, recorded that afternoon)."""
        cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "ADMIN_USER_ID": "42", "STATE_DB": ":memory:"})
        store = m.Store(":memory:")
        store.put("anchor:HSI", [CLOSE, "24600", "16:10 现货收市时", "10/2026", CLOSE + 3_000, CLOSE + 3_000])
        bot = m.Bot(cfg, store, Clock(), None)
        bot.hsi_daily.daily = {dt.date(2026, 9, 23): D("24595.00")}

        async def details(mid):
            return {"outcomes": ["Up", "Down"], "status": "REGISTERED", "resolved": None}
        bot.predict.market_details = details
        return bot

    async def card(bot, now):
        """One refresh at ``now``: (the card's odds, its market as the trader sees it — 涨 offered at 55¢ × 300)."""
        bot.market.now = now
        await bot.hsi.refresh(now, force=True)
        odds = bot.hsi_odds(now)
        if not isinstance(odds, m.CloseOdds):
            return odds, None
        slug = m.predict_slug(bot.config.predict_slugs["HSI"], odds.target)
        bot.predict.books["HSI"] = m.PredictBook("HSI", slug, "902", "t", ((D("0.50"), D("100")),), ((D("0.55"), D("300")),), now)
        (mk,) = [x for x in bot.sim_markets(now) if x.key == "HSI"]
        return odds, mk

    async def trader(bot, now) -> list:
        """One look of the paper trader (the real one follows it): the trades it has made so far."""
        bot.sim_ran = -1e9
        await bot.sim_step(now)
        return sorted((t["label"], t["status"], t["shares"]) for t in bot.sim_trades().values())

    real_get, real_health = m.http_get, m.SOURCE_HEALTH
    m.http_get, m.SOURCE_HEALTH = fake_get, m.SourceHealth()
    try:
        # --- (a) the bot starts at 21:30 on a page that stands still (frozen since who knows when) -------------------------
        bot = start()
        odds, mk = await card(bot, at(21, 30))
        q = bot.hsi.quote
        # what the card shows is as it was: etnet's running block, dated by its first sighting, standing for the market
        assert q.source == "etnet" and q.session == "夜市" and q.quoted_ms == at(21, 30) and not bot.hsi.futures_problem(q, at(21, 30)), q
        assert 0.77 < odds.fair_up < 0.79 and odds.proxy_note.startswith("恒指期货 24,846 / 16:10 现货收市时 24,600 → +1.000%"), odds
        # ...but nothing tells that block's age: no suggestion, no order (300 涨 were bought at 55¢ on this very look)
        assert odds.warn == "恒指期货 etnet 页面启动后尚未见变动，报价时间无法确认" and mk.hold == odds.warn, (odds.warn, mk.hold)
        assert await trader(bot, at(21, 30)) == []
        # nine minutes on the page is as it was first read: still held (it passed until 21:40, and again after any restart)
        odds, mk = await card(bot, at(21, 39))
        assert bot.hsi.quote.quoted_ms == at(21, 30) and not bot.hsi.futures_problem(bot.hsi.quote, at(21, 39)), bot.hsi.quote
        assert "尚未见变动" in odds.warn and mk.hold == odds.warn and await trader(bot, at(21, 39)) == [], (odds.warn, mk.hold)
        # the page moves: a live one. The card suggests again and the trader takes the offer, as before
        feed["last"] = 24850
        odds, mk = await card(bot, at(21, 39, 2))
        assert bot.hsi.quote.quoted_ms == at(21, 39, 2) and not odds.warn and not mk.hold, (odds.warn, mk.hold)
        assert await trader(bot, at(21, 39, 2)) == [("吃涨", "filled", 300.0)]

        # --- (b) a page that freezes while the bot runs: last seen to change at 21:00 ---------------------------------------
        bot = start()
        feed["last"] = 24840
        await card(bot, at(20, 59, 58))
        feed["last"] = 24846
        odds, mk = await card(bot, at(21, 0))
        assert bot.hsi.quote.quoted_ms == at(21, 0) and not odds.warn and not mk.hold, (odds.warn, mk.hold)
        odds, mk = await card(bot, at(21, 3))  # three minutes: a short gap, as before
        assert isinstance(odds, m.CloseOdds) and not odds.warn and not mk.hold, (odds, mk.hold)
        # past three minutes without a change the futures are not just quiet (they print every few seconds): the estimate
        # stays, nothing is suggested from it (the taker bought here; the card said nothing until 21:10)
        odds, mk = await card(bot, at(21, 3, 1))
        stuck = "恒指期货报价停在 09-23 21:00，已超 3 分钟未更新"
        assert odds.warn == stuck and mk.hold == stuck and 0.77 < odds.fair_up < 0.79, (odds.warn, mk.hold, odds.fair_up)
        assert await trader(bot, at(21, 3, 1)) == []
        # the quote itself still stands for the market for futures_problem's ten minutes: same source, same figures
        odds, mk = await card(bot, at(21, 10))
        q = bot.hsi.quote
        assert q.source == "etnet" and q.quoted_ms == at(21, 0) and not bot.hsi.futures_problem(q, at(21, 10)) and not bot.hsi.error, q
        assert odds.warn == stuck and mk.hold == stuck and odds.proxy_note.startswith("恒指期货 24,846 / 16:10 现货收市时 24,600"), odds
        # ...after which it is turned down as ever (no other source answering: the odds pause)
        odds, mk = await card(bot, at(21, 10, 1))
        assert odds == "恒指期货报价停在 09-23 21:00，已超 10 分钟未更新，暂不输出新概率" and mk is None, odds
        assert await trader(bot, at(21, 10, 1)) == []
        # it moves again: priced and traded as before
        feed["last"] = 24852
        odds, mk = await card(bot, at(21, 12))
        assert isinstance(odds, m.CloseOdds) and not odds.warn and not mk.hold and bot.hsi.quote.quoted_ms == at(21, 12), odds
        assert await trader(bot, at(21, 12)) == [("吃涨", "filled", 300.0)]

        # --- a source with its own time (Eastmoney, the Sina CFD) is judged by that; lunch aside; nothing held while shut ---
        timed = m.FuturesQuote("恒指主连", D("24846"), D("24610"), None, None, None, at(21, 0), "东方财富", fetched_ms=at(21, 3, 1))
        assert bot.hsi.futures_hold(timed, at(21, 3)) == "" and bot.hsi.futures_hold(timed, at(21, 3, 1)) == stuck
        noon = m.dataclasses.replace(timed, quoted_ms=at(11, 59))
        assert bot.hsi.futures_hold(noon, at(13, 2)) == "" and "已超 3 分钟未更新" in bot.hsi.futures_hold(noon, at(13, 2, 1))
        final = m.dataclasses.replace(timed, quoted_ms=at(16, 29))  # 16:50: between the day and the night session
        assert m.hk_futures_session(at(16, 50), bot.hsi.holidays) == "休市" and bot.hsi.futures_hold(final, at(16, 50)) == ""
    finally:
        m.http_get, m.SOURCE_HEALTH = real_get, real_health


asyncio.run(feed_hsi_futures_stale())


# FEED-HSI-SPOT-AFTER-CLOSE: after the 16:10 cash close, until the day's dated bar is in (it counts from 16:25), the cash
# index on the HSI quote IS the day's close: the after-hours card's reference — which every trade opened on the next
# session's market keeps as its settlement line — and, from 16:15, the provisional outcome the day's own trades are
# pre-settled on. spot_problem looked at the figure's time only while the session ran, and the refresh kept a newer timed
# figure only then as well. So at the 16:10 refresh etnet's own index — standing since 16:00 through the auction, or stuck
# since 15:50 — took the place of the timed feeds' close and passed for it, as Tencent's 15-minute-delayed feed did. A
# figure printed before the closing auction's random end (16:08–16:10) is no close: the card waits and says what for,
# nothing is recorded from it, and the timed feeds are asked again every round. In session, once the bar is in, and for
# the page's futures block, everything is as it was.
async def feed_hsi_spot_after_close() -> None:
    import datetime as dt, json
    D = m.D

    def at(hour: int, minute: int, second: int = 0, day: int = 30, month: int = 9) -> int:  # Wednesday 2026-09-30 (10-01: a holiday)
        return int(dt.datetime(2026, month, day, hour, minute, second, tzinfo=m.BEIJING).timestamp() * 1000)

    PREV, CLOSE, AUCTION, STUCK = D("24510.09"), D("24595.00"), D("24550.00"), D("24400.00")
    waits = "恒指收盘价待确认（现货报价停在 09-30 15:50，早于 09-30 16:08 起的随机收市，日 K 未出）"
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "ADMIN_USER_ID": "42", "STATE_DB": ":memory:",
                             "KOSPI_INDEX": "off", "SSE_INDEX": "off"})
    hk = cfg.holidays["hk"]

    # --- spot_problem itself: after the close, the day's bar not in, the figure must come from the close ---------------------
    h = m.IndexFutures(True, hk)
    h.dated_close = lambda day: PREV if day == dt.date(2026, 9, 29) else None
    fut = m.FuturesQuote("恒指期货(10/2026)日市", D("24600"), D("24520"), None, None, None, at(16, 12), "etnet",
                         fetched_ms=at(16, 12), session="日市")

    def cash(value, when, source="腾讯"):
        return h.with_spot(fut, m.IndexQuote("恒生指数", value, PREV, None, None, None, when, source))

    assert h.spot_problem(cash(STUCK, at(15, 50)), at(16, 20)) == waits, h.spot_problem(cash(STUCK, at(15, 50)), at(16, 20))
    assert "停在 09-30 15:57" in h.spot_problem(cash(D("24480"), at(15, 57)), at(16, 12))  # Tencent's feed, 15 minutes behind
    assert "停在 09-30 10:05" in h.spot_problem(cash(D("24700"), at(10, 5)), at(16, 40))   # any figure of the day passed
    assert "停在 09-30 16:07" in h.spot_problem(cash(AUCTION, at(16, 7, 59)), at(16, 12))  # the auction has not matched yet
    assert h.spot_problem(cash(AUCTION, 0), at(16, 12)) == "恒指收盘价待确认（现货报价时间未知，09-30 日 K 未出）"
    # a print from the auction's random end (16:08–16:10) or later is the close: that evening, and until the next open
    for when, now in ((at(16, 8), at(16, 12)), (at(16, 9, 40), at(21, 0)), (at(16, 9, 40), at(12, 0, day=1, month=10)),
                      (at(16, 9, 40), at(9, 0, day=2, month=10))):
        assert h.spot_problem(cash(CLOSE, when, "新浪"), now) == "", (m.stamp(when), m.stamp(now))
    # in session nothing changed (the auction keeps its ten minutes), nor once the dated bar is in (the reference then)
    assert h.spot_problem(cash(AUCTION, at(15, 59, 58)), at(16, 9)) == ""
    h.dated_close = lambda day: {dt.date(2026, 9, 29): PREV, dt.date(2026, 9, 30): CLOSE}.get(day)
    assert h.spot_problem(cash(D("24700"), at(10, 5)), at(16, 40)) == ""
    # a half day closes at 12:10: the same two minutes
    half = m.IndexFutures(True, hk)
    eve = lambda hour, minute, second=0: at(hour, minute, second, day=24, month=12)
    assert "早于 12-24 12:08 起的随机收市" in half.spot_problem(cash(AUCTION, eve(12, 7)), eve(12, 15))
    assert half.spot_problem(cash(CLOSE, eve(12, 8, 30)), eve(12, 15)) == ""

    # --- the real refresh path through the 16:10 close -----------------------------------------------------------------------
    # etnet's 恒生指數現貨 block has been stuck at 24,400 since 15:50 (its futures block moves on); Eastmoney's timed index
    # follows the market, stands at 24,550 through the auction and prints the close, 24,595, at 16:09:30
    feed = {"em_down": False}

    class Clock:
        now = 0

        def now_ms(self):
            return self.now

    clock = Clock()

    def page(now: int) -> bytes:
        last = 24600 + (min(now, at(16, 30)) // 1000) % 41  # the day session's block: moving until 16:30
        return (f"""<div><h3>恒生指數期貨(10/2026)</h3><span>日市</span><div>&#9650;{last:,} +80 (+0.33%) 高水50</div>
<div>最高: 24,650 最低: 24,500 前收市: 24,520 開市: 24,530</div></div>
<div>恒生指數現貨 &#9660;{STUCK:,.2f} -110.09 (-0.45%) 前收市: {PREV:,.2f} 開市: 24,520</div>""").encode()

    def eastmoney(now: int) -> tuple:
        if now < at(16, 0):
            return D("24500") + D((now - at(15, 50)) // 60_000), now
        return (CLOSE, at(16, 9, 30)) if now >= at(16, 9, 30) else (AUCTION, at(16, 0))

    async def fake_get(url, timeout=15, headers=None):
        if "etnet" in url:
            return page(clock.now)
        if "100.HSI" in url and not feed["em_down"]:
            value, when = eastmoney(clock.now)
            return json.dumps({"data": {"f43": float(value), "f60": float(PREV), "f86": when // 1000, "f58": "恒生指数"}}).encode()
        raise m.RemoteError("HTTP 502")  # Eastmoney's futures, the Sina CFD, Sina's and Tencent's index: down

    real_get, real_health = m.http_get, m.SOURCE_HEALTH
    m.http_get, m.SOURCE_HEALTH = fake_get, m.SourceHealth()
    try:
        store = m.Store(":memory:")
        store.put("anchor:HSI", [at(16, 10), "24600", "16:10 现货收市时", "10/2026", at(16, 10, 1), at(16, 10, 1)])
        bot = m.Bot(cfg, store, clock, None)
        bot.hsi_daily.daily = {dt.date(2026, 9, 29): PREV}
        hsi = bot.hsi

        async def look(now: int, force: bool = True):
            """One refresh at ``now``: (the card's odds, the settlement line a trade opened now would keep; None: no market)."""
            clock.now = now
            await hsi.refresh(now, force=force)
            odds = bot.hsi_odds(now)
            if isinstance(odds, m.CloseOdds):
                slug = m.predict_slug(cfg.predict_slugs["HSI"], odds.target)
                bot.predict.books["HSI"] = m.PredictBook("HSI", slug, "902", "t", ((D("0.45"), D("50")),), ((D("0.47"), D("50")),), now)
            lines = [mk.settle["line"] for mk in bot.sim_markets(now) if mk.key == "HSI"]
            return odds, (lines[0] if lines else None)

        for now in (at(15, 50), at(15, 56), at(16, 2), at(16, 9, 57)):  # the session: the futures page, then the timed index
            await look(now)
            clock.now = now + 1000
            await hsi.refresh_spot(now + 1000)
        q = hsi.quote
        assert (q.source, q.spot, q.spot_source, q.spot_time) == ("etnet", CLOSE, "东方财富", at(16, 9, 30)), q
        assert isinstance(bot.hsi_odds(at(16, 9, 58)), m.CloseOdds)
        # 16:10:01, the close refresh (cash_close_due). The page's stuck figure took the timed close's place here and stayed:
        # the reference until the bar (24,400), the line of every trade opened meanwhile, from 16:15 the day's outcome
        odds, line = await look(at(16, 10, 1), force=False)
        q = hsi.quote
        assert (q.source, q.spot, q.spot_source, q.spot_time) == ("etnet", CLOSE, "东方财富", at(16, 9, 30)), q
        assert isinstance(odds, m.CloseOdds) and odds.mode == "盘后" and odds.ref == CLOSE and not odds.warn, odds
        assert line == float(CLOSE), line
        # a round in which no timed feed answers: the page figure is all there is, and it is not the close. The card waits
        feed["em_down"] = True
        odds, line = await look(at(16, 10, 21))
        assert hsi.quote.spot == STUCK and hsi.spot_problem(hsi.quote, at(16, 10, 21)) == waits, hsi.quote
        assert odds == waits + "，暂不输出概率" and line is None and waits in hsi.spot_error, (odds, line, hsi.spot_error)
        clock.now = at(16, 16)
        bot.record_predictions(at(16, 16))  # ...and no outcome is written from it (it was, from 16:15 on)
        assert store.get("outcome:HSI:2026-09-30") is None, store.get("outcome:HSI:2026-09-30")
        # the timed feed is back: it is asked again in that very round; its close prices the card and is the outcome so far
        feed["em_down"] = False
        odds, line = await look(at(16, 16, 2))
        assert isinstance(odds, m.CloseOdds) and odds.ref == CLOSE and line == float(CLOSE), (odds, line)
        bot.record_predictions(at(16, 16, 2))
        assert store.get("outcome:HSI:2026-09-30") == float(CLOSE), store.get("outcome:HSI:2026-09-30")
        assert store.get("outsrc:HSI:2026-09-30")["source"] == "东方财富 实时（日K未出）", store.get("outsrc:HSI:2026-09-30")
        # 16:40, the futures shut: the page's futures block stands as it did (FEED-ETNET-CLOSED-BLOCK asks which day the
        # page is, and it is the day's) — its cash figure alone is not the close, and the timed one stays the reference
        odds, line = await look(at(16, 40))
        q = hsi.quote
        own = hsi.dated(m.parse_etnet_futures(page(at(16, 40)), at(16, 40), hk), at(16, 40))
        assert own.spot == STUCK and own.spot_source == "etnet" and own.quoted_ms == at(16, 30), own
        assert hsi.futures_problem(own, at(16, 40)) == "" and hsi.spot_problem(own, at(16, 40)) == waits
        assert q.source == "etnet" and q.quoted_ms == at(16, 30) and (q.spot, q.spot_source) == (CLOSE, "东方财富"), q
        assert isinstance(odds, m.CloseOdds) and odds.ref == CLOSE and line == float(CLOSE), (odds, line)
        # the dated bar arrives: it is the reference and the outcome, whatever the page shows (as before)
        bot.hsi_daily.daily[dt.date(2026, 9, 30)] = D("24595.31")
        feed["em_down"] = True
        odds, line = await look(at(16, 41))
        assert hsi.quote.spot == STUCK and isinstance(odds, m.CloseOdds) and odds.ref == D("24595.31") and line == 24595.31, (odds, line)
        bot.record_predictions(at(16, 41))
        assert store.get("outcome:HSI:2026-09-30") == 24595.31, store.get("outcome:HSI:2026-09-30")
    finally:
        m.http_get, m.SOURCE_HEALTH = real_get, real_health


asyncio.run(feed_hsi_spot_after_close())


# --- 1.150.0: the first Binance clock sync is not skipped on a freshly booted machine -------------------------------------------
async def sync_clock_fresh_boot():
    """time.monotonic() counts from boot: on a host up for less than ten minutes a never-synced clock (once 0.0) read as
    synced moments ago — sync_clock skipped the first sync and the cycle took the zero for a recent sync."""
    import types
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off"})
    market = m.Binance(cfg)
    asked = []

    async def get(path, params=None, *a, **k):
        asked.append(path)
        return {"serverTime": int(time.time() * 1000) + 5000}
    market.get = get
    real_time = m.time
    m.time = types.SimpleNamespace(monotonic=lambda: 120.0, time=real_time.time)  # main's view only: two minutes after boot
    try:
        await market.sync_clock()
    finally:
        m.time = real_time
    assert asked == ["/fapi/v1/time"] and 4000 < market.offset_ms < 6000 and market.clock_checked == 120.0, (asked, market.offset_ms, market.clock_checked)


asyncio.run(sync_clock_fresh_boot())

print("AUDIT_FEEDS_OK")
