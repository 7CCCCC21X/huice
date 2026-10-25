"""1.86.0: PredictStream — Predict's WebSocket beside the REST rounds: the hot markets' books pushed as they change."""
import asyncio, sys, time, json
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

D = m.D
NOW = int(time.time() * 1000)
HSI_SLUG = "hang-seng-index-up-or-down-on-october-26-2026"


class FakeWs:
    """A connection the tests feed: frames pushed in, frames sent out, an exception pushed in ends it."""
    def __init__(self):
        self.sent, self.incoming, self.closed = [], asyncio.Queue(), False

    async def send(self, text):
        self.sent.append(json.loads(text))

    async def recv(self):
        item = await self.incoming.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def close(self):
        self.closed = True

    def push(self, msg):
        self.incoming.put_nowait(msg if isinstance(msg, (str, BaseException)) else json.dumps(msg))


def feed_with_books(**env):
    feed = m.PredictFeed(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "PREDICT_API_KEY": "k1", **env}))
    feed.books["HSI"] = m.PredictBook("HSI", HSI_SLUG, "7", "HSI up?", ((D("0.80"), D("100")),), ((D("0.85"), D("100")),), NOW - 5000, 200)
    feed.books["BTC"] = m.PredictBook("BTC", "will-btc-hit-70000-or-90000-first", "8", "BTC", (), ((D("0.5"), D("10")),), NOW - 5000)
    feed.ladder_keys.add("BTC-HIT-10")
    book = m.PredictBook("BTC-HIT-10", "what-price-will-bitcoin-hit-in-october-2026", "41", "BTC ↑ $90k",
                         ((D("0.10"), D("50")),), ((D("0.20"), D("50")),), NOW - 5000, 200)
    feed.ladder_rows["BTC-HIT-10"] = {"41": m.LadderRow(D("90000"), "41", "BTC ↑ $90k", book, "", "")}
    feed.ladder_publish("BTC-HIT-10")
    return feed


async def run():
    m.PREDICT_WS_BACKOFF = (0.01, 0.05)  # the tests do not wait seconds between connections
    feed = feed_with_books()
    seen = []
    async def hook(key): seen.append(key)
    feed.on_update = hook
    stream = feed.stream
    connections = []
    async def connect(url, headers):
        assert url == m.PREDICT_WS_URL and headers == {"x-api-key": "k1"}, (url, headers)
        ws = FakeWs(); connections.append(ws); return ws
    stream.connect = connect
    stopping = asyncio.Event()
    assert stream.enabled and not stream.connected and stream.line() == "尚未连接"
    # the trader's wants: the single markets first, then the ladder levels
    stream.want({"41": ("BTC-HIT-10", True), "7": ("HSI", False), "8": ("BTC", False)})
    assert list(stream.wanted) == ["7", "8", "41"]
    task = asyncio.create_task(stream.run(stopping))
    await asyncio.sleep(0.05)
    ws = connections[0]
    assert stream.connected and stream.connects == 1 and "已连接" in stream.line(), stream.line()
    subs = [f for f in ws.sent if f["method"] == "subscribe"]
    assert [f["params"] for f in subs] == [["predictOrderbook/7"], ["predictOrderbook/8"], ["predictOrderbook/41"]] and [f["requestId"] for f in subs] == [1, 2, 3], ws.sent
    assert set(stream.subscribed) == {"7", "8", "41"}
    # the answers: 7 and 41 accepted, 8 refused (not asked for again at once)
    ws.push({"type": "R", "requestId": 1, "success": True}); ws.push({"type": "R", "requestId": 3, "success": True})
    ws.push({"type": "R", "requestId": 2, "success": False, "error": {"code": "invalid_topic", "message": "unknown market"}})
    await asyncio.sleep(0.05)
    assert "8" not in stream.subscribed and stream.refused["8"][0] == "invalid_topic：unknown market" and "invalid_topic" in stream.error, (stream.refused, stream.error)
    stream.wake.set(); await asyncio.sleep(0.05)
    assert not [f for f in ws.sent if f["method"] == "subscribe" and f["params"] == ["predictOrderbook/8"] and f["requestId"] > 2]
    # a snapshot: its levels replace the REST book's, the age restarts, the read time is now, the express lane is told
    ws.push({"type": "M", "topic": "predictOrderbook/7", "data": {"version": 5, "marketId": 7, "updateTimestampMs": NOW, "orderCount": 3,
             "bids": [["0.81", "110.5"], ["0.80", "400"]], "asks": [["0.85", "12"]], "lastOrderSettled": None, "settlementsPending": {"asks": [], "bids": []}}})
    await asyncio.sleep(0.05)
    book = feed.books["HSI"]
    assert book.bids == ((D("0.81"), D("110.5")), (D("0.80"), D("400"))) and book.asks == ((D("0.85"), D("12")),), book
    assert book.fetched_ms >= NOW and book.slug == HSI_SLUG and book.fee_bps == 200 and book.market_id == "7" and book.title == "HSI up?"
    assert seen == ["HSI"] and stream.books == 1 and stream.live == {"7"} and feed.book_asked["HSI"] > 0 and stream.versions["7"] == 5 and "HSI" in stream.pushed
    # an older or repeated version is dropped; a one-sided message replaces that side only; object levels are read too
    ws.push({"type": "M", "topic": "predictOrderbook/7", "data": {"version": 4, "bids": [["0.5", "1"]], "asks": [["0.6", "1"]]}})
    ws.push({"type": "M", "topic": "predictOrderbook/7", "data": {"version": 5, "bids": [["0.5", "1"]], "asks": [["0.6", "1"]]}})
    ws.push({"type": "M", "topic": "predictOrderbook/7", "data": {"version": 6, "asks": [{"price": "0.84", "size": "20"}]}})
    await asyncio.sleep(0.05)
    book = feed.books["HSI"]
    assert stream.dropped == 2 and book.bids[0] == (D("0.81"), D("110.5")) and book.asks == ((D("0.84"), D("20")),) and stream.books == 2, (stream.dropped, book)
    # a ladder level: its row's book, the ladder republished, the express lane told
    ws.push({"type": "M", "topic": "predictOrderbook/41", "data": {"version": 1, "bids": [["0.12", "30"]], "asks": [["0.19", "40"]]}})
    await asyncio.sleep(0.05)
    row = feed.ladder_rows["BTC-HIT-10"]["41"]
    assert row.book.bids == ((D("0.12"), D("30")),) and row.book.asks == ((D("0.19"), D("40")),) and row.book.fee_bps == 200
    assert feed.ladders["BTC-HIT-10"][0].book is row.book and seen[-1] == "BTC-HIT-10" and feed.row_asked["BTC-HIT-10"]["41"] > 0 and stream.live == {"7", "41"}
    # another topic is not ours; a market nobody asked for, a frame that is not JSON: dropped, nothing else happens
    ws.push({"type": "M", "topic": "assetPriceUpdate/x", "data": {"price": 1}})
    ws.push({"type": "M", "topic": "predictOrderbook/99", "data": {"bids": [], "asks": []}})
    ws.push("not json")
    await asyncio.sleep(0.05)
    assert stream.books == 3 and stream.dropped == 4 and seen == ["HSI", "HSI", "BTC-HIT-10"], (stream.books, stream.dropped, seen)
    # the heartbeat: echoed with the server's own data; the pushed books' age restarts, a market never pushed is left alone
    feed.books["HSI"] = m.dataclasses.replace(feed.books["HSI"], fetched_ms=NOW - 100_000)
    feed.ladder_rows["BTC-HIT-10"]["41"] = m.dataclasses.replace(row, book=m.dataclasses.replace(row.book, fetched_ms=NOW - 100_000))
    ws.push({"type": "M", "topic": "heartbeat", "data": 1761300000000})
    await asyncio.sleep(0.05)
    assert ws.sent[-1] == {"method": "heartbeat", "data": 1761300000000} and stream.heartbeats == 1
    assert feed.books["HSI"].fetched_ms >= NOW and feed.ladder_rows["BTC-HIT-10"]["41"].book.fetched_ms >= NOW and feed.ladders["BTC-HIT-10"][0].book.fetched_ms >= NOW
    assert feed.books["BTC"].fetched_ms == NOW - 5000, "a market never pushed (refused) is not touched"
    # the wants change: a market dropped is unsubscribed and forgotten, the rest stays
    stream.want({"7": ("HSI", False), "8": ("BTC", False)})
    await asyncio.sleep(0.05)
    assert ws.sent[-1] == {"method": "unsubscribe", "requestId": 4, "params": ["predictOrderbook/41"]}, ws.sent[-1]
    assert set(stream.subscribed) == {"7"} and "41" not in stream.versions and stream.live == {"7"}
    ws.push({"type": "M", "topic": "predictOrderbook/41", "data": {"version": 2, "bids": [["0.13", "30"]], "asks": [["0.19", "40"]]}})
    await asyncio.sleep(0.05)
    assert feed.ladder_rows["BTC-HIT-10"]["41"].book.bids == ((D("0.12"), D("30")),), "a push for a market let go is not applied"
    # a REST read replacing a pushed book is compared (/diag); one replacing a REST book is not
    stream.note_rest("HSI", feed.books["HSI"], m.dataclasses.replace(feed.books["HSI"], asks=((D("0.86"), D("1")),)))
    assert stream.replaced == 1 and stream.mismatch == 1 and stream.last_mismatch == "HSI：推送 买1 81.0¢ 卖1 84.0¢，REST 买1 81.0¢ 卖1 86.0¢", stream.last_mismatch
    stream.note_rest("HSI", feed.books["HSI"], feed.books["HSI"])
    assert stream.replaced == 1 and "HSI" not in stream.pushed
    line = stream.line()
    assert "订阅 1 个市场、在推 1 个" in line and "收到盘口 3 次、心跳 1 次" in line and "被拒绝 1 个主题（8：invalid_topic" in line and "丢弃 5 帧" in line \
        and "REST 覆盖推送 1 次，其中盘口不同 1 次" in line, line
    # the connection drops: made anew after the backoff, the subscriptions sent afresh, the versions and the live set forgotten
    stream.refused.clear()
    ws.push(ConnectionError("closed by peer"))
    await asyncio.sleep(0.3)
    assert len(connections) == 2 and stream.connects == 2 and ws.closed and "closed by peer" in stream.closed_why, (len(connections), stream.closed_why)
    ws2 = connections[1]
    assert [f["params"] for f in ws2.sent if f["method"] == "subscribe"] == [["predictOrderbook/7"], ["predictOrderbook/8"]]
    assert not stream.versions and not stream.live and not stream.pushed and set(stream.subscribed) == {"7", "8"}
    ws2.push({"type": "M", "topic": "predictOrderbook/7", "data": {"version": 1, "bids": [["0.70", "1"]], "asks": [["0.75", "1"]]}})
    await asyncio.sleep(0.05)
    assert feed.books["HSI"].bid == (D("0.70"), D("1")) and stream.versions["7"] == 1, "a lower version on a new connection is the snapshot"
    # silence: no frame for PREDICT_WS_SILENT_SECONDS: the connection is given up and made anew
    m.PREDICT_WS_SILENT_SECONDS = 0.1
    ws2.push({"type": "M", "topic": "heartbeat", "data": 2})  # the wait under way was given the old limit: one frame, then the new one applies
    await asyncio.sleep(0.5)
    assert len(connections) >= 3 and "没有收到任何帧" in stream.closed_why, (len(connections), stream.closed_why)
    m.PREDICT_WS_SILENT_SECONDS = 45.0
    # a connection that cannot be made: tried again after the backoff, nothing else happens
    async def refuse(url, headers): raise m.RemoteError("HTTP 401: 接口请求失败")
    stream.connect = refuse
    connections[-1].push(ConnectionError("gone"))
    await asyncio.sleep(0.3)
    assert not stream.connected and "HTTP 401" in stream.error and "尚未连接" not in stream.line() and "未连接" in stream.line(), stream.line()
    # the stop: the run ends, the connection is closed
    stream.connect = connect
    stopping.set()
    await asyncio.wait_for(task, 2)
    assert not stream.connected and all(ws.closed for ws in connections)
    # off: nothing is connected, the line says so
    off = m.PredictFeed(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "PREDICT_WS": "off"}))
    assert not off.stream.enabled and off.stream.line() == "关（PREDICT_WS=off）" and m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}).predict_ws
    stop2 = asyncio.Event(); run2 = asyncio.create_task(off.stream.run(stop2)); await asyncio.sleep(0.05); stop2.set(); await asyncio.wait_for(run2, 2)
    assert off.stream.connects == 0
    # the feed's targets for the stream: the hot single markets with a book and the hot ladder levels; no trader: every single market
    assert feed.stream_targets({"HSI", "BTC-HIT-10"}, {"41"}) == {"7": ("HSI", False), "41": ("BTC-HIT-10", True)}
    assert feed.stream_targets({"HSI", "BTC-HIT-10"}, set()) == {"7": ("HSI", False)} and feed.stream_targets(None, None) == {"7": ("HSI", False), "8": ("BTC", False)}
    # want(): the single markets first, at most PREDICT_WS_MAX_TOPICS
    many = {str(i): (f"L{i}", True) for i in range(100)}
    stream.want({**many, "7": ("HSI", False)})
    assert len(stream.wanted) == m.PREDICT_WS_MAX_TOPICS and list(stream.wanted)[0] == "7"
    # updated(): pushes arriving faster than a look takes are coalesced — one look under way, one more after it, never a queue
    calls = []
    async def slow(key):
        calls.append(key); await asyncio.sleep(0.05)
    feed.on_update = slow
    for _ in range(5):
        feed.updated("HSI")
    feed.updated("BTC")
    await asyncio.sleep(0.25)
    assert calls == ["HSI", "BTC", "HSI"], calls
    # the bot: the stream follows the round's hot set, and the round goes on without it
    class FakeMarket:
        def now_ms(self): return NOW
    bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}), m.Store(":memory:"), FakeMarket(), None)
    bot.predict.books["HSI"] = feed.books["HSI"]
    bot.predict_hot = lambda now: ({"HSI"}, set())
    bot.predict_targets = lambda now: {"HSI": HSI_SLUG}
    rounds = []
    async def fake_refresh(targets, force=False, hot=None, hot_rows=None):
        rounds.append((targets, hot, hot_rows)); return m.Refreshed("ok")
    bot.predict.refresh = fake_refresh
    assert isinstance(await bot.refresh_books(NOW), m.Refreshed) and rounds == [({"HSI": HSI_SLUG}, {"HSI"}, set())]
    assert bot.predict.stream.wanted == {"7": ("HSI", False)} and bot.sim_wake
    async def not_due(targets, force=False, hot=None, hot_rows=None): return False
    bot.predict.refresh = not_due; bot.predict.stream.wanted = {}
    assert await bot.refresh_books(NOW) is False and bot.predict.stream.wanted == {}, "a round not due changes nothing"
    print("STREAM_OK")


asyncio.run(run())
