"""1.87.0: BinanceStream / HyperliquidStream — the prices pushed as they trade, beside the REST reads.
1.89.0: the subscription check (LIST_SUBSCRIPTIONS, the missing names subscribed again), the per-pair ages in /diag, a
mute connection backing off, BINANCE_WS_URL. 1.89.1: the raw endpoint (/ws + SUBSCRIBE) beside the combined one, a mute
connection moving to the next address, the last connection's counts in /diag."""
import asyncio, sys, time, json
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

D = m.D
NOW = int(time.time() * 1000)


class FakeWs:
    def __init__(self, url):
        self.url, self.sent, self.incoming, self.closed = url, [], asyncio.Queue(), False

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


class FakeMarket(m.Binance):
    """The futures REST read: one contract, its last trade a second old, its mark price half a second old."""
    def __init__(self, config):
        super().__init__(config)
        self.calls = 0

    async def get(self, path, **params):
        self.calls += 1
        if path == "/fapi/v2/ticker/price":
            return [{"symbol": "UNITREEUSDT", "price": "70.5", "time": NOW - 1000}]
        if path == "/fapi/v1/premiumIndex":
            return [{"symbol": "UNITREEUSDT", "markPrice": "70.6", "time": NOW - 500, "indexPrice": "70.55"}]
        raise m.RemoteError("no such path: " + path)

    def now_ms(self):
        return NOW


async def run():
    m.PRICE_WS_BACKOFF = (0.01, 0.05)  # the tests do not wait seconds between connections
    m.PRICE_WS_STEADY_SECONDS = 0.0    # any connection that delivered a price counts as steady
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"})
    assert cfg.binance_ws and cfg.hl_ws
    market = FakeMarket(cfg)
    bot = m.Bot(cfg, m.Store(":memory:"), market, None)
    fut, spot, hl = bot.binance_streams["futures"], bot.binance_streams["spot"], bot.hl_stream
    assert market.stream is fut and fut.enabled and spot.enabled and hl.enabled
    # what each connection subscribes to: the contract with its mark price and HYPE's ladder on futures, the crypto cards' pairs on spot
    assert fut.symbols() == ["UNITREEUSDT", "HYPEUSDT"], fut.symbols()
    assert fut.urls() == ["wss://fstream.binance.com/ws", "wss://fstream.binance.com/stream?streams=unitreeusdt@aggTrade/hypeusdt@aggTrade/unitreeusdt@markPrice@1s/hypeusdt@markPrice@1s"]
    assert spot.symbols() == ["BNBUSDT", "SOLUSDT", "BTCUSDT", "ETHUSDT", "ZECUSDT"], spot.symbols()
    assert len(spot.urls()) == 4 and spot.urls()[0] == "wss://data-stream.binance.vision/stream?streams=bnbusdt@aggTrade/solusdt@aggTrade/btcusdt@aggTrade/ethusdt@aggTrade/zecusdt@aggTrade" \
        and spot.urls()[1].startswith("wss://stream.binance.com:9443/stream?streams=") and spot.urls()[2:] == ["wss://data-stream.binance.vision/ws", "wss://stream.binance.com:9443/ws"]
    assert fut.headers() == {"User-Agent": f"CloseAlert/{m.VERSION}"} and hl.headers() == {}
    assert hl.coins() == {"xyz:UNITREE": [("quote", "UNITREEUSDT")], "xyz:KR200": [("quote", "KR200")], "HYPE": [("flip", "HYPE-SOL")], "SOL": [("flip", "HYPE-SOL")]}, hl.coins()
    assert hl.urls() == [m.HL_WS_URL] and fut.line() == "尚未连接" and any(l.startswith("📡 行情推送：币安合约 尚未连接") for l in bot.diag_state(NOW))
    # before the connection the cycle reads REST, and the REST rows seed the stream
    assert fut.rows(["UNITREEUSDT"]) is None
    rows = await market.prices()
    assert market.calls == 2 and rows["UNITREEUSDT"]["price"] == "70.5" and fut.latest["UNITREEUSDT"] == {"symbol": "UNITREEUSDT", "price": "70.5", "time": NOW - 1000, "markPrice": "70.6", "markTime": NOW - 500, "indexPrice": "70.55"}
    assert fut.rows(["UNITREEUSDT"]) is None, "seeded, not pushed: nothing stands in for REST yet"
    # the connections
    conns = []
    async def connect(url, headers):
        assert headers == ({} if "hyperliquid" in url else {"User-Agent": f"CloseAlert/{m.VERSION}"}), (url, headers)
        ws = FakeWs(url); conns.append(ws); return ws
    fut.connect = spot.connect = hl.connect = connect
    stopping = asyncio.Event()
    tasks = [asyncio.create_task(s.run(stopping)) for s in (fut, spot, hl)]
    await asyncio.sleep(0.05)
    assert len(conns) == 3 and all(s.connected and s.connects == 1 for s in (fut, spot, hl))
    wsf = next(ws for ws in conns if "fstream" in ws.url); wss = next(ws for ws in conns if "data-stream" in ws.url); wsh = next(ws for ws in conns if "hyperliquid" in ws.url)
    assert [f["subscription"]["coin"] for f in wsh.sent] == ["xyz:UNITREE", "xyz:KR200", "HYPE", "SOL"] \
        and all(f["method"] == "subscribe" and f["subscription"]["type"] == "activeAssetCtx" for f in wsh.sent), wsh.sent
    # futures is on the raw endpoint: the streams are subscribed once connected, then the server is asked which it registered;
    # spot is on the combined endpoint (the names in the address): asked at once. The answers are no price and not dropped (1.89.0)
    names4 = ["unitreeusdt@aggTrade", "hypeusdt@aggTrade", "unitreeusdt@markPrice@1s", "hypeusdt@markPrice@1s"]
    assert fut.NAME == "币安合约推送" and spot.NAME == "币安现货推送" and fut.url == "wss://fstream.binance.com/ws" and wsf.sent == [{"method": "SUBSCRIBE", "params": names4, "id": 1}] \
        and wss.sent == [{"method": "LIST_SUBSCRIPTIONS", "id": 1}], (wsf.sent, wss.sent)
    assert fut.registered is None and "wss://fstream.binance.com/ws）" in fut.line() and "订阅清单未回；最近推送：HYPEUSDT 从未、UNITREEUSDT 从未" in fut.line(), fut.line()
    wsf.push({"result": None, "id": 1})
    await asyncio.sleep(0.02)
    assert wsf.sent[1] == {"method": "LIST_SUBSCRIPTIONS", "id": 2} and fut.registered is None
    wsf.push({"result": names4, "id": 2})
    await asyncio.sleep(0.02)
    assert fut.registered == names4 and fut.listed_connect == 1 and fut.answers == 2 and fut.dropped == 0 and fut.pushed == 0 and fut.frames == 2 and len(wsf.sent) == 2 and fut.asked == {}
    assert "服务器登记 4/4 个流；最近推送：" in fut.line() and "补订" not in fut.line(), fut.line()
    # the server registered two of spot's five names: the three missing are subscribed again, then the list is asked again (the answer may come wrapped)
    wss.push({"result": ["bnbusdt@aggTrade", "ethusdt@aggTrade"], "id": 1})
    await asyncio.sleep(0.02)
    assert wss.sent[1] == {"method": "SUBSCRIBE", "params": ["solusdt@aggTrade", "btcusdt@aggTrade", "zecusdt@aggTrade"], "id": 2} and spot.resubscribed == 3, wss.sent
    assert "服务器登记 2/5 个流（补订过 3 个）" in spot.line(), spot.line()
    wss.push({"result": None, "id": 2})
    await asyncio.sleep(0.02)
    assert wss.sent[2] == {"method": "LIST_SUBSCRIPTIONS", "id": 3} and len(wss.sent) == 3
    wss.push({"stream": "", "data": {"result": ["bnbusdt@aggTrade", "ethusdt@aggTrade", "solusdt@aggTrade", "btcusdt@aggTrade", "zecusdt@aggTrade"], "id": 3}})
    await asyncio.sleep(0.02)
    assert len(spot.registered) == 5 and len(wss.sent) == 3 and spot.answers == 3 and spot.dropped == 0 and spot.error == ""
    assert "服务器登记 5/5 个流（补订过 3 个）" in spot.line(), spot.line()
    # a refusal, in either shape, is noted (not dropped); a second list still short after the resubscription is noted too
    wsf.push({"error": {"code": 2, "msg": "Invalid request: unknown"}, "id": 1}); wsf.push({"code": 0, "msg": "Unknown property", "id": 77})
    await asyncio.sleep(0.02)
    assert fut.error == "请求 被拒绝：0 Unknown property" and fut.answers == 4 and fut.dropped == 0, fut.error
    spot.fixing, spot.asked = True, {9: "LIST_SUBSCRIPTIONS"}
    wss.push({"result": ["bnbusdt@aggTrade"], "id": 9})
    await asyncio.sleep(0.02)
    assert spot.error.startswith("补订后服务器仍未登记 4 个流：solusdt@aggTrade") and len(wss.sent) == 3, spot.error
    spot.error = ""
    # a mark price alone keeps the row fresh; with the seeded last trade the cycle takes the pushed rows, no REST request
    wsf.push({"stream": "unitreeusdt@markPrice@1s", "data": {"e": "markPriceUpdate", "E": NOW, "s": "UNITREEUSDT", "p": "70.7", "i": "70.65", "P": "70.7", "r": "0.0001", "T": NOW + 3600_000}})
    await asyncio.sleep(0.02)
    rows = fut.rows(["UNITREEUSDT"])
    assert rows == {"UNITREEUSDT": {"symbol": "UNITREEUSDT", "price": "70.5", "time": NOW - 1000, "markPrice": "70.7", "markTime": NOW, "indexPrice": "70.65"}}, rows
    assert fut.line().endswith("最近推送：UNITREEUSDT 0 秒前、HYPEUSDT 从未"), fut.line()  # the freshest first, never pushed last
    assert await market.prices() == rows and market.calls == 2 and fut.rows(["UNITREEUSDT", "OTHER"]) is None
    # a trade: the contract's quote between cycles, the baseline kept; the key looked at after the gathering delay
    bot.snapshots["UNITREEUSDT"] = {"quote": m.Quote.parse(rows["UNITREEUSDT"], "UNITREEUSDT", NOW, cfg.max_age), "baseline": "kept"}
    looked = []
    async def hook(key): looked.append(key)
    bot.predict.on_update = hook
    for key in ("UNITREEUSDT", "KOSPI", "HYPE-SOL", "BNB", "BTC", "BTC-2026-10"):
        bot.predict.books[key] = m.PredictBook(key, f"{key.lower()}-slug", str(hash(key) % 1000), key, (), (), NOW)
    bot.predict.ladders["HYPE-HIT-10"] = bot.predict.ladders["BTC-HIT-10"] = []
    wsf.push({"stream": "unitreeusdt@aggTrade", "data": {"e": "aggTrade", "E": NOW + 10, "s": "UNITREEUSDT", "a": 1, "p": "71.2", "q": "3", "f": 1, "l": 1, "T": NOW + 10, "m": False}})
    await asyncio.sleep(0.02)
    q = bot.snapshots["UNITREEUSDT"]["quote"]
    assert q.price == D("71.2") and q.timestamp_ms == NOW + 10 and q.source == "last" and bot.snapshots["UNITREEUSDT"]["baseline"] == "kept"
    assert looked == [] and bot.price_dirty == {"UNITREEUSDT"}, "gathered, not looked at yet"
    await asyncio.sleep(0.3)
    assert looked == ["UNITREEUSDT"] and not bot.price_dirty and fut.pushed == 2
    # the mark price while the trade is fresh moves nothing: no look
    wsf.push({"stream": "unitreeusdt@markPrice@1s", "data": {"e": "markPriceUpdate", "E": NOW + 1000, "s": "UNITREEUSDT", "p": "70.9", "i": "70.8"}})
    await asyncio.sleep(0.3)
    assert looked == ["UNITREEUSDT"] and bot.snapshots["UNITREEUSDT"]["quote"].price == D("71.2") and fut.latest["UNITREEUSDT"]["markPrice"] == "70.9"
    # HYPE on futures: the ladder's price, its REST price read deferred, the look on its key
    hype = bot.ranges["HYPE-HIT-10"]
    wsf.push({"stream": "hypeusdt@aggTrade", "data": {"e": "aggTrade", "s": "HYPEUSDT", "p": "31.5", "T": NOW + 20, "E": NOW + 20}})
    await asyncio.sleep(0.3)
    assert hype.price == D("31.5") and hype.priced_ms == NOW and time.monotonic() - hype.times["price"] < 1 and looked[-1] == "HYPE-HIT-10"
    # spot: the touch cards (at a line the path check is due at once), the monthly card, the ladders — in one gathered look
    bnb, btc_t, btc_u, btc_r, btc_y = bot.touches["BNB"], bot.touches["BTC"], bot.updowns["BTC-2026-10"], bot.ranges["BTC-HIT-10"], bot.ranges["BTC-HIT-2027"]
    bnb.times["scan"] = btc_t.times["scan"] = 1.0
    n = len(looked)
    wss.push({"stream": "bnbusdt@aggTrade", "data": {"e": "aggTrade", "s": "BNBUSDT", "p": "812.3", "T": NOW + 30, "E": NOW + 30}})
    wss.push({"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "s": "BTCUSDT", "p": "90500", "T": NOW + 31, "E": NOW + 31}})
    await asyncio.sleep(0.02)
    assert bnb.price == D("812.3") and bnb.priced_ms == NOW and bnb.times["scan"] == 1.0, "between the lines: the path check keeps its time"
    assert btc_t.price == D("90500") and btc_t.times["scan"] == -1e9, "at the $90k line: the path check is due at once"
    assert btc_u.price == D("90500") and btc_r.price == D("90500") and btc_y.price == D("90500") and btc_r.failures["price"] == ""
    assert bot.price_dirty == {"BNB", "BTC", "BTC-2026-10", "BTC-HIT-10", "BTC-HIT-2027"}, bot.price_dirty
    await asyncio.sleep(0.3)
    assert looked[n:] == ["BNB", "BTC", "BTC-2026-10", "BTC-HIT-10"], looked[n:]  # BTC-HIT-2027 has no book or ladder on this feed: no look
    # the REST price read is deferred by a push: a refresh within PRICE_SECONDS asks nothing
    asked = []
    async def no_get(path, **p):
        asked.append(path); raise m.RemoteError("not now")
    bnb.get = no_get
    bnb.sigma, bnb.sigma_ms = 0.5, NOW  # σ in hand (without one the hourly read is due at once), the path check not due
    bnb.times["vol"] = bnb.times["scan"] = time.monotonic()
    await bnb.refresh(NOW)
    assert asked == [] and bnb.error == ""
    # Hyperliquid: KR200's context is the quote the KOSPI card prices from after hours; a flip coin's mid prices the pair once both are in
    wsh.push({"channel": "subscriptionResponse", "data": {"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": "xyz:KR200"}}})
    wsh.push({"channel": "activeAssetCtx", "data": {"coin": "xyz:KR200", "ctx": {"funding": "0.00001", "openInterest": "10", "prevDayPx": "1120", "dayNtlVlm": "1",
                                                                                 "premium": "0", "oraclePx": "1125.5", "markPx": "1126.1", "midPx": "1126.0", "impactPxs": ["1125.9", "1126.1"]}}})
    await asyncio.sleep(0.3)
    q = bot.hl.quotes["KR200"]
    assert q.coin == "xyz:KR200" and q.mark == D("1126.1") and q.mid == D("1126.0") and q.oracle == D("1125.5") and q.prev_day == D("1120") and q.funding == D("0.00001") and q.fetched_ms >= NOW
    assert looked[-1] == "KOSPI" and hl.pushed == 1 and "KR200" not in bot.hl.notes
    flip = bot.flips["HYPE-SOL"]
    wsh.push({"channel": "activeAssetCtx", "data": {"coin": "HYPE", "ctx": {"markPx": "30.1", "midPx": "30.05"}}})
    await asyncio.sleep(0.3)
    assert flip.prices == {"HYPE": D("30.05")} and flip.priced_ms == 0 and looked[-1] == "KOSPI", "half a pair prices nothing yet"
    wsh.push({"channel": "activeAssetCtx", "data": {"coin": "SOL", "ctx": {"markPx": "140.2", "midPx": "140.15"}}})
    await asyncio.sleep(0.3)
    assert flip.prices == {"HYPE": D("30.05"), "SOL": D("140.15")} and flip.priced_ms == NOW and time.monotonic() - flip.times["price"] < 1 and looked[-1] == "HYPE-SOL"
    # the ping; pong and subscription answers are nothing, an error is noted
    await hl.tick(wsh)
    assert wsh.sent[-1] == {"method": "ping"}
    wsh.push({"channel": "pong"}); wsh.push({"channel": "error", "data": "Invalid subscription activeAssetCtx"})
    await asyncio.sleep(0.02)
    assert hl.error.startswith("Invalid subscription") and hl.dropped == 0 and hl.frames == 6
    # frames not understood are dropped, nothing else happens
    wsf.push("garbage"); wsf.push({"stream": "x", "data": {"e": "aggTrade"}}); wsf.push({"stream": "y", "data": {"e": "kline", "s": "BTCUSDT"}})
    wsh.push({"channel": "activeAssetCtx", "data": {"coin": "SOL", "ctx": {"markPx": "n/a"}}})
    await asyncio.sleep(0.02)
    assert fut.dropped == 3 and hl.dropped == 1
    line = fut.line()
    assert "已连接" in line and "收到 4 次行情" in line and "丢弃 3 帧" in line and __import__("re").search(r"最近推送：HYPEUSDT \d 秒前、UNITREEUSDT \d 秒前$", line), line
    assert any(l.startswith("📡 行情推送：币安合约 已连接") and "币安现货 已连接" in l and "Hyperliquid 已连接" in l for l in bot.diag_state(NOW))
    # a broken connection: the rows no longer stand in, the cycle reads REST again; made anew after the backoff
    m.PRICE_WS_STEADY_SECONDS, fut.backoff = 60.0, 0.05  # this connection is too short to count as steady: the state is checked before the next one
    wsf.push(ConnectionError("gone"))
    await asyncio.sleep(0.02)
    assert not fut.connected and fut.rows(["UNITREEUSDT"]) is None
    line = fut.line()
    assert __import__("re").search(r"^未连接（上次断开：gone；上次连接 wss://fstream.binance.com/ws \d 秒、收到 \d+ 帧、4 次行情；0 秒后重连）；丢弃 3 帧；"
                                   r"上次清单：服务器登记 4/4 个流（第 1 次连接）；最近推送：", line), line
    calls = market.calls
    await market.prices()
    assert market.calls == calls + 2
    await asyncio.sleep(0.3)
    assert fut.connected and fut.connects == 2 and "gone" in fut.closed_why and fut.backoff == 0.05 and fut.host_index == 0, "it delivered: the same address again"
    m.PRICE_WS_STEADY_SECONDS = 0.0
    wsf2 = conns[-1]
    assert wsf2 is not wsf and wsf2.url == "wss://fstream.binance.com/ws" and wsf2.sent == [{"method": "SUBSCRIBE", "params": names4, "id": 3}] \
        and fut.asked == {3: "SUBSCRIBE"} and "订阅清单未回（第 1 次连接时服务器登记 4/4 个流）" in fut.line(), (wsf2.sent, fut.line())
    # the raw endpoint's payloads come bare
    wsf2.push({"e": "aggTrade", "E": NOW + 40, "s": "UNITREEUSDT", "p": "71.5", "T": NOW + 40})
    await asyncio.sleep(0.02)
    assert fut.latest["UNITREEUSDT"]["price"] == "71.5" and fut.pushed == 5 and fut.dropped == 3
    # silence: no frame for SILENT_SECONDS, the connection is made anew
    m.HyperliquidStream.SILENT_SECONDS = 0.1
    wsh.push({"channel": "pong"})  # the wait under way was given the old limit: one frame, then the new one applies
    await asyncio.sleep(0.5)
    assert hl.connects >= 2 and "没有收到任何帧" in hl.closed_why, (hl.connects, hl.closed_why)
    assert hl.backoff >= 0.02, "a mute connection (made, nothing pushed) backs off like a failed one"
    m.HyperliquidStream.SILENT_SECONDS = 60.0
    # a host that cannot be reached: the next one is tried after the backoff
    spot_urls = []
    async def refuse(url, headers):
        spot_urls.append(url); raise m.RemoteError("网络错误 (ConnectionRefusedError)")
    spot.connect = refuse
    wss.push(ConnectionError("gone"))
    await asyncio.sleep(0.4)
    assert len(spot_urls) >= 2 and spot_urls[0].startswith("wss://data-stream.binance.vision") and spot_urls[1].startswith("wss://stream.binance.com:9443"), spot_urls
    assert "未连接" in spot.line() and "网络错误" in spot.error
    # the stop
    spot.connect = connect
    stopping.set()
    await asyncio.wait_for(asyncio.gather(*tasks), 3)
    assert not any(s.connected for s in (fut, spot, hl)) and all(ws.closed for ws in conns)
    # off, and nothing to stream: the lines say so, no connection is made
    off = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "BINANCE_WS": "off", "HL_WS": "off", "BNB_TOUCH": "off", "HL_TICKERS": "off", "HL_INDEX": "off"})
    obot = m.Bot(off, m.Store(":memory:"), FakeMarket(off), None)
    assert not obot.binance_streams["spot"].enabled and obot.binance_streams["spot"].line() == "关" and not obot.hl_stream.enabled
    assert obot.binance_streams["spot"].symbols() == [] and obot.hl_stream.coins() == {} and obot.binance_streams["futures"].symbols() == list(off.symbols)
    on = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "BNB_TOUCH": "off", "HL_TICKERS": "off", "HL_INDEX": "off"})
    nbot = m.Bot(on, m.Store(":memory:"), FakeMarket(on), None)
    assert nbot.binance_streams["spot"].line() == "没有要订阅的市场" and nbot.hl_stream.urls() == []
    stop2 = asyncio.Event(); run2 = asyncio.create_task(nbot.hl_stream.run(stop2)); await asyncio.sleep(0.05); stop2.set(); await asyncio.wait_for(run2, 2)
    assert nbot.hl_stream.connects == 0
    # a mute connection doubles the wait; one that delivered puts it back to the shortest
    mute = nbot.binance_streams["futures"]
    mconns = []
    async def mconnect(url, headers):
        ws = FakeWs(url); mconns.append(ws); return ws
    mute.connect, mute.backoff = mconnect, 0.04
    stop3 = asyncio.Event(); run3 = asyncio.create_task(mute.run(stop3)); await asyncio.sleep(0.05)
    assert mconns[0].url == "wss://fstream.binance.com/ws" and mconns[0].sent[0]["method"] == "SUBSCRIBE"
    mconns[0].push({"stream": "unitreeusdt@markPrice@1s", "data": {"e": "markPriceUpdate", "E": NOW, "s": "UNITREEUSDT", "p": "70.7"}})
    mconns[0].push(ConnectionError("gone"))
    await asyncio.sleep(0.1)
    assert mute.backoff == 0.01 and len(mconns) == 2 and mute.pushed == 1 and mconns[1].url == mconns[0].url, (mute.backoff, len(mconns))
    mconns[1].push(ConnectionError("gone"))
    await asyncio.sleep(0.1)
    assert mute.backoff == 0.02 and len(mconns) == 3 and mute.last_session[1:] == (0, 0), (mute.backoff, len(mconns))
    # a mute connection moves to the next address: the combined endpoint, the names in the address, the list asked at once
    assert mconns[2].url.startswith("wss://fstream.binance.com/stream?streams=unitreeusdt@aggTrade/") and mconns[2].sent == [{"method": "LIST_SUBSCRIPTIONS", "id": mute.request_id}], mconns[2].sent
    mconns[2].push(ConnectionError("gone"))
    await asyncio.sleep(0.1)
    assert len(mconns) == 4 and mconns[3].url == "wss://fstream.binance.com/ws", "round again"
    stop3.set(); await asyncio.wait_for(run3, 2)
    # BINANCE_WS_URL: the futures stream's address(es), the query appended; the spot addresses stay
    relay = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "BINANCE_WS_URL": " wss://relay.example/stream/ , wss://fstream.binance.com/ws, ws://10.0.0.5:8080/relay"})
    assert relay.binance_ws_url == ("wss://relay.example", "wss://fstream.binance.com", "ws://10.0.0.5:8080/relay")
    rbot = m.Bot(relay, m.Store(":memory:"), FakeMarket(relay), None)
    rurls = rbot.binance_streams["futures"].urls()
    assert rurls[:3] == ["wss://relay.example/ws", "wss://fstream.binance.com/ws", "ws://10.0.0.5:8080/relay/ws"] and len(rurls) == 6 \
        and rurls[3].startswith("wss://relay.example/stream?streams=unitreeusdt@aggTrade/") and rurls[5].startswith("ws://10.0.0.5:8080/relay/stream?streams=") \
        and rbot.binance_streams["spot"].urls()[0].startswith("wss://data-stream.binance.vision/"), rurls
    assert cfg.binance_ws_url == () and fut.bases() == m.BINANCE_WS_FUTURES
    for bad in ("https://relay.example/stream", "wss://relay.example/stream?streams=a", "wss://user@relay.example/stream", "relay.example"):
        try:
            m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "BINANCE_WS_URL": bad}); raise AssertionError(bad)
        except ValueError as error:
            assert "BINANCE_WS_URL" in str(error), error
    assert m.age_text(5) == "5 秒前" and m.age_text(599) == "599 秒前" and m.age_text(600) == "10 分前" and m.age_text(36000) == "10 小时前" and m.age_text(-3) == "0 秒前"
    # a bot without the streams' attributes (the tests' stubs) still samples: prices() reads REST
    bare = FakeMarket(on)
    assert bare.stream is None and (await bare.prices())["UNITREEUSDT"]["price"] == "70.5"
    print("PRICEWS_OK")


asyncio.run(run())
