"""1.87.0: BinanceStream / HyperliquidStream — the prices pushed as they trade, beside the REST reads."""
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
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"})
    assert cfg.binance_ws and cfg.hl_ws
    market = FakeMarket(cfg)
    bot = m.Bot(cfg, m.Store(":memory:"), market, None)
    fut, spot, hl = bot.binance_streams["futures"], bot.binance_streams["spot"], bot.hl_stream
    assert market.stream is fut and fut.enabled and spot.enabled and hl.enabled
    # what each connection subscribes to: the contract with its mark price and HYPE's ladder on futures, the crypto cards' pairs on spot
    assert fut.symbols() == ["UNITREEUSDT", "HYPEUSDT"], fut.symbols()
    assert fut.urls() == ["wss://fstream.binance.com/stream?streams=unitreeusdt@aggTrade/hypeusdt@aggTrade/unitreeusdt@markPrice@1s/hypeusdt@markPrice@1s"]
    assert spot.symbols() == ["BNBUSDT", "SOLUSDT", "BTCUSDT", "ETHUSDT", "ZECUSDT"], spot.symbols()
    assert len(spot.urls()) == 2 and spot.urls()[0] == "wss://data-stream.binance.vision/stream?streams=bnbusdt@aggTrade/solusdt@aggTrade/btcusdt@aggTrade/ethusdt@aggTrade/zecusdt@aggTrade" \
        and spot.urls()[1].startswith("wss://stream.binance.com:9443/stream?streams=")
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
        assert headers == {}
        ws = FakeWs(url); conns.append(ws); return ws
    fut.connect = spot.connect = hl.connect = connect
    stopping = asyncio.Event()
    tasks = [asyncio.create_task(s.run(stopping)) for s in (fut, spot, hl)]
    await asyncio.sleep(0.05)
    assert len(conns) == 3 and all(s.connected and s.connects == 1 for s in (fut, spot, hl))
    wsf = next(ws for ws in conns if "fstream" in ws.url); wss = next(ws for ws in conns if "data-stream" in ws.url); wsh = next(ws for ws in conns if "hyperliquid" in ws.url)
    assert [f["subscription"]["coin"] for f in wsh.sent] == ["xyz:UNITREE", "xyz:KR200", "HYPE", "SOL"] \
        and all(f["method"] == "subscribe" and f["subscription"]["type"] == "activeAssetCtx" for f in wsh.sent), wsh.sent
    # a mark price alone keeps the row fresh; with the seeded last trade the cycle takes the pushed rows, no REST request
    wsf.push({"stream": "unitreeusdt@markPrice@1s", "data": {"e": "markPriceUpdate", "E": NOW, "s": "UNITREEUSDT", "p": "70.7", "i": "70.65", "P": "70.7", "r": "0.0001", "T": NOW + 3600_000}})
    await asyncio.sleep(0.02)
    rows = fut.rows(["UNITREEUSDT"])
    assert rows == {"UNITREEUSDT": {"symbol": "UNITREEUSDT", "price": "70.5", "time": NOW - 1000, "markPrice": "70.7", "markTime": NOW, "indexPrice": "70.65"}}, rows
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
    assert "已连接" in line and "收到 4 次行情" in line and "丢弃 3 帧" in line, line
    assert any(l.startswith("📡 行情推送：币安合约 已连接") and "币安现货 已连接" in l and "Hyperliquid 已连接" in l for l in bot.diag_state(NOW))
    # a broken connection: the rows no longer stand in, the cycle reads REST again; made anew after the backoff
    wsf.push(ConnectionError("gone"))
    await asyncio.sleep(0.02)
    assert not fut.connected and fut.rows(["UNITREEUSDT"]) is None
    calls = market.calls
    await market.prices()
    assert market.calls == calls + 2
    await asyncio.sleep(0.3)
    assert fut.connected and fut.connects == 2 and "gone" in fut.closed_why
    # silence: no frame for SILENT_SECONDS, the connection is made anew
    m.HyperliquidStream.SILENT_SECONDS = 0.1
    wsh.push({"channel": "pong"})  # the wait under way was given the old limit: one frame, then the new one applies
    await asyncio.sleep(0.5)
    assert hl.connects >= 2 and "没有收到任何帧" in hl.closed_why, (hl.connects, hl.closed_why)
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
    # a bot without the streams' attributes (the tests' stubs) still samples: prices() reads REST
    bare = FakeMarket(on)
    assert bare.stream is None and (await bare.prices())["UNITREEUSDT"]["price"] == "70.5"
    print("PRICEWS_OK")


asyncio.run(run())
