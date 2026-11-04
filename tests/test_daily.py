"""1.97.0: the daily crypto up/down markets — BTC / ETH / BNB "Up or Down on <date>", a new market every day: Up when the
Binance 1-minute candle of <date> 12:00 ET closes above the one of the day before at 12:00 ET, Down when below, 50-50 when
equal. One card per coin follows them: it moves to the next day's market once the day's final candle is read, and a
position settles on its own day's closes. In beta the trader buys at most SIM_CRYPTO_DAY_SHARES (10) at a time, holds at
most SIM_CRYPTO_DAY_MAX_SHARES (100) on one market, both sides, and buys nothing in its last SIM_CRYPTO_DAY_QUIET_MINUTES (30)."""
import asyncio, math, sys, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
D = m.D
H, MIN = 3_600_000, 60_000
utc = lambda *a: int(dt.datetime(*a, tzinfo=dt.timezone.utc).timestamp() * 1000)
et = m.et_ms

# --- the markets as their rules state them ---------------------------------------------------------------------------------
BTC, ETH, BNB = m.DAILY_UPDOWN_MARKETS
assert [(s.key, s.stem, s.symbol, s.coin, s.hour, s.minute) for s in m.DAILY_UPDOWN_MARKETS] == [
    ("BTC-DAY", "bitcoin", "BTCUSDT", "BTC", 12, 0), ("ETH-DAY", "ethereum", "ETHUSDT", "ETH", 12, 0), ("BNB-DAY", "bnb", "BNBUSDT", "BNB", 12, 0)]
NOV4, NOV5 = dt.date(2026, 11, 4), dt.date(2026, 11, 5)
SPEC = BTC.day_spec(NOV4)
# "the Binance 1 minute candle for BTC/USDT Nov 3 '26 12:00 in the ET timezone (noon)" against "the Nov 4 '26 12:00 ET
# candle": standard time at both ends (daylight time ended on 11-01)
assert (SPEC.key, SPEC.slug, SPEC.symbol, SPEC.name) == ("BTC-DAY", "bitcoin-up-or-down-on-november-4-2026", "BTCUSDT", "BTC 11-04 涨跌")
assert SPEC.start_ms == utc(2026, 11, 3, 17, 0) and SPEC.end_ms == utc(2026, 11, 4, 17, 0)
assert SPEC.label(SPEC.start_ms) == "11-03 12:00 ET（北京 11-04 01:00）" and SPEC.label(SPEC.end_ms) == "11-04 12:00 ET（北京 11-05 01:00）"
assert [s.day_spec(NOV4).slug for s in (ETH, BNB)] == ["ethereum-up-or-down-on-november-4-2026", "bnb-up-or-down-on-november-4-2026"]
assert [s.day_spec(NOV4).name for s in (ETH, BNB)] == ["ETH 11-04 涨跌", "BNB 11-04 涨跌"]
# each candle at its own date's ET offset: over a daylight-time switch the window is 25 or 23 hours
NOV1 = BTC.day_spec(dt.date(2026, 11, 1))
assert NOV1.start_ms == utc(2026, 10, 31, 16, 0) and NOV1.end_ms == utc(2026, 11, 1, 17, 0) and NOV1.end_ms - NOV1.start_ms == 25 * H
MAR14 = BTC.day_spec(dt.date(2027, 3, 14))  # daylight time starts 2027-03-14 02:00
assert MAR14.end_ms == utc(2027, 3, 14, 16, 0) and MAR14.end_ms - MAR14.start_ms == 23 * H
assert m.DailyUpDownSpec.day_of(SPEC) == NOV4 and m.DailyUpDownSpec.day_of(NOV1) == dt.date(2026, 11, 1)
# the market a card follows: the one whose final candle has not closed yet
EVENING = et(2026, 11, 3, 20, 16, -5)  # 11-03 20:16 ET (北京 11-04 09:16), when these markets were added
assert BTC.current(EVENING) == SPEC and BTC.current(utc(2026, 11, 3, 17, 1)) == SPEC  # from 11-03 12:01 ET on
assert BTC.current(SPEC.end_ms + 59_999) == SPEC  # the final minute still running
assert BTC.current(SPEC.end_ms + 60_000) == BTC.day_spec(NOV5) and BTC.day_spec(NOV5).start_ms == SPEC.end_ms  # next: from that candle
# their keys are their own (the favourite, /edge and SIM_SKIP keep to the card through the days)
others = {s.key for s in (*m.TOUCH_MARKETS, *m.UPDOWN_MARKETS, *m.FLIP_MARKETS, *m.CAP_MARKETS, *m.RANGE_MARKETS, *m.STOCK_HIT_MARKETS)}
assert not others & {s.key for s in m.DAILY_UPDOWN_MARKETS}
assert m.market_driver("updown", "BTC-DAY") == ("BTCUSDT", "BTC") and m.market_driver("updown", "BNB-DAY") == ("BNBUSDT", "BNB")
assert m.SIM_KINDS["updown"] == "加密涨跌"

# --- the beta settings -----------------------------------------------------------------------------------------------------
c = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"})
assert (c.sim_crypto_day_shares, c.sim_crypto_day_max_shares, c.sim_crypto_day_quiet) == (10, 100, 30)
c = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_CRYPTO_DAY_SHARES": "20", "SIM_CRYPTO_DAY_MAX_SHARES": "0",
                       "SIM_CRYPTO_DAY_QUIET_MINUTES": "45"})
assert (c.sim_crypto_day_shares, c.sim_crypto_day_max_shares, c.sim_crypto_day_quiet) == (20, 0, 45)
for bad in ({"SIM_CRYPTO_DAY_SHARES": "-1"}, {"SIM_CRYPTO_DAY_MAX_SHARES": "x"}, {"SIM_CRYPTO_DAY_QUIET_MINUTES": "1441"}):
    try:
        m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", **bad}); assert False, bad
    except ValueError:
        pass
KEYS = ("SIM_CRYPTO_DAY_SHARES", "SIM_CRYPTO_DAY_MAX_SHARES", "SIM_CRYPTO_DAY_QUIET_MINUTES")
assert set(KEYS) <= m.CONTROL_KEY_SET and all(m.CONTROL_FORMS[k]["kind"] == "number" for k in KEYS)  # the control page changes them


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


ENV = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "SIM_MARKETS": "updown",
       "SIM_WAYS": "both", "SIM_MAKER_MIN_BID": "0", "SIM_MAKER_EXIT_CENTS": "0", "SIM_MAKER_SESSION": "off", "SIM_MAKER_POINTS": "off"}


def binance(closes: dict, clock: list, price: str = "70500"):
    """A fake Binance spot for one pair: the 1-minute candle of each open time in ``closes`` (none for the rest, as Binance
    answers for a minute it has not got), the price, and hourly candles up to ``clock[0]`` (closes alternating ±0.4%)."""
    async def get(path, **params):
        if path == "ticker/price":
            return {"symbol": params["symbol"], "price": price}
        if params["interval"] == "1m":
            o = params["startTime"]
            return [[o, closes[o], closes[o], closes[o], closes[o], "1", o + 59_999, "0", 1, "0", "0", "0"]] if o in closes else []
        last = clock[0] - clock[0] % H
        return [[t, "1", "1", "1", f"{70000 * (1.004 if t // H % 2 else 1.0):.2f}", "1", t + H - 1, "0", 1, "0", "0", "0"]
                for t in range(last - params["limit"] * H, last, H)]
    return get


def book(slug, bids, asks, key="BTC-DAY", at=EVENING):
    return m.PredictBook(key, slug, "501", "t", tuple((D(p), D(q)) for p, q in bids), tuple((D(p), D(q)) for p, q in asks), at, None)


async def run():
    store = m.Store(":memory:")
    bot = m.Bot(m.Config.from_env(ENV), store, FM(EVENING), None)
    # --- the cards: one per coin, on the market whose final candle is next --------------------------------------------------
    assert list(bot.updowns) == ["BTC-DAY", "ETH-DAY", "BNB-DAY"] and bot.updowns["BTC-DAY"].spec == SPEC
    targets = bot.predict_targets(EVENING)
    assert {k: targets[k] for k in ("BTC-DAY", "ETH-DAY", "BNB-DAY")} == {s.key: s.day_spec(NOV4).slug for s in m.DAILY_UPDOWN_MARKETS}
    assert {s.day_spec(NOV4).slug for s in m.DAILY_UPDOWN_MARKETS} <= bot.predict.want_info  # the outcome names: which side is Up
    assert [(r["key"], r["name"]) for r in bot.sim_catalog(EVENING) if r["kind"] == "updown"] == [
        ("BTC-DAY", "BTC 11-04 涨跌"), ("ETH-DAY", "ETH 11-04 涨跌"), ("BNB-DAY", "BNB 11-04 涨跌")]
    assert [rm.spec.key for rm in bot.pair_markets[("spot", "BNBUSDT")]] == ["BNB", "BNB-DAY"]  # pushed BNB trades price both cards
    assert {"BTCUSDT", "ETHUSDT", "BNBUSDT"} <= set(bot.binance_streams["spot"].symbols())
    btc, eth = bot.updowns["BTC-DAY"], bot.updowns["ETH-DAY"]
    clock, closes = [EVENING], {SPEC.start_ms: "70000"}
    btc.get = binance(closes, clock)
    await btc.refresh(EVENING)
    assert btc.error == "" and btc.close_of("start") == D("70000") and btc.close_of("end") is None and btc.price == D("70500")
    odds = btc.odds(EVENING)
    assert isinstance(odds, m.UpDownOdds) and 0.55 < odds.fair_up < 0.8 and abs(odds.years * 365 * 24 - (SPEC.end_ms + MIN - EVENING) / H) < 1e-9
    # the book: priced as Up whatever the outcome order, and only this day's (the day before's is no book for it)
    bot.predict.info[SPEC.slug] = {"outcomes": ["Up", "Down"], "created_ms": 0}
    bot.predict.books["BTC-DAY"] = book(SPEC.slug, [("0.35", "500")], [("0.40", "500"), ("0.45", "500")])
    assert bot.updown_book(SPEC)[0] is bot.predict.books["BTC-DAY"]
    bot.predict.books["BTC-DAY"] = book("bitcoin-up-or-down-on-november-3-2026", [("0.35", "500")], [("0.40", "500")])
    assert bot.updown_book(SPEC) == (None, "")
    bot.predict.books["BTC-DAY"] = book(SPEC.slug, [("0.35", "500")], [("0.40", "500"), ("0.45", "500")])
    card = bot.updown_payload(btc, EVENING)
    assert (card["name"], card["symbol"], card["group"], card["close_ms"]) == ("BTC 11-04 涨跌", "BTC-DAY", "crypto", SPEC.end_ms + MIN), card
    assert card["close_label"] == "11-04 12:00 ET（北京 11-05 01:00）这根 1 分钟 K 的收盘价；高于起点为涨、低于为跌、相同按 50/50 结算"
    assert card["ref"] == "70,000.00" and card["ref_note"].startswith("币安 BTCUSDT 1 分钟 K 收盘：11-03 12:00 ET，北京 11-04 01:00"), card["ref_note"]
    assert card["predict"]["url"].startswith("https://predict.fun/zh-cn/market/bitcoin-up-or-down-on-november-4-2026")

    # --- what the trader sees: the day's market, marked for the beta limits -----------------------------------------------------
    mk = next(x for x in bot.sim_markets(EVENING) if x.key == "BTC-DAY")
    assert (mk.kind, mk.market, mk.item, mk.sides) == ("updown", SPEC.slug, "BTC 11-04 涨跌", ("涨", "跌"))
    assert mk.settle == {"end": SPEC.end_ms, "start": SPEC.start_ms, "day": "2026-11-04"} and mk.fair_up == odds.fair_up
    NONE = m.TradeMap()
    assert bot.sim_order_cap(mk) == 10 and bot.sim_market_room(NONE, mk, "up") == (100, "")
    other = m.dataclasses.replace(mk, kind="updown", settle={"end": SPEC.end_ms})  # a monthly up/down market: none of it
    assert bot.sim_order_cap(other) == math.inf and bot.sim_market_room(NONE, other, "up") == (math.inf, "") and bot.sim_crypto_day_version(other) == {}
    # a taker sweeps every ask under its cap (SIM_TAKER_SWEEP_SHARES 300) — here no more than a buy: 10 shares
    cap = bot.sim_taker_cap(mk, "up", 200)
    assert cap >= 0.45 and bot.sim_taker_shares(mk) == 10 and bot.sim_taker_want(mk, "up", cap) == 10
    trades, changed = bot.sim_trades(), []
    bot.sim_take(mk, trades, EVENING, changed)
    taker = trades[f"{SPEC.slug}|up|吃"]
    assert (taker["status"], taker["order"], taker["shares"], taker["price"] < 0.41) == ("filled", 10, 10, True), taker
    assert taker["settle"] == mk.settle and taker["driver"] == "BTCUSDT"
    assert {k: v for k, v in taker["version"].items() if k.startswith("sim_crypto_day_")} == {
        "sim_crypto_day_shares": 10, "sim_crypto_day_max_shares": 100, "sim_crypto_day_quiet": 30, "sim_crypto_day_edge": 0}
    # a resting order: 10 shares, not SIM_SHARES' 100
    bot.sim_quote(mk, trades, EVENING, changed)
    resting = [t for t in trades.values() if t.get("maker")]
    assert len(resting) == 1 and resting[0]["order"] == 10 and resting[0]["status"] == "resting", resting

    # --- the cap, each side its own (1.98.0): its positions and orders, in flight or resting, deep or not ---------------------
    base = {"v": 2, "market": SPEC.slug, "slug": SPEC.slug, "item": SPEC.name, "kind": "updown", "key": "BTC-DAY", "fills": [],
            "revisions": [], "settle": mk.settle, "driver": "BTCUSDT", "driver_name": "BTC", "opened": EVENING - H}
    fill = lambda side, n, tid, price: (f"{SPEC.slug}|{side}|吃#{tid}", {**base, "side": side, "label": "吃" + ("涨" if side == "up" else "跌"),
                                                                        "maker": False, "status": "filled", "shares": float(n), "order": float(n), "price": price})
    held = m.TradeMap(dict([fill("up", 60, 1, 0.40), fill("down", 100, 1, 0.55),
                            (f"{SPEC.slug}|up|挂", {**base, "side": "up", "label": "挂涨", "maker": True, "status": "resting", "shares": 5.0,
                                                    "order": 35.0, "price": 0.36})]))
    # 涨 holds 60 + 35 = 95: 5 left; 跌's 100 neither counts against it nor offsets it
    assert bot.sim_market_room(held, mk, "up") == (5.0, "")
    q = m.taker_quote(mk.book, "up", 10, 200, cap)
    picked, why = bot.sim_taker_pick(mk, held, "up", q, EVENING)
    assert why == "" and picked["got"] == 5, (picked, why)  # cut to what is left, not refused
    maker = m.BookEdge("涨", True, 0.36, 0.2, 500)
    assert bot.sim_maker_pick(mk, held, "up", maker, EVENING) == (5.0, "")
    deep = m.dataclasses.replace(maker, front=True)  # a deep-bid order too: no order outside this cap
    assert bot.sim_maker_pick(mk, held, "up", deep, EVENING) == (5.0, "")
    down_full = "跌这一边持仓加挂单已有 100 份，达到加密日涨跌每边上限 100 份（内测，SIM_CRYPTO_DAY_MAX_SHARES）"
    assert bot.sim_market_room(held, mk, "down") == (0.0, down_full)
    assert bot.sim_taker_pick(mk, held, "down", m.taker_quote(mk.book, "down", 10, 200), EVENING)[1] == down_full
    full = m.TradeMap({**held, **dict([fill("up", 5, 2, 0.41)])})
    room, why = bot.sim_market_room(full, mk, "up")
    assert room == 0 and why == "涨这一边持仓加挂单已有 100 份，达到加密日涨跌每边上限 100 份（内测，SIM_CRYPTO_DAY_MAX_SHARES）", why
    assert bot.sim_taker_pick(mk, full, "up", q, EVENING)[1] == why and bot.sim_maker_pick(mk, full, "up", maker, EVENING)[1] == why
    settled = m.TradeMap({**full, f"{SPEC.slug}|up|吃#2": {**full[f"{SPEC.slug}|up|吃#2"], "status": "settled"}})
    assert bot.sim_market_room(settled, mk, "up") == (5.0, "")  # a settled record holds nothing

    # --- nothing bought in the last half hour; a resting order withdrawn ---------------------------------------------------------
    late, early = et(2026, 11, 4, 11, 31, -5), et(2026, 11, 4, 11, 29, -5)
    quiet = "加密日涨跌结束前 30 分钟不买（内测，SIM_CRYPTO_DAY_QUIET_MINUTES）"
    assert bot.sim_quiet(mk.settle, late) == quiet and bot.sim_quiet(mk.settle, early) == ""
    assert bot.sim_quiet({"end": SPEC.end_ms}, late) == ""  # another market: SIM_QUIET_MINUTES alone (off)
    assert bot.sim_quiet(mk.settle, SPEC.end_ms) == "已到结束时间，等结果出来：不开新单，挂单撤掉"
    assert bot.sim_taker_pick(mk, NONE, "up", q, late)[1] == quiet and bot.sim_maker_pick(mk, NONE, "up", maker, late)[1] == quiet
    assert bot.sim_withdraw_reason(resting[0], mk, late) == quiet and bot.sim_withdraw_reason(resting[0], mk, early) == ""
    longer = m.Bot(m.Config.from_env({**ENV, "SIM_QUIET_MINUTES": "45"}), m.Store(":memory:"), FM(EVENING), None)
    assert longer.sim_quiet(mk.settle, et(2026, 11, 4, 11, 20, -5)) == "结束前 45 分钟不交易"  # the longer of the two
    # the limits off: the general counts alone
    free = m.Bot(m.Config.from_env({**ENV, **{k: "0" for k in KEYS}}), m.Store(":memory:"), FM(EVENING), None)
    assert free.sim_order_cap(mk) == math.inf and free.sim_market_room(full, mk, "up") == (math.inf, "") and free.sim_quiet(mk.settle, late) == ""
    assert free.sim_taker_shares(mk) == 100 and free.sim_maker_pick(mk, NONE, "up", maker, EVENING) == (100.0, "")
    # the settings, as the control page and /sim show them
    assert [bot.control_value(k) for k in KEYS] == ["10", "100", "30"] and bot.control_value("SIM_CRYPTO_DAY_EDGE_CENTS") == "0"
    assert "加密日涨跌（BTC / ETH / BNB，内测）：每次最多买 10 份，每个市场每边持仓加挂单最多 100 份，结束（12:00 ET）前 30 分钟起不买、撤掉挂单" in bot.sim_text()

    # --- the entry bar: SIM_EDGE_CENTS (10¢) unless SIM_CRYPTO_DAY_EDGE_CENTS gives these markets their own --------------------
    assert bot.sim_edge_bar(mk) == bot.sim_taker_bar(mk) == bot.sim_maker_mode(mk)[0] == 0.10 == bot.sim_edge_bar(other)
    six = m.Bot(m.Config.from_env({**ENV, "SIM_CRYPTO_DAY_EDGE_CENTS": "6"}), m.Store(":memory:"), FM(EVENING), None)
    assert six.sim_edge_bar(mk) == six.sim_taker_bar(mk) == six.sim_maker_mode(mk)[0] == 0.06 and six.sim_edge_bar(other) == 0.10
    assert six.sim_taker_cap(mk, "up", 200) > cap and six.control_value("SIM_CRYPTO_DAY_EDGE_CENTS") == "6"  # a lower bar pays more
    assert "加密日涨跌（BTC / ETH / BNB，内测）：净优势 ≥ 6.0¢ 才买，每次最多买 10 份" in six.sim_text()
    for bad in ("51", "-1"):
        try:
            m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_CRYPTO_DAY_EDGE_CENTS": bad}); assert False, bad
        except ValueError:
            pass
    # the withdrawal line stays half a cent under the market's own bar (a 4¢ bar: 3.5¢, not the 5¢ line of the rest)
    four = m.Bot(m.Config.from_env({**ENV, "SIM_CRYPTO_DAY_EDGE_CENTS": "4", "SIM_MAKER_EXIT_CENTS": "5"}), m.Store(":memory:"), FM(EVENING), None)
    assert abs(four.sim_maker_exit(mk) - 0.035) < 1e-12 and four.sim_maker_exit(other) == four.sim_maker_exit() == 0.05
    # a resting order placed under another bar is judged again by today's: 7¢ of edge was enough at 6¢, not at 10¢
    seven = {**resting[0], "price": round(mk.fair_up - 0.07, 6), "version": {**resting[0]["version"], "sim_crypto_day_edge": 0.06}}
    assert bot.sim_requote_reason(seven, mk, EVENING).startswith("触发门槛已改为 10.0¢，现净优势 7.0¢ 不达标"), bot.sim_requote_reason(seven, mk, EVENING)
    assert six.sim_requote_reason({**seven, "version": resting[0]["version"]}, mk, EVENING) == ""  # placed at 10¢, 7¢ still clears 6¢

    # --- the day ends: the final candle read, the card moves on, the day's positions settle on its closes ---------------------
    eth.get = binance({ETH.day_spec(NOV4).start_ms: "2500"}, clock, "2510")  # Binance never gives ETH's final candle here
    bot.updowns = {"BTC-DAY": btc, "ETH-DAY": eth}
    clock[0] = SPEC.end_ms + 30_000
    await bot.refresh_updown(clock[0])  # 12:00:30 ET: the final minute still running
    assert btc.spec == SPEC and eth.spec.end_ms == SPEC.end_ms
    clock[0] = SPEC.end_ms + 90_000
    btc.times["candle"] = eth.times["candle"] = -1e9
    await bot.refresh_updown(clock[0])  # 12:01:30 ET: Binance has not given it yet — the card waits on its day
    assert btc.spec == SPEC and "币安没有返回 11-04 12:00 ET（北京 11-05 01:00）这根 1 分钟 K 线" in btc.error, btc.error
    closes[SPEC.end_ms] = "69000"
    clock[0] = SPEC.end_ms + 120_000
    btc.times["candle"] = -1e9
    await bot.refresh_updown(clock[0])  # read: the day is settled on it and the card moves to 11-05 at once
    NEXT = BTC.day_spec(NOV5)
    assert store.get(f"updown:{SPEC.slug}:end")["close"] == "69000" and btc.spec == NEXT and btc.times["candle"] < 0
    assert NEXT.slug in bot.predict.want_info and bot.predict_targets(clock[0])["BTC-DAY"] == NEXT.slug
    assert eth.spec.end_ms == SPEC.end_ms  # ETH still waits for its final candle
    assert bot.updown_book(NEXT) == (None, "")  # the 11-04 book until 11-05's is read: no comparison, no trade
    clock[0] = SPEC.end_ms + 150_000
    await bot.refresh_updown(clock[0])  # the new day's first candle is the one just read
    assert btc.close_of("start") == D("69000") and btc.spec.name == "BTC 11-05 涨跌"
    clock[0] = SPEC.end_ms + MIN + bot.UPDOWN_ROLL_WAIT_MS
    await bot.refresh_updown(clock[0])  # ten minutes on, still nothing from Binance: ETH moves on regardless
    assert eth.spec == ETH.day_spec(NOV5)
    # the 11-04 taker settles on its own day's closes (69,000 under 70,000: Down), not on the card's new day
    up, note, proof = bot.sim_result(taker, clock[0])
    assert (up, proof["start"], proof["end"]) == (0.0, 70000.0, 69000.0) and note == "终点 69,000.00，起点 70,000.00", (up, note, proof)
    assert bot.updown_of(taker).spec.slug == SPEC.slug and bot.updown_of({**taker, "settle": {"end": SPEC.end_ms}}) is None
    eth_trade = {**taker, "key": "ETH-DAY", "market": ETH.day_spec(NOV4).slug, "slug": ETH.day_spec(NOV4).slug,
                 "settle": {"end": SPEC.end_ms, "start": SPEC.start_ms, "day": "2026-11-04"}}
    assert bot.sim_result(eth_trade, clock[0]) is None  # its final close never read: Predict's result settles it
    assert bot.sim_due(eth_trade, SPEC.end_ms + bot.SIM_SETTLE_MS + 1)

    # --- a restart: shortly after noon the card starts on the day just over while its final close is unread -------------------
    s2 = m.Store(":memory:")
    at = NEXT.end_ms + 3 * MIN
    assert m.Bot(m.Config.from_env(ENV), s2, FM(at), None).updowns["BTC-DAY"].spec == NEXT
    s2.put(f"updown:{NEXT.slug}:end", {"open": NEXT.end_ms, "close": "71000", "read_ms": at})
    assert m.Bot(m.Config.from_env(ENV), s2, FM(at), None).updowns["BTC-DAY"].spec == BTC.day_spec(dt.date(2026, 11, 6))
    later = m.Bot(m.Config.from_env(ENV), m.Store(":memory:"), FM(NEXT.end_ms + MIN + bot.UPDOWN_ROLL_WAIT_MS), None)
    assert later.updowns["BTC-DAY"].spec.slug == "bitcoin-up-or-down-on-november-6-2026"
    print("DAILY_OK")


asyncio.run(run())


async def backtest():
    """1.98.0: the threshold backtest. Every SHADOW_EVERY_MS each daily market's two sides as a taker finds them — the net
    edge of buying the ask for one buy, kept from 2¢ when a buy was possible then, whatever the entry bar — and /calib's
    report of every bar once the market has a result: how many, on how many markets, the edge claimed, the gain a share."""
    store = m.Store(":memory:")
    bot = m.Bot(m.Config.from_env(ENV), store, FM(EVENING), None)
    btc = bot.updowns["BTC-DAY"]
    store.put(f"updown:{SPEC.slug}:start", {"open": SPEC.start_ms, "close": "70000"})
    btc.price, btc.priced_ms, btc.sigma, btc.sigma_ms = D("70500"), EVENING, 0.4, EVENING
    bot.predict.info[SPEC.slug] = {"outcomes": ["Up", "Down"], "created_ms": 0}
    bot.predict.books["BTC-DAY"] = book(SPEC.slug, [("0.35", "500")], [("0.40", "500")])
    fair = btc.odds(EVENING).fair_up
    cost = lambda ask: ask + 0.02 * min(ask, 1 - ask)  # Predict's taker fee when the market states none
    bot.shadow_sample(EVENING)
    rows = dict(store.items("shadow:"))
    assert list(rows) == [f"shadow:{SPEC.slug}|up|{EVENING}"], rows  # 涨 at 40¢ is cheap; 跌 at 65¢ (1 − the 35¢ bid) is dear
    first = rows[f"shadow:{SPEC.slug}|up|{EVENING}"]
    assert (first["side"], first["kind"], first["key"], first["settle"]["day"]) == ("up", "updown", "BTC-DAY", "2026-11-04")
    assert abs(first["cost"] - cost(0.40)) < 1e-12 and abs(first["edge"] - (fair - cost(0.40))) < 1e-12 and first["edge"] > 0.2
    bot.shadow_sample(EVENING + MIN)  # within five minutes: nothing more
    assert store.count("shadow:") == 1
    # five minutes on, the 涨 ask at the price leaving about 5¢
    ask = next(a / 100 for a in range(99, 1, -1) if fair - cost(a / 100) >= 0.04)
    later = EVENING + 5 * MIN
    btc.priced_ms = btc.sigma_ms = later
    bot.predict.books["BTC-DAY"] = book(SPEC.slug, [("0.35", "500")], [(f"{ask:.2f}", "500")], at=later)
    bot.shadow_sample(later)
    second = store.get(f"shadow:{SPEC.slug}|up|{later}")
    assert second is not None and 0.04 <= second["edge"] < 0.06 and second["need"] < 0.04, second
    # nothing kept when the trader could not buy: the last half hour, a stale book, a card that holds back
    late = et(2026, 11, 4, 11, 31, -5)
    btc.priced_ms = btc.sigma_ms = late
    bot.predict.books["BTC-DAY"] = book(SPEC.slug, [("0.35", "500")], [("0.40", "500")], at=late)
    bot.shadow_sample(late)
    stale = late - 30 * MIN  # (sampled at half past ten, its book read at ten)
    btc.priced_ms = btc.sigma_ms = stale
    bot.predict.books["BTC-DAY"] = book(SPEC.slug, [("0.35", "500")], [("0.40", "500")], at=stale - 30 * MIN)
    bot.shadow_sample(stale)
    assert store.count("shadow:") == 2
    # before the result: counted, not scored
    head = ("🧪 加密日涨跌吃单门槛回测（每 5 分钟看一次每个市场的两边：按卖1 买一笔的净优势 ≥ 2¢、当时能买就记一条；"
            "不下单，出结果后按每份实际赚赔评分，保留 60 天）：")
    assert bot.shadow_report() == [head, "  已记 2 条 / 1 个市场，还没有出结果的"]
    # the result: 69,000 under 70,000 is Down, so every 涨 bought lost what it cost
    store.put(f"updown:{SPEC.slug}:end", {"open": SPEC.end_ms, "close": "69000"})
    lines = bot.shadow_report()
    pct = lambda x: f"{x * 100:+.1f}¢"
    both = f"2 次 / 1 个市场，下单时平均净优势 {pct((first['edge'] + second['edge']) / 2)}，实际每份 {pct(-(first['cost'] + second['cost']) / 2)}，赚的占 0%"
    alone = f"1 次 / 1 个市场，下单时平均净优势 {pct(first['edge'])}，实际每份 {pct(-first['cost'])}，赚的占 0%"
    assert lines == [head, f"  门槛 2¢：{both}", f"  门槛 4¢：{both}", *(f"  门槛 {bar:g}¢：{alone}" for bar in (6, 8, 10, 12, 15, 20)),
                     "  （同一个市场的几次一起输赢，可信度看市场数：不到 30 个市场时差别多半是运气）"], lines
    assert bot.calibration_text().endswith("\n".join(lines))  # /calib carries it
    # the samples are pruned after SHADOW_KEEP_DAYS
    bot.shadow_pruned, bot.shadow_last = 0.0, -10**15
    bot.shadow_sample(EVENING + m.SHADOW_KEEP_DAYS * m.DAY_MS + 3 * MIN)  # the first is 60 days and 3 minutes old, the second not yet 60 days
    assert store.count("shadow:") == 1 and store.get(f"shadow:{SPEC.slug}|up|{later}") is not None
    print("BACKTEST_OK")


asyncio.run(backtest())
