"""1.102.0: 宇树科技 (UNITREE) 涨或跌 15分 — a market every quarter hour (unitree-updown-15m-<window start, Unix seconds>):
"Up" when the UNITREE/USDT price at the end of the window is greater than at its start, "Down" when lower, 50-50 when equal.
Chainlink's UNITREE/USDT data stream settles it, which "uses the index price (field i) from Binance's USDS-Margined Futures
Mark Price Stream": the card reads that index (pushed every second, read when the push is late) and each boundary's value as
the close of the index's 1-minute candle ending there. In beta: at most 10 shares a buy, 100 on each side of one window,
no order of any kind in a window's last 90 seconds (1.102.1; 120 in 1.102.0), resting ones withdrawn."""
import asyncio, json, math, sys, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
D = m.D
BJ = lambda h, mi, s=0: int(dt.datetime(2026, 11, 4, h, mi, s, tzinfo=m.BEIJING).timestamp() * 1000)
T0 = BJ(11, 0)            # 1793761200 s: the window of the link the market came with
T1 = T0 + 15 * 60_000     # its end, the next window's start

# --- the spec: the window a time falls in, its slug, its two index candles -------------------------------------------------
W = m.WINDOW_UPDOWN_MARKETS[0]
assert (W.key, W.stem, W.symbol, W.coin, W.minutes) == ("UNITREE-15M", "unitree", "UNITREEUSDT", "宇树", 15)
spec = W.current(BJ(11, 5, 30))
assert spec.slug == "unitree-updown-15m-1793761200" and T0 // 1000 == 1793761200 and spec.name == "宇树 15分涨跌 11:00–11:15", spec
assert (spec.start_ms, spec.end_ms) == (T0 - 60_000, T1 - 60_000)  # the index candles that end at 11:00 and at 11:15
assert W.current(T0) == spec and W.current(T1 - 1) == spec and W.current(T1).slug == "unitree-updown-15m-1793768400"  # 13:00: no 11:15 window
assert W.before(spec) == W.current(T0 - 1) and W.before(spec).slug == "unitree-updown-15m-1793760300" and W.tag(spec) == {"window": "15m"}
# the schedule (1.103.0): 宇树's windows follow the Shanghai session (09:30–11:30, 13:00–15:00), the quarter hour before each close
# without a market (the Predict page lists windows ending 09:45 … 11:15 and 13:15 … 14:45); weekends and holidays skipped
at = lambda h, mi, d=4: int(dt.datetime(2026, 11, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
first = lambda ms, hol=frozenset(): m.stamp(W.current(ms, hol).start_ms + 60_000, seconds=False)
assert W.exchange == "sh" and [first(at(h, mi)) for h, mi in ((9, 0), (9, 30), (11, 14), (11, 15), (12, 59), (13, 0), (14, 30), (14, 45), (15, 10))] == [
    "11-04 09:30", "11-04 09:30", "11-04 11:00", "11-04 13:00", "11-04 13:00", "11-04 13:00", "11-04 14:30", "11-05 09:30", "11-05 09:30"]
assert first(at(15, 10, 6)) == "11-09 09:30" and first(at(15, 10, 6), frozenset({dt.date(2026, 11, 9)})) == "11-10 09:30"
assert [W.exists(at(h, mi)) for h, mi in ((9, 15), (9, 30), (11, 0), (11, 15), (13, 0), (14, 30), (14, 45))] == [False, True, True, False, True, True, False]
last = lambda ms, hol=frozenset(): m.stamp(W.before(W.current(ms, hol), hol).start_ms + 60_000, seconds=False)
assert last(at(13, 0)) == "11-04 11:00" and last(at(9, 30)) == "11-03 14:30" and last(at(9, 30, 9)) == "11-06 14:30"
assert last(at(9, 30, 10), frozenset({dt.date(2026, 11, 9)})) == "11-06 14:30"
# SK 海力士: the KRX session (09:00–15:30 KST = 08:00–14:30 Beijing), the user's link the 11:45 Beijing (12:45 KST) window
K = m.WINDOW_UPDOWN_MARKETS[1]
assert (K.key, K.stem, K.symbol, K.coin, K.exchange, K.minutes) == ("SKHYNIX-15M", "skhynix", "SKHYNIXUSDT", "SK 海力士", "kr", 15)
assert m.stamp(K.current(at(7, 50)).start_ms + 60_000, seconds=False) == "11-04 08:00" and K.exists(at(14, 0)) and not K.exists(at(14, 15))
assert K.current(at(11, 50)).slug == "skhynix-updown-15m-1793763900" and K.current(at(11, 50)).name == "SK 海力士 15分涨跌 11:45–12:00"
assert m.stamp(K.before(K.current(at(8, 0))).start_ms + 60_000, seconds=False) == "11-03 14:00"
assert W.ROLL_WAIT_MS == 60_000 and m.DailyUpDownSpec.ROLL_WAIT_MS == 10 * 60_000

# --- σ from the index's 1-minute candles: the last four hours, or the last hour when it moves more ---------------------------
rows = lambda closes, end: [[end - (len(closes) - i) * 60_000, c, c, c, c, "0", end - (len(closes) - i) * 60_000 + 59_999, "0", 0, "0", "0", "0"]
                            for i, c in enumerate(closes)]
calm = [str(90 * (1 + 0.0002 * (i % 2))) for i in range(241)]
assert abs(m.minute_vol(rows(calm, T0), T0) - 0.0002 * math.sqrt(365 * 24 * 60) * math.sqrt(240 / 239)) < 0.02
lively = calm[:181] + [str(90 * (1 + 0.002 * (i % 2))) for i in range(60)]
assert m.minute_vol(rows(lively, T0), T0) > 5 * m.minute_vol(rows(calm, T0), T0)  # the last hour's, higher
assert m.minute_vol(rows(["90"] * 241, T0), T0) == 0.0  # not moved at all: the card pauses on it
try:
    m.minute_vol(rows(["90"] * 30, T0), T0); assert False
except ValueError as error:
    assert str(error) == "指数 1 分钟 K 线不足"
# the session's minutes alone count for σ (1.103.0): the flat lunch break and the jump over it are left out
calm_m = lambda n, base: [str(base * (1 + 0.0002 * (i % 2))) for i in range(n)]
day_rows = rows(calm_m(121, 90), BJ(11, 30)) + rows(["95"] * 90, BJ(13, 0)) + rows(calm_m(61, 95), BJ(14, 1))
session_vol = m.minute_vol(day_rows, BJ(14, 1), 240, m.session_minute_fn("sh"))
assert abs(session_vol - m.minute_vol(rows(calm_m(241, 90), BJ(11, 30)), BJ(11, 30))) < 0.02 < 1 < m.minute_vol(day_rows, BJ(14, 1), 240)
assert m.session_minute_fn("sh")(at(9, 30)) and not m.session_minute_fn("sh")(at(11, 30)) and not m.session_minute_fn("sh")(at(9, 30, 7))
assert m.session_segments("sh", dt.date(2026, 11, 4)) == [(at(9, 30), at(11, 30)), (at(13, 0), at(15, 0))] and m.session_segments("kr", dt.date(2026, 11, 7)) == []


class FM:
    """The bot's own Binance futures feed (BINANCE_BASE_URL): the index's 1-minute candles, its premium index."""
    def __init__(self, now):
        self.now, self.config, self.paths = now, None, []
        self.index = {}       # candle open time -> close: the boundaries
        self.premium = None   # (price, time)
        self.closes = calm

    def now_ms(self): return self.now

    async def get(self, path, **params):
        self.paths.append(path)
        if path == "/fapi/v1/indexPriceKlines":
            assert params["pair"] == "UNITREEUSDT" and params["interval"] == "1m", params
            if "startTime" in params:
                close = self.index.get(params["startTime"])
                return [] if close is None else [[params["startTime"], close, close, close, close, "0", params["startTime"] + 59_999, "0", 0, "0", "0", "0"]]
            return rows(self.closes, self.now - self.now % 60_000)
        if path == "/fapi/v1/premiumIndex":
            assert params == {"symbol": "UNITREEUSDT"}
            return {"symbol": "UNITREEUSDT", "markPrice": "90.5", "indexPrice": self.premium[0], "time": self.premium[1]}
        raise AssertionError(path)


ENV = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "SKHYNIXUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "SIM_WAYS": "both",
       "SIM_MARKETS": "all", "SIM_MAKER_POINTS": "off", "SIM_SHARES": "100"}


def book(fair_bid, fair_ask, at, slug, mid="701"):
    return m.PredictBook("UNITREE-15M", slug, mid, "t", ((D(fair_bid), D("300")),), ((D(fair_ask), D("300")),), at, None)


async def run():
    # --- the settings ------------------------------------------------------------------------------------------------------
    c = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"})
    assert (c.sim_crypto_15m_shares, c.sim_crypto_15m_max_shares, c.sim_crypto_15m_quiet, c.sim_crypto_15m_edge) == (10, 100, 90, 0)
    for bad in ({"SIM_CRYPTO_15M_QUIET_SECONDS": "601"}, {"SIM_CRYPTO_15M_EDGE_CENTS": "51"}, {"SIM_CRYPTO_15M_SHARES": "-1"}):
        try: m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", **bad}); assert False, bad
        except ValueError: pass
    assert {"SIM_CRYPTO_15M_SHARES", "SIM_CRYPTO_15M_MAX_SHARES", "SIM_CRYPTO_15M_QUIET_SECONDS", "SIM_CRYPTO_15M_EDGE_CENTS"} <= m.CONTROL_KEY_SET

    # --- the card: an index market, priced by the pair's mark price stream (field i), never by its trades ----------------------
    fm = FM(T0 + 5_000)
    store = m.Store(":memory:")
    prev = W.before(spec)  # the window before, its end read already (else a card started within a minute of it settles that one first)
    store.put(f"updown:{prev.slug}:end", {"open": prev.end_ms, "close": "89.9500", "read_ms": T0 + 2_000})
    bot = m.Bot(m.Config.from_env(ENV), store, fm, None)
    card = bot.updowns["UNITREE-15M"]
    assert isinstance(card, m.IndexUpDownMarket) and card.spec == spec and bot.updown_rolling["UNITREE-15M"] is W
    assert bot.index_markets["UNITREEUSDT"] == [card] and set(bot.index_markets) == {"UNITREEUSDT", "SKHYNIXUSDT"} and all(card not in v for v in bot.pair_markets.values())
    sk = bot.updowns["SKHYNIX-15M"]
    assert isinstance(sk, m.IndexUpDownMarket) and sk.exchange == "kr" and sk.spec == K.before(K.current(T0 + 5_000))  # its 10:45 window's end unread: settled first
    assert sk.point(sk.spec.start_ms) == "10:45（韩国时间 11:45）" and sk.spec.name == "SK 海力士 15分涨跌 10:45–11:00"
    futures = bot.binance_streams["futures"]
    assert futures.symbols()[:2] == ["SKHYNIXUSDT", "UNITREEUSDT"] and "unitreeusdt@markPrice@1s" in futures.names()  # not in SYMBOLS: streamed all the same
    assert bot.predict_targets(T0 + 5_000)["UNITREE-15M"] == spec.slug and spec.slug in bot.predict.want_info
    assert bot.control_value("SIM_CRYPTO_15M_SHARES") == "10" and bot.control_value("SIM_CRYPTO_15M_QUIET_SECONDS") == "90"
    # before the start's candle is read
    assert card.odds(T0 + 5_000) == "等待币安 UNITREEUSDT 11:00 的指数价（起点）" and card.exchange == "sh" and card.trading(T0)
    fm.index[T0 - 60_000], fm.premium = "90.0000", ("90.1200", T0 + 4_500)
    await card.refresh(T0 + 5_000)
    assert card.close_of("start") == D("90.0000") and card.price == D("90.1200") and card.sigma > 0 and card.error == "", card.error
    assert fm.paths.count("/fapi/v1/indexPriceKlines") == 2 and "/fapi/v1/premiumIndex" in fm.paths
    odds = card.odds(T0 + 5_000)
    up, flat, down, _ = m.updown_odds(90.12, 90.0, card.sigma, (T1 - T0 - 5_000) / m.YEAR_MS)
    assert isinstance(odds, m.UpDownOdds) and abs(odds.fair_up - (up + flat / 2)) < 1e-12 and odds.fair_up > 0.5 and not odds.settled
    # the stream: a mark price frame carries the index (i, at its event time E); an older one, or a trade, moves nothing
    await futures.frame(None, json.dumps({"e": "markPriceUpdate", "s": "UNITREEUSDT", "p": "90.40", "i": "90.3000", "E": T0 + 6_000}))
    assert card.price == D("90.3000") and card.index_ms == T0 + 6_000 and "UNITREE-15M" in bot.price_dirty
    await futures.frame(None, json.dumps({"e": "markPriceUpdate", "s": "UNITREEUSDT", "p": "90.40", "i": "89.0000", "E": T0 + 5_500}))
    await futures.frame(None, json.dumps({"e": "aggTrade", "s": "UNITREEUSDT", "p": "95.00", "T": T0 + 7_000}))
    assert card.price == D("90.3000")
    # the same index a second later (a flat market): the card is kept fresh, nothing is woken for it
    bot.price_dirty.clear()
    await futures.frame(None, json.dumps({"e": "markPriceUpdate", "s": "UNITREEUSDT", "p": "90.40", "i": "90.3000", "E": T0 + 6_900}))
    assert card.index_ms == T0 + 6_900 and card.price == D("90.3000") and not bot.price_dirty
    # the premium index read: only when the push is late, and never older than what the push gave
    fm.premium = ("90.2500", T0 + 5_800)
    card.times["price"] = -1e9
    await card.refresh(T0 + 7_000)
    assert card.price == D("90.3000")  # the read's index is older than the pushed one
    # paused: an index a minute old; an index that has not moved for hours
    assert card.odds(T0 + 6_000 + 61_000).startswith("指数价停在 ") and "暂停概率" in card.odds(T0 + 67_000)
    sigma, card.sigma = card.sigma, 0.0
    assert card.odds(T0 + 7_000) == "指数在最近的交易时段里没有变动，暂停概率"
    card.sigma = sigma

    # --- what holds the card back: Predict's target price, the time range its question names --------------------------------
    odds = card.odds(T0 + 7_000)
    assert bot.updown_hold(card, odds, T0 + 7_000) == ("", "")
    bot.predict.strikes[spec.slug] = (D("90.0300"), 0.0)  # Chainlink's start a hair off ours: the same window
    assert bot.updown_hold(card, odds, T0 + 7_000) == ("", "；与 Predict 目标价一致")
    bot.predict.strikes[spec.slug] = (D("90.3000"), 0.0)  # 0.33% off: the window read wrongly (its start a quarter hour off)
    assert bot.updown_hold(card, odds, T0 + 7_000)[0] == "Predict 目标价 90.3000 与起点指数 90.0000 不一致，请核实；暂不给建议"
    bot.predict.strikes.pop(spec.slug)
    bot.predict.info[spec.slug] = {"outcomes": ["Up", "Down"], "created_ms": 0, "question": "UNITREE Up or Down - November 3, 10:00PM-10:15PM ET"}
    assert bot.updown_hold(card, odds, T0 + 7_000) == ("", "")
    bot.predict.info[spec.slug]["question"] = "UNITREE Up or Down - November 3, 9:45PM-10:00PM ET"  # the slug was the window's end
    assert bot.updown_hold(card, odds, T0 + 7_000)[0] == ("Predict 市场写的时间段（9:45PM-10:00PM ET）和按链接推算的窗口（22:00 ET 起）对不上，"
                                                           "请核实；暂不给建议")
    bot.predict.info[spec.slug]["question"] = "宇树科技 (UNITREE) 涨或跌 15分"  # no time named: nothing to check
    assert bot.updown_hold(card, odds, T0 + 7_000) == ("", "")
    # the half of the day written once, at the end ("10:00-10:15PM"), names the start's too; over noon or midnight the start
    # is in the other half ("11:45-12:00PM": an hour before noon; "11:45-12:00AM": before midnight); a 24-hour range as is
    for question, ok in (("10:00-10:15PM ET", True), ("10:00-10:15AM ET", False), ("22:00-22:15 ET", True), ("22:00 - 22:15 EST", True)):
        bot.predict.info[spec.slug]["question"] = f"UNITREE Up or Down - November 3, {question}"
        assert (bot.window_title_problem(spec) == "") is ok, (question, bot.window_title_problem(spec))
    midnight, noon = W.window_spec(T0 + 105 * 60_000), W.window_spec(T0 - 615 * 60_000)  # 11:45PM–12:00AM ET; 11:45AM–12:00PM ET
    for window, question, ok in ((midnight, "11:45-12:00AM ET", True), (midnight, "11:45PM-12:00AM ET", True), (midnight, "11:45-12:00PM ET", False),
                                 (noon, "11:45-12:00PM ET", True), (noon, "11:45AM-12:00PM ET", True), (noon, "11:45-12:00AM ET", False)):
        bot.predict.info[window.slug] = {"outcomes": ["Up", "Down"], "created_ms": 0, "question": f"UNITREE Up or Down - {question}"}
        assert (bot.window_title_problem(window) == "") is ok, (window.slug, question, bot.window_title_problem(window))

    # --- the card on the page: in 合约标的, beside the pair's own card ----------------------------------------------------------
    now = T0 + 7_000
    fm.now = now
    bot.predict.books["UNITREE-15M"] = book("0.40", "0.45", now, spec.slug)
    pay = bot.updown_payload(card, now)
    assert (pay["name"], pay["group"], pay["source"], pay["ref"], pay["effective"]) == ("宇树 15分涨跌 11:00–11:15", "contract", "币安合约指数价",
                                                                                     "90.0000", "90.3000"), pay
    assert pay["close_label"].startswith("11:15 的币安合约 UNITREEUSDT 指数价（Chainlink 结算源）") and pay["close_ms"] == T1
    assert pay["ref_note"] == "币安合约 UNITREEUSDT 指数价，11:00" and pay["predict"]["bids"][0] == [0.4, 300.0], pay
    items = [i for i in bot.odds_payload()["items"] if i.get("symbol") == "UNITREE-15M"]
    assert len(items) == 1 and items[0]["group"] == "contract"
    json.dumps(pay)

    # --- trading it, in beta: 10 shares a buy, 100 a side, nothing in the last 120 seconds -----------------------------------
    mk = next(x for x in bot.sim_markets(now) if x.key == "UNITREE-15M")
    assert (mk.kind, mk.market, mk.settle) == ("updown", spec.slug, {"end": T1 - 60_000, "start": T0 - 60_000, "window": "15m"})
    assert bot.sim_order_cap(mk) == 10 and bot.sim_taker_shares(mk) == 10 and bot.sim_market_room({}, mk, "up") == (100, "")
    assert bot.sim_edge_bar(mk) == bot.config.sim_edge and bot.sim_beta_version(mk) == {
        "sim_crypto_15m_shares": 10, "sim_crypto_15m_max_shares": 100, "sim_crypto_15m_quiet": 90, "sim_crypto_15m_edge": 0}
    trades, changed = m.TradeMap(), []
    bot.sim_take(mk, trades, now, changed)
    taken = [t for _, t in changed if not t["maker"]]
    assert [(t["label"], t["order"], t["shares"]) for t in taken] == [("吃涨", 10.0, 10.0)], [(t["label"], t["order"]) for _, t in changed]
    assert taken[0]["settle"]["window"] == "15m" and taken[0]["version"]["sim_crypto_15m_shares"] == 10
    assert taken[0]["entry"]["sources"][0]["what"] == "指数" and taken[0]["entry"]["basis"]["window"] == spec.slug
    held = {"v": 2, "market": spec.slug, "slug": spec.slug, "item": spec.name, "kind": "updown", "key": "UNITREE-15M", "side": "up",
            "label": "吃涨", "maker": False, "status": "filled", "shares": 100.0, "order": 100.0, "price": 0.45, "fills": [], "revisions": [],
            "settle": mk.settle, "opened": now}
    room, why = bot.sim_market_room(m.TradeMap({f"{spec.slug}|up|吃": held}), mk, "up")
    assert room == 0 and why == "涨这一边持仓加挂单已有 100 份，达到15 分钟涨跌每边上限 100 份（内测，SIM_CRYPTO_15M_MAX_SHARES）", why
    assert bot.sim_quiet(mk.settle, T1 - 91_000) == "" and bot.sim_quiet(mk.settle, T1 - 89_000) == "15 分钟涨跌结束前 90 秒不下单（内测，SIM_CRYPTO_15M_QUIET_SECONDS）"
    # the last 90 seconds: no order of any kind — a taker the book would fill is not bought, a resting order is withdrawn
    late = lambda at: dataclasses_replace(mk, book=book("0.40", "0.45", at, spec.slug))
    for at, want in ((T1 - 91_000, [("吃涨", 10.0)]), (T1 - 89_000, [])):
        got = []
        bot.sim_take(late(at), m.TradeMap(), at, got)
        bot.sim_quote(late(at), m.TradeMap(), at, got)
        assert [(t["label"], t["order"]) for _, t in got if not t["maker"]] == want and (at < T1 - 90_000 or not got), (at, got)
    resting = {**held, "maker": True, "label": "挂涨", "status": "resting", "shares": 0.0, "price": 0.30}
    assert bot.sim_withdraw_reason(resting, late(T1 - 89_000), T1 - 89_000) == "15 分钟涨跌结束前 90 秒不下单（内测，SIM_CRYPTO_15M_QUIET_SECONDS）"
    assert bot.sim_withdraw_reason(resting, late(T1 - 91_000), T1 - 91_000) == ""
    assert bot.sim_quiet(mk.settle, T1 - 59_000) == "已到结束时间，等结果出来：不开新单，挂单撤掉"  # the final minute: never
    long_quiet = m.Bot(m.Config.from_env({**ENV, "SIM_QUIET_MINUTES": "10", "SIM_CRYPTO_15M_QUIET_SECONDS": "0", "SIM_CRYPTO_15M_EDGE_CENTS": "6"}),
                       m.Store(":memory:"), FM(now), None)
    assert long_quiet.sim_quiet(mk.settle, T1 - 9 * 60_000) == ""  # SIM_QUIET_MINUTES would take most of the window: not applied here
    assert abs(long_quiet.sim_edge_bar(mk) - 0.06) < 1e-12 and long_quiet.sim_edge_bar(dataclasses_replace(mk, settle={"end": 1})) == long_quiet.config.sim_edge
    text = bot.sim_text()
    assert "15 分钟涨跌（宇树、SK 海力士，内测）：每次最多买 10 份，每个市场每边持仓加挂单最多 100 份，每个窗口最后 90 秒不下单、撤掉挂单（SIM_CRYPTO_15M_*）" in text, text
    # no Telegram alert: a quarter-hour window is over before an alert confirmed over a minute is acted on by hand
    seen = []
    bot.edge_observe = lambda state, market, mk_, *a, **k: seen.append(market)
    bot.edge_ran = -1e9
    await bot.edge_alerts(now)
    assert spec.slug not in seen

    # --- the window ends: the end's index read, the card moves on, the trade settles on its own window -----------------------
    bot.sim_save([(f"{spec.slug}|up|吃", {**taken[0], "status": "filled"})])
    # the kept closes of windows older than 8 days go at the roll (two rows a window, 192 a day); a 7-day-old window's stay
    ancient, recent = W.window_spec(T0 - 9 * m.DAY_MS), W.window_spec(T0 - 7 * m.DAY_MS)
    for old_spec in (ancient, recent):
        store.put(f"updown:{old_spec.slug}:start", {"open": old_spec.start_ms, "close": "80"})
        store.put(f"updown:{old_spec.slug}:end", {"open": old_spec.end_ms, "close": "81"})
    fm.index[T1 - 60_000] = "90.2000"
    fm.now = bot.market.now = T1 + 2_000
    card.times["candle"] = -1e9
    await bot.refresh_updown(T1 + 2_000)
    nxt = W.current(T1)
    assert card.spec == nxt and nxt.slug == "unitree-updown-15m-1793768400" and card.close_of("start") is None, card.spec  # 13:00: after the break
    # the feed follows the next window from a quarter hour before it, not through the lunch break
    assert "UNITREE-15M" not in bot.predict_targets(T1 + 2_000) and "UNITREE-15M" in bot.predict_targets(nxt.start_ms + 60_000 - 15 * 60_000)
    # consecutive windows: the candle that settled one is the next one's line, kept at the roll (10:45–11:00 → 11:00–11:15)
    c3 = m.Bot(m.Config.from_env(ENV), m.Store(":memory:"), FM(T0 - 30_000), None)
    assert c3.updowns["UNITREE-15M"].spec == W.before(spec)
    c3.market.index[T0 - 60_000], c3.market.now = "89.9500", T0 + 2_000
    await c3.refresh_updown(T0 + 2_000)
    assert c3.updowns["UNITREE-15M"].spec == spec and c3.updowns["UNITREE-15M"].close_of("start") == D("89.9500")
    assert nxt.slug in bot.predict.want_info
    assert store.get(f"updown:{ancient.slug}:end") is None and store.get(f"updown:{recent.slug}:end") == {"open": recent.end_ms, "close": "81"}
    assert store.get(f"updown:{spec.slug}:end") == {"open": spec.end_ms, "close": "90.2000", "read_ms": T1 + 2_000} and bot.updown_prune(W, T1 + 2_000) == 0
    trade = bot.sim_trades()[f"{spec.slug}|up|吃"]
    past = bot.updown_of(trade)
    assert isinstance(past, m.IndexUpDownMarket) and past.spec.slug == spec.slug and past.futures == card.futures
    result = bot.sim_result(trade, T1 + 2_000)
    assert result is not None and result[0] == 1.0 and "终点 90.2" in result[1], result  # 90.20 over 90.00: 涨
    # an end within 0.05% of the start: Chainlink's report may differ from the index candle — Predict's result decides
    fm.index[nxt.start_ms], fm.index[nxt.end_ms] = "90.2000", "90.2300"
    tie = {**trade, "market": nxt.slug, "slug": nxt.slug, "settle": {"end": nxt.end_ms, "start": nxt.start_ms, "window": "15m"}}
    at_end = nxt.end_ms + 60_000 + 2_000
    await bot.updown_of(tie).refresh(at_end)
    assert bot.updown_of(tie).odds(at_end).startswith("终点指数 90.2300 与起点 90.2000 相差不到 0.05%")
    assert bot.sim_result(tie, at_end) is None
    # a restart within a minute of a window's end: the card starts on that window while its end is not read
    fresh = m.Bot(m.Config.from_env(ENV), m.Store(":memory:"), FM(T1 + 30_000), None)
    assert fresh.updowns["UNITREE-15M"].spec == spec
    assert m.Bot(m.Config.from_env(ENV), m.Store(":memory:"), FM(T1 + 61_000), None).updowns["UNITREE-15M"].spec == nxt
    print("WINDOW_OK")


def dataclasses_replace(mk, **kw):
    import dataclasses
    return dataclasses.replace(mk, **kw)


asyncio.run(run())
