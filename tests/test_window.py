"""1.102.0: 宇树科技 (UNITREE) 涨或跌 15分 — a market every quarter hour (unitree-updown-15m-<window start, Unix seconds>):
"Up" when the UNITREE/USDT price at the end of the window is greater than at its start, "Down" when lower, 50-50 when equal.
Chainlink's UNITREE/USDT data stream settles it, which "uses the index price (field i) from Binance's USDS-Margined Futures
Mark Price Stream": the card reads that index (pushed every second, read when the push is late) and each boundary's value as
the close of the index's 1-minute candle ending there. In beta: at most 10 shares a buy, 100 on each side of one window,
no order of any kind in a window's last 90 seconds (1.102.1; 120 in 1.102.0), resting ones withdrawn."""
import asyncio, csv, io, json, math, sys, time, datetime as dt
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
assert W.current(T0) == spec and W.current(T1 - 1) == spec and W.current(T1).slug == "unitree-updown-15m-1793762100"  # 11:15–11:30 (1.105.2: it exists)
assert W.before(spec) == W.current(T0 - 1) and W.before(spec).slug == "unitree-updown-15m-1793760300" and W.tag(spec) == {"window": "15m"}
# the schedule (1.103.0; 1.105.2): 宇树's windows follow the Shanghai session (09:30–11:30, 13:00–15:00), a window ending at a
# close included (宇树 has an 11:15–11:30 market; 1.103.0 had left the quarter hour before each close out); weekends and holidays skipped
at = lambda h, mi, d=4: int(dt.datetime(2026, 11, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
first = lambda ms, hol=frozenset(): m.stamp(W.current(ms, hol).start_ms + 60_000, seconds=False)
assert W.exchange == "sh" and [first(at(h, mi)) for h, mi in ((9, 0), (9, 30), (11, 14), (11, 15), (12, 59), (13, 0), (14, 30), (14, 45), (15, 10))] == [
    "11-04 09:30", "11-04 09:30", "11-04 11:00", "11-04 11:15", "11-04 13:00", "11-04 13:00", "11-04 14:30", "11-04 14:45", "11-05 09:30"]
assert first(at(15, 10, 6)) == "11-09 09:30" and first(at(15, 10, 6), frozenset({dt.date(2026, 11, 9)})) == "11-10 09:30"
assert [W.exists(at(h, mi)) for h, mi in ((9, 15), (9, 30), (11, 0), (11, 15), (11, 30), (13, 0), (14, 30), (14, 45), (15, 0))] == [False, True, True, True, False, True, True, True, False]
last = lambda ms, hol=frozenset(): m.stamp(W.before(W.current(ms, hol), hol).start_ms + 60_000, seconds=False)
assert last(at(13, 0)) == "11-04 11:15" and last(at(9, 30)) == "11-03 14:45" and last(at(9, 30, 9)) == "11-06 14:45"
assert last(at(9, 30, 10), frozenset({dt.date(2026, 11, 9)})) == "11-06 14:45"
# SK 海力士: the KRX session (09:00–15:30 KST = 08:00–14:30 Beijing), the user's link the 11:45 Beijing (12:45 KST) window
K = m.WINDOW_UPDOWN_MARKETS[1]
assert (K.key, K.stem, K.symbol, K.coin, K.exchange, K.minutes) == ("SKHYNIX-15M", "skhynix", "SKHYNIXUSDT", "SK 海力士", "kr", 15)
assert m.stamp(K.current(at(7, 50)).start_ms + 60_000, seconds=False) == "11-04 08:00" and K.exists(at(14, 15)) and not K.exists(at(14, 30))
assert K.current(at(11, 50)).slug == "skhynix-updown-15m-1793763900" and K.current(at(11, 50)).name == "SK 海力士 15分涨跌 11:45–12:00"
assert m.stamp(K.before(K.current(at(8, 0))).start_ms + 60_000, seconds=False) == "11-03 14:15"
assert W.ROLL_WAIT_MS == 60_000 and m.DailyUpDownSpec.ROLL_WAIT_MS == 10 * 60_000

# --- σ from the index's 1-minute candles: the last four hours, the last hour weighing about half (1.106.0) ---------------------
rows = lambda closes, end: [[end - (len(closes) - i) * 60_000, c, c, c, c, "0", end - (len(closes) - i) * 60_000 + 59_999, "0", 0, "0", "0", "0"]
                            for i, c in enumerate(closes)]
calm = [str(90 * (1 + 0.0002 * (i % 2))) for i in range(241)]
assert abs(m.minute_vol(rows(calm, T0), T0) - 0.0002 * math.sqrt(365 * 24 * 60) * math.sqrt(240 / 239)) < 0.02
lively = calm[:181] + [str(90 * (1 + 0.002 * (i % 2))) for i in range(60)]
assert m.minute_vol(rows(lively, T0), T0) > 5 * m.minute_vol(rows(calm, T0), T0)  # the last hour weighs most
assert abs(m.minute_vol(rows(lively, T0), T0, half_life=100_000) - m.minute_vol(rows(lively, T0), T0, open_floor=False, half_life=100_000)) < 1e-12 \
    and m.minute_vol(rows(lively, T0), T0, half_life=100_000) < 0.75 * m.minute_vol(rows(lively, T0), T0), "flat weights: the hour of moves is a quarter of the four (σ ×0.69)"
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
# 1.106.0: a session under an hour old is priced at least on its own minutes — the open moves more than the weighted figure, still
# carrying the morning's calm, has caught up with
open_rows = rows(calm_m(121, 90), BJ(11, 30)) + rows([str(90 * (1 + 0.002 * (i % 2))) for i in range(21)], BJ(13, 21))
sh = m.session_minute_fn("sh")
assert m.minute_vol(open_rows, BJ(13, 21), 240, sh) > 1.3 > m.minute_vol(open_rows, BJ(13, 21), 240, sh, open_floor=False) > 0.5
assert len(m.minute_returns(open_rows, BJ(13, 21), sh)) == 139 and m.minute_returns(open_rows, BJ(13, 21), sh)[-20][0] == BJ(13, 1)
# 1.121.0: the shape of the day (vol_shape): the usual volatility by quarter hour of the local day from the last two weeks' 15-minute
# candles; σ is the level the last hours show over the level those same hours usually have, times the window's usual (minute_vol_shaped)
bj_day = lambda d, h, mi: int(dt.datetime(2026, 10, 20 + d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
def quarter_rows(days, lively=lambda h: 20 <= h <= 23, step=0.001, big=0.002):
    """Ten days of 15-minute candles: ±step a candle, ±big in the lively hours (a 2× σ there)."""
    out = []
    for d in range(days):
        for q in range(96):
            o = bj_day(d, q // 4, q % 4 * 15)
            move = (big if lively(q // 4) else step) * (1 if q % 2 else -1)
            out.append([o, "100", "100", "100", str(100 * math.exp(move)), "0", o + 899_999, "0", 0, "0", "0", "0"])
    return out
SHAPE_NOW = bj_day(10, 3, 0)
shape = m.vol_shape(quarter_rows(10), SHAPE_NOW, m.BEIJING)
assert shape is not None and shape.candles == 960 and abs(shape.span_days - (10 - 1 / 96)) < 1e-6 and shape.slot(bj_day(3, 21, 7)) == 84 and shape.slot(bj_day(3, 0, 0)) == 0
assert m.vol_shape(quarter_rows(10)[:95], SHAPE_NOW, m.BEIJING) is None and m.vol_shape(quarter_rows(10)[:96], SHAPE_NOW, m.BEIJING) is not None
assert m.vol_shape(quarter_rows(10), bj_day(0, 1, 0), m.BEIJING) is None, "the running and the later candles do not count"
assert 1.55 < shape.sigma_at(bj_day(0, 21, 30)) / shape.sigma_at(bj_day(0, 3, 0)) < 1.8, "2× σ in the lively hours, pooled with the neighbours and the day's mean"
assert shape.sigma_at(bj_day(0, 19, 45)) > shape.sigma_at(bj_day(0, 19, 0)) and abs(shape.sigma_at(bj_day(0, 19, 0)) - shape.sigma_at(bj_day(0, 3, 0))) < 1e-12, "the quarter before a lively hour borrows half of it; a quiet one among quiet ones is the same as any"
assert abs(shape.sigma_at(bj_day(0, 3, 0)) - math.sqrt((8 * shape.mean + 20 * 0.001 ** 2 / 15 * 365 * 24 * 60) / 28)) < 1e-9, "a quiet slot: its ten candles, the neighbours' at half, eight at the mean"
assert shape.sigma_at(bj_day(0, 3, 0)) == shape.sigma_at(bj_day(0, 3, 14)) and shape.var_at(bj_day(0, 3, 0)) == shape.var[12]
# the level: calm 1-minute candles in a quiet hour price a lively hour's window at the shape's ratio, a quiet one's near the plain figure
calm_rows = rows(calm, bj_day(10, 3, 0))  # 03:00: four calm hours (23:00–03:00 crosses the lively ones: the usual there is above the 03:00 slot's)
plain = m.minute_vol(calm_rows, SHAPE_NOW)
shaped, factor = m.minute_vol_shaped(calm_rows, SHAPE_NOW, shape=shape, at_ms=bj_day(10, 21, 30))
assert abs(shaped - plain * factor) < 1e-12 and 1.55 < factor < 1.65 and m.minute_vol(calm_rows, SHAPE_NOW, shape=shape, at_ms=bj_day(10, 21, 30)) == shaped, factor
quiet_shaped, quiet_factor = m.minute_vol_shaped(calm_rows, SHAPE_NOW, shape=shape, at_ms=bj_day(10, 3, 0))
assert 0.9 < quiet_factor < 0.97 and quiet_shaped < plain < shaped, (quiet_factor, factor)  # the recent minutes crossed the lively hours: their usual is above the 03:00 slot's
assert m.minute_vol_shaped(calm_rows, SHAPE_NOW, shape=shape)[1] == quiet_factor, "no at_ms: now's slot"
assert m.minute_vol_shaped(calm_rows, SHAPE_NOW)[1] == 1.0 and m.minute_vol_shaped(calm_rows, SHAPE_NOW)[0] == plain, "no shape: the plain figure"
assert m.minute_vol_shaped(rows(["90"] * 241, SHAPE_NOW), SHAPE_NOW, shape=shape, at_ms=bj_day(10, 21, 30)) == (0.0, 1.0), "not moved at all: 0, whatever the shape"
spike = m.VolShape({slot: (1e-3 if slot == 86 else 1e-9 if slot == 12 else 1e-5) for slot in range(96)}, 1e-5, 960, 10.0, m.BEIJING, SHAPE_NOW)
assert m.minute_vol_shaped(calm_rows, SHAPE_NOW, shape=spike, at_ms=bj_day(10, 21, 30))[1] == 2.5 and m.minute_vol_shaped(calm_rows, SHAPE_NOW, shape=spike, at_ms=bj_day(10, 3, 0))[1] == 0.4, "bounded: a slot ten times, a hundredth of the recent minutes' usual"
# a stock's shape counts the candles wholly inside its session: the lunch break and the night are no slot of its day
sh_rows = quarter_rows(10, lively=lambda h: h == 9, step=0.001, big=0.003)  # the open hour's candles 3× (two of them in session: 09:30, 09:45)
sh_shape = m.vol_shape(sh_rows, bj_day(10, 15, 0), m.BEIJING, m.session_minute_fn("sh"))
assert sh_shape is not None and sh_shape.candles == 8 * 16, sh_shape.candles  # eight trading days of 16 quarter hours (10-24/25 are a weekend)
in_session = [k for k in range(96) if m.session_minute_fn("sh")(bj_day(0, k // 4, k % 4 * 15))]
assert len(in_session) == 16 and sh_shape.var[47] == sh_shape.var[49] == sh_shape.mean != sh_shape.var[52] and sh_shape.mean > sh_shape.var[46] > sh_shape.var[52], \
    "11:30–13:00 and the night: no candles, the mean stays — the 11:30 slot borrows half of 11:15's (quiet), 13:00 is a session slot of its own"
assert sh_shape.sigma_at(bj_day(0, 9, 30)) > 1.5 * sh_shape.sigma_at(bj_day(0, 14, 0)) and sh_shape.sigma_at(bj_day(0, 9, 45)) > sh_shape.sigma_at(bj_day(0, 10, 0)) > sh_shape.sigma_at(bj_day(0, 14, 0))
try:
    m.vol_shape([[bj_day(0, 1, 0), "0", "1", "1", "1", "0", bj_day(0, 1, 0) + 899_999, "0", 0, "0", "0", "0"]] * 100, SHAPE_NOW, m.BEIJING); assert False
except ValueError as error:
    assert str(error) == "15 分钟 K 线价格无效"
assert (m.SHAPE_DAYS, m.SHAPE_MIN_CANDLES, m.SHAPE_PRIOR, m.SHAPE_FACTOR_BOUNDS, m.IndexUpDownMarket.SHAPE_SECONDS, m.IndexUpDownMarket.SHAPE_ROWS, m.SpotWindowMarket.SHAPE_ROWS) == (14, 96, 8, (0.4, 2.5), 3600, 1500, 1000)
# 1.122.0: a pushed or read index is as fresh as its own event time — a frame delivered minutes late (a stalled socket catching up)
# used to make the card look current and price again on an old index
_late = m.IndexUpDownMarket(m.Store(":memory:"), W.current(T0), None, "sh", m.session_minute_fn("sh"))
assert _late.push_index(D("90"), T0 - 599_000, T0) and _late.priced_ms == T0 - 599_000 and T0 - _late.priced_ms > _late.PRICE_STALE_MS and not _late.push_index(D("91"), T0 - 600_000, T0)
assert _late.push_index(D("90.5"), T0 - 1_000, T0) and _late.priced_ms == T0 - 1_000 and T0 - _late.priced_ms <= _late.PRICE_STALE_MS
assert _late.push_index(D("90.6"), T0 + 5_000, T0) and _late.priced_ms == T0, "an event time ahead of the bot's clock counts as now"
# 1.106.1: the daily crypto cards' hourly σ weighs the last five days about half — a month of calm does not price a week that moves
hourly = lambda closes, end: [[end - (len(closes) - i) * 3600_000, c, c, c, c, "0", end - (len(closes) - i) * 3600_000 + 3599_999, "0", 0, "0", "0", "0"]
                              for i, c in enumerate(closes)]
month = [str(70000 * (1 + 0.001 * (i % 2))) for i in range(600)] + [str(70000 * (1 + 0.01 * (i % 2))) for i in range(122)]
assert m.realized_vol(hourly(month, T0), T0, half_life=120) > 1.5 * m.realized_vol(hourly(month, T0), T0) > 0 and m.UpDownMarket.SIGMA_HALF_LIFE == 120
assert m.session_segments("sh", dt.date(2026, 11, 4)) == [(at(9, 30), at(11, 30)), (at(13, 0), at(15, 0))] and m.session_segments("kr", dt.date(2026, 11, 7)) == []


class FM:
    """The bot's own Binance futures feed (BINANCE_BASE_URL): the index's 1-minute candles, its premium index."""
    def __init__(self, now):
        self.now, self.config, self.paths = now, None, []
        self.index = {}       # candle open time -> close: the boundaries
        self.premium = None   # (price, time)
        self.closes = calm
        self.shape_rows = []  # the 15-minute candles the shape of the day is read from (1.121.0): none = no shape, the plain σ

    def now_ms(self): return self.now

    async def get(self, path, **params):
        self.paths.append(path)
        if path == "/fapi/v1/indexPriceKlines":
            assert params["pair"] == "UNITREEUSDT" and params["interval"] in ("1m", "15m"), params
            if params["interval"] == "15m":
                assert params["limit"] == 1500 and "startTime" not in params, params
                return self.shape_rows
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
    assert c.sim_crypto_15m_maker_quiet == 180  # 1.116.0: no new resting order in a window's last three minutes
    for bad in ({"SIM_CRYPTO_15M_QUIET_SECONDS": "601"}, {"SIM_CRYPTO_15M_EDGE_CENTS": "51"}, {"SIM_CRYPTO_15M_SHARES": "-1"}, {"SIM_CRYPTO_15M_MAKER_QUIET_SECONDS": "601"}):
        try: m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", **bad}); assert False, bad
        except ValueError: pass
    assert {"SIM_CRYPTO_15M_SHARES", "SIM_CRYPTO_15M_MAX_SHARES", "SIM_CRYPTO_15M_QUIET_SECONDS", "SIM_CRYPTO_15M_EDGE_CENTS", "SIM_CRYPTO_15M_MAKER_QUIET_SECONDS"} <= m.CONTROL_KEY_SET

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
    assert bot.control_value("SIM_CRYPTO_15M_MAKER_QUIET_SECONDS") == "180"
    # before the start's candle is read
    assert card.odds(T0 + 5_000) == "等待币安 UNITREEUSDT 11:00 的指数价（起点）" and card.exchange == "sh" and card.trading(T0)
    # the index is read every 2 s from a quarter hour before the window to its end, every 30 s otherwise (σ: 60 s / 600 s)
    far = card.at(W.window_spec(T0 + 7_200_000))
    assert card.cadence(T0 + 5_000) == (1, 60) and far.cadence(T0 + 5_000) == (30, 600) and far.cadence(T0 + 7_200_000 - 15 * 60_000) == (1, 60)
    fm.index[T0 - 60_000], fm.premium = "90.0000", ("90.1200", T0 + 4_500)
    await card.refresh(T0 + 5_000)
    assert card.close_of("start") == D("90.0000") and card.price == D("90.1200") and card.sigma > 0 and card.error == "", card.error
    assert fm.paths.count("/fapi/v1/indexPriceKlines") == 3 and "/fapi/v1/premiumIndex" in fm.paths  # the start candle, the shape (15m), σ (1m)
    assert card.shape is None and card.shape_factor == 1.0 and card.shape_error == "近 14 日的 15 分钟 K 线不足 96 根" and card.shape_ms == T0 + 5_000 - 55 * 60_000
    # 1.121.0: the shape of the day (vol_shape) is read with σ — at once while there is none, then hourly — and σ is put on it:
    # the window's quarter hour's usual over the recent minutes' usual; the next window's market carries the shape; the σ note says so
    fm.shape_rows = [[o, "100", "100", "100", str(100 * math.exp((0.003 if dt.datetime.fromtimestamp(o / 1000, m.BEIJING).hour == 9 else 0.001) * (1 if (o // 900_000) % 2 else -1))),
                      "0", o + 899_999, "0", 0, "0", "0", "0"] for o in range(T0 - 12 * 86_400_000, T0, 900_000)]  # twelve days of quarter hours, the 09:xx ones 3×
    shaped = m.IndexUpDownMarket(store, spec, bot.binance_futures, "sh", m.session_minute_fn("sh"))
    n_before = fm.paths.count("/fapi/v1/indexPriceKlines")
    await shaped.read_vol(T0 + 5_000)
    assert shaped.shape is not None and shaped.shape_ms == T0 + 5_000 and shaped.shape_error == "" and shaped.shape.candles >= 96 and shaped.shape.tz == m.BEIJING
    assert fm.paths.count("/fapi/v1/indexPriceKlines") == n_before + 2  # the shape (15m) and σ (1m)
    minute_rows = [[T0 - (len(calm) - i) * 60_000, c, c, c, c, "0", T0 - (len(calm) - i) * 60_000 + 59_999, "0", 0, "0", "0", "0"] for i, c in enumerate(calm)]  # what the fake serves
    plain = m.minute_vol(minute_rows, T0 + 5_000, 240, m.session_minute_fn("sh"))
    assert 0.5 < shaped.shape_factor < 0.95 and abs(shaped.sigma - plain * shaped.shape_factor) < 1e-12, (shaped.shape_factor, shaped.sigma, plain)  # 11:00 is a quiet quarter, the recent minutes crossed the open's
    assert shaped.shape.sigma_at(T0 - 90 * 60_000) > 2 * shaped.shape.sigma_at(T0) and shaped.shape_note() == f"，分时形态 ×{shaped.shape_factor:.2f}（近 12 日同一时段）", shaped.shape_note()
    shaped.push_index(D("90.1200"), T0 + 4_500, T0 + 5_000)
    pay = bot.updown_payload(shaped, T0 + 5_000)
    assert pay["sigma_note"].startswith("币安合约 UNITREEUSDT 指数近 4 小时 1 分钟 K（近 1 小时权重过半，开盘首小时按开盘后的）年化 ") and shaped.shape_note() in pay["sigma_note"], pay["sigma_note"]
    assert abs(pay["sigma_daily"] - shaped.sigma / math.sqrt(365)) < 1e-12
    await shaped.read_vol(T0 + 65_000)  # within the hour: σ alone is read again
    assert fm.paths.count("/fapi/v1/indexPriceKlines") == n_before + 3 and shaped.shape_ms == T0 + 5_000
    nxt = shaped.at(W.window_spec(T1))
    assert nxt.shape is shaped.shape and nxt.shape_ms == shaped.shape_ms and nxt.shape_factor == 1.0  # carried; its own σ not read yet
    fm.shape_rows = [[o, "0", "1", "1", "1", "0", o + 899_999, "0", 0, "0", "0", "0"] for o in range(T0 - 86_400_000, T0, 900_000)]  # a bad read an hour on
    await shaped.read_vol(T0 + 3_605_000)
    assert shaped.shape is not None and shaped.shape_error == "15 分钟 K 线价格无效" and shaped.shape_ms == T0 + 3_605_000 - 55 * 60_000, "the shape in hand stands; tried again in five minutes"
    assert shaped.shape_note().startswith("，分时形态 ×") and fm.paths.count("/fapi/v1/indexPriceKlines") == n_before + 5
    fm.shape_rows = []
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
    # an index read by REST that moves the card wakes the express lane as a push would (1.104.2: the futures stream mute here)
    bot.price_dirty.clear()
    fm.premium = ("90.3300", T0 + 8_000)
    card.times["price"] = -1e9
    await bot.refresh_updown(T0 + 8_000)
    assert card.price == D("90.3300") and bot.price_dirty == {"UNITREE-15M"}
    bot.price_dirty.clear()
    card.times["price"] = -1e9
    await bot.refresh_updown(T0 + 9_000)  # the same index again: nothing moved, nothing to look at
    assert card.price == D("90.3300") and not bot.price_dirty
    # paused: an index a minute old; an index that has not moved for hours
    assert card.odds(T0 + 8_000 + 61_000).startswith("指数价停在 ") and "暂停概率" in card.odds(T0 + 70_000)
    sigma, card.sigma = card.sigma, 0.0
    assert card.odds(T0 + 7_000) == "指数在最近的交易时段里没有变动，暂停概率"
    card.sigma = sigma

    # --- 1.108.0: the stock's own exchange quote leads the index (LEAD_QUOTE): the index at the start times the quote's ratio to
    # its own start price, when the quote is fresh, the start price was caught and the two agree within 1% ------------------
    assert m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}).lead_quote == "on" and m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "LEAD_QUOTE": " Shadow"}).lead_quote == "shadow"
    try: m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "LEAD_QUOTE": "maybe"}); assert False
    except ValueError as error: assert "LEAD_QUOTE 只能是 on、shadow、off" in str(error)
    assert "LEAD_QUOTE" in m.CONTROL_KEY_SET and bot.control_value("LEAD_QUOTE") == "on" and (sk.lead_mode, sk.lead_code) == ("on", "000660")
    assert (card.lead_mode, card.lead_code) == ("off", "")  # UNITREEUSDT is not in this test's SYMBOLS: no exchange quote is read for it (StockMarket.refresh_live)
    bot.config.tickers["UNITREEUSDT"] = m.parse_tickers("UNITREEUSDT=sh:688836", ("UNITREEUSDT",))["UNITREEUSDT"]  # as SYMBOLS would have it
    card.lead_code, card.lead_mode = "688836", "on"
    assert card.at(spec).lead_mode == "off"  # a past window's market: the index alone
    quote = lambda last, quoted, fetched: m.IndexQuote("宇树科技", D(last), D("149.00"), None, None, None, quoted, "腾讯", "交易中", fetched)
    assert card.lead_price(T0 + 9_000) == (None, 0.0, "交易所没有实时行情") and card.effective(T0 + 9_000) == (D("90.3300"), "") and card.lead_text(T0 + 9_000) == ""
    bot.stocks.live["UNITREEUSDT"] = quote("150.00", T0 + 8_500, T0 + 9_000)  # the first quote after the start moment (T0): the window's start price
    bot.price_dirty.clear(); card.times["price"] = -1e9
    await bot.refresh_updown(T0 + 9_000)
    assert card.lead_start() == (D("150.00"), T0 + 8_500) and store.get(f"updown:{spec.slug}:lead")["price"] == "150.00"
    # the stock has not moved since the start: the index's 90.33 is behind — the card prices at the line, and the express lane looks
    assert card.effective(T0 + 9_000) == (D("90.0000"), "交易所现价先行 -0.37%") and bot.price_dirty == {"UNITREE-15M"}, card.effective(T0 + 9_000)
    bot.stocks.live["UNITREEUSDT"] = quote("150.30", T0 + 9_800, T0 + 10_000)  # +0.2% at the exchange
    bot.price_dirty.clear(); card.times["price"] = -1e9
    await bot.refresh_updown(T0 + 10_000)
    led = card.effective(T0 + 10_000)[0]
    assert led == D("90.18") and card.odds(T0 + 10_000).price == led and card.odds(T0 + 10_000).line == D("90.0000") and bot.price_dirty == {"UNITREE-15M"}
    assert card.lead_text(T0 + 10_000) == ("交易所现价先行：腾讯 688836 最新价 150.30（" + m.stamp(T0 + 9_800) + "），相对窗口起点 150.00 为 +0.20%，"
                                           "折算指数 90.1800，比币安指数 90.3300 -0.17%；结算仍按币安指数（Chainlink）"), card.lead_text(T0 + 10_000)
    pay = bot.updown_payload(card, T0 + 10_000)
    assert (pay["source"], pay["effective"]) == ("交易所现价先行", "90.1800") and pay["proxy_note"].startswith("交易所现价先行：腾讯 688836"), pay["proxy_note"]
    ev = bot.market_evidence("updown", T0 + 10_000, mkt=card, odds=card.odds(T0 + 10_000))
    assert (ev["basis"]["price"], ev["basis"]["index"], ev["basis"]["lead_price"], ev["basis"]["lead_start"], ev["basis"]["lead_used"]) == (90.18, 90.33, 150.3, 150.0, True)
    assert ev["sources"][2]["what"] == "交易所" and ev["sources"][2]["symbol"] == "688836" and ev["sources"][2]["source"] == "腾讯" and ev["sources"][2]["price"] == 150.3, ev["sources"][2]
    # a quote the poll has not refreshed for 15 s, or one 1% off the index: the index alone (the note says why)
    assert card.lead_price(T0 + 25_001)[0] is None and card.lead_price(T0 + 25_001)[2] == "交易所行情 15 秒没读到" and card.effective(T0 + 25_001) == (D("90.3300"), "")
    card.note_lead(quote("153.10", T0 + 10_500, T0 + 11_000), T0 + 11_000)
    assert card.lead_price(T0 + 11_000)[0] is None and card.lead_price(T0 + 11_000)[2].startswith("交易所价折算 91.8600 与指数 90.3300 差 +1.68%，不用")
    assert card.effective(T0 + 11_000) == (D("90.3300"), "") and card.lead_text(T0 + 11_000).startswith("交易所现价不先行：腾讯 688836 最新价 153.10")
    # shadow: the card trades on the index, the sample keeps both fair prices; /calib scores them against the result
    card.lead_mode = "shadow"
    card.note_lead(quote("150.30", T0 + 11_800, T0 + 12_000), T0 + 12_000)
    assert card.effective(T0 + 12_000) == (D("90.3300"), "") and card.odds(T0 + 12_000).price == D("90.3300") and card.lead_text(T0 + 12_000).startswith("交易所现价只记录（LEAD_QUOTE=shadow）：")
    row = bot.window_snapshot(card, "window", 600, T0 + 12_000)
    assert row["price"] == 90.33 and row["lead_used"] is False and abs(row["lead_gap"] - math.log(90.18 / 90.33)) < 1e-9 and 0 < row["up_lead"] < row["up_index"] < 1, row
    assert row["up"] == row["up_index"]  # priced on the index in shadow mode
    card.lead_mode = "on"
    assert bot.window_snapshot(card, "window", 600, T0 + 12_000)["up"] == row["up_lead"]
    for i, (ui, ul, res) in enumerate(((0.55, 0.62, 1.0), (0.40, 0.30, 0.0), (0.70, 0.60, 1.0))):
        store.put(f"wsample:lead-test-{i}:840", {"t": T0 - 3_600_000 - i, "slug": f"lead-test-{i}", "key": "UNITREE-15M", "card": "宇树 15分涨跌", "kind": "window",
                                                 "start": T0 - 3_600_000, "end": T0 - 2_700_000, "left": 840, "up": ul, "up_index": ui, "up_lead": ul, "lead_gap": 0.001,
                                                 "lead_used": True, "result": res, "final": 1.0, "line": 1.0, "price": 1.0, "sigma": 0.5, "tau": 2.7e-5})
    rep = "\n".join(bot.window_report())
    assert "    交易所现价先行：3 条样本两种定价都有｜Brier 按指数 0.151、按交易所现价 0.131，先行更准的占 67%，先行幅度平均 0.10%（3 条按先行定价；" in rep, rep
    store.delete_keys([f"wsample:lead-test-{i}:840" for i in range(3)])
    # a bot that gets its first quote more than 20 s after a window's start prices that window on the index alone
    late = card.at(W.window_spec(T0 + 1_800_000)); late.lead_mode, late.lead_code = "on", "688836"
    at = late.spec.start_ms + 60_000
    late.note_lead(quote("151.00", at + 25_000, at + 26_000), at + 26_000)
    assert late.lead_start() is None and store.get(f"updown:{late.spec.slug}:lead")["missed"] is True
    late.note_lead(quote("151.00", at + 27_000, at + 28_000), at + 28_000)  # still missed: the marker is kept
    assert late.lead_start() is None and late.lead_price(at + 28_000) == (None, 0.0, "没有窗口起点的交易所价")
    bot.stocks.live.pop("UNITREEUSDT"); card.note_lead(None, T0 + 12_000)  # the rest of the test prices on the index
    assert card.effective(T0 + 12_000) == (D("90.3300"), "") and card.lead is None

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
                                                                                     "90.0000", "90.3300"), pay
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
        "sim_crypto_15m_shares": 10, "sim_crypto_15m_max_shares": 100, "sim_crypto_15m_quiet": 90, "sim_crypto_15m_edge": 0, "sim_crypto_15m_open_usd": 100,
        "sim_crypto_15m_maker_quiet": 180}
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
    assert room == 0 and why == "这个窗口两边持仓加挂单合计已有 100 份（涨 100、跌 0），达到15 分钟涨跌每个窗口合计上限 100 份（内测，SIM_CRYPTO_15M_MAX_SHARES）", why
    # 1.110.0: the window's cap is on both sides together, gross — 60 涨 filled and 50 跌 filled leave no room on either side;
    # a resting order counts at its whole count; nothing is netted
    both = m.TradeMap({f"{spec.slug}|up|吃": {**held, "shares": 60.0, "order": 60.0},
                       f"{spec.slug}|down|吃": {**held, "side": "down", "label": "吃跌", "shares": 50.0, "order": 50.0}})
    assert bot.sim_market_room(both, mk, "up") == (0.0, "这个窗口两边持仓加挂单合计已有 110 份（涨 60、跌 50），达到15 分钟涨跌每个窗口合计上限 100 份（内测，SIM_CRYPTO_15M_MAX_SHARES）")
    assert bot.sim_market_room(both, mk, "down")[0] == 0.0
    some = m.TradeMap({f"{spec.slug}|up|吃": {**held, "shares": 60.0, "order": 60.0},
                       f"{spec.slug}|down|挂": {**held, "side": "down", "label": "挂跌", "maker": True, "status": "resting", "shares": 0.0, "order": 10.0, "price": 0.3}})
    assert bot.sim_market_room(some, mk, "down") == (30.0, "") and bot.sim_market_room(some, mk, "up") == (30.0, "")
    # a floor order on a market in beta: its count (100 shares at 1¢, Predict's minimum order) is over the 10 a buy — none
    assert bot.sim_maker_pick(mk, m.TradeMap(), "up", m.BookEdge("涨", True, 0.01, 0.2, 0, floor=True), now) == (
        100.0, "托底单要 100 份（1.0¢×100 份 = Predict 最低订单金额 $1），超过15 分钟涨跌每单 10 份（内测，SIM_CRYPTO_15M_SHARES），不挂")
    # the unsettled windows' money (SIM_CRYPTO_15M_OPEN_USD): every window of the card, positions at cost, orders at their price
    tight = m.Bot(m.Config.from_env({**ENV, "SIM_CRYPTO_15M_OPEN_USD": "20"}), m.Store(":memory:"), FM(now), None)
    older = {**held, "market": W.before(spec).slug, "slug": W.before(spec).slug, "shares": 30.0, "order": 30.0, "price": 0.5}  # $15 on the window before, unsettled
    rest = {**held, "maker": True, "status": "resting", "shares": 0.0, "order": 10.0, "price": 0.3}  # $3 resting on this window
    tmap = m.TradeMap({f"{older['market']}|up|吃": older, f"{spec.slug}|up|挂": rest})
    assert tight.sim_window_open_usd(tmap, "UNITREE-15M") == 18.0 and tight.sim_window_usd_room(tmap, mk, 0.5) == (4.0, "") and bot.sim_window_usd_room(tmap, mk, 0.5) == (164.0, "")
    assert tight.sim_maker_pick(mk, tmap, "down", m.BookEdge("跌", True, 0.5, 0.2, 500), now) == (4.0, "")  # cut to the $2 left
    full_map = m.TradeMap({f"{older['market']}|up|吃": {**older, "shares": 40.0, "order": 40.0}})  # $20: nothing left
    assert tight.sim_window_usd_room(full_map, mk, 0.5) == (0.0, "这张卡未结算的窗口持仓加挂单已有 $20.00，达到15 分钟涨跌未结算窗口总资金上限 $20（内测，SIM_CRYPTO_15M_OPEN_USD）")
    assert tight.sim_window_usd_room(m.TradeMap({f"{older['market']}|up|吃": {**older, "shares": 39.0, "order": 39.0}}), mk, 0.5)[1].endswith("只剩 $0.50，不够一单（内测，SIM_CRYPTO_15M_OPEN_USD）")
    assert "SIM_CRYPTO_15M_OPEN_USD" in m.CONTROL_KEY_SET and bot.control_value("SIM_CRYPTO_15M_OPEN_USD") == "100"
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
    # 1.116.0: the last SIM_CRYPTO_15M_MAKER_QUIET_SECONDS (180) take no new resting order; a taker still goes and a resting order stays
    hush = "15 分钟涨跌结束前 180 秒不挂新单（内测，SIM_CRYPTO_15M_MAKER_QUIET_SECONDS）"
    assert bot.sim_maker_quiet(mk.settle, T1 - 181_000) == "" and bot.sim_maker_quiet(mk.settle, T1 - 179_000) == hush and bot.sim_maker_quiet({"end": T1}, T1 - 1) == ""
    edge = m.BookEdge("涨", True, 0.3, 0.2, 500)
    assert bot.sim_maker_pick(mk, m.TradeMap(), "up", edge, T1 - 181_000) == (10.0, "") and bot.sim_maker_pick(mk, m.TradeMap(), "up", edge, T1 - 179_000) == (10.0, hush)
    for at, n in ((T1 - 181_000, 1), (T1 - 179_000, 0)):
        got = []
        bot.sim_quote(late(at), m.TradeMap(), at, got)
        assert len([t for _, t in got if t["maker"]]) == n, (at, got)
    got = []
    bot.sim_take(late(T1 - 179_000), m.TradeMap(), T1 - 179_000, got)
    assert [(t["label"], t["order"]) for _, t in got] == [("吃涨", 10.0)] and bot.sim_withdraw_reason(resting, late(T1 - 179_000), T1 - 179_000) == ""
    # 1.118.0: a resting order filled to within a share of its count is withdrawn, its fills the position (the last fraction never fills)
    at = T1 - 300_000
    assert bot.sim_withdraw_reason({**resting, "shares": 9.5, "order": 10.0}, late(at), at) == "剩 0.5 份不到 1 份的零头，撤掉不再等（已成交 9.5 份照常持有）"
    assert bot.sim_withdraw_reason({**resting, "shares": 9.0, "order": 10.0}, late(at), at) == "" and bot.sim_withdraw_reason({**resting, "shares": 0.0, "order": 10.0}, late(at), at) == ""
    assert bot.sim_withdraw_reason({**resting, "shares": 10.0, "order": 10.0}, late(at), at) == ""  # nothing left: filled, not this
    hush_off = m.Bot(m.Config.from_env({**ENV, "SIM_CRYPTO_15M_MAKER_QUIET_SECONDS": "0"}), m.Store(":memory:"), FM(now), None)
    assert hush_off.sim_maker_quiet(mk.settle, T1 - 100_000) == "" and hush_off.sim_maker_pick(mk, m.TradeMap(), "up", edge, T1 - 100_000) == (10.0, "")
    long_quiet = m.Bot(m.Config.from_env({**ENV, "SIM_QUIET_MINUTES": "10", "SIM_CRYPTO_15M_QUIET_SECONDS": "0", "SIM_CRYPTO_15M_EDGE_CENTS": "6"}),
                       m.Store(":memory:"), FM(now), None)
    assert long_quiet.sim_quiet(mk.settle, T1 - 9 * 60_000) == ""  # SIM_QUIET_MINUTES would take most of the window: not applied here
    assert abs(long_quiet.sim_edge_bar(mk) - 0.06) < 1e-12 and long_quiet.sim_edge_bar(dataclasses_replace(mk, settle={"end": 1})) == long_quiet.config.sim_edge
    text = bot.sim_text()
    assert "15 分钟涨跌（宇树、SK 海力士、BTC、ETH、BNB，内测）：每次最多买 10 份（托底单也不例外），每个窗口两边持仓加挂单合计最多 100 份（不抵扣），每张卡未结算窗口的持仓加挂单合计不超过 $100，每个窗口最后 90 秒不下单、撤掉挂单（SIM_CRYPTO_15M_*）" in text, text
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
    nxt = W.current(T1)  # 1.105.2: the 11:15–11:30 window exists — consecutive, the settling candle is its line
    assert card.spec == nxt and nxt.slug == "unitree-updown-15m-1793762100" and card.close_of("start") == D("90.2000"), (card.spec, card.close_of("start"))
    assert "UNITREE-15M" in bot.predict_targets(T1 + 2_000)
    # the 11:15 window ends at the break: the card moves on to 13:00, and the feed follows it from a quarter hour before, not through the break
    T2 = T1 + 15 * 60_000
    fm.index[T2 - 60_000] = "90.5000"
    fm.now = bot.market.now = T2 + 2_000
    card.times["candle"] = -1e9
    await bot.refresh_updown(T2 + 2_000)
    nxt = W.current(T2)
    assert card.spec == nxt and nxt.slug == "unitree-updown-15m-1793768400" and card.close_of("start") is None, card.spec  # 13:00: after the break
    assert "UNITREE-15M" not in bot.predict_targets(T2 + 2_000) and "UNITREE-15M" in bot.predict_targets(nxt.start_ms + 60_000 - 15 * 60_000)
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
    # 1.107.2: /journal sends the journal's CSV, the words its filter (part of a card's name, 挂 / 吃, a kind); the window's
    # own columns (the start, the price and σ at the order, the minutes left, the fair price at the fill)
    doc = await bot.cmd_journal(m.Request("/journal", ["宇树", "吃"], 1, 0, 42))
    assert isinstance(doc, m.Document) and doc.filename == "journal-20261104-宇树_15分涨跌_吃单.csv" and doc.mime == "text/csv", doc.filename
    rows = list(csv.reader(io.StringIO(doc.data.decode("utf-8").lstrip("\ufeff"))))
    want = {tid for tid, t in bot.sim_trades().items() if not t.get("maker") and m.card_of(t) == "宇树 15分涨跌"}
    assert rows[0] == list(m.Bot.JOURNAL_COLUMNS) and {r[0] for r in rows[1:]} == want and f"{spec.slug}|up|吃" in want, (want, [r[0] for r in rows[1:]])
    assert doc.caption == f"交易日记 {len(want)} 笔（筛选：宇树 15分涨跌 · 吃单）；一笔一行，列的含义见 README", doc.caption
    col = dict(zip(rows[0], next(r for r in rows if r[0] == f"{spec.slug}|up|吃")))
    basis = trade["entry"]["basis"]
    assert float(col["起点"]) == basis["line"] == 90.0 and float(col["下单时现价"]) == round(basis["price"], 6) > 90 and float(col["σ（年化）"]) == round(basis["sigma"], 6) > 0
    assert float(col["下单距结束（分钟）"]) == round(basis["years"] * 525_600, 2) and 0 < float(col["下单距结束（分钟）"]) <= 15 and float(col["成交时公平价"]) == round(trade["fill_fair"], 6), col
    assert (await bot.cmd_journal(m.Request("/journal", ["没有这个"], 1, 0, 42))).startswith("没有名字含“没有这个”的市场。有记录的市场：宇树 15分涨跌")
    assert (await bot.cmd_journal(m.Request("/journal", ["宇树", "挂单"], 1, 0, 42))).startswith("没有符合的记录（筛选：宇树 15分涨跌 · 挂单）") or any(t.get("maker") for t in bot.sim_trades().values())
    whole = await bot.cmd_journal(m.Request("/journal", [], 1, 0, 42))
    assert whole.filename == "journal-20261104.csv" and whole.caption.startswith(f"交易日记 {len(bot.sim_trades())} 笔（全部）")
    sdoc = await bot.cmd_summary(m.Request("/summary", ["加密涨跌", "宇树"], 1, 0, 42))
    srows = list(csv.reader(io.StringIO(sdoc.data.decode("utf-8").lstrip("\ufeff"))))
    assert sdoc.filename == "summary-20261104-宇树_15分涨跌_加密涨跌.csv" and srows[0] == list(m.Bot.SUMMARY_COLUMNS) and srows[1][:3] == ["筛选", "宇树 15分涨跌 · 加密涨跌", str(sum(m.card_of(t) == "宇树 15分涨跌" for t in bot.sim_trades().values()))], srows[:2]
    assert sdoc.caption.startswith("复盘汇总（筛选：宇树 15分涨跌 · 加密涨跌）：每日汇总、今日按市场") and "15 分钟涨跌｜按下单时距结束" in {r[0] for r in srows}
    assert all(f"/{name}" in bot.cmd_help(None) and any(c.name == name for c in m.COMMANDS) for name in ("journal", "summary", "markout"))
    # 1.107.3: /markout: the 15-minute tables as text — a row per minutes-left bucket and per start hour, settled count and P&L
    said = await bot.cmd_markout(m.Request("/markout", ["宇树"], 1, 0, 42))
    assert said.startswith("📈 成交后市场走向｜宇树 15分涨跌（记录 ") and "15 分钟市场，按下单时离窗口结束：\n  剩 " in said and "15 分钟市场，按窗口开始（北京时间）：\n  11 时开始：" in said, said
    assert "15 分钟市场，按方向：\n  涨：" in said, said  # 1.110.0: each side on its own
    assert "吃单：" in said and ("已结算 1 笔，盈亏 $" in said or "还没有标记" in said), said
    # 1.107.4: the words match a card's name with spaces and case aside; a name shared by two cards says which to type
    base = {"v": 2, "fills": [], "revisions": [], "order": 10.0, "maker": False, "fair": 0.6, "edge": 0.1, "price": 0.5, "status": "filled",
            "shares": 10.0, "opened": T0, "filled": T0, "side": "up", "label": "吃涨", "key": "SKHYNIX", "driver": "SKHYNIXUSDT", "driver_name": "SK 海力士"}
    two = m.Bot(m.Config.from_env(ENV), m.Store(":memory:"), FM(T0), None)
    two.sim_save([("skd|up|吃", {**base, "market": "sk-hynix-inc-2026-11-05", "slug": "sk-hynix-inc-2026-11-05", "item": "SK 海力士｜SKHYNIXUSDT 11-05", "kind": "close",
                                 "settle": {"key": "SKHYNIXUSDT", "target": "2026-11-05", "line": 900.0, "close_ms": T0 + 3_600_000}}),
                  ("skw|up|吃", {**base, "market": "skhynix-updown-15m-1793858400", "slug": "skhynix-updown-15m-1793858400", "item": "SK 海力士 15分涨跌 14:00–14:15",
                                 "kind": "updown", "settle": {"end": T0 + 840_000, "start": T0 - 60_000, "window": "15m"}})])
    assert sorted({m.card_of(t) for t in two.sim_trades().values()}) == ["SK 海力士", "SK 海力士 15分涨跌"]
    assert two.journal_filter_words(["海力士", "挂"]) == ({}, "“海力士”匹配多个市场：SK 海力士、SK 海力士 15分涨跌。写具体些，比如 “SK 海力士” 或 “SK 海力士 15分涨跌”（空格、大小写不限）")
    assert two.journal_filter_words(["海力士15分涨跌", "挂"]) == ({"card": "SK 海力士 15分涨跌", "way": "maker"}, "")
    assert two.journal_filter_words(["sk海力士15分涨跌"]) == ({"card": "SK 海力士 15分涨跌"}, "") and two.journal_filter_words(["海力士", "15分"]) == ({"card": "SK 海力士 15分涨跌"}, "")
    assert two.journal_filter_words(["sk", "海力士"]) == ({"card": "SK 海力士"}, "")  # the whole name wins over the longer one that contains it
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
    assert m.Bot(m.Config.from_env(ENV), m.Store(":memory:"), FM(T1 + 61_000), None).updowns["UNITREE-15M"].spec == W.window_spec(T1)  # 11:15–11:30
    assert m.Bot(m.Config.from_env(ENV), m.Store(":memory:"), FM(T2 + 61_000), None).updowns["UNITREE-15M"].spec == nxt  # 13:00, over the break
    # --- 1.109.0: BTC 15分涨跌 — a spot pair's windows round the clock, settled on Chainlink's BTC/USDT stream (the mid of Binance
    # spot's top of book): the card reads the pair's trades and its 1-minute closes; in beta like the other 15-minute cards ------
    B = next(s for s in m.WINDOW_UPDOWN_MARKETS if s.key == "BTC-15M")
    assert (B.stem, B.symbol, B.coin, B.venue, B.exchange) == ("btc", "BTCUSDT", "BTC", "spot", "") and W.venue == "futures"
    bspec = B.window_spec(T0)
    assert bspec.slug == f"btc-updown-15m-{T0 // 1000}" and bspec.name == "BTC 15分涨跌 11:00–11:15" and B.current(T0 + 5_000) == bspec
    assert B.current(T0 + 20 * 60_000).start_ms == T1 - 60_000 and B.before(bspec).start_ms == T0 - 16 * 60_000  # every quarter hour, no calendar
    store2 = m.Store(":memory:")
    store2.put(f"updown:{B.before(bspec).slug}:end", {"open": T0 - 60_000, "close": "100000.00", "read_ms": T0 + 2_000})
    bbot = m.Bot(m.Config.from_env(ENV), store2, FM(T0 + 5_000), None)
    btc = bbot.updowns["BTC-15M"]
    assert isinstance(btc, m.SpotWindowMarket) and btc.spec == bspec and btc.lead_mode == "off" and btc.GROUP == "crypto" and btc.TIE == D("0.0001")
    assert "BTCUSDT" not in bbot.index_markets and [c.spec.key for c in bbot.pair_markets[("spot", "BTCUSDT")]][:3] == ["BTC-2027", "BTC-DAY", "BTC-15M"]  # the touch card (1.132.0), then the price ladders
    assert "BTCUSDT" in bbot.binance_streams["spot"].symbols() and "btcusdt@aggTrade" in bbot.binance_streams["spot"].names()
    assert "BTCUSDT" not in bbot.binance_streams["futures"].symbols() and set(bbot.index_markets) == {"UNITREEUSDT", "SKHYNIXUSDT"}
    assert bbot.predict_targets(T0 + 5_000)["BTC-15M"] == bspec.slug and bspec.slug in bbot.predict.want_info
    closes, tick = {bspec.start_ms: "100000.00"}, ["100050.00"]
    calm_btc = [str(100000 * (1 + 0.0002 * (i % 2))) for i in range(241)]

    async def spot(path, **params):
        if path == "ticker/price":
            assert params == {"symbol": "BTCUSDT"}, params
            return {"symbol": "BTCUSDT", "price": tick[0]}
        assert path == "klines" and params["symbol"] == "BTCUSDT" and params["interval"] in ("1m", "15m"), (path, params)
        if params["interval"] == "15m":
            assert params["limit"] == 1000, params  # the spot API's most (1.121.0: the shape of the day)
            return []
        if "startTime" in params:
            o, c = params["startTime"], closes.get(params["startTime"])
            return [] if c is None else [[o, c, c, c, c, "0", o + 59_999, "0", 0, "0", "0", "0"]]
        return [[T0 - (len(calm_btc) - i) * 60_000, c, c, c, c, "0", T0 - (len(calm_btc) - i) * 60_000 + 59_999, "0", 0, "0", "0", "0"]
                for i, c in enumerate(calm_btc)]  # (the module's rows() is shadowed by a list above)
    btc.get = spot
    assert btc.odds(T0 + 1_000) == "等待币安 BTCUSDT 22:00 ET（北京 11:00） 的现货价（起点）", btc.odds(T0 + 1_000)
    await bbot.refresh_updown(T0 + 5_000)
    odds = btc.odds(T0 + 5_000)
    assert isinstance(odds, m.UpDownOdds) and (odds.line, odds.price) == (D("100000.00"), D("100050.00")) and 0.6 < odds.fair_up < 0.9 and btc.sigma > 0, odds  # +0.05% on a calm σ: about 74%
    # a trade pushed on the spot stream prices it and wakes the express lane, as a pushed index does for the other cards
    bbot.price_dirty.clear()
    await bbot.binance_streams["spot"].frame(None, json.dumps({"e": "aggTrade", "s": "BTCUSDT", "p": "99950.00", "T": T0 + 6_000}))
    assert btc.price == D("99950.00") and "BTC-15M" in bbot.price_dirty and btc.odds(T0 + 6_000).fair_up < 0.5
    bbot.predict.books["BTC-15M"] = m.PredictBook("BTC-15M", bspec.slug, "702", "t", ((D("0.40"), D("300")),), ((D("0.45"), D("300")),), T0 + 6_000, None)
    bbot.predict.info[bspec.slug] = {"outcomes": ["Up", "Down"], "created_ms": 0, "question": "BTC Up or Down - November 3, 10:00PM-10:15PM ET"}  # which side is Up
    pay = bbot.updown_payload(btc, T0 + 6_000)
    assert (pay["group"], pay["source"], pay["ref"], pay["effective"]) == ("crypto", btc.SOURCE, "100,000.0000", "99,950.0000"), pay
    assert pay["close_label"].startswith("22:15 ET（北京 11:15） 的币安现货 BTCUSDT 价格（Chainlink 结算源）") and pay["ref_note"] == "币安现货 BTCUSDT 价格，22:00 ET（北京 11:00）", pay
    assert pay["proxy_note"] == btc.PROXY_NOTE and pay["sigma_note"].startswith("币安现货 BTCUSDT 近 4 小时 1 分钟 K（近 1 小时权重过半）年化 "), pay["sigma_note"]
    assert [i["group"] for i in bbot.odds_payload()["items"] if i.get("symbol") == "BTC-15M"] == ["crypto"]
    ev = bbot.market_evidence("updown", T0 + 6_000, mkt=btc, odds=btc.odds(T0 + 6_000))
    assert (ev["sources"][0]["what"], ev["sources"][0]["source"], ev["sources"][0]["type"], ev["sources"][0]["price"]) == ("现货", btc.SOURCE, "最新成交", 99950.0), ev["sources"]
    assert ev["sources"][1]["source"] == "币安现货 1 分钟 K" and ev["basis"]["window"] == bspec.slug and "lead_price" not in ev["basis"]
    mk = next(x for x in bbot.sim_markets(T0 + 6_000) if x.key == "BTC-15M")
    assert mk.settle == {"end": T1 - 60_000, "start": T0 - 60_000, "window": "15m"} and bbot.sim_order_cap(mk) == 10 and bbot.sim_market_room({}, mk, "up") == (100, "")
    assert m.card_of({"item": bspec.name, "kind": "updown", "settle": mk.settle}) == "BTC 15分涨跌" and bbot.window_kind(btc) == "window"
    assert {"kind": "updown", "key": "BTC-15M", "name": bspec.name, "skipped": False} in bbot.sim_catalog(T0 + 6_000)
    # 1.110.0: the line is Chainlink's figure as Predict relays it (the window's start price) once it agrees with the candle; a
    # spot window waits up to 60 s for it before suggesting anything, and a figure 0.1% off holds the card instead
    # 1.112.2: no card waits for the figure by default (the candle has matched it to half a tick; the figure replaces the line the
    # moment it comes, line_used); the wait itself stays (STRIKE_WAIT_MS) and applies only where a figure can come — REST gave
    # one for this card's kind lately, or GraphQL (retired) knows the category type
    assert card.STRIKE_WAIT_MS == 0 and btc.STRIKE_WAIT_MS == 0 and m.SpotWindowMarket.STRIKE_WAIT_MS == 0
    assert bbot.updown_hold(btc, btc.odds(T0 + 6_000), T0 + 6_000) == ("", "") and not bbot.predict.strike_possible(bspec.slug)
    bbot.predict.strike_seen["btc-updown-15m"] = time.monotonic()
    assert bbot.predict.strike_possible(bspec.slug) and bbot.updown_hold(btc, btc.odds(T0 + 6_000), T0 + 6_000) == ("", "")  # possible, not waited for
    btc.STRIKE_WAIT_MS = 20_000  # the wait, where wanted
    assert bbot.updown_hold(btc, btc.odds(T0 + 6_000), T0 + 6_000) == ("等 Predict 公布这个窗口的起点价（最多 20 秒）", "") and bbot.updown_hold(btc, btc.odds(T0 + 6_000), T0 + 21_000) == ("", "")
    bbot.predict.strike_seen["btc-updown-15m"] = time.monotonic() - 90_000  # a day without one: not expected any more
    assert bbot.updown_hold(btc, btc.odds(T0 + 6_000), T0 + 6_000) == ("", "") and not bbot.predict.strike_possible(bspec.slug)
    bbot.predict.types[bspec.slug] = "CryptoUpDownCategory"  # GraphQL would know the type (where it still answers)
    assert bbot.predict.strike_possible(bspec.slug) and bbot.updown_hold(btc, btc.odds(T0 + 6_000), T0 + 6_000)[0].startswith("等 Predict 公布")
    bbot.predict.graphql_until = time.monotonic() + 600  # paused after a 403: no wait either
    assert bbot.updown_hold(btc, btc.odds(T0 + 6_000), T0 + 6_000) == ("", "") and not bbot.predict.strike_possible(bspec.slug)
    bbot.predict.graphql_until = 0.0
    btc.STRIKE_WAIT_MS = 0
    bbot.predict.strikes[bspec.slug] = (D("100000.50"), 0.0)
    btc.times["price"] = -1e9
    await bbot.refresh_updown(T0 + 6_500)
    assert btc.strike == D("100000.50") and btc.odds(T0 + 6_500).line == D("100000.50") and btc.close_of("start") == D("100000.00")
    assert bbot.updown_hold(btc, btc.odds(T0 + 6_500), T0 + 6_500) == ("", "；与 Predict 目标价一致")
    assert bbot.market_evidence("updown", T0 + 6_500, mkt=btc, odds=btc.odds(T0 + 6_500))["basis"]["line_source"] == "Predict 目标价（结算源的起点）"
    bbot.predict.strikes[bspec.slug] = (D("100200.00"), 0.0)  # 0.2% off: another window's figure — the candle stays the line, nothing is suggested
    await bbot.refresh_updown(T0 + 6_600)
    assert btc.odds(T0 + 6_600).line == D("100000.00") and bbot.updown_hold(btc, btc.odds(T0 + 6_600), T0 + 6_600)[0] == "Predict 目标价 100,200.0000 与起点现货 100,000.0000 不一致，请核实；暂不给建议"
    bbot.predict.strikes.pop(bspec.slug)
    # 1.110.1: /market shows what Predict says about the card's market, and which line the card prices on
    assert bbot.market_slug_of("btc15分涨跌", T0 + 6_600) == (bspec.slug, "BTC-15M") and bbot.market_slug_of(bspec.slug, T0 + 6_600) == (bspec.slug, "BTC-15M")
    assert bbot.market_slug_of("没有这个", T0 + 6_600) == ("", "没有名字含“没有这个”的卡片") and bbot.market_slug_of("btc", T0 + 6_600)[1].startswith("“btc”匹配多个：")
    async def fake_resolve(slug): return {"id": "55", "title": "BTC 涨或跌 15分"} if slug == bspec.slug else None
    async def fake_details(mid): return {"outcomes": ["Up", "Down"], "created_ms": T0 - 900_000, "start_price": D("100000.50"), "status": "OPEN", "fee_bps": 200,
                                         "resolved": None, "price_fields": {"startPrice": "100000.5"}, "rules": "", "question": "", "trading_status": "TRADING"}
    async def fake_strike(slug, mid, eager=False): return D("100000.50")
    async def fake_fetch(url, payload=None): return {"id": "c", "slug": bspec.slug, "markets": [{"id": "55", "marketData": {"marketId": "55", "startPrice": "100000.5"}}]}
    bbot.predict.resolve, bbot.predict.market_details, bbot.predict.strike, bbot.predict.fetch = fake_resolve, fake_details, fake_strike, fake_fetch
    bbot.predict.types[bspec.slug] = "CryptoUpDownCategory"
    said = await bbot.cmd_market(m.Request("/market", ["btc15分涨跌"], 1, 0, 42))
    assert said.split("\n") == [f"🔎 {bspec.slug}（BTC-15M）", "市场 #55：BTC 涨或跌 15分；类别类型 CryptoUpDownCategory",
                                 f"结果名称 Up、Down；状态 OPEN / TRADING；创建 {m.stamp(T0 - 900_000, seconds=False)}；手续费 200 bps",
                                 "详情里和价格有关的字段：startPrice=100000.5",
                                 "类别 REST 里和价格有关的字段：markets[0].marketData.startPrice=100000.5", "GraphQL 可用",  # 1.112.0
                                 "Predict 起点价（startPrice）：100,000.5000",
                                 "卡片：起点 K 线 100,000.0000；定价用的起点 100,000.5000（Predict 起点价）；两者差 +0.001%"], said
    assert (await bbot.cmd_market(m.Request("/market", [], 1, 0, 42))).startswith("用法：/market ") and "没有名字含" in await bbot.cmd_market(m.Request("/market", ["没有这个"], 1, 0, 42))
    btc.strike = None
    # settled on the pair's 1-minute closes: the end 0.011% over the start is 涨; one within 0.01% is left to Predict
    closes[bspec.end_ms] = "100011.00"
    btc.times["candle"] = -1e9  # the candles are asked for every 5 s (monotonic): due now
    await bbot.refresh_updown(T1 + 2_000)
    assert store2.get(f"updown:{bspec.slug}:end")["close"] == "100011.00" and btc.spec.start_ms == T1 - 60_000  # read, and the card moved on
    done = btc.at(bspec).odds(T1 + 2_000)
    assert isinstance(done, m.UpDownOdds) and done.settled and (done.up, done.price, done.line) == (1.0, D("100011.00"), D("100000.00")), done
    tie = btc.at(B.before(bspec))  # the window before: its end (100,000) against a start 0.004% under it
    store2.put(f"updown:{tie.spec.slug}:start", {"open": tie.spec.start_ms, "close": "99996.00", "read_ms": T0})
    assert tie.odds(T1).startswith("终点现货 100,000.0000 与起点 99,996.0000 相差不到 0.01%，Chainlink 的报价可能不同"), tie.odds(T1)
    print("WINDOW_OK")


def dataclasses_replace(mk, **kw):
    import dataclasses
    return dataclasses.replace(mk, **kw)


asyncio.run(run())


class FakeTg:
    def __init__(self): self.sent, self.docs = [], []
    async def call(self, *a, **k): return True
    async def send(self, chat, thread, text, reply_markup=None, parse_mode=None): self.sent.append((chat, text)); return True
    async def send_document(self, chat, thread, doc): self.docs.append((chat, thread, doc)); return True


async def samples():
    """1.104.0: the model samples. Each rolling card's market is sampled at fixed times before it ends — a 15-minute window
    at 14 / 10 / 5 / 2 minutes left — with the model's fair price, σ and τ, the index, the book's middle and touch; the
    market's result settles them at the roll (or later, on the kept end); /calib scores them and /samples sends them as a
    CSV through Telegram (the review page serves samples.csv)."""
    fm, store, tg = FM(T0 + 5_000), m.Store(":memory:"), FakeTg()
    prev = W.before(spec)
    store.put(f"updown:{prev.slug}:end", {"open": prev.end_ms, "close": "89.9500", "read_ms": T0 + 2_000})
    bot = m.Bot(m.Config.from_env({**ENV, "ADMIN_USER_ID": "42"}), store, fm, tg)
    card = bot.updowns["UNITREE-15M"]
    fm.index[T0 - 60_000], fm.premium = "90.0000", ("90.1200", T0 + 4_500)
    await card.refresh(T0 + 5_000)
    bot.predict.books["UNITREE-15M"] = book("0.40", "0.45", T0 + 5_000, spec.slug)
    bot.predict.info[spec.slug] = {"outcomes": ["Up", "Down"], "created_ms": 0, "question": ""}
    assert m.WSAMPLE_HORIZONS == {"window": (840, 600, 300, 120), "day": (43200, 21600, 7200, 1800)}
    # nothing before a minute in; then the 14-minutes-left sample, once (SK 海力士's 10:45 window is past its moments: passed over)
    assert bot.window_sample(T0 + 30_000) == 0 and not store.keys("wsample:")
    assert bot.window_sample(T0 + 61_000) == 1 and bot.window_sample(T0 + 62_000) == 0
    row = store.get(f"wsample:{spec.slug}:840")
    assert {k: row[k] for k in ("left", "kind", "card", "key", "start", "end", "mkt", "bid", "ask", "line", "price", "hold")} == {
        "left": 840, "kind": "window", "card": "宇树 15分涨跌", "key": "UNITREE-15M", "start": T0, "end": T1, "mkt": 0.425, "bid": 0.4, "ask": 0.45,
        "line": 90.0, "price": 90.12, "hold": ""}, row
    assert 0.5 < row["up"] < 1 and row["sigma"] == card.sigma and abs(row["tau"] - (T1 - T0 - 61_000) / m.YEAR_MS) < 1e-15 and row["need"] >= 0.02
    assert store.keys("wsample:") == [f"wsample:{spec.slug}:840"]
    # a sample due more than a minute ago (a restart) is not taken; the next moments are
    assert bot.window_sample(T0 + 5 * 60_000 + 70_000) == 0 and store.get(f"wsample:{spec.slug}:600") is None
    fm.premium = ("90.2500", T0 + 10 * 60_000 - 500)  # the index kept fresh (a stale one leaves a sample without a fair price)
    card.times["price"] = -1e9
    await card.refresh(T0 + 10 * 60_000)
    assert bot.window_sample(T0 + 10 * 60_000 + 1_000) == 1 and store.get(f"wsample:{spec.slug}:300")["left"] == 300
    fm.premium = ("90.3000", T0 + 13 * 60_000 - 500)
    card.times["price"] = -1e9
    await card.refresh(T0 + 13 * 60_000)
    assert bot.window_sample(T0 + 13 * 60_000 + 500) == 1 and store.get(f"wsample:{spec.slug}:120")["price"] == 90.3
    report = "\n".join(bot.window_report())
    assert "宇树 15分涨跌：1 个市场、3 条样本，已有结果 0 个市场 / 0 条" in report and "剩 14 分" not in report, report
    # the window ends: its samples get the result at the roll — 90.20 over 90.00, 涨
    fm.index[T1 - 60_000] = "90.2000"
    fm.now = bot.market.now = T1 + 2_000
    card.times["candle"] = -1e9
    card.strike = D("90.1900")  # 1.122.0: Predict's start price of the window just over — within 0.1% of the next window's candle (90.20)
    await bot.refresh_updown(T1 + 2_000)
    assert card.spec.start_ms == T1 - 60_000 and card.strike is None and card.times["strike"] == -1e9, "the roll drops the old window's start price"
    assert card.line_used() == D("90.2000"), "the new window prices on its own candle, not on the old window's start price"
    rows = [r for r in bot.window_rows() if r["slug"] == spec.slug]
    assert [r["result"] for r in rows] == [1.0, 1.0, 1.0] and all(abs(r["move"] - math.log(90.2 / 90.0)) < 1e-12 and r["final"] == 90.2 for r in rows)
    report = "\n".join(bot.window_report())
    first = store.get(f"wsample:{spec.slug}:840")
    assert "宇树 15分涨跌：1 个市场、3 条样本，已有结果 1 个市场 / 3 条" in report, report
    assert f"剩 14 分：1 条｜模型 Brier {(1 - first['up']) ** 2:.3f}，盘口中间价 0.331（1 条有盘口，模型更准的占 100%），抛硬币 0.250" in report, report
    assert "剩 2 分：1 条" in report and "校准：模型 80–100¢ 的" in report and "波动：1 个市场，实际 / 预测（RMS z）" in report, report
    # 1.106.0: σ calibration from the windows' actual moves (SIGMA_CALIB): each market's earliest sample made with the current
    # estimator, z = move / (raw σ√τ), the RMS pooled with 10 windows at 1 (1.121.0; 20 before) — ten windows that moved half
    # of σ√τ: ×0.78 alone; a sample of an earlier estimator (2: the weighted four hours; 1) does not count; the factor is what
    # the card prices with, and the note and /calib say so
    assert bot.config.sigma_calib and m.CONTROL_FORMS["SIGMA_CALIB"]["kind"] == "choice" and bot.control_value("SIGMA_CALIB") == "on"
    assert first["sigma_raw"] == card.sigma and first["calib"] == 1.0 and first["model"] == 3 and first["shape"] == 1.0 and m.IndexUpDownMarket.SIGMA_MODEL == 3 and m.UpDownMarket.SIGMA_MODEL == 2
    st = 0.5 * math.sqrt(2.7e-5)
    for i in range(10):
        base = {"t": T0 - (i + 2) * 900_000, "start": T0 - (i + 2) * 900_000, "end": T0 - (i + 1) * 900_000, "slug": f"c{i}-updown-15m", "key": "UNITREE-15M",
                "kind": "window", "card": "宇树 15分涨跌", "up": 0.6,
                "sigma": 0.5, "sigma_raw": 0.5, "calib": 1.0, "model": m.IndexUpDownMarket.SIGMA_MODEL, "tau": 2.7e-5, "line": 100.0, "result": 1.0, "final": 100.0 * math.exp(0.5 * st)}
        store.put(f"wsample:c{i}-updown-15m:840", {**base, "left": 840, "move": 0.5 * st})
        store.put(f"wsample:c{i}-updown-15m:120", {**base, "left": 120, "move": 3 * st})  # a later sample of the same market: not the one counted
    store.put("wsample:old-updown-15m:840", {"t": T0 - 20 * 900_000, "start": T0 - 20 * 900_000, "end": T0 - 19 * 900_000, "slug": "old-updown-15m", "key": "UNITREE-15M", "kind": "window", "card": "宇树 15分涨跌",
                                             "left": 840, "up": 0.6, "sigma": 0.5, "sigma_raw": 0.5, "model": 2, "tau": 2.7e-5, "line": 100.0, "move": 3 * st, "result": 1.0, "final": 101.0})
    bot.sigma_calib_cache.clear()
    real = store.get(f"wsample:{spec.slug}:840")  # the window sampled above has its result too: the eleventh
    z_real = math.log(real["final"] / real["price"]) / (real["sigma_raw"] * math.sqrt(real["tau"]))  # 1.122.0: the end over the price at the sample
    assert abs(m.sample_z(real, raw=True) - z_real) < 1e-12 and m.sample_z({"sigma": 0.5, "tau": 2.7e-5, "move": 0.001}) == 0.001 / (0.5 * math.sqrt(2.7e-5)) \
        and m.sample_z({"sigma": 0.0, "tau": 1.0, "move": 0.1}) is None and m.sample_z({"sigma": 0.5, "tau": 1.0}) is None
    factor, n = bot.sigma_calib("UNITREE-15M")
    assert n == 11 and abs(factor - math.sqrt((10 * 0.25 + z_real ** 2 + 10) / 21)) < 1e-9 and bot.sigma_calib("UNITREE-15M") == (factor, 11), (factor, n, z_real)
    assert (bot.SIGMA_CALIB_PRIOR, bot.SIGMA_CALIB_RANGE) == (10, (0.5, 2.0))
    assert bot.sigma_calib("SKHYNIX-15M") == (1.0, 0)
    card.calib = (factor, n)
    assert abs(card.sigma_used - card.sigma * factor) < 1e-12
    assert f"σ 校准系数：×{factor:.2f}（按 11 个窗口" in "\n".join(bot.window_report()) and "卡片按它定价" in "\n".join(bot.window_report())
    card.calib = (1.0, 0)
    await bot.refresh_updown(fm.now)  # the refresh applies the measured factor…
    assert card.calib == (factor, 11), card.calib
    bot.config = dataclasses_replace(bot.config, sigma_calib=False)  # …unless the setting is off
    await bot.refresh_updown(fm.now)
    assert card.calib == (1.0, 0) and "SIGMA_CALIB=off，卡片不用它" in "\n".join(bot.window_report())
    bot.config = dataclasses_replace(bot.config, sigma_calib=True)
    extreme = m.Bot(m.Config.from_env(ENV), store, FM(T0), None)  # the range: twenty windows at 3σ would say ×2.5, capped at 2.0 (1.4 before 1.121.0)
    for i in range(20):
        store.put(f"wsample:e{i}-updown-15m:840", {"t": T0 - (i + 30) * 900_000, "start": T0 - (i + 30) * 900_000, "end": T0 - (i + 29) * 900_000, "slug": f"e{i}-updown-15m",
                                                   "key": "SKHYNIX-15M", "kind": "window", "card": "SK 海力士 15分涨跌",
                                                   "left": 840, "up": 0.6, "sigma": 0.5, "sigma_raw": 0.5, "calib": 1.0, "model": m.IndexUpDownMarket.SIGMA_MODEL, "tau": 2.7e-5, "line": 100.0, "move": 3 * st,
                                                   "result": 1.0, "final": 100.0 * math.exp(3 * st)})
    assert extreme.sigma_calib("SKHYNIX-15M") == (2.0, 20)
    # the daily crypto cards (1.106.1): a window a day each, pooled under one factor; a card's own key counts its 15-minute windows alone
    for i, key in enumerate(("BTC-DAY", "ETH-DAY", "BNB-DAY", "BTC-DAY", "ETH-DAY")):
        st_day = 0.5 * math.sqrt(1 / 730)
        store.put(f"wsample:d{i}-updown-day:43200", {"t": T0 - (i + 50) * 86_400_000, "start": T0 - (i + 50) * 86_400_000, "end": T0 - (i + 49) * 86_400_000,
                                                     "slug": f"d{i}-updown-day", "key": key, "kind": "day", "card": key[:3] + " 日涨跌", "left": 43200, "up": 0.6,
                                                     "sigma": 0.5, "sigma_raw": 0.5, "calib": 1.0, "model": 2, "tau": 1 / 730, "line": 100.0, "move": 2 * st_day,
                                                     "result": 1.0, "final": 100.0 * math.exp(2 * st_day)})
    pooled = m.Bot(m.Config.from_env(ENV), store, FM(T0), None)
    assert abs(pooled.sigma_calib(m.CRYPTO_DAY_CALIB)[0] - math.sqrt((5 * 4 + 10) / 15)) < 1e-9 and pooled.sigma_calib(m.CRYPTO_DAY_CALIB)[1] == 5
    assert pooled.sigma_calib("BTC-DAY") == (1.0, 0) and "σ 校准系数（BTC / ETH / BNB 日涨跌合并）：×1.41（按 5 个市场" in "\n".join(pooled.window_report())
    store.delete_keys([k for k in store.keys("wsample:") if k.startswith(("wsample:c", "wsample:e", "wsample:old", "wsample:d"))])  # the made-up windows go
    bot.sigma_calib_cache.clear()
    # a window that ended while the bot was down is settled on its kept end when the samples are read; an end within the tie
    # band keeps no result (Predict's call), the move all the same
    for slug, close in (("x-updown-15m-1", "101"), ("y-updown-15m-2", "100.02")):
        store.put(f"wsample:{slug}:840", {"t": T0 - 3_600_000, "slug": slug, "key": "X", "item": slug, "kind": "window", "card": "测试 15分涨跌",
                                          "start": T0 - 3_600_000, "end": T0 - 2_700_000, "left": 840, "up": 0.6, "sigma": 0.5, "tau": 2.7e-5, "line": 100.0})
        store.put(f"updown:{slug}:end", {"open": T0 - 2_760_000, "close": close})
    by = {r["slug"]: r for r in bot.window_rows()}
    assert by["x-updown-15m-1"]["result"] == 1.0 and by["x-updown-15m-1"]["final"] == 101.0
    assert by["y-updown-15m-2"]["result"] is None and abs(by["y-updown-15m-2"]["move"] - math.log(1.0002)) < 1e-12
    assert "测试 15分涨跌：2 个市场、2 条样本，已有结果 1 个市场 / 1 条（另 1 个市场终点贴着起点，结果以 Predict 为准、不评分）" in "\n".join(bot.window_report())
    # 1.115.1: a sample is judged with its own card's tie band — the BTC spot window's is 0.01%, so an end 0.02% up is 涨 there
    # (a tie on an index window); one judged a tie with the index band before gets its result when the samples are read
    assert bot.updowns["BTC-15M"].TIE == D("0.0001") and bot.window_tie({"key": "BTC-15M"}) == 0.0001 and bot.window_tie({"key": "X"}) == 0.0005
    store.put("wsample:btc-updown-15m-9:840", {"t": T0 - 3_600_000, "slug": "btc-updown-15m-9", "key": "BTC-15M", "item": "b9", "kind": "window", "card": "现货测试 15分涨跌",
                                               "start": T0 - 3_600_000, "end": T0 - 2_700_000, "left": 840, "up": 0.6, "sigma": 0.5, "tau": 2.7e-5, "line": 100.0})
    store.put("updown:btc-updown-15m-9:end", {"open": T0 - 2_760_000, "close": "100.02"})
    store.put("wsample:btc-updown-15m-8:840", {"t": T0 - 7_200_000, "slug": "btc-updown-15m-8", "key": "BTC-15M", "item": "b8", "kind": "window", "card": "现货测试 15分涨跌",
                                               "start": T0 - 7_200_000, "end": T0 - 6_300_000, "left": 840, "up": 0.4, "sigma": 0.5, "tau": 2.7e-5, "line": 100.0,
                                               "final": 99.97, "move": math.log(0.9997), "result": None})
    by = {r["slug"]: r for r in bot.window_rows()}
    assert by["btc-updown-15m-9"]["result"] == 1.0 and by["btc-updown-15m-8"]["result"] == 0.0 and store.get("wsample:btc-updown-15m-8:840")["result"] == 0.0
    assert by["y-updown-15m-2"]["result"] is None  # the index window's tie stays Predict's call
    assert "现货测试 15分涨跌：2 个市场、2 条样本，已有结果 2 个市场 / 2 条\n" in "\n".join(bot.window_report()) + "\n"
    store.delete_keys(["wsample:btc-updown-15m-9:840", "wsample:btc-updown-15m-8:840", "updown:btc-updown-15m-9:end"])
    # the CSV: a row a sample, the result columns filled where there is one
    text = bot.window_csv()
    assert text.startswith("\ufeff")
    table = list(csv.reader(io.StringIO(text.lstrip("\ufeff"))))
    assert table[0] == list(m.Bot.WSAMPLE_COLUMNS) and len(table) == 6
    line = next(r for r in table[1:] if r[3] == spec.slug and r[6] == "840")
    assert line[:6] == [m.dt.datetime.fromtimestamp((T0 + 61_000) / 1000, m.BEIJING).strftime("%Y-%m-%d %H:%M:%S"), "宇树 15分涨跌", "window", spec.slug,
                        "2026-11-04 11:00:00", "2026-11-04 11:15:00"] and line[8:11] == ["0.425", "0.4", "0.45"] and line[21:23] == ["1", "90.2"]
    assert float(line[7]) == float(f"{first['up']:.6g}") and float(line[23]) == float(f"{math.log(90.2 / 90.0):.6g}") and line[24] and line[19] == ""
    # /samples sends it as a document; the review page serves it
    await bot.process_message({"text": "/samples", "chat": {"id": 1}, "from": {"id": 42}, "date": time.time()})
    assert len(tg.docs) == 1 and tg.docs[0][0] == 1 and tg.docs[0][2].filename == "model-samples-20261104.csv" and tg.docs[0][2].data == text.encode("utf-8")
    assert tg.docs[0][2].caption == "模型样本 5 条（已有结果 4 条），3 个市场；列的含义见 README" and tg.docs[0][2].mime == "text/csv", tg.docs[0][2].caption
    empty = m.Bot(m.Config.from_env({**ENV, "ADMIN_USER_ID": "42"}), m.Store(":memory:"), FM(T0), FakeTg())
    assert (await empty.cmd_samples(m.Request("/samples", [], 1, 0, 42))).startswith("还没有样本")
    web = m.WebServer(bot, 0, "t" * 20)
    port = await web.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET /p/{'t' * 20}/samples.csv HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".encode())
    await writer.drain()
    raw = await reader.read()
    writer.close()
    await web.stop()
    assert raw.startswith(b"HTTP/1.1 200") and b"text/csv" in raw and raw.endswith(text.encode("utf-8")), raw[:200]
    # old samples go after 60 days
    store.put("wsample:z-updown-15m-3:840", {"t": T0 - 61 * m.DAY_MS, "slug": "z-updown-15m-3", "kind": "window", "left": 840})
    assert bot.window_prune(T0) == 1 and store.get("wsample:z-updown-15m-3:840") is None and bot.window_prune(T0) == 0
    assert "/samples" in bot.cmd_help(None) and any(c.name == "samples" for c in m.COMMANDS)
    print("SAMPLES_OK")


asyncio.run(samples())
