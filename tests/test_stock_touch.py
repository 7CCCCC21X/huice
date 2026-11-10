"""1.146.0: NVIDIA (NVDA) 先触 $160 / $210 — a US stock's "which price first" market. Pyth's 1-minute candles of the regular
session settle it (the first Low ≤ $160 or High ≥ $210 between the market's creation and 2027-04-30 23:59 ET; pre-market and
after-hours prices do not count; neither: 50/50). The card reads Yahoo (as the hit markets do), prices in trading time, checks
the path by daily bars and today's minutes, and advises only while the regular session runs.

Run: python tests/test_stock_touch.py
"""
import asyncio
import datetime as dt
import json
import sys
import urllib.parse

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

D = m.D
ET = lambda mo, d, h, mi, y=2026: m.et_wall_ms(dt.date(y, mo, d), h, mi)
NOW = ET(11, 10, 11, 0)          # Tuesday 11:00 ET (16:00 UTC): the session running
START = ET(11, 2, 10, 0)         # the market's creation (Predict's createdAt): Monday 11-02 10:00 ET
SPEC = next(s for s in m.TOUCH_MARKETS if s.key == "NVDA-2027")


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now
    async def prices(self): return {}
    async def get(self, path, **params): return {}


def chart(bars, price=None, when=None):
    """A Yahoo v8 chart body from (open ms, open, high, low, close) bars."""
    meta = {"currency": "USD", "symbol": "NVDA", "gmtoffset": -18000, "regularMarketPrice": price, "regularMarketTime": when}
    col = lambda i: [b[i] for b in bars]
    return json.dumps({"chart": {"result": [{"meta": meta, "timestamp": [b[0] // 1000 for b in bars],
                                             "indicators": {"quote": [{"open": col(1), "high": col(2), "low": col(3), "close": col(4)}]}}],
                                 "error": None}}).encode()


class Yahoo:
    """The chart feed: the daily bars of the last year (closes 185 and 186.85 in turn: ±1% a day), today's minutes from 09:30."""
    def __init__(self, now):
        self.now, self.day_highs, self.day_lows, self.minute_lows = now, {}, {}, {}
        self.asked = []

    def daily(self):
        out, day, i = [], m.et_date(self.now) - dt.timedelta(days=365), 0
        while day <= m.et_date(self.now):
            if m.us_session(day):
                close = 185.0 * (1.01 if i % 2 else 1.0)
                out.append((m.us_session(day)[0], close * 0.999, self.day_highs.get(day, close * 1.005), self.day_lows.get(day, close * 0.995), close))
                i += 1
            day += dt.timedelta(days=1)
        return out

    def minutes(self):
        session = m.us_session(m.et_date(self.now))
        if not session:
            return []
        out, o = [], session[0]
        while o + 60_000 <= min(self.now, session[1]) + 60_000 and o < session[1] and o <= self.now:
            out.append((o, 185.0, 185.5, self.minute_lows.get(o, 184.5), 185.2))
            o += 60_000
        return out

    async def fetch(self, url, extra=None, record=True):
        q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        assert "/NVDA?" in url and q.get("range") in ("1d", "1y"), url
        self.asked.append(q["interval"])
        if q["interval"] == "1d":
            return chart(self.daily())
        return chart(self.minutes(), price=185.2, when=self.now // 1000 - 20)


async def run():
    # --- the spec: Predict's slug, the two lines, the deadline (EDT), a US stock's market --------------------------------------
    assert (SPEC.slug, SPEC.symbol, SPEC.low, SPEC.high, SPEC.created_ms, SPEC.stock) == ("will-nvidia-nvda-stock-hit-160-or-210-first", "NVDA", D("160"), D("210"), 0, True)
    assert SPEC.deadline_ms == m.et_ms(2027, 4, 30, 23, 59, -4) and m.touch_spec("NVDA-2027") is SPEC
    assert "04-30 23:59 ET" in SPEC.close_label() and m.touch_outcome("$160", SPEC) == "low" and m.touch_outcome("$210", SPEC) == "high"
    assert m.market_driver("touch", "NVDA-2027") == ("NVDA", "NVDA")

    # --- the bot: a StockTouchMarket, nothing of it on Binance's streams ----------------------------------------------------------
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT"})
    bot = m.Bot(cfg, m.Store(":memory:"), FM(NOW), None)
    card = bot.touches["NVDA-2027"]
    assert isinstance(card, m.StockTouchMarket) and type(bot.touches["BNB-2027"]) is m.TouchMarket and card.GROUP == "contract"
    assert "NVDA" not in bot.binance_streams["spot"].symbols() and ("spot", "NVDA") not in bot.pair_markets
    assert bot.predict_targets(NOW)["NVDA-2027"] == SPEC.slug
    assert "美股先触·NVDA" in [p[0] for p in bot.diag_probes(NOW)]

    # --- the session running, nothing reached since the creation: priced in trading time, advised ---------------------------------
    yahoo = Yahoo(NOW)
    m.fetch_source = yahoo.fetch
    card.start_ms = START
    await card.refresh(NOW)
    assert card.error == "" and card.price == D("185.2000") and card.fetched_ms == NOW and sorted(yahoo.asked) == ["1d", "1m"], card.error
    assert card.history == {"kind": "clear", "through": ET(11, 10, 0, 0), "start": START}, card.history
    sigma = card.sigma
    assert 0.14 < sigma < 0.18, sigma  # ±1% a day over 252 sessions: 15.8%
    years = m.us_trading_years(NOW, SPEC.deadline_ms + 60_000, card.share[0] if card.share else 1.0)
    assert card.remaining_years(NOW) == years and 0.45 < years < 0.48, years  # some 117 sessions to 04-30
    odds = card.odds(NOW)
    want = m.first_touch(185.2, 160.0, 210.0, sigma, 0.0, years)
    assert isinstance(odds, m.TouchOdds) and abs(odds.fair_upper - want.fair_upper) < 1e-12 and 0.3 < odds.fair_upper < 0.7, odds
    assert card.advice_problem(NOW) == "" and card.status() == "开盘以来未触线（核至 11-10 13:00）", card.status()
    assert card.left_text(NOW).endswith(" 个交易日（171 天）") and 115 < float(card.left_text(NOW).split(" ")[0]) < 118, card.left_text(NOW)  # 11:00 ET: today's rest a part
    assert card.model_swing(NOW) > 0
    # the card: in 合约标的, Yahoo standing in for Pyth, σ over sessions, the time left in sessions
    pay = bot.touch_payload(card, NOW)
    assert pay["group"] == "contract" and pay["name"] == "NVDA 先触 160/210" and pay["source"] == "Yahoo 行情（NASDAQ:NVDA，代 Pyth）", pay
    t = pay["touch"]
    assert t["sigma_note"] == "60 个交易日收盘、按 252 个交易日年化" and "个交易日" in t["left"] and t["feed"] == "Yahoo" and "常规交易时段" in t["model"], t
    ev = bot.market_evidence("touch", NOW, touch=card)
    assert abs(ev["basis"]["years"] - years) < 1e-12 and "Yahoo 行情（NASDAQ:NVDA，代 Pyth）" in json.dumps(ev, ensure_ascii=False) and "股价" in json.dumps(ev, ensure_ascii=False)
    # the page: the card's own σ line and feed
    for want_js in ('(t.sigma_note||"30 日小时收盘年化")', '(t.left||(t.years*365).toFixed(1)+" 天")', '(t.feed||"币安")+"刷新失败："', 't.model||"双边界首达'):
        assert want_js in m.WEB_PAGE, want_js

    # --- 1.147.0: Binance's futures index (NVDAUSDT, the 15-minute card's) is the live price in the session, Yahoo the check ---------
    assert card.yahoo == (D("185.2000"), NOW - 20_000) and not card.index_live
    assert card.note_index(D("185.6"), NOW - 500, NOW) and card.price == D("185.6") and card.index_live and card.priced_ms == NOW - 500
    assert card.SOURCE == "币安合约 NVDAUSDT 指数价（交易时段每秒更新，Yahoo 核对）" and bot.touch_payload(card, NOW)["source"] == card.SOURCE
    assert not card.note_index(D("200"), NOW, NOW) and card.price == D("185.6")            # 8% off Yahoo's trade: another instrument
    assert not card.note_index(D("185.7"), NOW - 16_000, NOW)                               # older than 15 s
    assert not card.note_index(D("185.7"), NOW - 1_000, NOW)                                # older than the one in hand
    assert not card.note_index(D("185.7"), ET(11, 10, 17, 0), ET(11, 10, 17, 0))            # after the close: no
    card.times["price"] = -1e9
    await card.refresh(NOW + 1_000)  # Yahoo's read in between: the live index stands, Yahoo kept as the check
    assert card.price == D("185.6") and card.index_live and card.yahoo[0] == D("185.2000")
    bot.price_dirty.clear()
    bot.price_pushed("futures", "NVDAUSDT", {"symbol": "NVDAUSDT", "indexPrice": "185.90", "markTime": NOW + 2_000, "markPrice": "185.95"})
    assert card.price == D("185.90") and "NVDA-2027" in bot.price_dirty  # the push: the card moves, the express lane looks
    nvda15 = bot.updowns["NVDA-15M"]
    nvda15.index_ms, nvda15.price = NOW + 3_000, D("186.0")
    await bot.refresh_touch(NOW + 3_500)  # without the push: the index the 15-minute card read, each touch round
    assert card.price == D("186.0") and card.index_live
    odds = card.odds(NOW + 3_500)
    assert isinstance(odds, m.TouchOdds) and abs(odds.fair_upper - m.first_touch(186.0, 160.0, 210.0, card.sigma, 0.0, card.remaining_years(NOW + 3_500)).fair_upper) < 1e-12
    card.times["price"] = -1e9
    await card.refresh(NOW + 30_000)  # the index gone quiet for over 15 s: Yahoo's trade again
    assert card.price == D("185.2000") and not card.index_live and card.SOURCE.startswith("Yahoo")

    # --- after the close: the last trade stands, the odds too, but no advice until the next open ------------------------------------
    evening = ET(11, 10, 17, 0)
    card.fetched_ms = evening  # read two minutes ago, as between sessions it is (the trade itself the close's)
    assert isinstance(card.odds(evening), m.TouchOdds)
    assert card.advice_problem(evening).startswith("美股休市中：盘前盘后的价格不算数") and "11-11 22:30 开盘后恢复" in card.advice_problem(evening), card.advice_problem(evening)
    card.fetched_ms = NOW
    # in the session, today's minutes unread for over five minutes: no advice
    card.minutes_ms = NOW - 6 * 60_000
    assert card.advice_problem(NOW) == "今天的 1 分钟 K 超过 5 分钟没读到；暂不给建议"
    card.minutes_ms = NOW

    # --- a minute today reaches $160: decided at once ----------------------------------------------------------------------------
    yahoo.minute_lows[ET(11, 10, 10, 30)] = 159.9
    card.times["price"] = -1e9
    await card.refresh(NOW)
    assert card.history["kind"] == "low" and card.history["time"] == ET(11, 10, 10, 30) and card.odds(NOW) == m.TouchOdds(1.0, 0.0, 0.0)
    assert card.status().startswith("已于 ") and card.status().endswith("先触及 $160") and card.advice_problem(NOW) == ""

    # --- a finished session reached $210 (its daily High): the first such session decides -----------------------------------------
    store = m.Store(":memory:")
    high = m.StockTouchMarket(store, SPEC)
    high.start_ms = START
    yahoo2 = Yahoo(NOW)
    yahoo2.day_highs[dt.date(2026, 11, 5)] = 211.0
    yahoo2.day_lows[dt.date(2026, 11, 6)] = 159.0  # later: does not count
    m.fetch_source = yahoo2.fetch
    await high.refresh(NOW)
    assert high.history["kind"] == "high" and high.history["daily"] and m.et_date(high.history["time"]) == dt.date(2026, 11, 5), high.history
    assert high.status().startswith("已于 11-05（美东）先触及 $210（日 K：高 211") and high.odds(NOW) == m.TouchOdds(0.0, 1.0, 0.0)

    # --- the creation's own day reached a line: its daily bar holds minutes before the creation too — a manual check --------------
    amb = m.StockTouchMarket(m.Store(":memory:"), SPEC)
    amb.start_ms = START
    yahoo3 = Yahoo(NOW)
    yahoo3.day_lows[dt.date(2026, 11, 2)] = 159.5
    m.fetch_source = yahoo3.fetch
    await amb.refresh(NOW)
    assert amb.history["kind"] == "ambiguous" and amb.status().startswith("需人工核对：11-02（美东）这个交易日碰到了线")
    assert amb.advice_problem(NOW).startswith("创建当天或同一个交易日碰到了线（日 K 分不出先后），需人工到 Pyth 核对")

    # --- the creation unknown (Predict not read yet): priced, not advised; a day's bars unread: not advised ----------------------------
    fresh = m.StockTouchMarket(m.Store(":memory:"), SPEC)
    m.fetch_source = Yahoo(NOW).fetch
    await fresh.refresh(NOW)
    assert isinstance(fresh.odds(NOW), m.TouchOdds) and fresh.advice_problem(NOW) == "开盘时间未知，无法核验此前是否已触线；暂不给建议"
    fresh.start_ms = START
    fresh.save({"kind": "clear", "through": ET(11, 9, 0, 0)})
    assert fresh.advice_problem(NOW).startswith("触线核验停在 ") and "日 K 未读到" in fresh.advice_problem(NOW)
    # Yahoo silent: the odds wait
    broken = m.StockTouchMarket(m.Store(":memory:"), SPEC)

    async def down(url, extra=None, record=True):
        raise m.RemoteError("HTTP 502: Bad Gateway")
    m.fetch_source = down
    await broken.refresh(NOW)
    assert broken.odds(NOW).startswith("等待 Yahoo 行情（价格：") and "日 K：" in broken.error, broken.error
    print("STOCK_TOUCH_OK")


asyncio.run(run())
