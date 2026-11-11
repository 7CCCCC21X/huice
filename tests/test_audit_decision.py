"""Audit regressions for the decision cluster (paper trader's decision layer and its review figures)."""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import offline  # noqa: F401
import main as m
import preopen as p

NOW = int(dt.datetime(2026, 11, 10, 12, 0, tzinfo=m.BEIJING).timestamp() * 1000)


# --- SIM-EV-SIGMA: a pre-open window filled on both sides is one bet whose variance is that of the unhedged difference ---------
def preopen_row(i: int, up: float, down: float, result: str) -> dict:
    return {"key": "BTC-15M", "slug": f"btc-updown-15m-{i}", "name": "BTC 15分涨跌 10:00–10:15", "price": "0.48", "shares": "20",
            "start_ms": NOW - (i + 1) * 900_000, "phase": "closed", "accounted": True, "result": result, "attempt": 0,
            "orders": {"up": {"state": "final", "filled": up}, "down": {"state": "final", "filled": down}}}


# 40 one-sided windows at 48¢ × 20 that all lost (the price ran away from the filled side), then 200 windows filled 20 + 20:
# +0.80 each whatever the result — EV = P&L, no risk at all
rows = [preopen_row(i, 20.0, 0.0, "down") for i in range(40)] + [preopen_row(100 + i, 20.0, 20.0, "up" if i % 2 else "down") for i in range(200)]
bets = p.ev_bets(rows)
assert len(bets) == 240, ("one bet per window, not per filled side", len(bets))
pairs = [b for b in bets if b["shares"] == 40.0]
assert len(pairs) == 200 and all(b["var"] == 0.0 for b in pairs), "a window filled alike on both sides carries no variance"
assert all(abs(b["ev"] - 0.8) < 1e-9 and abs(b["pnl"] - 0.8) < 1e-9 for b in pairs), [(b["ev"], b["pnl"]) for b in pairs[:2]]
assert all(b["var"] == 100.0 and abs(b["pnl"] + 9.6) < 1e-9 for b in bets if b["shares"] == 20.0), "one-sided: filled² / 4, as before"
ev = m.sim_ev_timeline(bets, NOW)
g = next(x for x in ev["strategies"] if x["name"] == "预挂")
assert g["bets"] == 240 and g["verdict"] == "激进" and g["z"] <= -m.EV_FLAG_Z, (g["bets"], g["z"], g["verdict"])
assert ev["recent"]["flag"], "the hourly check sees the overclaim the hedged windows used to hide"
# an uneven both-sides fill: only the difference is at risk — 20 up, 5 down at 48¢, up won: 20·0.52 − 5·0.48 = 8.0, var 15² / 4
(b,) = p.ev_bets([preopen_row(7, 20.0, 5.0, "up")])
assert (b["shares"], round(b["ev"], 9), round(b["pnl"], 9), b["var"]) == (25.0, 0.5, 8.0, 56.25), b

# --- SIM-MARKET-DRIVER-WINDOWS: a 15/5-minute window card is read from its Binance pair, so it counts in the pair's risk group -----
win = {"end": 1, "start": 0, "window": "15m"}
assert m.market_driver("updown", "BTC-15M", win, "BTC 15分涨跌 16:30–16:45") == ("BTCUSDT", "BTC")
assert m.market_driver("updown", "ETH-15M", win, "ETH 15分涨跌 16:30–16:45") == ("ETHUSDT", "ETH")
assert m.market_driver("updown", "BTC-5M", {"end": 1, "start": 0, "window": "5m"}, "BTC 5分涨跌 16:30–16:35") == ("BTCUSDT", "BTC")
assert m.market_driver("updown", "UNITREE-15M", win, "宇树 15分涨跌 11:00–11:15")[0] == "UNITREEUSDT"
assert not {s.key for s in m.WINDOW_UPDOWN_MARKETS} & {s.key for s in (*m.UPDOWN_MARKETS, *m.DAILY_UPDOWN_MARKETS)}


def window_rec(key: str, settle: dict, item: str) -> dict:
    return {"kind": "updown", "key": key, "settle": settle, "item": item, "side": "up", "price": 0.5, "shares": 100,
            "status": "filled", "market": key}


groups = m.sim_groups([window_rec("BTC-DAY", {"end": 5, "day": "2026-11-10"}, "BTC 11-10 涨跌"),
                       window_rec("BTC-15M", win, "BTC 15分涨跌 16:30–16:45")])
assert [(g["driver"], g["worst"]) for g in groups] == [("BTCUSDT", -100.0)], [(g["driver"], g["name"], g["worst"]) for g in groups]

# ... and inside the pair's group every window stays an event of its own. BTC 15分 16:30–16:45 and BTC 5分 16:40–16:45 end on the
# same candle: 涨 on the first and 跌 on the second both lose when BTC falls to 16:40 and then rises a little, still under its
# 16:30 price. Keyed on the end alone they were hedged as one event (worst $0), which made the merged group's cap looser than
# the two separate groups had been (-50 each).
import types

card15, card5 = (next(s for s in m.WINDOW_UPDOWN_MARKETS if s.key == k) for k in ("BTC-15M", "BTC-5M"))
W0 = 1793867400_000  # 11-05 16:30 Beijing
w15, w5, w5_first = card15.window_spec(W0), card5.window_spec(W0 + 10 * 60_000), card5.window_spec(W0)
assert w15.end_ms == w5.end_ms and w15.start_ms != w5.start_ms and w5_first.start_ms == w15.start_ms


def held(card, spec, side: str, price: float = 0.5, **more) -> dict:
    return {"kind": "updown", "key": card.key, "item": spec.name, "side": side, "price": price, "shares": 100, "status": "filled",
            "market": spec.slug, "settle": {"end": spec.end_ms, "start": spec.start_ms, **card.tag(spec)}, **more}


groups = m.sim_groups([held(card15, w15, "up"), held(card5, w5, "down")])
assert [(g["driver"], g["worst"], g["markets"]) for g in groups] == [("BTCUSDT", -100.0, 2)], [(g["driver"], g["worst"], g["event"]) for g in groups]
assert m.event_of(held(card15, w15, "up")) != m.event_of(held(card5, w5, "down"))
assert m.sim_groups([held(card15, w15, "up"), held(card5, w5_first, "down")])[0]["worst"] == -100.0  # the same start, another end
# the two sides of ONE window still hedge each other: 100 涨 + 100 跌 at 60¢ lose 20 whichever way it ends
assert m.event_of(held(card15, w15, "up")) == m.event_of(held(card15, w15, "down"))
assert abs(m.sim_groups([held(card15, w15, "up", 0.6), held(card15, w15, "down", 0.6)])[0]["worst"] + 20.0) < 1e-9
# a window that ends with a monthly card's last candle (its record carries only the end) is another event than the month
month = {"kind": "updown", "key": "BTC-2026-11", "item": "BTC 11月涨跌", "side": "down", "price": 0.5, "shares": 100, "status": "filled",
         "market": "btc-nov", "settle": {"end": w15.end_ms}, "driver": "BTCUSDT", "driver_name": "BTC"}
assert m.group_worst_case([held(card15, w15, "up"), month], "BTC")[0] == -100.0
assert m.event_of(month) == ("updown", "", str(w15.end_ms)), "a record without a start: keyed as before"
# the cap itself (sim_group_room, the paper and the real trader's): with 100 涨 held on the 15-minute window, 100 跌 on the
# 5-minute window ending with it is refused under a $60 cap (was let through: the pair read as hedged, worst $0)
room_bot = types.SimpleNamespace(config=types.SimpleNamespace(sim_group_usd=60.0), sim_resting_cost=lambda t: float(t.get("price") or 0),
                                 sim_group_order=lambda mk, price, shares, taker=None: (price, shares))
book15 = {"a": held(card15, w15, "up")}
sim_market = lambda card, spec: m.SimMarket(spec.slug, spec.name, "updown", card.key, 0.5, None, 0.0, "", ("涨", "跌"),
                                            {"end": spec.end_ms, "start": spec.start_ms, **card.tag(spec)})
refusal = m.Bot.sim_group_room(room_bot, book15, sim_market(card5, w5), "down", 0.5, 100)
assert "合计将亏 $100.00，超过组上限 $60" in refusal, refusal
assert m.Bot.sim_group_room(room_bot, book15, sim_market(card15, w15), "down", 0.5, 100) == ""  # the other side of the held window: a hedge

# --- SIM-OWN-ON-BOOK-CLOCK: own_on_book compares a book's read with Predict's answer on one clock (the books' wall clock) ---------
# Predict answered at local wall time L; answered_at is on the Binance-offset clock (here the local clock lags Binance by 2 s,
# so answered_at = L + 2000). A book read 500 ms after the answer already shows our order: it is ours, not someone else's 买1.
# A book read 500 ms before the answer cannot show it.
L = NOW
for offset in (2000, -2000):
    rec = {"order": 10.0, "shares": 0.0, "live": {"state": "open", "answered_ms": L, "answered_at": L + offset}}
    after = m.PredictBook("HSI", "hsi-slug", "101", "t", [], [], L + 500, None)
    before = m.PredictBook("HSI", "hsi-slug", "101", "t", [], [], L - 500, None)
    assert m.own_on_book(rec, after) == 10.0, ("a book read after the answer shows our order", offset, m.own_on_book(rec, after))
    assert m.own_on_book(rec, before) == 0.0, ("a book read before the answer cannot show it", offset, m.own_on_book(rec, before))
# a record saved before the fix (answered_at only) keeps the old comparison
old = {"order": 10.0, "shares": 4.0, "live": {"state": "open", "answered_at": L}}
assert m.own_on_book(old, m.PredictBook("HSI", "hsi-slug", "101", "t", [], [], L + 1, None)) == 6.0
assert m.own_on_book(old, m.PredictBook("HSI", "hsi-slug", "101", "t", [], [], L - 1, None)) == 0.0

# --- SIM-SELF-MATCH: a taker never buys one side at or through our own live resting buy of the other side -------------------------
# Our real 涨 maker rests at 60¢ × 50 and the fair price fell to 30¢: the 跌 side's "liquidity" at 40¢ = 1 − 60¢ is our own bid.
# The real taker is a LIMIT order at its cap, so a cap at or over 40¢ would match ourselves (a complete set plus the fee).
from decimal import Decimal as D  # noqa: E402

SELF = "hsi-self-match"


class SelfFM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


sbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_TAKER_HOLD_SECONDS": "0"}), m.Store(":memory:"), SelfFM(NOW), None)


def self_mk(bids, at=NOW - 1000):
    b = m.PredictBook("HSI", SELF, "1", "t", tuple((D(p), D(q)) for p, q in bids), ((D("0.62"), D("40")),), at, 200)
    return m.SimMarket(SELF, "恒生指数", "close", "HSI", 0.30, b, 0.02, "", ("涨", "跌"),
                       {"key": "HSI", "target": "2026-11-10", "line": 24600.0, "close_ms": NOW + 4 * 3600_000})


own_up = {"v": 2, "market": SELF, "slug": SELF, "item": "恒生指数", "kind": "close", "key": "HSI", "side": "up", "label": "挂涨",
          "maker": True, "status": "resting", "shares": 0.0, "order": 50.0, "price": 0.60, "fills": [], "revisions": [], "opened": NOW - 60_000,
          "live": {"state": "open", "order_id": "9", "answered_ms": NOW - 50_000, "answered_at": NOW - 50_000}}


def self_take(bids, rec):
    changed = []
    sbot.sim_take(self_mk(bids), m.TradeMap({f"{SELF}|up|挂": dict(rec)} if rec else {}), NOW, changed)
    return [t for _, t in changed if not t["maker"]]


# the only bid is ours: nothing to take (before the fix: 50 shares of 吃跌 at 40¢, all from our own order)
assert self_take([("0.60", "50")], own_up) == [], self_take([("0.60", "50")], own_up)
assert [t["label"] for t in self_take([("0.60", "50")], None)] == ["吃跌"]  # someone else's 60¢ bid: bought as before
# someone's 62¢ bid above ours: only it is taken, the cap (the live LIMIT price) a cent under our level
(tk,) = self_take([("0.62", "30"), ("0.60", "50")], own_up)
assert tk["label"] == "吃跌" and tk["cap"] <= 0.39 + 1e-9 and tk["shares"] <= 30 + 1e-9, (tk["cap"], tk["shares"])
# conservative on the order's state: in flight, cancelling and unknown count; a cancel Predict confirmed after the book was
# read still counts (the book may show it), one confirmed before does not; a paper record (no "live") and a filled one never
x = self_mk([("0.60", "50")])
for state in ("pending", "placing", "open", "cancelling", "unknown"):
    assert sbot.sim_taker_self_cap(m.TradeMap({"a": {**own_up, "live": {"state": state}}}), x, "down", 0.59) == 0.39, state
done = lambda at: {**own_up, "status": "cancelled", "live": {"state": "done", "final": "cancelled", "cancel_done_ms": at}}
assert sbot.sim_taker_self_cap(m.TradeMap({"a": done(x.book.fetched_ms + 1)}), x, "down", 0.59) == 0.39
assert sbot.sim_taker_self_cap(m.TradeMap({"a": done(x.book.fetched_ms - 1)}), x, "down", 0.59) == 0.59
assert sbot.sim_taker_self_cap(m.TradeMap({"a": {k: v for k, v in own_up.items() if k != "live"}}), x, "down", 0.59) == 0.59
assert sbot.sim_taker_self_cap(m.TradeMap({"a": {**own_up, "shares": 50.0}}), x, "down", 0.59) == 0.59
assert sbot.sim_taker_self_cap(m.TradeMap({"a": own_up}), x, "up", 0.59) == 0.59  # the same side is the maker logic's business
# a 跌 maker at 45¢ rests as an ask at 55¢: the 涨 taker stays under 55¢
assert sbot.sim_taker_self_cap(m.TradeMap({"a": {**own_up, "side": "down", "price": 0.45}}), x, "up", 0.70) == 0.54

# --- SIM-BLOCK-NOTE-WRITES: a refusal whose countdown ticks ("N 秒后再试") is noted once per window, not on every look ----------
bmk = self_mk([("0.60", "50")])
btid = f"{SELF}|up|吃"
writes = lambda: sbot.store.touched.get("simblock", 0)
w0 = writes()
sbot.sim_note_block(btid, bmk, "吃涨", 0.40, "下单失败：HTTP 400，120 秒后再试", NOW)
sbot.sim_note_block(btid, bmk, "吃涨", 0.40, "下单失败：HTTP 400，119 秒后再试", NOW + 1000)
sbot.sim_note_block(btid, bmk, "吃涨", 0.40, "Predict 接口限流冷却中（118 秒后重试）", NOW + 1500)
assert writes() - w0 == 2, ("the countdown alone made a new record", writes() - w0)  # before the fix: 3
sbot.sim_note_block(btid, bmk, "吃涨", 0.40, "Predict 接口限流冷却中（90 秒后重试）", NOW + 30_000)
assert writes() - w0 == 2, writes() - w0
sbot.sim_note_block(btid, bmk, "吃涨", 0.40, "收涨时这组仓位合计将亏 $350.00，超过组上限 $300", NOW + 31_000)
assert writes() - w0 == 3, "a different refusal is still noted at once"
sbot.sim_note_block(btid, bmk, "吃涨", 0.40, "收涨时这组仓位合计将亏 $350.00，超过组上限 $300", NOW + 31_000 + sbot.SIM_BLOCK_NOTE_MS)
assert writes() - w0 == 4, "the same refusal is noted again once the window has passed"
(rec,) = [b for b in sbot.sim_blocks() if b["id"] == btid]
assert rec["first"] == NOW and rec["why"].startswith("收涨时"), rec

# --- SIM-MAKER-EFF-SURVIVOR: the makers' efficiency counts the unfilled attempts sim_prune deleted ---------------------------------
# sim_prune keeps every fill and only the newest 20 empty attempts per slot, so the figures taken over what is left were the
# survivors': a slot quoted 100 times with 5 fills read 5 / 25 = 20% filled instead of 5%
pbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), m.Store(":memory:"), SelfFM(NOW), None)
pbot.sim_markets = lambda now: []  # no books: the open positions are just not marked
HSI = "hang-seng-index-up-or-down-on-november-10-2026"
WIN = "btc-updown-15m-1"
hsi = {"v": 2, "market": HSI, "slug": HSI, "market_id": "101", "item": "恒生指数", "kind": "close", "key": "HSI", "side": "up", "label": "挂涨",
       "maker": True, "fair": 0.7, "settle": {"key": "HSI", "target": "2026-11-10", "line": 24600.0, "close_ms": NOW + 3_600_000},
       "order": 100.0, "fills": [], "revisions": [], "price": 0.55, "edge": 0.15}
btc = {**hsi, "market": WIN, "slug": WIN, "market_id": "202", "item": "BTC 15分涨跌 12:00–12:15", "kind": "updown", "key": "BTC-15M",
       "driver": "BTCUSDT", "driver_name": "BTC", "order": 50.0, "settle": {"end": NOW + 900_000, "start": NOW, "window": "15m"}}
gone = lambda n, cents: {"opened": NOW - n * 1000, "status": "cancelled", "shares": 0.0,
                         "withdrawn": {"at": NOW - n * 1000 + 400, "why": f"买1 已到 {cents}¢，跟过去", "unfilled": 100.0}}
attempts = [(f"{HSI}|up|挂#{n}", {**hsi, **gone(n, 40 + n % 7)}) for n in range(1, 96)]  # 95 withdrawn empty after 0.4 s each
attempts += [(f"{HSI}|up|挂#{n}", {**hsi, "opened": NOW - n * 1000, "status": "filled", "shares": 100.0, "filled": NOW - n * 1000 + 100}) for n in range(96, 101)]
attempts += [(f"{WIN}|down|挂#{n}", {**btc, "side": "down", "label": "挂跌", "opened": NOW - n * 1000, "status": "expired", "shares": 0.0,
                                     "settled": NOW - n * 1000 + 900}) for n in range(1, 31)]  # another card: 30 lapsed at the end, 0.9 s each
attempts += [(f"{HSI}|down|吃#{n}", {**hsi, "side": "down", "label": "吃跌", "maker": False, "opened": NOW - n * 1000, "status": "cancelled",
                                     "shares": 0.0}) for n in range(1, 26)]  # takers that lapsed: pruned too, never part of the makers' figures
pbot.sim_save(attempts)
makers_of = lambda flt=None: pbot.journal_analysis(flt)["analysis"]["makers"]
before = makers_of()
assert (before["placed"], before["unfilled"], before["cancelled"], before["expired"], before["filled"]) == (130, 125, 95, 30, 5), before
assert abs(before["fill_rate"] - 5 / 130) < 1e-12 and abs(before["share_rate"] - 500 / 11_500) < 1e-12 and before["rest_n"] == 125, before
before_hsi, before_btc = makers_of({"card": "恒生指数"}), makers_of({"kind": "updown"})
assert pbot.sim_prune() == 75 + 10 + 5 and pbot.store.count("sim:") == len(pbot.sim_trades()) == 155 - 90
after = makers_of()
assert (after["placed"], after["unfilled"], after["fill_rate"]) == (130, 125, 5 / 130), ("the pruned attempts fell out of the figures", after)  # before the fix: 45, 40, 5 / 45
assert after == before, (before, after)  # shares, rests and withdrawal reasons too: nothing the page shows moved with the prune
assert makers_of({"card": "恒生指数"}) == before_hsi and before_hsi["fill_rate"] == 0.05 and before_hsi["placed"] == 100  # the page's filter: per card …
assert makers_of({"kind": "updown"}) == before_btc and (before_btc["placed"], before_btc["expired"], before_btc["fill_rate"]) == (30, 30, 0.0)  # … and per kind
assert makers_of({"way": "maker"}) == before and makers_of({"way": "taker"})["placed"] == 0 and makers_of({"card": "恒生指数", "kind": "updown"})["placed"] == 0
assert "挂单成交率 4%（130 笔挂单" in pbot.daily_report_text(NOW), pbot.daily_report_text(NOW)  # the daily report's line: 5 / 130, was 5 / 45 = 11%
# what is kept: one line per card and kind (not per slot: a 15-minute card opens two new slots every window), outside "sim:"
stored = {(r["card"], r["kind"]): r for r in pbot.store.get(m.SIM_PRUNED_KEY)}
assert set(stored) == {("恒生指数", "close"), ("BTC 15分涨跌", "updown")} and m.SIM_PRUNED_KEY not in pbot.sim_trades(), stored
h = stored[("恒生指数", "close")]
assert (h["n"], h["cancelled"], h.get("expired", 0), h["order"], h["rest_ms"], h["rest_n"]) == (75, 75, 0, 7500.0, 75 * 400, 75), h
assert h["reasons"] == {"买N 已到 N¢，跟过去": 75}, h["reasons"]
# a later prune adds to the totals; one with nothing to remove leaves them alone
assert pbot.sim_prune() == 0 and pbot.store.get(m.SIM_PRUNED_KEY) == list(stored.values())
pbot.sim_save([(f"{HSI}|up|挂#{n}", {**hsi, **gone(n - 200, 50)}) for n in range(101, 111)])  # ten newer attempts: ten older ones go
assert pbot.sim_prune() == 10
later = makers_of()
assert (later["placed"], later["unfilled"], later["cancelled"]) == (140, 135, 105) and abs(later["fill_rate"] - 5 / 140) < 1e-12, later
# the function alone, as before: without the totals it reads only the records it is given
alone = m.sim_maker_efficiency(list(pbot.sim_trades().values()))
assert (alone["placed"], alone["unfilled"]) == (45, 40) and m.sim_maker_efficiency([], [])["fill_rate"] is None, alone
# the reasons kept per card and kind are bounded: past SIM_PRUNED_REASONS distinct ones a new one counts as 其他
many = m.sim_pruned_count([], [{**hsi, "status": "cancelled", "shares": 0.0, "opened": NOW, "withdrawn": {"at": NOW + 1, "why": f"原因{chr(0x4E00 + i)}"}}
                               for i in range(m.SIM_PRUNED_REASONS + 5)])
assert len(many) == 1 and many[0]["n"] == m.SIM_PRUNED_REASONS + 5 and len(many[0]["reasons"]) == m.SIM_PRUNED_REASONS + 1 and many[0]["reasons"]["其他"] == 5, many

# --- SIM-TOUCH-DECIDED-HOLD: a first-touch market's hold is one answer for the card, the trader and the edge alerts ---------------
# BTC 先触 70k / 90k, the path verified clear up to a minute ago, BTC at 84,000: the model prices "90k first" at 70¢. Predict's
# book already trades "90k first" as settled (its "$70,000" outcome 1¢ bid / 2¢ offered): a wick through 90,000 that came back
# inside within its own minute, which the path check reads only at its next scan. The card held ("定局"); the trader, handed the
# market's own reasons alone, bought 300 × "70k first" at 2¢ and rested a bid on it (a real order with LIVE on), and the
# Telegram alerts announced it as 新机会 — the side the book priced at 2¢ because it had lost.
import asyncio  # noqa: E402

TSPEC = m.touch_spec("BTC-2027")
TOUCH_ENV = {"TELEGRAM_BOT_TOKEN": "1:x", "PROBABILITY": "off"}  # the defaults otherwise: SIM_MARKETS=close, the edge alerts on
TOUCH_TRADER = {**TOUCH_ENV, "SIM_MARKETS": "touch", "SIM_WAYS": "both", "SIM_TAKER_HOLD_SECONDS": "0", "SIM_MAKER_SESSION": "off",
                "SIM_MAKER_POINTS": "off", "SIM_MAKER_MIN_BID": "0"}
TOUCH_LOW = {"kind": "low", "time": NOW - 3_600_000, "hi": 70_400.0, "lo": 69_990.0}  # the path record of "70k first"


def touch_set(bot, price: str, bid: str, ask: str, at: int = NOW, hist: dict | None = None) -> None:
    """BTC at ``price`` now, σ fresh, the path record ``hist`` (default: clear, read up to a minute ago), and the book of
    Predict's first outcome ("$70,000": touch_book turns it into the "90k first" side) at ``bid`` / ``ask``."""
    touch = bot.touches[TSPEC.key]
    touch.price, touch.priced_ms, touch.sigma, touch.sigma_ms = D(price), at, 0.45, at
    bot.store.put(f"touch:{TSPEC.slug}", {**(hist or {"kind": "clear", "through": at - 60_000}), "start": touch.start_ms})
    bot.predict.info[TSPEC.slug] = {"outcomes": ["$70,000", "$90,000"], "created_ms": touch.start_ms}
    bot.predict.books[TSPEC.key] = m.PredictBook(TSPEC.key, TSPEC.slug, "501", "BTC", ((D(bid), D("2000")),), ((D(ask), D("3000")),), at)


def touch_bot(env: dict, price: str, bid: str, ask: str, hist: dict | None = None):
    bot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), SelfFM(NOW), None)
    bot.touches.setdefault(TSPEC.key, m.TouchMarket(bot.store, TSPEC))  # listed today; its line stays in TOUCH_SETTLED once it is decided
    touch_set(bot, price, bid, ask, NOW, hist)
    return bot


def touch_views(bot, at: int = NOW):
    """(the web card's hold, the SimMarket the trader and the alerts judge) of the BTC touch market."""
    card = bot.touch_payload(bot.touches[TSPEC.key], at)
    assert card["touch"]["hold"] == card["predict"]["hold"]
    (mk,) = [x for x in bot.sim_markets(at) if x.key == TSPEC.key]
    return card["predict"]["hold"], mk


async def touch_step(bot) -> set:
    """One express look and one step of the paper trader: the labels of the orders it opened."""
    await bot.sim_express_look(TSPEC.key, NOW)
    bot.sim_ran = -1e9
    await bot.sim_step(NOW)
    return {t["label"] for t in bot.sim_trades().values()}


async def touch_decided_hold():
    # the book trades "90k first" as settled (bid 98¢) against the model's 70¢ — and, the other way, "70k first" (90k first
    # offered at 3¢) against the model's 30¢: the trader holds as the card does, takers and resting orders, step and express lane
    for price, bid, ask, line in (("84000", "0.01", "0.02", "90k"), ("76000", "0.97", "0.98", "70k")):
        bot = touch_bot(TOUCH_TRADER, price, bid, ask)
        hold, mk = touch_views(bot)
        assert f"盘口已把 {line} 先触当作定局" in hold and mk.hold == hold, (line, hold, mk.hold)  # before the fix: mk.hold == ""
        assert [x.hold for x in bot.sim_markets_for(TSPEC.key, NOW)] == [hold]  # the express lane's view, the real trader's send-time re-check
        assert bot.sim_in_scope(mk) and bot.sim_taker_gap(mk, bot.sim_trades(), NOW)["state"] != "ready"
        assert bot.sim_maker_gate(mk, NOW).startswith("卡片暂不建议（盘口已把"), bot.sim_maker_gate(mk, NOW)
        assert await touch_step(bot) == set(), bot.sim_trades()  # before: 吃70k × 300 at 2¢ and 挂70k × 100 at 1¢ (吃90k / 挂90k the other way)
    # an open question (the "$70,000" outcome 10¢ / 12¢, "90k first" bid 88¢: under the 90¢ that reads as settled): no hold, it trades
    bot = touch_bot(TOUCH_TRADER, "84000", "0.10", "0.12")
    hold, mk = touch_views(bot)
    assert hold == "" and mk.hold == "" and 0.6 < mk.fair_up < 0.8, (hold, mk.hold, mk.fair_up)
    assert "吃70k" in await touch_step(bot), bot.sim_trades()
    # our own path record against the book: "70k first" on record, the book trading "90k first" as settled — one of the two is
    # wrong: held alike, nothing bought (before: 吃70k at 2¢ as a sure thing worth 95¢); the book agreeing (90k first offered at
    # 3¢), the decided odds stand and nothing holds
    bot = touch_bot(TOUCH_TRADER, "70500", "0.01", "0.02", TOUCH_LOW)
    hold, mk = touch_views(bot)
    assert "定局" in hold and mk.hold == hold and mk.fair_up == 0.0, (hold, mk.hold, mk.fair_up)
    assert await touch_step(bot) == set(), bot.sim_trades()
    hold, mk = touch_views(touch_bot(TOUCH_TRADER, "70500", "0.97", "0.98", TOUCH_LOW))
    assert hold == "" and mk.hold == "" and mk.fair_up == 0.0, (hold, mk.hold)
    # the Telegram edge alerts look at every priced card whatever SIM_MARKETS says (the defaults: 指数/个股日涨跌 alone): through the
    # confirmation time nothing is announced on the held card (before: "🟢 新机会｜BTC 先触 70k/90k … 挂70k @ 1.0¢ … 也可以：吃70k
    # @ 2.0¢"); on the open one the announcement comes as ever
    for bid, ask, want in (("0.01", "0.02", []), ("0.10", "0.12", ["appear"])):
        bot = touch_bot(TOUCH_ENV, "84000", bid, ask)
        assert not bot.sim_in_scope(touch_views(bot)[1]) and bot.config.edge_alert
        for at in (NOW, NOW + int(bot.config.edge_alert_confirm * 1000) + 1000):
            bot.market.now, bot.edge_ran = at, -1e9
            touch_set(bot, "84000", bid, ask, at)
            await bot.edge_alerts(at)
        kinds = [(st.get("alert") or {}).get("kind") for st in (bot.store.get("edgealerts", {}) or {}).values()]
        assert [k for k in kinds if k] == want, (bid, ask, bot.store.get("edgealerts", {}))


asyncio.run(touch_decided_hold())

# --- SIM-STALE-BOOK-REQUOTE: a resting bid the fair price has fallen under is withdrawn on a stale or crossed book too -------------
# A bid rests at 55¢ on a fair price of 70¢. Predict's book stops refreshing — its reads failing, or the feed in its own 429
# cooldown while the trading client still works — and is stale after 90 s; the card's fair price (the index feeds', not the
# book's: nothing holds the card) falls to 45¢. The exit line (SIM_MAKER_EXIT_CENTS, 5¢) is the rule that withdraws an order
# whose edge is gone, and it stood behind the "no fresh view of the market" return: the bid stayed, 10¢ over the bot's own price,
# until the stale-feed rule five minutes after the book's last read — with LIVE on a real order, for whoever saw the move.
import live  # noqa: E402

STALE = "hsi-stale-book"
STALE_SETTLE = {"key": "HSI", "target": "2026-11-10", "line": 24600.0, "close_ms": NOW + 4 * 3600_000}
STALE_ENV = {"TELEGRAM_BOT_TOKEN": "1:x", "SIM_WAYS": "maker", "SIM_MAKER_POINTS": "off"}  # the defaults otherwise: the 5¢ exit line, the 10¢ bar


def stale_mk(fair, read, bids=(("0.55", "300"),), asks=(("0.62", "100"),), **more):
    """The 恒指 daily card at a fair price of ``fair``, its book (买1 55¢ × 300, 卖1 62¢) last read at ``read``."""
    b = m.PredictBook("HSI", STALE, "401", "t", tuple((D(p), D(q)) for p, q in bids), tuple((D(p), D(q)) for p, q in asks), read, 200)
    return m.SimMarket(STALE, "恒生指数", "close", "HSI", fair, b, 0.03, more.pop("hold", ""), ("涨", "跌"), STALE_SETTLE, {}, **more)


def stale_bot(**env):
    return m.Bot(m.Config.from_env({**STALE_ENV, **env}), m.Store(":memory:"), SelfFM(NOW), None)


requote = lambda bot, rec, mk: bot.sim_requote_reason(rec, mk, NOW)
qbot = stale_bot()
bid55 = {"maker": True, "side": "up", "price": 0.55, "order": 100.0, "shares": 0.0, "status": "resting", "market": STALE, "kind": "close",
         "key": "HSI", "settle": STALE_SETTLE}
GONE = "净优势降到 -10.0¢，低于撤单线 5.0¢"
CROSSED = (("0.54", "100"),)  # 卖1 under the 55¢ 买1: a snapshot caught mid-update, which heartbeats keep "fresh" for as long as it lasts
assert requote(qbot, bid55, stale_mk(0.45, NOW)) == GONE  # a fresh book: as ever
for age in (95_000, 200_000, 299_000):  # stale (PREDICT_STALE_MS, 90 s) but not yet for five minutes — before the fix: ""
    assert requote(qbot, bid55, stale_mk(0.45, NOW - age)) == GONE, (age, requote(qbot, bid55, stale_mk(0.45, NOW - age)))
assert requote(qbot, bid55, stale_mk(0.45, NOW, asks=CROSSED)) == GONE  # a crossed book — before the fix: ""
assert requote(qbot, {**bid55, "side": "down"}, stale_mk(0.55, NOW - 95_000)) == GONE  # a 跌 bid at 55¢, 跌 worth 45¢
# the stale-feed rules come first, as before: five minutes on a daily market, 90 s on a 15-minute window
assert requote(qbot, bid55, stale_mk(0.45, NOW - 301_000)) == "盘口断流超过 5 分钟，撤单"
win15 = {"end": NOW + 600_000, "start": NOW - 300_000, "window": "15m"}
assert requote(qbot, {**bid55, "kind": "updown", "settle": win15},
               m.dataclasses.replace(stale_mk(0.45, NOW - 95_000), kind="updown", settle=win15)) == "盘口断流超过 90 秒，撤单"
# what still stands on a stale or crossed book: an order that has its edge (the line is the 5¢ exit line, not the 10¢ entry bar);
# a held card's order (the fair price itself is in doubt: SIM_NO_ODDS_WITHDRAW_MINUTES' business); any order with the exit line off
for fair in (0.70, 0.61):
    assert requote(qbot, bid55, stale_mk(fair, NOW - 95_000)) == "" and requote(qbot, bid55, stale_mk(fair, NOW, asks=CROSSED)) == "", fair
for mk in (stale_mk(0.45, NOW - 95_000, hold="σ 是先验值"), stale_mk(0.45, NOW, asks=CROSSED, hold="σ 是先验值"), stale_mk(0.45, NOW, hold="σ 是先验值")):
    assert requote(qbot, bid55, mk) == "", requote(qbot, bid55, mk)
assert requote(stale_bot(SIM_MAKER_EXIT_CENTS="0"), bid55, stale_mk(0.45, NOW - 95_000)) == ""
# ... and the rules that read the book wait for a fresh one: the 买1 gone down to 50¢ is followed there, not on its stale copy
fbot, moved = stale_bot(SIM_MAKER_FOLLOW_SETTLE_SECONDS="0"), (("0.50", "300"),)
assert requote(fbot, bid55, stale_mk(0.70, NOW, bids=moved)) == "买1 移到 50.0¢，改跟" and requote(fbot, bid55, stale_mk(0.70, NOW - 95_000, bids=moved)) == ""
# After hours the card is priced from a proxy and only bids with SIM_MAKER_AFTER_HOURS_CENTS (here 15¢) of edge rest, withdrawn a
# cent under it: that line reads no book either. A 40¢ bid whose edge fell to 8¢ goes on a stale or crossed book as on a fresh
# one (before the fix: kept, it still cleared the 5¢ exit line); in session the exit line alone applies
abot, bid40 = stale_bot(SIM_MAKER_AFTER_HOURS_CENTS="15"), {**bid55, "price": 0.40}
AFTER_HOURS = "盘后只挂净优势 ≥ 15.0¢ 的，现 8.0¢ 低于盘后撤单线 14.0¢"


def night_mk(fair, read, asks=(("0.47", "100"),), in_session=False):
    """The same card with its underlying not trading (``in_session`` False), the book at 买1 40¢ × 300, 卖1 47¢."""
    return stale_mk(fair, read, bids=(("0.40", "300"),), asks=asks, in_session=in_session)


assert requote(abot, bid40, night_mk(0.48, NOW)) == AFTER_HOURS  # a fresh book: as ever
assert requote(abot, bid40, night_mk(0.48, NOW - 95_000)) == AFTER_HOURS, requote(abot, bid40, night_mk(0.48, NOW - 95_000))  # stale — before: ""
assert requote(abot, bid40, night_mk(0.48, NOW, asks=(("0.39", "100"),))) == AFTER_HOURS  # crossed — before: ""
assert requote(abot, bid40, night_mk(0.545, NOW - 95_000)) == ""  # 14.5¢: inside the cent of slack under the bar
assert requote(abot, bid40, night_mk(0.48, NOW - 95_000, in_session=True)) == "" and requote(abot, bid40, night_mk(0.48, NOW - 95_000, in_session=None)) == ""


class StaleApi:
    """Predict as the real trader's client sees it: the market's details and our own orders, every order call noted."""
    def __init__(self):
        self.jwt, self.jwt_exp, self.auth_error, self.lookup = "t", 0, "", ""
        self.open, self.closed, self.calls = {}, {}, []
        self.markets = {"401": {"id": 401, "status": "REGISTERED", "isNegRisk": False, "isYieldBearing": False, "feeRateBps": 200,
                                "conditionId": "0x" + "ab" * 32, "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": "4011"},
                                                                              {"name": "Down", "indexSet": 2, "onChainId": "4012"}]}}

    async def ensure_auth(self): pass
    async def market(self, mid): return self.markets[str(mid)]
    async def positions(self): return []

    async def create_order(self, body):
        oid, order = str(101 + len(self.open) + len(self.closed)), body["data"]["order"]
        self.calls.append(("create", oid))
        self.open[oid] = {"id": oid, "status": "OPEN", "amount": order["takerAmount"], "amountFilled": "0", "strategy": body["data"]["strategy"],
                          "order": {**order}}
        return {"order_id": oid, "hash": order["hash"], "code": None}

    async def remove_orders(self, ids):
        self.calls.append(("remove", list(ids)))
        removed = [i for i in ids if i in self.open]
        for i in removed:
            self.closed[i] = {**self.open.pop(i), "status": "CANCELLED"}
        return {"removed": removed, "noop": [i for i in ids if i not in removed]}

    async def orders(self, status="OPEN", **params):
        return [dict(r) for r in (self.open.values() if status == "OPEN" else self.closed.values()) if r["status"] == status]

    async def order(self, oid, hash_=""):
        row = self.open.get(oid) or self.closed.get(oid)
        return dict(row) if row is not None else None


class StaleChain:
    async def account_owner(self, account): return "0x8fd379246834eac74B8419FfdA202CF8051F7A03"
    async def usdt_balance(self, owner=None): return 1000 * 10 ** 18
    async def bnb_balance(self, owner=None): return 10 ** 17
    async def allowance(self, key, owner=None): return 10 ** 30


async def stale_book_orders():
    """The paper trader and the real one over the same looks (the step, then the express lane): the bid placed on a fresh book
    at a fair price of 70¢, the book then frozen at that read while the fair price moves."""
    tid = f"{STALE}|up|挂"
    resting, pulled = {tid: ("resting", "")}, {f"{tid}#1": ("cancelled", GONE)}
    for real in (False, True):
        world = {"markets": []}
        if real:  # LIVE=on with the SDK test suite's key (never a real one); Predict's API and the chain are the stand-ins above
            bot = live.LiveBot(m.Config.from_env({**STALE_ENV, "LIVE": "on", "PREDICT_PRIVATE_KEY": "0x" + "a" * 64, "PREDICT_API_KEY": "k"}),
                               m.Store(":memory:"), SelfFM(NOW), None)
            bot.live.api, bot.live.chain = StaleApi(), StaleChain()
            await bot.live_prepare()
            assert bot.live.ready, bot.live.ready_error
        else:
            bot = stale_bot()
        bot.sim_markets = lambda now, only=None: world["markets"]
        placed, cancel = [("create", "101")], [("create", "101"), ("remove", ["101"])]
        for secs, fair, read, want, calls, on_predict in (
                (0, 0.70, 0, resting, placed, ["101"]),
                (95, 0.70, 0, resting, placed, ["101"]),  # the book stale, the edge still there: the order stands, as before
                (100, 0.45, 0, pulled, cancel, []),       # the fair price under the bid: withdrawn now — before the fix it rested until +300 s
                (120, 0.70, 0, pulled, cancel, []),       # the edge back, the book still stale: nothing is placed on a book nobody has read
                (150, 0.70, 150, {**pulled, **resting}, [*cancel, ("create", "102")], ["102"])):  # read again: quoted afresh
            at = NOW + secs * 1000
            bot.market.now, bot.sim_ran, world["markets"] = at, -1e9, [stale_mk(fair, NOW + read * 1000)]
            await bot.sim_step(at)
            await bot.sim_express("HSI")
            if real:
                await bot.live_fills_wait()
            got = {k: (t["status"], (t.get("withdrawn") or {}).get("why", "")) for k, t in bot.sim_trades().items()}
            assert got == want, (real, secs, got)
            if real:  # the cancel reached Predict, and no order went out while the book was stale
                assert bot.live.api.calls == calls and sorted(bot.live.api.open) == on_predict, (secs, bot.live.api.calls, sorted(bot.live.api.open))


asyncio.run(stale_book_orders())

print("AUDIT_DECISION_OK")
