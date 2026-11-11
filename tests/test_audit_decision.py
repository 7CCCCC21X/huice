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

# --- SIM-GROUP-WORST-CASE: one price path decides one first-touch market, or one ladder in one window — not a driver's every such market ---
# BTCUSDT carries BTC 先触 70k/90k (to 2027-05-05), BTC 11月价格 (to 11-30) and BTC 2027前价格 (to 12-31): one risk group. One path
# priced all their positions together and could not tell the markets apart: the first-touch market's own lines were not among its
# levels (any rise counted as "90k first"), and a level counted as touched whatever its ladder's window. Three shapes read as
# hedged — or as a sure gain — that one real path loses whole, so SIM_GROUP_USD (the paper trader's cap and the real one's) let
# them grow until other limits bound, and the review page's 最坏单一事件 showed the same figure.
G_TOUCH = m.touch_spec("BTC-2027")
# the two ladders as main lists them on 11-10, kept here as this section's own lines and put (back) into main's table, where a
# market's driver is looked up: a ladder leaves that table once its window is over
G_NOV = m.RangeSpec("BTC-HIT-11", "what-price-will-bitcoin-hit-in-november-2026", "BTCUSDT", "spot", "BTC 11月价格",
                    m.et_ms(2026, 11, 1, 0, 0, -4), m.et_ms(2026, 11, 30, 23, 59, -5), "down")
G_Y27 = m.RangeSpec("BTC-HIT-2027", "what-price-will-bitcoin-hit-before-2027", "BTCUSDT", "spot", "BTC 2027前价格",
                    m.et_ms(2025, 11, 24, 14, 0, -5), m.et_ms(2026, 12, 31, 23, 59, -5))
m.RANGE_MARKETS = (*(s for s in m.RANGE_MARKETS if s.key not in {G_NOV.key, G_Y27.key}), G_NOV, G_Y27)
G_LOW, G_HIGH, G_DAY = float(G_TOUCH.low), float(G_TOUCH.high), 86_400_000
assert NOW < G_NOV.end_ms < G_Y27.end_ms < G_TOUCH.deadline_ms and (G_LOW, G_HIGH) == (70_000.0, 90_000.0)
assert {m.market_driver("touch", G_TOUCH.key)[0], m.market_driver("range", G_NOV.key)[0], m.market_driver("range", G_Y27.key)[0]} == {"BTCUSDT"}


def g_touch(side: str, price: float, shares: float = 100.0, spec=G_TOUCH) -> dict:
    """A position on a first-touch market, as the trader records it: "up" = the high line (90k) first."""
    return {"kind": "touch", "key": spec.key, "market": spec.slug, "item": "BTC 先触 70k/90k", "side": side, "price": price, "shares": shares,
            "order": shares, "status": "filled", "maker": False, "settle": {"deadline": spec.deadline_ms}}


def g_level(spec, arrow: str, target: int, side: str, price: float, shares: float = 100.0) -> dict:
    """A position on one level of a price ladder: ``arrow`` "up" (↑: a High at or over the level in the window) or "down" (↓)."""
    return {"kind": "range", "key": spec.key, "market": f"{spec.slug}#{arrow}{target}", "item": f"{spec.name} {'↑' if arrow == 'up' else '↓'} ${target // 1000}k",
            "side": side, "price": price, "shares": shares, "order": shares, "status": "filled", "maker": False,
            "settle": {"target": str(target), "dir": arrow, "end": spec.end_ms}}


def g_real(positions: list, path: list) -> float:
    """What ``positions`` really make on a price path — [(time, price)] from now on, a straight move between two points — each
    market by its own rules: a level is touched by a point at or before its own ladder's end; the first-touch market goes to
    the line reached first before its deadline, 50/50 when neither is."""
    total = 0.0
    for q in positions:
        s = q["settle"]
        if q["kind"] == "range":
            seen = [px for at, px in path if at <= s["end"]]
            up = float(max(seen) >= float(s["target"]) if s["dir"] == "up" else min(seen) <= float(s["target"]))
        else:
            first = next((px for at, px in path if at <= s["deadline"] and not G_LOW < px < G_HIGH), None)
            up = 0.5 if first is None else float(first >= G_HIGH)
        total += ((up if q["side"] == "up" else 1 - up) - q["price"]) * q["shares"]
    return total


G_CASES = (
    # 「90k」 on the first touch + No on 11月 ↑ $85k: BTC rises to 85.5k, then falls through 70k — both lose (was: worst $0)
    ([g_touch("up", 0.50), g_level(G_NOV, "up", 85_000, "down", 0.50)],
     [(NOW, 80_000.0), (NOW + 5 * G_DAY, 85_500.0), (NOW + 10 * G_DAY, 69_500.0)], -100.0, "BTC 先触低线、BTC 涨到 $85k"),
    # 「70k」 + No on 11月 ↓ $75k: down to 74.5k, then up through 90k — both lose (was: worst $0)
    ([g_touch("down", 0.50), g_level(G_NOV, "down", 75_000, "down", 0.50)],
     [(NOW, 80_000.0), (NOW + 5 * G_DAY, 74_500.0), (NOW + 10 * G_DAY, 90_500.0)], -100.0, "BTC 先触高线、BTC 跌到 $75k"),
    # Yes on 11月 ↑ $100k + No on 2027前 ↑ $100k: no 100k in November, 100k on 12-10 — both lose (was: a sure gain of $15)
    ([g_level(G_NOV, "up", 100_000, "up", 0.30), g_level(G_Y27, "up", 100_000, "down", 0.55)],
     [(NOW, 80_000.0), (G_NOV.end_ms + 10 * G_DAY, 100_500.0)], -85.0, "BTC 都没触及、BTC 涨到 $100k"),
)
for g_held, g_path, g_loss, g_event in G_CASES:
    assert abs(g_real(g_held, g_path) - g_loss) < 1e-9, (g_event, g_real(g_held, g_path))  # the path really loses every leg
    g_worst, g_said = m.group_worst_case(g_held, "BTC")
    assert g_worst <= g_loss + 1e-9, ("the group's worst case is milder than a real path's loss", g_event, g_worst, g_said)
    assert abs(g_worst - g_loss) < 1e-9 and g_said == g_event, (g_worst, g_said)
# two first-touch markets on one pair (the earlier BTC 70k/90k, off the page but still to be settled, and the one to 2027) are two
# questions: 「90k」 on one and 「70k」 on the other both lose when each goes the wrong way (was: hedged, worst $0)
g_old = m.touch_spec("BTC")
assert g_old in m.TOUCH_SETTLED and m.market_driver("touch", g_old.key)[0] == "BTCUSDT" and g_old.key != G_TOUCH.key
assert m.group_worst_case([g_touch("up", 0.50, spec=g_old), g_touch("down", 0.50)], "BTC") == (-100.0, "BTC 先触低线、BTC 先触高线")
# a stock's "hits $100 by <date>" category lists a market per deadline under ONE key (StockRangeMarket.choose): Yes by June and No
# by December at the same level both lose when the level is first reached in between (was: a sure gain of $15)
g_june, g_dec = ({"kind": "range", "key": "STRC-100", "side": side, "price": price, "shares": 100.0,
                  "settle": {"target": "100", "dir": "up", "end": end}} for side, price, end in (("up", 0.30, NOW + 30 * G_DAY), ("down", 0.55, NOW + 200 * G_DAY)))
assert abs(m.group_worst_case([g_june, g_dec], "STRC")[0] + 85.0) < 1e-9, m.group_worst_case([g_june, g_dec], "STRC")
assert m.path_family(g_june) != m.path_family(g_dec) and m.path_family(g_touch("up", 0.5)) != m.path_family(g_touch("up", 0.5, spec=g_old))
# what stays one family, priced under one path as before: one ladder's levels in one window (the two sides of a level hedge; a
# level on each side of the price is lost on the path that touches both), and the two sides of one first-touch market
g_pair = [g_level(G_NOV, "up", 100_000, "down", 0.60), g_level(G_NOV, "up", 100_000, "up", 0.40)]
assert m.path_family(g_pair[0]) == m.path_family(g_pair[1]) == m.path_family(g_level(G_NOV, "down", 75_000, "down", 0.5))
assert abs(m.group_worst_case(g_pair, "BTC")[0]) < 1e-9, m.group_worst_case(g_pair, "BTC")
g_worst, g_said = m.group_worst_case([g_level(G_NOV, "up", 85_000, "down", 0.60), g_level(G_NOV, "down", 75_000, "down", 0.60)], "BTC")
assert abs(g_worst + 120.0) < 1e-9 and g_said in {"BTC 涨到 $85k 又跌到 $75k", "BTC 跌到 $75k 又涨到 $85k"}, (g_worst, g_said)
assert abs(m.group_worst_case([g_touch("up", 0.60), g_touch("down", 0.60)], "BTC")[0] + 20.0) < 1e-9
# what the careful way round costs: No on 11月 ↑ $100k with Yes on 2027前 ↑ $100k is a real hedge (a touch in November is a touch
# before 2027: the pair loses $20 at most), counted as two losses — the cap refuses more, it never lets more through
g_nested = [g_level(G_NOV, "up", 100_000, "down", 0.80), g_level(G_Y27, "up", 100_000, "up", 0.40)]
assert abs(m.group_worst_case(g_nested, "BTC")[0] + 120.0) < 1e-9 and m.group_worst_case(g_nested, "BTC")[0] <= -20.0
# the review page's 最坏单一事件 (sim_groups): the first shape at 300 shares a side — $345 at cost, all of it lost on the path above
(g_row,) = m.sim_groups([g_touch("up", 0.55, 300.0), g_level(G_NOV, "up", 85_000, "down", 0.60, 300.0)])
assert (g_row["driver"], g_row["markets"], g_row["event"]) == ("BTCUSDT", 2, "BTC 先触低线、BTC 涨到 $85k"), g_row
assert abs(g_row["worst"] + 345.0) < 1e-9 and abs(g_row["cost"] - 345.0) < 1e-9, (g_row["worst"], g_row["cost"])  # was: −$45


def g_market(rec: dict) -> m.SimMarket:
    """The market of a position, as the trader is offered it (the cap reads its kind, key and settle)."""
    book = m.PredictBook(rec["key"], rec["market"].partition("#")[0], "1", "t", ((D("0.49"), D("1000")),), ((D("0.51"), D("1000")),), NOW, 200)
    return m.SimMarket(rec["market"], rec["item"], rec["kind"], rec["key"], 0.5, book, 0.02, "", ("Yes", "No"), rec["settle"])


# the cap itself (sim_group_room: the paper trader's gate and the real trader's first one) at $300, sizes within a taker's 300
# shares a side — the defaults, spelt out. The real trader's class with LIVE off (the SDK test suite's key, never a real one):
# its own limits say nothing then, the cap's answer is all there is
G_ENV = {"TELEGRAM_BOT_TOKEN": "1:x", "SIM_GROUP_USD": "300", "SIM_WAYS": "taker", "SIM_TAKER_MAX_SHARES": "300", "SIM_TAKER_SWEEP_SHARES": "300"}
g_bots = [m.Bot(m.Config.from_env(G_ENV), m.Store(":memory:"), SelfFM(NOW), None),
          live.LiveBot(m.Config.from_env({**G_ENV, "PREDICT_PRIVATE_KEY": "0x" + "a" * 64}), m.Store(":memory:"), SelfFM(NOW), None)]
# four legs of 300 shares, $585 at cost, all of it lost when BTC reaches 89.9k in November (85k touched, 100k not) and goes on
# through 90k to 100.5k by 12-10
g_legs = [g_level(G_NOV, "up", 100_000, "up", 0.30, 300.0), g_level(G_Y27, "up", 100_000, "down", 0.55, 300.0),
          g_touch("down", 0.50, 300.0), g_level(G_NOV, "up", 85_000, "down", 0.60, 300.0)]
g_run = [(NOW, 80_000.0), (NOW + 10 * G_DAY, 89_900.0), (G_NOV.end_ms + 10 * G_DAY, 100_500.0)]
assert abs(g_real(g_legs, g_run) + 585.0) < 1e-9
for g_bot in g_bots:
    assert g_bot.config.sim_group_usd == 300 and g_bot.config.live_mode == "off"
    g_book = m.TradeMap({"t": g_touch("up", 0.55, 300.0)})
    g_no85 = g_level(G_NOV, "up", 85_000, "down", 0.60, 300.0)
    g_why = g_bot.sim_group_room(g_book, g_market(g_no85), "down", 0.60, 300.0)
    assert g_why.startswith("BTC 先触低线、BTC 涨到 $85k时这组仓位合计将亏 $345.00，超过组上限 $300（BTC 已有 1 笔"), g_why  # was: "" (let through)
    # ... and the order is cut to what fits, as on one ladder: $165 held, 225 more shares at 60¢ make the $300
    assert g_bot.sim_size_down(299, lambda n: not g_bot.sim_group_room(g_book, g_market(g_no85), "down", 0.60, float(n))) == 225
    # an order still resting counts as if filled, in its own family as well: 「90k」 × 300 bid at 55¢, nothing filled yet
    g_book = m.TradeMap({"t": {**g_touch("up", 0.55, 0.0), "order": 300.0, "status": "resting", "maker": True}})
    assert "合计将亏 $345.00，超过组上限 $300" in g_bot.sim_group_room(g_book, g_market(g_no85), "down", 0.60, 300.0)  # was: "" (read as −$180)
    # the candidate counts with the positions of its own ladder (its key): the other side of a held level is still a hedge
    g_book = m.TradeMap({"n": g_level(G_NOV, "up", 100_000, "down", 0.80, 300.0)})
    assert g_bot.sim_group_room(g_book, g_market(g_level(G_NOV, "up", 100_000, "up", 0.40)), "up", 0.40, 300.0) == ""
    # the four legs offered in turn, the cap deciding how much of each goes in: all $585 went in whole (the cap read −$285)
    g_book, g_took = m.TradeMap(), []
    for g_leg in g_legs:
        g_fits = lambda n, leg=g_leg: not g_bot.sim_group_room(g_book, g_market(leg), leg["side"], leg["price"], float(n))
        g_n = 300 if g_fits(300) else g_bot.sim_size_down(299, g_fits) or 0
        g_took.append(g_n)
        if g_n:
            g_book = m.TradeMap({**g_book, g_leg["market"] + g_leg["side"]: {**g_leg, "shares": float(g_n), "order": float(g_n)}})
    g_cost = sum(t["price"] * t["shares"] for t in g_book.values())
    assert g_took == [300, 300, 90, 0] and abs(g_cost - 300.0) < 1e-9, (g_took, g_cost)  # was: [300, 300, 300, 300], $585
    assert g_real(list(g_book.values()), g_run) >= -g_bot.config.sim_group_usd - 1e-9  # what that path now costs: the cap, no more
    assert abs(m.group_worst_case(list(g_book.values()), "BTC")[0] + 300.0) < 1e-9


def g_offer(rec: dict, mid: str, fair_up: float, bid: str, ask: str, sides=("Yes", "No")) -> m.SimMarket:
    """The market of ``rec`` as a card prices it: Yes (the high line) worth ``fair_up``, its book 1000 shares at ``bid`` / ``ask``."""
    book = m.PredictBook(rec["key"], rec["market"].partition("#")[0], mid, "t", ((D(bid), D("1000")),), ((D(ask), D("1000")),), NOW, 200)
    return m.SimMarket(rec["market"], rec["item"], rec["kind"], rec["key"], fair_up, book, 0.02, "", sides, rec["settle"])


async def group_cap_step():
    """The paper trader's own looks at those four markets, each offering some 24¢ of net edge on its leg's side: the same cap and
    sizes, the markets traded 先触价 and 价格阶梯 (SIM_MARKETS), the takers buying at the first look."""
    bot = m.Bot(m.Config.from_env({**G_ENV, "SIM_MARKETS": "touch,range", "SIM_TAKER_HOLD_SECONDS": "0", "SIM_TAKER_HOLD_RANGE_SECONDS": "0"}),
                m.Store(":memory:"), SelfFM(NOW), None)
    offers = [g_offer(g_legs[0], "901", 0.55, "0.28", "0.30"), g_offer(g_legs[1], "902", 0.20, "0.45", "0.47"),
              g_offer(g_legs[2], "903", 0.25, "0.50", "0.52", ("90k", "70k")), g_offer(g_legs[3], "904", 0.15, "0.40", "0.42")]
    bot.sim_markets = lambda now: offers
    for look in range(3):
        bot.market.now, bot.sim_ran = NOW + look * 5000, -1e9
        await bot.sim_step(NOW + look * 5000)
    got = list(bot.sim_trades().values())
    cost = sum(t["price"] * t["shares"] for t in got)
    # before the fix all four went in whole: 300 shares each, $594.90 at cost, the cap reading −$294.90 — and −$594.90 on the path
    assert {t["item"]: (t["label"], t["shares"]) for t in got} == {g_legs[0]["item"]: ("吃Yes", 300.0), g_legs[1]["item"]: ("吃No", 300.0),
                                                                   g_legs[2]["item"]: ("吃70k", 79.0)}, [(t["item"], t["label"], t["shares"]) for t in got]
    assert 299.0 < cost <= 300.0 and g_real(got, g_run) >= -300.0 - 1e-9, (cost, g_real(got, g_run))
    (row,) = bot.sim_report()["groups"]  # the page's figure is the path's
    assert abs(row["worst"] + cost) < 1e-9 and set(row["event"].split("、")) == {"BTC 都没触及", "BTC 涨到 $100k", "BTC 先触高线"}, row
    blocked = {b["item"]: b["why"] for b in bot.sim_blocks()}  # what did not fit is on record, with the reason
    assert set(blocked) == {g_legs[2]["item"], g_legs[3]["item"]} and all("超过组上限 $300" in why for why in blocked.values()), blocked


asyncio.run(group_cap_step())

# --- SIM-STALE-CLOCK: a look judges on the clock read where its decisions are made, not on the one its job began with -------------
# The step's job reads the clock; then the real trader's prefetch waits for Predict (a new market's details — seconds when Predict
# is slow, API_TIMEOUT at the worst — or a sign-in) and the step waits for the express lane's lock; only then are the markets
# priced, on the data of that moment, and judged. On the job's clock everything looked that much younger than it was: a book past
# PREDICT_STALE_MS (90 s) was traded, a taker's run of buyable looks (SIM_TAKER_HOLD_RANGE_SECONDS, the price ladders' 30 s wick
# filter) began before the first look, a taker's unfilled rest outlived LIVE_TAKER_WAIT_SECONDS. The send-time re-check kept for
# decisions that waited (live_taker_recheck) saw none of it: the order is stamped off the real clock (1.138.0) — "decided this
# very second".
CLOCK_ENV = {"TELEGRAM_BOT_TOKEN": "1:x", "SIM_WAYS": "taker", "SIM_MARKETS": "all", "SIM_TAKER_MAX_SHARES": "0", "SIM_TAKER_SWEEP_SHARES": "0"}
CLOCK_LIVE = {**CLOCK_ENV, "LIVE": "on", "PREDICT_PRIVATE_KEY": "0x" + "a" * 64, "PREDICT_API_KEY": "k",  # the SDK test suite's key, never a real one
              "LIVE_MAX_ORDER_USD": "100", "LIVE_MAX_OPEN_USD": "250"}
CLOCK_SETTLE = {"key": "CLK", "target": "2026-11-10", "line": 100.0, "close_ms": NOW + 4 * 3600_000}


class ClockApi(StaleApi):
    """Predict with three markets (501 and 502: two daily cards; 503: one level of a price ladder). The details read of market
    ``slow`` takes ``lag`` ms of the bot's clock; ``on_create`` / ``on_remove`` / ``on_orders`` are awaited while that call's
    request or answer is on its way."""
    def __init__(self, clock):
        super().__init__()
        self.clock, self.slow, self.lag = clock, "", 0
        self.on_create = self.on_remove = self.on_orders = None
        self.markets = {mid: {**self.markets["401"], "id": int(mid), "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": mid + "1"},
                                                                              {"name": "Down", "indexSet": 2, "onChainId": mid + "2"}]}
                        for mid in ("501", "502", "503")}

    async def market(self, mid):
        if str(mid) == self.slow:
            self.clock.now += self.lag
        return self.markets[str(mid)]

    async def create_order(self, body):
        out = await super().create_order(body)  # on Predict now...
        if self.on_create:
            await self.on_create(out["order_id"])  # ...its answer on its way back
        return out

    async def remove_orders(self, ids):
        if self.on_remove:
            await self.on_remove(list(ids))  # the request on its way: nothing removed yet
        return await super().remove_orders(ids)

    async def orders(self, status="OPEN", **params):
        rows = await super().orders(status, **params)  # Predict's answer as of now...
        if self.on_orders:
            await self.on_orders(status)  # ...on its way back
        return rows

    def tokens(self) -> list:
        """The outcome token of every order sent so far ("5011": market 501's first outcome, 涨 / Yes)."""
        return [row["order"]["tokenId"] for row in (*self.open.values(), *self.closed.values())]


def clock_card(mid: str, read: int, fair: float = 0.70, bid: str = "0.40", ask: str = "0.58") -> m.SimMarket:
    """A daily card on market ``mid``, its book last read at ``read``: 涨 worth ``fair``, offered at 58¢ × 400 — some 11¢ of
    net edge for a taker."""
    b = m.PredictBook(f"CLK-{mid}", f"clock-card-{mid}", mid, "t", ((D(bid), D("300")),), ((D(ask), D("400")),), read, 200)
    return m.SimMarket(b.slug, f"时钟卡 {mid}", "close", b.key, fair, b, 0.03, "", ("涨", "跌"), CLOCK_SETTLE, {})


def clock_level(read: int) -> m.SimMarket:
    """One level of a price ladder (market 503): Yes worth 50¢, offered at 30¢ × 400."""
    b = m.PredictBook("CLK-RANGE", "clock-range", "503", "t", ((D("0.10"), D("500")),), ((D("0.30"), D("400")),), read, 200)
    return m.SimMarket("clock-range#503", "时钟阶梯 ↑ $120k", "range", "CLK-RANGE", 0.50, b, 0.03, "", ("Yes", "No"),
                       {"target": "120000", "dir": "up", "end": NOW + 6 * 3600_000}, {}, makers=False)


async def clock_bot(**env):
    """The real trader over the stand-ins (Predict's API with the slow details read, the chain), signed in; its markets are
    world["markets"]. (the bot, the API, the world)."""
    clock, world = SelfFM(NOW), {"markets": []}
    bot = live.LiveBot(m.Config.from_env({**CLOCK_LIVE, **env}), m.Store(":memory:"), clock, None)
    bot.live.api, bot.live.chain = ClockApi(clock), StaleChain()
    bot.sim_markets = lambda now, only=None: world["markets"]
    await bot.live_prepare()
    assert bot.live.ready, bot.live.ready_error
    return bot, bot.live.api, world


async def clock_step(bot, world, at: int, *markets) -> None:
    """The step's job starting at ``at`` (the clock its job reads) over ``markets``."""
    bot.market.now, bot.sim_ran, world["markets"] = at, -1e9, list(markets)
    await bot.sim_step(at)


async def clock_stale_book():
    """Market 501's details are known; its book was read at NOW and not again. The job starts 85 s later (the book fresh) and its
    prefetch reads the details of ANOTHER market, new on the page: 12 s. The step decides 97 s after the book's read — nothing is
    sent on it (before the fix: a real LIMIT order, the send-time re-check passed over). Without the slow read the 85 s book
    trades as ever."""
    for lag, traded in ((12_000, False), (0, True)):
        bot, api, world = await clock_bot()
        world["markets"] = [clock_card("501", NOW)]
        await bot.live_prefetch(NOW)
        assert "501" in bot.live.markets and "502" not in bot.live.markets
        api.slow, api.lag = "502", lag
        priced, synced, real_sync = [], [], bot.live_sync
        bot.sim_markets = lambda now, only=None, priced=priced, world=world: (priced.append(now), world["markets"])[1]

        async def sync_spy(now_ms, synced=synced, real_sync=real_sync):
            synced.append(now_ms)
            return await real_sync(now_ms)
        bot.live_sync = sync_spy
        start = NOW + 85_000
        await clock_step(bot, world, start, clock_card("501", NOW), clock_card("502", start))
        trades = bot.sim_trades()
        assert ("5011" in api.tokens()) == traded and ("clock-card-501|up|吃" in trades) == traded, (lag, api.tokens(), sorted(trades))
        assert "5021" in api.tokens(), (lag, api.tokens(), bot.sim_blocks()[:2])  # the new market's own book is fresh: it trades either way
        # the prefetch prices the cards on the job's clock (it runs first); the decisions and all that follows them on the look's —
        # the cards' own holds (a quote's age, a market's last minutes) and the time left to the close are judged as of then too
        assert priced[0] == start and set(priced[1:]) == {start + lag}, (lag, [at - start for at in priced])
        fresh = trades["clock-card-502|up|吃"]
        # ...which is the record's time, the stamp the send-time re-check measures from (1.138.0, unchanged: a decision of this
        # very second goes out as made) and the clock of the sync that follows the decisions
        assert bot.sim_looked_ms == start + lag and fresh["opened"] == start + lag and fresh["live"]["events"][0]["at"] == start + lag, \
            (lag, bot.sim_looked_ms - start, fresh["opened"] - start, fresh["live"]["events"][0]["at"] - start)
        assert fresh["live"]["state"] == "open" and synced == [start + lag], (lag, fresh["live"]["state"], [s - start for s in synced])


async def clock_hold_run():
    """The price ladders' wick filter (SIM_TAKER_HOLD_RANGE_SECONDS, 30 s by default): a side's run of buyable looks begins when
    the step really looked — 12 s after its job began — so the express lane, on the real clock, buys after 30 s of it (before
    the fix: after 18 s, the run stamped at the job's start)."""
    bot, api, world = await clock_bot()
    assert bot.config.sim_taker_hold_range == 30.0 and bot.config.sim_taker_hold == 0.0, "the defaults"
    api.slow, api.lag = "502", 12_000
    await clock_step(bot, world, NOW, clock_level(NOW), clock_card("502", NOW))
    looked = NOW + 12_000
    assert bot.sim_taker_seen["clock-range#503|up|吃"] == looked and "5031" not in api.tokens(), \
        (bot.sim_taker_seen["clock-range#503|up|吃"] - NOW, api.tokens())
    api.lag = 0
    for after, bought in ((18_000, False), (31_000, True)):
        bot.market.now, world["markets"] = looked + after, [clock_level(looked + after)]
        await bot.sim_express("CLK-RANGE")
        assert ("5031" in api.tokens()) == bought, (after, api.tokens())


async def clock_taker_rest():
    """A taker's LIMIT order rests unfilled (LIVE_TAKER_WAIT_SECONDS, 60 by default). The next job starts 57 s after Predict's
    answer and waits 5 s for a new market's details: the step looks 62 s after it and its sync cancels the rest (before the fix:
    kept for another step as "57 s old" — the sync ran on the job's clock, placed_at is read off the real one)."""
    bot, api, world = await clock_bot()
    assert bot.config.live_taker_wait == 60
    await clock_step(bot, world, NOW, clock_card("501", NOW))
    rest = bot.sim_trades()["clock-card-501|up|吃"]["live"]
    placed, oid = rest["placed_at"], rest["order_id"]
    assert rest["state"] == "open" and placed == NOW and oid in api.open
    api.slow, api.lag = "502", 5_000
    await clock_step(bot, world, placed + 57_000, clock_card("501", placed + 57_000), clock_card("502", placed + 57_000))
    rest = next(t["live"] for t in bot.sim_trades().values() if (t.get("live") or {}).get("order_id") == oid)
    assert rest["state"] == "done" and rest["cancel_why"] == "吃单 60 秒未全部成交" and oid not in api.open, (rest["state"], rest.get("cancel_why"))


async def clock_behind_lock():
    """The lock. A look that waits for the other lane's decisions judges on the clock after the wait — the step, and the express
    lane too (it reads its clock before it waits): a book 89 s old when the look began and 90.5 s old when it could judge is not
    traded (before the fix: judged 89 s old, bought). The paper trader here: the clock is the decision layer's, whoever sends."""
    paper = m.Bot(m.Config.from_env(CLOCK_ENV), m.Store(":memory:"), SelfFM(NOW), None)
    seen, offered, after = [], {}, []
    paper.sim_markets = lambda now, only=None: (seen.append(now), [offered["card"]])[1]
    real_after = paper.sim_express_after

    async def after_spy(now_ms):
        after.append(now_ms)
        return await real_after(now_ms)
    paper.sim_express_after = after_spy

    async def behind_the_lock(look, card):
        offered["card"], paper.market.now, paper.sim_ran = card, NOW, -1e9
        await paper.sim_lock.acquire()          # the other lane decides...
        waiting = asyncio.ensure_future(look())
        await asyncio.sleep(0)                  # ...this look has read its clock and waits for the lock...
        paper.market.now = NOW + 1_500          # ...a second and a half
        paper.sim_lock.release()
        await asyncio.wait_for(waiting, 5)

    for look in (lambda: paper.sim_step(NOW), lambda: paper.sim_express("CLK-501")):
        await behind_the_lock(look, clock_card("501", NOW - 89_000))
        assert seen[-1] == NOW + 1_500 and not paper.sim_trades(), (seen[-1] - NOW, sorted(paper.sim_trades()))
    assert paper.sim_looked_ms == NOW + 1_500
    # ...and what follows an express decision (the real trader's orders and cancels) is not stamped before the look it follows
    await behind_the_lock(lambda: paper.sim_express("CLK-501"), clock_card("501", NOW))
    opened = [t["opened"] - NOW for t in paper.sim_trades().values()]
    assert opened == [1_500] and after == [NOW + 1_500], (opened, [a - NOW for a in after])
    # a caller's clock ahead of the bot's own stands, as the decision's stamp does (max): nothing is judged on an earlier clock
    paper.market.now, paper.sim_ran = NOW, -1e9
    await paper.sim_step(NOW + 5_000)
    assert seen[-1] == NOW + 5_000 and paper.sim_looked_ms == NOW + 5_000


async def clock_sync_list():
    """The step's sync runs on the clock the step decided on — not on one read when the sync begins. Its open-order list is
    stamped with that clock and weighed against the records' own stamps (live_readopt: a list older than a record's last change
    says nothing about it), and the express lane stamps a record when its pass begins, not when Predict answers its cancel. An
    express pass beside the step's own placement: the index moved, the resting 55¢ bid is withdrawn, its cancel on its way; the
    step's sync asks for the list, Predict answers with the bid still on it, then the cancel lands and is confirmed, then the
    list arrives. Stamped at the sync's start that list would be "newer" than the record and take the cancelled order back
    (恢复跟踪, a ⚠️ message, a second cancel); on the step's clock it is left alone, as before the fix."""
    bot, api, world = await clock_bot(SIM_WAYS="maker", SIM_MAKER_EXIT_CENTS="6", SIM_MAKER_MIN_BID="100", SIM_MAKER_SESSION="off",
                                      SIM_MAKER_POINTS="off", SIM_MAKER_SPREAD_CENTS="10", SIM_MAKER_FOLLOW_SETTLE_SECONDS="0")
    bot.live_market_filter = False  # (the stand-in's list does not filter by market: one read a sync)
    resting = lambda mid, at: clock_card(mid, at, 0.70, "0.55", "0.58")
    world["markets"] = [resting("501", NOW)]
    await bot.live_prefetch(NOW)
    await bot.sim_express("CLK-501")
    first = bot.sim_trades()["clock-card-501|up|挂"]["live"]
    oid = first["order_id"]
    assert first["state"] == "open" and oid in api.open
    notes, phase = [], {"n": 0}
    bot.live_notify = lambda text, key=None, **more: notes.append(text)
    cancel_sent, cancel_lands = asyncio.Event(), asyncio.Event()
    at = NOW + 5_000

    async def on_create(order_id):
        if phase["n"] == 0:  # the step's own order (market 502) is on Predict, its answer 200 ms away...
            phase["n"] = 1
            bot.market.now += 200
            world["markets"] = [clock_card("501", bot.market.now, 0.60, "0.50", "0.53"), resting("502", at)]  # ...the index moved meanwhile:
            phase["express"] = asyncio.ensure_future(bot.sim_express("CLK-501"))  # the lane withdraws the 55¢ bid (5¢ left, under the 6¢ line)...
            await asyncio.wait_for(cancel_sent.wait(), 5)                         # ...and its cancel request is on its way

    async def on_remove(ids):
        if oid in ids and not cancel_lands.is_set():
            cancel_sent.set()
            await asyncio.wait_for(cancel_lands.wait(), 5)  # Predict has not processed the removal yet

    record = lambda: next(t for t in bot.sim_trades().values() if (t.get("live") or {}).get("order_id") == oid)

    async def on_orders(status):
        if phase["n"] == 1 and status == "OPEN":  # the step's sync: Predict's answer, the 55¢ bid still on it, is on its way back...
            phase["n"] = 2
            assert oid in api.open
            cancel_lands.set()                    # ...while the removal is processed and answered
            for _ in range(60):
                await asyncio.sleep(0)
            phase["ended"] = record()["live"]["state"] == "done"
    api.on_create, api.on_remove, api.on_orders = on_create, on_remove, on_orders
    await clock_step(bot, world, at, resting("501", at), resting("502", at))
    await asyncio.wait_for(phase["express"], 5)
    await bot.live_fills_wait()
    # the race ran: the list was made with the bid on it, and the record had ended (the cancel confirmed) before the list arrived
    assert phase["n"] == 2 and phase["ended"] and bot.market.now == at + 200, phase
    old = record()
    events = [e["what"] for e in old["live"]["events"]]
    assert old["live"]["state"] == "done" and old["live"]["final"] == "cancelled" and old["live"]["cancel_confirmed"], old["live"]
    assert not any("重新出现在开放订单列表" in e for e in events) and not any("本已按结束处理" in n for n in notes), (events, notes)
    assert [c for c in api.calls if c[0] == "remove"] == [("remove", [oid])], api.calls


CLOCK_CASES = (clock_stale_book, clock_hold_run, clock_taker_rest, clock_behind_lock, clock_sync_list)
for clock_case in CLOCK_CASES:
    asyncio.run(clock_case())

print("AUDIT_DECISION_OK")
