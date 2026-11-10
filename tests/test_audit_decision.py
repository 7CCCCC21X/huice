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

print("AUDIT_DECISION_OK")
