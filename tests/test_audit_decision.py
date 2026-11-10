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

print("AUDIT_DECISION_OK")
