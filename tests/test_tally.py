"""Offline checks of the review page's 市场涨跌统计 (1.143.0): each window's up or down by Binance's 1-minute candles —
the counts, the runs, the hours, the ties and gaps, Predict's own results against the candles', the range, the page; 跟随还是反转
and 预挂成交的那一边 (1.144.0).

Run: python tests/test_tally.py
"""
import asyncio
import datetime as dt
import json
import os
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

BJ = lambda *a: int(dt.datetime(2026, *a, tzinfo=m.BEIJING).timestamp() * 1000)
DAY0 = BJ(11, 8, 0, 0)    # Sunday 11-08, Beijing
NOW = BJ(11, 8, 10, 2)    # 10:02: the 5-minute windows from 00:00 to 09:55 are over (120 of them)
TOKEN = "t" * 24

# the boundary prices of the day's 5-minute windows (the close of the candle ending at each boundary): six up, five down, a tie,
# then up and down in turn — 60 up, 59 down, 1 tie
STEPS = [1] * 6 + [-1] * 5 + [0] + [1 if k % 2 == 0 else -1 for k in range(108)]
BOUNDS = [100.0]
for s in STEPS:
    BOUNDS.append(BOUNDS[-1] + s)


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def close_at(open_ms: int, base: float = 100.0) -> float:
    """The close of the 1-minute candle that opens at ``open_ms``: a boundary's price when it ends on one, else the base."""
    end = open_ms + 60_000
    k, rest = divmod(end - DAY0, 300_000)
    return BOUNDS[k] if not rest and 0 <= k < len(BOUNDS) else base


def close_near(open_ms: int) -> float:
    """BNB's candles (1.144.1): the day's boundaries 600.00 and 600.03 in turn — every window up or down by 0.005%, inside the
    0.01% band (a close call each), the other closes 600."""
    end = open_ms + 60_000
    k, rest = divmod(end - DAY0, 300_000)
    return 600.0 + 0.03 * (k % 2) if not rest and 0 <= k <= 120 else 600.0


def candles(start: int, limit: int, now: int, close=close_at) -> list:
    """Binance's answer: the candles from ``start``, the one still open included (its close time ahead of now)."""
    out = []
    for i in range(limit):
        o = start + i * 60_000
        if o > now:
            break
        out.append([o, "0", "0", "0", str(close(o)), "0", o + 59_999, "0", 0, "0", "0", "0"])
    return out


def day_rows() -> list[dict]:
    """The synthetic day's windows as tally_rows gives them (a 5-minute step each): 6 up, 5 down, a tie, then up and down in turn."""
    return [{"s": DAY0 + i * 300_000, "e": DAY0 + (i + 1) * 300_000, "r": "u" if s > 0 else "d" if s < 0 else "f"} for i, s in enumerate(STEPS)]


# 预挂 fills for the day (1.144.0): (the window's index, up filled, down filled, price, Predict's result kept): the up side filled where the
# candles went down 20 times and up 5 times, the down side where they went up 15 times and down 5 times — 10 wins of 45, adverse selection
FILLS = ([(i, 10, 0, 0.49, "") for i in range(13, 52, 2)] + [(i, 10, 0, 0.49, "") for i in (12, 14, 16, 18, 20)]
         + [(i, 0, 10, 0.49, "") for i in range(52, 81, 2)] + [(i, 0, 10, 0.49, "") for i in (81, 83, 85, 87, 89)]
         + [(i, 10, 0, 0.48, "") for i in (13, 15, 17)]                                 # a deeper level in windows already counted: the window once, the level apart
         + [(100, 10, 5, 0.49, ""), (101, 10, 10, 0.49, ""), (103, 3, 10, 0.49, ""), (105, 10, 2, 0.49, "")]  # both sides: hedged, the larger side 2 of 3
         + [(11, 10, 0, 0.49, "")])                                                     # the tie: no result yet


def ledger(fills: list) -> list[dict]:
    """Pre-open ledger records as quote_of reads them: BTC-5M windows of the day, the price, the shares filled a side, the result kept."""
    out = []
    for n, (k, up, down, price, result) in enumerate(fills):
        start = DAY0 + k * 300_000
        out.append({"key": f"BTC-5M-{n}", "name": "BTC 5分涨跌 测试", "slug": f"btc-updown-5m-{start // 1000}" + ("#1" if price != 0.49 else "") + "@1",
                    "price": price, "shares": 10.0, "start_ms": start, "phase": "done", "accounted": bool(result), "result": result, "attempt": 1,
                    "orders": {side: {"state": "filled" if got else "cancelled", "filled": got} for side, got in (("up", up), ("down", down))}})
    return out


def unit_checks():
    """跟随还是反转 and 预挂成交的那一边 (1.144.0) on rows and fills made by hand; the close calls (1.144.1)."""
    # 1.144.1: a move inside the band is up or down all the same, marked a close call; an exact tie alone is flat
    near = m.tally_rows([(0, 5, 10, 20), (5, 10, 20, 30), (10, 15, 30, 40), (15, 20, 40, 50), (20, 25, 50, 60)],
                        {10: 100.0, 20: 100.005, 30: 100.005, 40: 99.99, 50: 99.99}, 0.0001)
    assert [(r["r"], r.get("n", 0)) for r in near] == [("u", 1), ("f", 0), ("d", 0), ("f", 0), ("x", 0)], near  # +0.005%: near; −0.015%: not
    assert m.tally_rows([(0, 5, 10, 20)], {10: 100.0, 20: 100.004}, 0.0)[0] == {"s": 0, "e": 5, "a": 100.0, "b": 100.004, "r": "u"}  # a daily card: no band
    seen = m.tally_summary(near, {0: "d", 10: "d"})  # Predict saw the close call the other way; the clear one alike
    assert seen["counts"] == {"total": 5, "up": 1, "down": 1, "flat": 2, "missing": 1, "near": 1}, seen["counts"]
    assert seen["checks"] == {"n": 2, "agree": 1, "bad": [{"s": 0, "r": "u", "o": "d", "n": 1}], "near": {"n": 1, "agree": 0}}, seen["checks"]
    rows = day_rows()
    f = m.tally_follow(rows)
    # pairs: 5 ups after ups, an up into a down, 4 downs after downs (the tie breaks the chain), then 107 turns; p = 60/119
    assert (f["pairs"], f["same"], f["after_up"], f["after_down"], f["expected"], f["z"]) == (117, 9, {"n": 60, "up": 5}, {"n": 57, "down": 4}, 58.5, -9.16), f
    assert [(r["k"], r["len"], r["n"], r["same"]) for r in f["runs"]] == [("u", 1, 55, 1), ("u", 2, 1, 1), ("u", 3, 1, 1), ("u", 4, 3, 2),
                                                                           ("d", 1, 54, 1), ("d", 2, 1, 1), ("d", 3, 1, 1), ("d", 4, 1, 1)], f["runs"]
    assert f["verdict"] == "偏反转（明显，超过 3σ）：涨跌容易交替" and f["p"] == round(60 / 119, 4)
    # a gap (the next starts later than the last ended: a closed market) or a missing candle breaks the chain, and the run restarts
    gap = [{"s": 0, "e": 5, "r": "u"}, {"s": 5, "e": 10, "r": "u"}, {"s": 20, "e": 25, "r": "u"}, {"s": 25, "e": 30, "r": "x"}, {"s": 30, "e": 35, "r": "u"}, {"s": 35, "e": 40, "r": "u"}]
    g = m.tally_follow(gap)
    assert (g["pairs"], g["same"], g["z"]) == (2, 2, None) and [(r["k"], r["len"], r["n"]) for r in g["runs"] if r["n"]] == [("u", 1, 2)], g  # p = 1: no spread to test against
    assert g["verdict"] == f"样本太少（不到 {m.TALLY_MIN_PAIRS} 对），先别下结论"
    assert m.tally_follow([])["pairs"] == 0 and m.tally_follow([])["z"] is None
    # streaky sequences read as following, a fair coin as nothing: 40 ups then 40 downs; up, up, down, down … (a pair in turn)
    streak = [{"s": i, "e": i + 1, "r": "u" if i < 40 else "d"} for i in range(80)]
    assert m.tally_follow(streak)["verdict"].startswith("偏跟随（明显，超过 3σ）：涨了容易接着涨") and m.tally_follow(streak)["z"] > 3
    pairs = [{"s": i, "e": i + 1, "r": "u" if i // 2 % 2 == 0 else "d"} for i in range(80)]  # same and turn in turn: half the pairs the same
    assert abs(m.tally_follow(pairs)["z"]) < 2 and m.tally_follow(pairs)["verdict"] == "像抛硬币，看不出跟随或反转"
    assert m.tally_reading(2.4, 50, 30, "对", ("偏跟随", "x"), ("偏反转", "y"), "e") == "偏跟随（有一点，2σ 到 3σ，可能是运气，换个时间段再看）：x"
    assert m.tally_reading(-2.4, 50, 30, "对", ("偏跟随", "x"), ("偏反转", "y"), "e").startswith("偏反转（有一点") and m.tally_reading(1.9, 50, 30, "对", ("a", "b"), ("c", "d"), "e") == "e"
    # the fills: one-sided windows by their side, a window's levels together, Predict's result over the candles', the hedged ones apart
    fills = [{"s": DAY0 + k * 300_000, "up": up, "down": down, "price": price, "result": res} for k, up, down, price, res in FILLS]
    fills += [{"s": DAY0 + 107 * 300_000, "up": 0, "down": 10, "price": 0.49, "result": "s"},   # Predict settled 50-50: not a win, not a loss
              {"s": DAY0 + 500 * 300_000, "up": 10, "down": 0, "price": 0.49, "result": ""}]   # not one of the range's windows
    x = m.tally_fills(rows, fills)
    assert x["windows"] == 51 and x["up"] == {"n": 26, "decided": 25, "won": 5, "expected": 12.6} and x["down"] == {"n": 21, "decided": 20, "won": 5, "expected": 9.9}, x
    assert x["one"]["decided"] == 45 and x["one"]["won"] == 10 and x["one"]["expected"] == 22.5 and x["one"]["z"] == -3.73, x["one"]
    assert x["one"]["verdict"] == "有逆向选择（明显，超过 3σ）：成交那边赢得比随机少，被吃掉的多是要输的那边"
    assert x["both"] == {"n": 4, "uneven": 3, "heavy_won": 2} and (x["split"], x["unknown"], x["kline"]) == (1, 1, 45) and x["price"] == 0.4894, x
    assert x["levels"] == [{"price": 0.49, "n": 45, "won": 10, "expected": 22.5}, {"price": 0.48, "n": 3, "won": 0, "expected": 1.5}], x["levels"]
    # Predict's result where the ledger keeps one: an up fill the candles call down, Predict up — a win; too few to read
    few = m.tally_fills(rows, [{"s": DAY0 + 13 * 300_000, "up": 10, "down": 0, "price": 0.49, "result": "u"}, {"s": DAY0 + 13 * 300_000, "up": 0, "down": 0, "price": 0.48, "result": ""}])
    assert (few["windows"], few["up"]["won"], few["kline"], few["one"]["z"] is not None) == (1, 1, 0, True), few
    assert few["one"]["verdict"] == f"样本太少（不到 {m.TALLY_MIN_FILLS} 场），先别下结论" and few["levels"] == [{"price": 0.49, "n": 1, "won": 1, "expected": 0.5}]
    assert m.tally_fills(rows, [])["windows"] == 0 and m.tally_fills(rows, [])["price"] is None and m.tally_fills([], [])["one"]["z"] is None
    assert len(ledger(FILLS)) == len(FILLS)


async def run():
    unit_checks()
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"})
    bot = m.Bot(cfg, m.Store(":memory:"), FM(NOW), None)
    calls, broken = [], set()

    async def spot(path, **params):
        calls.append((path, params["symbol"], params["startTime"], params["limit"]))
        if params["symbol"] in broken:
            raise m.RemoteError("HTTP 500: boom")
        return candles(params["startTime"], params["limit"], bot.market.now, close_near if params["symbol"] == "BNBUSDT" else close_at)
    m.binance_spot = spot

    async def settle():
        await asyncio.gather(*list(bot.tally_tasks.values()), return_exceptions=True)

    # the first ask reads the candles in the background; the second answers
    first = bot.tally_payload("BTC-5M", "2026-11-08", "2026-11-08")
    assert first["ready"] is False and "正在读币安 1 分钟 K 线" in first["message"] and first["name"] == "BTC 5分", first
    await settle()
    assert {c[:2] for c in calls} == {("klines", "BTCUSDT")} and all(c[3] == 1000 for c in calls) and all(c[2] % m.TALLY_CHUNK_MS == 0 for c in calls), calls
    p = bot.tally_payload("BTC-5M", "2026-11-08", "2026-11-08")
    assert p["ready"] and p["counts"] == {"total": 120, "up": 60, "down": 59, "flat": 1, "missing": 0, "near": 0}, p["counts"]
    assert p["range"] == "11-08 00:00–10:00" and (p["from"], p["to"], p["today"]) == ("2026-11-08", "2026-11-08", "2026-11-08"), p["range"]
    assert p["streaks"]["up"] == {"n": 6, "from": DAY0, "to": DAY0 + 30 * 60_000} and p["streaks"]["down"] == {"n": 5, "from": DAY0 + 30 * 60_000, "to": DAY0 + 55 * 60_000}
    assert [w["r"] for w in p["windows"][:13]] == ["u"] * 6 + ["d"] * 5 + ["f", "u"] and p["windows"][0]["a"] == 100.0 and p["windows"][0]["b"] == 101.0
    assert [(h["h"], h["up"], h["down"], h["flat"]) for h in p["hours"][:2]] == [(0, 6, 5, 1), (1, 6, 6, 0)] and len(p["hours"]) == 10
    assert p["checks"] == {"n": 0, "agree": 0, "bad": [], "near": {"n": 0, "agree": 0}} and p["source"] == "币安现货 1 分钟 K 线" and p["tie"] == float(m.SpotWindowMarket.TIE)
    assert p["days"] == [{"d": "2026-11-08", "label": "11-08 周日", "total": 120, "up": 60, "down": 59, "flat": 1, "missing": 0, "best_up": 6, "best_down": 5}], p["days"]  # 1.143.1
    asked = len(calls)
    assert bot.tally_payload("BTC-5M", "2026-11-08", "2026-11-08")["ready"] and len(calls) == asked, "kept: a second look reads nothing"
    # the chunk still filling is read again a minute on, the answer shown as it stood meanwhile
    bot.market.now = NOW + 6 * 60_000
    for k, (read, closes) in list(bot.tally_cache.items()):
        bot.tally_cache[k] = (read - m.TALLY_FRESH_S - 1, closes)
    again = bot.tally_payload("BTC-5M", "2026-11-08", "2026-11-08")
    assert again["ready"] and again["counts"]["missing"] == 1 and again["counts"]["total"] == 121, again["counts"]  # 10:00–10:05 over, its end candle not in the kept chunk yet
    await settle()
    assert bot.tally_payload("BTC-5M", "2026-11-08", "2026-11-08")["counts"]["missing"] == 0 and len(calls) > asked
    bot.market.now = NOW
    # Predict's own results where the bot holds them: a record Predict confirmed agrees, one it settled the other way is listed
    for i, (k, side, payout) in enumerate(((0, "up", 1.0), (6, "up", 1.0), (11, "down", 0.5))):
        start = DAY0 + k * 300_000
        bot.store.put(f"sim:w{i}|{side}|挂", {"market": f"w{i}", "slug": f"btc-updown-5m-{start // 1000}", "item": "BTC 5分涨跌", "kind": "updown", "key": "BTC-5M",
                                              "side": side, "label": "挂涨", "maker": True, "fair": 0.6, "opened": start - 60_000, "settle": {"window": "5m"},
                                              "price": 0.5, "shares": 10.0, "status": "settled", "filled": start, "edge": 0.1, "payout": payout,
                                              "settled": start + 600_000, "confirm": "confirmed" if i != 1 else "mismatch"})
    bot.store.put("sim:w3|up|挂", {**bot.store.get("sim:w0|up|挂"), "market": "w3", "slug": f"btc-updown-5m-{(DAY0 + 300_000) // 1000}", "confirm": ""})  # pre-settled only: not Predict's
    p = bot.tally_payload("BTC-5M", "2026-11-08", "2026-11-08")
    assert p["checks"] == {"n": 2, "agree": 1, "bad": [{"s": DAY0 + 6 * 300_000, "r": "d", "o": "u"}], "near": {"n": 0, "agree": 0}}, p["checks"]
    assert [(w.get("o"), w["r"]) for w in p["windows"] if w.get("o")] == [("u", "u"), ("u", "d"), ("s", "f")]
    # 1.144.0: 跟随还是反转 over the day's windows; 预挂成交的那一边 from the pre-open ledger (none on this bot until it has one)
    assert p["follow"] == m.tally_follow(day_rows()) and p["fills"]["windows"] == 0, (p["follow"], p["fills"])
    rows = ledger(FILLS)
    bot.preopen_rows = lambda: rows
    p = bot.tally_payload("BTC-5M", "2026-11-08", "2026-11-08")
    assert (p["fills"]["windows"], p["fills"]["one"]["won"], p["fills"]["one"]["decided"], p["fills"]["one"]["z"]) == (50, 10, 45, -3.73), p["fills"]
    assert p["fills"]["both"] == {"n": 4, "uneven": 3, "heavy_won": 2} and p["fills"]["unknown"] == 1 and p["checks"]["n"] == 2  # no result kept: nothing official
    assert bot.tally_fill_rows(m.tally_card("ETH-5M"), {DAY0 + k * 300_000 for k in range(120)}) == []  # another card's windows: none of these
    kept = ledger([(13, 10, 0, 0.49, "up"), (14, 0, 0, 0.49, ""), (15, 0, 10, 0.49, "split")])  # the result the ledger keeps reaches the stat; an unfilled record is none
    bot.preopen_rows = lambda: kept
    assert bot.tally_fill_rows(m.tally_card("BTC-5M"), {DAY0 + k * 300_000 for k in range(120)}) == [
        {"s": DAY0 + 13 * 300_000, "up": 10.0, "down": 0.0, "price": 0.49, "result": "u"}, {"s": DAY0 + 15 * 300_000, "up": 0.0, "down": 10.0, "price": 0.49, "result": "s"}]
    bot.preopen_rows = lambda: rows
    # the range: a future day is today; more than TALLY_MAX_DAYS days are cut to the last ones; yesterday and the last 7 days
    assert (bot.tally_payload("BTC-5M", "2026-12-01", "2026-12-05")["from"], bot.tally_payload("BTC-5M", "2026-12-01", "2026-12-05")["to"]) == ("2026-11-08", "2026-11-08")
    wide = bot.tally_payload("BTC-5M", "2026-09-01", "2026-11-08")
    assert (wide["from"], wide["to"]) == ("2026-10-09", "2026-11-08")
    assert (bot.tally_payload("BTC-5M", rng="yesterday")["from"], bot.tally_payload("BTC-5M", rng="yesterday")["to"]) == ("2026-11-07", "2026-11-07")
    assert (bot.tally_payload("BTC-5M", rng="7d")["from"], bot.tally_payload("BTC-5M", rng="7d")["to"]) == ("2026-11-02", "2026-11-08")
    assert (bot.tally_payload("BTC-5M", rng="30d")["from"], bot.tally_payload("BTC-5M", rng="30d")["to"]) == ("2026-10-10", "2026-11-08")  # 1.143.1
    await settle()
    two = bot.tally_payload("BTC-5M", "2026-11-07", "2026-11-08")  # a row a day: 11-07's every window flat here (the candles' base price), 11-08's as above
    assert two["ready"] and [(d["d"], d["total"], d["up"], d["down"], d["flat"]) for d in two["days"]] == [("2026-11-07", 288, 0, 0, 288), ("2026-11-08", 120, 60, 59, 1)], two["days"]
    assert two["range"] == "11-07 00:00 到 11-08 10:00" and two["counts"]["total"] == 408
    assert bot.tally_payload("nope")["key"] == "BTC-5M" and bot.tally_payload("BTC-5M", "bad", "date")["from"] == "2026-11-08"
    await settle()
    # 1.144.1: BNB's every window moved 0.005% — up or down all the same (it was 平或待定), each a close call
    assert bot.tally_payload("BNB-5M", "2026-11-08", "2026-11-08")["ready"] is False
    await settle()
    bnb = bot.tally_payload("BNB-5M", "2026-11-08", "2026-11-08")
    assert bnb["counts"] == {"total": 120, "up": 60, "down": 60, "flat": 0, "missing": 0, "near": 120}, bnb["counts"]
    assert all(w.get("n") == 1 for w in bnb["windows"]) and bnb["follow"]["pairs"] == 119 and bnb["follow"]["same"] == 0
    # a gap: the candles of a chunk not read (the read failed) — those windows 没数据, the reason said, the page not held
    broken.add("ETHUSDT")
    assert bot.tally_payload("ETH-5M", "2026-11-08", "2026-11-08")["ready"] is False
    await settle()
    gap = bot.tally_payload("ETH-5M", "2026-11-08", "2026-11-08")
    assert gap["ready"] and gap["counts"]["missing"] == 120 and gap["errors"] and "HTTP 500" in gap["errors"][0], gap
    # an index card reads Binance futures' index candles; a daily market's boundaries are the noon-ET candles themselves
    seen = []

    async def futures(path, **params):
        seen.append((path, params["pair"]))
        return candles(params["startTime"], params["limit"], bot.market.now)
    bot.binance_futures = futures
    assert bot.tally_payload("XAU-15M")["ready"] is False
    await settle()
    xau = bot.tally_payload("XAU-15M")
    assert xau["ready"] and xau["name"] == "黄金 15分" and seen and set(seen) == {("indexPriceKlines", "XAUUSDT")} and xau["source"] == "币安合约指数 1 分钟 K 线"
    assert xau["tie"] == float(m.IndexUpDownMarket.TIE) and xau["counts"]["total"] == 40  # 00:00–09:45: the windows over by 10:02
    day = m.tally_card("BTC-1D")
    days = m.tally_windows(day, BJ(11, 1, 0, 0), BJ(11, 9, 0, 0), NOW)
    assert [(s, e, a, b) for s, e, a, b in days][:1] == [(m.et_ms(2026, 10, 31, 12, 0, -4), m.et_ms(2026, 11, 1, 12, 0, -5), m.et_ms(2026, 10, 31, 12, 0, -4), m.et_ms(2026, 11, 1, 12, 0, -5))]
    assert len(days) == 7 and all(a == s and b == e for s, e, a, b in days) and m.tally_tie(day) == 0.0 and m.tally_name(day) == "BTC 日"
    hour = m.tally_card("BTC-1H")
    hours = m.tally_windows(hour, DAY0, DAY0 + 86_400_000, NOW)
    assert len(hours) == 10 and all(a == s - 60_000 and b == e - 60_000 for s, e, a, b in hours) and m.tally_name(hour) == "BTC 小时"
    # the route: journal.json?tally=…, not cached (it turns ready within seconds); the page's section
    web = m.WebServer(bot, 0, TOKEN)
    web.CACHE_SECONDS = {}
    status, ctype, body = web.route("GET", f"/p/{TOKEN}/journal.json", "tally=BTC-5M&range=today")
    got = json.loads(body)
    assert status == 200 and ctype.startswith("application/json") and got["ready"] and got["counts"]["up"] == 60 and any(c["key"] == "BNB-1D" for c in got["cards"])
    page = m.JOURNAL_PAGE
    for want in ('id="tallybox"', 'id="tcard"', 'data-r="7d"', "function tallyDots(", "function tallyRender(", "市场涨跌统计", "range="):
        assert want in page, want
    await browser_check(bot)
    print("TALLY_OK")


async def browser_check(bot):
    """The section on the page: the dots (a dot a window, a row an hour), the counts, the range, the run lines, the mismatch."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("browser check skipped (no Playwright)")
        return
    chrome = next((p for p in [os.environ.get("CHROMIUM_PATH", ""), "/opt/pw-browsers/chromium-1194/chrome-linux/chrome"] if p and os.path.exists(p)), "")
    if not (chrome or os.environ.get("PLAYWRIGHT_BROWSERS_PATH")):
        print("browser check skipped (no Chromium)")
        return
    web = m.WebServer(bot, 0, TOKEN)
    web.CACHE_SECONDS = {}
    port = await web.start()
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(**({"executable_path": chrome} if chrome else {}))
            page = await browser.new_page(viewport={"width": 390, "height": 900})
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            await page.goto(f"http://127.0.0.1:{port}/p/{TOKEN}/journal")
            await page.wait_for_selector("#tally .tdots .tdot")
            assert await page.locator("#tally .tdots .tdot").count() == 120
            assert await page.locator("#tally .tdots .tdot.bad").count() == 1 and await page.locator("#tally .tdots .trow").count() == 10
            text = await page.locator("#tally").inner_text()
            for want in ("统计区间 11-08 00:00–10:00", "已结束 120 场", "涨 60 局", "跌 59 局", "平 1 局", "涨占 50%", "最长连涨 6 局（00:00–00:30）",
                         "最长连跌 5 局（00:30–00:55）", "与 Predict 官方结果核对 2 场：一致 1 · 不一致 1 场（00:30 K 线跌、官方涨）", "按小时"):
                assert want in text, (want, text)
            assert await page.locator("#tcard option").count() == len(m.preopen_cards()) and await page.input_value("#tcard") == "BTC-5M"
            for want in ("跟随还是反转", "上一场涨之后，下一场涨 5/60（8%）· 上一场跌之后，下一场跌 4/57（7%）", "同向 9 次、反向 108 次；这段涨占 50%，如果每场互不相干，同向该有 58.5 次，实际偏离 −9.2σ → 偏反转（明显，超过 3σ）：涨跌容易交替",
                         "连涨 4 场以上", "预挂成交的那一边", "只成交一边、分出结果的 45 场里，成交那边赢 10 场；随机成交该赢 22.5 场，实际偏离 −3.7σ → 有逆向选择（明显，超过 3σ）",
                         "这些成交均价 48.9¢：成交那边赢的占比要高过 48.9% 才赚钱", "45 场还没有官方结果，按 K 线算", "另有 1 场结果还没定，没算进去",
                         "两边都成交的 4 场两边对冲，不算输赢（其中两边不一样多的 3 场，多的那边赢 2 场）", "按价格档"):  # 1.144.0
                assert want in text, (want, text)
            assert await page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"), "no sideways scroll with the new tables"
            # 1.143.1: a dot tapped says its window; only the mismatches on request; a range of days: a row a day, the dots of the day picked
            await page.locator("#tally .tdots .tdot").nth(6).click()
            assert "00:30–00:35 跌：106 → 105（-0.943%）；Predict 官方 涨（和 K 线不一致）" in await page.inner_text("#tdetail"), await page.inner_text("#tdetail")
            await page.click("#tally button:has-text('只看和官方不一致的')")
            assert await page.locator("#tally .tdots.only").count() == 1
            await page.click("#tally button:has-text('显示全部场次')")
            await page.click("#tallybody .tq[data-r='7d']")
            await page.locator("#tally .tdays tr.pick").nth(6).wait_for()
            assert await page.locator("#tally .tdays tr.pick").count() == 7 and "11-08 周日" in await page.locator("#tally .tdays tr.on").inner_text()
            assert await page.locator("#tally .tdots .tdot").count() == 120 and "11-08 的每一场" in await page.locator("#tally").inner_text()
            await page.locator("#tally .tdays tr.pick").nth(5).click()  # 11-07
            await page.locator("#tally .tdots .tdot.f").nth(287).wait_for()
            assert await page.locator("#tally .tdots .tdot").count() == 288 and "11-07 周六" in await page.locator("#tally .tdays tr.on").inner_text()
            assert "开始和结束完全一样才记平；涨跌不到 0.01% 的照涨跌算，画浅色" in text and "涨跌不到 0.01%" in await page.inner_text("#tally .tlegend")  # 1.144.1
            assert await page.locator("#tally .tdots .tdot.n").count() == 0 and "浅色圆点" not in text
            await page.select_option("#tcard", "BNB-5M")  # 1.144.1: every window a close call — up or down, drawn light
            await page.click("#tallybody .tq[data-r='today']")
            await page.locator("#tally .tdots .tdot.n").nth(119).wait_for()
            bnb = await page.locator("#tally").inner_text()
            assert "已结束 120 场" in bnb and "涨 60 局" in bnb and "跌 60 局" in bnb and "平 0 局" in bnb, bnb
            assert "其中 120 场涨跌不到 0.01%（浅色圆点）：Predict 按 Chainlink 的价格结算，差距这么小时官方结果偶尔会和 K 线相反" in bnb, bnb
            await page.locator("#tally .tdots .tdot").nth(0).click()
            assert "00:00–00:05 涨：600 → 600.03（+0.005%），涨跌不到 0.01%，官方结果可能相反；机器人手里没有这一场的官方结果" in await page.inner_text("#tdetail")
            assert await page.evaluate("getComputedStyle(document.querySelector('#tally .tdots .tdot.n')).opacity") == "0.42"
            await page.select_option("#tcard", "ETH-5M")  # the gap: every dot hollow, the reason said
            await page.locator("#tally .tdots .tdot.x").nth(119).wait_for()  # (the page's CSP refuses wait_for_function's string)
            assert "有一部分 K 线没读到" in await page.locator("#tally").inner_text()
            await page.click("#tallybody .tq[data-r='yesterday']")
            for _ in range(100):
                if await page.input_value("#tfrom") == "2026-11-07":
                    break
                await asyncio.sleep(0.05)
            assert await page.input_value("#tfrom") == "2026-11-07" and await page.input_value("#tto") == "2026-11-07"
            assert await page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"), "no sideways scroll"
            assert not errors, errors
            await browser.close()
    finally:
        await web.stop()


asyncio.run(run())
