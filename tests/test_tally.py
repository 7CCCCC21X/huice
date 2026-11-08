"""Offline checks of the review page's 市场涨跌统计 (1.143.0): each window's up or down by Binance's 1-minute candles —
the counts, the runs, the hours, the ties and gaps, Predict's own results against the candles', the range, the page.

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


def candles(start: int, limit: int, now: int) -> list:
    """Binance's answer: the candles from ``start``, the one still open included (its close time ahead of now)."""
    out = []
    for i in range(limit):
        o = start + i * 60_000
        if o > now:
            break
        out.append([o, "0", "0", "0", str(close_at(o)), "0", o + 59_999, "0", 0, "0", "0", "0"])
    return out


async def run():
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"})
    bot = m.Bot(cfg, m.Store(":memory:"), FM(NOW), None)
    calls, broken = [], set()

    async def spot(path, **params):
        calls.append((path, params["symbol"], params["startTime"], params["limit"]))
        if params["symbol"] in broken:
            raise m.RemoteError("HTTP 500: boom")
        return candles(params["startTime"], params["limit"], bot.market.now)
    m.binance_spot = spot

    async def settle():
        await asyncio.gather(*list(bot.tally_tasks.values()), return_exceptions=True)

    # the first ask reads the candles in the background; the second answers
    first = bot.tally_payload("BTC-5M", "2026-11-08", "2026-11-08")
    assert first["ready"] is False and "正在读币安 1 分钟 K 线" in first["message"] and first["name"] == "BTC 5分", first
    await settle()
    assert {c[:2] for c in calls} == {("klines", "BTCUSDT")} and all(c[3] == 1000 for c in calls) and all(c[2] % m.TALLY_CHUNK_MS == 0 for c in calls), calls
    p = bot.tally_payload("BTC-5M", "2026-11-08", "2026-11-08")
    assert p["ready"] and p["counts"] == {"total": 120, "up": 60, "down": 59, "flat": 1, "missing": 0}, p["counts"]
    assert p["range"] == "11-08 00:00–10:00" and (p["from"], p["to"], p["today"]) == ("2026-11-08", "2026-11-08", "2026-11-08"), p["range"]
    assert p["streaks"]["up"] == {"n": 6, "from": DAY0, "to": DAY0 + 30 * 60_000} and p["streaks"]["down"] == {"n": 5, "from": DAY0 + 30 * 60_000, "to": DAY0 + 55 * 60_000}
    assert [w["r"] for w in p["windows"][:13]] == ["u"] * 6 + ["d"] * 5 + ["f", "u"] and p["windows"][0]["a"] == 100.0 and p["windows"][0]["b"] == 101.0
    assert [(h["h"], h["up"], h["down"], h["flat"]) for h in p["hours"][:2]] == [(0, 6, 5, 1), (1, 6, 6, 0)] and len(p["hours"]) == 10
    assert p["checks"] == {"n": 0, "agree": 0, "bad": []} and p["source"] == "币安现货 1 分钟 K 线" and p["tie"] == float(m.SpotWindowMarket.TIE)
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
    assert p["checks"] == {"n": 2, "agree": 1, "bad": [{"s": DAY0 + 6 * 300_000, "r": "d", "o": "u"}]}, p["checks"]
    assert [(w.get("o"), w["r"]) for w in p["windows"] if w.get("o")] == [("u", "u"), ("u", "d"), ("s", "f")]
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
            for want in ("统计区间 11-08 00:00–10:00", "已结束 120 场", "涨 60 局", "跌 59 局", "平或待定 1 局", "涨占 50%", "最长连涨 6 局（00:00–00:30）",
                         "最长连跌 5 局（00:30–00:55）", "与 Predict 官方结果核对 2 场：一致 1 · 不一致 1 场（00:30 K 线跌、官方涨）", "按小时"):
                assert want in text, (want, text)
            assert await page.locator("#tcard option").count() == len(m.preopen_cards()) and await page.input_value("#tcard") == "BTC-5M"
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
