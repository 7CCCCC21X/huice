"""The trades' review page (/p/<token>/journal): the tiles (today, 7 days, lifetime, open), the folded statistics and
risks, filters with counts, the list by day (light rows: the open trades and the last JOURNAL_HISTORY finished ones,
the rest on request), one trade's whole record fetched when it is opened (why the model priced it, the prices behind
it, proxy and anchor, how it filled, how it was settled, version), deep links, and the CSV / JSON exports. The browser
part needs Playwright + Chromium and is skipped without them."""
import asyncio, csv, io, json, os, sys, urllib.parse, urllib.request, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP, except to 127.0.0.1)
import main as m
D = m.D
BJ = lambda mo, d, h, mi=0: int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
NOW = BJ(10, 5, 10, 0)
CLOSE = BJ(10, 5, 16, 10)
HSI_SLUG = "hang-seng-index-up-or-down-on-october-5-2026"
KOSPI_SLUG = "kospi-composite-index-up-or-down-on-october-5-2026"
TOKEN = "t" * 20


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def book(bids, asks, at, slug, key, mid):
    return m.PredictBook(key, slug, mid, "t", tuple((D(p), D(q)) for p, q in bids), tuple((D(p), D(q)) for p, q in asks), at, None)


EVIDENCE = {"basis": {"fair_up": 0.70, "ref": 24600.0, "ref_note": "10-02 收盘", "effective": 24702.5, "sigma_daily": 0.012,
                      "remaining": 0.5, "beta": 1.0, "mode": "盘后", "direct": False, "close_ms": CLOSE},
            "sources": [{"what": "恒指期货", "source": "etnet", "symbol": "恒指期货(10/2026)", "type": "期货最新价", "price": 24702.0,
                         "quoted_ms": NOW - 20_000, "fetched_ms": NOW - 5_000},
                        {"what": "参考收盘", "source": "tencent 日K", "symbol": "HSI", "type": "收盘", "price": 24600.0}],
            "proxy": {"proxy": "恒指期货", "contract": "10/2026", "anchor": 24500.0, "anchor_note": "16:10 现货收市时", "approx": False}}


async def build():
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                             "SIM_WAYS": "both", "SIM_MARKETS": "all", "SIM_MAKER_MIN_BID": "0", "SIM_MAKER_EXIT_CENTS": "0", "SIM_MAKER_SESSION": "off", "SIM_MAKER_POINTS": "off", "SIM_MAKER_SPREAD_CENTS": "10", "SIM_TAKER_MAX_SHARES": "0"})  # the review page is checked with maker trades too (resting as placed)
    bot = m.Bot(cfg, m.Store(":memory:"), FM(NOW), None)
    world = {"markets": []}
    bot.sim_markets = lambda now: world["markets"]
    answers = {}

    async def details(mid):
        return answers.get(mid, {"outcomes": ["Up", "Down"], "status": "REGISTERED", "resolved": None})
    bot.predict.market_details = details

    async def step(at, *markets):
        bot.market.now, bot.sim_ran = at, -1e9
        world["markets"] = list(markets)
        bot.sim_checked.clear()
        await bot.sim_step(at)

    hsi = lambda fair, bids, asks, at: m.SimMarket(HSI_SLUG, "恒生指数", "close", "HSI", fair, book(bids, asks, at, HSI_SLUG, "HSI", "101"),
                                                   0.03, "", ("涨", "跌"), {"key": "HSI", "target": "2026-10-05", "line": 24600.0,
                                                                         "close_ms": CLOSE}, EVIDENCE)
    kospi = lambda bids, asks, at: m.SimMarket(KOSPI_SLUG, "KOSPI", "close", "KOSPI", 0.20, book(bids, asks, at, KOSPI_SLUG, "KOSPI", "102"),
                                               0.02, "", ("涨", "跌"), {"key": "KOSPI", "target": "2026-10-05", "line": 3300.0,
                                                                     "close_ms": BJ(10, 5, 14, 30)}, {"basis": {"fair_up": 0.2}})
    await step(NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW), kospi([("0.35", "30"), ("0.30", "500")], [("0.38", "100")], NOW))
    await step(NOW + 60_000, hsi(0.66, [("0.54", "10")], [("0.55", "30")], NOW + 60_000))
    await step(NOW + 120_000, hsi(0.60, [("0.52", "10")], [("0.53", "500")], NOW + 120_000))
    bot.note_outcome("HSI", "2026-10-05", 24650.0, "tencent 日K")
    bot.note_outcome("KOSPI", "2026-10-05", 3300.0, "Yahoo ^KS11 日K")
    await step(CLOSE + 61 * 60_000)
    answers["101"] = {"outcomes": ["Up", "Down"], "status": "RESOLVED", "resolved": {"index": 0, "name": "Up", "split": False, "how": "outcomes.status"}}
    answers["102"] = {"outcomes": ["Up", "Down"], "status": "RESOLVED", "resolved": {"index": 1, "name": "Down", "split": False, "how": "outcomes.status"}}
    await step(CLOSE + 72 * 60_000)
    # an open order on another market, and an old record kept before evidence was saved
    flip = m.SimMarket("hype-flip#1", "HYPE 反超 SOL", "flip", "HYPE", 0.30, book([("0.20", "200")], [("0.30", "50")], CLOSE + 73 * 60_000,
                       "will-hype-flip-sol-by-nov-26", "HYPE", "300"), 0.02, "", ("Yes", "No"), {"end": BJ(11, 1, 11, 59)},
                       {"basis": {"a": 41.2, "b": 160.5}, "sources": [{"what": "HYPE", "source": "Hyperliquid", "price": 41.2}]})
    await step(CLOSE + 73 * 60_000, flip)
    bot.store.put("sim:old|up|挂", {"market": "old", "slug": "old-slug", "item": "上证指数", "kind": "close", "key": "SSE", "side": "up",
                                   "label": "挂涨", "maker": True, "fair": 0.62, "signal": 0.12, "opened": NOW - 86_400_000, "settle": {},
                                   "price": 0.50, "shares": 100, "status": "settled", "filled": NOW - 80_000_000, "fill_fair": 0.58,
                                   "edge": 0.12, "payout": 0.0, "settled": NOW - 3_600_000, "note": "10-04 收盘 3,849，对 3,850"})
    return bot


async def run():
    bot = await build()
    trades = bot.sim_trades()
    hsi_maker = f"{HSI_SLUG}|up|挂"
    assert trades[hsi_maker]["confirm"] == "confirmed" and trades[f"{KOSPI_SLUG}|down|吃"]["confirm"] == "mismatch"
    assert trades["hype-flip#1|up|挂"]["status"] == "resting" and len(trades) == 6, sorted(trades)
    # the routes: the page, its data and the two exports; the token guards them all
    web = m.WebServer(bot, 0, TOKEN); web.CACHE_SECONDS = {}
    status, ctype, body = web.route("GET", f"/p/{TOKEN}/journal")
    assert status == 200 and ctype.startswith("text/html") and "交易复盘" in body.decode()
    assert 'id="ctl"' in body.decode() and 'href=base+"/control"' in body.decode()  # the control page is one tap away from the review page
    status, _, page = web.route("GET", f"/p/{TOKEN}")
    assert status == 200 and 'id="ctl"' in page.decode() and 'id="jrn"' in page.decode()  # ...and from the odds page's header (the phone too)
    status, ctype, body = web.route("GET", f"/p/{TOKEN}/journal.json")
    data = json.loads(body)
    assert status == 200 and ctype.startswith("application/json") and len(data["trades"]) == 6 and data["total"]["mismatch"] == 1
    waits = {t["id"]: t["wait"] for t in data["trades"]}  # every open trade says what it waits for; a settled one nothing
    assert waits["hype-flip#1|up|挂"] == "挂 20.0¢，最低卖价 30.0¢（高出 10.0¢）：要有人卖到挂价或更低才算成交；现在公平价 30.0¢", waits
    assert waits[hsi_maker] == "" and data["total"]["cancelled"] == 1  # the KOSPI maker, nothing filled, withdrawn at the close (1.54.3)
    assert trades[f"{KOSPI_SLUG}|down|挂#1"]["status"] == "cancelled" and trades[f"{KOSPI_SLUG}|down|挂#1"]["withdrawn"]["why"] == "已到结束时间，等结果出来：不开新单，挂单撤掉"
    # the list rows are light (1.59.1): the whole record comes when a trade is opened (?id=) and for the exports (?full=1)
    assert all("entry" not in t and "fills" not in t for t in data["trades"]) and data["more"] == 0 and data["mode"] == "paper" and data["history"] == 300
    assert [r["name"] for r in data["recent"]] == ["今天", "近 7 天"] and data["recent"][0]["settled"] == data["total"]["settled"] > 0
    # 1.80.0: the daily summary — per day the records were opened on, by kind and way under each, and 累计
    s = data["summary"]
    assert [r["label"] for r in s] == ["10-05 周一", "10-04 周日", "累计"], [r["label"] for r in s]
    assert s[0]["records"] == 5 and s[0]["markets"] == 3 and s[0]["settled"] == 3 and s[0]["open"] == 0 and s[0]["resting"] == 1 and s[0]["filled"] == 430 and abs(s[0]["volume"] - 251.23) < 1e-6, s[0]
    assert [p["name"] for p in s[0]["parts"]] == ["指数/个股日涨跌", "反超", "挂单", "吃单"], [p["name"] for p in s[0]["parts"]]
    assert sum(p["records"] for p in s[0]["parts"][:2]) == 5 and sum(p["records"] for p in s[0]["parts"][2:]) == 5
    assert s[1]["records"] == 1 and s[1]["losses"] == 1 and abs(s[1]["pnl"] + 50) < 1e-9 and s[1]["parts"][0]["name"] == "指数/个股日涨跌"
    assert s[-1]["day"] == "" and s[-1]["records"] == 6 and abs(s[-1]["pnl"] - data["total"]["pnl"]) < 1e-9 and s[-1]["settled"] == data["total"]["settled"]
    assert m.sim_summary([]) == [] and 'id="summary"' in web.route("GET", f"/p/{TOKEN}/journal")[2].decode() and "jrnsum" in m.JOURNAL_PAGE
    # 1.81.0: the analysis tables (per underlying, per edge / fair bucket, per session part, the makers, the refused buys), the
    # marks, the summary CSV and the daily report
    hsi_mk = m.SimMarket(HSI_SLUG, "恒生指数", "close", "HSI", 0.70, book([("0.55", "300")], [("0.58", "400")], NOW, HSI_SLUG, "HSI", "101"),
                         0.03, "", ("涨", "跌"), {"key": "HSI", "target": "2026-10-05", "line": 24600.0, "close_ms": CLOSE}, EVIDENCE)
    bot.sim_note_block(f"{HSI_SLUG}|down|吃", hsi_mk, "吃跌", 0.40, "收涨时这组仓位合计将亏 $350.00，超过组上限 $300", NOW + 30_000)
    data = json.loads(web.route("GET", f"/p/{TOKEN}/journal.json")[2])
    a = data["analysis"]
    names = [r["name"] for r in a["items"]]
    assert names == ["恒生指数", "KOSPI", "上证指数", "HYPE"] and sum(r["records"] for r in a["items"]) == 6, a["items"]  # the most traded first
    hsi_row = next(r for r in a["items"] if r["name"] == "恒生指数")
    assert hsi_row["settled"] == hsi_row["wins"] == 2 and hsi_row["hit"] == 1.0 and abs(hsi_row["avg_fair"] - 0.7) < 1e-9 and abs(hsi_row["expected"] - 48.48) < 1e-6, hsi_row
    assert [(r["name"], r["records"]) for r in a["edges"]] == [("10–15¢", 4), ("15–20¢", 2)], a["edges"]  # an edge of 0.0999…98 is 10¢; 15¢ starts the next bucket
    assert [(r["name"], r["records"]) for r in a["fairs"]] == [("30–50¢", 1), ("50–70¢", 1), ("70–90¢", 4)], a["fairs"]
    assert [(r["name"], r["records"]) for r in a["sessions"]] == [("盘中", 2), ("休市日", 3), ("无交易时段", 1)], a["sessions"]  # 10-05 is a KRX holiday; the old record a Sunday
    assert "markout" in a["sessions"][0] and a["sessions"][1]["settled"] == 2 and a["sessions"][1]["wins"] == a["sessions"][1]["losses"] == 1
    mk = a["makers"]
    assert mk["placed"] == 4 and mk["filled"] == 2 and mk["partial"] == 0 and mk["unfilled"] == 1 and mk["resting"] == 1 and mk["cancelled"] == 1 and mk["expired"] == 0, mk
    assert abs(mk["fill_rate"] - 2 / 3) < 1e-9 and mk["wait_n"] == 2 and mk["wait_median"] == 6_400_000 and mk["rest_n"] == 1, mk
    assert mk["reasons"] == [{"why": "已到结束时间，等结果出来：不开新单，挂单撤掉", "n": 1}], mk["reasons"]
    bl = a["blocked"]
    assert bl["n"] == 1 and bl["settled"] == 1 and bl["losses"] == 1 and bl["shares"] == 100 and abs(bl["pnl"] + 40) < 1e-9 and bl["rows"][0]["payout"] == 0, bl
    assert data["marked"] == 0 and data["unrealized"] == 0 and data["summary"][0]["marked"] == 0 and all("mark" in t for t in data["trades"])  # nothing held: nothing to mark
    flip_mk = next(iter(bot.sim_markets(NOW)))  # the HYPE flip market: 买1 20¢, 卖1 30¢
    marks = bot.sim_marks({flip_mk.market: flip_mk}, CLOSE + 73 * 60_000)
    assert marks == {("hype-flip#1", "up"): 0.25, ("hype-flip#1", "down"): 0.75}, marks
    assert bot.sim_marks({flip_mk.market: flip_mk}, CLOSE + 73 * 60_000 + m.LIVE_STALE_MS + 1) == {}  # a stale book marks nothing
    assert m.book_mark(book([("0.20", "1")], [], NOW, "x", "X", "1"), "up") is None  # nor a one-sided one
    held = {"market": "hype-flip#1", "side": "up", "status": "filled", "shares": 50, "price": 0.28, "payout": None}
    s1 = m.summary_of([held], marks)
    assert s1["open"] == 1 and s1["marked"] == 1 and abs(s1["unrealized"] + 1.5) < 1e-9 and m.summary_of([held])["marked"] == 0, s1
    assert m.bucket_of(0.10, m.EDGE_BUCKETS) == "10–15¢" and m.bucket_of(0.0999, m.EDGE_BUCKETS) == "<10¢" and m.bucket_of(0.95, m.FAIR_BUCKETS) == "≥90¢"
    # the summary CSV: one file, the table's name first
    status, ctype, body = web.route("GET", f"/p/{TOKEN}/summary.csv")
    text = body.decode("utf-8")
    assert status == 200 and ctype == "text/csv; charset=utf-8" and text.startswith("\ufeff表,行,记录,市场"), text[:40]
    srows = list(csv.reader(io.StringIO(text[1:])))
    tables = [r[0] for r in srows[1:]]
    assert tables[0] == "每日汇总" and {"按标的", "按下单时净优势", "按下单时公平价", "按时段", "挂单效率", "挂单撤单原因", "被拦下的机会"} <= set(tables), sorted(set(tables))
    assert next(r for r in srows if r[1] == "累计")[2] == "6" and next(r for r in srows if r[0] == "挂单效率" and r[1] == "挂出")[2] == "4"
    assert next(r for r in srows if r[0] == "按标的" and r[1] == "恒生指数")[18] == "1.0" and next(r for r in srows if r[0] == "被拦下的机会")[10] == "-40.0"
    assert 'id="scsv"' in web.route("GET", f"/p/{TOKEN}/journal")[2].decode() and "jrnana" in m.JOURNAL_PAGE
    # the daily report: once per day and subscription, from SIM_DAILY_REPORT (16:30) on, within six hours of it
    sent = []

    async def fake_tell(chat, thread, text, reply_markup=None, html_mode=False, fresh=None, render=None):
        sent.append((chat, thread, text))
        return True
    bot.tell = fake_tell
    bot.store.put("subscriptions", {"1:0": {"chat": 1, "thread": 0, "active": True}, "2:0": {"chat": 2, "thread": 0, "active": False}})
    at = CLOSE + 73 * 60_000  # 17:23 Beijing
    await bot.daily_report(BJ(10, 5, 16, 0)); await bot.drain_deliveries()
    assert sent == []  # not due yet
    await bot.daily_report(at); await bot.drain_deliveries()
    assert len(sent) == 1 and sent[0][:2] == (1, 0), sent
    report = sent[0][2]
    for want in ("模拟交易日报", "10-05 周一", "今天下单 5 笔（挂单 3 笔、吃单 2 笔）｜3 个市场｜成交 430 份 $251.23", "今天最多：恒生指数 2 笔、KOSPI 2 笔、HYPE 1 笔",
                 "今天结算 4 笔：赢 3 输 1｜盈亏 +$128.77", "持仓 0 笔 成本 $0.00｜挂单中 1", "累计 结算 4 笔 赢 3 输 1｜盈亏 +$128.77｜收益率 +42.7%", "挂单成交率 67%（4 笔挂单"):
        assert want in report, (want, report)
    assert "复盘页" not in report  # no web port in this test
    assert bot.store.get("simreport:2026-10-05:1:0") and not bot.store.get("simreport:2026-10-05:2:0")
    await bot.daily_report(at + 60_000); await bot.drain_deliveries()
    assert len(sent) == 1  # once a day
    keep = bot.config
    bot.store.put("subscriptions", {"3:0": {"chat": 3, "thread": 0, "active": True}})
    bot.config = m.dataclasses.replace(keep, sim_daily_report="")
    await bot.daily_report(at); await bot.drain_deliveries()
    assert len(sent) == 1  # off
    bot.config = m.dataclasses.replace(keep, sim_daily_report="09:00")
    await bot.daily_report(at); await bot.drain_deliveries()
    assert len(sent) == 1  # 09:00 was more than six hours ago: the day's report is skipped, not sent late
    bot.config = m.dataclasses.replace(keep, sim_daily_report="12:00")
    await bot.daily_report(at); await bot.drain_deliveries()
    assert len(sent) == 2 and sent[1][0] == 3  # within the window: the bot was down at noon, the report goes now
    bot.config = keep
    bot.store.put("subscriptions", {})
    empty = m.Bot(keep, m.Store(":memory:"), FM(at), None); empty.tell = fake_tell  # a bot with no record at all: nothing to say
    empty.store.put("subscriptions", {"1:0": {"chat": 1, "thread": 0, "active": True}})
    await empty.daily_report(at); await empty.drain_deliveries()
    assert len(sent) == 2 and empty.store.get("simreport:2026-10-05:1:0") == {"skipped": at} and empty.daily_report_text(at) == ""
    env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT"}
    assert m.Config.from_env(env).sim_daily_report == "16:30" and m.Config.from_env({**env, "SIM_DAILY_REPORT": "off"}).sim_daily_report == ""
    assert m.Config.from_env({**env, "SIM_DAILY_REPORT": "7:05"}).sim_daily_report == "07:05"
    for bad in ("25:00", "16:60", "noon"):
        try:
            m.Config.from_env({**env, "SIM_DAILY_REPORT": bad})
            raise AssertionError(f"{bad} accepted")
        except ValueError:
            pass
    status, _, body = web.route("GET", f"/p/{TOKEN}/journal.json", "full=1")
    full = json.loads(body)
    assert status == 200 and len(full["trades"]) == 6 and sum("entry" in t for t in full["trades"]) == 5 and full["more"] == 0
    status, _, body = web.route("GET", f"/p/{TOKEN}/journal.json", "id=" + urllib.parse.quote(hsi_maker, safe=""))
    one = json.loads(body)["trade"]
    assert status == 200 and one["id"] == hsi_maker and one["entry"]["basis"]["ref"] == 24600.0 and one["fills"] and one["real"] is False
    assert web.route("GET", f"/p/{TOKEN}/journal.json", "id=nope")[0] == 404
    bot.JOURNAL_HISTORY = 2  # the page lists the open trades and the last two finished ones; ?all=1 every one
    part = json.loads(web.route("GET", f"/p/{TOKEN}/journal.json")[2])
    assert [t["id"] for t in part["trades"]][:1] == ["hype-flip#1|up|挂"] and len(part["trades"]) == 3 and part["more"] == 3 and part["history"] == 2, part["trades"]
    assert len(json.loads(web.route("GET", f"/p/{TOKEN}/journal.json", "all=1")[2])["trades"]) == 6
    bot.JOURNAL_HISTORY = m.Bot.JOURNAL_HISTORY
    status, ctype, body = web.route("GET", f"/p/{TOKEN}/journal.csv")
    text = body.decode("utf-8")
    assert status == 200 and ctype == "text/csv; charset=utf-8" and text.startswith("﻿编号,下单时间,市场")
    rows = list(csv.reader(io.StringIO(text[1:])))
    assert len(rows) == 7
    row = dict(zip(rows[0], next(r for r in rows if r[0] == f"{HSI_SLUG}|up|挂")))
    assert row["参考线"] == "24600.0" and row["口径"] == "盘后" and row["下单距收盘（小时）"] == "6.17" and row["最旧报价年龄（秒）"] == "20.0", row
    assert row["代理与锚点"] == "恒指期货；锚点 24500.0（16:10 现货收市时）" and "恒指期货=etnet 恒指期货(10/2026) @2026-10-05 09:59:40" in row["行情来源"]
    assert web.route("GET", f"/p/{'x' * 20}/journal")[0] == 404 and web.route("GET", f"/p/{TOKEN}/journal.xml")[0] == 404
    await browser_check(bot, data)
    print("JOURNAL_OK")


async def browser_check(bot, data):
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        async_playwright = None
    chrome = next((p for p in [os.environ.get("CHROMIUM_PATH", ""), "/opt/pw-browsers/chromium-1194/chrome-linux/chrome"]
                   if p and os.path.exists(p)), "")
    if async_playwright is None or not (chrome or os.environ.get("PLAYWRIGHT_BROWSERS_PATH")):
        print("browser check skipped (no Playwright/Chromium)")
        return
    web = m.WebServer(bot, 0, TOKEN); web.CACHE_SECONDS = {}; port = await web.start()
    url = f"http://127.0.0.1:{port}/p/{TOKEN}/journal"
    hsi_maker, old = f"{HSI_SLUG}|up|挂", "old|up|挂"
    bot.JOURNAL_HISTORY = 2  # the page lists the open trades and the last two finished ones; the rest on request
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**({"executable_path": chrome} if chrome else {}))
        page = await browser.new_page(viewport={"width": 390, "height": 900})
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        await page.goto(url)
        await page.wait_for_selector("#list .tr")
        assert await page.get_attribute("#back", "href") == f"/p/{TOKEN}"
        # the exports are addressed from the page's own path, so they also work when the page was opened as /journal/
        assert await page.get_attribute("#csv", "href") == f"/p/{TOKEN}/journal.csv" and await page.get_attribute("#json", "href") == f"/p/{TOKEN}/journal.json?full=1"
        assert await page.inner_text("#title") == "模拟交易复盘"  # a bot without a trading key: paper
        tiles = await page.inner_text("#tiles")
        tot = data["total"]
        for want in ("今天", "近 7 天", "累计", f"已结算 {tot['settled']} 笔", f"结果不一致 {tot['mismatch']}", "进行中", "挂单中 1"):
            assert want in tiles, (want, tiles)
        # the statistics, the groups and the common risks are folded away; one tap shows them
        assert await page.is_hidden("#statsbody") and "模型预期" in await page.inner_text("#statsum")
        await page.click("#statshd")
        assert await page.is_visible("#statsbody") and "模型预期（下单时）" in await page.inner_text("#statsbody") and await page.is_visible("#groups")
        await page.click("#statshd")
        assert await page.is_hidden("#statsbody")
        # three rows at first (the resting order and the last two finished ones), every one on request, by day
        assert await page.locator("#list .tr").count() == 3 and "更早的 3 笔可载入" in await page.inner_text("#meta")
        await page.click("#more button:has-text('载入更早的 3 笔')")
        await page.wait_for_selector("#list .tr >> nth=5")
        assert await page.locator("#list .tr").count() == 6 and await page.locator("#list .day").count() == 2  # 10-05, and the old record's 10-04
        # filters: by how the result stands, by way of trading; a chip nobody matches is not shown; 全部 clears them all
        await page.click("#filters [data-f=bad]")
        assert await page.locator("#list .tr").count() == 1
        await page.click("#filters [data-f=exp]")
        assert await page.locator("#list .tr").count() == 1  # the KOSPI maker: no seller ever reached it
        await page.click("#filters [data-f=rest]")
        assert await page.locator("#list .tr").count() == 1 and "⏳ 挂 20.0¢，最低卖价 30.0¢（高出 10.0¢）" in await page.inner_text("#list .tr .t3")
        assert await page.locator("#filters [data-f=open]").count() == (1 if any(t["status"] == "filled" for t in data["trades"]) else 0)
        await page.click("#filters [data-f=all]")
        await page.click("#filters [data-f=taker]")
        assert await page.locator("#list .tr").count() == 2
        await page.click("#filters [data-f=taker]")  # the same chip again: off
        assert await page.locator("#list .tr").count() == 6
        # one trade's whole record, fetched when it is opened
        await page.click(f"#list .tr:has-text('恒生指数 挂涨') button.row")
        det = page.locator("#list .tr.open .det")
        await det.locator("h3").first.wait_for()
        heads = await det.locator("h3").all_inner_texts()
        assert heads == ["判断依据（下单时）", "判断依据（成交时）", "行情来源", "代理与锚点", "成交证据", "结算证据", "版本与设置"], heads
        text = await det.inner_text()
        for want in ("建议门槛", "3.0¢", "etnet", "恒指期货(10/2026)", "16:10 现货收市时", "推定成交", "排在挂单时该价位已有的 300 份之后",
                     "看到卖单：55.0¢×30", "本地预结算", "Predict 结果", "涨 / Yes · Up", "代码版本", m.VERSION, "下单时卡片上的四个方向"):
            assert want in text, (want, text)
        assert urllib.parse.unquote(await page.evaluate("location.hash"))[1:] == hsi_maker
        # a mismatch says so and keeps the revision
        await page.click(f"#list .tr:has-text('KOSPI 吃跌') button.row")
        await page.wait_for_selector("#list .tr.open .det h3")
        text = await page.locator("#list .tr.open .det").inner_text()
        assert "本地结果与 Predict 不一致，已按 Predict 的结果重新结算" in text and "Predict 最终结果" in text and "0.5 → 1" in text, text
        assert await page.locator("#list .tr.open").count() == 1  # one record open at a time
        # an old record says what it lacks
        await page.click(f"#list .tr:has-text('上证指数 挂涨') button.row")
        await page.wait_for_selector("#list .tr.open .det .note")
        assert "旧版本记下的交易" in await page.locator("#list .tr.open .det").inner_text()
        await page.click(f"#list .tr:has-text('HYPE 反超 SOL') button.row")
        await page.wait_for_selector("#list .tr.open .det h3")
        assert "现在等什么\n挂 20.0¢，最低卖价 30.0¢" in await page.locator("#list .tr.open .det").inner_text()
        assert await page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"), "no sideways scroll"
        if os.environ.get("WEB_SCREENSHOT"):
            await page.screenshot(path=os.environ["WEB_SCREENSHOT"], full_page=True)
        # a link to one trade opens it straight away — even one the first page does not list (the whole list is fetched)
        page2 = await browser.new_page(viewport={"width": 1100, "height": 900})
        await page2.goto(url + "#" + urllib.parse.quote(old, safe=""))
        await page2.wait_for_selector("#list .tr.open .det .note")
        assert "上证指数 挂涨" in await page2.inner_text("#list .tr.open .row") and await page2.locator("#list .tr").count() == 6
        # the exports, as a browser fetches them
        csv_text = await page2.evaluate("fetch('journal.csv').then(r => r.text())")
        assert csv_text.startswith("﻿编号") or csv_text.startswith("编号"), csv_text[:20]
        got = await page2.evaluate("fetch('journal.json?full=1').then(r => r.json()).then(d => [d.trades.length, d.trades.filter(t => t.entry).length])")
        assert got == [6, 5]
        await page2.close()
        assert not errors, errors
        await browser.close()
    bot.JOURNAL_HISTORY = m.Bot.JOURNAL_HISTORY
    await web.stop()


asyncio.run(run())
