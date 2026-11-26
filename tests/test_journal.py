"""The trades' review page (/p/<token>/journal): the tiles (today, 7 days, lifetime, open), the folded statistics and
risks, filters with counts, the list by day (light rows: the open trades and the last JOURNAL_HISTORY finished ones,
the rest on request), one trade's whole record fetched when it is opened (why the model priced it, the prices behind
it, proxy and anchor, how it filled, how it was settled, version), deep links, and the CSV / JSON exports. The browser
part needs Playwright + Chromium and is skipped without them."""
import asyncio, csv, io, json, os, sys, time, urllib.parse, urllib.request, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP, except to 127.0.0.1)
import main as m
import math
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
    # 1.117.1: when a record last changed state — a settled one at its settlement, a resting one with no fill at its order
    by_id = {t["id"]: t for t in data["trades"]}
    assert all(t["updated"] == t["settled"] >= t["opened"] for t in data["trades"] if t["status"] == "settled" and t["settled"]), by_id
    assert by_id["hype-flip#1|up|挂"]["updated"] == by_id["hype-flip#1|up|挂"]["opened"]
    assert m.trade_updated({"opened": 5, "fills": [{"at": 9}], "live": {"events": [{"at": 7}], "cancel_done_ms": 8}}) == 9 and m.trade_updated({}) == 0
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
    # 1.113.0: a day is one line until it is clicked (sumDays); 今日按市场 has a refresh button and the time of the data
    assert "dayOpen" in m.JOURNAL_PAGE and 'id="scref"' in m.JOURNAL_PAGE and 'id="scat"' in m.JOURNAL_PAGE and "function clock(" in m.JOURNAL_PAGE
    assert 'id="anaref"' in m.JOURNAL_PAGE and 'id="anaat"' in m.JOURNAL_PAGE  # 1.113.1: the 复盘分析 header's refresh button and the time of its data
    assert 'id="s-detail"' in m.JOURNAL_PAGE and "rows=1" in m.JOURNAL_PAGE and "function cardDetail(" in m.JOURNAL_PAGE  # 1.114.0: a card's records under the table
    assert 'id="q"' in m.JOURNAL_PAGE and "function matches(" in m.JOURNAL_PAGE  # 1.118.1: the search box over the list
    assert "function recTable(" in m.JOURNAL_PAGE  # 1.114.1: as one compact table (by market, a subtotal each, a total), a row opening the record
    assert 'id="d-detail"' in m.JOURNAL_PAGE and "function dayDetail(" in m.JOURNAL_PAGE and "lean=1" in m.JOURNAL_PAGE  # 1.115.0: a day's records under the summary
    # 1.81.0: the analysis tables (per underlying, per edge / fair bucket, per session part, the makers, the refused buys), the
    # marks, the summary CSV and the daily report
    hsi_mk = m.SimMarket(HSI_SLUG, "恒生指数", "close", "HSI", 0.70, book([("0.55", "300")], [("0.58", "400")], NOW, HSI_SLUG, "HSI", "101"),
                         0.03, "", ("涨", "跌"), {"key": "HSI", "target": "2026-10-05", "line": 24600.0, "close_ms": CLOSE}, EVIDENCE)
    bot.sim_note_block(f"{HSI_SLUG}|down|吃", hsi_mk, "吃跌", 0.40, "收涨时这组仓位合计将亏 $350.00，超过组上限 $300", NOW + 30_000)
    data = json.loads(web.route("GET", f"/p/{TOKEN}/journal.json")[2])
    a = data["analysis"]
    names = [r["name"] for r in a["items"]]
    assert names == ["恒生指数", "KOSPI", "上证指数", "HYPE"] and sum(r["records"] for r in a["items"]) == 6, a["items"]  # the most traded first
    cards = a["cards_today"]  # 1.105.1: today's records per card — a market's P&L for the day; the chips on the page filter by card
    assert {r["name"] for r in cards} == {"恒生指数", "KOSPI", "HYPE"} and sum(r["records"] for r in cards) == 5 and cards[0]["name"] == "恒生指数" and cards[0]["settled"] == 2, cards
    assert {t["card"] for t in data["trades"] if t["kind"] == "close"} == {"恒生指数", "KOSPI", "上证指数"} and all(t.get("card") for t in data["trades"]), [t.get("card") for t in data["trades"]]
    assert m.card_of({"item": "宇树 15分涨跌 10:45–11:00", "kind": "updown", "settle": {"window": "15m"}}) == "宇树 15分涨跌"
    assert m.card_of({"item": "BTC 11-04 涨跌", "kind": "updown", "settle": {"day": "2026-11-04"}, "driver": "BTCUSDT", "driver_name": "BTC"}) == "BTC 日涨跌"
    assert m.card_of({"item": "恒生指数｜10-05", "kind": "close"}) == "恒生指数" and 'id="s-cards"' in m.JOURNAL_PAGE and "cardF" in m.JOURNAL_PAGE
    hsi_row = next(r for r in a["items"] if r["name"] == "恒生指数")
    assert hsi_row["settled"] == hsi_row["wins"] == 2 and hsi_row["hit"] == 1.0 and abs(hsi_row["avg_fair"] - 0.7) < 1e-9 and abs(hsi_row["expected"] - 48.48) < 1e-6, hsi_row
    assert [(r["name"], r["records"]) for r in a["edges"]] == [("10–15¢", 4), ("15–20¢", 2)], a["edges"]  # an edge of 0.0999…98 is 10¢; 15¢ starts the next bucket
    assert [(r["name"], r["records"]) for r in a["fairs"]] == [("30–50¢", 1), ("50–70¢", 1), ("70–90¢", 4)], a["fairs"]
    assert [(r["name"], r["records"]) for r in a["sessions"]] == [("盘中", 2), ("休市日", 3), ("无交易时段", 1)], a["sessions"]  # 10-05 is a KRX holiday; the old record a Sunday
    assert "markout" in a["sessions"][0] and a["sessions"][1]["settled"] == 2 and a["sessions"][1]["wins"] == a["sessions"][1]["losses"] == 1
    mk = a["makers"]
    assert mk["placed"] == 4 and mk["filled"] == 2 and mk["partial"] == 0 and mk["unfilled"] == 1 and mk["resting"] == 1 and mk["cancelled"] == 1 and mk["expired"] == 0, mk
    assert abs(mk["fill_rate"] - 2 / 3) < 1e-9 and mk["wait_n"] == 2 and mk["wait_median"] == 3_230_000 and mk["rest_n"] == 1, mk  # 1.83.0: two waits' median is their mean (60 s and 6 400 s: 3 230 s, not the upper one)
    assert m.median([10, 90]) == 50 and m.median([3, 1, 2]) == 2 and m.median([]) is None and m.median(iter([4])) == 4
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
    # --- 1.107.0: the daily summary and the analysis over the page's filter (a card, a kind, a way), the summary CSV too ---------
    fj = lambda q: json.loads(web.route("GET", f"/p/{TOKEN}/journal.json", q)[2])
    every = bot.sim_trades().values()
    hs = fj("analysis=1&card=" + urllib.parse.quote("恒生指数"))
    assert hs["filter"] == {"card": "恒生指数"} and hs["filter_text"] == "恒生指数" and hs["records"] == sum(m.card_of(t) == "恒生指数" for t in every) == 2, hs["records"]
    assert [r["name"] for r in hs["analysis"]["items"]] == ["恒生指数"] and hs["summary"][-1]["records"] == 2 and hs["analysis"]["blocked"]["n"] == 1
    block = bot.sim_blocks()[0]
    assert (block["kind"], block["key"], "window" in block) == ("close", "HSI", False), block  # a refused buy names its kind: the filter's card
    ko = fj("analysis=1&card=KOSPI")
    assert {r["name"] for r in ko["analysis"]["items"]} == {"KOSPI"} and ko["analysis"]["blocked"]["n"] == 0 and ko["records"] == 2
    makers = fj("analysis=1&way=maker")
    assert makers["filter_text"] == "挂单" and makers["records"] == sum(bool(t.get("maker")) for t in every) == 4 and makers["analysis"]["blocked"]["n"] == 0  # the refused buy was a taker
    assert fj("analysis=1&kind=flip&way=maker")["records"] == 1 and fj("analysis=1&kind=flip")["filter_text"] == "反超"
    junk = fj("analysis=1&kind=nope&way=sideways&card=")
    assert junk["filter"] == {} and junk["records"] == 6 and junk["filter_text"] == ""
    assert m.block_matches({"label": "吃跌", "market": "x"}, {"card": "恒生指数"}, {"x"}) and not m.block_matches({"label": "吃跌", "market": "y"}, {"card": "恒生指数"}, {"x"})  # an older block: by market
    # --- 1.114.0: a card's records of a day as list rows (what opens under a line of 今日按市场): every one of them, newest first,
    # the settled ones' P&L adding up to the card's line; "today" is the bot's Beijing day, a day nothing was opened on is empty,
    # a day that does not read means every day
    hs_rows = fj("rows=1&day=today&card=" + urllib.parse.quote("恒生指数"))
    assert hs_rows["filter"] == {"card": "恒生指数"} and hs_rows["day"] == "2026-10-05" and [t["card"] for t in hs_rows["trades"]] == ["恒生指数"] * 2, hs_rows["day"]
    assert hs_rows["trades"][0]["opened"] >= hs_rows["trades"][1]["opened"] and set(hs_rows["trades"][0]) <= set(m.Bot.JOURNAL_LIGHT) | {"live_state", "order_id"}
    hs_line = next(r for r in data["analysis"]["cards_today"] if r["name"] == "恒生指数")
    assert hs_rows["total"]["records"] == hs_line["records"] == 2 and abs(hs_rows["total"]["pnl"] - hs_line["pnl"]) < 1e-9
    assert abs(sum(t["pnl"] or 0 for t in hs_rows["trades"]) - hs_line["pnl"]) < 1e-9 and hs_rows["total"]["settled"] == sum(t["status"] == "settled" for t in hs_rows["trades"])
    assert fj("rows=1&day=2026-10-04&card=" + urllib.parse.quote("恒生指数"))["trades"] == [] and fj("rows=1&day=2026-10-04")["total"]["records"] == 1  # the 上证 record, opened the day before
    assert fj("rows=1&day=nope")["total"]["records"] == 6 and fj("rows=1")["day"] == "" and fj("rows=1&day=today&way=maker")["total"]["records"] == 3
    lean = fj("rows=1&day=today&lean=1")  # 1.115.0: only the settled, open and resting records sent (the KOSPI maker was withdrawn), the total over every one
    assert lean["total"]["records"] == 5 and len(lean["trades"]) == 4 and {t["status"] for t in lean["trades"]} == {"settled", "resting"}, [t["status"] for t in lean["trades"]]
    # --- 1.151.0: 按市场 over a range of days (journal.json?cards=1&range=… | from=…&to=…): the model trader's records opened on those
    # Beijing days per card, each with 偏离 — the settled P&L against the expectation at the order in σ (luck_of) — and, unfiltered,
    # the pre-open mode's lines (none on the paper bot); a line's records over the same days (rows=1&range=…)
    today = fj("cards=1&range=today")
    assert (today["from"], today["to"], today["label"], today["range"]) == ("2026-10-05", "2026-10-05", "今天", "today")
    assert [(r["name"], r["records"], round(r["pnl"], 6)) for r in today["rows"]] == [(r["name"], r["records"], round(r["pnl"], 6)) for r in data["analysis"]["cards_today"]]
    hs = next(r for r in today["rows"] if r["name"] == "恒生指数")
    assert hs["luck"]["bets"] == 2 and abs(hs["luck"]["pnl"] - hs["pnl"]) < 1e-6 and abs(hs["luck"]["ev"] - hs["expected"]) < 1e-6 and hs["luck"]["sigma"] > 0
    assert abs(hs["luck"]["z"] - (hs["luck"]["pnl"] - hs["luck"]["ev"]) / hs["luck"]["sigma"]) < 1e-3 and hs["luck"]["verdict"] == "样本不足"
    yday = fj("cards=1&range=yesterday")
    assert (yday["from"], yday["label"], [r["name"] for r in yday["rows"]]) == ("2026-10-04", "昨天", ["上证指数"])
    week = fj("cards=1&range=7d")
    assert (week["from"], week["to"], week["label"]) == ("2026-09-29", "2026-10-05", "09-29 至 10-05（7 天）")
    assert {r["name"] for r in week["rows"]} == {r["name"] for r in today["rows"]} | {"上证指数"} and sum(r["records"] for r in week["rows"]) == 6
    assert fj("cards=1&from=2026-10-04&to=2026-10-04")["rows"] == yday["rows"] and fj("cards=1&range=all")["label"].startswith("全部（")
    makers = fj("cards=1&range=7d&kind=close&way=maker")  # the filter bar's kind / way apply (and leave the pre-open lines out, as in the analysis)
    assert makers["filter"] == {"kind": "close", "way": "maker"} and {(r["name"], r["records"]) for r in makers["rows"]} == {("恒生指数", 1), ("KOSPI", 1), ("上证指数", 1)}, makers["rows"]
    assert all(r["records"] for r in fj("cards=1&range=7d&way=taker")["rows"]) and fj("cards=1&range=7d&way=taker")["filter"] == {"way": "taker"}
    wk_rows = fj("rows=1&lean=1&range=7d&card=" + urllib.parse.quote("上证指数"))
    assert (wk_rows["from"], wk_rows["to"], wk_rows["day"], wk_rows["total"]["records"], len(wk_rows["trades"])) == ("2026-09-29", "2026-10-05", "", 1, 1)
    assert fj("rows=1&from=2026-10-05&to=2026-10-05&card=" + urllib.parse.quote("上证指数"))["total"]["records"] == 0
    # the days a query covers (journal_days, the tally's too): presets, a range, never after today, at most so many days, junk = today
    d = lambda *a: m.journal_days(*a, dt.date(2026, 10, 5), 31)
    assert d("", "", "7d") == (dt.date(2026, 9, 29), dt.date(2026, 10, 5)) and d("", "", "yesterday") == (dt.date(2026, 10, 4),) * 2
    assert d("2026-10-01", "2026-12-01", "") == (dt.date(2026, 10, 1), dt.date(2026, 10, 5)) and d("2026-10-05", "2026-10-01", "") == (dt.date(2026, 10, 1),) * 2
    assert d("2026-01-01", "2026-10-05", "") == (dt.date(2026, 9, 5), dt.date(2026, 10, 5)) and d("nope", "", "") == (dt.date(2026, 10, 5),) * 2
    assert d("", "", "all") == (dt.date(2026, 9, 5), dt.date(2026, 10, 5)) and m.journal_days_label(dt.date(2026, 10, 3), dt.date(2026, 10, 3), dt.date(2026, 10, 5)) == "10-03"
    status, _, body = web.route("GET", f"/p/{TOKEN}/summary.csv", "card=" + urllib.parse.quote("恒生指数"))
    frows = list(csv.reader(io.StringIO(body.decode("utf-8")[1:])))
    assert status == 200 and frows[1][:3] == ["筛选", "恒生指数", "2"] and next(r for r in frows if r[1] == "累计")[2] == "2", frows[:3]
    assert "今日按市场" in {r[0] for r in frows}
    # 1.107.2: the journal's CSV takes the filter too, and the page's 导出 CSV button carries it
    status, _, body = web.route("GET", f"/p/{TOKEN}/journal.csv", "card=" + urllib.parse.quote("恒生指数"))
    jrows = list(csv.reader(io.StringIO(body.decode("utf-8")[1:])))
    assert status == 200 and jrows[0] == list(m.Bot.JOURNAL_COLUMNS) and {r[0] for r in jrows[1:]} == {tid for tid, t in bot.sim_trades().items() if m.card_of(t) == "恒生指数"} and len(jrows) == 3
    assert len(list(csv.reader(io.StringIO(web.route("GET", f"/p/{TOKEN}/journal.csv")[2].decode("utf-8")[1:])))) == 7  # every record without one
    assert 'document.getElementById("csv").href=base+"/journal.csv"+(fk?"?"+fk:"")' in m.JOURNAL_PAGE
    web.cache = {f"x{i}": (0.0, b"") for i in range(70)}  # the cache drops what expired once it holds many (the filtered analyses: one a filter)
    web.cached("journal.json?analysis&card=y", lambda: b"{}")
    assert len(web.cache) == 1
    # 1.154.26 — the review page's data is seconds of the event loop a build (11-18 20:55: 6–7 s each, once a minute while the
    # page stood open). While the rule taker goes first — or will within two minutes (Bot.rules_clear) — none is built: the
    # last one is served, however old, or the request is told to come back; the exports and the tally likewise
    assert m.WebServer.CACHE_SECONDS["journal.json"] == 30.0 and m.WebServer.HEAVY_CLEAR_S == 120.0 and bot.rules_clear(120.0) is True
    built, asked = [], []
    real_payload, real_clear = bot.journal_payload, bot.rules_clear
    bot.journal_payload = lambda full=False, history=None: built.append(1) or real_payload(full, history)
    web.cache.clear()
    web.CACHE_SECONDS = {"journal.json": 30.0}
    first = web.route("GET", f"/p/{TOKEN}/journal.json")
    assert first[0] == 200 and built == [1] and web.route("GET", f"/p/{TOKEN}/journal.json")[2] is first[2] and built == [1], "kept half a minute"
    web.cache["journal.json"] = (0.0, first[2])  # (grown old)
    bot.rules_clear = lambda seconds: asked.append(seconds) or False
    stale = web.route("GET", f"/p/{TOKEN}/journal.json")
    assert stale[0] == 200 and stale[2] is first[2] and built == [1] and asked == [120.0], "the rule taker first: the last one built, nothing made"
    web.cache.clear()
    for path in ("journal.json", "journal.json?all=1", "journal.json?analysis=1&card=x", "journal.json?cards=1&range=7d", "journal.json?rows=1&day=today",
                 "journal.json?tally=HSI", "journal.csv", "summary.csv", "samples.csv"):
        name, _, query = path.partition("?")
        status, ctype, body = web.route("GET", f"/p/{TOKEN}/{name}", query)
        told = json.loads(body)
        assert status == 503 and ctype.startswith("application/json") and told["busy"] is True and "规则吃单正在优先时段" in told["message"] and built == [1], (path, status)
    assert web.route("GET", f"/p/{TOKEN}/journal.json", "id=nope")[0] == 404, "one record is no build: answered as ever"
    assert web.route("GET", f"/p/{TOKEN}/data.json")[0] == 200 and web.route("GET", f"/p/{TOKEN}/journal")[0] == 200, "the pages and the odds are no heavy bodies"
    bot.rules_clear = real_clear
    again = web.route("GET", f"/p/{TOKEN}/journal.json")
    assert again[0] == 200 and built == [1, 1] and web.route("GET", f"/p/{TOKEN}/journal.csv")[0] == 200
    bot.rules_clear = lambda seconds: 1 / 0
    web.cache.clear()
    assert web.route("GET", f"/p/{TOKEN}/journal.json")[0] == 200, "a bot that cannot say keeps no page waiting"
    bot.rules_clear, bot.journal_payload = real_clear, real_payload
    web.CACHE_SECONDS = {}
    web.cache.clear()
    # the last build is what is served while the rule taker goes first: not dropped with the expired ones when the cache is trimmed
    web.cache = {f"x{i}": (0.0, b"") for i in range(70)}
    web.cache["journal.json"], web.cache["journal.json?all"] = (0.0, b"LAST"), (0.0, b"ALL")
    web.cached("journal.json?analysis&card=y", lambda: b"{}")
    assert (web.cache["journal.json"][1], web.cache["journal.json?all"][1], len(web.cache)) == (b"LAST", b"ALL", 3), sorted(web.cache)
    web.cache.clear()
    # 1.154.27 — ...and a heavy body is kept a hundred times as long as its build held the event loop (ten minutes at the most):
    # however a page asks — a tab opened before a deploy runs the old script, a reload a minute; several tabs — the builds are
    # a hundredth of the loop's time at the most. 11-18 23:34: ten builds in ten minutes, 3.2–4 s each, the body kept 30 s
    web.CACHE_SECONDS = {"journal.json": 30.0, "data.json": 1.0}
    slow = lambda: time.sleep(0.4) or b"{}"
    before = time.monotonic()
    web.cached("journal.json", slow)
    kept = web.cache["journal.json"][0] - before
    assert 40.0 <= kept < 60.0, kept  # 100 × 0.4 s (and the build's own 0.4 s), not the 30 s of a quick one
    web.cache.clear()
    web.cached("journal.json", lambda: b"{}")
    assert 29.0 < web.cache["journal.json"][0] - time.monotonic() <= 30.0, "a quick build: the plain half minute"
    web.HEAVY_KEEP_MAX_S = 35.0
    web.cache.clear(); before = time.monotonic(); web.cached("journal.json?analysis&card=z", slow)
    assert 35.0 <= web.cache["journal.json?analysis&card=z"][0] - before < 36.0, "ten minutes at the most (35 s here)"
    del web.HEAVY_KEEP_MAX_S
    web.cache.clear(); before = time.monotonic(); web.cached("data.json", slow)
    assert web.cache["data.json"][0] - before < 2.0, "the cards' own data is no heavy body: a second, whatever it took"
    web.CACHE_SECONDS = {}
    web.cache.clear(); before = time.monotonic(); web.cached("journal.json", slow)
    assert web.cache["journal.json"][0] - before < 1.0, "a body that is not kept at all stays so"
    web.cache.clear()
    assert (m.WebServer.HEAVY_KEEP_FACTOR, m.WebServer.HEAVY_KEEP_MAX_S) == (100.0, 600.0)
    # ...and never more than 64 bodies kept, whatever is asked (they stay for minutes now): the soonest to end go, the page's own
    # list — what is served while the rule taker goes first — never
    web.CACHE_SECONDS = {"journal.json": 30.0}
    web.cached("journal.json", lambda: b"list")
    for i in range(80):
        web.cached(f"journal.json?analysis&card={i}", lambda: b"{}")
        assert len(web.cache) <= 64, (i, len(web.cache))
    assert web.cache["journal.json"][1] == b"list" and "journal.json?analysis&card=79" in web.cache and "journal.json?analysis&card=0" not in web.cache
    web.CACHE_SECONDS = {}
    web.cache.clear()
    # a body is built within the bot's review_scope (LiveBot: each of the controller's own records worked out once a build)
    import contextlib
    steps = []

    @contextlib.contextmanager
    def scope():
        steps.append("in")
        yield
        steps.append("out")
    bot.review_scope = scope
    web.cached("anything", lambda: steps.append("build") or b"{}")
    del bot.review_scope
    assert steps == ["in", "build", "out"], steps
    web.cache.clear()
    # ...the page says why (the 503's own words — in every one of its loaders), marks data that is old, asks every 5 minutes
    # instead of every one, and not at all while it is not looked at
    assert 'async function oops(r){if(r.status===503){let j={};try{j=await r.json()}catch(e){}return new Error(j.message||"HTTP 503")}return new Error("HTTP "+r.status)}' in m.JOURNAL_PAGE
    assert m.JOURNAL_PAGE.count("if(!r.ok)throw await oops(r)") == 7 and 'throw new Error("HTTP "+r.status)' not in m.JOURNAL_PAGE
    assert '(Date.now()-(data.server_ms||Date.now())>120000?"（"+Math.round((Date.now()-data.server_ms)/60000)+" 分钟前算的：' in m.JOURNAL_PAGE
    assert 'load();setInterval(()=>{if(document.visibilityState!=="hidden")load()},300000);' in m.JOURNAL_PAGE and "setInterval(load,60000)" not in m.JOURNAL_PAGE
    assert 'document.addEventListener("visibilitychange",()=>{if(document.visibilityState==="visible"&&Date.now()-okAt>60000)load()});' in m.JOURNAL_PAGE
    # --- 1.107.0: 按设置版本 — a table per setting that changed, a row per value with the stretch it ran ---------------------------
    v0 = dict(bot.sim_version())
    assert "sim_maker_follow_settle" in v0 and "sigma_calib" in v0 and "sim_maker_min_bid" not in v0  # (the fixture: SIM_MAKER_MIN_BID=0)
    rec = lambda i, opened, payout=1.0, drop=(), **ver: {"market": f"m{i}", "side": "up", "kind": "close", "key": "HSI", "item": "恒生指数", "maker": False,
                                                         "status": "settled", "shares": 10.0, "order": 10.0, "price": 0.5, "payout": payout, "edge": 0.1,
                                                         "fair": 0.6, "opened": opened,
                                                         "version": {k: v for k, v in {**v0, **ver}.items() if k not in drop}}
    recs = [rec(0, 1_000, sim_edge=0.10), rec(1, 2_000, 0.0, sim_edge=0.10), rec(2, 3_000, sim_edge=0.12), rec(3, 4_000, sim_edge=0.12, code="1.106.9"),
            rec(4, 5_000, sim_edge=0.10, code="1.106.0"), rec(5, 6_000, sim_edge=0.10, code="1.106.0", sim_maker_min_bid=100),
            rec(6, 7_000, sim_edge=0.10, code="1.106.0", sim_maker_min_bid=100, drop=("sim_taker_hold_range", "sigma_calib")),
            {"market": "old", "status": "settled", "opened": 500, "shares": 1.0, "price": 0.5, "payout": 1.0}]  # no version at all: left out
    vs = m.sim_by_version(recs)
    # the latest change first; an "on only" setting absent is off (关); one always written but absent (an older record) is no value
    assert [k["key"] for k in vs] == ["sim_maker_min_bid", "sim_edge", "code"], [k["key"] for k in vs]
    mb, edge, code = vs
    assert (mb["label"], mb["env"], mb["changed"], mb["switches"]) == ("买1 至少多少份才跟", "SIM_MAKER_MIN_BID", 6_000, 1)
    assert [(r["name"], r["records"]) for r in mb["rows"]] == [("关", 5), ("100", 2)]
    assert (edge["label"], edge["env"], edge["changed"], edge["switches"]) == ("触发买入的净优势（¢/份）", "SIM_EDGE_CENTS", 5_000, 2)
    r10, r12 = edge["rows"]
    assert (r10["name"], r10["records"], r10["settled"], r10["wins"], r10["losses"], r10["first"], r10["last"], r10["runs"]) == ("10¢", 5, 5, 4, 1, 1_000, 7_000, 2), r10
    assert abs(r10["pnl"] - 15.0) < 1e-9 and abs(r10["expected"] - 5.0) < 1e-9 and abs(r10["hit"] - 0.8) < 1e-9 and abs(r10["avg_fair"] - 0.6) < 1e-9, r10
    assert (r12["name"], r12["records"], r12["runs"], r12["first"], r12["last"]) == ("12¢", 2, 1, 3_000, 4_000) and abs(r12["pnl"] - 10.0) < 1e-9
    assert [(r["name"], r["records"]) for r in code["rows"]] == [(".".join(m.VERSION.split(".")[:2]), 3), ("1.106", 4)] and code["label"] == "代码版本"
    many = m.sim_by_version([rec(i, 1_000 * (i + 1), code=f"1.{90 + i}.0") for i in range(10)])
    assert [r["name"] for r in many[0]["rows"]] == ["更早（3 种取值）", "1.93", "1.94", "1.95", "1.96", "1.97", "1.98", "1.99"] and many[0]["rows"][0]["records"] == 3
    assert m.version_text("sim_crypto_day_edge", 0.0) == "0（同买入门槛）" and m.version_text("sim_crypto_day_edge", 0.1) == "10¢" and m.version_text("sim_skip", ()) == "不排除"
    assert m.version_text("sim_maker_deep_markets", ("HSI", "KOSPI")) == "HSI、KOSPI" and m.version_text("sigma_calib", False) == "关" and m.version_text("sim_maker_exit", 0.0) == "关"
    assert m.version_value("sim_maker_deep_markets", {"sim_maker_deep_markets": ["KOSPI", "HSI"]}) == (True, ("HSI", "KOSPI")) and m.version_value("sim_edge", {}) == (False, None)
    assert m.sim_by_version([rec(0, 1_000), rec(1, 2_000)]) == [] and data["analysis"]["versions"] == []  # nothing changed: no table
    # 1.154.26: the tables read each record's fields once (SummaryColumns) where every row walked its records' — forty tables
    # over 12,800 records was most of the 6–7 s a build of the review page held the event loop (11-18 20:55). The same
    # figures to the last bit: the same terms, summed in the same order — against the rows made as they were before
    def by_version_before(trades, marks=None):
        opened = lambda t: int(t.get("opened") or 0)
        ordered = sorted((t for t in trades if isinstance(t.get("version"), dict)), key=opened)
        prints = [json.dumps(t["version"], sort_keys=True, default=str) for t in ordered]
        sample = {}
        for t, fp in zip(ordered, prints):
            sample.setdefault(fp, t["version"])
        out = []
        for key in m.VERSION_KNOBS:
            value_of = {fp: m.version_value(key, v) for fp, v in sample.items()}
            if len({value for ok, value in value_of.values() if ok}) < 2:
                continue
            groups, runs, switches, previous = {}, {}, [], m.VERSION_UNRECORDED
            for t, fp in zip(ordered, prints):
                ok, value = value_of[fp]
                if not ok:
                    continue
                groups.setdefault(value, []).append(t)
                if previous is m.VERSION_UNRECORDED or value != previous:
                    if previous is not m.VERSION_UNRECORDED:
                        switches.append(opened(t))
                    runs[value] = runs.get(value, 0) + 1
                    previous = value
            row = lambda name, rows, stretches: {"name": name, "first": opened(rows[0]), "last": max(opened(t) for t in rows), "runs": stretches,
                                                 **m.summary_of(rows, marks), **m.settled_extras(rows)}
            values = sorted(groups, key=lambda v: opened(groups[v][0]))
            early = values[:-m.VERSION_ROWS_MAX + 1] if len(values) > m.VERSION_ROWS_MAX else []
            rows = ([row(f"更早（{len(early)} 种取值）", sorted((t for v in early for t in groups[v]), key=opened), 1)] if early else []) \
                + [row(m.version_text(key, v), groups[v], runs[v]) for v in values[len(early):]]
            label, env = m.version_label(key)
            out.append({"key": key, "label": label, "env": env, "changed": switches[-1] if switches else 0, "switches": len(switches), "rows": rows})
        return sorted(out, key=lambda k: -k["changed"])
    assert m.sim_by_version(recs) == by_version_before(recs) and m.sim_by_version(recs, {("m0", "up"): 0.4}) == by_version_before(recs, {("m0", "up"): 0.4})
    import random
    rng = random.Random(26)
    vers, cur = [], dict(v0)
    knobs = [k for k in m.VERSION_KNOBS if k in v0 and k != "code" and isinstance(v0[k], (int, float)) and not isinstance(v0[k], bool)]
    for i in range(14):  # fourteen versions: the code's and a setting or two changed at each, one of them back and forth
        cur = {**cur, "code": f"1.{100 + i}.{i}", **{k: cur[k] + 1 for k in rng.sample(knobs, 2)}, knobs[0]: v0[knobs[0]] + i % 2}
        vers.append(cur)
    wild = []
    for i in range(700):
        status = rng.choice(["settled"] * 5 + ["filled", "resting", "resting", "cancelled", "expired"])
        shares = 0.0 if status in ("cancelled", "expired") or (status == "resting" and rng.random() < 0.5) else rng.choice([3.0, 10.0, 12.5, 0.1 + 0.2])
        t = {"market": f"w{i % 90}", "side": rng.choice(["up", "down"]), "kind": "close", "key": "HSI", "item": "恒生指数", "maker": rng.random() < 0.5, "status": status,
             "shares": shares, "order": 12.5, "price": rng.choice([0.1, 0.33, 0.47, 0.52, 0.7]), "edge": rng.choice([0.07, 0.1, 0.13, None]), "fair": rng.choice([0.55, 0.6, 0.71]),
             "opened": 10_000 + rng.randrange(5_000) * 7, "version": dict(vers[min(13, i // 50)]) if i % 97 else None}
        if status == "settled":
            t["shares"], t["payout"] = max(shares, 1.0), rng.choice([1.0, 0.0, 0.5, 1, 0])
        wild.append(t)
    rng.shuffle(wild)
    wild_marks = {(f"w{i}", side): rng.choice([0.2, 0.45, 0.61]) for i in range(0, 90, 3) for side in ("up", "down")}
    got = m.sim_by_version(wild, wild_marks)
    assert got == by_version_before(wild, wild_marks), "the same tables"
    assert len(got) >= 3 and any(len(k["rows"]) == m.VERSION_ROWS_MAX and k["rows"][0]["name"].startswith("更早") for k in got), [(k["key"], len(k["rows"])) for k in got]
    assert json.dumps(got, sort_keys=False) == json.dumps(by_version_before(wild, wild_marks), sort_keys=False), "the rows' fields in the same order too"
    assert m.sim_by_version(wild) == by_version_before(wild) and m.sim_by_version([]) == []
    one = wild[0]["version"] or wild[1]["version"]
    assert m.version_print(one) == json.dumps(one, sort_keys=True, default=str) and m.VERSION_PRINTS[id(one)][0] is one
    # --- 1.107.0: the 15-minute tables — by the window's start hour (the card named when there are two), by the minutes left -----
    def win(item, key, start, opened, payout=1.0):
        return {"market": f"{key}-{start}", "kind": "updown", "key": key, "item": item, "side": "up", "maker": True, "status": "settled", "shares": 10.0,
                "order": 10.0, "price": 0.5, "payout": payout, "edge": 0.1, "fair": 0.6, "opened": opened, "fills": [],
                "settle": {"start": start - 60_000, "end": start + 14 * 60_000, "window": "15m"}}
    u930, u945, u1300, k800 = BJ(10, 5, 9, 30), BJ(10, 5, 9, 45), BJ(10, 5, 13, 0), BJ(10, 5, 8, 0)
    wins = [win("宇树 15分涨跌 09:30–09:45", "UNITREE-15M", u930, u930 + 60_000), win("宇树 15分涨跌 09:45–10:00", "UNITREE-15M", u945, u945 + 10 * 60_000, 0.0),
            win("宇树 15分涨跌 13:00–13:15", "UNITREE-15M", u1300, u1300 + 13 * 60_000), win("SK 海力士 15分涨跌 08:00–08:15", "SKHYNIX-15M", k800, k800 + 4 * 60_000),
            dict(win("BTC 10-05 涨跌", "BTC-DAY", u930, u930), settle={"end": u930, "day": "2026-10-05"})]  # a daily card: not a window
    w = m.sim_by_window(wins)
    assert [(r["name"], r["records"]) for r in w["hours"]] == [("SK 海力士 15分涨跌 08 时开始", 1), ("宇树 15分涨跌 09 时开始", 2), ("宇树 15分涨跌 13 时开始", 1)], w["hours"]
    assert [(r["name"], r["records"]) for r in w["left"]] == [("剩 ≥12 分", 1), ("剩 9–12 分", 1), ("剩 3–6 分", 1), ("剩 <3 分", 1)], w["left"]
    assert w["hours"][1]["losses"] == 1 and "markout" in w["left"][0] and [r["name"] for r in m.sim_by_window(wins[:3])["hours"]] == ["09 时开始", "13 时开始"]
    assert m.sim_by_window([]) == {"hours": [], "left": [], "sides": []} and data["analysis"]["windows"] == {"hours": [], "left": [], "sides": []}
    page = m.JOURNAL_PAGE
    for want in ('id="a-versions"', 'id="a-windows"', 'id="sumfilt"', 'id="anafilt"', "journal.json?analysis=1&", "清除筛选", "按设置版本",
                 'id="a-preopen"', "function preopenSection(", "开盘前预挂",
                 'id="unrecbox"', "function unrecorded(", 'anaOpen=true;try{localStorage.setItem("jrnana","1")}',  # 1.143.1: a pre-open row opens 复盘分析 before scrolling to it
                 'id="a-ev"', "function evSection(", "function evChart(", "模型 EV 对实际盈亏"):  # 1.128.0: the pre-open mode's section; 1.130.3: the positions beyond the records; 1.140.0: the EV timeline
        assert want in page, want
    assert data["analysis"]["preopen"] is None  # the paper bot has no pre-open mode: the section stays hidden
    # --- 1.140.0: 模型 EV 对实际盈亏 — the poker tracker's all-in EV line over the settled bets, with the model's own ±2σ band ---------
    ev = data["analysis"]["ev"]
    settled = [t for t in trades.values() if t["status"] == "settled" and float(t.get("shares") or 0) > 0]
    assert ev["total"]["bets"] == len(settled) == 4 and ev["total"]["verdict"] == "样本不足" and not ev["total"]["flag"], ev["total"]
    assert [d["label"] for d in ev["days"]] == ["10-04 周日", "10-05 周一"] and abs(ev["total"]["pnl"] - sum((t["payout"] - t["price"]) * t["shares"] for t in settled)) < 1e-9
    assert abs(ev["total"]["ev"] - sum(t["edge"] * t["shares"] for t in settled)) < 1e-9 and [g["name"] for g in ev["strategies"]] == ["原策略挂单", "原策略吃单"]
    assert ev["cards"][0]["name"] in {"恒生指数", "KOSPI"} and (ev["days_span"], ev["window_days"], ev["min_bets"], ev["flag_z"]) == (30, 7, 30, 2.0)
    flt_ev = bot.journal_analysis({"card": "恒生指数"})["analysis"]["ev"]  # the filter: the model's records of that card only (the pre-open fills, had there been any, stay out)
    assert flt_ev["total"]["bets"] == sum(1 for t in settled if m.card_of(t) == "恒生指数") and [g["name"] for g in flt_ev["cards"]] == ["恒生指数"]

    def bet(i, payout, maker=True, fair=0.6, price=0.5, shares=10.0, days_ago=0):
        return {"market": f"m{i}", "kind": "updown", "key": "BTC-15M", "item": "BTC 15分涨跌 10:00–10:15", "side": "up", "maker": maker, "status": "settled",
                "shares": shares, "order": shares, "price": price, "payout": payout, "edge": fair - price, "fair": fair,
                "opened": NOW - days_ago * 86_400_000 + i * 60_000, "settle": {"start": NOW, "end": NOW + 900_000, "window": "15m"}}
    lost = [bet(i, 0.0) for i in range(40)]
    tl = m.sim_ev_timeline(m.sim_ev_bets(lost + [bet(99, 1.0, days_ago=40), {**bet(98, 1.0), "status": "filled"}, bet(97, 1.0, shares=0)]), NOW)
    t = tl["total"]  # the 40 losers; the one 40 days old, the open one and the one without a fill are no bets
    assert (t["bets"], round(t["ev"], 6), round(t["pnl"], 6), t["verdict"], t["flag"]) == (40, 40.0, -200.0, "激进", True), t
    assert abs(t["sigma"] - math.sqrt(40 * 100 * 0.24)) < 1e-9 and abs(t["z"] - (-240 / math.sqrt(960))) < 1e-6
    assert len(tl["days"]) == 1 and tl["days"][0]["label"] == "10-05 周一" and tl["days"][0]["cum_pnl"] == -200.0 and tl["recent"]["bets"] == 40 and tl["recent"]["flag"]
    assert [(g["name"], g["bets"]) for g in tl["strategies"]] == [("原策略挂单", 40)] and [(g["name"], g["bets"]) for g in tl["cards"]] == [("BTC 15分涨跌", 40)]
    fair = m.sim_ev_timeline(m.sim_ev_bets([bet(i, 1.0 if i % 10 < 6 else 0.0, maker=i % 2 == 0, days_ago=i % 3) for i in range(60)]), NOW)
    assert fair["total"]["verdict"] == "相符" and not fair["total"]["flag"] and [d["label"] for d in fair["days"]] == ["10-03 周六", "10-04 周日", "10-05 周一"], fair["days"]
    assert abs(fair["total"]["pnl"] - fair["total"]["ev"]) < 1e-9 and [g["name"] for g in fair["strategies"]] == ["原策略挂单", "原策略吃单"]  # 60% paid at fair 60¢: as claimed
    assert fair["days"][-1]["cum_pnl"] == fair["total"]["pnl"] and fair["days"][-1]["sigma"] == fair["total"]["sigma"]
    few = m.sim_ev_timeline(m.sim_ev_bets(lost[:10]), NOW)
    assert few["total"]["verdict"] == "样本不足" and not few["total"]["flag"] and few["recent"]["z"] < -2  # the figure stands, the verdict waits for 30 bets
    lucky = m.sim_ev_timeline(m.sim_ev_bets([bet(i, 1.0) for i in range(40)]), NOW)
    assert lucky["total"]["verdict"] == "保守" and not lucky["total"]["flag"]
    assert m.sim_ev_timeline([], NOW) == {"days": [], "total": m.ev_group("全部", []), "recent": m.ev_group("近 7 天", []), "strategies": [], "cards": [],
                                          "days_span": 30, "window_days": 7, "min_bets": 30, "flag_z": 2.0, "label": ""}  # (1.154.70: a span's name, none for the default)
    assert m.ev_bet(NOW, "预挂", "BTC 15分涨跌", 0.49, 10, 0.5, 0.1, 5.1)["var"] == 25.0 and m.ev_bet(NOW, "x", "y", 0.99, 10, 1.2, 0, 0)["fair"] == 0.99
    # the hourly check: 30+ settled bets in the last 7 days falling 2σ short of the model's claim → one notice a day, to the subscribed chats
    cfg2 = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"})
    evbot = m.Bot(cfg2, m.Store(":memory:"), FM(NOW), None)
    for i, t in enumerate(lost):
        evbot.store.put(f"sim:m{i}|up|挂", {**t, "slug": f"m{i}", "label": "挂涨", "settled": NOW + 900_000})
    notices = []
    evbot.sim_notify = lambda text: notices.append(text)
    said = evbot.sim_ev_check(NOW)
    assert "把优势看高了" in said and "40 笔已结算，模型预期 +$40.00，实际 −$200.00，偏离 −7.7σ" in said and notices == [said] and evbot.store.get("ev_notice")["at"] == NOW, said
    evbot.sim_ev_checked = -1e9
    assert evbot.sim_ev_check(NOW + 3600_000) == "" and len(notices) == 1  # once a day
    evbot.sim_ev_checked = -1e9
    assert evbot.sim_ev_check(NOW + 25 * 3600_000) != "" and len(notices) == 2
    assert evbot.sim_ev_check(NOW + 26 * 3600_000) == "" and len(notices) == 2  # hourly: not even computed
    line = evbot.sim_ev_line()
    assert line.startswith("⚠️ 模型 EV 对实际（近 7 天，含预挂）：40 笔｜预期 +$40.00｜实际 −$200.00｜偏离 −7.7σ → 激进"), line
    assert "偏离 −7.7σ → 激进" in evbot.sim_text() and m.Bot(cfg2, m.Store(":memory:"), FM(NOW), None).sim_ev_line() == ""
    assert "📐 模型 EV 对实际（近 7 天，含预挂）：4 笔｜" in bot.sim_text() and "（不足 30 笔）" in bot.sim_text()
    # the notice goes out through the deliveries, to the active subscriptions only (the real sim_notify)
    evbot2 = m.Bot(cfg2, m.Store(":memory:"), FM(NOW), object())
    told = []

    async def tell2(chat, thread, text, **kw):
        told.append((chat, thread, text))
        return True
    evbot2.tell = tell2
    evbot2.store.put("subscriptions", {"1:0": {"chat": 1, "thread": 0, "active": True}, "2:0": {"chat": 2, "thread": 0, "active": False}})
    evbot2.sim_notify("hello"); await evbot2.drain_deliveries()
    assert told == [(1, 0, "hello")] and evbot2.sim_seq == 1
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
        # 1.152.2: 账户里有、记录里没有的持仓 sits below 统计与风险, folded to one line; opened, the market is named in full and linked,
        # with its value and result — and no 均价 / 持仓时间 columns when Predict gave none (they were all "—")
        assert await page.is_hidden("#unrecbox"), "a paper bot: nothing to show"
        await page.evaluate("""data.live={unrecorded:[{name:"Will $PONS FDV hit $20M before November 2026? / No",shares:10.7535,price:null,at:0,value:10.75,
          url:"https://predict.fun/zh-cn/market/what-fdv-will-pons-hit-before-nov-2026",status:"WON",market_id:"778",index_set:2}],unknown_orders:2,positions_at:Date.now()};unrecorded()""")
        assert await page.is_visible("#unrecbox") and await page.is_hidden("#unrecbody") and await page.inner_text("#foldunrec") == "展开"
        assert await page.inner_text("#unrecsum") == "1 个 · 现值 $10.75 · 挂单 2 张", await page.inner_text("#unrecsum")
        assert await page.evaluate("!!(document.getElementById('statsbox').compareDocumentPosition(document.getElementById('unrecbox'))&Node.DOCUMENT_POSITION_FOLLOWING)")
        await page.click("#unrechd")
        assert await page.is_visible("#unrecbody") and await page.inner_text("#foldunrec") == "收起" and await page.evaluate("localStorage.getItem('jrnunrec')") == "1"
        assert await page.locator("#unrectbl .urow").count() == 1 and await page.inner_text("#unrectbl .urow a") == "Will $PONS FDV hit $20M before November 2026? / No"
        assert await page.get_attribute("#unrectbl .urow a", "href") == "https://predict.fun/zh-cn/market/what-fdv-will-pons-hit-before-nov-2026"
        assert await page.inner_text("#unrectbl .urow .small") == "账户多出 10.7535 份 · 现值 $10.75 · 已结算：赢", "no 均价 nor 持仓时间 when Predict gave none"
        assert "账户上还有 2 张没有记录的挂单" in await page.inner_text("#unrecread")
        await page.click("#unrechd")
        assert await page.is_hidden("#unrecbody") and await page.evaluate("localStorage.getItem('jrnunrec')") == "0"
        await page.evaluate("data.live=null;unrecorded()")
        assert await page.is_hidden("#unrecbox")
        # 1.153.0: 规则吃单 in 复盘分析 — each rule's totals (the real and the paper apart) and the windows bought
        await page.evaluate("""rulesSection({mode:"paper",markets:{gold:{on:true,shares:50,name:"黄金"},oil:{on:false,shares:50,name:"原油"}},max_day_usd:200,fast:"01:00-07:00",lead_minutes:300,speed:{orders:9,filled:4,lag:262,own:18,trip:310,wake:4,timed:7,best:{lag:120,own:9,trip:270}},
          races:[{slug:"xau-updown-15m-2",rule:"gold_shut",name:"黄金休市 4 档",window:"黄金 15分涨跌 06:15–06:30",start_ms:Date.now(),cents:60,offer_at:Date.now()-18000000,lead_min:300,peak:150,cut_ms:350,gone_ms:350,open:true,asked:300,bought:0,
            orders:[{shares:300,filled:0,paper:false,seen:150,fresh:true,after:253,lag:252,own:1,trip:340}]},
           {slug:"xau-updown-15m-1",rule:"gold_shut",name:"黄金休市 4 档",window:"黄金 15分涨跌 06:00–06:15",start_ms:Date.now(),cents:60,offer_at:0,lead_min:null,peak:0,cut_ms:null,gone_ms:null,open:false,asked:212,bought:212,
            orders:[{shares:166,filled:166,paper:false,seen:null,fresh:null,after:null,lag:240,own:96,trip:350},{shares:46,filled:46,paper:false,seen:null,fresh:false,after:null,lag:252,own:4100,trip:366}]},
           {slug:"xau-updown-15m-0",rule:"gold_shut",name:"黄金休市 4 档",window:"黄金 15分涨跌 05:45–06:00",start_ms:Date.now(),cents:60,offer_at:0,lead_min:null,peak:0,cut_ms:null,gone_ms:null,open:false,asked:0,bought:0,
            orders:[{shares:166,filled:0,paper:false,seen:null,fresh:null,state:"not_sent",error:"限流冷却中",after:null,lag:null,own:null,trip:null}]}],rules:[{rule:"gold_shut",name:"黄金休市 4 档",side:"up",cents:58,live_cents:70,course:{windows:3,shares:60,cost:39.6,settled:2,wins:2,losses:0,pnl:19.5},mode:"paper",windows:3,
          shares:150,cost:78.9,settled:2,wins:2,losses:0,pnl:46.1,open:1,watch:{watched:20,offered:12,offer_avg:35.5,best_mid:0.51,open_lead:142,offer_lead:299.9,lost:5,life:180,won:4,won_life:640}}],recent:[{slug:"xau-updown-15m-1",rule:"gold_shut",name:"黄金休市 4 档",
          window:"黄金 15分涨跌 06:00–06:15",start_ms:Date.now(),side:"up",mode:"paper",shares:50,live:20,avg:0.526,cost:26.3,result:"up",pnl:23.2}]})""")
        rtxt = await page.text_content("#a-rules")
        for want in ("只记录（不下单）", "黄金 开（每场最多 50 份）、原油 关", "每天最多 $200", "每秒检查时段（北京时间） 01:00-07:00", "对手提前 300 分钟挂单的时刻前后也每秒检查",
                     "下单速度（近 30 天真实订单 9 张，4 张有成交）：盘口变化→推送到达 中位 262 ms（最快 120）、到达→发出 18 ms（最快 9）［其中到达→开始处理 4 ms］、发出→应答 310 ms（最快 270）",
                     "中位 300 分钟", "5 场（存活中位 0.2 秒）", "4 场（出现到被吃掉中位 0.6 秒）", "盘口观察（近 30 天）", "35.5", "中位 142 秒", "黄金休市 4 档", "涨 ≤58¢", "+$46.10", "2/0", "黄金 15分涨跌 06:00–06:15", "52.6¢", "+$23.20",
                     # 1.154.14: 盘中吃单 — the course's price beside the rule's, what was bought in the windows' course by itself, a window's part of it
                     "涨 ≤58¢｜盘中 ≤70¢", "其中盘中买的（开场后，规则方向领先时）", "66.0¢", "$39.60", "+$19.50", "50（盘中 20）",
                     # 1.154.16: each window's race — the offer's coming, how soon it was first taken from and gone, what was bought, each order
                     "抢单记录（北京时间；开始被吃 / 被吃光 = 卖单出现之后多久，按 Predict 盘口的时间戳）", "300 分钟", "0.35 秒", "没买到",
                     "要 300 成交 0，出现后 253 ms 发出，推送 252 ms，到达→发出 1 ms，往返 340 ms", "没记到", "212/212",
                     "要 166 成交 166，推送 240 ms，到达→发出 96 ms，往返 350 ms；要 46 成交 46，推送 252 ms，到达→发出 4100 ms（不是盘口一到就下的），往返 366 ms",
                     "要 166：没下成（限流冷却中）"):  # 1.154.18: an order that never left is not one that bought nothing
            assert want in rtxt, (want, rtxt)
        # ...a rule with no price before the window and one for its course; one with neither (and the speed of a version before 1.154.15: no fastest)
        await page.evaluate("""rulesSection({mode:"on",markets:{},speed:{orders:2,filled:2,lag:246,own:2098,trip:358},rules:[{rule:"gold_shut",name:"黄金休市 4 档",side:"up",cents:0,live_cents:60,mode:"on",windows:0,shares:0,cost:0,settled:0,wins:0,losses:0,pnl:0,open:0},
          {rule:"gold_open",name:"黄金开盘第一档",side:"down",cents:0,live_cents:0,mode:"on",windows:1,shares:5,cost:2.5,settled:0,wins:0,losses:0,pnl:0,open:1}],recent:[]})""")
        rtxt2 = await page.text_content("#a-rules")
        assert "涨（盘前不买）｜盘中 ≤60¢" in rtxt2 and "跌（已关）" in rtxt2 and "其中盘中买的" not in rtxt2, rtxt2
        assert "盘口变化→推送到达 中位 246 ms、到达→发出 2098 ms、发出→应答 358 ms" in rtxt2, rtxt2
        assert await page.get_attribute("#h-rules", "data-k") == "ruletaker", "its own fold key: 统计与风险's 规则 keeps rules"
        # 1.154.36: the control key is asked for in the page, as a password (prompt() has no password mode: a phone showed it as typed)
        assert "prompt(msg" not in m.JOURNAL_PAGE and "prompt(msg" not in m.WEB_PAGE
        await page.evaluate("()=>{window.kd=keyDialog('需要控制口令\\n这是实盘账户的交易记录')}")
        await page.locator(".kdlg .kbox").wait_for()
        assert await page.get_attribute(".kdlg input.kin", "type") == "password" and await page.get_attribute(".kdlg input.kin", "autocomplete") == "current-password"
        assert "这是实盘账户的交易记录" in await page.inner_text(".kdlg p")
        await page.fill(".kdlg input.kin", "secret-456")
        await page.press(".kdlg input.kin", "Enter")  # (the form: Enter submits, as on a phone's keyboard)
        assert await page.evaluate("window.kd") == "secret-456" and await page.locator(".kdlg").count() == 0
        await page.evaluate("()=>{window.kd=keyDialog('x')}")
        await page.locator(".kdlg .kbox").wait_for()
        await page.keyboard.press("Escape")
        assert await page.evaluate("window.kd") is None and await page.locator(".kdlg").count() == 0
        # 1.154.34: 开盘前预挂 by price level — each level's settled fills against a coin toss (preopen.ev_line): 47¢ worth 3¢ a share
        import preopen as P
        bj = lambda h: int(dt.datetime(2026, 10, 5, h, tzinfo=m.BEIJING).timestamp() * 1000)

        def pre(start, up, down, result, level=0, price=0.47, shares=30):
            payout = {"up": 1.0, "down": 0.0}[result]
            return {"slug": f"btc-updown-15m-{start // 1000}" + (f"#{level}" if level else ""), "key": "BTC-15M", "name": "BTC 15分涨跌 x", "start_ms": start,
                    "phase": "closed", "accounted": True, "result": result, "pnl": (payout - price) * up + ((1 - payout) - price) * down,
                    "price": str(price), "shares": str(shares), "level": level, "attempt": 0,
                    "orders": {side: {"state": "final", "filled": float(f)} for side, f in (("up", up), ("down", down))}}
        po_rows = [pre(bj(10), 30, 0, "up"), pre(bj(10), 0, 80, "up", 1, 0.43, 80), pre(bj(11), 0, 30, "up"), pre(bj(12), 30, 30, "down")]
        po = P.review(po_rows, NOW)
        await page.evaluate("po=>preopenSection(po)", po)
        ptxt = await page.text_content("#a-preopen")
        for want in ("预期对实际", "按五五开预期 +$9.20，实际 −$30.80，差 −$40.00（偏离 −1.4σ）", "两边都成交 2 场 −$16.70 · 只成交一边 1 场 −$14.10，成交那边赢 0 场（0%）",
                     "按价格档位的预期对实际在上面“按市场”下方"):  # 1.154.35: the level table moved up, on 按市场's days (below)
            assert want in ptxt, (want, ptxt)
        assert "每份：预期 → 实际" not in ptxt
        # 1.113.0: the daily summary is one line per day until that line is clicked (its kind / way lines then show; a second
        # click folds them); 今日按市场 has a refresh button and says when the data was read
        days = page.locator("#summary tr.dayrow")
        assert await days.count() >= 1 and await page.locator("#summary tr.sub").count() == 0 and (await days.first.inner_text()).startswith("▸ ")
        assert await page.is_hidden("#d-detail")
        await days.first.click()
        assert await page.locator("#summary tr.sub").count() >= 2 and (await days.first.inner_text()).startswith("▾ ")
        # 1.115.0: the open day's records show under the summary, by card with each card's figures first, a total at the end
        await page.wait_for_selector("#d-detail table tr.rec")
        assert "10-05 周一 · 这一天的每一笔" in await page.inner_text("#d-detail h3") and "5 笔" in await page.inner_text("#d-detail .day")
        assert {x for x in await page.locator("#d-detail tr.grp td:first-child").all_inner_texts()} == {"恒生指数", "KOSPI", "HYPE"}
        assert await page.locator("#d-detail tr.rec").count() == 4 and "另有 1 笔未成交 / 撤单的没有列出" in await page.inner_text("#d-detail p.small")
        day_total = await page.locator("#d-detail tr.all td").all_inner_texts()
        assert day_total[0] == "合计" and day_total[1] == "4 笔" and day_total[6] == "赢 3 输 0" and day_total[7] == "+$178.77", day_total
        await days.first.click()
        assert await page.locator("#summary tr.sub").count() == 0 and await page.is_hidden("#d-detail")
        assert (await page.inner_text("#scat")).startswith("已刷新 ") and await page.inner_text("#scref") == "刷新"
        read_at = await page.evaluate("okAt")  # when the data was last read
        await page.click("#scref")  # disabled and "刷新中…" while the page re-reads, then back
        await page.wait_for_selector("#scref:not([disabled])")
        assert await page.inner_text("#scref") == "刷新" and await page.evaluate("okAt") > read_at and await page.locator("#list .tr").count() == 3
        # 1.113.1: the 复盘分析 header has one too; it re-reads without unfolding the section (the header's click folds / unfolds)
        assert await page.is_hidden("#anabody")
        read_at = await page.evaluate("okAt")
        await page.click("#anaref")
        await page.wait_for_selector("#anaref:not([disabled])")
        assert await page.is_hidden("#anabody") and await page.inner_text("#anaref") == "刷新" and await page.evaluate("okAt") > read_at
        await page.click("#anahd")
        assert await page.is_visible("#anabody") and (await page.inner_text("#anaat")).startswith("已刷新 ")
        # 1.152.1: each part of 复盘分析 folds on its own (按标的 folded the first time, remembered once opened), the
        # explanations wait behind 说明, the exports behind one menu, 市场涨跌统计 folded until opened
        items = page.locator('details.fold[data-k="items"]')
        assert await items.get_attribute("open") is None and await page.is_hidden("#a-items")
        await items.locator("summary").click()
        await page.locator("#a-items table").wait_for()
        for _ in range(50):  # (the toggle event comes a task later)
            if await page.evaluate("localStorage.getItem('jrnfold:items')") == "1":
                break
            await asyncio.sleep(0.02)
        assert await page.evaluate("localStorage.getItem('jrnfold:items')") == "1"
        hints = page.locator("#sumbody details.hint")
        assert await hints.count() == 1 and await hints.first.get_attribute("open") is None and await hints.first.locator("p").first.is_hidden()
        await hints.first.locator("summary").click()
        assert "选的日子（北京时间）里下单的记录按卡片汇总" in await hints.first.inner_text()
        await hints.first.locator("summary").click()
        assert await page.is_hidden("#csv") and await page.get_attribute("#csv", "href") == f"/p/{TOKEN}/journal.csv"
        await page.click("#exports summary")
        assert await page.is_visible("#csv") and await page.is_visible("#json")
        await page.click("#title")  # a tap elsewhere closes the menu
        assert await page.is_hidden("#csv")
        assert await page.is_hidden("#tallybody") and await page.inner_text("#foldtally") == "展开"
        await page.click("#anahd")
        assert await page.is_hidden("#anabody")
        # 1.131.5: the pre-open mode's rows in 今日按市场 (the live bot adds them, preopen.cards_today): named "· 开盘前预挂"; 1.151.0: a
        # click opens where its money went under the table (preopen.card_diag: the notes, the one-sided fills, by start hour, by
        # lead, by side), the jump to the mode's own section a button there; no records panel (the model trader's are not theirs)
        diag = {"windows": 2, "one": {"name": "只成交一边", "windows": 2, "settled": 2, "pnl": -9.4, "one": 2, "won": 0, "z": -1.4142},
                "both": {"name": "两边都成交", "windows": 0, "settled": 0, "pnl": 0.0, "one": 0, "won": 0},
                "hours": [{"name": "06:00", "windows": 2, "settled": 2, "pnl": -9.4, "one": 2, "won": 0}],
                "lead": [{"name": "开场前 6 小时以上", "windows": 2, "settled": 2, "pnl": -9.4, "one": 2, "won": 0}],
                "sides": [{"name": "涨", "records": 3, "filled_windows": 0, "filled": 0.0, "volume": 0.0, "settled": 0, "wins": 0, "losses": 0, "ties": 0, "pnl": 0.0},
                          {"name": "跌", "records": 3, "filled_windows": 2, "filled": 20.0, "volume": 9.4, "settled": 2, "wins": 0, "losses": 2, "ties": 0, "pnl": -9.4}],
                "levels": [{"name": "47¢", "price": 0.47, "records": 3, "windows": 3, "resting": 0, "filled_windows": 2, "filled": 20.0, "volume": 9.4, "pending": 0,
                            "settled": 2, "wins": 0, "losses": 2, "ties": 0, "pnl": -9.4, "pending_cost": 0.0, "withdrawn": 1, "settled_shares": 20.0, "ev": 0.6,
                            "sigma": 7.0711, "z": -1.41, "both": {"windows": 0, "pnl": 0.0}, "one": {"windows": 2, "won": 0, "pnl": -9.4}}],  # 1.154.34: with its 预期对实际
                "notes": ["亏损集中在 06:00（北京时间）开始的场次"]}
        po_row = {"name": "BTC 15分涨跌 · 开盘前预挂", "preopen": True, "records": 3, "markets": 3, "filled": 10.0, "volume": 4.9, "settled": 1, "wins": 1,
                  "losses": 0, "ties": 0, "pnl": 5.1, "open": 0, "open_cost": 0.0, "resting": 2, "marked": 0, "unrealized": 0.0, "expected": 0.1, "hit": 1.0, "avg_fair": 0.5,
                  "luck": {"bets": 4, "ev": 0.1, "pnl": 5.1, "sigma": 2.5, "z": 2.0, "verdict": "样本不足"}, "diag": diag}
        plain = bot.sim_analysis
        bot.sim_analysis = lambda trades, marks, flt=None: (lambda a: {**a, "cards_today": [*a["cards_today"], *([] if flt else [po_row])]})(plain(trades, marks, flt))
        await page.click("#scref")
        await page.wait_for_selector("#s-cards tr.dayrow:has-text('开盘前预挂')")
        po_line = page.locator("#s-cards tr.dayrow", has_text="开盘前预挂")
        assert await po_line.count() == 1 and (await po_line.inner_text()).startswith("▸ BTC 15分涨跌 · 开盘前预挂") and "+$5.10" in await po_line.inner_text()
        assert "+2.0σ 好于预期（4 场）" in await po_line.inner_text()
        await po_line.locator("td").first.click()
        await page.wait_for_selector("#s-detail ul.notes")
        assert (await po_line.inner_text()).startswith("▾ ") and "BTC 15分涨跌 · 开盘前预挂 · 今天：钱亏在哪" in await page.inner_text("#s-detail h3")
        text = await page.inner_text("#s-detail")
        for want in ("亏损集中在 06:00（北京时间）开始的场次", "成交那边赢 0 场（0%，五五开该一半，偏离 −1.4σ）", "按开始时间", "开场前 6 小时以上", "按方向",
                     "按价格档：预期（按五五开）对实际", "+3.0¢ → −47.0¢", "+$0.60", "−$10.00（−1.4σ）", "0/2（0%，保本 47%）"):  # 1.154.34
            assert want in text, (want, text)
        assert await page.locator("#s-detail tr.rec").count() == 0, "the model trader's records panel is not a pre-open line's"
        await po_line.locator("td").first.click()
        assert await page.is_hidden("#s-detail") and (await po_line.inner_text()).startswith("▸ ")
        bot.sim_analysis = plain
        await page.click("#scref")
        await page.wait_for_selector("#scref:not([disabled])")
        # 1.114.0: a line of 今日按市场 opens the card's records of the day under the table (read from the server), a second click folds them
        assert await page.is_hidden("#s-detail")
        hs_line = page.locator("#s-cards tr.dayrow", has_text="恒生指数")
        assert await hs_line.count() == 1 and (await hs_line.inner_text()).startswith("▸ 恒生指数")
        await hs_line.locator("td").first.click()
        await page.wait_for_selector("#s-detail table tr.rec")
        assert (await hs_line.inner_text()).startswith("▾ 恒生指数") and "恒生指数 · 今天的每一笔" in await page.inner_text("#s-detail h3")
        assert await page.locator("#s-detail tr.rec").count() == 2 and "2 笔" in await page.inner_text("#s-detail .day") and "已结算" in await page.inner_text("#s-detail .day")
        # 1.114.1: one compact table — the record's market (the day), time, side, price, shares, cost, result, P&L, state, order id;
        # one market today, so no subtotal; the total line adds the two up to the card's line; a row opens the whole record
        heads = await page.locator("#s-detail table th").all_inner_texts()
        assert heads == ["市场", "时间", "方向", "价格", "份数", "成本", "结果", "盈亏", "状态", "订单号"], heads
        recs = [await r.locator("td").all_inner_texts() for r in await page.locator("#s-detail tr.rec").all()]
        assert [r[0] for r in recs] == ["恒生指数"] * 2 and {r[2] for r in recs} == {"挂涨", "吃涨"} and all(r[6] == "赢" and r[8] == "已确认" for r in recs), recs  # the item is the card's name here
        assert await page.locator("#s-detail tr.grp").count() == 0 and await page.locator("#s-detail tr.all").count() == 1
        total = await page.locator("#s-detail tr.all td").all_inner_texts()
        assert total[0] == "合计" and total[1] == "2 笔" and total[4] == "400" and total[5] == "$231.52" and total[6] == "赢 2 输 0" and total[7] == "+$168.48", total
        await page.locator("#s-detail tr.rec").first.locator("td").first.click()
        det = page.locator("#s-detail tr.det .det")
        await det.locator("h3").first.wait_for()
        assert await page.locator("#s-detail tr.det").count() == 1 and "判断依据（下单时）" in await det.locator("h3").first.inner_text()
        await page.locator("#s-detail tr.rec.open").first.locator("td").first.click()  # the same row again: closed
        assert await page.locator("#s-detail tr.det").count() == 0 and await page.locator("#list .tr.open").count() == 0
        await page.click("#s-detail button:has-text('收起')")
        assert await page.is_hidden("#s-detail") and (await hs_line.inner_text()).startswith("▸ 恒生指数")
        # 1.151.0: 按市场 over other days — a preset or from..to read from the server (journal.json?cards=1&…), the 偏离 column, a
        # line's records over those days; the pre-open mode's lines of those days come with their breakdown
        assert await page.inner_text("#scrng") == "今天" and await page.get_attribute("#cbar .tq.on", "data-r") == "today"
        assert (await page.locator("#s-cards table th").all_inner_texts())[-1] == "偏离"
        await page.click("#cbar [data-r=yesterday]")
        await page.wait_for_selector("#scrng:text-is('昨天')")
        assert [x[2:] for x in await page.locator("#s-cards tr.dayrow td:first-child").all_inner_texts()] == ["上证指数"]
        assert await page.input_value("#cfrom") == "2026-10-04" and await page.input_value("#cto") == "2026-10-04"
        await page.click("#cbar [data-r='7d']")
        await page.wait_for_selector("#scrng:has-text('7 天')")
        assert await page.inner_text("#scrng") == "09-29 至 10-05（7 天）" and await page.get_attribute("#cbar .tq.on", "data-r") == "7d"
        names = {x[2:] for x in await page.locator("#s-cards tr.dayrow td:first-child").all_inner_texts()}
        assert {"恒生指数", "KOSPI", "上证指数"} <= names, names
        sh = page.locator("#s-cards tr.dayrow", has_text="上证指数")
        await sh.locator("td").first.click()
        await page.wait_for_selector("#s-detail table tr.rec")
        assert "上证指数 · 09-29 至 10-05（7 天） 的每一笔" in await page.inner_text("#s-detail h3") and await page.locator("#s-detail tr.rec").count() == 1
        await page.click("#s-detail button:has-text('收起')")
        assert await page.is_hidden("#cfrom"), "1.152.1: the custom dates wait behind 自定义"
        await page.click("#ccust summary")
        await page.fill("#cfrom", "2026-10-05")
        await page.fill("#cto", "2026-10-05")
        await page.click("#cgo")
        await page.wait_for_selector("#scrng:text-is('今天')")
        assert await page.locator("#cbar .tq.on").count() == 0 and "上证指数" not in await page.inner_text("#s-cards")
        po_range = {**po_row, "name": "黄金 15分涨跌 · 开盘前预挂", "luck": {**po_row["luck"], "z": -3.29, "bets": 30}}
        bot.preopen_cards = lambda first, last, since_ms=0: [po_range] if first <= "2026-10-04" <= last else []  # (1.154.70: since_ms too)
        # 1.154.35: 预挂按价格档位 under 按市场, on its days (preopen.levels_between) — hidden without the mode (the paper bot's today)
        assert await page.is_hidden("#plbox")
        bot.preopen_levels = lambda first, last, since_ms=0: P.levels_between(po_rows, first, last)
        await page.click("#cbar [data-r='30d']")
        await page.wait_for_selector("#s-cards tr.dayrow:has-text('黄金 15分涨跌')")
        await page.wait_for_selector("#plbox:not([hidden])")
        ltxt = await page.inner_text("#plbox")
        for want in ("预挂按价格档位 · 09-06 至 10-05（30 天）", "日期跟着上面“按市场”选的", "按五五开预期 +$9.20，实际 −$30.80，差 −$40.00（偏离 −1.4σ）",
                     "47¢×30", "+3.0¢ → +3.0¢", "+$3.60", "1/2（50%，保本 47%）", "43¢×80", "+7.0¢ → −43.0¢", "+$5.60", "−$34.40", "−$40.00（−1.0σ）", "0/1（0%，保本 43%）"):
            assert want in ltxt, (want, ltxt)
        heads = await page.locator("#plbox .tbl th").all_inner_texts()
        assert heads == ["档位", "有成交 / 场次", "已结算", "已结算份数", "每份：预期 → 实际", "预期", "实际", "实际 − 预期（偏离）", "两边都成交", "只成交一边：那边赢"], heads
        assert await page.evaluate("!!(document.getElementById('s-cards').compareDocumentPosition(document.getElementById('plbox'))&Node.DOCUMENT_POSITION_FOLLOWING)")
        assert await page.evaluate("!!(document.getElementById('plbox').compareDocumentPosition(document.getElementById('anabody'))&Node.DOCUMENT_POSITION_FOLLOWING)"), "above 复盘分析"
        gold = page.locator("#s-cards tr.dayrow", has_text="黄金 15分涨跌")
        assert "−3.3σ 不像运气" in await gold.inner_text() and await gold.locator("b.bad").count() >= 1
        await gold.locator("td").first.click()
        await page.wait_for_selector("#s-detail ul.notes")
        assert "黄金 15分涨跌 · 开盘前预挂 · 09-06 至 10-05（30 天）：钱亏在哪" in await page.inner_text("#s-detail h3")
        await page.evaluate("document.getElementById('ccust').open=true")  # days without a window of the mode: the block says so
        await page.fill("#cfrom", "2026-10-01")  # (dates not read before: 昨天's answer is the server's from earlier, cached)
        await page.fill("#cto", "2026-10-03")
        await page.click("#cgo")
        await page.wait_for_selector("#plrng:has-text('10-01')")
        assert "这段时间预挂没有记录" in await page.inner_text("#plbox") and await page.locator("#plbox .tbl").count() == 0
        await page.click("#cbar [data-r=today]")
        assert await page.inner_text("#scrng") == "今天" and await page.is_hidden("#s-detail") and await page.is_hidden("#plbox")
        del bot.preopen_cards, bot.preopen_levels
        assert await page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"), "no sideways scroll with the range bar"
        assert not errors, errors
        # three rows at first (the resting order and the last two finished ones), every one on request, by day
        assert await page.locator("#list .tr").count() == 3 and "更早的 3 笔可载入" in await page.inner_text("#meta")
        # 1.143.1: a failed read of the earlier ones says so on the button, which tries again (it used to stay at 载入中…)
        fail_all = lambda url: url.endswith("journal.json?all=1")
        await page.route(fail_all, lambda route: route.abort())
        await page.click("#more button:has-text('载入更早的 3 笔')")
        await page.locator("#more button:has-text('点这里重试')").wait_for()
        assert await page.locator("#list .tr").count() == 3 and await page.is_enabled("#more button")
        await page.unroute(fail_all)
        await page.click("#more button:has-text('点这里重试')")
        await page.wait_for_selector("#list .tr >> nth=5")
        assert await page.locator("#list .tr").count() == 6 and await page.locator("#list .day").count() == 2  # 10-05, and the old record's 10-04
        # filters: by how the result stands, by way of trading; a chip nobody matches is not shown; 全部 clears them all
        await page.click("#filters [data-f=bad]")
        assert await page.locator("#list .tr").count() == 1
        await page.click("#filters [data-f=exp]")
        assert await page.locator("#list .tr").count() == 1  # the KOSPI maker: no seller ever reached it
        await page.click("#filters [data-f=rest]")
        assert await page.locator("#list .tr").count() == 1 and "⏳ 挂 20.0¢，最低卖价 30.0¢（高出 10.0¢）" in await page.inner_text("#list .tr .t3")
        assert await page.locator("#filters [data-f=open]").count() == (1 if any(t["status"] == "filled" or (t["status"] == "resting" and t["shares"] > 0) for t in data["trades"]) else 0)
        await page.click("#filters [data-f=all]")
        # 1.117.1: a row says when the record last changed (the settlement here), after its order time
        assert "更新 " in await page.locator("#list .tr:has-text('恒生指数 挂涨') .t2").inner_text()
        await page.click("#filters [data-f=taker]")
        assert await page.locator("#list .tr").count() == 2
        await page.click("#filters [data-f=taker]")  # the same chip again: off
        assert await page.locator("#list .tr").count() == 6
        # 1.143.1: 全部 is the status chip's own — a market picked stays picked; 清除筛选 clears them all
        await page.click("#filters [data-f='c:KOSPI']")
        await page.click("#filters [data-f=done]")
        assert await page.locator("#list .tr").count() == 1
        await page.click("#filters [data-f=all]")
        assert await page.locator("#list .tr").count() == 2 and await page.locator("#filters [data-f='c:KOSPI'].on").count() == 1
        await page.click("#fclear")
        assert await page.locator("#list .tr").count() == 6 and await page.locator("#fclear").count() == 0
        # 1.118.1: the search box — every word must appear in a row's market, side, order id, price, count, state or note
        await page.fill("#q", "KOSPI")
        assert await page.locator("#list .tr").count() == 2 and await page.inner_text("#qn") == "匹配 2 笔"
        await page.fill("#q", "恒生 吃涨 58.8")
        assert await page.locator("#list .tr").count() == 1 and "恒生指数 吃涨 @ 58.8¢" in await page.inner_text("#list .tr .t1")
        await page.fill("#q", "没有这个")
        assert await page.locator("#list .tr").count() == 0 and "没有符合条件的交易" in await page.inner_text("#list")
        await page.fill("#q", "")
        assert await page.locator("#list .tr").count() == 6 and await page.inner_text("#qn") == ""
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
