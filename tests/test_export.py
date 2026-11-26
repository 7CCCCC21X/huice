"""1.154.51 — everything the review page holds, by Telegram: /export and /tally.

The user: "复盘里面有哪些数据可以导出来，后面拿来优化；之前连续 3 把加大预挂份额的怎么看；怎么导出数据；都用 Tg 机器人导出".
The bot sent three files (/journal, /summary, /samples); the pre-open mode's ledger and its tables, the rule taker's records,
the 市场涨跌统计 (the table "预挂按前面连涨 / 连跌的场数" among it) and the journal's whole records were on the web page alone.

Here: the range and card words; the streak sums added up over cards (one card's figures as they were); /tally for one
card (the text, a row a window) and for several (the sums together, a line a card, a row a card and run length), in
the background once the bot's loops run, held while the rule taker goes first; /export's menu and each kind — the three
old files by their new word, the pre-open records and tables (on the real trader's ledger too, its archive counted),
the rule taker's records, the whole records gzipped, everything at once; a command's answer in several parts; a large
file sent gzipped. Entirely offline.

Run: python tests/test_export.py
"""
import asyncio, csv, gzip, io, json, pathlib, sys
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import types
import main as m
import live as L
import preopen
import rules

# test_tally's and test_journal's fixtures without running their suites: only what stands before "async def run()"
def head_of(name: str) -> dict:
    src = (ROOT / "tests" / f"{name}.py").read_text(encoding="utf-8").split("\nasync def run():")[0]
    space: dict = {"__file__": str(ROOT / "tests" / f"{name}.py"), "__name__": name}
    exec(compile(src, f"{name}_head", "exec"), space)
    return space


TT, TJ = head_of("test_tally"), head_of("test_journal")
DAY0, NOW, FILLS, ledger, candles, close_at, day_rows, FM = (TT[k] for k in ("DAY0", "NOW", "FILLS", "ledger", "candles", "close_at", "day_rows", "FM"))
table = lambda doc: list(csv.reader(io.StringIO(doc.data.decode("utf-8").lstrip("﻿"))))
req = lambda command, *args: m.Request(command, list(args), 1, 0, 42)


def tally_bot() -> m.Bot:
    """A bot at 11-08 10:02 (Beijing) whose Binance candles are test_tally's day, with its pre-open fills on BTC 5分."""
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"})
    bot = m.Bot(cfg, m.Store(":memory:"), FM(NOW), None)
    bot.reads = []

    async def spot(path, **params):
        bot.reads.append(("spot", params["symbol"], params["startTime"]))
        return candles(params["startTime"], params["limit"], bot.market.now, close_at)

    async def futures(path, **params):
        bot.reads.append(("index", params["pair"], params["startTime"]))
        return candles(params["startTime"], params["limit"], bot.market.now, close_at)
    m.binance_spot, bot.binance_futures = spot, futures
    rows = ledger(FILLS)
    bot.preopen_rows = lambda: rows
    bot.TALLY_PACE_S = 0.0
    return bot


def the_words() -> None:
    bot = tally_bot()
    D = lambda s: m.dt.date.fromisoformat(s)
    days = lambda *words, default="7d", most=m.TALLY_MAX_DAYS: bot.export_days(list(words), default, most)
    assert days() == (D("2026-11-02"), D("2026-11-08"), [], False, False)
    assert days("今天")[:2] == (D("2026-11-08"), D("2026-11-08")) and days("昨天")[:2] == (D("2026-11-07"), D("2026-11-07")) and days("today")[4] is True
    assert days("30d")[:2] == days("30天")[:2] == (D("2026-10-10"), D("2026-11-08")) and days("7天")[:2] == days("一周")[:2] == days()[:2]
    assert days("14天")[:2] == days("14d")[:2] == (D("2026-10-26"), D("2026-11-08")) and days("1天")[:2] == (D("2026-11-08"), D("2026-11-08"))
    assert days("90天")[:2] == (D("2026-10-09"), D("2026-11-08")), "no more than the most days: the first moved up"
    assert days("2026-11-05")[:2] == (D("2026-11-05"), D("2026-11-05")) and days("2026-11-06", "2026-11-03")[:2] == (D("2026-11-03"), D("2026-11-06"))
    assert days("2026-12-01")[:2] == (D("2026-11-08"), D("2026-11-08")), "never after today"
    assert days("btc", "5分", "30d", "全部") == (D("2026-10-10"), D("2026-11-08"), ["btc", "5分"], True, True)
    assert days("全部", default="all", most=m.CARDS_MAX_DAYS)[2:] == ([], True, False)
    try:
        days("2026-13-45")
    except ValueError as error:
        assert "日期格式" in str(error)
    else:
        raise AssertionError("a date that is none was taken")
    # the cards: none — every one; a key or a name — that one; part of a name — those that hold it (5分 is not 15分)
    named = lambda *words: bot.tally_cards_named(list(words))
    every = list(m.preopen_cards())
    assert named() == (every, "") and len(every) == 29
    assert [c.key for c in named("BTC-5M")[0]] == [c.key for c in named("btc", "5分")[0]] == [c.key for c in named("btc5分钟")[0]] == ["BTC-5M"]
    assert [c.key for c in named("btc")[0]] == ["BTC-15M", "BTC-5M", "BTC-1H", "BTC-1D"], [c.key for c in named("btc")[0]]
    five, quarter = [c.key for c in named("5分")[0]], [c.key for c in named("15分")[0]]
    assert five == ["BTC-5M", "ETH-5M", "BNB-5M"] and len(quarter) == 20 and "BTC-15M" in quarter and not set(five) & set(quarter), (five, quarter)
    assert [c.key for c in named("5m")[0]] == five and [c.key for c in named("小时")[0]] == ["BTC-1H", "ETH-1H", "BNB-1H"]
    assert named("没有这张") == ([], "没有名字含“没有这张”的卡。能统计的卡：" + "、".join(m.tally_name(c) for c in every))
    print("THE_WORDS_OK")


def sums_added_up() -> None:
    """One card's figures are what they were (test_tally holds them to the cent); two cards' sums together are one card's
    with both cards' windows — as if the second card's day followed the first's, a gap between."""
    res = "uuuddxuuuuux"
    rows = [{"s": i * 5, "e": i * 5 + 5, "r": r} for i, r in enumerate(res)]
    fl = lambda i, up, down, price=0.47, result="", at=0: {"s": at + i * 5, "up": up, "down": down, "price": price, "result": result}
    fills = [fl(1, 10, 0), fl(3, 10, 0), fl(5, 0, 10, result="u"), fl(6, 0, 10, 0.43), fl(9, 0, 10), fl(10, 10, 0), fl(10, 0, 10), fl(11, 10, 0), fl(99, 10, 0)]
    one = m.tally_streak_sums(rows, fills)
    assert m.tally_streak_view(one) == m.tally_streaks(rows, fills) and one["pending"] == 1
    assert m.tally_runs(rows) == {0: ("", 0), 5: ("u", 1), 10: ("u", 2), 15: ("u", 3), 20: ("d", 1), 25: ("d", 2), 30: ("", 0), 35: ("u", 1), 40: ("u", 2),
                                  45: ("u", 3), 50: ("u", 4), 55: ("u", 4)}, m.tally_runs(rows)
    w = m.tally_window_fills(rows, fills)
    assert sorted(w) == [5, 15, 25, 30, 45, 50, 55] and w[50]["up"] == w[50]["down"] == 10 and round(w[50]["pnl"], 4) == 0.6 and w[55]["o"] == "" and w[55]["pnl"] is None
    assert (w[25]["o"], round(w[25]["pnl"], 4), round(w[25]["ev"], 4), round(w[25]["cost"], 4)) == ("u", -4.7, 0.3, 4.7), w[25]  # Predict's result over the candles'
    far = 10_000
    rows2 = [{"s": far + i * 5, "e": far + i * 5 + 5, "r": "u"} for i in range(30)]
    fills2 = [fl(i, 10, 0, 0.45, at=far) for i in range(4, 30)]  # 26 windows after four ups or more, the up side filled and won
    two = m.tally_streak_sums(rows2, fills2)
    both = m.tally_streak_add(m.tally_streak_add(None, one), two)
    assert m.tally_streak_view(both) == m.tally_streaks(rows + rows2, fills + fills2)
    assert (both["double"]["windows"], both["rest"]["windows"], both["pending"]) == (3 + 26, 3, 1)
    assert m.tally_streak_view(one)["double"]["windows"] == 3 and one["double"]["windows"] == 3, "adding up leaves a card's own sums as they were"
    assert m.tally_streak_view(m.tally_streak_add(None, one)) == m.tally_streaks(rows, fills)
    print("SUMS_ADDED_UP_OK")


async def one_card() -> None:
    bot = tally_bot()
    text, doc = await bot.cmd_tally(req("/tally", "BTC-5M", "2026-11-08"))
    lines = text.split("\n")
    assert lines[0] == "📊 BTC 5分｜今天（11-08 00:00–10:00）｜币安现货 1 分钟 K 线", lines[0]
    assert lines[1] == "场次 120：涨 60、跌 59、平 1｜最长连涨 6 场、最长连跌 5 场", lines[1]
    assert lines[2] == "跟随还是反转：偏反转（明显，超过 3σ）：涨跌容易交替（相邻两场都分出涨跌的 117 对，同向 9 对，随机该 58.5 对，偏离 −9.2σ）", lines[2]
    assert lines[3] == ("预挂成交的那一边：有成交 50 场，只成交一边且分出涨跌的 45 场，成交那边赢 10 场（随机该 22.5 场，偏离 −3.7σ）——"
                        "有逆向选择（明显，超过 3σ）：成交那边赢得比随机少，被吃掉的多是要输的那边；保本要赢 49%"), lines[3]
    assert lines[4] == bot.TALLY_STREAK_HEAD and [x.split("：")[0] for x in lines[5:12]] == ["· " + n for n in (*m.TALLY_RUN_NAMES, "连 3 场以上合计", "其余合计")]
    assert lines[5] == "· 前面没有连续：1 场 10 份，每份预期 +1.0¢ → 实际 +51.0¢（+$5.10，偏离 +1.0σ）；只成交一边赢 1/1（100%），保本 49%", lines[5]
    assert lines[6] == ("· 连 1 场：48 场 530 份，每份预期 +1.1¢ → 实际 −25.9¢（−$137.40，偏离 −3.9σ）；只成交一边赢 9/44（20%），保本 49%；"
                        "顺着 0/35（0%）、逆着 9/9（100%）；两边都成交 4 场 +$2.60"), lines[6]
    assert lines[7:10] == ["· 连 2 场：没有成交", "· 连 3 场：没有成交", "· 连 4 场以上：没有成交"] and lines[10] == "· 连 3 场以上合计：没有成交"
    assert lines[12:] == ["结论：样本太少（不到 20 场），先别下结论", "还有 1 场有成交但结果未定，没算进去"], lines[12:]
    # the figures are the page's own
    page = bot.tally_payload("BTC-5M", "2026-11-08", "2026-11-08")
    assert bot.tally_card_text(page, page["streak_fills"], "今天") == text
    # the file: a row a window
    rows = table(doc)
    assert doc.filename == "tally-BTC-5M-20261108-20261108.csv" and doc.mime == "text/csv" and doc.caption.startswith("BTC 5分 今天：一场一行")
    assert rows[0] == list(m.Bot.TALLY_WINDOW_COLUMNS) and len(rows) == 121
    assert rows[1] == ["BTC 5分", "2026-11-08 00:00:00", "2026-11-08 00:05:00", "100.0", "101.0", "涨", "", "", "", "0", "", "", "", "", ""], rows[1]
    assert rows[6][5:10] == ["涨", "", "", "涨", "4"] and rows[7][5:10] == ["跌", "", "", "涨", "4"] and rows[8][5:10] == ["跌", "", "", "跌", "1"], rows[6:9]
    assert rows[12][5] == "平" and rows[12][10:] == ["10.0", "0.0", "4.9", "0.1", ""], rows[12]  # the tie: filled, no result — no P&L yet
    assert rows[13][8:10] == ["", "0"] and rows[14] == ["BTC 5分", "2026-11-08 01:05:00", "2026-11-08 01:10:00", "102.0", "101.0", "跌", "", "", "涨", "1",
                                                        "20.0", "0.0", "9.7", "0.3", "-9.7"], rows[13:15]  # two levels of one window together
    filled = [r for r in rows[1:] if r[10]]
    assert len(filled) == 50 and round(sum(float(r[14]) for r in filled if r[14]), 4) == -132.3 == m.tally_streaks(day_rows(), bot.tally_fill_rows(m.tally_card("BTC-5M"), {r["s"] for r in day_rows()}))["rest"]["pnl"]
    # Predict's own result where the ledger keeps one is in its column, and settles the window
    kept = ledger([(13, 10, 0, 0.49, "up"), (15, 0, 10, 0.49, "split")])
    bot.preopen_rows = lambda: kept
    _, doc2 = await bot.cmd_tally(req("/tally", "btc", "5分", "今天"))
    rows2 = table(doc2)
    assert rows2[14][5:8] == ["跌", "", "涨"] and rows2[14][10:] == ["10.0", "0.0", "4.9", "0.1", "5.1"], rows2[14]  # the ledger's "up" over the candles' down: won
    assert rows2[16][5:8] == ["跌", "", "50/50"] and rows2[16][10:] == ["0.0", "10.0", "4.9", "0.1", "0.1"], rows2[16]  # 50/50: each share back at 50¢
    # a card without a pre-open fill: its windows all the same, and said
    text3, doc3 = await bot.cmd_tally(req("/tally", "ETH-5M", "今天"))
    assert text3.split("\n")[3] == "这段时间预挂在这张卡上没有成交，“连涨 / 连跌后预挂”没有可统计的" and len(table(doc3)) == 121 and "ETH 5分" in text3
    assert await bot.cmd_tally(req("/tally", "没有这张")) == bot.tally_cards_named(["没有这张"])[1]
    assert (await bot.cmd_tally(req("/tally", "BTC-5M", "2026-13-45"))).startswith("❌ 日期格式应为")
    print("ONE_CARD_OK")


async def several_cards() -> None:
    bot = tally_bot()
    eth = [{**p, "key": f"ETH-5M-{n}", "name": "ETH 5分涨跌 测试", "slug": p["slug"].replace("btc-", "eth-")} for n, p in enumerate(ledger(FILLS))]
    both = ledger(FILLS) + eth
    bot.preopen_rows = lambda: both
    text, doc = await bot.cmd_tally(req("/tally", "今天"))
    lines = text.split("\n")
    assert lines[0] == "📊 预挂按前面连涨 / 连跌｜29 张卡合计｜今天" and lines[1] == bot.TALLY_STREAK_HEAD, lines[:2]
    # two cards with the same fills: twice the windows, the shares and the money, a share as it was
    assert lines[3].startswith("· 连 1 场：96 场 1,060 份，每份预期 +1.1¢ → 实际 −25.9¢（−$274.80，偏离 −5.5σ）；只成交一边赢 18/88（20%），保本 49%"), lines[3]
    assert lines[8].startswith("· 其余合计：98 场 1,080 份") and lines[9] == "结论：样本太少（不到 20 场），先别下结论" and lines[10] == "还有 2 场有成交但结果未定，没算进去", lines[8:11]
    assert lines[11] == "各卡（连 3 场以上 对 其余，每份实际；成交场次多的在前）：", lines[11]
    assert sorted(lines[12:14]) == ["· BTC 5分：连 3 场以上 0 场 —｜其余 49 场 −24.5¢", "· ETH 5分：连 3 场以上 0 场 —｜其余 49 场 −24.5¢"], lines[12:14]
    assert lines[14:] == ["没有预挂成交的卡 27 张", "单看一张卡（含每一场的明细文件）：/tally BTC-5M 7d"], lines[14:]
    rows = table(doc)
    assert doc.filename == "tally-streaks-20261108-20261108.csv" and rows[0] == list(m.Bot.TALLY_GROUP_COLUMNS) and len(rows) == 1 + 7 * 30
    assert [r[1] for r in rows[1:8]] == [*m.TALLY_RUN_NAMES, "连 3 场以上合计", "其余合计"] and {r[0] for r in rows[1:8]} == {"合计"}
    # (a card's 连 1 场: 500 shares at 49¢ and 30 at 48¢ are worth 5.6 against a coin toss; two cards' σ is one's × √2, so z = −286 / 52.144)
    assert rows[2][:10] == ["合计", "连 1 场", "96", "1060.0", "11.2", "-274.8", "52.144", "-5.48", "0.0106", "-0.2592"], rows[2]
    btc = [r for r in rows if r[0] == "BTC 5分"]
    one = m.tally_streaks(day_rows(), bot.tally_fill_rows(m.tally_card("BTC-5M"), {r["s"] for r in day_rows()}))
    assert len(btc) == 7 and btc[1][2:6] == ["48", "530.0", "5.6", "-137.4"] and float(btc[6][5]) == one["rest"]["pnl"] == -132.3, btc
    assert btc[1][6:8] == [str(one["groups"][1]["sigma"]), str(one["groups"][1]["z"])] and round(float(btc[1][6]) * 2 ** 0.5, 3) == 52.144, btc[1]
    assert btc[1][10:] == ["44", "9", "0.4894", "35", "0", "9", "9", "4", "2.6"], btc[1]
    # the words choose the cards: the 5-minute ones — three, one of them with fills on neither
    text5, doc5 = await bot.cmd_tally(req("/tally", "5分", "今天"))
    assert text5.startswith("📊 预挂按前面连涨 / 连跌｜3 张卡合计｜今天") and "没有预挂成交的卡 1 张" in text5 and len(table(doc5)) == 1 + 7 * 4
    # the range's records are read once for all the cards, and each symbol's candles once
    seen = len(bot.reads)
    await bot.cmd_tally(req("/tally", "今天"))
    assert len(bot.reads) == seen and len(set(bot.reads)) == seen, "kept: a second look reads nothing, and no chunk was read twice (BTC's four cards share one)"
    assert {r[:2] for r in bot.reads} == {m.tally_source(c) for c in m.preopen_cards()} - {("index", "SKHYNIXUSDT"), ("index", "UNITREEUSDT"), ("index", "ZHIPUUSDT")}  # (a Sunday: those three have no window)
    # nothing settled anywhere: said, not a table of noughts
    bot.preopen_rows = lambda: []
    none_text, none_doc = await bot.cmd_tally(req("/tally", "今天"))
    assert none_text == "📊 预挂按前面连涨 / 连跌｜29 张卡合计｜今天\n这段时间这些卡上预挂没有已出结果的成交，没有可统计的" and len(table(none_doc)) == 1 + 7 * 30
    # a card whose candles cannot be read is counted without its runs, and named
    bot2 = tally_bot()

    async def broken(path, **params):
        if params["symbol"] == "ETHUSDT":
            raise m.RemoteError("HTTP 500: boom")
        return candles(params["startTime"], params["limit"], bot2.market.now, close_at)
    m.binance_spot = broken
    bot2.preopen_rows = lambda: both
    gap_text, _ = await bot2.cmd_tally(req("/tally", "5分", "今天"))
    assert "⚠️ K 线有缺的卡（缺的那几场和它后面一场按“前面没有连续”算）：ETH 5分" in gap_text, gap_text
    print("SEVERAL_CARDS_OK")


async def in_the_background() -> None:
    """With the bot's loops running the answer follows: the command says so at once; a second one waits for the first; while
    the rule taker goes first nothing is started, and a job under way holds its next card."""
    bot = tally_bot()
    sent: list = []

    async def answer(request, reply, markup=None, html_mode=False):
        sent.append(reply)
    bot.answer = answer
    bot.reference_tasks = [asyncio.get_running_loop().create_future()]  # (the loops run)
    said = await bot.cmd_tally(req("/tally", "BTC-5M", "今天"))
    assert said == "统计中：BTC 5分，今天——要读币安的 1 分钟 K 线，几秒后发来" and not sent, said
    assert await bot.cmd_tally(req("/tally", "ETH-5M")) == "上一次 /tally 还在算，等它发来再要下一个"
    await bot.tally_job
    assert len(sent) == 1 and sent[0][0].startswith("📊 BTC 5分｜今天") and isinstance(sent[0][1], m.Document), sent
    assert (await bot.cmd_tally(req("/tally"))).startswith("统计中：29 张卡，11-02 至 11-08（7 天）——要读币安的 1 分钟 K 线，一两分钟后发来")
    await bot.tally_job
    assert (await bot.cmd_tally(req("/tally", "30d"))).endswith("大约 7 分钟后发来")
    bot.tally_job.cancel()
    # a job that fails says so
    bot.tally_job = None

    async def boom(cards, d0, d1):
        raise RuntimeError("坏了")
    bot.tally_report = boom
    m.LOG.disabled = True  # (the failure is logged with its traceback: not into this suite's output)
    try:
        await bot.cmd_tally(req("/tally", "BTC-5M"))
        await bot.tally_job
    finally:
        m.LOG.disabled = False
    assert sent[-1] == "❌ 统计失败：坏了", sent[-1]
    # the rule taker goes first: not now
    busy = tally_bot()
    busy.rules_clear = lambda seconds: False
    assert await busy.cmd_tally(req("/tally")) == busy.EXPORT_BUSY + " /tally" and await busy.cmd_export(req("/export", "预挂")) == busy.EXPORT_BUSY + " /export"
    assert (await busy.cmd_export(req("/export"))).startswith("📦 复盘数据导出"), "what there is to export is said all the same"
    # ...and a job under way waits for it between two cards, TALLY_HOLD_S at the most — then ends with what it has
    held = tally_bot()
    clear = {"n": 0}

    def rules_clear(seconds):
        clear["n"] += 1
        return clear["n"] <= 3  # the command's own check and the first two cards; then the taker's turn, for good
    held.rules_clear = rules_clear
    held.TALLY_HOLD_S = 10.0
    naps: list[float] = []
    real_sleep = asyncio.sleep

    async def nap(seconds, *a):
        naps.append(seconds)
        await real_sleep(0)
    asyncio.sleep = nap
    try:
        part, _ = await held.cmd_tally(req("/tally", "5分", "今天"))
    finally:
        asyncio.sleep = real_sleep
    assert part.startswith("📊 预挂按前面连涨 / 连跌｜2 张卡合计｜今天") and "⚠️ K 线没读到、没算进去的卡：BNB 5分" in part and naps.count(5.0) == 2, (part, naps)
    print("IN_THE_BACKGROUND_OK")


def preopen_rows_for_export() -> list[dict]:
    """A pre-open ledger: BTC 5分 windows of 11-07 and 11-08 — settled wins and losses, a hedged one, one waiting for its result,
    one withdrawn unfilled, one resting, one whose orders never went out."""
    def pair(n, day_start, k, price, up, down, result="", accounted=False, phase="done", states=("filled", "cancelled"), attempt=1, **more):
        start = day_start + k * 300_000
        orders = {"up": {"state": states[0] if up else states[1], "filled": up}, "down": {"state": states[0] if down else states[1], "filled": down}}
        pnl = None
        if accounted and (up or down) and result:
            payout = {"up": 1.0, "down": 0.0, "split": 0.5}[result]
            pnl = round((payout - price) * up + ((1 - payout) - price) * down, 6)
        return {"key": f"BTC-5M-{n}", "name": "BTC 5分涨跌 11-0x", "slug": f"btc-updown-5m-{start // 1000}" + ("" if price == 0.47 else "#1") + f"@{attempt}",
                "price": price, "shares": 100.0, "start_ms": start, "phase": phase, "accounted": accounted, "result": result, "attempt": attempt,
                "orders": orders, **({"pnl": pnl} if pnl is not None else {}), **more}
    eve = DAY0 - m.DAY_MS
    return [pair(0, eve, 10, 0.47, 100.0, 0.0, "up", True, created_ms=eve + 10 * 300_000 - 86_000_000, cancel_sent_ms=eve + 10 * 300_000 - 15_000,
                 cancel_done_ms=eve + 10 * 300_000 - 14_400),
            pair(1, eve, 11, 0.47, 30.0, 0.0, "down", True),
            pair(2, DAY0, 2, 0.47, 100.0, 100.0, "up", True),
            pair(3, DAY0, 2, 0.43, 0.0, 60.0, "up", True),
            pair(4, DAY0, 3, 0.47, 0.0, 100.0, "split", True),
            pair(5, DAY0, 4, 0.47, 50.0, 0.0),                                    # filled, the result not booked yet
            pair(6, DAY0, 5, 0.47, 0.0, 0.0, "", True, states=("filled", "cancelled")),  # withdrawn, nothing filled
            pair(7, DAY0, 200, 0.47, 0.0, 0.0, phase="active", states=("open", "open")),  # resting
            pair(8, DAY0, 6, 0.47, 0.0, 0.0, "", True, states=("filled", "not_sent"))]     # never placed: no quote at all


async def the_preopen_files() -> None:
    bot = tally_bot()
    rows = preopen_rows_for_export()
    for p in rows:
        if p["key"] == "BTC-5M-0":
            p["orders"]["up"]["fill_ms"] = p["start_ms"] - 600_000  # first seen filled ten minutes before the start
    bot.preopen_rows = lambda: rows
    bot.preopen_review = lambda: preopen.review(rows, bot.market.now_ms())
    docs = await bot.cmd_export(req("/export", "预挂"))
    assert [d.filename for d in docs] == ["preopen-records-20261108.csv", "preopen-summary-20261108.csv"], docs
    assert docs[0].caption.startswith("预挂记录 6 条（全部日期，只有有成交的；其中有成交 6 条）：一条记录 = 一场的一个档位的一次挂单"), docs[0].caption
    recs = table(docs[0])
    assert recs[0] == list(m.Bot.PREOPEN_RECORD_COLUMNS) and len(recs) == 7
    col = lambda row: dict(zip(recs[0], row))
    first = col(recs[1])
    assert first == {"场次": rows[0]["slug"].split("@")[0], "卡片": "BTC 5分涨跌", "场次名": "BTC 5分涨跌 11-0x", "开场时间": "2026-11-07 00:50:00", "星期": "周六",
                     "档位价": "0.47", "每边份数": "100.0", "第几次挂": "1", "状态": "已结算", "挂出涨": "1", "挂出跌": "1", "成交涨": "100.0", "成交跌": "0.0", "成交份数": "100.0",
                     "成交额": "47.0", "结果": "涨", "盈亏": "53.0", "涨边盈亏": "53.0", "跌边盈亏": "", "按五五开预期": "3.0", "挂出时距开场（分钟）": "1433.3",
                     "首次成交距开场（分钟）": "10.0", "撤单用时（秒）": "0.6"}, first
    assert [col(r)["开场时间"] for r in recs[1:]] == sorted(col(r)["开场时间"] for r in recs[1:]), "the earliest window first"
    by = {(col(r)["开场时间"][-8:], col(r)["档位价"]): col(r) for r in recs[2:]}
    assert (by[("00:55:00", "0.47")]["结果"], by[("00:55:00", "0.47")]["盈亏"]) == ("跌", "-14.1")                       # 30 shares of the losing side at 47¢
    hedged = by[("00:10:00", "0.47")]
    assert (hedged["成交涨"], hedged["成交跌"], hedged["盈亏"], hedged["涨边盈亏"], hedged["跌边盈亏"], hedged["按五五开预期"]) == ("100.0", "100.0", "6.0", "53.0", "-47.0", "6.0")
    assert (by[("00:10:00", "0.43")]["盈亏"], by[("00:15:00", "0.47")]["结果"], by[("00:15:00", "0.47")]["盈亏"]) == ("-25.8", "50/50", "3.0")
    waiting = by[("00:20:00", "0.47")]
    assert (waiting["状态"], waiting["结果"], waiting["盈亏"], waiting["按五五开预期"]) == ("待结算", "", "", "1.5"), waiting
    # 全部: the unfilled quotes of the working records too — never one whose orders did not go out
    every = table((await bot.cmd_export(req("/export", "预挂", "全部")))[0])
    assert len(every) == 9 and {col(r)["状态"] for r in every[1:]} == {"已结算", "待结算", "已撤", "挂着"}, [col(r)["状态"] for r in every[1:]]
    assert (await bot.cmd_export(req("/export", "preopen", "all")))[0].caption.startswith("预挂记录 8 条（全部日期，含没成交的；其中有成交 6 条）")
    # a range: the windows that start on those days (Beijing)
    eve = await bot.cmd_export(req("/export", "预挂", "昨天"))
    assert len(table(eve[0])) == 3 and eve[0].caption.startswith("预挂记录 2 条（11-07 至 11-07 开场的，只有有成交的"), eve[0].caption
    assert len(table((await bot.cmd_export(req("/export", "预挂", "2026-11-08", "全部")))[0])) == 1 + 6
    # the tables: the review's own figures
    review = preopen.review(rows, bot.market.now_ms())
    tabs = table(docs[1])
    assert tabs[0] == list(m.Bot.PREOPEN_SUMMARY_COLUMNS) and docs[1].caption.startswith("预挂统计表（全部日期，归档的旧记录也计入）")
    names = [r[0] for r in tabs[1:]]
    assert [n for i, n in enumerate(names) if i == 0 or names[i - 1] != n] == ["合计", "按天", "按卡", "按档位", "按开始小时", "按方向", "周末对工作日"], names
    cell = lambda row: dict(zip(tabs[0], row))
    total = cell(tabs[1])
    assert (total["行"], int(total["挂出记录"]), int(total["场次"]), int(total["还挂着"]), int(total["有成交记录"]), float(total["成交份数"]), float(total["盈亏"])) == (
        "全部", review["total"]["records"], review["total"]["windows"], 1, 6, 540.0, review["total"]["pnl"]), total  # (280 up and 260 down)
    assert (int(total["挂出记录"]), int(total["场次"]), float(total["已结算份数"]), float(total["两边都成交盈亏"]), float(total["只成交一边盈亏"])) == (8, 7, 490.0, -19.8, 38.9), total
    assert float(total["盈亏"]) == round(53.0 - 14.1 + 6.0 - 25.8 + 3.0, 4) and int(total["待结算记录"]) == 1 and int(total["已撤没成交"]) == 1
    assert (float(total["按五五开预期"]), total["偏离σ"], int(total["两边都成交场次"]), int(total["只成交一边场次"]), int(total["只成交一边赢"])) == (
        review["ev"]["ev"], str(review["ev"]["z"]), review["ev"]["both"]["windows"], review["ev"]["one"]["windows"], review["ev"]["one"]["won"])
    levels = [cell(r) for r in tabs[1:] if r[0] == "按档位"]
    assert [(lv["档位价"], lv["每边份数"]) for lv in levels] == [("0.47", "100.0"), ("0.43", "100.0")] and float(levels[1]["盈亏"]) == -25.8 and float(levels[1]["按五五开预期"]) == 4.2, levels
    sides = {cell(r)["行"]: cell(r) for r in tabs[1:] if r[0] == "按方向"}
    assert (float(sides["涨"]["成交份数"]), float(sides["跌"]["成交份数"]), round(float(sides["涨"]["盈亏"]) + float(sides["跌"]["盈亏"]), 4)) == (280.0, 260.0, float(total["盈亏"])), sides
    assert (sides["涨"]["盈亏"], sides["跌"]["盈亏"], sides["跌"]["赢"], sides["跌"]["输"]) == ("91.9", "-69.8", "1", "2"), sides  # (down: two losses to "up", the 50/50 a gain of 3)
    assert {cell(r)["行"] for r in tabs[1:] if r[0] == "按天"} == {"11-07", "11-08"}
    # nothing recorded: said
    bot.preopen_rows = lambda: []
    bot.preopen_review = lambda: None
    assert await bot.cmd_export(req("/export", "预挂")) == "预挂没有记录（开盘前预挂没开过，或这段时间没有成交；加“全部”连没成交的也导出）"
    assert await bot.cmd_export(req("/export", "预挂", "全部")) == "预挂没有记录（开盘前预挂没开过，或这段时间没有挂单）"
    print("THE_PREOPEN_FILES_OK")


async def the_real_trader() -> None:
    """The real trading module's bot (LiveBot) on a real ledger: its working records in the first file; the archived ones —
    old quotes that filled nothing — not there, yet counted in the tables as the review page counts them."""
    book = preopen.Ledger(":memory:")
    rows = preopen_rows_for_export()
    old_start = DAY0 - 6 * m.DAY_MS
    # (the card's key as another record's: a card's latest window stays in the working records whatever its age)
    rows.append({"key": "BTC-5M-7", "name": "BTC 5分涨跌 旧", "slug": f"btc-updown-5m-{old_start // 1000}@1", "price": 0.47, "shares": 100.0, "start_ms": old_start,
                 "end_ms": old_start + 300_000, "phase": "closed", "accounted": True, "result": "", "attempt": 1,
                 "orders": {"up": {"state": "final", "filled": 0.0}, "down": {"state": "final", "filled": 0.0}}})
    for row in rows:
        book.save(row)
    assert book.archive_old(NOW) == 1 and len(book.rows) == len(rows) - 1, "the old withdrawn quote is in the archive"
    paper = tally_bot()
    bot = L.LiveBot.__new__(L.LiveBot)
    bot.preopen, bot.config, bot.market, bot.store = types.SimpleNamespace(ledger=book), paper.config, paper.market, paper.store
    bot.rules_clear = lambda seconds: True
    records, tables = await bot.cmd_export(req("/export", "预挂", "全部"))
    recs, tabs = table(records), table(tables)
    assert len(recs) == 1 + 8 and all("旧" not in r[2] for r in recs[1:]) and records.caption.startswith("预挂记录 8 条（全部日期，含没成交的；其中有成交 6 条）")
    total = dict(zip(tabs[0], tabs[1]))
    review = bot.preopen_review()
    assert (int(total["挂出记录"]), int(total["场次"]), int(total["已撤没成交"])) == (review["total"]["records"], review["total"]["windows"], review["total"]["withdrawn"]) == (9, 8, 2), total
    assert {dict(zip(tabs[0], r))["行"] for r in tabs[1:] if r[0] == "按天"} == {"11-02", "11-07", "11-08"}  # the archived quote's day is a line of the table
    assert (float(total["盈亏"]), float(total["成交份数"])) == (22.1, 540.0), "the archive adds no fill and no money"
    # the rule taker's records: the store's while none runs — none here
    assert await bot.cmd_export(req("/export", "规则")) == "规则吃单没有记录（没开过，或真实交易模块没启动）"
    print("THE_REAL_TRADER_OK")


async def the_rules_file() -> None:
    bot = tally_bot()
    start = DAY0 + 6 * 3_600_000  # 11-08 06:00 Beijing: gold's shut quarter hour
    recs = {
        "a": {"slug": "xau-updown-15m-1", "rule": "gold_shut", "card": "XAU-15M", "side": "up", "mode": "on", "name": "黄金 15分 06:00", "start_ms": start,
              "end_ms": start + 900_000, "market_id": "9", "index_set": 1, "cents": 62, "created_ms": start - 3_600_000, "result": "up", "pnl": 34.2,
              "orders": [{"shares": 100.0, "filled": 90.0, "avg": 0.62, "limit": 0.62, "state": "filled", "sent_wall": start - 1_799_000, "seen_wall": start - 1_799_400}],
              "seen": {"offer_ms": start - 1_800_000, "offer_ts": start - 1_800_200, "offer_max": 400.0, "cut_ms": start - 1_799_000, "cut_ts": start - 1_799_300,
                       "gone_ms": start - 1_797_000, "gone_ts": start - 1_797_200, "cents": 62}},
        "b": {"slug": "xau-updown-15m-1", "rule": "gold_shut", "card": "XAU-15M", "side": "up", "mode": "paper", "name": "黄金 15分 06:00", "start_ms": start,
              "end_ms": start + 900_000, "cents": 62, "orders": [{"shares": 100.0, "filled": 100.0, "avg": 0.60, "limit": 0.62, "paper": True, "state": "filled"}],
              "result": "up", "pnl": 40.0},
        "c": {"slug": "cl-updown-15m-2", "rule": "oil_shut", "card": "CL-15M", "side": "down", "mode": "on", "name": "原油 15分 06:15", "start_ms": start + 900_000,
              "end_ms": start + 1_800_000, "cents": 55, "orders": [], "result": "", "pnl": None}}
    bot.rules_records = lambda: recs
    doc = await bot.cmd_export(req("/export", "规则"))
    rows = table(doc)
    assert doc.filename == "rules-records-20261108.csv" and doc.caption.startswith("规则吃单 3 场（其中买到的 2 场）：一场一条规则一行") and rows[0] == list(m.Bot.RULES_COLUMNS)
    real, paper, oil = (dict(zip(rows[0], r)) for r in rows[1:])
    assert real == {"场次": "xau-updown-15m-1", "规则": "gold_shut", "规则名": "黄金休市 4 档", "场次名": "黄金 15分 06:00", "开场时间": "2026-11-08 06:00:00", "方向": "涨",
                    "模式": "真实", "规则价（¢）": "62", "成交份数": "90.0", "成交均价": "0.62", "成本": "55.8", "其中盘中买的份数": "0.0", "结果": "涨", "盈亏": "34.2",
                    "卖单出现时间": "2026-11-08 05:29:59", "卖单出现距开场（分钟）": "30.0", "卖单最多份数": "400.0", "多久被吃第一口（毫秒）": "900",
                    "多久被吃光（毫秒）": "3000", "开场前要了多少": "100.0", "开场前买到多少": "90.0"}, real
    assert (paper["模式"], paper["成交份数"], paper["成交均价"], paper["盈亏"], paper["卖单出现时间"]) == ("只记录", "100.0", "0.6", "40.0", ""), paper
    assert (oil["规则名"], oil["方向"], oil["成交份数"], oil["成交均价"], oil["结果"], oil["盈亏"], oil["开场前要了多少"]) == ("原油休市 4 档", "跌", "0.0", "", "", "", ""), oil
    assert rules.races(recs, "on", 0)[0]["gone_ms"] == 3000, "the race's own figures"
    bot.rules_records = lambda: {}
    assert await bot.cmd_export(req("/export", "rules")) == "规则吃单没有记录（没开过，或真实交易模块没启动）"
    print("THE_RULES_FILE_OK")


async def the_old_files_and_everything() -> None:
    """The three files the bot already sent, by their words; the whole records gzipped; and 全部."""
    bot = await TJ["build"]()
    bot.TALLY_PACE_S = bot.EXPORT_BREATH_S = 0.0

    async def none(path, **params):
        return []
    m.binance_spot, bot.binance_futures = none, none
    menu = await bot.cmd_export(req("/export"))
    assert menu.startswith("📦 复盘数据导出（都是 CSV 文件") and all(f"/export {w}" in menu for w in ("全部", "日记", "汇总", "样本", "预挂", "规则", "json", "涨跌"))
    assert await bot.cmd_export(req("/export", "别的")) == "没有“别的”这一种。\n\n" + menu
    same = lambda a, b: (a.filename, a.data, a.caption) == (b.filename, b.data, b.caption)
    assert same(await bot.cmd_export(req("/export", "日记", "恒生", "挂")), await bot.cmd_journal(req("/journal", "恒生", "挂")))
    assert same(await bot.cmd_export(req("/export", "journal")), await bot.cmd_journal(req("/journal")))
    assert same(await bot.cmd_export(req("/export", "汇总", "恒生")), await bot.cmd_summary(req("/summary", "恒生")))
    assert await bot.cmd_export(req("/export", "样本")) == await bot.cmd_samples(req("/samples"))  # (no sample on this bot: the same words)
    assert (await bot.cmd_export(req("/export", "日记", "没有这个"))).startswith("没有名字含“没有这个”的市场")
    # the whole records: the review page's JSON export, gzipped — every record, each with what the list rows leave out
    doc = await bot.cmd_export(req("/export", "json"))
    whole = json.loads(gzip.decompress(doc.data).decode("utf-8"))
    page = bot.journal_payload(full=True, history=0)
    assert doc.filename == "journal-full-20261005.json.gz" and doc.mime == "application/gzip" and doc.caption.startswith(f"交易日记完整记录 {len(bot.sim_trades())} 笔（全部）")
    assert [t["id"] for t in whole["trades"]] == [t["id"] for t in page["trades"]] and whole["trades"] == json.loads(json.dumps(page["trades"], ensure_ascii=False, default=str))
    assert whole["version"] == m.VERSION and whole["filter"] == "" and whole["total"] == json.loads(json.dumps(page["total"], default=str)) and any("entry" in t for t in whole["trades"])
    bot.EXPORT_JSON_BATCH = 2  # (several batches: the same file's records)
    again = json.loads(gzip.decompress((await bot.cmd_export(req("/export", "json"))).data).decode("utf-8"))
    assert again["trades"] == whole["trades"]
    some = await bot.cmd_export(req("/export", "json", "恒生", "吃"))
    part = json.loads(gzip.decompress(some.data).decode("utf-8"))
    want = {tid for tid, t in bot.sim_trades().items() if m.card_of(t) == "恒生指数" and not t.get("maker")}
    assert some.filename == "journal-full-20261005-恒生指数_吃单.json.gz" and {t["id"] for t in part["trades"]} == want and want and part["filter"] == "恒生指数 · 吃单"
    assert (await bot.cmd_export(req("/export", "json", "没有这个"))).startswith("没有名字含“没有这个”的市场")
    # 全部: the journal, the summary, then what else there is — here no sample, no pre-open record, no rule taker
    parts = await bot.cmd_export(req("/export", "全部"))
    docs = [x for x in parts if isinstance(x, m.Document)]
    # (1.154.68: the calibration's two files too — the report, and the snapshots: none here, the header alone)
    assert [d.filename for d in docs] == ["journal-20261005.csv", "summary-20261005.csv", "calib-report-20261005.txt", "calib-snapshots-20261005.csv",
                                          "tally-streaks-20260929-20261005.csv"], [d.filename for d in docs]
    words = [x for x in parts if not isinstance(x, m.Document)]
    assert words[0] == "已发 5 个文件；没有数据的：样本、预挂、规则。每个文件是什么、怎么单独要：/export" and words[1].startswith("📊 预挂按前面连涨 / 连跌｜29 张卡合计｜"), words
    # ...with the loops running the tally follows by itself, and the line says so
    rows = ledger(FILLS)
    bot.preopen_rows = lambda: rows
    sent: list = []

    async def answer(request, reply, markup=None, html_mode=False):
        sent.append(reply)
    bot.answer = answer
    bot.reference_tasks = [asyncio.get_running_loop().create_future()]
    parts = await bot.cmd_export(req("/export", "all"))
    assert [x.filename for x in parts[:-1]] == ["journal-20261005.csv", "summary-20261005.csv", "preopen-records-20261005.csv",
                                                "calib-report-20261005.txt", "calib-snapshots-20261005.csv"], parts[:-1]
    assert parts[-1] == "已发 5 个文件；涨跌统计（全部卡，近 7 天）还在读 K 线，算完随后发来；没有数据的：样本、规则。每个文件是什么、怎么单独要：/export", parts[-1]
    await bot.tally_job
    assert len(sent) == 1 and isinstance(sent[0][1], m.Document)
    print("THE_OLD_FILES_AND_EVERYTHING_OK")


async def an_answer_in_parts() -> None:
    """A command's answer of several parts goes out one after another; a single one as it always did; a large file gzipped,
    one too large even so said instead of sent."""
    bot = tally_bot()
    out: list = []

    class Telegram:
        async def send_document(self, chat, thread, doc):
            out.append(("file", chat, thread, doc.filename, len(doc.data), doc.caption, doc.mime))

    async def tell(chat, thread, text, markup=None, html_mode=False, **more):
        out.append(("text", chat, thread, text, markup, html_mode))
        return True
    bot.telegram, bot.tell = Telegram(), tell
    request = req("/export", "全部")
    small = m.Document("a.csv", b"x" * 100, "小的")
    await bot.answer(request, [small, "说一句", "", m.Document("b.csv", b"y" * 10, "")])
    assert out == [("file", 1, 0, "a.csv", 100, "小的", "text/csv"), ("text", 1, 0, "说一句", None, False), ("file", 1, 0, "b.csv", 10, "", "text/csv")], out
    out.clear()
    await bot.answer(request, "一句话", {"k": 1}, True)
    await bot.answer(request, small)
    await bot.answer(request, "")  # (a single answer, empty or not, goes as before)
    assert out == [("text", 1, 0, "一句话", {"k": 1}, True), ("file", 1, 0, "a.csv", 100, "小的", "text/csv"), ("text", 1, 0, "", None, False)], out
    # the command's own way: a handler's list reaches the chat
    out.clear()
    bot.handlers["/export"] = lambda r: [small, "好了"]
    bot.config = m.dataclasses.replace(bot.config, admin_id=42)
    await bot.process_message({"text": "/export 全部", "chat": {"id": 1, "type": "private"}, "from": {"id": 42}})
    assert [x[0] for x in out] == ["file", "text"] and out[1][3] == "好了", out
    # a large file is gzipped (its name and caption say so), in a thread; what it holds is the same
    old = m.DOCUMENT_GZIP_BYTES, m.DOCUMENT_MAX_BYTES
    try:
        m.DOCUMENT_GZIP_BYTES = 1_000
        big = m.Document("big.csv", ("一行,数据\n" * 2_000).encode("utf-8"), "大的")
        fit = await m.document_fit(big)
        assert fit.filename == "big.csv.gz" and fit.mime == "application/gzip" and gzip.decompress(fit.data) == big.data and len(fit.data) < 1_000
        assert fit.caption == "大的｜文件大（0 MB），已压缩成 .gz，解压后就是原文件" and await m.document_fit(fit) is fit and await m.document_fit(small) is small
        out.clear()
        await bot.answer(request, [big])
        assert out == [("file", 1, 0, "big.csv.gz", len(fit.data), fit.caption, "application/gzip")], out
        # ...and one past what a bot may send even so is said, the rest still sent
        m.DOCUMENT_MAX_BYTES = 50
        out.clear()
        await bot.answer(request, [big, m.Document("c.csv", b"z", "")])
        assert out[0][0] == "text" and out[0][3].startswith("❌ big.csv 压缩后还有 0 MB，超过 Telegram 机器人能发的 50 MB") and out[1][3] == "c.csv", out
    finally:
        m.DOCUMENT_GZIP_BYTES, m.DOCUMENT_MAX_BYTES = old
    # the help and the menu name both commands
    assert all(f"/{name}" in bot.cmd_help(None) and any(c.name == name for c in m.COMMANDS) and f"/{name}" in bot.handlers for name in ("export", "tally"))
    assert all(len(c.menu_entry()["description"]) <= 256 for c in m.COMMANDS) and len(m.HELP) < 4096
    print("AN_ANSWER_IN_PARTS_OK")


async def run() -> None:
    the_words()
    sums_added_up()
    await one_card()
    await several_cards()
    await in_the_background()
    await the_preopen_files()
    await the_real_trader()
    await the_rules_file()
    await the_old_files_and_everything()
    await an_answer_in_parts()
    print("EXPORT_OK")


if __name__ == "__main__":
    asyncio.run(run())
