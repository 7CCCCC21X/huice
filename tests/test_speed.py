"""/speed (1.145.0): every index source asked once a pace — the request times, how often the figure changes, the stops
on a refusal or repeated failures, the rounds that never ask one website twice at once, the report and its advice.

Run: python tests/test_speed.py
"""
import asyncio
import datetime as dt
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

BJ = lambda *a: int(dt.datetime(2026, *a, tzinfo=m.BEIJING).timestamp() * 1000)
MON = BJ(11, 9, 10, 0)    # Monday 10:00, Beijing: Shanghai, Hong Kong (cash and futures) and Seoul all trade
SUN = BJ(11, 8, 16, 15)   # Sunday: nothing trades
TX_SSE = ('v_sh000001="1~上证指数~000001~3888.37~3936.52~3930~' + "~".join(["0"] * 24) + '~20260924161400~x";').encode("gbk")
SOURCES = [("上证实时", "腾讯", "https://qt.gtimg.cn/q=sh000001"), ("上证实时", "新浪", "https://hq.sinajs.cn/list=sh000001"),
           ("A50实时", "新浪CFD", "https://hq.sinajs.cn/list=hf_CHA50CFD"), ("恒指现货", "腾讯", "https://qt.gtimg.cn/q=hkHSI"),
           ("恒指现货", "新浪", "https://hq.sinajs.cn/list=rt_hkHSI"), ("恒指期货", "etnet", "https://www.etnet.com.hk/www/tc/futures/index.php"),
           ("恒指期货", "新浪CFD", "https://hq.sinajs.cn/list=hf_HSI"),
           ("KOSPI", "Naver", "https://polling.finance.naver.com/api/realtime/domestic/index/KOSPI"),
           ("KOSPI200", "Naver", "https://polling.finance.naver.com/api/realtime/domestic/index/KPI200")]


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


class TG:
    def __init__(self): self.sent = []
    async def call(self, *a, **k): return True
    async def send(self, chat, thread, text, reply_markup=None, parse_mode=None): self.sent.append(text)


def unit_checks():
    # rounds: no website twice in a round — Sina serves four of the feeds, so four rounds; the rest fill the first ones
    rounds = m.speed_rounds(SOURCES)
    assert [[f"{f}/{n}" for f, n, _ in r] for r in rounds] == [["上证实时/腾讯", "上证实时/新浪", "恒指期货/etnet", "KOSPI/Naver"],
                                                               ["A50实时/新浪CFD", "恒指现货/腾讯", "KOSPI200/Naver"], ["恒指现货/新浪"],
                                                               ["恒指期货/新浪CFD"]], rounds
    assert all(len({m.SourceHealth.host(u) for _, _, u in r}) == len(r) for r in rounds)
    # the figures: a source that moves every 3 seconds, asked every second (its quote times carry seconds), one timeout among them
    base = BJ(11, 9, 10, 0) + 1_000
    samples = [{"ok": True, "ms": 100 + 10 * i, "at": base + i * 1000 + 2000, "last": 3000.0 + i // 3, "q": base + (i // 3) * 3000} for i in range(12)]
    samples.insert(5, {"ok": False, "ms": 8000, "at": base + 5500, "error": "请求超时"})
    st = m.speed_stats(samples)
    assert (st["n"], st["ok"], st["errors"], st["error"], st["changes"], st["step_ms"], st["lag_ms"]) == (13, 12, 1, "请求超时", 3, 3000, 3000), st
    assert (st["median_ms"], st["p90_ms"], st["max_ms"]) == (155.0, 200, 210), st  # the failed request's 8 s is no answer time
    # quote times in whole minutes (Tencent's Composite, 1.145.2): no lag read off them, the minute turning over is no change, and the
    # step comes from the figure's own changes — three seconds, not "约 60 秒一变"; without quote times likewise
    whole = [{**x, "q": BJ(11, 9, 10, 0) + (i // 6) * 60_000} for i, x in enumerate(samples) if x["ok"]]
    assert (m.speed_stats(whole)["lag_ms"], m.speed_stats(whole)["changes"], m.speed_stats(whole)["step_ms"]) == (None, 3, 3000), m.speed_stats(whole)
    bare = [{"ok": True, "ms": 50, "at": i * 1000, "last": float(i // 4), "q": 0} for i in range(13)]
    assert m.speed_stats(bare)["changes"] == 3 and m.speed_stats(bare)["step_ms"] == 4000 and m.speed_stats(bare)["lag_ms"] is None
    empty = m.speed_stats([{"ok": False, "ms": 10, "at": 0, "error": "HTTP 403: 访问被拒绝"}])
    assert (empty["ok"], empty["median_ms"], empty["p90_ms"], empty["max_ms"], empty["changes"], empty["step_ms"]) == (0, None, None, None, 0, None)
    # the report: per feed, the sources' lines and the advice
    clean = {"n": 30, "ok": 30, "errors": 0, "error": "", "median_ms": 300, "p90_ms": 450, "max_ms": 900, "changes": 10, "step_ms": 3000, "lag_ms": 2000}
    slow = {**clean, "median_ms": 1400, "p90_ms": 2100, "max_ms": 3000}
    paged = {**clean, "changes": 3, "step_ms": 10_000, "lag_ms": None}
    feeds = [{"feed": "上证实时", "open": True, "now_s": 5, "sources": [{"name": "腾讯", "stats": clean, "stopped": ""},
                                                                      {"name": "新浪", "stats": {**empty, "n": 1}, "stopped": "被拒（HTTP 403）"}]},
             {"feed": "A50实时", "open": True, "now_s": 5, "sources": [{"name": "新浪CFD", "stats": {**clean, "ok": 2, "n": 5, "errors": 3, "error": "请求超时"},
                                                                      "stopped": "连续失败 3 次"}]},
             {"feed": "恒指期货", "open": True, "now_s": 10, "sources": [{"name": "etnet", "stats": paged, "stopped": ""}]},
             {"feed": "KOSPI", "open": True, "now_s": 10, "sources": [{"name": "Naver", "stats": slow, "stopped": ""}]},
             {"feed": "恒指现货", "open": True, "now_s": 1, "sources": [{"name": "腾讯", "stats": clean, "stopped": ""}]}]
    text = m.speed_text(feeds, 1.0, 30, MON, 4)
    for want in ("⏱️ 指数查价测速 v" + m.VERSION, "每个源每 1 秒查一次、各 30 秒；同一网站一次只测一个源，共 4 轮约 120 秒",
                 "【上证实时】交易中｜现在每 5 秒查一次",
                 "✅ 腾讯：30/30 次成功｜请求 中位 0.3 秒、九成在 0.5 秒内、最长 0.9 秒｜数值变了 10 次，约 3.0 秒一变，报价时间比现在晚 2.0 秒",
                 "❌ 新浪：1 次全失败｜HTTP 403: 访问被拒绝｜已停：被拒（HTTP 403）",
                 "👉 可以改成每 1 秒查：实际约 1.3 秒一次（请求 0.3 秒 + 循环间隔 1 秒），平均早约 2.1 秒看到新数值",
                 "⚠️ 新浪CFD：2/5 次成功", "｜失败 3 次：请求超时｜已停：连续失败 3 次", "👉 每 1 秒查会出错（请求超时），先保持现在的每 5 秒",
                 "👉 可以改成每 1 秒查：实际约 1.3 秒一次（请求 0.3 秒 + 循环间隔 1 秒），平均早约 4.6 秒看到新数值；数据源本身约 10 秒才变一次，多数请求拿到的数不变",
                 "👉 请求本身就慢（九成在 2.1 秒内）：每秒查也只能约 2.4 秒一次", "👉 现在的每 1 秒已经接近最快，改了也早不了多少", "把这份结果发给我"):
        assert want in text, (want, text)
    assert "现在都休市" not in text and "约 3.0 秒一变；数据源本身" not in text  # a 3-second source against a 1.3-second read: no such note
    shut = m.speed_text([{**feeds[0], "open": False}], 1.0, 30, SUN, 1)
    assert "⚠️ 现在都休市：只测得出请求耗时和会不会被拒" in shut and "【上证实时】休市｜现在每 5 秒查一次" in shut and "｜休市，数值不动" in shut, shut
    assert "（开盘后再测一次确认数值多久一变）" in shut and "数值变了" not in shut
    assert "没有在用的指数数据源" in m.speed_text([], 1.0, 30, SUN, 0)
    # 1.145.2: a delayed feed (Tencent's Hong Kong index, 15 minutes behind) and one that never moves while its market trades are said so,
    # and the advice rests on the fresh source after them
    late = {**clean, "lag_ms": 905_000}
    still = {**clean, "changes": 0, "step_ms": None, "lag_ms": 118_000}
    stuck = {**clean, "changes": 0, "step_ms": None, "lag_ms": None}
    hk = m.speed_text([{"feed": "恒指现货", "open": True, "now_s": 3, "sources": [{"name": "腾讯", "stats": late, "stopped": ""},
                                                                              {"name": "新浪", "stats": {**clean, "lag_ms": 1400}, "stopped": ""}]},
                       {"feed": "恒指期货", "open": True, "now_s": 10, "sources": [{"name": "etnet", "stats": paged, "stopped": ""},
                                                                               {"name": "新浪CFD", "stats": still, "stopped": ""}]},
                       {"feed": "KOSPI", "open": True, "now_s": 10, "sources": [{"name": "Naver", "stats": stuck, "stopped": ""}]}], 0.5, 30, MON, 4)
    for want in ("报价时间比现在晚 905 秒｜⚠️ 落后约 15 分钟（延迟行情），不按它定间隔", "👉 可以改成每 1 秒查：实际约 1.3 秒一次（请求 0.3 秒 + 循环间隔 1 秒），平均早约 1.1 秒看到新数值",
                 "✅ 新浪CFD：30/30 次成功", "数值变了 0 次，报价时间比现在晚 118 秒｜⚠️ 落后约 2 分钟（延迟行情），不按它定间隔",
                 "数值变了 0 次｜⚠️ 交易中却一直没变，不按它定间隔", "👉 这一路没有又新鲜又在动的源，先保持现在的每 10 秒"):
        assert want in hk, (want, hk)


async def run():
    unit_checks()
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "ADMIN_USER_ID": "42", "SYMBOLS": "UNITREEUSDT"})
    tg = TG()
    bot = m.Bot(cfg, m.Store(":memory:"), FM(MON), tg)
    m.SOURCE_HEALTH.off = frozenset({"eastmoney"})  # SOURCES_OFF=eastmoney, as the deployment has it
    # the feeds the bot reads: their sources switched on, whether each market trades, the cadence it is read at now
    feeds = bot.speed_feeds(MON)
    assert [(f["feed"], [s[0] for s in f["sources"]], f["now_s"]) for f in feeds] == [  # 1.145.2: the cadences /speed showed, Sina first for Hong Kong
        ("上证实时", ["腾讯", "新浪"], 1), ("A50实时", ["新浪CFD"], 1), ("恒指现货", ["新浪", "腾讯"], 1), ("恒指期货", ["etnet", "新浪CFD"], 2),
        ("KOSPI", ["Naver"], 1), ("KOSPI200", ["Naver"], 1)], feeds
    opened = {f["feed"]: f["open"] for f in feeds}
    assert opened["上证实时"] and opened["恒指现货"] and opened["恒指期货"] and opened["KOSPI"] and opened["KOSPI200"], opened
    assert not any(f["open"] for f in bot.speed_feeds(SUN))
    last, quoted = feeds[0]["sources"][0][3](TX_SSE)  # 腾讯's own parser, as the feed reads it: the figure and its quote time
    assert (float(last), quoted) == (3888.37, BJ(9, 24, 16, 14)), (last, quoted)
    assert len(m.speed_rounds([(f["feed"], s[0], s[1]) for f in feeds for s in f["sources"]])) == 4
    m.SOURCE_HEALTH.off = frozenset()

    # a run: Tencent answers and moves every third answer; Sina refuses (403: left alone at once); the A50 CFD times out (three, then
    # left alone); etnet answers — and no website is ever asked twice at once
    asked: dict[str, int] = {}
    busy: dict[str, int] = {}
    worst: dict[str, int] = {}

    async def fetch(url, extra=None, record=True):
        assert record is False, "the test must not move the live sources' order"
        host = m.SourceHealth.host(url)
        busy[host] = busy.get(host, 0) + 1
        worst[host] = max(worst.get(host, 0), busy[host])
        try:
            await asyncio.sleep(0.005)
            asked[url] = asked.get(url, 0) + 1
            if "list=sh000001" in url:
                raise m.RemoteError("HTTP 403: 访问被拒绝")
            if "hf_CHA50CFD" in url:
                raise m.RemoteError("网络错误 (URLError: timed out)")
            k = asked[url]
            return f"{3000 + k // 3}|{MON + (k // 3) * 3000 + 1000}".encode()
        finally:
            busy[host] -= 1
    m.fetch_source = fetch

    def parse(raw):
        price, quoted = raw.decode().split("|")
        return m.D(price), int(quoted)
    fake = [{"feed": "上证实时", "open": True, "now_s": 5, "sources": [("腾讯", "https://qt.gtimg.cn/q=sh000001", {}, parse),
                                                                     ("新浪", "https://hq.sinajs.cn/list=sh000001", {}, parse)]},
            {"feed": "A50实时", "open": True, "now_s": 5, "sources": [("新浪CFD", "https://hq.sinajs.cn/list=hf_CHA50CFD", {}, parse)]},
            {"feed": "恒指期货", "open": True, "now_s": 10, "sources": [("etnet", "https://www.etnet.com.hk/www/tc/futures/index.php", {}, parse)]}]
    bot.speed_feeds = lambda now_ms: fake
    tested, ran = await bot.speed_test(0.03, 0.4)
    got = {(f["feed"], s["name"]): s for f in tested for s in f["sources"]}
    tx, sina, cfd, etnet = got[("上证实时", "腾讯")], got[("上证实时", "新浪")], got[("A50实时", "新浪CFD")], got[("恒指期货", "etnet")]
    assert ran == 2 and max(worst.values()) == 1, (ran, worst)  # Sina's two sources in two rounds
    assert sina["stopped"] == "被拒（HTTP 403）" and sina["stats"]["n"] == 1 and asked["https://hq.sinajs.cn/list=sh000001"] == 1, sina
    assert cfd["stopped"] == "连续失败 3 次" and cfd["stats"]["n"] == 3 and cfd["stats"]["errors"] == 3, cfd
    assert tx["stopped"] == "" and tx["stats"]["errors"] == 0 and tx["stats"]["ok"] >= 6 and tx["stats"]["changes"] >= 2 and tx["stats"]["step_ms"] == 3000, tx
    assert 6 <= etnet["stats"]["n"] <= 16 and etnet["stopped"] == "", etnet  # every 0.03 s for 0.4 s, no faster
    assert bot.speed_pool is not None and m.HTTP_POOL.get() is None  # its own threads, the context left as it was

    # the command: its pace checked; run inline here (no background loops), the note first, then the report
    req = lambda *args: m.Request("/speed", list(args), 1, 0, 42)
    assert (await bot.cmd_speed(req("9"))).startswith("用法：/speed") and (await bot.cmd_speed(req("x"))).startswith("用法：/speed")
    m.SPEED_ROUND_SECONDS = 0.2
    report = await bot.cmd_speed(req("0.5"))
    assert tg.sent and tg.sent[-1].startswith("⏱️ 正在测指数查价速度：每个源每 0.5 秒查一次、各 0.2 秒，共 2 轮约 0.4 秒"), tg.sent
    assert report.startswith("⏱️ 指数查价测速 v") and "【上证实时】交易中｜现在每 5 秒查一次" in report and "已停：被拒（HTTP 403）" in report, report
    assert "/speed 0.5  指数查价测速（开盘时用）" in m.HELP and bot.handlers["/speed"] == bot.cmd_speed, m.HELP[:400]
    entry = next(c for c in m.COMMANDS if c.name == "speed").menu_entry()
    assert entry["command"] == "speed" and len(entry["description"]) <= 256, entry  # Telegram's menu limit
    print("SPEED_OK")


asyncio.run(run())
