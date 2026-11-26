"""1.148.0: two candidate improvements of the SSE in-session model, scored by /calib before any card uses them — an in-session
σ from the EWMA of the sessions' open→close moves plus today's own minutes (kept on each SSE 盘中 snapshot), and the model
blended with Predict's middle, its weight walked forward by target day.

Run: python tests/test_calib_alt.py
"""
import datetime as dt
import math
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

D = m.D

# --- the EWMA open→close σ: the recent days weigh most --------------------------------------------------------------------------
opens = [D("100")] * 40
calm, lively = D("100.5"), D("102")
closes = [calm] * 30 + [lively] * 10  # the last ten sessions moved four times as much
plain, _ = m.body_vol(opens, closes)
ewma, n_eff = m.body_vol_ewma(opens, closes)
assert 2 * plain < ewma < math.log(1.02) and 15 < n_eff < 40, (plain, ewma, n_eff)  # twice the plain average: the lively days count
assert m.body_vol_ewma(opens[:5], closes[:5]) is None

# --- today's own minutes: neighbouring minutes only (not over lunch), a day's worth --------------------------------------------
t0 = 1_800_000_000_000 // 60_000 * 60_000
mins = {t0 + i * 60_000: 3000 * (1.001 if i % 2 else 1.0) for i in range(30)}
mins.update({t0 + (120 + i) * 60_000: 2000.0 for i in range(5)})  # after the break, another level: the jump not counted
sigma, n = m.today_vol(mins)
assert n == 33 and abs(sigma - math.sqrt((29 * math.log(1.001) ** 2) / 33 * 240)) < 1e-12, (sigma, n)
assert m.today_vol({}) == (0.0, 0)

# --- Vols.alt_intraday: the EWMA blended with the prior, today's minutes counted in ----------------------------------------------
cls = m.VolBook
vols = cls({})
vols.record("SSE", [D("3000")] + [D("3000")] * 40, "test", [None] + [D("3000") * (D("1") - (D("0.004") if i % 2 else D("0.006"))) for i in range(40)], save=False)
base, n0 = vols.alt_intraday("SSE", "SSE")
assert n0 == 0 and base > 0
busy = {t0 + i * 60_000: 3000 * (1.004 if i % 2 else 1.0) for i in range(121)}  # two hours of 0.4% a minute
more, n1 = vols.alt_intraday("SSE", "SSE", busy)
assert n1 == 120 and more > 3 * base, (base, more)  # today's own minutes weigh as much as the days behind them
assert cls({}).alt_intraday("SSE", "SSE") is None  # no bars yet

# --- the blend, walked forward: each day weighted by the days before it -------------------------------------------------------
rows = []
for d in range(12):
    day = f"2026-10-{d + 1:02d}"
    for k in range(4):
        hit = float((d + k) % 2)
        # the model overconfident and often wrong, the market's middle mild and leaning right: the blend leans to the market
        rows.append({"target": day, "hit": hit, "up": 0.95 if (d + k) % 3 else 0.05, "mkt": 0.65 if hit else 0.35})
w = m.blend_weight(rows)
assert 0.0 <= w < 0.5, w
line = m.blend_line(rows)
assert line.startswith("  现行模型与盘口加权（逐日向前，28 条/7 日；每天的权重只用之前的日子拟合）：Brier ") and "｜盘口单独 " in line and f"按全部日子拟合的模型权重 {w:.1f}" in line, line
assert m.blend_line(rows[:20]) == ""  # five days: not enough to walk forward

# --- /calib prints both: the alternative beside the card's on the same snapshots, and the blends -----------------------------------
preds, outcomes = [], {}
for d in range(12):
    day = f"2026-10-{d + 1:02d}"
    up = (d % 2 == 0)
    outcomes[f"SSE:{day}"] = 3010.0 if up else 2990.0
    for k in range(3):
        preds.append({"key": "SSE", "t": d * 10 + k, "target": day, "mode": "盘中", "ref": 3000.0, "up": 0.3 if up else 0.7,
                      "eff": 3000.0, "mkt": 0.6 if up else 0.4, "R": 0.5, "sigma": 0.01, "sigma_raw": 0.01, "move": 0.0, "beta": 1.0,
                      "up_alt": 0.55 if up else 0.45, "sigma_alt": 0.012, "today_n": 30})
text = "\n".join(m.calibration_report(preds, outcomes))
assert "改进 σ（开盘到收盘按 12 日半衰期加权 + 当天已走的分钟波动；只评估，卡片未用）：36 条/12 日｜Brier 0.202" in text and "｜改进 σ 更准" in text, text
assert "现行模型与盘口加权（逐日向前" in text and "改进 σ与盘口加权（逐日向前" in text, text

# --- CnIndex.note_minute: continuous trading only (not the lunch break, not the closing auction), today's only -------------------
BJ = lambda d, h, mi: int(dt.datetime(2026, 11, d, h, mi, 5, tzinfo=m.BEIJING).timestamp() * 1000)  # 11-10 is a Tuesday
quote_at = lambda now, last: m.IndexQuote("上证指数", D(last), D("2990"), None, None, None, now, "腾讯", fetched_ms=now)
cn = m.CnIndex()
for i, (h, mi) in enumerate([(9, 31), (11, 29), (11, 35), (12, 59), (13, 0), (14, 56), (14, 58), (15, 1)]):
    cn.quote = quote_at(BJ(10, h, mi), 3000 + i)
    cn.note_minute(BJ(10, h, mi))
kept = [(9, 31), (11, 29), (13, 0), (14, 56)]
assert sorted(cn.minutes) == [BJ(10, h, mi) // 60_000 * 60_000 for h, mi in kept], sorted(cn.minutes)
assert [cn.minutes[k] for k in sorted(cn.minutes)] == [3000.0, 3001.0, 3004.0, 3005.0]
cn.error = "源失败"
cn.note_minute(BJ(10, 14, 50))
assert len(cn.minutes) == 4  # a failed read notes nothing
cn.error = ""
cn.quote = quote_at(BJ(11, 9, 40), 3100)
cn.note_minute(BJ(11, 9, 40))
assert cn.minutes == {BJ(11, 9, 40) // 60_000 * 60_000: 3100.0}  # the next day starts afresh


# --- the SSE 盘中 snapshot keeps what the alternative σ would have shown, beside the card's own --------------------------------------
cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off"})
store = m.Store(":memory:")
bot = m.Bot(cfg, store, m.Binance(cfg), None)
target = dt.date(2026, 11, 10)
card = m.close_odds("上证指数", D("3000"), D("3012"), 0.008, 0.6, target, D("0.01"), "11-09 收盘", "上证现货 3,012（盘中直接用现货）", "x")
bot.sse_odds, bot.kospi_odds, bot.hsi_odds = (lambda now: card), (lambda now: None), (lambda now: None)
now = BJ(10, 10, 30)
bot.record_predictions(now)
saved = [v for _, v in store.items("pred:SSE:")]
assert len(saved) == 1 and "up_alt" not in saved[0], saved  # no bars yet: nothing to set beside it
bot.vols.record("SSE", [D("3000")] * 41, "test", [None] + [D("3000") * (D("1") - (D("0.004") if i % 2 else D("0.006"))) for i in range(40)], save=False)
bot.cn.minutes = {BJ(10, 9, 30) + i * 60_000: 3000 * (1.001 if i % 2 else 1.0) for i in range(60)}
bot.pred_last.clear()
bot.record_predictions(now + 60_000)
snap = max((v for _, v in store.items("pred:SSE:")), key=lambda v: v["t"])
sigma, n = bot.vols.alt_intraday("SSE", "SSE", bot.cn.minutes)
want = m.close_odds("上证指数", D("3000"), D("3012"), sigma, 0.6, target, D("0.01"), "", "", "", widen=card.widen).fair_up
assert n == 59 and snap["today_n"] == 59 and abs(snap["sigma_alt"] - sigma * card.widen) < 1e-12 and abs(snap["up_alt"] - want) < 1e-12, snap
assert abs(snap["up"] - card.fair_up) < 1e-12 and abs(snap["up_alt"] - snap["up"]) > 0.01, snap  # the card's own figure unchanged beside it
after = m.close_odds("上证指数", D("3000"), D("3012"), 0.008, 1.0, target, D("0.01"), "11-09 收盘", "A50", "x", mode="盘后")
bot.sse_odds = lambda now: after
bot.pred_last.clear()
bot.record_predictions(now + 120_000)
snap = max((v for _, v in store.items("pred:SSE:")), key=lambda v: v["t"])
assert snap["mode"] == "盘后" and "up_alt" not in snap, snap  # after hours: the card's model only

# --- 1.149.0: plain /calib shows the daily 涨跌 cards alone (the indices, the stock cards); /calib all shows everything -------------
store.put("outcome:SSE:2026-11-10", 3020.0)
store.put("pred:OTHER:1", dict(snap, key="OTHER", t=1))  # a key no daily card has (a retired stock card, say)
short = bot.calibration_text(full=False)
assert short.startswith("📐 概率模型回测·日涨跌卡") and "📐 SSE·盘中" in short and "OTHER" not in short, short
assert short.endswith("全市场快照、15 分钟窗口和影子样本等其余部分：/calib all") and "全市场快照（" not in short, short
whole = bot.calibration_text()
assert "📐 OTHER·盘后" in whole and "其余部分" not in whole, whole
req = lambda *args: m.Request("calib", list(args), 1, 0, 1)
import asyncio
assert asyncio.run(bot.cmd_calib(req())) == short

# --- 1.154.68: /calib all (and /export 校准) as two files — the whole report, and the snapshots it scores beside their closes ---------
bot.EXPORT_BREATH_S = 0
rows_in_store = len([1 for _ in store.items("pred:")])
for answer in (asyncio.run(bot.cmd_calib(req("all"))), asyncio.run(bot.cmd_calib(req("全部"))),
               asyncio.run(bot.cmd_export(m.Request("export", ["校准"], 1, 0, 1)))):
    report, snaps, said = answer
    assert isinstance(report, m.Document) and report.filename.startswith("calib-report-") and report.filename.endswith(".txt"), report
    assert report.mime == "text/plain" and report.data.decode("utf-8-sig") == whole
    assert isinstance(snaps, m.Document) and snaps.filename.startswith("calib-snapshots-") and snaps.mime == "text/csv", snaps
    assert said.startswith("已发 2 个文件") and f"模型快照 {rows_in_store} 条（其中 3 条已有收盘结果）" in said, said
import csv as _csv
import io as _io
table = list(_csv.reader(_io.StringIO(snaps.data.decode("utf-8-sig"))))
assert tuple(table[0]) == m.Bot.CALIB_COLUMNS and len(table) == 1 + rows_in_store, table[0]
col = {name: i for i, name in enumerate(table[0])}
sse = [r for r in table[1:] if r[col["代码"]] == "SSE"]
assert len(sse) == 3 and all(r[col["卡片"]] == "上证指数" and r[col["收盘"]] == "3020.0" and r[col["结果"]] == "涨" for r in sse), sse
assert [r[col["模式"]] for r in sse] == ["盘中", "盘中", "盘后"] and float(sse[1][col["备选σ"]]) > 0 and sse[0][col["备选σ"]] == "", sse
assert abs(float(sse[0][col["收盘对原始参考（对数）"]]) - math.log(3020 / 3000)) < 1e-6 and float(sse[0][col["模型涨"]]) == round(card.fair_up, 4), sse[0]
other = [r for r in table[1:] if r[col["代码"]] == "OTHER"]
assert len(other) == 1 and other[0][col["收盘"]] == "" and other[0][col["结果"]] == "", other  # no close for it: unscored, kept
assert "/export 校准 —— 两个文件" in bot.export_menu()
print("CALIB_ALT_OK")
