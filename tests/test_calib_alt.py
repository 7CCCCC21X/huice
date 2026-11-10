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
print("CALIB_ALT_OK")
