"""1.154.69 — the user, after the model review of 11-26 (上证指数、SHEIN 希音的模型优化 → 只改代码，两个都继续做):

- SSE_SIGMA_EXTRA (default 1.5): the SSE card's in-session σ that many times wider on top of SIGMA_CALIB's factor. The
  in-session model had scored worse than Predict's own middle walked forward and lost buying 跌 on an index already down.
  The snapshot divides it back out of its raw σ, so the residuals SIGMA_CALIB measures stay on the estimate alone.
- A stock card's σ factor (measured as an index card's) is applied — each card and mode apart, and only where /calib's
  walk-forward check says the scaled σ scored better (stock_sigma_calibs). Before, "个股卡只评估、不套用".

Run: python tests/test_sigma_extra.py
"""
import datetime as dt
import math
import random
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

D = m.D


def bj(mo: int, d: int, h: int, mi: int) -> int:
    return int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)


class Clock:
    def __init__(self, now: int):
        self.now, self.config = now, None

    def now_ms(self) -> int:
        return self.now


def bot_at(now: int, store=None, **env: str) -> m.Bot:
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT,CXMTUSDT", **env})
    bot = m.Bot(cfg, store or m.Store(":memory:"), Clock(now), None)
    bot.vols.estimates["SSE"] = (0.012, 40, "test")
    bot.vols.bodies["SSE"] = (0.009, 40)
    return bot


# --- the setting ------------------------------------------------------------------------------------------------------------
assert m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}).sse_sigma_extra == 1.5, "the default the user chose"
assert m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SSE_SIGMA_EXTRA": "1"}).sse_sigma_extra == 1.0
assert m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SSE_SIGMA_EXTRA": "2.25"}).sse_sigma_extra == 2.25
for bad in ("0.5", "4"):
    try:
        m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SSE_SIGMA_EXTRA": bad})
    except ValueError:
        pass
    else:
        raise AssertionError(f"SSE_SIGMA_EXTRA={bad} taken: 1–3 only")

# --- the SSE card in session: σ × 1.5 on top of the factor, said in the note; after hours untouched -------------------------------
now = bj(11, 10, 10, 30)  # a Tuesday, the morning session
prev, last = D("3900"), D("3870")


def in_session(extra: str, calib: dict) -> tuple[m.CloseOdds, dict, m.Bot]:
    bot = bot_at(now, SSE_SIGMA_EXTRA=extra)
    bot.vols.calib = dict(calib)
    bot.cn.quote = m.IndexQuote("上证指数", last, prev, None, None, None, now - 5000, "腾讯")
    odds = bot.sse_odds(now)
    assert isinstance(odds, m.CloseOdds) and odds.mode == "盘中", odds
    bot.kospi_odds = bot.hsi_odds = lambda now: None
    bot.sse_odds = lambda now, odds=odds: odds
    bot.record_predictions(now)
    return odds, bot.store.get(f"pred:SSE:{now}"), bot


plain, plain_snap, plain_bot = in_session("1", {})
wide, wide_snap, wide_bot = in_session("1.5", {})
assert abs(wide.sigma_daily - 1.5 * plain.sigma_daily) < 1e-12 and wide.widen == plain.widen, (wide.sigma_daily, plain.sigma_daily)
assert "盘中再 ×1.5（SSE_SIGMA_EXTRA）" in wide.sigma_note and "SSE_SIGMA_EXTRA" not in plain.sigma_note, wide.sigma_note
assert wide.fair_up > plain.fair_up, "an index already down: a wider σ ahead gives 涨 more room"
assert wide_snap["extra"] == 1.5 and plain_snap["extra"] == 1.0 and wide_snap["calib"] == plain_snap["calib"] == 1.0
assert abs(wide_snap["sigma_raw"] - plain_snap["sigma_raw"]) < 1e-12, "the extra divided out: the residuals see the estimate alone"
assert abs(wide_snap["sigma"] - 1.5 * plain_snap["sigma"]) < 1e-12, "...while the snapshot's own σ is the one the card priced with"
# with a 盘中 factor too: both on the σ, both out of the raw one
both, both_snap, _ = in_session("1.5", {("SSE", "盘中"): (1.3, 20)})
assert abs(both.sigma_daily - 1.5 * 1.3 * plain.sigma_daily) < 1e-12 and both_snap["calib"] == 1.3
assert abs(both_snap["sigma_raw"] - plain_snap["sigma_raw"]) < 1e-12
assert wide_bot.sse_sigma_extra("盘后") == 1.0 and wide_bot.sse_sigma_extra("盘中") == 1.5
assert wide_bot.control_value("SSE_SIGMA_EXTRA") == "1.5"

# --- a stock card's factor: applied where the walk-forward check agrees, per card and mode ----------------------------------------
rnd = random.Random(7)
t0 = bj(9, 1, 10, 0)
rows = []
for i in range(24):
    day = (dt.date(2026, 9, 1) + dt.timedelta(days=i)).isoformat()
    # 宇树: the model's σ twice what the stock moves — the close comes in on the side the live price is on, as a σ half as
    # wide says: the scaled σ scores better walked forward, the factor (< 1) is applied
    gap = rnd.choice((-1, 1)) * 0.004
    close_u = 100 * math.exp(gap + rnd.gauss(0, 0.002))
    rows.append((f"outcome:UNITREEUSDT:{day}", close_u))
    # 长鑫: the residuals small as well (a factor < 1 measured), but the close on either side of the reference at random
    # while the card leaned one way — a sharper σ only scores worse: the check says no, nothing applied
    close_c = 100 * math.exp(rnd.gauss(0, 0.002))
    rows.append((f"outcome:CXMTUSDT:{day}", close_c))
    for k in range(6):
        t = t0 + i * 86_400_000 + k * 1_800_000
        rows.append((f"pred:UNITREEUSDT:{t}", {"key": "UNITREEUSDT", "t": t, "target": day, "mode": "盘中", "ref": 100.0, "ref_raw": 100.0,
                                                "eff": 100 * math.exp(gap), "up": 0.5, "move": gap, "beta": 1.0, "sigma": 0.008,
                                                "sigma_raw": 0.008, "R": 0.5}))
        rows.append((f"pred:CXMTUSDT:{t}", {"key": "CXMTUSDT", "t": t, "target": day, "mode": "盘中", "ref": 100.0, "ref_raw": 100.0,
                                             "eff": 100.4, "up": 0.5, "move": 0.0, "beta": 1.0, "sigma": 0.008, "sigma_raw": 0.008, "R": 0.5}))
store = m.Store(":memory:")
store.put_many(rows)
store.flush()
bot = bot_at(bj(9, 28, 10, 0), store)
measured = bot.stock_sigma_calibs()
assert ("UNITREEUSDT", "盘中") in measured and measured[("UNITREEUSDT", "盘中")][0] < 0.9, measured
assert ("CXMTUSDT", "盘中") not in measured, measured
assert m.calib_walk_scores([]) is None  # nothing scored: no check, nothing applied
assert bot.vols.calib == {**bot.index_sigma_calibs(), **measured}, "at the start: the indices' and the stock cards' together"
# the card prices with it: VolBook.get applies a stock key's factor as an index's
bot.vols.estimates["UNITREEUSDT"] = (0.04, 30, "test")
scaled, note = bot.vols.get("UNITREEUSDT", "sh", intraday=False)
unscaled, _ = bot.vols.get("UNITREEUSDT", "sh", intraday=False, calibrated=False)
assert abs(scaled - unscaled) < 1e-12, "the 盘后 factor: none measured for it"
scaled, note = bot.vols.get("UNITREEUSDT", "sh", intraday=True)
bot.vols.calib.pop(("UNITREEUSDT", "盘中"))
plain_in, _ = bot.vols.get("UNITREEUSDT", "sh", intraday=True)
assert abs(scaled - measured[("UNITREEUSDT", "盘中")][0] * plain_in) < 1e-12 and "快照残差校准" in note, (scaled, plain_in, note)
# a stock's close measures its own factors again, and nothing else's
bot.vols.calib = {("SSE", "盘中"): (1.3, 20), ("CXMTUSDT", "盘后"): (1.1, 15)}
calls = []
real = bot.index_sigma_calibs
bot.index_sigma_calibs = lambda: calls.append(1) or real()
last_day = (dt.date(2026, 9, 1) + dt.timedelta(days=24)).isoformat()
bot.note_outcome("UNITREEUSDT", last_day, 100.3, "t")
assert not calls and bot.vols.calib[("SSE", "盘中")] == (1.3, 20) and bot.vols.calib[("CXMTUSDT", "盘后")] == (1.1, 15)
assert {k: v for k, v in bot.vols.calib.items() if k[0] == "UNITREEUSDT"} == bot.stock_sigma_calibs(["UNITREEUSDT"])
# the switch off: none at all
bot.config = __import__("dataclasses").replace(bot.config, sigma_calib=False)
bot.note_outcome("UNITREEUSDT", last_day, 100.4, "t")
assert bot.vols.calib == {}

# --- /calib says it -------------------------------------------------------------------------------------------------------------
text = bot.calibration_text(full=False)
assert "个股卡：下面的逐日向前检验说校准更准时，SIGMA_CALIB 开着就按它定价，否则只评估——1.154.69）" in text, text
assert "逐日向前检验（σ 校准" in text and "（校准更准）" in text and "（校准没有更准）" in text, text
print("SIGMA_EXTRA_OK")
