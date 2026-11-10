"""Audit regression tests for the models-b cluster."""
import sys, math, random, re
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m


# --- MOD-FIT-WALKFORWARD-STRIKE: /calib's proxy-fit walk-forward must price P(close > shown ref), the reference the
# hit is judged on (the Predict target price when the card used one), not P(close > ref_raw) ------------------------
def fit_walkforward(strike_factor: float) -> tuple[float, float]:
    """(Brier on the 拟合模型 line of calibration_report, Brier of the same walk-forward priced on the shown reference)."""
    rng = random.Random(3)
    preds, outcomes = [], {}
    for i in range(40):
        day = f"2026-{6 + i // 28:02d}-{i % 28 + 1:02d}"
        ref_raw = 3000.0
        strike = ref_raw * strike_factor
        move = rng.gauss(0, 0.006)
        close = ref_raw * math.exp(move + rng.gauss(0, 0.005))
        outcomes[f"SSE:{day}"] = close
        for j in range(3):
            eff = ref_raw * math.exp(move) * strike / ref_raw
            up = 1 - m.norm_cdf(math.log(strike / eff) / 0.005)
            row = {"key": "SSE", "t": i * 10 + j, "target": day, "mode": "盘后", "ref": strike, "ref_raw": ref_raw,
                   "eff": eff, "up": up, "move": move, "beta": 1.0, "sigma": 0.005, "sigma_raw": 0.005, "R": 1.0}
            if strike != ref_raw:
                row["strike"] = strike
            preds.append(row)
    text = "\n".join(m.calibration_report(preds, outcomes))
    line = next(l for l in text.splitlines() if l.startswith("  逐日向前检验（") and "拟合模型" in l)
    shown = float(re.search(r"拟合模型 Brier ([0-9.]+)", line).group(1))
    done = [dict(p, close=outcomes[f"SSE:{p['target']}"]) for p in preds]
    for r in done:
        r["hit"] = 1.0 if r["close"] > r["ref"] else 0.5 if r["close"] == r["ref"] else 0.0
        r["y"] = math.log(r["close"] / r["ref_raw"])
    days = sorted({r["target"] for r in done})
    ps, hs = [], []
    for day in days[m.CALIB_MIN_DAYS:]:
        a, b, k = m.fit_proxy([r for r in done if r["target"] < day])
        for r in (r for r in done if r["target"] == day):
            ps.append(m.norm_cdf((a + b * r["move"] - math.log(r["ref"] / r["ref_raw"])) / (k * math.sqrt(r["R"]))))
            hs.append(r["hit"])
    return shown, m.brier_scores(ps, hs)[0]


shown, expected = fit_walkforward(1.004)  # Predict's target 0.4% above the local close
assert abs(shown - round(expected, 3)) < 1e-9, (shown, expected)
assert abs(shown - 0.144) < 1e-9 and abs(shown - 0.220) > 0.05, shown
shown0, expected0 = fit_walkforward(1.0)  # no strike: ref == ref_raw, the line is what it always was
assert abs(shown0 - round(expected0, 3)) < 1e-9, (shown0, expected0)

# --- MOD-YAHOO-SORT: parse_yahoo_daily keeps one bar per date (the later stamp), so Yahoo's live duplicate of today
# with a null open no longer raises TypeError, and two numeric opens no longer order the copies by open --------------
import datetime as _dt, json as _json
from decimal import Decimal as _D


def yahoo_chart(opens: list) -> bytes:
    kst = _dt.timezone(_dt.timedelta(hours=9))
    stamps = [int(_dt.datetime(2026, 11, d, h, mi, tzinfo=kst).timestamp()) for d, h, mi in ((9, 9, 0), (10, 9, 0), (10, 15, 30))]
    return _json.dumps({"chart": {"result": [{"meta": {"gmtoffset": 32400}, "timestamp": stamps,
                        "indicators": {"quote": [{"open": opens, "close": [4005, 4020, 4021.5]}]}}]}}).encode()


bars = m.parse_yahoo_daily(yahoo_chart([4000, 4010, None]))
assert [b[0] for b in bars] == [_dt.date(2026, 11, 9), _dt.date(2026, 11, 10)], bars
assert bars[1][2] == _D("4021.50") and bars[1][1] == _D("4010"), bars
bars = m.parse_yahoo_daily(yahoo_chart([4000, 4012, 4011]))
assert len(bars) == 2 and bars[1][2] == _D("4021.50") and bars[1][1] == _D("4011"), bars
bars = m.parse_yahoo_daily(yahoo_chart([4000, None, 4010]))
assert len(bars) == 2 and bars[1] == (_dt.date(2026, 11, 10), _D("4010"), _D("4021.50")), bars

# --- MOD-KOSPI-AFTER-CLOSE: 15:30-15:33 KST the KRX session still reads live (cards roll over at KRX_SETTLED), so
# kospi_odds prices today's already-decided close 盘中 with the 1-minute floor; record_predictions must not save that as
# a prediction (it would be scored by /calib and fed into the (KOSPI, 盘中) σ factor). Before the close it still saves --
_kst = _dt.timezone(_dt.timedelta(hours=9))
_at = lambda h, mi, s=0: int(_dt.datetime(2026, 11, 10, h, mi, s, tzinfo=_kst).timestamp() * 1000)  # a Tuesday
_cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "SSE_INDEX": "off"})
_bot = m.Bot(_cfg, m.Store(":memory:"), m.Binance(_cfg), None)
_bot.sse_odds = _bot.hsi_odds = lambda now: None
_now = _at(15, 31)
_bot.kospi.quote = m.IndexQuote("KOSPI", _D("4105.20"), _D("4100.00"), None, None, None, _at(15, 30, 40), "Naver", fetched_ms=_now)
_odds = _bot.kospi_odds(_now)
assert isinstance(_odds, m.CloseOdds) and _odds.mode == "盘中" and _odds.target == _dt.date(2026, 11, 10), _odds  # the premise
_bot.record_predictions(_now)
assert [v for _, v in _bot.store.items("pred:KOSPI")] == [], list(_bot.store.items("pred:KOSPI"))
assert "KOSPI" not in _bot.pred_last  # the skipped tick does not use up the half-hour slot
_bot2 = m.Bot(_cfg, m.Store(":memory:"), m.Binance(_cfg), None)
_bot2.sse_odds = _bot2.hsi_odds = lambda now: None
_now = _at(15, 20)
_bot2.kospi.quote = m.IndexQuote("KOSPI", _D("4105.20"), _D("4100.00"), None, None, None, _now - 5_000, "Naver", fetched_ms=_now)
_bot2.record_predictions(_now)
_rows = [v for _, v in _bot2.store.items("pred:KOSPI")]
assert len(_rows) == 1 and _rows[0]["mode"] == "盘中" and _rows[0]["target"] == "2026-11-10", _rows

print("AUDIT_MODELS_B_OK")
