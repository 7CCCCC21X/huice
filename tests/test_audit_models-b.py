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

print("AUDIT_MODELS_B_OK")
