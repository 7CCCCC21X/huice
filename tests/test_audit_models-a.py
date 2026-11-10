"""Audit regression tests for the models-a cluster."""
import sys, math, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
D = m.D


# --- MOD-WIDEN-CALIB: the big-move widen is measured against the day σ before SIGMA_CALIB's factor. Measured against the
# calibrated one, the 盘后 factor moved the in-session card's widen (and so its price), cancelled out of a big after-hours
# move's σ ahead, and the snapshot's sigma_raw (the priced σ divided by the factor) was not raw: the factor fed on itself ----
KST = dt.timezone(dt.timedelta(hours=9))


def _kr(mo: int, d: int, h: int, mi: int) -> int:
    return int(dt.datetime(2026, mo, d, h, mi, tzinfo=KST).timestamp() * 1000)


def _bj(mo: int, d: int, h: int, mi: int) -> int:
    return int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)


class _Clock:
    def __init__(self, now: int): self.now, self.config = now, None
    def now_ms(self) -> int: return self.now


def _bot(now: int, calib: dict, **env: str) -> m.Bot:
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "SKHYNIXUSDT", "KOSPI_BETA": "1",
                             "KOSPI_BETA_DYNAMIC": "off", **env})
    assert cfg.sigma_calib and cfg.big_move_sigmas == 1.5  # the defaults
    bot = m.Bot(cfg, m.Store(":memory:"), _Clock(now), None)
    for key in ("KOSPI", "SSE", "HSI"):
        bot.vols.estimates[key] = (0.012, 40, "test")
        bot.vols.bodies[key] = (0.009, 40)  # 1.122.0's open→close in-session σ
    bot.vols.calib = dict(calib)
    return bot


def _raw_day(key: str) -> float:
    """The day σ the closes give (no factor in the book)."""
    return _bot(_kr(11, 10, 12, 0), {}).vols.get(key, key)[0]


def _in_session(key: str, calib: dict, sigmas: float) -> tuple[m.CloseOdds, dict]:
    """The card in session with the live print ``sigmas`` raw day σ below the previous close, and its snapshot."""
    move = -sigmas * _raw_day(key)
    prev = {"KOSPI": D("4000"), "SSE": D("3900"), "HSI": D("25000")}[key]
    last = (prev * D(str(math.exp(move)))).quantize(D("0.01"))
    if key == "KOSPI":
        now = _kr(11, 10, 10, 30)
        bot = _bot(now, calib)
        bot.kospi.quote = m.IndexQuote("KOSPI", last, prev, None, None, None, now - 5000, "Naver", fetched_ms=now - 5000)
        odds = bot.kospi_odds(now)
    elif key == "SSE":
        now = _bj(11, 10, 10, 30)
        bot = _bot(now, calib)
        bot.cn.quote = m.IndexQuote("上证指数", last, prev, None, None, None, now - 5000, "腾讯")
        odds = bot.sse_odds(now)
    else:
        now = _bj(11, 10, 10, 30)
        bot = _bot(now, calib)
        bot.hsi.quote = m.FuturesQuote("x", last, prev, None, None, None, now - 5000, "etnet", last, "etnet", prev)
        odds = bot.hsi_odds(now)
    assert isinstance(odds, m.CloseOdds) and odds.mode == "盘中" and odds.direct, (key, odds)
    for other in {"KOSPI": ("sse_odds", "hsi_odds"), "SSE": ("kospi_odds", "hsi_odds"), "HSI": ("kospi_odds", "sse_odds")}[key]:
        setattr(bot, other, lambda now: None)
    setattr(bot, {"KOSPI": "kospi_odds", "SSE": "sse_odds", "HSI": "hsi_odds"}[key], lambda now, odds=odds: odds)
    bot.record_predictions(now)
    return odds, bot.store.get(f"pred:{key}:{now}")


# 1. in session: a 盘后 factor (fitted on the after-hours proxies' residuals) no longer moves the in-session card. Before the
#    fix KOSPI's widen on a −2.15σ day went 1.43 → 1.02 under a 盘后 factor of 1.4 (and to 2.0 under 0.7), the snapshot's
#    "raw" σ with it, although the card had no 盘中 factor
for key in ("KOSPI", "SSE", "HSI"):
    off, off_snap = _in_session(key, {}, 2.15)
    assert 1.4 < off.widen < 1.5, (key, off.widen)
    for f in (1.4, 0.7):
        on, snap = _in_session(key, {(key, "盘后"): (f, 20)}, 2.15)
        assert on.widen == off.widen and on.sigma == off.sigma and on.fair_down == off.fair_down, (key, f, on.widen, off.widen)
        assert on.sigma_day == off.sigma_day == _raw_day(key) and on.sigma_note == off.sigma_note, (key, f, on.sigma_day, on.sigma_note)
        assert snap["calib"] == 1.0 and snap["sigma_raw"] == off_snap["sigma_raw"] == off.sigma_daily * off.widen, (key, f, snap)
    # a 盘中 factor scales the in-session σ ahead (the widen as it was) and is divided back out of the snapshot's raw σ
    on, snap = _in_session(key, {(key, "盘中"): (1.4, 20)}, 2.15)
    assert on.widen == off.widen and abs(on.sigma - 1.4 * off.sigma) < 1e-12, (key, on.sigma, off.sigma)
    assert snap["calib"] == 1.4 and abs(snap["sigma_raw"] - off_snap["sigma_raw"]) < 1e-12, (key, snap)

# 2. after hours (HL KR200 −3% since the 15:30 anchor): the 盘后 factor scales the widened σ ahead as SIGMA_CALIB says (before
#    the fix it cancelled out: |move|·√R/1.5 whatever the factor), and the snapshot's sigma_raw is the σ the card prices with
#    the factor off (before: 1.43% instead of 2.00% under a factor of 1.4 — residuals scored 1.4× too large)
AH_NOW, AH_DAY = _kr(11, 10, 20, 0), dt.date(2026, 11, 10)


def _after_hours(bot: m.Bot, move: float) -> m.CloseOdds:
    bot.kospi.quote = m.IndexQuote("KOSPI", D("4000"), D("3990"), None, None, None, _kr(11, 10, 15, 35), "Naver",
                                   fetched_ms=_kr(11, 10, 15, 35))
    bot.kospi.daily[AH_DAY] = D("4000")
    bot.anchors["KOSPI"] = (m.Bot.kospi_day_close_ms(AH_DAY), D("1000"))
    bot.kospi_anchor_note = "15:30 一分钟K"
    px = D("1000") * D(str(math.exp(move)))
    bot.hl.quotes["KR200"] = m.HlQuote("xyz:KR200", px, None, px, None, None, AH_NOW - 30_000)
    odds = bot.kospi_odds(AH_NOW)
    assert isinstance(odds, m.CloseOdds) and odds.mode == "盘后" and odds.beta == 1.0, odds
    return odds


def _after_hours_snap(calib: dict, move: float = -0.03) -> tuple[m.CloseOdds, dict]:
    bot = _bot(AH_NOW, calib, HSI_FUTURES="off", SSE_INDEX="off")
    odds = _after_hours(bot, move)
    bot.record_predictions(AH_NOW)
    return odds, bot.store.get(f"pred:KOSPI:{AH_NOW}")


off, off_snap = _after_hours_snap({})
assert 1.4 < off.widen < 1.5 and abs(off_snap["sigma_raw"] - off.sigma_daily * off.widen) < 1e-15, (off.widen, off_snap)
for f in (1.4, 1.25, 0.7):
    on, snap = _after_hours_snap({("KOSPI", "盘后"): (f, 20)})
    assert on.widen == off.widen and on.sigma_day == off.sigma_day, (f, on.widen, off.widen)
    assert abs(on.sigma - f * off.sigma) < 1e-12 and (on.fair_up > off.fair_up) == (f > 1), (f, on.sigma, off.sigma)
    assert snap["calib"] == f and abs(snap["sigma_raw"] - off_snap["sigma_raw"]) < 1e-12, (f, snap["sigma_raw"], off_snap["sigma_raw"])

# 3. a stored factor within 0.005 of 1 is one VolBook.get does not apply: the snapshot divides out the factor its σ carried
#    (1), not the stored one (before: sigma_raw = σ / 1.004 and calib 1.004 on a σ priced without it)
on, snap = _after_hours_snap({("KOSPI", "盘后"): (1.004, 20)})
assert on.sigma == off.sigma and snap["calib"] == 1.0 and snap["sigma_raw"] == snap["sigma"] == off_snap["sigma_raw"], snap
vb = m.VolBook({}, None)
vb.calib = {("KOSPI", "盘后"): (1.004, 20), ("KOSPI", "盘中"): (1.3, 0), ("SSE", "盘后"): (0.8, 12)}
assert vb.factor("KOSPI", "盘后") == 1.0 and vb.factor("KOSPI", "盘中") == 1.0 and vb.factor("SSE", "盘后") == 0.8 and vb.factor("HSI", "盘后") == 1.0
assert vb.get("SSE", "SSE", calibrated=False) == m.VolBook({}, None).get("SSE", "SSE") and vb.get("SSE", "SSE")[0] == 0.8 * vb.get("SSE", "SSE", calibrated=False)[0]

# 4. the calibration loop through the real record_predictions → note_outcome → index_sigma_calibs: every target day a big
#    after-hours move (widened), its outcome exactly 1 true σ off (the true σ being the uncalibrated card's: no factor is
#    called for), starting from a 盘后 factor of 1.4. The factor now goes to 1; before it stuck at 1.077 for good (each row's
#    residual scored on a "raw" σ still divided by the factor in force fed that factor back)
truth = _after_hours(_bot(AH_NOW, {}, HSI_FUTURES="off", SSE_INDEX="off"), -0.03)
bot = _bot(AH_NOW, {}, HSI_FUTURES="off", SSE_INDEX="off")
_after_hours(bot, -0.03)
bot.record_predictions(AH_NOW)  # notes the fixture's own 11-10 close out of the way (that measures the factors afresh: none)
bot.vols.calib = {("KOSPI", "盘后"): (1.4, 20)}
factors = []
for i in range(8):
    t = AH_NOW + 1000 * (i + 1)
    bot.pred_last.clear()
    bot.record_predictions(t)
    snap = bot.store.get(f"pred:KOSPI:{t}")
    target = (dt.date(2025, 1, 1) + dt.timedelta(days=i)).isoformat()
    bot.store.put(f"pred:KOSPI:{t}", dict(snap, target=target))
    assert snap["widen"] > 1, snap  # a big-move day: the σ ahead widened
    close = snap["ref_raw"] * math.exp(snap["beta"] * snap["move"] + (1 if i % 2 else -1) * truth.sigma)
    bot.note_outcome("KOSPI", target, close, "test")
    factors.append(bot.vols.calib[("KOSPI", "盘后")][0])
assert all(abs(f - 1.0) < 1e-9 for f in factors), factors

print("AUDIT_MODELS_A_OK")
