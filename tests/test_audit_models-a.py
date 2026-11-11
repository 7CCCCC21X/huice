"""Audit regression tests for the models-a cluster."""
import asyncio, sys, math, time, types, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L
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


# --- MOD-STRIKE-SIGMA-DAY: settle_on_strike re-prices a card against Predict's own target price and rebuilt it without the
# whole day's σ, so close_odds kept sigma_daily — in session the open-to-close part, after hours (1.150.0) the σ with the
# 盘后 factor — as CloseOdds.sigma_day, the σ SIM_AGAINST_MOVE_SIGMAS counts the day's move in. With Predict's figure known
# the rule fired early (σ ahead below the day's) or late (a 盘中 / 盘后 factor above 1: a buy let into the move it was set
# to refuse, a resting bid kept). Now a known strike changes neither that σ nor the verdict. Real card builders → odds_items
# → settle_on_strike → sim_markets → sim_against_move / sim_withdraw_reason ----
GUARD = {"SIM_AGAINST_MOVE_SIGMAS": "1.5", "SIM_AGAINST_MOVE_INDEX": "on", "SIM_WAYS": "both"}  # both: resting bids stand
SKH = "SKHYNIXUSDT"


def _kospi_live(calib: dict, sigmas: float) -> tuple[m.Bot, int, str]:
    """In session: the live KOSPI ``sigmas`` raw day σ below the previous close."""
    now = _kr(11, 10, 10, 30)
    bot = _bot(now, calib, **GUARD)
    prev = D("4000")
    last = (prev * D(str(math.exp(-sigmas * _raw_day("KOSPI"))))).quantize(D("0.01"))
    bot.kospi.quote = m.IndexQuote("KOSPI", last, prev, None, None, None, now - 5000, "Naver", fetched_ms=now - 5000)
    return bot, now, "KOSPI"


def _kospi_after(calib: dict, sigmas: float) -> tuple[m.Bot, int, str]:
    """After hours: HL KR200 ``sigmas`` raw day σ below its 15:30 anchor (β 1)."""
    bot = _bot(AH_NOW, calib, HSI_FUTURES="off", SSE_INDEX="off", **GUARD)
    _after_hours(bot, -sigmas * _raw_day("KOSPI"))
    return bot, AH_NOW, "KOSPI"


def _skhynix_live(calib: dict, sigmas: float) -> tuple[m.Bot, int, str]:
    """In session: SK hynix's own print ``sigmas`` day σ below the previous close (a stock card: the rule needs no
    SIM_AGAINST_MOVE_INDEX)."""
    now = _kr(11, 10, 10, 30)
    bot = _bot(now, calib, SIM_AGAINST_MOVE_SIGMAS="1.5", SIM_WAYS="both")
    bot.vols.estimates[SKH], bot.vols.bodies[SKH] = (0.03, 40, "test"), (0.02, 40)
    prev = D("1000000")
    last = (prev * D(str(math.exp(-sigmas * bot.vols.get(SKH, "kr")[0])))).quantize(D("1000"))
    bot.stocks.closes[SKH] = m.StockMarket.baseline(m.StockTicker("kr", "000660"), m.STOCK_MARKETS["kr"], "Naver", dt.date(2026, 11, 9), prev)
    bot.stocks.live[SKH] = m.IndexQuote("SK하이닉스", last, prev, None, None, None, now - 5000, "Naver", fetched_ms=now - 5000)
    bot.snapshots[SKH] = {"quote": types.SimpleNamespace(price=D("700"), timestamp_ms=now)}
    return bot, now, SKH


def _seen(bot: m.Bot, now: int, key: str) -> tuple[m.CloseOdds, m.SimMarket]:
    """The card's odds as shown and its SimMarket, through the real odds_items → settle_on_strike → sim_markets."""
    shown = next(odds for title, odds in bot.odds_items(now) if bot.predict_key(title) == key)
    assert isinstance(shown, m.CloseOdds), shown
    return shown, next(mk for mk in bot.sim_markets(now) if mk.key == key)


CASES = [  # (card, SIGMA_CALIB factors, the move in raw day σ, Predict's target price off our reference,
    #          the card's sigma_daily / its day σ, 1.5 day σ reached); the comments: what the strike did before the fix
    (_skhynix_live, {}, 1.3, D("-1000"), 0.69, False),  # in session: blocked at 1.27 day σ, said "1.8 个日 σ"
    (_kospi_live, {}, 1.3, D("0.5"), 0.76, False),      # blocked at 1.3, "1.7 个日 σ"
    (_kospi_live, {("KOSPI", "盘中"): (1.5, 21), ("KOSPI", "盘后"): (1.0, 21)}, 1.6, D("0.5"), 1.14, True),  # let through
    (_kospi_after, {("KOSPI", "盘后"): (1.4, 20)}, 1.6, D("0.5"), 1.40, True),  # the 1.150.0 card's raw day σ: let through
    (_kospi_after, {("KOSPI", "盘后"): (0.7, 20)}, 1.3, D("0.5"), 0.70, False),  # blocked, "1.9 个日 σ"
    (_kospi_after, {}, 1.3, D("0.5"), 1.00, False),  # no factor after hours: one σ either way, as before
]
for build, calib, sigmas, off, ratio, reached in CASES:
    label = (build.__name__, calib, sigmas)
    bot, now, key = build(calib, sigmas)
    card = bot.kospi_odds(now) if key == "KOSPI" else bot.contract_odds(SKH, D("700"), now, now)
    assert isinstance(card, m.CloseOdds) and card.direct == (build is not _kospi_after), (label, card)
    day = bot.vols.get(key, "KOSPI" if key == "KOSPI" else "kr", calibrated=False)[0]
    assert card.sigma_day == day and abs(card.sigma_daily / day - ratio) < 0.005, (label, card.sigma_daily, day)
    slug = m.predict_slug(bot.config.predict_slugs[key], card.target)
    bot.predict.books[key] = m.PredictBook(key, slug, "1", "t", ((D("0.10"), D("50")),), ((D("0.12"), D("50")),), now)
    unsettled, plain = _seen(bot, now, key)  # Predict's figure not known yet: the card as built
    assert unsettled == card and plain.sigma_day == day, (label, unsettled, plain.sigma_day)
    bot.predict.strikes[slug] = (card.ref + off, time.monotonic())
    shown, mk = _seen(bot, now, key)
    assert shown.ref == card.ref + off and "Predict 目标价" in shown.ref_note, (label, shown.ref)
    # the whole day's σ goes along to the shown odds and their SimMarket (before: shown.sigma_day == card.sigma_daily)
    assert shown.sigma_day == card.sigma_day == mk.sigma_day == plain.sigma_day, (label, shown.sigma_day, mk.sigma_day, card.sigma_day)
    # ...and nothing else moves: the σ ahead, the widen and its note as the card's, the prices those of the strike alone
    assert shown.sigma == card.sigma and shown.widen == card.widen and shown.sigma_note == card.sigma_note, label
    assert m.dataclasses.replace(shown, sigma_day=0.0) == m.dataclasses.replace(m.close_odds(
        shown.name, shown.ref, shown.effective, shown.sigma_daily, shown.remaining, shown.target, shown.tick, shown.ref_note,
        shown.proxy_note, shown.sigma_note, shown.unit, shown.beta, shown.mode, widen=shown.widen), warn=shown.warn, sigma_day=0.0), label
    # the rule's verdict and a resting 涨 bid's fate are the same with and without Predict's figure
    verdict, before = bot.sim_against_move(mk, "up"), bot.sim_against_move(plain, "up")
    assert bool(verdict) == bool(before) == reached, (label, verdict, before)
    assert bot.sim_against_move(mk, "down") == "", label  # a fall never blocks the side it favours
    bid = {"kind": "close", "key": key, "side": "up", "maker": True, "status": "resting", "order": 50, "shares": 0}
    assert bot.sim_withdraw_reason(bid, mk, now) == bot.sim_withdraw_reason(bid, plain, now) == verdict, (label, bot.sim_withdraw_reason(bid, mk, now))
    if reached:
        assert verdict.startswith(f"今日已跌 {abs(mk.move) * 100:.1f}%、{abs(mk.move) / day:.1f} 个日 σ") and abs(mk.move) / day >= 1.5, (label, verdict)
# odds without a day σ of their own (sigma_day 0, the CloseOdds default) still fall back to their sigma_daily
bot, now, key = _kospi_after({("KOSPI", "盘后"): (1.4, 20)}, 0.5)
bare = m.dataclasses.replace(bot.kospi_odds(now), sigma_day=0.0)
bot.predict.strikes[m.predict_slug(bot.config.predict_slugs["KOSPI"], bare.target)] = (bare.ref + D("0.5"), time.monotonic())
assert bot.settle_on_strike("KOSPI", bare).sigma_day == bare.sigma_daily != _raw_day("KOSPI")


# --- MOD-WINDOW-STALE-PRICE: an up/down card takes its odds from the price in hand as if it were this instant's, and the one
# check of that price's age was the display's (PRICE_STALE_MS: a minute on a 15-minute card, five on a daily one). While the
# price feed stalled (the stream silent or reconnecting, the REST read in a rate-limit cooldown) and Predict's book went on
# moving, the trader bought the side the stale price made look cheap: a 59-s-old price with four minutes left is 19¢ off per
# σ of the move it has not seen. Now a price read longer ago than 2% of the time left (5 s at least) is not traded on — the
# card holds its suggestion (SimMarket.hold) and a resting order is withdrawn (SimMarket.price_problem → sim_requote_reason) —
# while the odds stay on the card until the documented minute. The daily crypto cards' five minutes are the same defect,
# under the same rule; a stock card is judged by its index's age while the index is the price its odds are taken at ----
WT0 = _bj(11, 4, 11, 0)   # BTC 15分涨跌 11:00–11:15 Beijing (11-03 22:00–22:15 ET)
WT1 = WT0 + 15 * 60_000
W4 = WT1 - 4 * 60_000     # four minutes left: before the last 180 s (no new resting order) and the last 90 s (no order at all)
WLINE = D("100000.00")
BTC15 = next(s for s in m.WINDOW_UPDOWN_MARKETS if s.key == "BTC-15M")
WENV = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "SKHYNIXUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "SIM_MARKETS": "updown",
        "SIM_MAKER_POINTS": "off"}  # the rest at its default: SIM_WAYS=taker, SIM_EDGE_CENTS=10, ten shares a buy on these cards


def _window_bot(now: int, cls: type = m.Bot, **env: str) -> tuple:
    """A real Bot (or LiveBot) whose BTC 15-minute card is on the 11:00–11:15 window: the line at 100,000, σ 50%."""
    spec = BTC15.window_spec(WT0)
    store = m.Store(":memory:")
    store.put(f"updown:{BTC15.before(spec).slug}:end", {"open": WT0 - 60_000, "close": str(WLINE), "read_ms": WT0 + 2_000})
    store.put(f"updown:{spec.slug}:start", {"open": spec.start_ms, "close": str(WLINE), "read_ms": WT0 + 2_000})
    bot = cls(m.Config.from_env({**WENV, **env}), store, _Clock(now), None)
    card = bot.updowns["BTC-15M"]
    assert isinstance(card, m.SpotWindowMarket) and card.spec == spec, card.spec
    card.sigma, card.sigma_ms = 0.5, now
    bot.predict.info[spec.slug] = {"outcomes": ["Up", "Down"], "created_ms": 0, "question": "BTC Up or Down - November 3, 10:00PM-10:15PM ET"}
    return bot, card, spec


def _priced(card: m.UpDownMarket, price: object, at: int) -> None:
    """The card's price as a push or a read at ``at`` left it."""
    card.price, card.priced_ms = D(str(price)), at
    if isinstance(card, m.IndexUpDownMarket):
        card.index_ms = at


def _wbook(bot: m.Bot, key: str, slug: str, bid: str, ask: str, at: int) -> None:
    """Predict's book of the card's market, read at ``at``: 涨 bid / ask, 300 shares each."""
    bot.predict.books[key] = m.PredictBook(key, slug, "702", "t", ((D(bid), D("300")),), ((D(ask), D("300")),), at, None)


def _looks(bot: m.Bot, key: str, *nows: int) -> dict:
    """The trader's two looks at each moment — the step (sim_decide) and the express lane — and the records after them."""
    async def go() -> None:
        for now in nows:
            bot.market.now = now
            await (await bot.sim_decide(now))
            await bot.sim_express_look(key, now)
    asyncio.run(go())
    return dict(bot.sim_trades())


def _mk(bot: m.Bot, key: str, now: int) -> m.SimMarket:
    return next(x for x in bot.sim_markets(now) if x.key == key)


# 1. the finding's case: four minutes left, the card's price standing at the line for 59 / 30 / 6 s while the fresh book shows
#    Up 67/70 — it has priced a +1σ move of those 59 s (the true fair Up: 0.69). The card still shows its 50¢ (the display's
#    minute is not over) but holds its suggestion, and neither the step nor the express lane buys. Before: 吃跌 10 @ 33¢ at
#    every look — 100 shares, the window's cap, within the minute
for age in (59_000, 30_000, 6_000):
    bot, card, spec = _window_bot(W4)
    _wbook(bot, "BTC-15M", spec.slug, "0.67", "0.70", W4)
    _priced(card, WLINE, W4 - age)
    odds = card.odds(W4)
    assert isinstance(odds, m.UpDownOdds) and abs(odds.fair_up - 0.5) < 0.001, (age, odds)  # still priced: PRICE_STALE_MS is the display's
    mk = _mk(bot, "BTC-15M", W4)
    got = _looks(bot, "BTC-15M", W4)
    assert got == {}, (age, [(t["label"], t["shares"], round(t["price"], 4)) for t in got.values()])  # (before: 吃跌 10 @ 0.3366, twice)
    stale = f"现货价 {age // 1000} 秒未更新，超过离结束 4.0 分钟时的上限 5 秒"
    assert mk.hold == stale + "；暂不给建议" and bot.updown_hold(card, odds, W4) == (mk.hold, "") and abs(mk.fair_up - 0.5) < 0.001, (age, mk.hold)
    assert mk.price_problem == card.price_problem(W4) == stale, (age, mk.price_problem)
    pay = bot.updown_payload(card, W4)  # the page: the odds and the warning, no direction framed
    assert "missing" not in pay and abs(pay["fair_up"] - 0.5) < 0.001 and pay["warn"] == pay["predict"]["hold"] == mk.hold, pay
    assert pay["predict"]["edges"] and not any(e["best"] for e in pay["predict"]["edges"]), pay["predict"]["edges"]

# 2. a fresh price trades as before: at the line with a real edge in the book (涨 offered at 35¢) the taker buys 涨 — a second
#    old, and at the very limit (5 s old with four minutes left); with the price where the book has it, nothing (the control)
for age in (1_000, 5_000):
    bot, card, spec = _window_bot(W4)
    _wbook(bot, "BTC-15M", spec.slug, "0.30", "0.35", W4)
    _priced(card, WLINE, W4 - age)
    mk = _mk(bot, "BTC-15M", W4)
    assert mk.hold == "" and mk.price_problem == "" and card.price_problem(W4) == "", (age, mk.hold, mk.price_problem)
    got = _looks(bot, "BTC-15M", W4)
    assert got and all((t["label"], t["shares"], t["status"]) == ("吃涨", 10.0, "filled") for t in got.values()), (age, got)
seen = D(f"{float(WLINE) * math.exp(0.5 * math.sqrt(59_000 / m.YEAR_MS)):.2f}")  # where the price is, a σ of 59 s on
bot, card, spec = _window_bot(W4)
_wbook(bot, "BTC-15M", spec.slug, "0.67", "0.70", W4)
_priced(card, seen, W4 - 500)
mk = _mk(bot, "BTC-15M", W4)
assert abs(mk.fair_up - 0.69) < 0.005 and mk.hold == "" and _looks(bot, "BTC-15M", W4) == {}, (mk.fair_up, mk.hold)
# ...and the documented minute still pauses the odds themselves (README: 推送一分钟没来就暂停概率)
_priced(card, WLINE, W4 - 61_000)
assert card.odds(W4).startswith("现货价停在 ") and card.odds(W4).endswith("（61 秒未更新），暂停概率"), card.odds(W4)
assert "missing" in bot.updown_payload(card, W4) and all(x.key != "BTC-15M" for x in bot.sim_markets(W4))

# 3. the limit: 2% of the time left to the window's end, 5 s at least. What it bounds — a σ of unseen move shifts the fair
#    price at the line by Φ(√(age / left)) − ½, whatever σ: 19¢ for the 59 s at four minutes that used to pass, 5.6¢ under
#    the share, 9¢ at the floor where the orders stop (90 s left)
assert (m.UpDownMarket.TRADE_STALE_SHARE, m.UpDownMarket.TRADE_STALE_MIN_MS) == (0.02, 5_000)
assert (m.IndexUpDownMarket.PRICE_STALE_MS, m.UpDownMarket.PRICE_STALE_MS) == (60_000, 300_000)  # the display's pauses, as they were
shift = lambda age_s, left_s: m.norm_cdf(math.sqrt(age_s / left_s)) - 0.5
assert 0.18 < shift(59, 240) < 0.20 and all(0.056 < shift(0.02 * left, left) < 0.057 for left in (900, 600, 300)) and 0.09 < shift(5, 90) < 0.095
bot, card, spec = _window_bot(W4)
for left_s, limit_ms in ((840, 16_800), (600, 12_000), (300, 6_000), (250, 5_000), (240, 5_000), (91, 5_000)):
    now = WT1 - left_s * 1000
    for age, held in ((limit_ms - 200, False), (limit_ms + 200, True), (-3_000, False)):  # (−3 s: a push stamped after the look's clock was read)
        _priced(card, WLINE, now - age)
        assert bool(card.price_problem(now)) is held and bool(card.advice_problem(card.odds(now), now)) is held, (left_s, age, card.price_problem(now))
_priced(card, WLINE, WT1 - 840_000 - 17_000)
assert card.price_problem(WT1 - 840_000) == "现货价 17 秒未更新，超过离结束 14.0 分钟时的上限 16.8 秒", card.price_problem(WT1 - 840_000)
card.sigma_ms = WT1 - 840_000 - m.IndexUpDownMarket.SIGMA_STALE_MS - 1  # σ too old as well: its own reason comes first, as before
assert card.advice_problem(card.odds(WT1 - 840_000), WT1 - 840_000) == "波动率超过一天未更新；暂不给建议"

# 4. a resting order: under a hold it stands unjudged (sim_requote_reason: "no fresh view of the market: the order stands"),
#    and before there was a hold it stood on the stale fair price. Now the step withdraws it, and the next look with a fresh
#    price places it afresh. Ten minutes left: 挂涨 36¢ goes on a fresh price; half a minute later the price has not come
#    again (31 s old, the limit there 11.4 s) while the others' bids have fallen away to 33¢ — they saw the fall
N1 = WT0 + 5 * 60_000
bot, card, spec = _window_bot(N1, SIM_WAYS="both")
_wbook(bot, "BTC-15M", spec.slug, "0.36", "0.40", N1)
_priced(card, WLINE, N1 - 1_000)
tid = f"{spec.slug}|up|挂"
got = _looks(bot, "BTC-15M", N1)
assert {k: (t["label"], t["status"], t["order"], t["price"]) for k, t in got.items()} == {tid: ("挂涨", "resting", 10.0, 0.36)}, got
N2 = N1 + 30_000
card.sigma_ms = N2
_wbook(bot, "BTC-15M", spec.slug, "0.33", "0.37", N2)
mk = _mk(bot, "BTC-15M", N2)
why = "现货价 31 秒未更新，超过离结束 9.5 分钟时的上限 11.4 秒，撤单"
assert mk.price_problem and mk.hold and bot.sim_requote_reason(got[tid], mk, N2) == why, (mk.hold, bot.sim_requote_reason(got[tid], mk, N2))
said = bot.sim_why(mk, m.TradeMap(got), N2)["maker"]["sides"][0]["text"]  # the page's 为什么挂 row says what the next look does
assert said.startswith("挂单中：36.0¢×10 份") and said.endswith("；下一轮撤掉：" + why.removesuffix("，撤单")), said
got = _looks(bot, "BTC-15M", N2)  # withdrawn by the step; nothing placed or bought on the stale price (the stale 50¢ against a 37¢ ask)
assert {k: (t["status"], t["withdrawn"]["why"], t["withdrawn"]["rule"]) for k, t in got.items()} == {tid + "#1": ("cancelled", why, True)}, got
N3 = N2 + 2_000  # the feed is back: a fresh price, the book where it was — the order is placed afresh and stands
_priced(card, WLINE, N3 - 500)
_wbook(bot, "BTC-15M", spec.slug, "0.36", "0.40", N3)
got = _looks(bot, "BTC-15M", N3)
assert {k: (t["status"], t["price"]) for k, t in got.items()} == {tid: ("resting", 0.36), tid + "#1": ("cancelled", 0.36)}, got
assert bot.sim_requote_reason(got[tid], _mk(bot, "BTC-15M", N3), N3) == ""

# 5. a stock card under LEAD_QUOTE=on: while its exchange quote leads (the index at the start × the quote's ratio to its own
#    start) the odds are the quote's, not the index's, and nothing changes there — how long a quote may go unread stays
#    LEAD_STALE_MS's business (15 s). Once the quote leads no more, or is only recorded (shadow), the index is the price in
#    use and its age counts. 宇树 15 分, four minutes left, the quote +0.07% since the start
U15 = m.WINDOW_UPDOWN_MARKETS[0]
uspec = U15.window_spec(WT0)
assert U15.key == "UNITREE-15M" and uspec.end_ms + 60_000 == WT1


def _lead_bot(index_age: int, lead_age: int | None) -> tuple:
    """A real Bot whose 宇树 card has its index read ``index_age`` ms ago and its exchange quote fetched ``lead_age`` ms ago."""
    store = m.Store(":memory:")
    store.put(f"updown:{U15.before(uspec).slug}:end", {"open": WT0 - 60_000, "close": "90.0000", "read_ms": WT0 + 2_000})
    store.put(f"updown:{uspec.slug}:start", {"open": uspec.start_ms, "close": "90.0000", "read_ms": WT0 + 2_000})
    store.put(f"updown:{uspec.slug}:lead", {"price": "150.00", "at": WT0 + 500, "read_ms": WT0 + 1_000, "source": "腾讯"})  # the quote at the start (note_lead)
    bot = m.Bot(m.Config.from_env(WENV), store, _Clock(W4), None)
    card = bot.updowns["UNITREE-15M"]
    assert type(card) is m.IndexUpDownMarket and card.spec == uspec, card.spec
    card.sigma, card.sigma_ms, card.lead_mode = 0.5, W4, "on"
    _priced(card, "90.0500", W4 - index_age)
    if lead_age is not None:
        card.lead = m.IndexQuote("宇树科技", D("150.10"), D("149.00"), None, None, None, W4 - lead_age - 300, "腾讯", "交易中", W4 - lead_age)
    bot.predict.info[uspec.slug] = {"outcomes": ["Up", "Down"], "created_ms": 0, "question": ""}
    _wbook(bot, "UNITREE-15M", uspec.slug, "0.45", "0.50", W4)  # 涨 offered at 50¢ against a fair 68¢ (the quote's) or 65¢ (the index's)
    return bot, card


for index_age, lead_age, led, problem in (
        (1_000, 2_000, True, ""),                    # both fresh: the quote leads
        (30_000, 2_000, True, ""),                   # the quote leads: the odds are its, whatever the index's age — as before
        (1_000, 16_000, False, ""),                  # the quote dropped (LEAD_STALE_MS): the index, fresh
        (8_000, 16_000, False, "指数价 8 秒未更新"),   # ...the index itself too old: held (before: bought)
        (8_000, None, False, "指数价 8 秒未更新")):    # no exchange quote at all: the index's age
    bot, card = _lead_bot(index_age, lead_age)
    assert (card.effective(W4)[1] != "") is led and card.effective(W4)[0] == (D("90.0600") if led else D("90.0500")), (index_age, lead_age, card.effective(W4))
    mk = _mk(bot, "UNITREE-15M", W4)
    want = f"{problem}，超过离结束 4.0 分钟时的上限 5 秒" if problem else ""
    assert mk.price_problem == card.price_problem(W4) == want and mk.hold == (want + "；暂不给建议" if want else ""), (index_age, lead_age, mk.hold)
    got = _looks(bot, "UNITREE-15M", W4)
    if problem:
        assert got == {}, (index_age, lead_age, got)
    else:
        assert got and all((t["label"], t["shares"]) == ("吃涨", 10.0) for t in got.values()), (index_age, lead_age, got)
bot, card = _lead_bot(8_000, 2_000)
card.lead_mode = "shadow"  # the quote only recorded: the card prices on the index, and it is the index's age that counts
assert card.effective(W4) == (D("90.0500"), "") and card.price_problem(W4) == "指数价 8 秒未更新，超过离结束 4.0 分钟时的上限 5 秒"

# 6. the daily crypto cards (BTC / ETH / BNB 日涨跌): PRICE_STALE_MS is five minutes there and they trade until half an hour
#    before the end. 40 minutes left: a 294-s-old price was traded on (0.4·√(294 / 2400) = 14¢ off per σ of unseen move); the
#    limit there is 48 s — over the 30-s cadence of the REST read that prices the card while its stream is quiet. Twelve hours
#    from the end 2% is far more than the five minutes after which the card shows no odds at all: nothing changes there
DAY = next(s for s in m.DAILY_UPDOWN_MARKETS if s.key == "BTC-DAY")
day = DAY.day_spec(dt.date(2026, 11, 4))
assert m.UpDownMarket.PRICE_SECONDS == 30 and m.Config.from_env(WENV).sim_crypto_day_quiet == 30
for left_min, age, held in ((40, 294_000, True), (40, 49_000, True), (40, 47_000, False), (40, 31_000, False), (720, 294_000, False)):
    now = day.end_ms + 60_000 - left_min * 60_000
    store = m.Store(":memory:")
    store.put(f"updown:{day.slug}:start", {"open": day.start_ms, "close": "100000", "read_ms": day.start_ms + 61_000})
    bot = m.Bot(m.Config.from_env(WENV), store, _Clock(now), None)
    card = bot.updowns["BTC-DAY"]
    assert type(card) is m.UpDownMarket and card.spec.slug == day.slug, card.spec
    card.sigma, card.sigma_ms = 0.45, now
    _priced(card, "100000", now - age)
    bot.predict.info[day.slug] = {"outcomes": ["Up", "Down"], "created_ms": 0}
    _wbook(bot, "BTC-DAY", day.slug, "0.63", "0.66", now)  # the book has seen a rise the card's price has not: 跌 offered at 37¢
    mk = _mk(bot, "BTC-DAY", now)
    got = _looks(bot, "BTC-DAY", now)
    if held:
        want = f"币安价格 {age // 1000} 秒未更新，超过离结束 40.0 分钟时的上限 48 秒"
        assert mk.price_problem == want and mk.hold == want + "；暂不给建议" and got == {}, (left_min, age, mk.hold, got)
        assert bot.updown_payload(card, now)["warn"] == mk.hold and "missing" not in bot.updown_payload(card, now)
    else:
        assert mk.hold == "" and mk.price_problem == "" and got and all((t["label"], t["shares"]) == ("吃跌", 10.0) for t in got.values()), (left_min, age, mk.hold, got)

# 7. the real trader: an order decided on a fresh price and held back a moment (a cooldown, a sign-in) is judged again right
#    before it is signed (LiveBot.live_taker_recheck) — a price gone stale meanwhile voids it, a resting order as a taker — and
#    a taker's unfilled rest is cancelled (live_taker_stale). The SDK test suite's key (tests/test_trading.py: never a real
#    one); nothing is signed or sent here
lb, card, spec = _window_bot(W4, cls=L.LiveBot, SIM_WAYS="both", LIVE="on", PREDICT_PRIVATE_KEY="0x" + "a" * 64, PREDICT_API_KEY="k")
_wbook(lb, "BTC-15M", spec.slug, "0.36", "0.40", W4)


def _pending(maker: bool, price: float) -> dict:
    """A real order decided three seconds ago, not sent yet: 挂涨 at ``price``, or 吃涨 with its limit there."""
    return {"v": 2, "market": spec.slug, "slug": spec.slug, "item": spec.name, "kind": "updown", "key": "BTC-15M", "side": "up",
            "label": "挂涨" if maker else "吃涨", "maker": maker, "status": "resting", "shares": 0.0, "order": 10.0, "price": price, "cap": price,
            "fills": [], "revisions": [], "settle": lb.updown_settle(spec), "opened": W4 - 3_000, "version": {"sim_shares": lb.config.sim_shares},
            "live": {"state": "pending", "want": {"maker": maker, "limit": price, "cap": price}, "events": [{"at": W4 - 3_000, "what": "决定下单"}]}}


_priced(card, WLINE, W4 - 1_000)  # the price fresh: both go out, the rest waits
assert lb.live_taker_recheck(_pending(True, 0.36), W4) == ("", False) and lb.live_taker_recheck(_pending(False, 0.37), W4) == ("", False)
assert lb.live_taker_stale(_pending(False, 0.37), _mk(lb, "BTC-15M", W4), W4) == ""
_priced(card, WLINE, W4 - 8_000)  # ...the price 8 s old at the send: neither does
stale = "现货价 8 秒未更新，超过离结束 4.0 分钟时的上限 5 秒"
assert lb.live_taker_recheck(_pending(True, 0.36), W4) == (f"盘口变了：{stale}，撤单，3 秒前的挂单决定作废", False)
assert lb.live_taker_recheck(_pending(False, 0.37), W4) == (f"卡片暂不建议（{stale}；暂不给建议），3 秒前的吃单决定作废", False)
assert lb.live_taker_stale(_pending(False, 0.37), _mk(lb, "BTC-15M", W4), W4) == f"卡片暂不建议（{stale}；暂不给建议），剩余吃单撤掉"


# --- MOD-NVDA-FROZEN-INDEX: NVDA 先触 $160 / $210 (StockTouchMarket) takes Binance's NVDAUSDT futures index as its live price
# in the session (1.147.0), Yahoo's 30-second trade being the check on it and the fallback. The index counted as live by its
# delivery alone — a push's event time E, a premiumIndex read's server time: both new every second whether or not the value
# behind them moved. So an index stalled at one value while the stock went on trading stayed the price for as long as it kept
# within 2% of Yahoo's trade, and that trade priced nothing: ten minutes of it with the stock up 1.9% put the "$210 first"
# fair price 6.4¢ off with no hold on the card, and under a 3¢ bar the taker bought the other side above its worth. Now a
# value delivered unchanged for over INDEX_STILL_MS (15 s, by the deliveries' own times) is refused and the next Yahoo read
# prices the card — every read after it too: the standing value is not taken back — until the index moves again. Both ways
# the index reaches the card: raw markPrice frames → BinanceStream.frame → Bot.price_pushed, and the NVDA-15M card's
# premiumIndex read → Bot.refresh_touch ----
import json, urllib.parse  # noqa: E401,E402

NV0 = m.et_wall_ms(dt.date(2026, 11, 10), 11, 0)        # Tuesday 11:00 ET: the regular session running
NV_CREATED = m.et_wall_ms(dt.date(2026, 11, 2), 10, 0)  # the market's creation (Predict's createdAt)
NVDA = next(s for s in m.TOUCH_MARKETS if s.key == "NVDA-2027")
NV_LAG = 150                  # a delivery's own time (the push's E, the read's server time) is this long before the bot's clock
NV_STALL, NV_END = 120, 720   # seconds after NV0: the index's last change before it stalls; ten minutes on, when it moves again


def _nv_index(k: int) -> str:
    """Binance's index k seconds after NV0: a new value every second, but for a quiet 20 s (70–90: the stock itself
    still) — then stalled at 185.00 from NV_STALL to NV_END, and after that moving again where the stock is."""
    if k > NV_END:
        return f"{_nv_stock(k):.2f}"
    return "185.00" if 70 <= k <= 90 or k >= NV_STALL else f"{185 + 0.01 * (k % 5):.2f}"


def _nv_stock(k: int) -> float:
    """The stock (Yahoo's trade): where the index is while that lives; from NV_STALL on up $3.50 every ten minutes (+1.9%)."""
    return float(_nv_index(k)) if k < NV_STALL else 185.0 + 3.5 * (k - NV_STALL) / 600


class _NvYahoo:
    """Yahoo's chart feed on the bot's clock: a year of daily bars (±1% a day: σ 15.8%), today's minutes (nowhere near a
    line), the stock's last trade five seconds old."""
    def __init__(self, clock: _Clock): self.clock = clock

    async def fetch(self, url: str, extra: object = None, record: bool = True) -> bytes:
        q, now = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query)), self.clock.now
        assert "/NVDA?" in url and q.get("interval") in ("1m", "1d"), url
        price = when = None
        if q["interval"] == "1d":
            bars, day = [], m.et_date(NV0) - dt.timedelta(days=365)
            while day < m.et_date(NV0):
                if m.us_session(day):
                    close = 185.0 * (1.01 if len(bars) % 2 else 1.0)
                    bars.append((m.us_session(day)[0], close * 0.999, close * 1.005, close * 0.995, close))
                day += dt.timedelta(days=1)
        else:
            price, when = _nv_stock((now - NV0) // 1000), now // 1000 - 5
            bars = [(o, price, price + 0.1, price - 0.1, price) for o in range(m.us_session(m.et_date(NV0))[0], now, 60_000)]
        col = lambda n: [b[n] for b in bars]
        return json.dumps({"chart": {"result": [{"meta": {"regularMarketPrice": price, "regularMarketTime": when, "gmtoffset": -18000},
                                                 "timestamp": [b[0] // 1000 for b in bars],
                                                 "indicators": {"quote": [{"open": col(1), "high": col(2), "low": col(3), "close": col(4)}]}}],
                                     "error": None}}).encode()


async def _nv_idle(now_ms: int) -> None:
    return None


async def _nv_stalled(rest: bool) -> None:
    """Twelve minutes of a real Bot, second by second: the index delivered (a pushed frame — or, ``rest``, the 15-minute
    card's read with the stream mute), the first-touch round (Bot.refresh_touch), Yahoo's read every 30 s."""
    clock = _Clock(NV0)
    bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                   "SIM_MARKETS": "touch", "SIM_EDGE_CENTS": "3"}), m.Store(":memory:"), clock, None)
    card, nvda15, stream = bot.touches["NVDA-2027"], bot.updowns["NVDA-15M"], bot.binance_streams["futures"]
    assert isinstance(card, m.StockTouchMarket) and bot.index_markets["NVDAUSDT"] == [nvda15] and "NVDAUSDT" in stream.symbols()
    for key, touch in bot.touches.items():
        if key != "NVDA-2027":
            touch.refresh = _nv_idle  # the crypto first-touch cards read Binance: not this test's business
    card.start_ms = NV_CREATED
    bot.predict.info[NVDA.slug] = {"outcomes": ["$160", "$210"], "created_ms": NV_CREATED}
    m.fetch_source = _NvYahoo(clock).fetch

    async def premium_index(path: str, **params: object) -> dict:  # the 15-minute card's read: the index now, the server's time
        assert path == "premiumIndex" and params == {"symbol": "NVDAUSDT"}, (path, params)
        return {"symbol": "NVDAUSDT", "markPrice": "185.05", "indexPrice": _nv_index((clock.now - NV0) // 1000), "time": clock.now - NV_LAG}
    nvda15.futures = premium_index
    await card.refresh(NV0)
    assert card.error == "" and card.price == D("185.0000") and not card.index_live and card.advice_problem(NV0) == "", (card.error, card.price)
    sources: list[tuple[int, str]] = []  # each change of where the card's price is from: (the second, "index" | "Yahoo")

    async def second(k: int) -> None:
        clock.now = NV0 + k * 1000
        if rest:
            await nvda15.read_price(clock.now)
        else:
            await stream.frame(None, json.dumps({"e": "markPriceUpdate", "E": clock.now - NV_LAG, "s": "NVDAUSDT", "p": "185.05", "i": _nv_index(k)}))
        if k % 30 == 0:
            card.times["price"] = -1e9  # Yahoo's read is due
        await bot.refresh_touch(clock.now)
        source = "index" if card.index_live else "Yahoo"
        if not sources or sources[-1][1] != source:
            sources.append((k, source))

    at = lambda k: NV0 + k * 1000 - NV_LAG  # the time of the delivery of second k
    # 1. the index moving every second: it is the price, through Yahoo's reads at 30 s and 60 s (the trade kept as the check)
    for k in range(1, 70):
        await second(k)
    assert sources == [(1, "index")] and (card.price, card.priced_ms, card.yahoo[0]) == (D("185.04"), at(69), D("185.0000")), (sources, card.price)
    # 2. a quiet 20 s, the stock itself still: the value is taken for its first 15 s; Yahoo's read at 90 s finds the price
    #    in hand 5 s old and leaves the card on the index, and the next move is the price again — a pause is not a stall
    for k in range(70, 91):
        await second(k)
        assert card.index_live and (card.price, card.priced_ms) == (D("185.00"), at(min(k, 85))), (k, card.price, card.priced_ms - NV0)
    for k in range(91, NV_STALL + 1):
        await second(k)
        assert card.index_live and (card.price, card.priced_ms) == (D(_nv_index(k)), at(k)), (k, card.price, card.priced_ms - NV0)
    assert sources == [(1, "index")], sources
    # 3. the index stalls at 185.00 — delivered every second all the same, a time of its own on each — while the stock climbs
    #    1.9% in ten minutes. Taken for 15 s more, then refused; Yahoo's read at 150 s, the price in hand over 15 s old by
    #    then, takes the card, and so does every read after it: the standing value, never 2% from the trade, is not taken
    #    back. (Before: the index all ten minutes, the card at 185.00 with the stock at 188.50)
    for k in range(NV_STALL + 1, NV_END + 1):
        await second(k)
        if k < NV_STALL + 30:
            assert card.index_live and (card.price, card.priced_ms) == (D("185.00"), at(min(k, NV_STALL + 15))), (k, card.price, card.priced_ms - NV0)
        else:
            read = k - k % 30  # Yahoo's last read: the stock then, its trade 5 s before
            assert not card.index_live and card.price == card.yahoo[0] == D(f"{_nv_stock(read):.4f}") and card.priced_ms == NV0 + read * 1000 - 5_000, (k, card.price)
    assert sources == [(1, "index"), (NV_STALL + 30, "Yahoo")], sources
    now = clock.now
    assert card.price == D("188.5000") and card.SOURCE == "Yahoo 行情（NASDAQ:NVDA，代 Pyth）" and math.log(188.5 / 185.0) < card.INDEX_MAX_GAP
    # the odds are the stock's, 6.4¢ from the stalled index's, and still advised on (the card is fresh: Yahoo's trade)
    years = card.remaining_years(now)
    truth = m.first_touch(188.5, 160.0, 210.0, card.sigma, 0.0, years).fair_upper    # "$210 first" at the stock's price
    stalled = m.first_touch(185.0, 160.0, 210.0, card.sigma, 0.0, years).fair_upper  # ...and at the stalled index
    assert 0.06 < truth - stalled < 0.07 and card.odds(now) == m.first_touch(188.5, 160.0, 210.0, card.sigma, 0.0, years), (truth, stalled, card.odds(now))
    assert card.advice_problem(now) == "" and not card.price_stale(now)
    pay = bot.touch_payload(card, now)  # the page says which price it is
    assert (pay["source"], pay["touch"]["price"], pay["fair_up"]) == (card.SOURCE, "188.50", truth), pay
    # Predict's book has the stock's move in it: "$160 first" bid and offered a cent either side of its worth. Priced on the
    # stalled index the card had that side 6.4¢ too dear, and a 3¢ bar (a preset of the control page) took the offer — 300
    # shares a cent over their worth, a real order with LIVE on. Priced on the stock there is nothing to buy on either side
    ask = D(f"{1 - truth + 0.01:.2f}")
    assert (1 - stalled) - float(ask) > 0.03 + 0.01 and float(ask) > 1 - truth, (ask, truth, stalled)  # the bar cleared by a cent; a loss at the stock's price
    bot.predict.books["NVDA-2027"] = m.PredictBook("NVDA-2027", NVDA.slug, "77", "t", ((ask - D("0.02"), D("500")),), ((ask, D("500")),), now, None)
    mk = _mk(bot, "NVDA-2027", now)
    assert (mk.kind, mk.hold, mk.fair_up) == ("touch", "", truth), (mk.hold, mk.fair_up)
    await (await bot.sim_decide(now))
    await bot.sim_express_look("NVDA-2027", now)
    assert bot.sim_trades() == {}, [(t["label"], t["shares"], t["price"]) for t in bot.sim_trades().values()]  # (before: 吃160, 300 shares at the 43¢ offer)
    # 4. the index moves again: the price at once
    await second(NV_END + 1)
    assert sources[-1] == (NV_END + 1, "index") and (card.price, card.priced_ms) == (D(_nv_index(NV_END + 1)), at(NV_END + 1)), (sources, card.price)
    assert card.SOURCE == "币安合约 NVDAUSDT 指数价（交易时段每秒更新，Yahoo 核对）" and abs(float(card.price) - _nv_stock(NV_END + 1)) < 0.01


_nv_fetch = m.fetch_source
for rest in (False, True):
    asyncio.run(_nv_stalled(rest))
m.fetch_source = _nv_fetch
# the limits those seconds rest on: 15 s without a delivery (1.147.0), 15 s without a change of the value, 2% from Yahoo's trade
assert (m.StockTouchMarket.INDEX_STALE_MS, m.StockTouchMarket.INDEX_STILL_MS, m.StockTouchMarket.INDEX_MAX_GAP) == (15_000, 15_000, 0.02)

# the open: an index that has stood at one value since before it (Binance's sources not on the session yet; a closure the
# calendar does not know) is not the price at 09:30 on the strength of its deliveries' times — it was, from the first second —
# and is from its first move on
NV_OPEN = m.et_wall_ms(dt.date(2026, 11, 10), 9, 30)
nv = m.StockTouchMarket(m.Store(":memory:"), NVDA)
nv.yahoo = (D("185.0000"), m.et_wall_ms(dt.date(2026, 11, 9), 16, 0))  # Monday's close, read before the open
nv.price, nv.priced_ms = nv.yahoo
for s in range(-120, 3):
    assert not nv.note_index(D("185.40"), NV_OPEN + s * 1000 - NV_LAG, NV_OPEN + s * 1000) and not nv.index_live and nv.price == D("185.0000"), s
assert nv.note_index(D("185.52"), NV_OPEN + 3_000 - NV_LAG, NV_OPEN + 3_000) and nv.index_live and nv.price == D("185.52")
# ...taken while it stands for 15 s, by the deliveries' own times, and no longer
for s in range(4, 24):
    assert not nv.note_index(D("185.52"), NV_OPEN + s * 1000 - NV_LAG, NV_OPEN + s * 1000) and nv.priced_ms == NV_OPEN + min(s, 18) * 1000 - NV_LAG, s
# a frame delivered late (a stalled socket catching up: 16 s old) is no news of the index as it is now — the other value in
# it does not start the standing one's clock afresh; one on time does (the index did move), and then it is the price
late = NV_OPEN + 24_000
assert not nv.note_index(D("185.30"), late - 16_000, late)
assert not nv.note_index(D("185.52"), late + 1_000 - NV_LAG, late + 1_000) and nv.priced_ms == NV_OPEN + 18_000 - NV_LAG
assert nv.note_index(D("185.30"), late + 2_000 - NV_LAG, late + 2_000) and (nv.price, nv.priced_ms) == (D("185.30"), late + 2_000 - NV_LAG)
assert nv.note_index(D("185.52"), late + 3_000 - NV_LAG, late + 3_000) and (nv.price, nv.priced_ms) == (D("185.52"), late + 3_000 - NV_LAG)
# nothing else of 1.147.0's rule moves: a delivery over 15 s old, one not newer than the price in hand, one 2% from Yahoo's
# trade and one after the close are refused as they were
assert not nv.note_index(D("185.60"), late + 3_000 - NV_LAG, late + 4_000) and not nv.note_index(D("185.60"), late - 12_000, late + 4_000)
assert not nv.note_index(D("189.00"), late + 5_000 - NV_LAG, late + 5_000) and nv.price == D("185.52") and nv.index_live
closed = m.et_wall_ms(dt.date(2026, 11, 10), 16, 0)
assert not nv.note_index(D("185.70"), closed - NV_LAG, closed) and nv.price == D("185.52")


# --- MOD-NVDA-OPEN-GAP: NVDA 先触 $160 / $210 advises in the regular session only — overnight the price in hand is the close
# and the stock goes on trading, so a quote on it is a free option for whoever saw the after-hours move. At 09:30 that hold
# lifted with the price in hand still the close: Yahoo's last trade is yesterday's until its first read of the new session
# (half a minute on any day; all morning from a host stuck on yesterday's chart), and Binance's index — the price that does
# know the open — is refused as "another instrument" for being over 2% from that very close whenever the stock has gapped
# over 2%. The staleness check lets the close stand for the session's first half hour (the hit markets' rule, borrowed), and
# the 1-minute-bar check is stamped by the read, whatever day its bars are of. So after a +4% open the card priced
# "$210 first" 15¢ under its worth with no hold on it, and from the session's first second the taker bought the other side
# above its worth off a book that had the open in it (a real order with LIVE on); with Yahoo stuck or failing the alert went
# out too. Now there is no advice in the session on a price from before its open — nor, later in it, on one over five
# minutes old by its own time: the same thing with Yahoo's trade standing still mid-session. Real Bot, second by second:
# the index as raw markPrice frames → BinanceStream.frame → Bot.price_pushed, the first-touch round (Bot.refresh_touch)
# with Yahoo's reads, Predict's book, the trader's step and express lane, the edge alerts ----
NVO_DAY = dt.date(2026, 11, 10)                # a Tuesday; NVDA closed at 185.00 on Monday
NVO_OPEN = m.us_session(NVO_DAY)[0]            # the bell: 09:30 ET
NVO_MON = m.us_session(dt.date(2026, 11, 9))   # Monday's session: its closing print is Yahoo's last trade until Tuesday's first
NVO_ENV = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "SIM_MARKETS": "touch"}
# ...the rest at its default: SIM_WAYS=taker, the 10¢ bar, no wait before a buy, the edge alerts on (10¢, held for 60 s)


def _nvo_hold(priced_ms: int) -> str:
    """The card's words while the session runs on a price from before its open (the price's own time, Beijing)."""
    return f"美股已开盘，现价还是开盘前的（停在 {m.stamp(priced_ms, seconds=False)}），今天的价格还没读到；暂不给建议"


def _nvo_old(priced_ms: int) -> str:
    """...and on a price of the session itself that is over five minutes old."""
    return f"美股交易中，现价还停在 {m.stamp(priced_ms, seconds=False)}（超过 5 分钟没更新）；暂不给建议"


def _nvo_chart(bars: list, price: float | None = None, when: int | None = None) -> bytes:
    """A Yahoo v8 chart body: (open ms, open, high, low, close) bars, the last regular trade and its time (s)."""
    col = lambda n: [b[n] for b in bars]
    return json.dumps({"chart": {"result": [{"meta": {"regularMarketPrice": price, "regularMarketTime": when, "gmtoffset": -18000},
                                             "timestamp": [b[0] // 1000 for b in bars],
                                             "indicators": {"quote": [{"open": col(1), "high": col(2), "low": col(3), "close": col(4)}]}}],
                                 "error": None}}).encode()


def _nvo_daily() -> list:
    """A year of finished sessions through Monday: closes 185 and 190.18 in turn (±2.8% a day: σ 44%), Monday's 185.00."""
    bars, day = [], NVO_DAY - dt.timedelta(days=365)
    while day < NVO_DAY:
        if m.us_session(day):
            close = 185.0 * (1.028 if len(bars) % 2 else 1.0)
            bars.append((m.us_session(day)[0], close * 0.999, close * 1.005, close * 0.995, close))
        day += dt.timedelta(days=1)
    return bars[:-1] + [(NVO_MON[0], 185.5, 186.0, 184.5, 185.0)]


class _NvOpenYahoo:
    """Yahoo's chart around Tuesday's bell: the 1-minute chart is Monday's, its last trade the closing print, until Yahoo
    shows a trade of Tuesday's (``first`` seconds after the bell; None: never — a host stuck on yesterday's chart), then
    Tuesday's minutes and the stock's trade 2 s old. From ``fails`` seconds after the bell on (None: never) the 1-minute
    read is refused."""
    def __init__(self, clock: _Clock, opened: float, first: int | None, fails: int | None):
        self.clock, self.opened, self.first, self.fails = clock, opened, first, fails

    async def fetch(self, url: str, extra: object = None, record: bool = True) -> bytes:
        q, now = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query)), self.clock.now
        assert "/NVDA?" in url and q.get("interval") in ("1m", "1d"), url
        if q["interval"] == "1d":
            return _nvo_chart(_nvo_daily())
        if self.fails is not None and now >= NVO_OPEN + self.fails * 1000:
            raise m.RemoteError("HTTP 429: Too Many Requests")
        if self.first is not None and now >= NVO_OPEN + self.first * 1000:
            price = self.opened
            return _nvo_chart([(o, price, price + 0.1, price - 0.1, price) for o in range(NVO_OPEN, now, 60_000)], price, now // 1000 - 2)
        return _nvo_chart([(o, 185.0, 185.3, 184.8, 185.0) for o in range(NVO_MON[0], NVO_MON[1], 60_000)], 185.0, NVO_MON[1] // 1000)


def _nvo_bot(clock: _Clock, fetch: object) -> tuple:
    """A real Bot at its defaults but for SIM_MARKETS=touch, its NVDA first-touch card reading ``fetch`` for Yahoo."""
    bot = m.Bot(m.Config.from_env(NVO_ENV), m.Store(":memory:"), clock, None)
    card, stream = bot.touches["NVDA-2027"], bot.binance_streams["futures"]
    assert isinstance(card, m.StockTouchMarket) and (bot.config.sim_ways, bot.config.sim_edge, bot.config.sim_taker_hold) == ("taker", 0.1, 0.0)
    assert bot.config.edge_alert and (bot.config.edge_alert_edge, bot.config.edge_alert_confirm) == (0.1, 60)
    for key, touch in bot.touches.items():
        if key != "NVDA-2027":
            touch.refresh = _nv_idle  # the crypto first-touch cards read Binance: not this test's business
    bot.predict.info[NVDA.slug] = {"outcomes": ["$160", "$210"], "created_ms": NV_CREATED}
    m.fetch_source = fetch
    return bot, card, stream


async def _nvo_index(stream: object, now: int, k: int, stock: float) -> None:
    """Binance's index of the second: the stock's price, a cent of movement in it every delivery (an index that lives)."""
    await stream.frame(None, json.dumps({"e": "markPriceUpdate", "E": now - NV_LAG, "s": "NVDAUSDT", "p": f"{stock:.2f}",
                                         "i": f"{stock + 0.01 * (k % 2):.2f}"}))


async def _nvo_look(bot: m.Bot, card: m.StockTouchMarket, k: int, now: int, stock: float, cheap: bool = False) -> dict:
    """One look at second ``k``: Predict's book has the stock's price in it — "$160 first", the outcome it lists first, bid
    and offered a cent either side of its worth there (``cheap``: "$210 first" offered 15¢ under its worth instead) — and
    what the card, the page, the trader (its step and its express lane) and the alerts make of it."""
    fair = lambda price: m.first_touch(price, 160.0, 210.0, card.sigma, 0.0, card.remaining_years(now)).fair_upper
    truth, close = fair(stock), fair(185.0)  # "$210 first" at the stock's price, and at Monday's close
    low = round(1 - truth, 2)                # what "$160 first" is worth, to the cent
    bid, ask = (low + 0.15, low + 0.17) if cheap else (low - 0.01, low + 0.01)
    bot.predict.books["NVDA-2027"] = m.PredictBook("NVDA-2027", NVDA.slug, "77", "t", ((D(f"{bid:.2f}"), D("500")),),
                                                   ((D(f"{ask:.2f}"), D("500")),), now, None)
    odds = card.odds(now)
    mk = next((x for x in bot.sim_markets(now) if x.key == "NVDA-2027"), None)
    row = {"k": k, "now": now, "price": card.price, "live": card.index_live, "priced": card.priced_ms, "odds": odds, "truth": truth,
           "close": close, "sigma": card.sigma, "why": card.advice_problem(now), "mk": mk, "pay": bot.touch_payload(card, now),
           "gap": bot.sim_taker_gap(mk, bot.sim_trades(), now)["state"] if mk else None}
    await (await bot.sim_decide(now))                # the trader's step...
    await bot.sim_express_look("NVDA-2027", now)     # ...and its express lane
    bot.edge_ran = -1e9
    await bot.edge_alerts(now)                       # the Telegram alerts (they look at every card, whatever SIM_MARKETS trades)
    return {**row, "bought": [(t["label"], t["shares"], round(t["price"], 4)) for t in bot.sim_trades().values()],
            "alert": dict((bot.store.get("edgealerts", {}) or {}).get(NVDA.slug) or {})}


async def _nv_open(opened: float, first: int | None, last: int, look: object, index: bool = True, fails: int | None = None,
                   chance: int | None = None) -> list[dict]:
    """From a minute before Tuesday's bell to ``last`` seconds after it, second by second: the stock at ``opened`` all
    along (pre-market too), Binance's index with it (``index`` False: nothing delivered), the first-touch round with
    Yahoo read on the card's cadence (every 30 s in the session); a look at the seconds ``look`` picks — from ``chance``
    seconds on with "$210 first" on offer 15¢ under its worth."""
    clock = _Clock(NVO_OPEN - 60_000)
    bot, card, stream = _nvo_bot(clock, _NvOpenYahoo(clock, opened, first, fails).fetch)
    rows = []
    for k in range(-60, last + 1):
        clock.now = now = NVO_OPEN + k * 1000
        if index:
            await _nvo_index(stream, now, k, opened)
        # Yahoo's reads by this clock, not the machine's: a minute before the bell (the card's first), then every 30 s of the session
        card.times["price"] = -1e9 if k == -60 or (k >= 0 and k % 30 == 0) else time.monotonic()
        await bot.refresh_touch(now)
        if look(k):
            rows.append(await _nvo_look(bot, card, k, now, opened, chance is not None and k >= chance))
    return rows


_nv_fetch = m.fetch_source
NVO_GAP = 192.50  # Tuesday opens 4% up (ln 3.97%: over INDEX_MAX_GAP from Monday's close): "$210 first" worth 65¢, 50¢ at the close
assert math.log(NVO_GAP / 185.0) > m.StockTouchMarket.INDEX_MAX_GAP
# 1. Yahoo healthy, its first trade of the day there 25 s after the bell (read at 09:30:30). Until then the card is on Monday's
#    close, the index refused every second for its distance from it: the odds stay on the card, as they do overnight, but
#    nothing is advised, bought or announced. (Before: no hold from 09:30:00 on, and 吃160 300 @ 36¢ at the first look —
#    "$160 first" being worth 35¢, 36.7¢ paid with the fee)
rows = asyncio.run(_nv_open(NVO_GAP, 25, 45, lambda k: k >= -5 or k % 30 == 0))  # every second from 09:29:55 on
for r in rows:
    k, mk = r["k"], r["mk"]
    assert isinstance(r["odds"], m.TouchOdds) and mk is not None and mk.kind == "touch" and mk.hold == r["why"] == r["pay"]["touch"]["hold"], (k, r["why"])
    assert r["bought"] == [] and not r["alert"], (k, r["bought"], r["alert"])
    if k < 30:
        assert (r["price"], r["live"], r["priced"]) == (D("185.0000"), False, NVO_MON[1]), (k, r["price"], r["live"])
        assert (r["why"] == _nvo_hold(NVO_MON[1])) if k >= 0 else r["why"].startswith("美股休市中：盘前盘后的价格不算数"), (k, r["why"])
        assert mk.fair_up == r["close"] and 0.14 < r["truth"] - r["close"] < 0.16 and 0.43 < r["sigma"] < 0.45, (k, mk.fair_up, r["truth"])  # 15¢ off
        assert r["gap"] == "wait" and r["pay"]["predict"]["hold"] == r["why"] and not any(e["best"] for e in r["pay"]["predict"]["edges"]), (k, r["gap"])
    elif k == 30:  # Yahoo's read: Tuesday's trade, 2 s old — a price of this session; the index is checked against it from now on
        assert (r["price"], r["live"], r["priced"]) == (D("192.5000"), False, r["now"] - 2_000) and r["why"] == "" and mk.fair_up == r["truth"], (r["price"], r["why"])
    else:          # ...and is the price from its next delivery
        assert r["live"] and r["priced"] == r["now"] - NV_LAG and r["why"] == "" and abs(mk.fair_up - r["truth"]) < 0.001 and r["gap"] == "near", (k, r["why"], r["gap"])
assert [r["k"] for r in rows] == [-60, -30, *range(-5, 46)]

# 2. Yahoo stuck on Monday's chart all morning (it answers; the trade it answers with is yesterday's), the index delivered and
#    right all the while: held for the half hour the close stands as the price, then the odds pause (price_stale, 1.83.0's
#    rule, as before). (Before: advised on for all thirty minutes — the 300 bought at the first look, and at 09:31 the alert
#    "新机会｜NVDA 先触 160/210：挂160 @ 34¢ 净优势 +16¢（模型 50¢），也可以：吃160 @ 36¢ +13.3¢", the wrong side)
rows = asyncio.run(_nv_open(NVO_GAP, None, 1830, lambda k: k % 30 == 0 or k == 1801))  # after each of Yahoo's reads
for r in rows:
    k = r["k"]
    assert (r["price"], r["live"], r["priced"]) == (D("185.0000"), False, NVO_MON[1]) and r["bought"] == [] and not r["alert"], (k, r["price"], r["bought"], r["alert"])
    if 0 <= k <= 1800:
        assert isinstance(r["odds"], m.TouchOdds) and r["why"] == r["mk"].hold == _nvo_hold(NVO_MON[1]) and r["gap"] == "wait", (k, r["why"], r["gap"])
    elif k > 1800:
        assert r["odds"] == f"Yahoo 行情停在 {m.stamp(NVO_MON[1], seconds=False)}，暂停概率" and r["mk"] is None, (k, r["odds"])
assert [r["k"] for r in rows][-3:] == [1800, 1801, 1830]  # the half hour to the second: the close is the price through 10:00:00

# 3. both feeds down at the bell — Yahoo refusing its reads from 09:29:30 on, nothing from Binance: the last good read is the
#    close's. Held until the odds pause, five minutes after that read (09:29:00). (Before: advised on until then, bought at
#    the first look and announced at 09:31)
rows = asyncio.run(_nv_open(NVO_GAP, None, 320, lambda k: k % 10 == 0, index=False, fails=-30))
for r in rows:
    k = r["k"]
    assert (r["price"], r["live"], r["priced"]) == (D("185.0000"), False, NVO_MON[1]) and r["bought"] == [] and not r["alert"], (k, r["price"], r["bought"], r["alert"])
    if 0 <= k <= 240:
        assert isinstance(r["odds"], m.TouchOdds) and r["why"] == r["mk"].hold == _nvo_hold(NVO_MON[1]) and r["gap"] == "wait", (k, r["why"], r["gap"])
    elif k > 240:
        assert r["odds"] == f"Yahoo 行情停在 {m.stamp(NVO_MON[1], seconds=False)}：价格：HTTP 429: Too Many Requests，暂停概率" and r["mk"] is None, (k, r["odds"])

# 4. the control, an open within 2% of the close (187.50: +1.35%): the index is the price from the bell on, as 1.147.0 has
#    it — all but the one delivery stamped before the bell, a pre-market value (the next, a second later, is the session's).
#    The book where the stock is: nothing bought. From 09:30:10 "$210 first" (worth 55¢) offered at 40¢: bought at that look
rows = asyncio.run(_nv_open(187.50, 25, 15, lambda k: k >= -5 or k % 30 == 0, chance=10))
for r in rows:
    k, mk = r["k"], r["mk"]
    if k < 0:
        assert not r["live"] and mk.hold.startswith("美股休市中：盘前盘后的价格不算数") and r["gap"] == "wait" and r["bought"] == [], (k, mk.hold)
        continue
    assert r["live"] and r["priced"] == r["now"] - NV_LAG and r["price"] in (D("187.50"), D("187.51")), (k, r["live"], r["price"])
    assert mk.hold == r["why"] == (_nvo_hold(NVO_OPEN - NV_LAG) if k == 0 else ""), (k, r["why"])
    assert abs(mk.fair_up - r["truth"]) < 0.001 and 0.54 < r["truth"] < 0.56, (k, mk.fair_up, r["truth"])
    assert r["gap"] == ("wait" if k == 0 else "near" if k < 10 else "ready") or k > 10, (k, r["gap"])
    assert r["bought"] == ([] if k < 10 else [("吃210", 300.0, 0.408)]), (k, r["bought"])  # 40¢ and the 2% fee on it

# 5. the same later in the session: from 11:05 Yahoo answers every read with the chart it had then (its trade of 11:04:55, at
#    185.00) while the stock climbs half a dollar a minute, to 192.50 (+4%) at 11:20. The index, right all along, is the price
#    until it is 2% from that trade (taken at 11:12:28, refused from 11:12:29 on, exactly as at the open); Yahoo's read at
#    11:13:00 finds the price in hand over 15 s old and puts the card back on the trade of 11:04:55 — over five minutes old:
#    not advised on. (Before: advised on for the half hour price_stale gives a trade — the alert "新机会｜NVDA 先触 160/210：
#    挂160 @ 38¢ +12¢" out at 11:15:30, 吃160 300 bought at 11:16:30 at 39¢, 39.8¢ with the fee, "$160 first" worth 38.5¢ by
#    then and falling.) At 11:20:00 Yahoo answers again: its trade at once, the index from the next second
NVS0 = m.et_wall_ms(NVO_DAY, 11, 0)
NVS_STUCK = (300, 1200)  # seconds after 11:00 between which Yahoo answers as it did at the first of them


def _nvs_stock(k: int) -> float:
    return 185.0 + 7.5 * (min(max(k, 300), 1200) - 300) / 900


async def _nvs_yahoo(url: str, extra: object = None, record: bool = True) -> bytes:
    q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
    if q["interval"] == "1d":
        return _nvo_chart(_nvo_daily())
    k = (_nvs_clock.now - NVS0) // 1000
    k = NVS_STUCK[0] if NVS_STUCK[0] <= k < NVS_STUCK[1] else k
    price, now = _nvs_stock(k), NVS0 + k * 1000
    return _nvo_chart([(o, price, price + 0.1, price - 0.1, price) for o in range(NVO_OPEN, now, 60_000)], price, now // 1000 - 5)


async def _nv_stuck() -> list[dict]:
    bot, card, stream = _nvo_bot(_nvs_clock, _nvs_yahoo)
    rows = []
    for k in range(0, 1231):
        _nvs_clock.now = now = NVS0 + k * 1000
        await _nvo_index(stream, now, k, _nvs_stock(k))
        card.times["price"] = -1e9 if k % 30 == 0 else time.monotonic()  # Yahoo's read every 30 s of this clock
        await bot.refresh_touch(now)
        if k % 30 == 0 or k in (748, 749, 1201):
            rows.append(await _nvo_look(bot, card, k, now, _nvs_stock(k)))
    return rows


_nvs_clock = _Clock(NVS0)
rows = asyncio.run(_nv_stuck())
stuck = NVS0 + (NVS_STUCK[0] - 5) * 1000  # the trade Yahoo is stuck on: 11:04:55
for r in rows:
    k, mk = r["k"], r["mk"]
    assert isinstance(r["odds"], m.TouchOdds) and mk.hold == r["why"] and r["bought"] == [] and not r["alert"], (k, r["why"], r["bought"], r["alert"])
    if k == 0:                  # 11:00:00, the card's first read: Yahoo's trade, 5 s old
        assert (r["price"], r["live"], r["priced"]) == (D("185.0000"), False, NVS0 - 5_000) and r["why"] == "" and mk.fair_up == r["truth"], (r["price"], r["why"])
    elif k <= 748 or k >= 1201:  # the index within 2% of Yahoo's trade: the price, each second's
        assert r["live"] and r["priced"] == r["now"] - NV_LAG and r["why"] == "" and abs(mk.fair_up - r["truth"]) < 0.001 and r["gap"] == "near", (k, r["live"], r["why"])
    elif k < 780:               # refused from 11:12:29 on; the last one taken stands until Yahoo's read finds it over 15 s old
        assert r["live"] and r["priced"] == NVS0 + 748_000 - NV_LAG and r["why"] == "" and abs(mk.fair_up - r["truth"]) < 0.001, (k, r["live"], r["priced"] - NVS0)
    elif k < 1200:              # Yahoo's trade of 11:04:55 again — the stock 2.1% to 4% from it, "$210 first" 8¢ to 15¢ under its worth on the card
        assert (r["price"], r["live"], r["priced"]) == (D("185.0000"), False, stuck) and r["why"] == _nvo_old(stuck) and r["gap"] == "wait", (k, r["price"], r["why"])
        assert mk.fair_up == r["close"] and 0.07 < r["truth"] - r["close"] < 0.15, (k, mk.fair_up, r["truth"])
        assert r["pay"]["touch"]["hold"] == r["pay"]["predict"]["hold"] == r["why"] and not any(e["best"] for e in r["pay"]["predict"]["edges"]), k
    else:                       # 11:20:00, Yahoo's read answers with the stock's trade (5 s old): the price, advised on
        assert (r["price"], r["live"], r["priced"]) == (D("192.5000"), False, r["now"] - 5_000) and r["why"] == "" and mk.fair_up == r["truth"], (k, r["price"], r["why"])
assert [r["k"] for r in rows if 720 < r["k"] <= 780 or r["k"] >= 1170] == [748, 749, 750, 780, 1170, 1200, 1201, 1230]
m.fetch_source = _nv_fetch

# the two rules themselves, on a card set by hand. The session's first minute, the price in hand Monday's close (read a moment ago)
nv = m.StockTouchMarket(m.Store(":memory:"), NVDA)
nv.start_ms = NV_CREATED
nv.save({"kind": "clear", "through": m.et_wall_ms(NVO_DAY, 0, 0)})
nv.sigma, nv.sigma_ms, at = 0.44, NVO_OPEN, NVO_OPEN + 60_000
nv.yahoo = (D("185.0000"), NVO_MON[1])
nv.price, nv.priced_ms, nv.fetched_ms, nv.minutes_ms = *nv.yahoo, at, at
assert nv.advice_problem(NVO_OPEN - 1).startswith("美股休市中：盘前盘后的价格不算数")  # a millisecond before the bell: the overnight hold, as it was
assert nv.advice_problem(NVO_OPEN) == nv.advice_problem(at) == _nvo_hold(NVO_MON[1])   # from the bell on: the same price, the same reason
assert isinstance(nv.odds(at), m.TouchOdds) and not nv.price_stale(at)  # the odds stand (price_stale is the hit markets', untouched)
for priced, held in ((NVO_OPEN - 1, True), (NVO_OPEN, False), (at - 20_000, False)):  # a price of this session — the opening print on — is advised on
    nv.priced_ms = priced
    assert bool(nv.advice_problem(at)) is held, priced - NVO_OPEN
# two hours in: a trade five minutes old is advised on, one older is not — though its odds stand for the half hour they did
at = NVO_OPEN + 2 * 3600_000
nv.fetched_ms = nv.minutes_ms = at
for age, why in ((20_000, ""), (300_000, ""), (300_001, _nvo_old(at - 300_001)), (29 * 60_000, _nvo_old(at - 29 * 60_000))):
    nv.yahoo = (D("185.0000"), at - age)
    nv.price, nv.priced_ms = nv.yahoo
    assert nv.advice_problem(at) == why and isinstance(nv.odds(at), m.TouchOdds), (age, nv.advice_problem(at))
assert m.StockTouchMarket.PRICE_STALE_MS == m.TouchMarket.PRICE_STALE_MS == 300_000  # the crypto card's five minutes
nv.priced_ms = at - 31 * 60_000
assert nv.odds(at) == f"Yahoo 行情停在 {m.stamp(at - 31 * 60_000, seconds=False)}，暂停概率"  # ...and pause after it, as before
# 1.147.0's fallback as it was: an index 8% from a Yahoo trade 20 s old is "another instrument" — refused, the trade advised on
nv.yahoo = (D("185.0000"), at - 20_000)
nv.price, nv.priced_ms = nv.yahoo
assert not nv.note_index(D("200.00"), at - NV_LAG, at) and (nv.price, nv.index_live) == (D("185.0000"), False) and nv.advice_problem(at) == ""
# a market already decided is not a matter of the price: nothing to hold
nv.priced_ms = NVO_MON[1]
nv.save({"kind": "high", "time": NVO_MON[0], "hi": 210.5, "lo": 184.5})
assert nv.advice_problem(at) == "" and nv.odds(at) == m.TouchOdds(0.0, 1.0, 0.0)


# --- MOD-NVDA-CLOSE-RESTING: NVDA 先触 $160 / $210 advises in the regular session only. From the bell on the price in hand is
# the closing print — Yahoo's regular-session trade knows nothing of the after hours, Binance's index is refused out of the
# session — while the stock trades on, and the card holds: "按收盘价报价会被知道盘后行情的人吃掉". That hold kept new orders
# away; the resting order placed in the session stood under it. An order under a hold is not judged (sim_requote_reason), the
# rules that end a quote with its session are the daily cards' (SIM_MAKER_SESSION, SIM_OPEN_QUIET_MINUTES), and the one rule
# left was the generic clock of SIM_NO_ODDS_WITHDRAW_MINUTES: 30 minutes by default, never at 0. NVDA's results come out some
# twenty minutes after the bell — the bid stood through them at the close's fair price, on Predict too with LIVE on. Now a
# resting order on a US stock's first-touch card goes at the first look after the session's end (Bot.sim_stock_touch_closed),
# whatever that setting says, and the next one is the next session's, on a price of that session. Real Bot and LiveBot: the
# card's own reads of a stand-in Yahoo, Predict's book, the trader's step and express lane, the real order on a stand-in
# Predict ----
NVC_DAY = dt.date(2026, 11, 18)                      # a Wednesday
NVC_CLOSE = m.us_session(NVC_DAY)[1]                 # its closing bell: 16:00 ET
NVC_NEXT = m.us_session(dt.date(2026, 11, 19))[0]    # Thursday's opening bell: 09:30 ET
NVC_ENV = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "SIM_MARKETS": "touch",
           "SIM_WAYS": "maker", "SIM_MAKER_POINTS": "off"}  # the finding's two choices (先触价, 挂单), and no waiting for Predict's points
# ...the rest at its default: 100 shares a resting order, the 10¢ bar, SIM_MAKER_SESSION on, SIM_NO_ODDS_WITHDRAW_MINUTES 30
NVC_WHY = "美股已收盘：盘前盘后的价格不算数、但会影响开盘价，按收盘价挂着的单会被知道盘后行情的人吃掉，收盘即撤（开盘后再挂）"
NVC_TID = f"{NVDA.slug}|up|挂"  # the resting order on "$210 first", the card's 涨 side
_nvc_clock = _Clock(NVC_CLOSE)


def _nvc_stock(now: int) -> float:
    """NVDA's regular-session price: 190.00 all Wednesday; Thursday opens 8% lower (the results came out after Wednesday's bell)."""
    return 190.0 if now < NVC_NEXT else 174.8


def _nvc_daily(today: dt.date) -> list:
    """A year of finished sessions before ``today``: closes 190.00 and 195.32 in turn (±2.8% a day: σ 44%), Wednesday's 190.00."""
    days = [d for d in (NVC_DAY - dt.timedelta(days=n) for n in range(365, -2, -1)) if m.us_session(d)]
    closes = [190.0 * (1.028 if (days.index(NVC_DAY) - i) % 2 else 1.0) for i in range(len(days))]
    return [(m.us_session(d)[0], c * 0.999, c * 1.005, c * 0.995, c) for d, c in zip(days, closes) if d < today]


async def _nvc_yahoo(url: str, extra: object = None, record: bool = True) -> bytes:
    """Yahoo's chart on the test's clock: the daily bars, and the 1-minute chart of the session running — the stock's trade
    2 s old — or, between two sessions, of the one just finished: its minutes, and its closing print as the last trade
    whatever the stock does after hours (includePrePost=false; regularMarketPrice is the regular session's)."""
    q, now = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query)), _nvc_clock.now
    assert "/NVDA?" in url and q.get("interval") in ("1m", "1d"), url
    if q["interval"] == "1d":
        return _nvo_chart(_nvc_daily(m.et_date(now)))
    day = m.et_date(now)
    while not m.us_session(day) or m.us_session(day)[0] > now:
        day -= dt.timedelta(days=1)  # no session today, or not yet: the last one before
    opened, closed = m.us_session(day)
    price = _nvc_stock(min(now, closed - 1))
    return _nvo_chart([(o, price, price + 0.1, price - 0.1, price) for o in range(opened, min(now, closed), 60_000)], price,
                      now // 1000 - 2 if now < closed else closed // 1000)


async def _nvc_silent(url: str, extra: object = None, record: bool = True) -> bytes:
    raise m.RemoteError("HTTP 502: Bad Gateway")


def _nvc_bot(now: int, cls: type = m.Bot, **env: str) -> tuple:
    """A real Bot (or LiveBot) that rests orders on the first-touch markets, its NVDA card reading the stand-in Yahoo."""
    _nvc_clock.now = now
    bot = cls(m.Config.from_env({**NVC_ENV, **env}), m.Store(":memory:"), _nvc_clock, None)
    card = bot.touches["NVDA-2027"]
    assert isinstance(card, m.StockTouchMarket) and bot.config.sim_markets == frozenset({"touch"}), bot.config.sim_markets
    for key, touch in bot.touches.items():
        if key != "NVDA-2027":
            touch.refresh = _nv_idle  # the crypto first-touch cards read Binance: not this test's business
    bot.predict.info[NVDA.slug] = {"outcomes": ["$160", "$210"], "created_ms": NV_CREATED}
    return bot, card


async def _nvc_feeds(bot: m.Bot, card: m.StockTouchMarket, now: int, bid: float, ask: float, read: bool = True, scan: bool = False) -> None:
    """The feeds at ``now``: Yahoo read by the card (``read``; ``scan``: the daily bars too — the cadence is the test's, not
    the machine's), the first-touch round, and Predict's book of the market — "$210 first" bid and offered at ``bid`` /
    ``ask``, 500 shares each (Predict lists "$160" first: the book it sends is that side's, the complement)."""
    _nvc_clock.now = now
    card.times["price"] = -1e9 if read else time.monotonic()
    if scan:
        card.times["scan"] = -1e9
    await bot.refresh_touch(now)
    bot.predict.books["NVDA-2027"] = m.PredictBook("NVDA-2027", NVDA.slug, "77", "t", ((D(f"{1 - ask:.2f}"), D("500")),),
                                                   ((D(f"{1 - bid:.2f}"), D("500")),), now, None)


async def _nvc_look(bot: m.Bot, card: m.StockTouchMarket, now: int, bid: float, ask: float, read: bool = True, scan: bool = False,
                    express: bool = True) -> dict:
    """The feeds at ``now`` (_nvc_feeds), then the trader's step and (``express``) its express lane; the records after them."""
    await _nvc_feeds(bot, card, now, bid, ask, read, scan)
    bot.sim_ran = -1e9  # the step is due (its timer is the machine's)
    await bot.sim_step(now)
    if express:
        await bot.sim_express("NVDA-2027")
    return {tid: (t["status"], t["price"], t["shares"]) for tid, t in bot.sim_trades().items()}


async def _nvc_paper() -> None:
    """1. the finding's case on paper, from two minutes before Wednesday's bell to Thursday's first minute."""
    at = NVC_CLOSE - 120_000
    bot, card = _nvc_bot(at)
    c = bot.config
    assert (c.sim_ways, c.sim_no_odds_withdraw, c.sim_maker_session, c.sim_shares, c.sim_edge) == ("maker", 30, True, 100.0, 0.1)
    # 15:58:00, the session running: NVDA 190.00, "$210 first" worth 60¢ and bid 44¢ / offered 47¢ on Predict. The resting
    # order joins the bid, and rests there to the session's last millisecond
    got = await _nvc_look(bot, card, at, 0.44, 0.47)
    fair = _mk(bot, "NVDA-2027", at).fair_up
    assert got == {NVC_TID: ("resting", 0.44, 0.0)} and 0.59 < fair < 0.61 and 0.43 < card.sigma < 0.45, (got, fair, card.sigma)
    assert (card.price, card.priced_ms, card.advice_problem(at)) == (D("190.0000"), at - 2_000, ""), (card.price, card.advice_problem(at))
    for at in (NVC_CLOSE - 60_000, NVC_CLOSE - 1):
        assert await _nvc_look(bot, card, at, 0.44, 0.47) == {NVC_TID: ("resting", 0.44, 0.0)} and _mk(bot, "NVDA-2027", at).hold == "", at
    # 16:00:00.000, the bell. The card's price is the closing print from here to Thursday's first trade, and it holds for
    # that. The page's 为什么挂 row says what the next look does with the order, and the step does it (the express lane
    # passes a held card by): withdrawn, as the strategy's own doing, the slot free — and nothing put in its place.
    # (Before: left where it was, under the hold, until 16:30:00)
    await _nvc_feeds(bot, card, NVC_CLOSE, 0.44, 0.47)
    mk = _mk(bot, "NVDA-2027", NVC_CLOSE)
    assert mk.hold.startswith("美股休市中：盘前盘后的价格不算数") and abs(mk.fair_up - fair) < 0.001 and (card.price, card.priced_ms) == (D("190.0000"), NVC_CLOSE), mk.hold
    fair = mk.fair_up  # ...and so is the fair price, all night: no trading time passes, and nothing the card reads moves
    said = bot.sim_why(mk, bot.sim_trades(), NVC_CLOSE)["maker"]["sides"][0]["text"]
    assert said.startswith("挂单中：44.0¢×100 份") and said.endswith("；下一轮撤掉：" + NVC_WHY), said
    got = await _nvc_look(bot, card, NVC_CLOSE, 0.44, 0.47, read=False, express=False)
    assert got == {NVC_TID + "#1": ("cancelled", 0.44, 0.0)}, got
    rec = bot.sim_trades()[NVC_TID + "#1"]
    assert rec["withdrawn"] == {"at": NVC_CLOSE, "why": NVC_WHY, "unfilled": 100.0, "rule": True} and rec["note"] == f"撤单：{NVC_WHY}，一份都没成交", rec
    assert bot.sim_unpriced_since == {}  # nothing is left for the generic clock to time
    # ...16:00:05; 16:20, the results: the stock −8% after hours, which the card cannot see (190.00, the close) and Predict's
    # book can — "$210 first", worth 30¢ now, bid 14¢ / offered 16¢: sellers 28¢ through where the order had rested; 16:31;
    # overnight; Thursday before the bell. Nothing fills, nothing rests, nothing is placed.
    # (Before: filled at 16:20, 100 shares at 44¢ of a side worth 30¢ — and with no such seller, withdrawn only at 16:30)
    after = m.first_touch(174.8, 160.0, 210.0, card.sigma, 0.0, card.remaining_years(NVC_CLOSE)).fair_upper
    assert 0.29 < after < 0.31 and 0.13 < 0.44 - after < 0.15, after
    for at, bid, ask in ((NVC_CLOSE + 5_000, 0.44, 0.47), (NVC_CLOSE + 20 * 60_000, 0.14, 0.16), (NVC_CLOSE + 31 * 60_000, 0.14, 0.16),
                         (NVC_CLOSE + 6 * 3600_000, 0.14, 0.16), (NVC_NEXT - 1, 0.14, 0.16)):
        got = await _nvc_look(bot, card, at, bid, ask, scan=at > NVC_CLOSE + 3600_000)
        mk = _mk(bot, "NVDA-2027", at)
        assert got == {NVC_TID + "#1": ("cancelled", 0.44, 0.0)} and mk.hold.startswith("美股休市中：") and mk.fair_up == fair, (at - NVC_CLOSE, got, mk.hold)
        assert (card.price, card.priced_ms) == (D("190.0000"), NVC_CLOSE) and "卡片暂不建议（美股休市中：" in bot.sim_maker_gate(mk, at), at - NVC_CLOSE
    # Thursday 09:30:00: the session runs, the price in hand is still Wednesday's close (MOD-NVDA-OPEN-GAP's hold): nothing
    # yet. 09:30:30, Yahoo's first read of the session — 174.80, "$210 first" worth 30¢: the next order goes on that price
    for at in (NVC_NEXT, NVC_NEXT + 5_000):
        got = await _nvc_look(bot, card, at, 0.14, 0.16, read=False)
        assert got == {NVC_TID + "#1": ("cancelled", 0.44, 0.0)} and _mk(bot, "NVDA-2027", at).hold.startswith("美股已开盘，现价还是开盘前的"), (at - NVC_NEXT, got)
    at = NVC_NEXT + 30_000
    got = await _nvc_look(bot, card, at, 0.14, 0.16)
    mk = _mk(bot, "NVDA-2027", at)
    assert (card.price, card.priced_ms, mk.hold) == (D("174.8000"), at - 2_000, "") and abs(mk.fair_up - after) < 0.001, (card.price, mk.hold, mk.fair_up)
    assert got == {NVC_TID: ("resting", 0.14, 0.0), NVC_TID + "#1": ("cancelled", 0.44, 0.0)}, got


async def _nvc_whatever(minutes: str, silent: bool) -> None:
    """2. the same order under another SIM_NO_ODDS_WITHDRAW_MINUTES — 0: 不撤, the one that left it up all night — and, ``silent``,
    with Yahoo silent after its read of 15:58 and the first look after the bell at 16:06: the card shows no odds by then (its
    last read is over five minutes old) and the trader has no SimMarket for the record. It goes at that look all the same.
    (Before: at 16:30, at 18:00, never; with Yahoo silent at 16:36 — 概率暂缺已超过 30 分钟 — or never)"""
    bot, card = _nvc_bot(NVC_CLOSE - 120_000, SIM_NO_ODDS_WITHDRAW_MINUTES=minutes)
    assert bot.config.sim_no_odds_withdraw == int(minutes)
    assert await _nvc_look(bot, card, NVC_CLOSE - 120_000, 0.44, 0.47) == {NVC_TID: ("resting", 0.44, 0.0)}
    at = NVC_CLOSE + (360_000 if silent else 0)
    if silent:
        m.fetch_source = _nvc_silent
    try:
        got = await _nvc_look(bot, card, at, 0.44, 0.47)
    finally:
        m.fetch_source = _nvc_yahoo
    assert got == {NVC_TID + "#1": ("cancelled", 0.44, 0.0)} and bot.sim_trades()[NVC_TID + "#1"]["withdrawn"]["why"] == NVC_WHY, (minutes, silent, got)
    priced = [x for x in bot.sim_markets(at) if x.key == "NVDA-2027"]
    assert (priced == [] and card.odds(at).startswith("Yahoo 行情停在 ")) if silent else priced[0].hold.startswith("美股休市中："), (minutes, silent, card.odds(at))


async def _nvc_session_hold() -> None:
    """3. in the session nothing changes: a hold that may pass leaves the order to the generic clock, as before. Wednesday
    11:00, the order resting; Yahoo goes silent. Its last read five minutes old, the odds pause and the order stands unpriced —
    for SIM_NO_ODDS_WITHDRAW_MINUTES from the first look that finds it so (11:06), then withdrawn in that rule's words."""
    t0 = m.et_wall_ms(NVC_DAY, 11, 0)
    bot, card = _nvc_bot(t0)
    assert await _nvc_look(bot, card, t0, 0.44, 0.47) == {NVC_TID: ("resting", 0.44, 0.0)}
    m.fetch_source = _nvc_silent
    try:
        for minutes, status in ((4, "resting"), (6, "resting"), (20, "resting"), (35.9, "resting"), (36.1, "cancelled")):
            at = t0 + int(minutes * 60_000)
            got = await _nvc_look(bot, card, at, 0.44, 0.47)
            assert [(s, p) for s, p, _ in got.values()] == [(status, 0.44)], (minutes, got)
            assert (minutes < 5) == any(x.key == "NVDA-2027" for x in bot.sim_markets(at)), minutes  # priced for five minutes after the last read
    finally:
        m.fetch_source = _nvc_yahoo
    assert bot.sim_trades()[NVC_TID + "#1"]["withdrawn"]["why"] == "概率暂缺已超过 30 分钟，撤单", bot.sim_trades()[NVC_TID + "#1"]["withdrawn"]


_nv_fetch = m.fetch_source
m.fetch_source = _nvc_yahoo
asyncio.run(_nvc_paper())
for minutes, silent in (("0", False), ("120", False), ("30", True), ("0", True)):
    asyncio.run(_nvc_whatever(minutes, silent))
asyncio.run(_nvc_session_hold())

# 4. the rule itself, on a record by hand — no SimMarket at all: read from the record's own kind and key. Closed is closed:
#    from the bell to the next bell to the millisecond, a weekend, a holiday, the 13:00 bell of an early close
bot, card = _nvc_bot(NVC_CLOSE)
rec = {"market": NVDA.slug, "slug": NVDA.slug, "key": "NVDA-2027", "kind": "touch", "maker": True, "status": "resting", "side": "up",
       "order": 100.0, "shares": 0.0, "price": 0.44, "settle": {"deadline": NVDA.deadline_ms}}
half = m.et_wall_ms(dt.date(2026, 11, 27), 13, 0)  # the day after Thanksgiving closes at 13:00 ET
assert m.us_session(dt.date(2026, 11, 26)) is None and m.us_session(dt.date(2026, 11, 27))[1] == half
for at, closed in ((NVC_CLOSE - 1, False), (NVC_CLOSE, True), (NVC_NEXT - 1, True), (NVC_NEXT, False),
                   (m.et_wall_ms(dt.date(2026, 11, 21), 11, 0), True),   # a Saturday
                   (m.et_wall_ms(dt.date(2026, 11, 26), 11, 0), True),   # Thanksgiving
                   (half - 1, False), (half, True), (m.et_wall_ms(dt.date(2026, 11, 27), 15, 0), True)):
    assert bot.sim_withdraw_reason(rec, None, at) == (NVC_WHY if closed else ""), (at, bot.sim_withdraw_reason(rec, None, at))
# not a taker's record; not the crypto first-touch cards, which trade around the clock
assert bot.sim_withdraw_reason({**rec, "maker": False}, None, NVC_CLOSE) == ""
for key in ("BNB-2027", "BTC-2027"):
    spec = bot.touches[key].spec
    assert type(bot.touches[key]) is m.TouchMarket and spec.deadline_ms > NVC_NEXT
    assert bot.sim_withdraw_reason({**rec, "market": spec.slug, "slug": spec.slug, "key": key, "settle": {"deadline": spec.deadline_ms}}, None, NVC_CLOSE) == ""
# a reason that applied before is given in its own words, as it was: the market over, the trader no longer resting orders
assert bot.sim_withdraw_reason(rec, None, NVDA.deadline_ms + 1) == "已到结束时间，等结果出来：不开新单，挂单撤掉"
assert _nvc_bot(NVC_CLOSE, SIM_WAYS="taker")[0].sim_withdraw_reason(rec, None, NVC_CLOSE) == "模拟交易已改为只吃单"
# only where the card itself holds — between two sessions, every state of its path check (the table above ran on a card that
# knows nothing yet: its creation unread) but a line reached. Decided, it holds nothing (the after hours move nothing either)
# and the order is left to the result, as before
assert card.start_ms == 0 and card.advice_problem(NVC_CLOSE) == "开盘时间未知，无法核验此前是否已触线；暂不给建议"
card.start_ms = NV_CREATED
for kind, closed in ((None, True), ("clear", True), ("ambiguous", True), ("low", False), ("high", False)):
    if kind:
        card.save({"kind": kind, "through": m.et_wall_ms(NVC_DAY, 0, 0), "time": m.et_wall_ms(NVC_DAY, 10, 0), "hi": 211.0, "lo": 159.0})
    assert card.history.get("kind") == kind and bool(card.advice_problem(NVC_CLOSE)) is closed, (kind, card.advice_problem(NVC_CLOSE))
    assert bot.sim_withdraw_reason(rec, None, NVC_CLOSE) == (NVC_WHY if closed else "") and bot.sim_withdraw_reason(rec, None, NVC_CLOSE - 1) == "", kind


# 5. the real trader: the order is a real one on Predict — a BUY of 100 "$210" at 44¢ that expires with the market, in 2027 —
#    and the cancel goes out in the bell's own look, so nothing is there for the results to hit. (Before: open on Predict
#    until the look at 16:30.) The SDK test suite's key (tests/test_trading.py: never a real one) and a stand-in Predict
class _NvcPredict:
    """Predict as the real trader sees it (tests/test_trading.py's FakeApi, trimmed): the market, our own orders."""
    def __init__(self):
        self.jwt, self.jwt_exp, self.auth_error, self.lookup = "t", 0, "", ""
        self.open, self.closed, self.calls, self.next_id = {}, {}, [], 100

    async def ensure_auth(self): pass

    async def market(self, mid):
        return {"id": int(mid), "status": "REGISTERED", "isNegRisk": False, "isYieldBearing": False, "feeRateBps": 200, "conditionId": "0x" + "ab" * 32,
                "outcomes": [{"name": "$160", "indexSet": 1, "onChainId": f"{mid}1"}, {"name": "$210", "indexSet": 2, "onChainId": f"{mid}2"}]}

    async def create_order(self, body):
        self.next_id += 1
        oid, data = str(self.next_id), body["data"]
        self.calls.append(("create", oid))
        self.open[oid] = {"id": oid, "status": "OPEN", "amount": data["order"]["takerAmount"], "amountFilled": "0", "strategy": data["strategy"],
                          "pricePerShare": data["pricePerShare"], "order": {**data["order"]}}
        return {"order_id": oid, "hash": data["order"]["hash"], "code": None}

    async def remove_orders(self, ids):
        self.calls.append(("remove", list(ids)))
        removed = [i for i in ids if i in self.open]
        for i in removed:
            self.closed[i] = {**self.open.pop(i), "status": "CANCELLED"}
        return {"removed": removed, "noop": [i for i in ids if i not in removed]}

    async def orders(self, status="OPEN", **params):
        return [dict(r) for r in (self.open.values() if status == "OPEN" else self.closed.values()) if r["status"] == status]

    async def order(self, oid, hash_=""):
        row = self.open.get(oid) or self.closed.get(oid) or next((r for r in (*self.open.values(), *self.closed.values()) if hash_ and r["order"]["hash"] == hash_), None)
        return dict(row) if row is not None else None

    async def positions(self): return []


class _NvcChain:
    async def account_owner(self, account): return "0x8fd379246834eac74B8419FfdA202CF8051F7A03"
    async def usdt_balance(self, owner=None): return 1000 * 10 ** 18
    async def bnb_balance(self, owner=None): return 10 ** 17
    async def allowance(self, key, owner=None): return 10 ** 30


def _nvc_pending(lb: object, decided: int) -> dict:
    """A real resting order decided at ``decided`` and not sent yet (held back by a cooldown, a sign-in): 挂210 at 44¢."""
    return {"v": 2, "market": NVDA.slug, "slug": NVDA.slug, "item": "NVDA 先触 160/210", "kind": "touch", "key": "NVDA-2027", "side": "up", "label": "挂210",
            "maker": True, "status": "resting", "shares": 0.0, "order": 100.0, "price": 0.44, "fills": [], "revisions": [], "opened": decided,
            "settle": {"deadline": NVDA.deadline_ms}, "version": {"sim_shares": lb.config.sim_shares},
            "live": {"state": "pending", "want": {"maker": True}, "events": [{"at": decided, "what": "决定下单"}]}}


async def _nvc_live() -> None:
    at = NVC_CLOSE - 120_000
    lb, card = _nvc_bot(at, cls=L.LiveBot, LIVE="on", PREDICT_PRIVATE_KEY="0x" + "a" * 64, PREDICT_API_KEY="k")
    api = lb.live.api = _NvcPredict()
    lb.live.chain = _NvcChain()
    await lb.live_prepare()
    assert lb.live.ready, lb.live.ready_error
    for at in (NVC_CLOSE - 120_000, NVC_CLOSE - 60_000, NVC_CLOSE - 1_000):  # 15:58:00 the order goes out; it rests on Predict to the bell
        assert await _nvc_look(lb, card, at, 0.44, 0.47) == {NVC_TID: ("resting", 0.44, 0.0)}, at - NVC_CLOSE
        live = lb.sim_trades()[NVC_TID]["live"]
        assert (live["state"], live["order_id"]) == ("open", "101") and api.calls == [("create", "101")] and list(api.open) == ["101"], (live["state"], api.calls)
    order = api.open["101"]
    assert (order["strategy"], order["pricePerShare"], order["order"]["tokenId"], order["order"]["side"]) == ("LIMIT", str(44 * 10 ** 16), "772", 0), order
    assert order["order"]["takerAmount"] == str(100 * 10 ** 18) and int(order["order"]["expiration"]) == NVDA.deadline_ms // 1000 + 7200  # Predict keeps it until then
    # a resting order decided three seconds ago and held back goes out while the session runs (the send-time re-check)...
    assert lb.live_taker_recheck(_nvc_pending(lb, at - 3_000), at) == ("", False)
    # 16:00:00: withdrawn by the step, the cancel sent and confirmed by its own sync. The results at 16:20 find no order
    got = await _nvc_look(lb, card, NVC_CLOSE, 0.44, 0.47, express=False)
    assert got == {NVC_TID + "#1": ("cancelled", 0.44, 0.0)} and api.calls == [("create", "101"), ("remove", ["101"])] and not api.open, (got, api.calls)
    rec = lb.sim_trades()[NVC_TID + "#1"]
    assert (rec["live"]["state"], rec["live"]["final"], rec["live"]["cancel_why"], rec["withdrawn"]["why"]) == ("done", "cancelled", NVC_WHY, NVC_WHY), rec["live"]
    assert api.closed["101"]["status"] == "CANCELLED" and api.closed["101"]["amountFilled"] == "0"
    # ...and not once the bell has rung: decided at 15:59:58, sent at 16:00:01 — void, no new order on the close
    assert lb.live_taker_recheck(_nvc_pending(lb, NVC_CLOSE - 2_000), NVC_CLOSE + 1_000) == (f"盘口变了：{NVC_WHY}，3 秒前的挂单决定作废", False)
    for at, bid, ask in ((NVC_CLOSE + 5_000, 0.44, 0.47), (NVC_CLOSE + 20 * 60_000, 0.14, 0.16), (NVC_CLOSE + 31 * 60_000, 0.14, 0.16)):
        assert await _nvc_look(lb, card, at, bid, ask) == got and api.calls == [("create", "101"), ("remove", ["101"])] and not api.open, (at - NVC_CLOSE, api.calls)


asyncio.run(_nvc_live())
m.fetch_source = _nv_fetch

print("AUDIT_MODELS_A_OK")
