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

print("AUDIT_MODELS_A_OK")
