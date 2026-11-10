"""Regression tests for the bot-core audit findings (cluster botcore)."""
import csv, io, math, sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

# --- BOT-CSV-REALIZED-Z: the /samples CSV's realized_z is the residual /calib and SIGMA_CALIB use (sample_z) -------------
cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "BTCUSDT"})
store = m.Store(":memory:")
class FakeMarket:
    def now_ms(self): return 1_790_000_000_000
bot = m.Bot(cfg, store, FakeMarket(), None)
store.delete_prefix("wsample:")
# a daily crypto sample at 12 h left whose price had already moved +1% from the line; the day closed +1.5% from the line
tau = 12 * 3600 / (365 * 86400)
settled = {"t": 1_789_990_000_000, "slug": "btc-updown-day-x", "key": "BTC-DAY", "item": "BTC 日涨跌", "kind": "day", "card": "BTC 日涨跌",
           "start": 1, "end": 2, "left": 43200, "up": 0.7, "sigma": 0.5, "sigma_raw": 0.5, "tau": tau, "price": 101.0, "line": 100.0, "model": 2}
assert bot.window_settle_row(settled, 101.5)
store.put("wsample:btc-updown-day-x:43200", settled)
# a sample of a market not ended yet: no result, so no realized_z
store.put("wsample:btc-updown-day-y:43200", {**settled, "slug": "btc-updown-day-y", "final": None, "move": None, "result": None, "t": 1_789_990_000_001})
table = list(csv.reader(io.StringIO(bot.window_csv().lstrip("﻿"))))
head, rows = table[0], {r[3]: r for r in table[1:]}
row = next(r for r in bot.window_rows() if r["slug"] == "btc-updown-day-x")
st = 0.5 * math.sqrt(tau)
z_cell = rows["btc-updown-day-x"][head.index("realized_z")]
assert z_cell == f"{m.sample_z(row):.6g}" and abs(float(z_cell) - math.log(101.5 / 101.0) / st) < 1e-5, z_cell
assert abs(float(z_cell) - 0.266851) < 1e-5 and abs(float(z_cell) - row["move"] / st) > 0.5, z_cell  # not the move from the line (≈0.8045)
assert rows["btc-updown-day-x"][head.index("sigma_sqrt_tau")] == f"{st:.6g}"
assert rows["btc-updown-day-y"][head.index("realized_z")] == ""
store.delete_prefix("wsample:")

# --- BOT-STOCK-QUOTE-CLOSE: a realtime quote stamped before the closing auction is not that day's close -----------------
import asyncio, datetime as dt
D = m.D
def bj_ms(y, mo, d, h, mi): return int(dt.datetime(y, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
def tencent_sh(last, prev, when):
    fields = ["0"] * 40
    fields[3], fields[4], fields[30] = last, prev, when
    return ('v_sh688836="' + "~".join(fields) + '";').encode("gbk")
sh_info = m.STOCK_MARKETS["sh"]
stuck = tencent_sh("47.90", "50.00", "20261009143000")  # feed stuck on the 14:30 print
try:
    got = m.parse_quote_close("腾讯", "sh", stuck, sh_info, bj_ms(2026, 10, 9, 15, 20))
    raise AssertionError(f"a 14:30 print was taken as the close: {got}")
except ValueError as error:
    assert "早于收盘竞价" in str(error), error
try:  # also on a later day (the stuck record still dated 10-09)
    m.parse_quote_close("腾讯", "sh", stuck, sh_info, bj_ms(2026, 10, 10, 10, 0)); raise AssertionError("stale close accepted")
except ValueError:
    pass
# while the session runs the record still gives the previous close (unchanged behaviour)
assert m.parse_quote_close("腾讯", "sh", stuck, sh_info, bj_ms(2026, 10, 9, 14, 40))[:2] == (None, D("50.00"))
good = tencent_sh("48.10", "50.00", "20261009150003")
assert m.parse_quote_close("腾讯", "sh", good, sh_info, bj_ms(2026, 10, 9, 15, 20))[:2] == (dt.date(2026, 10, 9), D("48.10"))
hk_info = m.STOCK_MARKETS["hk"]
sina_hk_early = ('var hq_str_rt_hk00625="SHEIN,希音,38.00,38.10,38.20,37.50,37.76,' + ",".join(["0"] * 10) + ',2026/10/09,15:40:00";').encode("gbk")
try:
    m.parse_quote_close("新浪", "hk", sina_hk_early, hk_info, bj_ms(2026, 10, 9, 17, 0)); raise AssertionError("HK 15:40 print accepted")
except ValueError:
    pass

async def quote_close_fallback():
    # Eastmoney down, Tencent stuck on 14:30 -> StockMarket asks Sina, whose 15:00:03 close is used
    sina_good = ('var hq_str_sh688836="宇树科技,49.00,50.00,48.10,' + ",".join(["0"] * 26) + ',2026-10-09,15:00:03,00";').encode("gbk")
    async def fake_get(url, timeout=15, headers=None):
        if "eastmoney" in url: raise m.RemoteError("网络错误 (RemoteDisconnected)")
        if "qt.gtimg.cn" in url: return stuck
        if "sina" in url: return sina_good
        raise AssertionError(url)
    real_get, real_sleep = m.http_get, m.asyncio.sleep
    async def no_sleep(_s): await real_sleep(0)
    m.http_get, m.asyncio.sleep = fake_get, no_sleep
    try:
        cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT"})
        sm = m.StockMarket(cfg, m.Store(":memory:"))
        await sm.refresh(bj_ms(2026, 10, 9, 15, 20), force=True)
        close = sm.closes["UNITREEUSDT"]
        assert close.value == D("48.10") and "新浪" in close.source, (close, sm.errors)
        # Sina stuck too -> no close for the day is invented from a 14:30 print
        sina_stuck = ('var hq_str_sh688836="宇树科技,49.00,50.00,47.90,' + ",".join(["0"] * 26) + ',2026-10-09,14:30:00,00";').encode("gbk")
        async def all_stuck(url, timeout=15, headers=None):
            if "eastmoney" in url: raise m.RemoteError("网络错误 (RemoteDisconnected)")
            return stuck if "qt.gtimg.cn" in url else sina_stuck
        m.http_get = all_stuck
        sm2 = m.StockMarket(cfg, m.Store(":memory:"))
        await sm2.refresh(bj_ms(2026, 10, 9, 15, 20), force=True)
        assert "UNITREEUSDT" not in sm2.closes or sm2.closes["UNITREEUSDT"].value != D("47.90"), sm2.closes
        assert "早于收盘竞价" in sm2.errors.get("UNITREEUSDT", ""), sm2.errors
    finally:
        m.http_get, m.asyncio.sleep = real_get, real_sleep
asyncio.run(quote_close_fallback())

# --- BOT-NOTE-CLOSES-SIM-OFF: with the paper trader off, the stock cards' closes are still recorded for /calib ----------
close_ms = bj_ms(2026, 10, 5, 15, 0)
for env in ({"SIM": "off"}, {"PREDICT": "off"}):
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", **env})
    store = m.Store(":memory:")
    bot = m.Bot(cfg, store, m.Binance(cfg), None)
    assert "模拟交易" not in [n for n, _ in bot.reference_jobs()]  # sim_step (the old only caller) is not scheduled
    bot.sse_odds = bot.kospi_odds = bot.hsi_odds = lambda now: "暂缺"
    bot.stocks.closes["UNITREEUSDT"] = m.Baseline(D("75.5"), "k", "label", 0, close_ms, "", "src")
    bot.record_predictions(close_ms + 3_600_000)
    assert store.get("outcome:UNITREEUSDT:2026-10-05") == 75.5, (env, store.get("outcome:UNITREEUSDT:2026-10-05"))
    assert store.get("outsrc:UNITREEUSDT:2026-10-05")["source"] == "src", env
    bot.record_predictions(close_ms + 7_200_000)  # again: idempotent
    assert store.get("outcome:UNITREEUSDT:2026-10-05") == 75.5

# --- BOT-SIGMA-CALIB-SCAN: the windows settling at a quarter hour cost one pass over the samples, not one per card ---------
# (each sigma_calib miss used to re-read, parse and sort every wsample: row for the one key asked for; refresh_updown asks for
# every card's, so the refresh after twenty-odd windows settled made twenty-odd full passes back to back on the event loop)
T_QH = bj_ms(2026, 11, 10, 21, 0)  # a Tuesday night quarter hour: the round-the-clock window cards end together
class Clock:
    now = T_QH - 1_000
    def now_ms(self): return self.now
clock, sstore = Clock(), m.Store(":memory:")
sbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), sstore, clock, None)
assert sbot.config.sigma_calib  # on by default
async def no_read(*a): return None
for mk in sbot.updowns.values():
    mk.refresh = no_read  # the card's own Binance reads: not under test
sbot.updown_strike = no_read
def srow(key, slug, kind, left, t, final, model):
    """A sample; settled (final, move, result) when ``final`` is given."""
    tau = left / (365 * 86400)
    return {"t": t, "slug": slug, "key": key, "item": slug, "kind": kind, "card": key, "start": t - 60_000, "end": t + left * 1000, "left": left,
            "up": 0.5, "sigma": 0.6, "sigma_raw": 0.6, "calib": 1.0, "tau": tau, "price": 100.0, "line": 100.0, "model": model,
            "final": final, "move": None if final is None else math.log(final / 100.0), "result": None if final is None else float(final > 100.0)}
for ci, (key, wspec) in enumerate(sbot.updown_windows.items()):  # six settled past windows a card, z from -1.4 to 1.4
    model, lefts = sbot.updowns[key].SIGMA_MODEL, (300, 120) if wspec.minutes == 5 else (840, 300)
    for i in range(6):
        t = T_QH - (i + 2) * 900_000
        for left in lefts:
            final = 100.0 * math.exp(0.6 * math.sqrt(left / (365 * 86400)) * (((ci + i) % 5) - 2) * 0.7 * (1 if left == lefts[0] else 3))
            sstore.put(f"wsample:{key.lower()}-past-{i}:{left}", srow(key, f"{key.lower()}-past-{i}", "window", left, t, final, model))
    sstore.put(f"wsample:{key.lower()}-old:840", srow(key, f"{key.lower()}-old", "window", 840, T_QH - 99 * 900_000, 103.0, model - 1))  # older estimator
for di, key in enumerate(sbot.updown_daily):
    for d in range(3):
        sstore.put(f"wsample:{key.lower()}-past-{d}:43200", srow(key, f"{key.lower()}-past-{d}", "day", 43200, T_QH - (d + 1) * 86_400_000,
                                                                 100.0 * math.exp(0.6 * math.sqrt(0.5 / 365) * (d - di) * 0.9), m.UpDownMarket.SIGMA_MODEL))
passes: list[int] = []
real_rows = sbot.window_rows
def counted_rows():
    passes.append(1)
    return real_rows()
sbot.window_rows = counted_rows

def calib_alone(key):
    """One key measured on its own, as sigma_calib did before (the reference: same rows, same rule)."""
    current = m.UpDownMarket.SIGMA_MODEL if key == m.CRYPTO_DAY_CALIB else getattr(sbot.updowns.get(key), "SIGMA_MODEL", m.IndexUpDownMarket.SIGMA_MODEL)
    firsts: dict = {}
    for r in real_rows():
        mine = r.get("kind") == "day" if key == m.CRYPTO_DAY_CALIB else (r.get("key") == key and r.get("kind") == "window")
        if not mine or r.get("move") is None or int(r.get("model") or 1) < current or not r.get("sigma_raw") or not r.get("tau"):
            continue
        if r["slug"] not in firsts or int(r["left"]) > int(firsts[r["slug"]]["left"]):
            firsts[r["slug"]] = r
    zs = [z for z in (m.sample_z(r, raw=True) for r in firsts.values()) if z is not None]
    f = math.sqrt((sum(z * z for z in zs) + sbot.SIGMA_CALIB_PRIOR) / (len(zs) + sbot.SIGMA_CALIB_PRIOR)) if zs else 1.0
    return min(max(f, sbot.SIGMA_CALIB_RANGE[0]), sbot.SIGMA_CALIB_RANGE[1]), len(zs)

def calib_key(key, mk): return key if isinstance(mk, m.IndexUpDownMarket) else m.CRYPTO_DAY_CALIB
def check_factors():
    for key, mk in sbot.updowns.items():
        assert mk.calib == calib_alone(calib_key(key, mk)), (key, mk.calib, calib_alone(calib_key(key, mk)))  # exactly the same figure

def settle_boundary(end_ms, close):
    """The cards whose window ends at end_ms: their own samples (not settled yet) and their final close kept, as the card's refresh reads it."""
    ending = [key for key in sbot.updown_windows if sbot.updowns[key].spec.end_ms + 60_000 == end_ms]
    for key in ending:
        spec = sbot.updowns[key].spec
        for left in (300, 120) if sbot.updown_windows[key].minutes == 5 else (840, 600, 300, 120):
            sstore.put(f"wsample:{spec.slug}:{left}", srow(key, spec.slug, "window", left, end_ms - left * 1000, None, sbot.updowns[key].SIGMA_MODEL))
        sstore.put(f"updown:{spec.slug}:end", {"open": spec.end_ms, "close": close, "read_ms": end_ms + 2_000})
    return ending

async def calib_scan():
    await sbot.refresh_updown(clock.now)  # the start: every card's factor measured
    assert len(passes) == 1, f"the first refresh read every sample {len(passes)} times"  # 24 before: one pass per card (the daily cards share one)
    check_factors()
    assert all(sbot.updowns[k].calib[1] for k in sbot.updown_rolling)  # every rolling card has windows behind its factor
    ending = settle_boundary(T_QH, "103.5")
    assert len(ending) >= 3, ending  # the round-the-clock 15-minute and 5-minute cards (twenty in 1.149.0)
    slugs, before = {k: mk.spec.slug for k, mk in sbot.updowns.items()}, {k: mk.calib for k, mk in sbot.updowns.items()}
    passes.clear()
    clock.now = T_QH + 2_000
    await sbot.refresh_updown(clock.now)  # the roll: window_resolve settles each card's samples and drops its factor
    rolled = sorted(k for k, mk in sbot.updowns.items() if mk.spec.slug != slugs[k])
    assert rolled == sorted(ending) and not passes, (rolled, passes)
    assert m.CRYPTO_DAY_CALIB in sbot.sigma_calib_cache, "a 15-minute window settling dropped the daily cards' pooled factor"
    assert not any(k in sbot.sigma_calib_cache for k in rolled)
    clock.now = T_QH + 3_000
    await sbot.refresh_updown(clock.now)
    assert len(passes) == 1, f"{len(rolled)} windows settled; the refresh after read every sample {len(passes)} times"  # len(rolled) + 1 before
    check_factors()
    assert all(sbot.updowns[k].calib != before[k] for k in rolled), "the settled windows (a 3.5% end) move their cards' factors"
    # the 5-minute mark: only the 5-minute cards end — still one pass, the pool and the 15-minute cards' factors kept meanwhile
    five = settle_boundary(T_QH + 300_000, "99.0")
    assert all(sbot.updown_windows[k].minutes == 5 for k in five), five
    if five:  # (while the bot has 5-minute cards: BTC / ETH / BNB in 1.149.0)
        slugs = {k: mk.spec.slug for k, mk in sbot.updowns.items()}
        passes.clear()
        clock.now = T_QH + 302_000
        await sbot.refresh_updown(clock.now)
        assert sorted(k for k, mk in sbot.updowns.items() if mk.spec.slug != slugs[k]) == sorted(five) and not passes
        assert m.CRYPTO_DAY_CALIB in sbot.sigma_calib_cache and all(k in sbot.sigma_calib_cache for k in ending if k not in five)
        clock.now = T_QH + 303_000
        await sbot.refresh_updown(clock.now)
        assert len(passes) == 1, passes  # 4 before: the three cards' and the pool's
        check_factors()
    # a daily market settling still drops the pool (the next refresh measures it again), a window never does
    btc_day = next(k for k in sbot.updown_daily)
    sbot.window_resolve(sbot.updowns[btc_day], sbot.updowns[btc_day].spec, D("1"))
    assert m.CRYPTO_DAY_CALIB not in sbot.sigma_calib_cache
    assert sbot.sigma_calib(m.CRYPTO_DAY_CALIB) == calib_alone(m.CRYPTO_DAY_CALIB)
    # a fresh start on the same samples: one pass for all the cards
    fresh = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), sstore, clock, None)
    for mk in fresh.updowns.values():
        mk.refresh = no_read
    fresh.updown_strike = no_read
    fresh_passes: list[int] = []
    fresh_rows = fresh.window_rows
    fresh.window_rows = lambda: fresh_passes.append(1) or fresh_rows()
    await fresh.refresh_updown(clock.now)
    assert len(fresh_passes) == 1 and all(fresh.updowns[k].calib == sbot.updowns[k].calib for k in sbot.updowns), fresh_passes
asyncio.run(calib_scan())

# index_sigma_calibs (on each outcome): the three indices' own snapshots and outcomes, not every card's
import random
rnd = random.Random(5)
for key in ("HSI", "KOSPI", "SSE", "UNITREEUSDT", "SKHYNIXUSDT"):
    for i in range(30):
        day, ms = f"2026-09-{1 + i % 25:02d}", 1_790_000_000_000 + i * 1_800_000
        sstore.put(f"pred:{key}:{ms}", {"key": key, "t": ms, "target": day, "mode": "盘中" if i % 3 else "盘后", "ref": 100.0, "ref_raw": 100.0 + i % 2,
                                        "up": 0.5, "move": 0.001 * (i % 5), "beta": 1.0, "sigma": 0.01, "R": 0.5 + (i % 4) / 10})
        sstore.put(f"outcome:{key}:{day}", 100.0 * math.exp(rnd.gauss(0, 0.012)))
sstore.put("pred:KOSPI:9000001", {"key": "KOSPI", "t": 9000001, "target": "2026-09-02", "mode": "盘中", "ref": 100.0, "up": 0.5, "move": 0.0,
                                  "beta": 1.0, "sigma": 0.01, "R": 1.0})  # a short stamp sorts after the others, in either read
def index_calibs_reference():
    """The factors as measured before: every card's pred: and outcome: records read, the indices' kept."""
    saved = [v for _, v in sstore.items("pred:") if isinstance(v, dict) and m.Bot.PRED_FIELDS <= v.keys() and v.get("key") in m.PREDICT_KEYS.values()]
    outcomes = {k.removeprefix("outcome:"): float(v) for k, v in sstore.items("outcome:")}
    groups: dict = {}
    for p in saved:
        close = outcomes.get(f"{p['key']}:{p['target']}")
        if close is not None and p.get("ref_raw", p.get("ref")):
            groups.setdefault((p["key"], str(p.get("mode") or "")), []).append(dict(p, y=math.log(close / float(p.get("ref_raw", p["ref"])))))
    return {key: m.residual_factor(rows) for key, rows in groups.items()}
read: list[str] = []
real_items = sstore.items
sstore.items = lambda prefix: read.append(prefix) or real_items(prefix)
got = sbot.index_sigma_calibs()
sstore.items = real_items
assert list(got.items()) == list(index_calibs_reference().items()) and len(got) == 6, got
assert sorted(read) == ["outcome:HSI:", "outcome:KOSPI:", "outcome:SSE:", "pred:HSI:", "pred:KOSPI:", "pred:SSE:"], read  # not "pred:" / "outcome:"

print("AUDIT_BOTCORE_OK")
