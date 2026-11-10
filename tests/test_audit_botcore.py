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

print("AUDIT_BOTCORE_OK")
