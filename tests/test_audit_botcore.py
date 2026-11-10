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

print("AUDIT_BOTCORE_OK")
