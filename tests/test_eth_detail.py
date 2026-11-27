"""1.154.75 — the user: "给ETH 5分钟 15分钟 预挂 加个单独的选项 / 然后搜索前面复盘的记录 建议多久前撤单 / 然后ETH 加个复盘详细的功能
下次方便分析".

- preopen.cancel_study: a card's settled records by how long before the start the first fill was seen (finer near the start than
  card_diag's spans), and each cancel time weighed — what would still have filled, what would have been spared; card_diag carries
  it, the review page's 按市场 breakdown shows it.
- /export 预挂明细 [卡] [范围]: a row a settled record with a fill — each side's first fill seen, the underlying's price then and at
  the window's start and end (Binance's 1-minute candles through the 市场涨跌统计's cache), and the study in words; ETH 5- and
  15-minute by default.

(ETH 5- and 15-minute's own settings: tests/test_preopen.py.)

Run: python tests/test_eth_detail.py
"""
import asyncio
import csv
import datetime as dt
import io
import sys
import time
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import preopen as P

NOW = int(dt.datetime(2026, 11, 27, 12, 0, tzinfo=m.BEIJING).timestamp() * 1000)


def record(key: str, start: int, level: float, up: float, down: float, result: str, pnl: float, lead_s: float | None, slug_stem: str = "eth",
           minutes: int = 5, down_lead_s: float | None = None) -> dict:
    """A settled pre-open record: 20 a side at ``level``, ``up`` / ``down`` filled, its first fill seen ``lead_s`` before the start."""
    orders = {}
    for side, filled, lead in (("up", up, lead_s), ("down", down, down_lead_s if down_lead_s is not None else lead_s)):
        orders[side] = {"hash": f"0x{start}{side}{level}", "state": "final", "filled": filled, "index_set": 1 + (side == "up"),
                        **({"fill_ms": int(start - lead * 1000)} if filled > 0 and lead is not None else {})}
    return {"key": key, "slug": f"{slug_stem}-updown-{minutes}m-{start // 1000}" + (f"#{int(level * 100)}" if level < 0.47 else ""),
            "name": f"{slug_stem.upper()} {minutes}分涨跌", "price": str(level), "shares": "20", "start_ms": start, "created_ms": start - 3_600_000,
            "phase": "closed", "accounted": True, "result": result, "pnl": pnl, "attempt": 0, "orders": orders}


# --- cancel_study: the spans and the cancel times -------------------------------------------------------------------------------------
S0 = NOW - 6 * 3_600_000
qs = [P.quote_of(r) for r in (
    record("ETH-5M", S0, 0.47, 20, 0, "down", -9.4, 10),          # seen 10 s before the start: lost
    record("ETH-5M", S0 + 300_000, 0.47, 20, 0, "down", -9.4, 45),  # 45 s: lost
    record("ETH-5M", S0 + 600_000, 0.47, 0, 20, "down", 10.6, 150),  # 2.5 min: won
    record("ETH-5M", S0 + 900_000, 0.47, 20, 20, "up", 1.2, 400),   # 6.7 min, both sides: hedged
    record("ETH-5M", S0 + 1_200_000, 0.47, 20, 0, "up", 10.6, -3),  # seen after the cancel: won
    record("ETH-5M", S0 + 1_500_000, 0.47, 20, 0, "down", -9.4, None),  # no time kept (before 1.151.0)
)]
study = P.cancel_study(qs)
assert (study["n"], study["unknown"], round(study["pnl"], 4)) == (5, 1, 3.6), study
spans = {b["name"]: (b["n"], b["one"], b["pnl"]) for b in study["buckets"]}
assert spans == {"撤单后才读到": (1, 1, 10.6), "开场前 30 秒内": (1, 1, -9.4), "开场前 30–60 秒": (1, 1, -9.4), "开场前 2–3 分钟": (1, 1, 10.6),
                 "开场前 5–10 分钟": (1, 0, 1.2)}, spans
cut = {c["s"]: (c["n"], c["pnl"], c["avoided"]) for c in study["cuts"]}
assert cut[20] == (3, 2.4, 1.2) and cut[60] == (2, 11.8, -8.2) and cut[180] == (1, 1.2, 2.4) and cut[600] == (0, 0.0, 3.6), cut
assert [c["label"] for c in study["cuts"]][:5] == ["开场前 20 秒", "开场前 30 秒", "开场前 60 秒", "开场前 90 秒", "开场前 2 分钟"]
text = P.cancel_study_text("ETH 5分", study, 20)
assert text.startswith("📊 ETH 5分：5 条已结算的成交，合计 +$3.60（另 1 条没记成交时刻）；现在开场前 20 秒撤单"), text
assert "开场前 30 秒内 1 条 −$9.40" in text and "开场前 60 秒 还成交 2 条 +$11.80" in text, text
assert "开场前 60 秒撤单留下的最多（+$11.80，避开 −$8.20）" in text, text
assert P.cancel_study_text("x", P.cancel_study([])) == "x：这段时间没有记下成交时刻的已结算成交"
# card_diag carries it (the review page's 按市场 breakdown)
assert P.card_diag(qs)["cancel"] == study
assert "撤单时间参考（一条记录 = 一场的一个档位" in m.JOURNAL_PAGE and "如果开场前多久撤单" in m.JOURNAL_PAGE


# --- /export 预挂明细 ---------------------------------------------------------------------------------------------------------------
class Clock:
    def __init__(self, now: int):
        self.now, self.config = now, None

    def now_ms(self) -> int:
        return self.now


cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "PREOPEN_ETH5_CANCEL_SECONDS": "120", "PREOPEN_15M": "off"})
bot = m.Bot(cfg, m.Store(":memory:"), Clock(NOW), None)
eth5, eth15, btc5 = m.tally_card("ETH-5M"), m.tally_card("ETH-15M"), m.tally_card("BTC-5M")
s5, s15 = NOW - 3 * 3_600_000, NOW - 2 * 3_600_000  # an ETH 5-minute and an ETH 15-minute window today
rows = [record("ETH-5M", s5, 0.47, 20, 0, "down", -9.4, 75.5),
        record("ETH-5M", s5, 0.43, 0, 0, "", 0.0, None),  # nothing filled: no row
        record("ETH-15M", s15, 0.47, 20, 20, "up", 1.2, 600, slug_stem="eth", minutes=15, down_lead_s=30),
        record("BTC-5M", s5, 0.47, 0, 20, "up", -9.4, 5, slug_stem="btc")]
for r in rows[2:3]:
    r["name"] = "ETH 15分涨跌"
bot.preopen_rows = lambda: rows
# the candles: ETH's 1-minute closes over the day, rising a dollar a minute from 3,000 — put in the 市场涨跌统计's cache, nothing read
source, symbol = m.tally_source(eth5)
assert (source, symbol) == ("spot", "ETHUSDT")
day0 = NOW - 24 * 3_600_000
for chunk in range(day0 // m.TALLY_CHUNK_MS * m.TALLY_CHUNK_MS, NOW + 3_600_000, m.TALLY_CHUNK_MS):
    bot.tally_cache[(source, symbol, chunk)] = (time.monotonic(), {t: 3000.0 + (t - day0) / 60_000 for t in range(chunk, chunk + m.TALLY_CHUNK_MS, 60_000)})
price = lambda t: 3000.0 + (t - day0) / 60_000
req = lambda *words: m.Request("export", list(words), 1, 0, 1)
doc, said = asyncio.run(bot.cmd_export(req("预挂明细")))
assert isinstance(doc, m.Document) and doc.filename.startswith("preopen-fills-") and doc.mime == "text/csv", doc
assert "预挂成交明细（ETH 5分、ETH 15分，" in doc.caption and "2 条" in doc.caption, doc.caption
table = list(csv.reader(io.StringIO(doc.data.decode("utf-8-sig"))))
assert tuple(table[0]) == m.Bot.FILL_DETAIL_COLUMNS, table[0]
col = {name: i for i, name in enumerate(table[0])}
assert [r[col["卡片"]] for r in table[1:]] == ["ETH 5分", "ETH 15分"], "ETH 5- and 15-minute by default, the record that filled nothing left out"
r5, r15 = table[1], table[2]
spec5 = eth5.window_spec(s5)
fill5 = s5 - 75_500
assert (r5[col["先成交的一边"]], r5[col["首次成交距开场（秒）"]], r5[col["涨边首次成交距开场（秒）"]], r5[col["跌边首次成交距开场（秒）"]]) == ("涨", "75.5", "75.5", ""), r5
assert float(r5[col["首次成交时标的价"]]) == price(fill5 // 60_000 * 60_000 - 60_000), "the last minute closed before the fill"
assert (float(r5[col["起点价"]]), float(r5[col["终点价"]])) == (price(spec5.start_ms), price(spec5.end_ms)), "the window's start and end as the card reads them"
assert abs(float(r5[col["首次成交到开场标的涨跌（%）"]]) - round((price(spec5.start_ms) / price(fill5 // 60_000 * 60_000 - 60_000) - 1) * 100, 4)) < 1e-9
assert abs(float(r5[col["场内涨跌（%）"]]) - round((price(spec5.end_ms) / price(spec5.start_ms) - 1) * 100, 4)) < 1e-9
assert (r5[col["结果"]], r5[col["盈亏"]], r5[col["涨边盈亏"]], r5[col["跌边盈亏"]], r5[col["档位价"]]) == ("跌", "-9.4", "-9.4", "", "0.47"), r5
assert (r15[col["先成交的一边"]], r15[col["首次成交距开场（秒）"]], r15[col["涨边首次成交距开场（秒）"]], r15[col["跌边首次成交距开场（秒）"]]) == ("涨", "600.0", "600.0", "30.0")
assert r15[col["结果"]] == "涨" and r15[col["场次名"]] == "ETH 15分涨跌"
assert said.startswith("📊 ETH 5分：1 条已结算的成交，合计 −$9.40；现在开场前 120 秒撤单"), said
assert "📊 ETH 15分：1 条已结算的成交" in said and "现在开场前 20 秒撤单" in said, said
# a card named, a range: BTC 5-minute alone — its candles' reads failing (no network here): the prices blank, the rows kept, asked once
b_source, b_symbol = m.tally_source(btc5)
asked = []
bot.tally_fetch = lambda source, symbol, chunks: asked.append(list(chunks))
started = time.monotonic()
doc, said = asyncio.run(bot.cmd_export(req("预挂明细", "btc", "5分", "7d")))
assert len(asked) == 1 and time.monotonic() - started < 5, (asked, time.monotonic() - started)
table = list(csv.reader(io.StringIO(doc.data.decode("utf-8-sig"))))
assert [r[0] for r in table[1:]] == ["BTC 5分"] and table[1][col["先成交的一边"]] == "跌", table
assert table[1][col["起点价"]] == "", "no candles of BTCUSDT in the cache: the prices left blank, the row kept"
assert said.startswith("📊 BTC 5分：")
# nothing in the range
said = asyncio.run(bot.cmd_export(req("预挂明细", "2026-10-01", "2026-10-02")))
assert said.startswith("ETH 5分、ETH 15分：10-01 至 10-02没有已结算的预挂成交") or "没有已结算的预挂成交" in said, said
assert "/export 预挂明细 [卡] [范围]" in bot.export_menu()
assert m.Bot.EXPORT_KINDS["预挂明细"] == m.Bot.EXPORT_KINDS["明细"] == "fills"
print("ETH_DETAIL_OK")
