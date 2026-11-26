"""1.154.70 — the user: "复盘那里可以选择日期，更新的版本时间到今天这样的选择". The review page's 按市场 range and the 模型 EV 对实际盈亏
section can begin at a version's first run here (vstart:<VERSION>, noted at the start; before that was noted, the version's
earliest record) and run to now — to the minute, not whole days — besides the presets and the custom days; the EV section
has a range of its own (the last 30 days by default, as before).

Here: journal_span; the versions' starts; journal_cards and journal_ev from a moment; sim_ev_timeline over a span; the
pre-open mode's 按市场 rows and price levels from a moment (cards_between, levels_between); the page's markup and script —
and, where Node and its Playwright are installed, the page itself in a browser.

Run: python tests/test_review_span.py
"""
import asyncio
import datetime as dt
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import preopen as p


def bj(mo: int, d: int, h: int, mi: int) -> int:
    return int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)


class Clock:
    def __init__(self, now: int):
        self.now, self.config = now, None

    def now_ms(self) -> int:
        return self.now


NOW = bj(11, 27, 10, 0)

# --- journal_span: a preset or days as before; a moment to now, named by its version ---------------------------------------------
versions = [{"version": "1.154.70", "at": bj(11, 26, 23, 30)}, {"version": "1.154.69", "at": bj(11, 26, 21, 0)}]
d0, d1, since, label = m.journal_span("", "", "7d", "", NOW, 366, versions)
assert (d0, d1, since, label) == (dt.date(2026, 11, 21), dt.date(2026, 11, 27), 0, "11-21 至 11-27（7 天）"), (d0, d1, since, label)
d0, d1, since, label = m.journal_span("", "", "", str(bj(11, 26, 23, 30)), NOW, 366, versions)
assert (d0, d1, since, label) == (dt.date(2026, 11, 26), dt.date(2026, 11, 27), bj(11, 26, 23, 30), "1.154.70 起（11-26 23:30 至今）")
assert m.journal_span("", "", "", str(bj(11, 25, 8, 15)), NOW, 366, versions)[3] == "11-25 08:15 至今", "a moment no version names"
assert m.journal_span("", "", "today", str(NOW + 60_000), NOW, 366, versions)[2:] == (0, "今天"), "a moment ahead: the range as asked"
assert m.journal_span("", "", "today", "abc", NOW, 366, versions)[2] == 0

# --- the versions: noted at the start, else each one's earliest record ----------------------------------------------------------
store = m.Store(":memory:")
cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT"})
bot = m.Bot(cfg, store, Clock(NOW), None)
rec = lambda tid, opened, code, pnl=None: store.put(f"sim:{tid}", {
    "market": tid, "slug": tid, "item": "上证指数", "kind": "close", "key": "SSE", "side": "up", "label": "吃涨", "maker": False,
    "fair": 0.62, "signal": 0.12, "opened": opened, "settle": {}, "price": 0.50, "shares": 100, "edge": 0.12, "version": {"code": code},
    **({"status": "settled", "filled": opened + 1000, "fill_fair": 0.6, "payout": 1.0 if pnl > 0 else 0.0, "settled": opened + 3_600_000}
       if pnl is not None else {"status": "cancelled"})})
rec("a|up|吃", bj(11, 20, 10, 0), "1.154.66", pnl=1)
rec("b|up|吃", bj(11, 26, 9, 40), "1.154.68", pnl=1)
rec("c|up|吃", bj(11, 26, 23, 45), "1.154.70", pnl=-1)
rec("d|up|吃", bj(11, 27, 9, 30), "1.154.70", pnl=1)
bot.store.put("vstart:1.154.70", bj(11, 26, 23, 30))  # noted: earlier than its first record
starts = bot.version_starts()
assert [v["version"] for v in starts] == ["1.154.70", "1.154.68", "1.154.66"], starts
assert starts[0]["at"] == bj(11, 26, 23, 30) and starts[0]["label"] == "1.154.70 起（11-26 23:30）", starts[0]
assert starts[1]["at"] == bj(11, 26, 9, 40), "not noted: its earliest record"
bot.note_version_start()
assert store.get(f"vstart:{m.VERSION}") is not None
kept = store.get(f"vstart:{m.VERSION}")
bot.market.now += 3_600_000
bot.note_version_start()
assert store.get(f"vstart:{m.VERSION}") == kept, "written once: the first run"
bot.market.now = NOW
assert "versions" in bot.journal_payload() and bot.journal_payload()["versions"][0]["version"] in ("1.154.70", m.VERSION)

# --- 按市场 and 模型 EV from a moment -------------------------------------------------------------------------------------------------
since = str(bj(11, 26, 23, 30))
cards = bot.journal_cards({}, "", "", "", since)
assert cards["label"] == "1.154.70 起（11-26 23:30 至今）" and cards["since"] == int(since) and cards["range"] == "", cards["label"]
assert cards["from"] == "2026-11-26" and cards["to"] == "2026-11-27"
assert sum(r["records"] for r in cards["rows"]) == 2, "the records from 23:30 on: not b (09:40 the same day)"
whole_days = bot.journal_cards({}, "2026-11-26", "2026-11-27", "")
assert sum(r["records"] for r in whole_days["rows"]) == 3 and whole_days["since"] == 0
ev = bot.journal_ev({}, "", "", "", since)
assert ev["label"] == "1.154.70 起（11-26 23:30 至今）" and ev["ev"]["label"] == ev["label"] and ev["ev"]["total"]["bets"] == 2, ev["ev"]["total"]
assert ev["ev"]["days_span"] == 2 and [d["day"] for d in ev["ev"]["days"]] == ["2026-11-26", "2026-11-27"]
default = bot.sim_ev()
assert default["days_span"] == m.EV_DAYS and default["label"] == "" and default["total"]["bets"] == 4, "the default: the last 30 days, as before"
assert bot.journal_ev({}, "", "", "7d")["ev"]["total"]["bets"] == 3, "11-21..11-27: not a (11-20)"
assert bot.journal_ev({}, "", "", "today")["ev"]["total"]["bets"] == 1

# --- sim_ev_timeline over a span -------------------------------------------------------------------------------------------------
bets = [m.ev_bet(bj(11, d, 12, 0), "原策略吃单", "上证指数", 0.5, 10, 0.6, 1.0, 5.0) for d in (10, 20, 26, 27)]
t = m.sim_ev_timeline(bets, NOW, first="2026-11-20", last="2026-11-27", since_ms=bj(11, 20, 13, 0), label="x")
assert t["total"]["bets"] == 2 and t["days_span"] == 8 and t["label"] == "x", t["total"]
assert m.sim_ev_timeline(bets, NOW)["total"]["bets"] == 4

# --- the pre-open mode's rows from a moment -------------------------------------------------------------------------------------
pair = lambda slug, start: {"slug": slug, "card": "BTC-5M", "start_ms": start, "level": 0, "price": 0.47, "shares": 30, "phase": "closed",
                            "accounted": True, "result": "up", "orders": {"up": {"state": "final", "filled": 30, "price": 0.47, "hash": slug + "u"},
                                                                         "down": {"state": "final", "filled": 0, "price": 0.47, "hash": slug + "d"}}}
rows = [pair("btc-updown-5m-1", bj(11, 26, 23, 0)), pair("btc-updown-5m-2", bj(11, 26, 23, 40)), pair("btc-updown-5m-3", bj(11, 27, 9, 0))]
try:
    every = p.cards_between(rows, "2026-11-26", "2026-11-27")
    later = p.cards_between(rows, "2026-11-26", "2026-11-27", None, bj(11, 26, 23, 30))
    assert sum(r["records"] for r in every) == 3 and sum(r["records"] for r in later) == 2, (every, later)
    lv_all, lv_later = p.levels_between(rows, "2026-11-26", "2026-11-27"), p.levels_between(rows, "2026-11-26", "2026-11-27", None, bj(11, 26, 23, 30))
    assert sum(r["records"] for r in lv_all["levels"]) == 3 and sum(r["records"] for r in lv_later["levels"]) == 2
except KeyError as error:  # the fixture's record lacks a field quote_of reads: a fuller one from test_preopen
    raise AssertionError(f"pre-open fixture: {error}") from error
assert p.since_day(0) == "" and p.since_day(bj(11, 26, 23, 30)) == "2026-11-26"

# --- the page: the version picks in 按市场's bar and in 模型 EV's own, wired to the queries ---------------------------------------------
page = m.JOURNAL_PAGE
for want in ('id="cver"', 'id="evbar"', 'id="evver"', 'id="evrng"', 'id="evfrom"', 'id="evto"', 'id="evgo"', "function fillVersions(", "function evView(",
             '"since="+cardSince', '"since="+evSince', '["ev=1",...evRange()]', "版本更新起…"):
    assert want in page, want
if shutil.which("node"):  # the script parses
    with tempfile.TemporaryDirectory() as tmp:
        js = pathlib.Path(tmp, "page.js")
        js.write_text("\n;\n".join(__import__("re").findall(r"<script>(.*?)</script>", page, __import__("re").S)), encoding="utf-8")
        assert subprocess.run(["node", "--check", str(js)], capture_output=True).returncode == 0, "the review page's script does not parse"

# --- in a browser, where Node and its Playwright are installed: pick a version in both, no page error -------------------------------
CHECK = r"""
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 390, height: 900 } });
  const errors = [];
  page.on('pageerror', e => errors.push(String(e)));
  await page.goto(process.argv[2]);
  await page.waitForFunction(() => document.querySelectorAll('#cver option').length > 1, null, { timeout: 15000 });
  const v = process.argv[3];  // the moment to pick: one of the options (the version noted at 11-26 23:30)
  if (!(await page.$$eval('#cver option', (os, v) => os.some(o => o.value === v), v))) throw new Error('no option ' + v);
  await page.selectOption('#cver', v);
  await page.waitForFunction(() => /起（/.test(document.getElementById('scrng').textContent), null, { timeout: 10000 });
  await page.evaluate(v => { const s = document.getElementById('evver'); s.value = v; s.dispatchEvent(new Event('change')); }, v);
  await page.waitForFunction(() => /起（/.test(document.getElementById('evrng').textContent), null, { timeout: 10000 });
  const out = { cards: await page.textContent('#scrng'), ev: await page.textContent('#evrng'), body: (await page.textContent('#a-ev')).slice(0, 80) };
  await page.evaluate(() => document.querySelector('#evbar .tq[data-r="30d"]').click());
  await page.waitForFunction(() => document.getElementById('evrng').textContent === '近 30 天');
  console.log(JSON.stringify({ ...out, errors }));
  await browser.close();
})().catch(e => { console.log(JSON.stringify({ failed: e.message })); process.exit(2); });
"""


async def browser_check() -> None:
    probe = subprocess.run(["node", "-e", "require('playwright')"], capture_output=True) if shutil.which("node") else None
    if probe is None or probe.returncode != 0:
        print("browser check skipped (no Node Playwright)")
        return
    web = m.WebServer(bot, 0, "t" * 24)
    web.CACHE_SECONDS = {}
    port = await web.start()
    with tempfile.TemporaryDirectory() as tmp:
        script = pathlib.Path(tmp, "check.js")
        script.write_text(CHECK, encoding="utf-8")
        proc = await asyncio.create_subprocess_exec("node", str(script), f"http://127.0.0.1:{port}/p/{'t' * 24}/journal", str(bj(11, 26, 23, 30)),
                                                    stdout=asyncio.subprocess.PIPE, env={**os.environ})
        out, _ = await asyncio.wait_for(proc.communicate(), 90)
    result = json.loads(out.decode().strip().splitlines()[-1])
    assert "failed" not in result and not result["errors"], result
    assert result["cards"] == "1.154.70 起（11-26 23:30 至今）" and result["ev"] == result["cards"], result
    assert "2 笔" in result["body"], result


asyncio.run(browser_check())
print("REVIEW_SPAN_OK")
