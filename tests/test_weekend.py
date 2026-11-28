"""1.154.80 — the review page's 周末数据 (an outside design the owner forwarded; the owner chose 方案一 — no archive rebuild —, "全都加"
and the weekend switch left as it is, its words made right).

- preopen: the weekend's hours and its Saturday (weekend_hour / weekend_of: Monday's first hours go with the Saturday before), the
  round-the-clock cards (仅加密: no exchange session, no CME / US hours), week_keep / date_week (the archive's counts by date:
  Saturday and Sunday the weekend's, Monday the weekdays');
- weekend_review: the weekend against the weekdays of the same span, weekend by weekend (each with its cards and stretches), by
  level, by card, by stretch of hours and by the hour, by how long before the start, the newest filled windows — the archive's
  counts routed by date; cards_between / levels_between with a week and a scope;
- main: weekend_span's presets; Bot.journal_weekend (the model trader's weekend beside the pre-open mode's); journal.json?weekend=1
  and &week= on 按市场 / 模型 EV / a line's records; /export 预挂 … 周末; PREOPEN_WEEKEND's words;
- the page: the 周末数据 card, the week buttons — and, where Node and its Playwright are installed, all of it in a browser.

Run: python tests/test_weekend.py
"""
import asyncio
import datetime as dt
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP, except to 127.0.0.1)
import main as m
import preopen as P

BJ = lambda mo, d, h, mi=0: int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
NOW = BJ(11, 25, 10, 0)  # a Wednesday
TOKEN = "t" * 20


def pair(key, name, start, price=0.47, shares=30, up=0.0, down=0.0, result="", level=0, fill_lead_s=None):
    payout = {"up": 1.0, "down": 0.0, "split": 0.5}.get(result)
    pnl = (payout - price) * up + ((1 - payout) - price) * down if payout is not None else 0.0
    orders = {}
    for side, f in (("up", up), ("down", down)):
        orders[side] = {"state": "final", "filled": float(f), **({"fill_ms": start - int(fill_lead_s * 1000)} if f and fill_lead_s is not None else {})}
    return {"slug": f"{key.lower()}-{start // 1000}" + (f"#{level}" if level else ""), "key": key, "name": f"{name} x", "start_ms": start,
            "phase": "closed", "accounted": True, "result": result, "pnl": pnl if (up or down) and payout is not None else 0.0,
            "price": str(price), "shares": str(shares), "level": level, "attempt": 0, "created_ms": start - 3_600_000, "orders": orders}


SAT10, SUN22, MON0330, MON0900, WED10 = BJ(11, 21, 10), BJ(11, 22, 22), BJ(11, 23, 3, 30), BJ(11, 23, 9), BJ(11, 25, 9)
NVDA_SAT01 = BJ(11, 21, 1)  # 11-20 12:00 New York: a US Friday afternoon, in the Beijing weekend
ROWS = [
    pair("BTC-5M", "BTC 5分涨跌", SAT10, up=30, result="up", fill_lead_s=600),           # one side, won: +0.53 × 30
    pair("BTC-5M", "BTC 5分涨跌", SAT10, price=0.43, shares=160, level=1),               # its deeper level: nothing filled
    pair("BTC-5M", "BTC 5分涨跌", SUN22, down=30, result="up", fill_lead_s=30),          # one side, lost: −0.47 × 30
    pair("ETH-15M", "ETH 15分涨跌", MON0330, up=30, down=30, result="down", fill_lead_s=7200),  # both sides: +0.06 × 30
    pair("BTC-5M", "BTC 5分涨跌", MON0900, up=30, result="down"),                        # Monday 09:00: a weekday
    pair("BTC-5M", "BTC 5分涨跌", WED10, down=30, result="down"),                        # a weekday
    pair("NVDA-15M", "英伟达 15分涨跌", NVDA_SAT01, up=30, result="down", fill_lead_s=60),  # the stock's Friday afternoon
]

# --- the helpers ------------------------------------------------------------------------------------------------------------------------
assert P.weekend_hour(BJ(11, 21, 0)) == 0 and P.weekend_hour(BJ(11, 22, 23, 59)) == 47 and P.weekend_hour(BJ(11, 23, 7, 59)) == 55
assert P.weekend_hour(BJ(11, 23, 8)) is None and P.weekend_hour(BJ(11, 20, 23, 59)) is None
assert P.weekend_of(MON0330) == P.weekend_of(SUN22) == P.weekend_of(SAT10) == "2026-11-21" and P.weekend_of(MON0900) == ""
assert P.weekend_label("2026-11-21") == "11-21 周末（周六 00:00 – 11-23 周一 08:00）"
assert P.week_keep(MON0330, "weekend") and not P.week_keep(MON0900, "weekend") and P.week_keep(MON0900, "weekday") and P.week_keep(WED10, "")
assert P.date_week("2026-11-21", "weekend") and P.date_week("2026-11-22", "weekend") and not P.date_week("2026-11-23", "weekend")
assert P.date_week("2026-11-23", "weekday"), "Monday's counts go with the weekdays (方案一: kept by date)"
coins = P.round_clock_coins()
assert {"BTC", "ETH", "BNB"} <= coins and not coins & {"英伟达", "黄金", "原油", "宇树", "海力士"}, coins
assert P.crypto_card("BTC 5分涨跌") and P.crypto_card("ETH 小时涨跌") and not P.crypto_card("英伟达 15分涨跌") and not P.crypto_card("黄金 15分涨跌")

# --- weekend_review: the crypto cards (the default) -------------------------------------------------------------------------------------
wr = P.weekend_review(ROWS, "2026-11-16", "2026-11-25")
weekend, weekdays = wr["compare"]
assert weekend["name"].startswith("周末（周六 00:00 到周一 08:00）") and weekdays["name"] == "工作日"
assert weekend["records"] == 4 and weekend["windows"] == 3 and weekend["filled_windows"] == 3, weekend  # (the deeper level a record of SAT10's window)
assert weekdays["records"] == 2 and weekdays["filled_windows"] == 2, weekdays
assert abs(weekend["pnl"] - (0.53 * 30 - 0.47 * 30 + 0.06 * 30)) < 1e-9 and abs(weekend["ev"] - 0.03 * 120) < 1e-9, weekend
assert weekend["both"]["windows"] == 1 and weekend["one"] == {"windows": 2, "won": 1, "pnl": round(0.53 * 30 - 0.47 * 30, 4)}, weekend
assert weekend["avg_price"] == 0.47 and weekend["settled_shares"] == 120
assert all("英伟达" not in q["name"] for q in wr["recent"]), "仅加密: the stock's Friday afternoon is left out"
(w1,) = wr["weekends"]
assert w1["sat"] == "2026-11-21" and w1["name"] == P.weekend_label("2026-11-21") and w1["records"] == 4, w1
assert [c["name"] for c in w1["cards"]] == ["BTC 5分涨跌", "ETH 15分涨跌"] and [s["name"] for s in w1["slots"]] == ["周六 08:00–20:00", "周日 20:00–周一 00:00", "周一 00:00–08:00"]
assert [h["name"] for h in wr["hours"]] == ["周六 10 时", "周日 22 时", "周一 03 时"]
slots = {s["name"]: s for s in wr["slots"]}
assert len(wr["slots"]) == 6 and slots["周六 08:00–20:00"]["records"] == 2 and slots["周六 00:00–08:00"]["records"] == 0 and slots["周一 00:00–08:00"]["both"]["windows"] == 1
assert [x["name"] for x in wr["lead"]] == ["开场前 1 分钟内", "开场前 10–60 分钟", "开场前 1–6 小时"], wr["lead"]
assert [r["start_ms"] for r in wr["recent"]] == [MON0330, SUN22, SAT10]
assert [c["card"] for c in wr["cards"]] == ["BTC 5分涨跌", "ETH 15分涨跌"] and all(c["preopen"] and "diag" in c for c in wr["cards"])
assert [lv["price"] for lv in wr["levels"]["levels"]] == [0.47, 0.43] and wr["levels"]["archived"] is False
assert wr["scope"] == "crypto" and wr["archived"] is False and wr["luck"]["bets"] == 3 and any("五五开的预期" in n for n in wr["notes"]), wr["notes"]
# ...every card: the stock's Friday afternoon is in the weekend, in the 周六 00:00–08:00 stretch
wa = P.weekend_review(ROWS, "2026-11-16", "2026-11-25", scope="all")
assert wa["compare"][0]["records"] == 5 and {s["name"]: s for s in wa["slots"]}["周六 00:00–08:00"]["records"] == 1
assert "英伟达 15分涨跌" in [c["card"] for c in wa["cards"]]
# ...from a version's first run: the windows before it are left out
ws = P.weekend_review(ROWS, "2026-11-16", "2026-11-25", since_ms=BJ(11, 21, 12))
assert ws["compare"][0]["records"] == 2 and [r["start_ms"] for r in ws["recent"]] == [MON0330, SUN22]
# ...days without a weekend: the weekdays alone
wd = P.weekend_review(ROWS, "2026-11-24", "2026-11-25")
assert wd["compare"][0]["records"] == 0 and wd["compare"][1]["records"] == 1 and wd["weekends"] == []


# --- the archive's counts, routed by date (方案一): Saturday and Sunday to the weekend, Monday to the weekdays ---------------------------
class Line:
    def __init__(self, records, windows):
        self.records, self.windows, self.latest = records, windows, 0


class Old:
    """A stand-in for ArchiveShare: its card-and-date and level-and-date lines, and what counted() was asked."""
    def __init__(self):
        self.lines = {"cdate": {("BTC 5分涨跌", "2026-11-21"): Line(100, 50), ("BTC 5分涨跌", "2026-11-23"): Line(40, 20),
                                ("英伟达 15分涨跌", "2026-11-21"): Line(7, 7), ("BTC 5分涨跌", "2026-11-14"): Line(9, 9)},
                      "ldate": {((0.47, 30.0), "2026-11-21"): Line(60, 30), ((0.47, 30.0), "2026-11-23"): Line(5, 5)}}
        self.asked = []

    def counted(self, r, kind, key, qs):
        self.asked.append((kind, key))
        line = self.lines[kind].get(key)
        if line is not None:
            r["records"] += line.records
            r["withdrawn"] += line.records
            r["windows"] += line.windows
        return r


old = Old()
wo = P.weekend_review(ROWS, "2026-11-16", "2026-11-25", old)
assert wo["compare"][0]["records"] == 4 + 100 and wo["compare"][1]["records"] == 2 + 40, wo["compare"]  # Monday's archived: the weekdays'
assert wo["weekends"][0]["records"] == 104 and wo["weekends"][0]["windows"] == 3 + 50
assert ("cdate", ("英伟达 15分涨跌", "2026-11-21")) not in old.asked and ("cdate", ("BTC 5分涨跌", "2026-11-14")) not in old.asked, "仅加密, and the span"
assert wo["levels"]["archived"] is False and all(k[0] != "ldate" for k in old.asked), "a level's counts are not kept by card: left out under 仅加密"
old2 = Old()
woa = P.weekend_review(ROWS, "2026-11-16", "2026-11-25", old2, scope="all")
assert woa["compare"][0]["records"] == 5 + 100 + 7 and woa["levels"]["archived"] is True
lv47 = next(x for x in woa["levels"]["levels"] if x["price"] == 0.47)
assert lv47["records"] == 4 + 60, lv47  # (the stock's Friday afternoon too: every card) Saturday's 60 in; Monday's 5 with the weekdays
assert ("ldate", ((0.47, 30.0), "2026-11-23")) not in old2.asked
# a weekend whose records were all archived still has its line
old3 = Old()
old3.lines["cdate"][("BTC 5分涨跌", "2026-11-15")] = Line(11, 11)  # (a Sunday: the 11-14 weekend)
w3 = P.weekend_review(ROWS, "2026-11-09", "2026-11-25", old3)
assert [w["sat"] for w in w3["weekends"]] == ["2026-11-21", "2026-11-14"] and w3["weekends"][1]["records"] == 9 + 11, w3["weekends"]
# cards_between / levels_between with a week
cw = P.cards_between(ROWS, "2026-11-16", "2026-11-25", week="weekend")
cd = P.cards_between(ROWS, "2026-11-16", "2026-11-25", week="weekday")
assert sum(c["records"] for c in cw) == 5 and sum(c["records"] for c in cd) == 2 and {c["card"] for c in cd} == {"BTC 5分涨跌"}
assert sum(c["records"] for c in P.cards_between(ROWS, "2026-11-16", "2026-11-25")) == 7, "no week: every window, as before"
lw = P.levels_between(ROWS, "2026-11-16", "2026-11-25", week="weekday")
assert sum(x["records"] for x in lw["levels"]) == 2 and "archived" not in lw, "no scope: the shape as before"

# --- main: the spans of 周末数据 -------------------------------------------------------------------------------------------------------
span = lambda s, now, **kw: m.weekend_span(s, kw.get("frm", ""), kw.get("to", ""), kw.get("since", ""), now)
d0, d1, since, label = span("last", NOW)
assert (d0, d1, since) == (dt.date(2026, 11, 16), dt.date(2026, 11, 23), 0) and label.startswith("上个周末（11-21"), (d0, d1, label)
d0, d1, _, label = span("4w", NOW)
assert (d0, d1) == (dt.date(2026, 10, 26), dt.date(2026, 11, 25)) and label == "近 4 个周末（10-31 起）", (d0, d1, label)
d0, d1, _, label = span("last", BJ(11, 22, 15))  # a Sunday: this weekend
assert (d0, d1) == (dt.date(2026, 11, 16), dt.date(2026, 11, 22)) and label.startswith("本周末（11-21")
assert span("last", BJ(11, 23, 7))[3].startswith("本周末（11-21") and span("last", BJ(11, 23, 9))[3].startswith("上个周末（11-21")
d0, d1, _, label = span("8w", NOW)
assert d0 == dt.date(2026, 9, 28) and label == "近 8 个周末（10-03 起）"
d0, d1, _, label = span("all", NOW)
assert (d1 - d0).days == m.CARDS_MAX_DAYS - 1 and label.startswith("全部周末")
d0, d1, _, label = span("", NOW, frm="2026-11-01", to="2026-11-10")
assert (d0, d1) == (dt.date(2026, 11, 1), dt.date(2026, 11, 10))
d0, d1, since, label = span("", NOW, since=str(BJ(11, 20, 9)))
assert since == BJ(11, 20, 9) and d0 == dt.date(2026, 11, 20)
assert m.journal_week("WEEKEND") == "weekend" and m.journal_week("x") == "" and m.at_week(MON0330, "weekend") and not m.at_week(MON0330, "weekday")


# --- main: the model trader's weekend, the routes, the week of 按市场 / 模型 EV, the export -----------------------------------------------
class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def trade(opened, payout, price=0.40, card="BTC 日涨跌", key="BTC-DAY"):
    slug = f"t{opened}"
    return slug, {"market": slug, "slug": slug, "item": card, "kind": "updown", "key": key, "side": "up", "label": "吃涨", "maker": False,
                  "fair": 0.55, "signal": 0.15, "opened": opened, "settle": {}, "price": price, "shares": 100, "status": "settled",
                  "filled": opened, "fill_fair": 0.55, "edge": 0.15, "payout": payout, "settled": opened + 3_600_000, "note": ""}


def make_bot():
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "SIM_MARKETS": "all"})
    bot = m.Bot(cfg, m.Store(":memory:"), FM(NOW), None)
    for opened, payout in ((SAT10 + 60_000, 1.0), (MON0330 + 60_000, 0.0), (WED10 + 60_000, 1.0)):
        slug, t = trade(opened, payout)
        bot.store.put(f"sim:{slug}|up|吃", t)
    bot.preopen_rows = lambda: list(ROWS)
    bot.preopen_weekend = lambda first, last, since_ms=0, scope="crypto": P.weekend_review(ROWS, first, last, None, since_ms, scope)
    bot.preopen_cards = lambda first, last, since_ms=0, week="": P.cards_between(ROWS, first, last, None, since_ms, week)
    bot.preopen_levels = lambda first, last, since_ms=0, week="": P.levels_between(ROWS, first, last, None, since_ms, week)
    return bot


bot = make_bot()
j = bot.journal_weekend("4w")
assert j["label"] == "近 4 个周末（10-31 起）" and j["scope"] == "crypto" and j["from"] == "2026-10-26" and j["to"] == "2026-11-25"
sw, swd = j["sim"]["compare"]
assert sw["records"] == 2 and swd["records"] == 1 and abs(sw["pnl"] - (0.60 * 100 - 0.40 * 100)) < 1e-9, sw
assert [w["sat"] for w in j["sim"]["weekends"]] == ["2026-11-21"] and j["sim"]["cards"][0]["name"] == "BTC 日涨跌"
assert j["preopen"]["compare"][0]["records"] == 4 and bot.journal_weekend("4w", scope="all")["preopen"]["compare"][0]["records"] == 5
assert bot.journal_weekend("nonsense")["label"].endswith("（30 天）"), "an unknown preset: the last 30 days"
web = m.WebServer(bot, 0, TOKEN)
status, _, body = web.route("GET", f"/p/{TOKEN}/journal.json", "weekend=1&span=last&scope=all")
got = json.loads(body)
assert status == 200 and got["span"] == "last" and got["scope"] == "all" and got["preopen"]["weekends"][0]["sat"] == "2026-11-21"
web.route("GET", f"/p/{TOKEN}/journal.json", "weekend=1&span=last&scope=crypto")
assert {"journal.json?weekend&span=last&from=&to=&since=&scope=all", "journal.json?weekend&span=last&from=&to=&since=&scope=crypto"} <= set(web.cache)
assert json.loads(web.route("GET", f"/p/{TOKEN}/journal.json", "weekend=1")[2])["span"] == "4w", "no span: the latest four"
cards = json.loads(web.route("GET", f"/p/{TOKEN}/journal.json", "cards=1&range=7d&week=weekend")[2])
assert cards["week"] == "weekend" and cards["label"].endswith(" · 仅周末"), cards["label"]
sim_rows = [r for r in cards["rows"] if not r.get("preopen")]
assert sum(r["records"] for r in sim_rows) == 2 and sum(r["records"] for r in cards["rows"] if r.get("preopen")) == 5
assert sum(r["records"] for r in json.loads(web.route("GET", f"/p/{TOKEN}/journal.json", "cards=1&range=7d&week=weekday")[2])["rows"] if not r.get("preopen")) == 1
assert sum(r["records"] for r in json.loads(web.route("GET", f"/p/{TOKEN}/journal.json", "cards=1&range=7d")[2])["rows"] if not r.get("preopen")) == 3
ev = json.loads(web.route("GET", f"/p/{TOKEN}/journal.json", "ev=1&range=30d&week=weekday")[2])
assert ev["week"] == "weekday" and ev["label"].endswith(" · 仅工作日") and ev["ev"]["total"]["bets"] == 1 + 2, ev["ev"]["total"]  # (1 trade + 2 pre-open windows)
evw = json.loads(web.route("GET", f"/p/{TOKEN}/journal.json", "ev=1&range=30d&week=weekend")[2])
assert evw["ev"]["total"]["bets"] == 2 + 4, evw["ev"]["total"]  # (2 trades + the four weekend windows: 模型 EV takes every card, the stock's Beijing Saturday 01:00 too)
rows = json.loads(web.route("GET", f"/p/{TOKEN}/journal.json", "rows=1&range=7d&week=weekend")[2])
assert len(rows["trades"]) == 2 and rows["total"]["records"] == 2


async def export() -> None:
    docs = await bot.export_preopen(["周末", "全部"])
    assert isinstance(docs, list) and docs[0].caption.startswith("预挂记录 5 条（全部日期，仅周末"), docs[0].caption
    lines = docs[0].data.decode("utf-8-sig").strip().splitlines()
    assert len(lines) == 1 + 5, lines
    weekdays = await bot.export_preopen(["工作日"])
    assert weekdays[0].caption.startswith("预挂记录 2 条（全部日期，仅工作日"), weekdays[0].caption
    none = await bot.export_preopen(["周末", "2026-11-24"])
    assert isinstance(none, str) and "的周末没有成交" in none, none
asyncio.run(export())
assert "周末" in bot.export_menu() and "美股卡周五美东 11:00" in m.CONTROL_LABELS.get("PREOPEN_WEEKEND", "") + dict((k, h) for k, _, h in m.CONTROL_KEYS)["PREOPEN_WEEKEND"]

# --- the page --------------------------------------------------------------------------------------------------------------------------
page = m.JOURNAL_PAGE
for want in ('id="wkbox"', 'id="wkbar"', 'id="wkbar2"', 'id="cweek"', 'id="evweek"', "function wkView(", "function wkTable(", '"week="+cardWeek', '"week="+evWeek',
             '#cbar .tq[data-r]', '#evbar .tq[data-r]', "看完整的周末数据 ↑"):
    assert want in page, want
assert page.index('id="sumbox"') < page.index('id="wkbox"') < page.index('id="tallybox"'), "between the daily summary and 市场涨跌统计"
node = shutil.which("node") or ("/opt/node22/bin/node" if os.path.exists("/opt/node22/bin/node") else "")
if node:
    with tempfile.TemporaryDirectory() as tmp:
        js = pathlib.Path(tmp, "page.js")
        js.write_text("\n;\n".join(re.findall(r"<script>(.*?)</script>", page, re.S)), encoding="utf-8")
        assert subprocess.run([node, "--check", str(js)], capture_output=True).returncode == 0, "the review page's script does not parse"

CHECK = r"""
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 390, height: 900 } });
  const errors = [];
  page.on('pageerror', e => errors.push(String(e)));
  await page.goto(process.argv[2]);
  await page.waitForSelector('#list .tr, #list p', { timeout: 15000 });
  await page.click('#wkhd');
  await page.waitForFunction(() => /① 周末 vs 同区间工作日/.test(document.getElementById('wk').textContent), null, { timeout: 15000 });
  const head = await page.textContent('#wksum');
  const wk = await page.textContent('#wk');
  // ② a weekend opened: its cards and stretches
  await page.evaluate(() => { const t = [...document.querySelectorAll('#wk .sub')].find(x => x.textContent.startsWith('② ')); t.nextElementSibling.querySelectorAll('tr')[1].click(); });
  await page.waitForFunction(() => /：按标的/.test(document.getElementById('wk').textContent), null, { timeout: 10000 });
  // ④ a card opened: where its money went
  await page.evaluate(() => { const t = [...document.querySelectorAll('#wk .sub')].find(x => x.textContent.startsWith('④ ')); t.nextElementSibling.querySelectorAll('tr')[1].click(); });
  await page.waitForFunction(() => /钱亏在哪/.test(document.getElementById('wk').textContent), null, { timeout: 10000 });
  // the model trader's, and every card
  await page.click('#wkbar2 .tq[data-m="both"]');
  await page.waitForFunction(() => /原策略（按下单时间/.test(document.getElementById('wk').textContent), null, { timeout: 10000 });
  await page.click('#wkbar2 .tq[data-c="all"]');
  await page.waitForFunction(() => /全部卡/.test(document.getElementById('wk').textContent), null, { timeout: 10000 });
  const all = await page.textContent('#wk');
  // 按市场: 仅周末
  await page.evaluate(() => { document.getElementById('sumbody').hidden = false; });
  await page.click('#cbar .tq[data-r="7d"]');
  await page.click('#cweek .tq[data-w="weekend"]');
  await page.waitForFunction(() => /仅周末/.test(document.getElementById('scrng').textContent), null, { timeout: 10000 });
  const cards = await page.textContent('#scrng');
  console.log(JSON.stringify({ head, wk: wk.slice(0, 4000), all: all.slice(0, 200), cards, errors }));
  await browser.close();
})().catch(e => { console.log(JSON.stringify({ failed: e.message })); process.exit(2); });
"""


async def browser_check() -> None:
    probe = subprocess.run(["node", "-e", "require('playwright')"], capture_output=True) if shutil.which("node") else None
    if probe is None or probe.returncode != 0:
        print("browser check skipped (no Node Playwright)")
        return
    b = make_bot()
    web = m.WebServer(b, 0, TOKEN)
    web.CACHE_SECONDS = {}
    asked: list[str] = []
    plain = web.route

    def route(method, path, query=""):
        asked.append(query)
        return plain(method, path, query)
    web.route = route
    port = await web.start()
    try:
        with tempfile.TemporaryDirectory() as tmp:
            script = pathlib.Path(tmp, "check.js")
            script.write_text(CHECK, encoding="utf-8")
            proc = await asyncio.create_subprocess_exec("node", str(script), f"http://127.0.0.1:{port}/p/{TOKEN}/journal", stdout=asyncio.subprocess.PIPE, env={**os.environ})
            out, _ = await asyncio.wait_for(proc.communicate(), 120)
    finally:
        await web.stop()
    result = json.loads(out.decode().strip().splitlines()[-1])
    assert "failed" not in result and not result["errors"], result
    assert result["head"].startswith("近 4 个周末") and "预挂 3 条已结算" in result["head"], result["head"]
    for want in ("① 周末 vs 同区间工作日", "② 逐个周末", "11-21 周末（周六 00:00 – 11-23 周一 08:00）", "③ 周末按价格档位", "④ 周末按标的", "⑤ 周末按时段",
                 "周六 08:00–20:00（亚洲、欧洲的周六白天）", "按小时（周六 00 时到周一 07 时）", "⑥ 按成交时距开场", "周末有成交的场次"):
        assert want in result["wk"], (want, result["wk"][:600])
    assert "全部卡" in result["all"] and result["cards"].endswith("仅周末"), result
    assert any(q.startswith("weekend=1&span=4w&scope=crypto") for q in asked) and any("weekend=1&span=4w&scope=all" in q for q in asked), asked
    assert any("cards=1" in q and "week=weekend" in q for q in asked), asked
    print("browser ok")


asyncio.run(browser_check())
print("WEEKEND_OK")
