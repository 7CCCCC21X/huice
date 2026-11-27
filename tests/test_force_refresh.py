"""1.154.78 — the owner: "加个强制刷新按钮". While the rule taker goes first (its priority: a makers' moment and its quarter hour, a
window's last quarter hour, a course, the per-second hours) the review page's data and the exports are not built (1.154.26): a
build holds the event loop for seconds. Now the owner can have them made all the same:

- the review page has 强制刷新 in its top line; for a minute after it the page's own reads carry force=1, and a keyed request
  (the control key checked) with it is built now (WEB_FORCE) — also a body still within its interval, unless it was built in
  the last FORCE_FRESH_S;
- only the rule taker's own seconds (rules_quiet — the whole bot keeps out of the loop's way then) are not forced: the page is
  told how many are left instead of being served an old body unasked;
- on a route that keys nothing (samples.csv), force=1 counts for the owner alone (the control key; a wrong one counted);
- /export … 强制, /samples 强制, /tally … 强制 make the export now — the taker's own seconds waited out first (force_quiet);
  the busy answers say how; a forced build made in the priority is written in the log.

And, where Node and its Playwright are installed, the page in a browser: the button there, the 503 turned into data by a click,
the taker's own seconds said, the export after it.

Run: python tests/test_force_refresh.py
"""
import asyncio
import contextlib
import dataclasses
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP, except to 127.0.0.1)
import main as m

TOKEN = "t" * 20
KEY = "k" * 16
NOW = int(time.time() * 1000)
req = lambda command, *args: m.Request(command, list(args), 1, 0, 42)


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def make_bot(key: str = KEY, real: bool = True):
    env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}
    if key:
        env["WEB_CONTROL_KEY"] = key
    bot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(NOW), None)
    bot.sim_has_real = lambda: real  # (a real account behind the data: the review data goes behind the key)
    state = {"clear": True, "quiet": 0.0}
    bot.rules_clear = lambda seconds: state["clear"]
    bot.rules_quiet = lambda: state["quiet"]
    built: list = []
    plain = bot.journal_payload
    bot.journal_payload = lambda full=False, history=None: built.append("full" if full else "list") or plain(full, history)
    bot.window_csv = lambda rows=None: built.append("samples") or "t,card\n"
    return bot, state, built


class Logged(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines: list[str] = []

    def emit(self, record):
        self.lines.append(record.getMessage())


m.LOG.setLevel(logging.INFO)  # (the forced builds' lines are INFO)
m.LOG.propagate = False       # (...caught here, not printed)


def forced(fn):
    token = m.WEB_FORCE.set(True)
    try:
        return fn()
    finally:
        m.WEB_FORCE.reset(token)


# --- the server: a forced build in the priority, not in the taker's own seconds -------------------------------------------------------
def server_rules() -> None:
    bot, state, built = make_bot()
    web = m.WebServer(bot, 0, TOKEN)
    logged = Logged()
    m.LOG.addHandler(logged)
    try:
        get = lambda name, query="": web.route("GET", f"/p/{TOKEN}/{name}", query)
        state["clear"] = False  # the rule taker goes first (or will within two minutes)
        status, _, body = get("journal.json")
        told = json.loads(body)
        assert status == 503 and told["busy"] is True and "规则吃单正在优先时段" in told["message"] and "强制刷新" in told["message"] and built == [], told
        assert web.busy() is True and web.busy_text() == m.WebServer.BUSY_TEXT
        # forced: built now, whatever the priority — and said in the log
        status, _, first = forced(lambda: get("journal.json"))
        assert status == 200 and built == ["list"] and "version" in json.loads(first), (status, built)
        assert any(line.startswith("强制刷新：规则吃单的优先时段里重算了 journal.json") for line in logged.lines), logged.lines
        assert forced(web.busy) is False
        # a second forced request within FORCE_FRESH_S: the body just built, nothing built again
        assert m.WebServer.FORCE_FRESH_S == 15.0 and forced(lambda: get("journal.json"))[2] is first and built == ["list"]
        # ...one built longer ago, still within its interval: built again when forced, served as it is when not
        web.built["journal.json"] -= 20.0
        assert get("journal.json")[2] is first and built == ["list"], "not forced: kept as ever"
        again = forced(lambda: get("journal.json"))[2]
        assert again is not first and built == ["list", "list"]
        # the rule taker's own seconds: even forced, nothing built — and no old body served unasked: how long they last is said
        state["quiet"] = 7.2
        web.built["journal.json"] -= 20.0
        status, _, body = forced(lambda: get("journal.json"))
        told = json.loads(body)
        assert status == 503 and told["busy"] is True and "还剩约 8 秒" in told["message"] and "强制刷新也不算" in told["message"] and built == ["list", "list"], told
        web.cache["journal.json"] = (0.0, again)  # (grown old)
        assert get("journal.json")[2] is again and built == ["list", "list"], "not forced: the last one, as before"
        assert forced(lambda: get("journal.json"))[0] == 503, "forced: said, not the old one"
        state["quiet"] = 0.0
        # the exports and the tally: refused in the priority, made when forced
        bot.tally_payload = lambda *a: built.append("tally") or {"ready": True}
        bot.journal_csv = lambda flt=None: built.append("csv") or "id\n"
        bot.journal_summary_csv = lambda flt=None: built.append("scsv") or "t\n"
        for name, query, made in (("journal.csv", "", "csv"), ("summary.csv", "", "scsv"), ("samples.csv", "", "samples"),
                                  ("journal.json", "tally=HSI", "tally"), ("journal.json", "full=1", "full"),
                                  ("journal.json", "analysis=1&card=x", None), ("journal.json", "cards=1&range=7d", None)):
            mark = len(built)
            status, _, body = get(name, query)
            assert status == 503 and json.loads(body)["busy"] and len(built) == mark, (name, query, status)
            status, _, body = forced(lambda: get(name, query))
            assert status == 200, (name, query, status, body[:200])
            assert made is None or built[mark:] == [made], (name, query, built[mark:])
        # a bot that cannot say how long the taker's seconds last keeps nothing waiting; one without them, likewise
        bot.rules_quiet = lambda: 1 / 0
        assert web.force_wait() == 0.0 and forced(web.busy) is False
        del bot.rules_quiet
        assert web.force_wait() == 0.0
        # outside the priority nothing changes: no line in the log for a forced build then
        state["clear"] = True
        logged.lines.clear()
        web.built["journal.json"] -= 20.0
        assert forced(lambda: get("journal.json"))[0] == 200 and not any("强制刷新" in line for line in logged.lines), logged.lines
        # the cache's record of when each body was built goes with the bodies (the filtered analyses: one a filter)
        web.cache = {f"x{i}": (0.0, b"") for i in range(70)}
        web.built.update({f"x{i}": 0.0 for i in range(70)})
        web.cached("journal.json?analysis&card=y", lambda: b"{}")
        assert set(web.built) <= set(web.cache) | {"journal.json?analysis&card=y"} and len(web.built) <= len(web.cache) + 1, (len(web.built), len(web.cache))
    finally:
        m.LOG.removeHandler(logged)
    print("server_rules ok")


# --- over HTTP: the keyed review data, and force on a route that keys nothing -----------------------------------------------------
async def http(port: int, path: str, key: str | None = None) -> tuple[int, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    head = f"GET {path} HTTP/1.1\r\nHost: x\r\n" + (f"X-Control-Key: {key}\r\n" if key is not None else "") + "\r\n"
    writer.write(head.encode("latin-1"))
    await writer.drain()
    data = await reader.read()
    writer.close()
    return int(data.split(b" ", 2)[1]), data.split(b"\r\n\r\n", 1)[1]


async def over_http() -> None:
    bot, state, built = make_bot()
    web = m.WebServer(bot, 0, TOKEN)
    port = await web.start()
    try:
        p = f"/p/{TOKEN}"
        state["clear"] = False
        assert (await http(port, p + "/journal.json?force=1"))[0] == 403 and built == [], "no key: nothing, forced or not"
        assert (await http(port, p + "/journal.json?force=1", "wrong-key-123"))[0] == 403 and built == []
        web.control_failures.clear()
        status, body = await http(port, p + "/journal.json", KEY)
        assert status == 503 and json.loads(body)["busy"] and built == []
        status, body = await http(port, p + "/journal.json?force=1", KEY)
        assert status == 200 and built == ["list"] and m.WEB_FORCE.get() is False, (status, built)
        assert (await http(port, p + "/journal.csv", KEY))[0] == 503
        assert (await http(port, p + "/journal.csv?force=1", KEY))[0] == 200
        # samples.csv keys nothing: force=1 counts with the control key alone — a wrong one is counted as any other
        mark = len(built)
        assert (await http(port, p + "/samples.csv?force=1"))[0] == 503 and len(built) == mark, "no key: not forced"
        assert (await http(port, p + "/samples.csv?force=1", "wrong-key-123"))[0] == 503 and len(built) == mark and web.control_failures, "a wrong key: not forced, counted"
        web.control_failures.clear()
        status, body = await http(port, p + "/samples.csv?force=1", KEY)
        assert status == 200 and built[mark:] == ["samples"], (status, built[mark:])
        assert (await http(port, p + "/samples.csv", KEY))[0] == 503, "the key alone forces nothing"
        status, body = await http(port, p + "/data.json?force=1", KEY)
        assert status == 200, "a light body: as ever"
    finally:
        await web.stop()
    # a paper bot with no control key: there is no account behind the data — the page link alone forces
    paper, pstate, pbuilt = make_bot(key="", real=False)
    pweb = m.WebServer(paper, 0, TOKEN)
    port = await pweb.start()
    try:
        pstate["clear"] = False
        assert (await http(port, f"/p/{TOKEN}/samples.csv"))[0] == 503
        assert (await http(port, f"/p/{TOKEN}/samples.csv?force=1"))[0] == 200 and pbuilt == ["samples"]
        assert (await http(port, f"/p/{TOKEN}/journal.json?force=1"))[0] == 200 and pbuilt == ["samples", "list"]
    finally:
        await pweb.stop()
    # a real account with no control key configured: its data is refused anyway, and nothing is forced on the open routes
    nokey, nstate, nbuilt = make_bot(key="", real=True)
    nweb = m.WebServer(nokey, 0, TOKEN)
    port = await nweb.start()
    try:
        nstate["clear"] = False
        assert (await http(port, f"/p/{TOKEN}/samples.csv?force=1"))[0] == 503 and nbuilt == []
        assert (await http(port, f"/p/{TOKEN}/journal.json?force=1"))[0] == 403 and nbuilt == []
    finally:
        await nweb.stop()
    print("over_http ok")


# --- Telegram: … 强制 -------------------------------------------------------------------------------------------------------------
async def telegram() -> None:
    bot, state, built = make_bot()
    logged = Logged()
    m.LOG.addHandler(logged)
    calls: list = []

    async def export_json(words):
        calls.append(("json", list(words)))
        return "JSON"

    async def export_preopen(words):
        calls.append(("preopen", list(words)))
        return "PREOPEN"
    bot.export_json, bot.export_preopen = export_json, export_preopen
    try:
        state["clear"] = False
        # not forced: the answer says how to force it, with the very command
        told = await bot.cmd_export(req("/export", "json", "上证"))
        assert isinstance(told, str) and "规则吃单正在优先时段" in told and told.endswith("例如 /export json 上证 强制") and calls == [], told
        told = await bot.cmd_export(req("/export", "预挂"))
        assert told == bot.export_busy("/export", ["预挂"]) and told.endswith("/export 预挂 强制"), told
        # forced: made now, the word not taken for a filter or a range
        assert await bot.cmd_export(req("/export", "json", "上证", "强制")) == "JSON" and calls == [("json", ["上证"])], calls
        assert await bot.cmd_export(req("/export", "强制", "预挂", "7d")) == "PREOPEN" and calls[-1] == ("preopen", ["7d"]), calls
        assert await bot.cmd_export(req("/export", "预挂", "FORCE")) == "PREOPEN", "force, in either case"
        assert any("强制导出：规则吃单的优先时段里做 /export json 上证" in line for line in logged.lines), logged.lines
        assert (await bot.cmd_export(req("/export", "强制"))).startswith("📦 复盘数据导出"), "nothing named: what there is"
        assert "最后加“强制”马上做" in bot.export_menu()
        # /journal, /summary never wait: the word is no card's name there
        assert bot.journal_filter_words(["强制"]) == ({}, "") and bot.journal_filter_words(["挂", "强制"]) == ({"way": "maker"}, "")
        assert await bot.cmd_export(req("/export", "日记", "强制")) == "还没有交易记录"
        # /samples
        rows = [{"t": NOW - 1000, "card": "BTC-15M", "kind": "window", "slug": "btc-1", "start": NOW - 60_000, "end": NOW + 840_000, "left": 840,
                 "up": 0.55, "mkt": 0.5, "result": None}]
        bot.window_rows = lambda: list(rows)
        told = await bot.cmd_samples(None)
        assert isinstance(told, str) and "样本导出这段时间不做" in told and "/samples 强制" in told, told
        assert isinstance(await bot.cmd_samples(req("/samples", "强制")), m.Document)
        assert isinstance(await bot.cmd_export(req("/export", "样本", "强制")), m.Document), "told on by /export"
        assert isinstance(await bot.cmd_export(req("/export", "样本")), str)
        # /tally: the command's own wait and the one between two cards
        seen: list = []

        async def tally_report(cards, d0, d1, forced=False):
            seen.append(forced)
            return ["TALLY"]
        bot.tally_cards_named = lambda rest: (["card"], "") if not rest else ([], f"没有“{' '.join(rest)}”")
        real_report, bot.tally_report = bot.tally_report, tally_report
        told = await bot.cmd_tally(req("/tally"))
        assert told == bot.export_busy("/tally", []) and told.endswith("/tally 强制") and seen == [], told
        assert await bot.cmd_tally(req("/tally", "强制")) == ["TALLY"] and seen == [True]
        assert await bot.cmd_tally(req("/tally", "7d", "强制")) == ["TALLY"] and seen == [True, True], "the word is no card's name"
        assert await bot.cmd_export(req("/export", "涨跌", "强制")) == ["TALLY"] and seen[-1] is True
        # /export 全部 强制: every file made now, the samples and the tally told so — each file after the taker's own seconds
        bot.export_rules = lambda: "RULES"

        async def calib_files():
            return "CALIB"
        bot.calib_files = calib_files
        quiets: list = []
        plain_quiet = bot.force_quiet

        async def force_quiet(what, say=True):
            quiets.append((what, say))
            await plain_quiet(what, say)
        bot.force_quiet = force_quiet
        assert await bot.cmd_export(req("/export", "全部")) == bot.export_busy("/export", ["全部"]) and quiets == []
        out = await bot.cmd_export(req("/export", "全部", "强制"))
        assert isinstance(out[0], m.Document) and out[0].filename.startswith("model-samples-") and "已发 1 个文件" in out[1] and out[-1] == "TALLY", out
        assert quiets[0] == ("/export 全部", True) and quiets.count(("/export 全部", False)) == 6, quiets
        assert ("/samples", True) in quiets and ("/tally", True) in quiets and seen[-1] is True, (quiets, seen)
        del bot.force_quiet
        state["clear"] = True
        assert await bot.cmd_tally(req("/tally")) == ["TALLY"] and seen[-1] is False
        state["clear"] = False
        bot.tally_report = real_report
        naps: list[float] = []
        real_sleep = asyncio.sleep

        async def nap(seconds, *a):
            naps.append(seconds)
            await real_sleep(0)
        ready: list = []

        async def tally_ready(key, frm, to, ledger, raw):
            ready.append(key)
            return {"ready": False}, 0
        bot.tally_ready = tally_ready
        bot.preopen_rows = lambda: []
        card = dataclasses.make_dataclass("Card", ["key", "coin", "minutes"])("BTC-15M", "BTC", 15)
        m.asyncio.sleep = nap
        try:
            import datetime as dt
            d = dt.date(2026, 11, 26)
            out = await bot.tally_report([card, card], d, d, True)
            assert ready == ["BTC-15M", "BTC-15M"] and 5.0 not in naps, (ready, naps)  # forced: no card held for the priority
            ready.clear()
            naps.clear()
            bot.TALLY_HOLD_S = 10.0
            out = await bot.tally_report([card], d, d)
            assert ready == [] and naps == [5.0, 5.0] and "没读到" in out[0], (ready, naps, out)  # not forced: held, then given up
        finally:
            m.asyncio.sleep = real_sleep
        # force_quiet: the taker's own seconds waited out — FORCE_QUIET_MAX_S at the most
        left = [3.0, 0.0]
        bot.rules_quiet = lambda: left.pop(0) if left else 0.0
        naps.clear()
        m.asyncio.sleep = nap
        try:
            await bot.force_quiet("/export json")
            assert naps == [3.05], naps
            bot.rules_quiet = lambda: 30.0
            naps.clear()
            await bot.force_quiet("/export json")
            assert abs(sum(naps) - bot.FORCE_QUIET_MAX_S) < 1e-9 and max(naps) <= 5.0, naps
            bot.rules_quiet = lambda: 1 / 0
            naps.clear()
            await bot.force_quiet("x")
            assert naps == []
            del bot.rules_quiet  # (a bot without that taker: nothing to wait for)
            await bot.force_quiet("x")
            assert naps == []
        finally:
            m.asyncio.sleep = real_sleep
        assert bot.take_force(None) == (False, []) and bot.take_force([" 强制 ", "", "json"]) == (True, ["json"])
    finally:
        m.LOG.removeHandler(logged)
    print("telegram ok")


# --- the page's script ---------------------------------------------------------------------------------------------------------------
def page_script() -> None:
    page = m.JOURNAL_PAGE
    for want in ('b.id="forcebtn"', 'b.textContent="强制刷新"', "forceUntil=Date.now()+60000", 'u+=(u.indexOf("?")>=0?"&":"?")+"force=1"',
                 '["csv","scsv","json","mcsv"]', "if(j.busy)e.busy=true", "load.keyBad=false"):
        assert want in page, want
    load = page[page.index("async function load(){"):page.index('["csv","scsv","json","mcsv"]')]
    assert load.count("showForce()") == 2, "the button with the line, whether the read worked or not"
    node = shutil.which("node") or ("/opt/node22/bin/node" if os.path.exists("/opt/node22/bin/node") else "")
    if not node:
        print("page_script: node not found, script run skipped")
        return
    with tempfile.TemporaryDirectory() as tmp:
        js = pathlib.Path(tmp, "page.js")
        js.write_text("\n;\n".join(re.findall(r"<script>(.*?)</script>", page, re.S)), encoding="utf-8")
        assert subprocess.run([node, "--check", str(js)], capture_output=True).returncode == 0, "the review page's script does not parse"
        # the wrapper alone: for the minute after the button, the page's own reads carry force=1 — once, nothing else's
        snippet = page[page.index("let forceUntil=0"):page.index("function showForce(")]
        run = pathlib.Path(tmp, "wrap.js")
        run.write_text("""const seen=[];globalThis.window={fetch:(u,o)=>{seen.push(u);return Promise.resolve(null)}};const base="/p/T";
""" + snippet + """
(async()=>{await window.fetch(base+"/journal.json");forceUntil=Date.now()+60000;
for(const u of [base+"/journal.json",base+"/journal.json?all=1","http://h/p/T/journal.csv",base+"/journal.json?full=1&force=1","https://elsewhere/x"])await window.fetch(u,{});
forceUntil=Date.now()-1;await window.fetch(base+"/journal.json");console.log(JSON.stringify(seen))})();""", encoding="utf-8")
        out = subprocess.run([node, str(run)], capture_output=True, text=True, timeout=60)
        assert out.returncode == 0, out.stderr[-2000:]
        seen = json.loads(out.stdout.strip().splitlines()[-1])
        assert seen == ["/p/T/journal.json", "/p/T/journal.json?force=1", "/p/T/journal.json?all=1&force=1", "http://h/p/T/journal.csv?force=1",
                        "/p/T/journal.json?full=1&force=1", "https://elsewhere/x", "/p/T/journal.json"], seen
    print("page_script ok")


# --- in a browser, where Node and its Playwright are installed ------------------------------------------------------------------------
CHECK = r"""
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 390, height: 900 }, acceptDownloads: true });
  const errors = [], alerts = [];
  page.on('pageerror', e => errors.push(String(e)));
  page.on('dialog', async d => { alerts.push(d.message()); await d.dismiss(); });
  await page.addInitScript(k => sessionStorage.setItem('ctlkey', k), process.argv[3]);
  const tell = async what => { await fetch(process.argv[4] + '/' + what); };
  await page.goto(process.argv[2]);
  // the rule taker goes first and nothing was built yet: the 503's words, and the button beside them
  await page.waitForSelector('#meta .bad');
  await page.waitForSelector('#forcebtn');
  const busy = await page.textContent('#meta');
  // an export in that time: told why, and where the button is
  await page.evaluate(() => { document.getElementById('exports').open = true; });
  await page.click('#csv');
  for (let i = 0; i < 100 && !alerts.length; i++) await page.waitForTimeout(100);
  const firstAlert = alerts.slice();
  // the button: the list is read with force=1 and built now
  await page.click('#forcebtn');
  await page.waitForFunction(() => /^数据 /.test(document.querySelector('#meta span').textContent), null, { timeout: 15000 });
  await page.waitForSelector('#forcebtn:not([disabled])');
  const after = await page.textContent('#meta');
  // ...and within the minute an export is made, not refused
  await page.evaluate(() => { document.getElementById('exports').open = true; });
  const [download] = await Promise.all([page.waitForEvent('download', { timeout: 15000 }), page.click('#csv')]);
  const saved = download.suggestedFilename();
  // the taker's own seconds: even forced, said how long they last
  await tell('quiet');
  await page.click('#forcebtn');
  await page.waitForFunction(() => /还剩约/.test(document.getElementById('meta').textContent), null, { timeout: 15000 });
  const quiet = await page.textContent('#meta');
  console.log(JSON.stringify({ busy, firstAlert, after, saved, quiet, alerts, errors }));
  await browser.close();
})().catch(e => { console.log(JSON.stringify({ failed: e.message })); process.exit(2); });
"""


async def browser_check() -> None:
    probe = subprocess.run(["node", "-e", "require('playwright')"], capture_output=True) if shutil.which("node") else None
    if probe is None or probe.returncode != 0:
        print("browser check skipped (no Node Playwright)")
        return
    bot, state, built = make_bot()
    bot.journal_csv = lambda flt=None: built.append("csv") or "id\n"
    web = m.WebServer(bot, 0, TOKEN)
    state["clear"] = False
    port = await web.start()
    asked: list[str] = []
    plain_keyed = web.journal_keyed

    def keyed(path, query, head, client):
        asked.append(path.rsplit("/", 1)[-1] + ("?" + query if query else ""))
        return plain_keyed(path, query, head, client)
    web.journal_keyed = keyed

    async def control(reader, writer):  # the test's own lever: the taker's seconds begin
        await reader.readuntil(b"\r\n\r\n")
        state["quiet"] = 9.0
        web.built.clear()
        web.cache.clear()
        writer.write(b"HTTP/1.1 200 OK\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        await writer.drain()
        writer.close()
    lever = await asyncio.start_server(control, "127.0.0.1", 0)
    lever_port = lever.sockets[0].getsockname()[1]
    try:
        with tempfile.TemporaryDirectory() as tmp:
            script = pathlib.Path(tmp, "check.js")
            script.write_text(CHECK, encoding="utf-8")
            proc = await asyncio.create_subprocess_exec("node", str(script), f"http://127.0.0.1:{port}/p/{TOKEN}/journal", KEY, f"http://127.0.0.1:{lever_port}",
                                                        stdout=asyncio.subprocess.PIPE, env={**os.environ})
            out, _ = await asyncio.wait_for(proc.communicate(), 120)
    finally:
        lever.close()
        await web.stop()
    result = json.loads(out.decode().strip().splitlines()[-1])
    assert "failed" not in result and not result["errors"], result
    assert "规则吃单正在优先时段" in result["busy"] and "强制刷新" in result["busy"], result
    assert len(result["firstAlert"]) == 1 and result["firstAlert"][0].startswith("导出失败：规则吃单正在优先时段") and "最上面的“强制刷新”" in result["firstAlert"][0], result
    assert result["after"].startswith("数据 ") and result["after"].endswith("强制刷新"), result
    assert result["saved"] == "交易日记.csv" and "csv" in built, (result, built)
    assert "还剩约 9 秒" in result["quiet"] and "强制刷新" in result["quiet"], result
    assert "journal.json" in asked and "journal.json?force=1" in asked and "journal.csv" in asked and "journal.csv?force=1" in asked, asked
    print("browser ok")


server_rules()
asyncio.run(over_http())
asyncio.run(telegram())
page_script()
asyncio.run(browser_check())
print("FORCE_REFRESH_OK")
