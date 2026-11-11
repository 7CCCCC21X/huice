"""Audit regressions of the control cluster (the /control page, WEB_CONTROL_KEY and the data it guards)."""
import asyncio, json, sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L
WEI = 10 ** 18
TOKEN = "t" * 20
NOW = 1_790_000_000_000
base = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080"}
REAL = {"v": 2, "market": "x", "slug": "x", "item": "x", "kind": "close", "key": "X", "side": "up", "label": "吃涨", "maker": False,
        "fair": 0.7, "opened": NOW, "settle": {}, "order": 10.0, "fills": [], "revisions": [], "price": 0.5, "shares": 10.0,
        "status": "filled", "edge": 0.2, "live": {"state": "done", "order_id": "999", "events": []}}


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


class FakeApi:
    def __init__(self):
        self.jwt, self.jwt_exp, self.auth_error, self.lookup = "t", 0, "", ""
        self.open, self.closed, self.calls, self.positions_rows, self.markets = {}, {}, [], [], {}
    async def ensure_auth(self): pass
    async def market(self, mid): return self.markets[str(mid)]
    async def remove_orders(self, ids): return {"removed": [], "noop": list(ids)}
    async def orders(self, status="OPEN", **params): return []
    async def order(self, oid, hash_=""): return None
    async def positions(self): return []


class FakeChain:
    async def usdt_balance(self, owner=None): return 500 * WEI
    async def bnb_balance(self, owner=None): return 10 ** 17
    async def allowance(self, key, owner=None): return 10 ** 30


async def request(port, raw):
    r, w = await asyncio.open_connection("127.0.0.1", port)
    w.write(raw); await w.drain()
    data = await r.read(); w.close()
    head, _, body = data.partition(b"\r\n\r\n")
    return int(head.split(b" ")[1]), body


async def get(port, name):
    return await request(port, f"GET /p/{TOKEN}/{name} HTTP/1.1\r\nHost: x\r\n\r\n".encode())


async def ctrl_no_control_key():
    # CTRL-NO-CONTROL-KEY: a live key (or real orders in the records) with WEB_CONTROL_KEY unset served the account, its
    # orders, positions and every trade to anyone holding the page link. Now it fails closed with a hint naming the key.
    store = m.Store(":memory:")
    bot = L.LiveBot(m.Config.from_env({**base, "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "PREDICT_API_KEY": "k"}), store, FM(NOW), None)
    bot.live.api, bot.live.chain = FakeApi(), FakeChain()
    await bot.live_prepare()
    store.put("sim:x|up|吃", dict(REAL))
    assert not bot.config.web_control_key and bot.config.live_key
    account = str((bot.control_payload().get("live") or {}).get("account") or "")
    web = m.WebServer(bot, 0, TOKEN); web.CACHE_SECONDS = {}; port = await web.start()
    try:
        assert web.journal_locked()
        for name in ("control.json", "journal.json", "journal.json?full=1", "journal.csv", "summary.csv", "why.json"):
            st, body = await get(port, name)
            assert st == 403, (name, st, body[:200])
            j = json.loads(body)
            assert j["locked"] and "WEB_CONTROL_KEY" in j["message"], (name, j)
            assert not account or account.encode() not in body, name
        assert not web.control_failures and not web.control_lock_until, "no key configured: nothing guessed wrong, nothing counted"
        st, body = await request(port, f"POST /p/{TOKEN}/control HTTP/1.1\r\nHost: x\r\nContent-Length: 18\r\n\r\n".encode() + b'{"action":"pause"}')
        assert st == 403 and "WEB_CONTROL_KEY" in json.loads(body)["message"]  # actions stay refused
    finally:
        await web.stop()

    # a paper bot with no key and no real records keeps the documented view-only console
    pstore = m.Store(":memory:")
    paper = m.Bot(m.Config.from_env(base), pstore, FM(NOW), None)
    web2 = m.WebServer(paper, 0, TOKEN); web2.CACHE_SECONDS = {}; port2 = await web2.start()
    try:
        assert not web2.journal_locked()
        st, body = await get(port2, "control.json")
        assert st == 200 and not json.loads(body)["enabled"]
        st, _ = await get(port2, "journal.json")
        assert st == 200
        # ... until its records hold a real order (a database kept after the key was removed): then the review data locks
        pstore.put("sim:x|up|吃", dict(REAL))
        assert paper.sim_has_real() and web2.journal_locked()
        for name in ("journal.json", "journal.csv", "control.json"):
            st, body = await get(port2, name)
            assert st == 403 and json.loads(body)["locked"] and "WEB_CONTROL_KEY" in json.loads(body)["message"], (name, st, body[:200])
    finally:
        await web2.stop()


asyncio.run(ctrl_no_control_key())


def ctrl_ladder_hot():
    # CTRL-LADDER-HOT: on price/market-cap ladder cards the action cell's red mark compared the edge with the bare global bar
    # (HOT) while the card frame, the 机会条 and the opened chips use the most specific bar set (level/card/section/global).
    # With any override the two disagreed (red cell on an unframed card, or a framed card whose cell is not red).
    src = open(m.__file__, encoding="utf-8").read()
    start = src.index("function priceLadderBook(")
    body = src[start:src.index("\nfunction ", start + 10)]
    act = body[body.index("const action=maker=>"):]
    act = act[:act.index("const line=")]
    assert 'e.edge>=HOT)a.classList.add("hot")' not in body, "ladder cell still uses the global bar"
    assert 'if(e.edge>=hotFor(it,r.label))a.classList.add("hot")' in act, "ladder cell must use the per-level bar"
    # the resolver it now shares with the frame: a 20c level bar over a 10c global bar keeps a 15c edge un-red
    import shutil, subprocess
    node = shutil.which("node")
    if node:
        i = src.index("function hotCentsFor(")
        res = src[i:src.index("function setHot(", i)]
        js = ("const favKey=it=>it.key,hotCents=10,hotMap={'row:A#L1':20,'card:B':5};" + res +
              "const a={key:'A',group:'levels'},b={key:'B',group:'levels'};"
              "console.log(JSON.stringify([0.15>=hotFor(a,'L1'),0.15>=hotFor(a,'L2'),0.07>=hotFor(b,'L1')]))")
        out = subprocess.run([node, "-e", js], capture_output=True, text=True, check=True).stdout.strip()
        assert json.loads(out) == [False, True, True], out


ctrl_ladder_hot()


def ctrl_mcsv():
    # CTRL-MCSV: the review page rewrote csv/scsv/json to base+"/..." so they also work when the page is opened as
    # /p/<t>/journal/ (trailing slash, which route serves too), but left 导出模型样本 (mcsv) relative: from there the browser
    # resolved it to /p/<t>/journal/samples.csv, a 404.
    import re, urllib.parse
    src = m.JOURNAL_PAGE
    assert 'getElementById("mcsv").href=base+"/samples.csv"' in src, "mcsv link must be rewritten like csv/scsv/json"
    # what the page's script computes, for both forms of the journal URL, lands on the served samples.csv
    class B:
        def window_csv(self): return "a,b\n1,2\n"
    ws = m.WebServer.__new__(m.WebServer)
    ws.token, ws.bot, ws.pages = TOKEN, B(), {"page": b"p", "journal": b"j", "control": b"c"}
    for page in ("/p/" + TOKEN + "/journal", "/p/" + TOKEN + "/journal/"):
        assert ws.route("GET", page)[0] == 200
        base_ = re.sub(r"/journal/?$", "", page)
        href = urllib.parse.urlsplit(urllib.parse.urljoin("http://h" + page, base_ + "/samples.csv")).path
        status, ctype, body = ws.route("GET", href)
        assert status == 200 and ctype.startswith("text/csv") and body == b"a,b\n1,2\n", (page, href, status)
    # the relative href alone 404s from the trailing-slash page: that is why the script must rewrite it
    rel = urllib.parse.urlsplit(urllib.parse.urljoin("http://h/p/" + TOKEN + "/journal/", "samples.csv")).path
    assert ws.route("GET", rel)[0] == 404


ctrl_mcsv()

async def ctrl_report_token():
    # CTRL-REPORT-TOKEN: the daily report put 复盘页：<web_base>/p/<token>/journal into every subscription, group topics
    # included, though the token is private (/web: 请勿转发) and, with WEB_CONTROL_KEY unset, opens control.json and the
    # journal. Now only the admin's private chat gets the link; groups get the same report without it.
    import datetime as dt
    now = int(dt.datetime(2026, 11, 10, 17, 0, tzinfo=m.BEIJING).timestamp() * 1000)
    cfg = m.Config.from_env({**base, "ADMIN_USER_ID": "42", "WEB_BASE_URL": "https://x.example"})
    assert cfg.sim and cfg.predict and cfg.sim_daily_report and not cfg.web_control_key
    bot = m.Bot(cfg, m.Store(":memory:"), FM(now), None)
    bot.web_token = "SECRETTOKEN1234567890"
    bot.sim_trades = lambda: {"a": {"status": "settled", "opened": now - 3_600_000, "settled": now - 60_000, "price": .4,
                                    "shares": 10, "payout": 1, "edge": .1, "maker": True, "item": "BTC"}}
    sent = {}

    async def tell(chat, thread, text, **kw):
        sent[(chat, thread)] = text
        return True
    bot.tell = tell
    bot.store.put("subscriptions", {"-100123:7": {"chat": -100123, "thread": 7, "active": True},
                                    "42:0": {"chat": 42, "thread": 0, "active": True}})
    await bot.daily_report(now)
    await bot.drain_deliveries()
    group, admin = sent[(-100123, 7)], sent[(42, 0)]
    assert bot.web_token not in group and "复盘页" not in group, group
    assert f"复盘页：https://x.example/p/{bot.web_token}/journal" in admin, admin
    for text in (group, admin):
        assert "交易日报" in text and "今天结算 1 笔" in text, text
    # the group copy ends its tree cleanly (the link was the last item, so the previous line now carries the corner)
    assert group.splitlines()[-1].lstrip().startswith("└"), group
    # every subscription is recorded as delivered for the day
    day = "simreport:2026-11-10"
    assert bot.store.get(f"{day}:-100123:7") and bot.store.get(f"{day}:42:0")


asyncio.run(ctrl_report_token())
async def ctrl_report_midnight():
    # CTRL-REPORT-MIDNIGHT: with a late SIM_DAILY_REPORT (23:30) the "sent when back, up to REPORT_WINDOW_MS later"
    # catch-up stopped at Beijing midnight: at 00:10 the due time was recomputed as tonight's 23:30, so yesterday's
    # report was never sent nor recorded. Now the latest passed due time (yesterday's 23:30) counts, and the report,
    # its text and its record are for that day.
    import datetime as dt
    BJ = lambda d, h, mi: int(dt.datetime(2026, 11, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
    bot = m.Bot(m.Config.from_env({**base, "SIM_DAILY_REPORT": "23:30"}), m.Store(":memory:"), FM(BJ(11, 0, 10)), None)
    assert bot.config.sim and bot.config.predict
    bot.store.put("subscriptions", {"1:0": {"chat": 1, "thread": 0, "active": True}})
    yesterday = BJ(10, 22, 0)  # one record opened and settled on 11-10, nothing on 11-11
    bot.sim_trades = lambda: {"a": {"status": "settled", "opened": yesterday, "settled": yesterday + 60_000, "price": .4,
                                    "shares": 10, "payout": 1, "edge": .1, "maker": True, "item": "BTC"}}
    sent = []

    async def tell(chat, thread, text, **kw):
        sent.append(text)
        return True
    bot.tell = tell
    await bot.daily_report(BJ(11, 0, 10))  # back 40 minutes after 23:30, past midnight
    await bot.drain_deliveries()
    assert len(sent) == 1, sent
    assert bot.store.get("simreport:2026-11-10:1:0") and not bot.store.keys("simreport:2026-11-11"), bot.store.keys("simreport:")
    assert "11-10" in sent[0] and "今天结算 1 笔" in sent[0] and "今天下单 1 笔" in sent[0], sent[0]
    await bot.daily_report(BJ(11, 0, 20)); await bot.drain_deliveries()
    assert len(sent) == 1  # once a day
    # more than six hours after yesterday's 23:30 (and before tonight's): nothing is sent or recorded
    late = m.Bot(bot.config, m.Store(":memory:"), FM(BJ(11, 6, 0)), None)
    late.store.put("subscriptions", {"1:0": {"chat": 1, "thread": 0, "active": True}})
    late.sim_trades, late.tell = bot.sim_trades, tell
    await late.daily_report(BJ(11, 6, 0)); await late.drain_deliveries()
    assert len(sent) == 1 and not late.store.keys("simreport:")
    # the same-day path is unchanged: at 23:40 on 11-10 the 11-10 report goes
    same = m.Bot(bot.config, m.Store(":memory:"), FM(BJ(10, 23, 40)), None)
    same.store.put("subscriptions", {"1:0": {"chat": 1, "thread": 0, "active": True}})
    same.sim_trades, same.tell = bot.sim_trades, tell
    await same.daily_report(BJ(10, 23, 40)); await same.drain_deliveries()
    assert len(sent) == 2 and same.store.get("simreport:2026-11-10:1:0") and "11-10" in sent[1]


asyncio.run(ctrl_report_midnight())


def ctrl_want_info():
    """CTRL-WANT-INFO: a rolled card's old slug leaves predict.want_info (it is never a target again); the set does not grow
    by one slug per roll for ever."""
    import datetime as dt
    env = {**base, "SYMBOLS": "BTCUSDT", "SIM_MARKETS": "all"}
    t0 = int(dt.datetime(2026, 11, 4, 0, 0, tzinfo=m.BEIJING).timestamp() * 1000)
    bot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(t0), None)
    assert bot.updown_rolling, "no rolling cards"
    start = len(bot.predict.want_info)
    key = next(k for k, d in bot.updown_rolling.items() if isinstance(d, m.WindowUpDownSpec))
    mkt, daily = bot.updowns[key], bot.updown_rolling[key]
    old = mkt.spec.slug
    assert old in bot.predict.want_info
    bot.updown_roll(mkt.spec.end_ms + 60_000 + daily.ROLL_WAIT_MS + 1)
    assert mkt.spec.slug != old, "the card did not roll"
    assert mkt.spec.slug in bot.predict.want_info and old not in bot.predict.want_info
    now = t0
    for _ in range(24 * 60):  # a simulated day, a roll check every minute
        now += 60_000
        bot.updown_roll(now)
    assert len(bot.predict.want_info) <= start + len(bot.updown_rolling), (len(bot.predict.want_info), start)
    assert {mk.spec.slug for mk in bot.updowns.values()} <= bot.predict.want_info


ctrl_want_info()
def ctrl_edge_level():
    """CTRL-EDGE-LEVEL: /edge saves a ladder level in the card's own spelling (4500 -> 4.5k, as level_label shows it), so the
    confirmed bar is the one edge_bar applies; a market without levels refuses one (it could never match a card); a level
    the market does not list is refused once its levels are known."""
    bot = m.Bot(m.Config.from_env(base), m.Store(":memory:"), FM(NOW), None)
    D = m.D
    assert bot.edge_target(["ETH", "11月", "4500"])[0] == "level:ETH-HIT-11|4.5k"
    assert bot.edge_target(["ETH", "11月", "4.5K"])[0] == "level:ETH-HIT-11|4.5k"
    reply = bot.cmd_edge(m.Request("/edge", ["ETH", "11月", "4500", "3"], 1, 0, 1))
    assert reply.startswith("✅"), reply
    book = m.PredictBook("ETH-HIT-11", "s", "1", "t", ((D("0.4"), D("10")),), ((D("0.5"), D("10")),), NOW)
    mk = m.SimMarket("s#1", "ETH 11月价格 ↑ " + m.level_label(4500), "range", "ETH-HIT-11", 0.6, book, 0.02, "", ("Yes", "No"), {})
    assert abs(bot.edge_bar(mk) - 0.03) < 1e-12, (bot.edge_bars()[0], bot.edge_bar(mk))
    before = dict(bot.edge_bars()[0])
    try:
        bot.cmd_edge(m.Request("/edge", ["恒指", "300m", "4"], 1, 0, 1)); assert False, "a level on a market without levels"
    except ValueError as e:
        assert "没有档位" in str(e), e
    assert bot.edge_bars()[0] == before
    bot.predict.ladders["ETH-HIT-11"] = [m.LadderRow(D(t), str(i), f"${t}", None) for i, t in enumerate((4000, 4500, 5000))]
    assert bot.edge_target(["ETH", "11月", "5000"])[0] == "level:ETH-HIT-11|5k"
    try:
        bot.edge_target(["ETH", "11月", "4600"]); assert False, "a level the market does not list"
    except ValueError as e:
        assert "4.5k" in str(e), e


ctrl_edge_level()


def ctrl_edge_level_stale():
    """CTRL-EDGE-LEVEL (follow-up): the bars the bug saved before the fix (the level as typed: "…|4500", a level on a
    market without levels) are still in the settings and listed by /edge; "/edge … off" takes each away on its own (it
    was refused, or answered 无需取消, and only /edge 清空 removed them, with every valid bar), and setting the level again
    replaces the dead bar. A bar of a level the market has dropped since can be taken away too."""
    bot = m.Bot(m.Config.from_env(base), m.Store(":memory:"), FM(NOW), None)
    D = m.D
    ask = lambda *args: bot.cmd_edge(m.Request("/edge", list(args), 1, 0, 1))
    old = {"level:HSI|300m": 4.0, "level:ETH-HIT-11|4500": 3.0, "key:HSI": 5.0}
    old_names = {"level:HSI|300m": "恒生指数 300m", "level:ETH-HIT-11|4500": "ETH 11月价格 4500", "key:HSI": "恒生指数"}
    bot.update_settings(edge_bars=dict(old), edge_names=dict(old_names))
    reply = ask("恒指", "300m", "off")
    assert reply.startswith("✅ 已取消") and "300m" not in reply.split("用法")[0].split("\n", 1)[1], reply
    assert bot.edge_bars()[0] == {"level:ETH-HIT-11|4500": 3.0, "key:HSI": 5.0}, bot.edge_bars()
    reply = ask("ETH", "11月", "4500", "off")
    assert reply.startswith("✅ 已取消"), reply
    assert bot.edge_bars() == ({"key:HSI": 5.0}, {"key:HSI": "恒生指数"}), bot.edge_bars()
    assert ask("ETH", "11月", "4500", "off").startswith("ℹ️")  # nothing left under either spelling
    try:
        ask("恒指", "300m", "off"); assert False, "no such bar: a level on a market without levels is still refused"
    except ValueError as e:
        assert "没有档位" in str(e), e
    # setting the level again replaces the dead bar: one bar, in the card's spelling
    bot.update_settings(edge_bars=dict(old), edge_names=dict(old_names))
    assert ask("ETH", "11月", "4500", "2").startswith("✅")
    assert bot.edge_bars()[0] == {"level:HSI|300m": 4.0, "level:ETH-HIT-11|4.5k": 2.0, "key:HSI": 5.0}, bot.edge_bars()
    assert "level:ETH-HIT-11|4500" not in bot.edge_bars()[1]
    try:
        ask("恒指", "300m", "4"); assert False, "setting a level on a market without levels stays refused"
    except ValueError as e:
        assert "没有档位" in str(e) and bot.edge_bars()[0]["level:HSI|300m"] == 4.0, e
    # the market drops the level (its rows no longer list 4500): the bar can still be taken away, typed either way
    bot.predict.ladders["ETH-HIT-11"] = [m.LadderRow(D(t), str(i), f"${t}", None) for i, t in enumerate((4000, 5000))]
    assert ask("ETH", "11月", "4500", "off").startswith("✅") and "level:ETH-HIT-11|4.5k" not in bot.edge_bars()[0]
    try:
        ask("ETH", "11月", "4500", "3"); assert False, "a level the market does not list cannot be set"
    except ValueError as e:
        assert "现有档位" in str(e), e
    assert bot.edge_bars()[0] == {"level:HSI|300m": 4.0, "key:HSI": 5.0}, bot.edge_bars()


ctrl_edge_level_stale()


def ctrl_prune():
    """CTRL-PRUNE: control_prune guessed the saved value to blame from the error text (cut to 280 characters) and gave up
    when it named no saved key — a failed int(), the timing keys past the cut, an environment variable made invalid by the
    saved PREOPEN_15M=on. control_config then fell back to the bare environment: the saved LIVE=off and the lowered
    LIVE_MAX_* limits were gone (env LIVE=on traded for real again at $100 an order) and every later save, /live mode off
    included, was refused. Where the text named several saved keys, valid ones were dropped instead, at start-up for good
    and in a page save silently (已保存并生效). Now the culprit is found by trial and a save never drops what it did not send."""
    import logging
    logging.disable(logging.WARNING)  # the drops are logged as warnings: expected here
    live = {**base, "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "PREDICT_API_KEY": "k", "STATE_DB": ":memory:",
            "WEB_CONTROL_KEY": "control-key-123456"}
    caps = {"LIVE": "off", "LIVE_MAX_ORDER_USD": "5", "LIVE_MAX_OPEN_USD": "50", "LIVE_MAX_DAILY_LOSS_USD": "20"}
    risk = lambda c: (c.live_mode, c.live_max_order_usd, c.live_max_open_usd, c.live_max_daily_loss_usd)
    # (a) saved on the page: LIVE=off, lower limits, the pre-open switch. Then one Railway variable makes the pre-open values
    # invalid together and the service restarts (a new bot on the same store). Every one of these came up LIVE=on, $100.
    for saved_extra, env_extra in (({"PREOPEN_ENTRY_SECONDS": "100"}, {"PREOPEN_CANCEL_SECONDS": "60"}),  # the rule's keys past the cut
                                   ({}, {"PREOPEN_MARKETS": "btc,eht"}), ({}, {"PREOPEN_PRICE_CENTS": "49.5"}),  # int() names no key
                                   ({}, {"PREOPEN_SHARES": "1"}), ({}, {"PREOPEN_LIMIT_GAP_PCT": "30"})):
        store = m.Store(":memory:")
        store.put("control:env", {**caps, "PREOPEN_15M": "on", **saved_extra})
        env = {**live, **env_extra}
        m.Config.from_env(env)  # the environment alone validates (the mode is off there): the process starts
        bot = L.LiveBot(m.Config.from_env(env), store, FM(NOW), None)
        c = bot.config
        assert risk(c) == ("off", 5, 50, 20), (env_extra, risk(c))  # was ("on", 100, 500, 200): the environment's
        assert bot.live.config.live_mode == "off" and not c.preopen.enabled, env_extra  # the trader's copy too; the mode stays off
        assert store.get("control:env") == {**caps, **saved_extra}, store.get("control:env")  # only the switch dropped, and forgotten
        assert [line.split("=")[0] for line in bot.control_dropped] == ["PREOPEN_15M"], bot.control_dropped
        text = bot.control_dropped_text()
        assert "PREOPEN_15M=on" in text and "真实交易模式 off" in text and "单笔上限 $5" in text and "/preopen on" in text, text
        # (b) nothing is blocked afterwards: the mode switch and the limits save; switching the pre-open mode back on is
        # refused while the conflict stands, and that refusal touches nothing else saved
        assert bot.control_set({"LIVE": "pause"})["ok"] and bot.config.live_mode == "pause"
        r = bot.control_set({"LIVE": "off", "LIVE_MAX_ORDER_USD": "4"})
        assert r["ok"] and bot.config.live_mode == "off" and bot.config.live_max_order_usd == 4, r
        r = bot.control_set({"PREOPEN_15M": "on"})
        assert not r["ok"] and not bot.config.preopen.enabled and store.get("control:env") == {**caps, "LIVE_MAX_ORDER_USD": "4", **saved_extra}, r
    # a failed int() names no key: saved while the mode was off, the environment switched it on — only that value goes
    store = m.Store(":memory:")
    store.put("control:env", {"LIVE": "pause", "LIVE_MAX_ORDER_USD": "10", "LIVE_MAX_OPEN_USD": "50", "LIVE_MAX_DAILY_LOSS_USD": "20",
                              "PREOPEN_PRICE_CENTS": "49.5", "PREOPEN_SHARES": "20"})
    c = m.Bot(m.Config.from_env({**live, "PREOPEN_15M": "on"}), store, FM(NOW), None).config
    assert risk(c) == ("pause", 10, 50, 20), risk(c)
    assert c.preopen.enabled and c.preopen.price == m.D("0.49") and c.preopen.shares == 20 and "PREOPEN_PRICE_CENTS" not in store.get("control:env")
    # no operator action at all: a newer version tightens a pre-open bound under a saved value
    store = m.Store(":memory:")
    store.put("control:env", {"LIVE": "pause", "LIVE_MAX_ORDER_USD": "5", "PREOPEN_15M": "on", "PREOPEN_PRICE_CENTS": "10"})
    old, m.PREOPEN_MIN_SIDE_USD = m.PREOPEN_MIN_SIDE_USD, 2.0
    try:
        c = m.Bot(m.Config.from_env(live), store, FM(NOW), None).config
    finally:
        m.PREOPEN_MIN_SIDE_USD = old
    assert risk(c)[:2] == ("pause", 5) and not c.preopen.enabled, risk(c)
    # (c) a page save that breaks the saved levels is refused: the levels and AHEAD are never dropped to let it through
    # (it answered 已保存并生效：PREOPEN_PRICE_CENTS=45 with the deeper levels gone and AHEAD 30 -> 1440)
    bot = m.Bot(m.Config.from_env(live), m.Store(":memory:"), FM(NOW), None)
    assert bot.control_set({"PREOPEN_15M": "on", "PREOPEN_LEVELS": "48x20,47x40", "PREOPEN_AHEAD_MINUTES": "30", "PREOPEN_DEEP_AHEAD_MINUTES": "10"})["ok"]
    saved = dict(bot.store.get("control:env"))
    r = bot.control_set({"PREOPEN_PRICE_CENTS": "45"})
    p = bot.config.preopen
    assert not r["ok"] and "PREOPEN_LEVELS" in r["message"] and bot.store.get("control:env") == saved, r
    assert p.ahead_minutes == 30 and len(p.levels) == 3 and p.price == m.D("0.49") and p.deep_ahead_minutes == 10
    # a stale value saved while the mode was off: switching on is refused, the $ cap is never dropped on the way
    bot = m.Bot(m.Config.from_env(live), m.Store(":memory:"), FM(NOW), None)
    assert bot.control_set({"PREOPEN_MAX_OPEN_USD": "50"})["ok"]
    bot.store.put("control:env", {**bot.store.get("control:env"), "PREOPEN_MARKETS": "btc,xyz"})  # a card name no longer listed
    r = bot.control_set({"PREOPEN_15M": "on"})
    assert not r["ok"] and not bot.config.preopen.enabled and bot.store.get("control:env") == {"PREOPEN_MAX_OPEN_USD": "50", "PREOPEN_MARKETS": "btc,xyz"}, r
    # (d) the same at start-up with the switch saved: the other saved pre-open values stay (the $20 cap above all); the mode
    # stays off rather than quoting with no cap (it came up on, cap 0 = none, 49¢, AHEAD 1440, those three deleted for good)
    store = m.Store(":memory:")
    values = {"PREOPEN_MAX_OPEN_USD": "20", "PREOPEN_PRICE_CENTS": "5", "PREOPEN_SHARES": "40", "PREOPEN_CANCEL_SECONDS": "60",
              "PREOPEN_AHEAD_MINUTES": "30", "PREOPEN_MARKETS": "btc,oldcard"}
    store.put("control:env", {"PREOPEN_15M": "on", **values})
    bot = m.Bot(m.Config.from_env(live), store, FM(NOW), None)
    assert not bot.config.preopen.enabled and store.get("control:env") == values, store.get("control:env")
    # (e) a value that stopped validating since it was saved never blocks a save, /live mode off included; the answer names it
    bot = m.Bot(m.Config.from_env({**live, "PREOPEN_CANCEL_SECONDS": "60"}), m.Store(":memory:"), FM(NOW), None)
    bot.store.put("control:env", {"LIVE": "on", "PREOPEN_15M": "on", "PREOPEN_ENTRY_SECONDS": "100"})  # as an older version could leave it
    r = bot.control_set({"LIVE": "off"})
    assert r["ok"] and bot.config.live_mode == "off" and bot.store.get("control:env") == {"LIVE": "off", "PREOPEN_ENTRY_SECONDS": "100"}, r
    assert "PREOPEN_15M 保存的值已失效" in r["message"] and "LIVE=off（原 on）" in r["message"], r
    # the pinned case is unchanged: one stale value alone is dropped, the rest kept
    store = m.Store(":memory:")
    store.put("control:env", {"SIM_EDGE_CENTS": "12", "SIM_SECONDS": "2", "LIVE": "off"})
    c = m.Bot(m.Config.from_env(live), store, FM(NOW), None).config
    assert c.sim_edge == 0.12 and c.sim_seconds == 5 and c.live_mode == "off" and store.get("control:env") == {"SIM_EDGE_CENTS": "12", "LIVE": "off"}
    logging.disable(logging.NOTSET)


ctrl_prune()


async def ctrl_prune_notice():
    """CTRL-PRUNE: saved settings dropped at a start reach the administrator on Telegram (Railway's log was the only trace)."""
    import time

    class Market(m.Binance):
        def now_ms(self): return int(time.time() * 1000)
        async def sync_clock(self): pass
        async def prices(self): return {"UNITREEUSDT": {"symbol": "UNITREEUSDT", "price": "73.13", "time": self.now_ms()}}
        async def get(self, path, **p): return []

    class Telegram:
        def __init__(self): self.sent = []
        async def call(self, method, payload=None, timeout=15):
            if method == "getUpdates":
                await asyncio.sleep(0.2)
                return []
            return {} if method in {"getWebhookInfo", "getMe"} else True
        async def send(self, chat, thread, text, reply_markup=None, parse_mode=None): self.sent.append((chat, text))

    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "ADMIN_USER_ID": "42", "SYMBOLS": "UNITREEUSDT", "BASELINE_MODE": "manual",
                             "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "SSE_INDEX": "off", "PREDICT": "off", "BNB_TOUCH": "off",
                             "SIM": "off", "PROBABILITY": "off", "WEB": "off", "POLL_SECONDS": "3"})
    store = m.Store(":memory:")
    store.put("control:env", {"SIM_EDGE_CENTS": "12", "SIM_SECONDS": "2"})  # SIM_SECONDS under its bound (3) since it was saved
    bot = m.Bot(cfg, store, Market(cfg), Telegram())
    runner = asyncio.create_task(bot.run())
    for _ in range(60):
        await asyncio.sleep(0.05)
        if bot.telegram.sent:
            break
    bot.stopping.set()
    await asyncio.wait_for(runner, 10)
    notes = [text for chat, text in bot.telegram.sent if chat == 42 and "网页控制台保存的设置" in text]
    assert len(notes) == 1 and "SIM_SECONDS=2" in notes[0] and "SIM_EDGE_CENTS" not in notes[0], bot.telegram.sent
    assert bot.config.sim_edge == 0.12 and store.get("control:env") == {"SIM_EDGE_CENTS": "12"}


asyncio.run(ctrl_prune_notice())


async def ctrl_datajson_leak():
    """CTRL-DATAJSON-LEAK: data.json, which the page link alone opens, carried the trader's own lines on every card — 挂单
    (挂涨 挂单中 5.0¢, 等待发送 before the order is even sent), 距吃单 (each side's filled shares, 这一边持仓 300 份, an order in
    flight with its Predict id) and the paper-trading card's nearest markets — while why.json (the same facts at length), the
    journal and control.json asked for the control key. With a real account behind the trader (a real-trading key, or real
    orders in the records) they are left out now: a card keeps a 距吃单 stub (no line is drawn; its 为什么挂 / 不挂单 row stays
    and asks for the key) and nothing in the file follows the account's orders or positions. A paper bot's page keeps them."""
    import datetime as dt
    D = m.D
    BJ = lambda h, mi=0: int(dt.datetime(2026, 10, 5, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
    now, slug, key = BJ(10), "sk-hynix-inc-up-or-down-on-october-5-2026", "control-key-123456"
    settle = {"key": "SKHYNIXUSDT", "target": "2026-10-05", "line": 1398000.0, "close_ms": BJ(16, 10)}
    stub = {"state": "off", "text": "", "private": True}
    driver = m.market_driver("close", "SKHYNIXUSDT", settle, "SK 海力士")
    taker = lambda side, real, **fields: (f"{slug}|{side}|吃", {
        "v": 2, "market": slug, "slug": slug, "market_id": "201", "item": "SK 海力士", "kind": "close", "key": "SKHYNIXUSDT", "side": side,
        "label": "吃涨" if side == "up" else "吃跌", "maker": False, "fills": [], "revisions": [], "settle": settle, "driver": driver[0],
        "driver_name": driver[1], "opened": now - 3_600_000, **fields, **({"live": real} if real else {})})
    flying = lambda: taker("up", {"state": "open", "order_id": "LIVEORDER4", "hash": "0xdef"},  # a real taker order on Predict, nothing filled yet
                           status="resting", shares=0.0, order=50.0, price=0.07, edge=0.1, fair=0.177)

    def build(live_key=False, orders="", control_key=True):
        env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "SKHYNIXUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "SIM_WAYS": "both",
               "SIM_MARKETS": "all", "SIM_MAKER_POINTS": "off", "SIM_MAKER_FLOOR_SHARES": "0", "SIM_MAKER_FOLLOW_SETTLE_SECONDS": "0",
               "SIM_TAKER_MAX_SHARES": "300",
               **({"WEB_CONTROL_KEY": key} if control_key else {}), **({"PREDICT_PRIVATE_KEY": "0x" + "a" * 64} if live_key else {})}
        bot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(now), None)
        book = m.PredictBook("SKHYNIXUSDT", slug, "201", "t", tuple((D(p), D(q)) for p, q in (("0.10", "212"), ("0.05", "312.1"), ("0.01", "1000"))),
                             tuple((D(p), D(q)) for p, q in (("0.35", "127.1"), ("0.43", "175"))), now, None)
        card = m.SimMarket(slug, "SK 海力士", "close", "SKHYNIXUSDT", 0.177, book, 0.04, "", ("涨", "跌"), settle, {}, in_session=True)
        bot.sim_markets = lambda now_ms, only=None: [card]
        pk = bot.predict_key("SK 海力士｜SKHYNIXUSDT")
        bot.predict.slugs[pk], bot.predict.books[pk] = slug, book  # the page's SK 海力士 card shows this market's book (id 201)
        if orders:
            real, changed = orders == "real", []
            bot.sim_quote(card, m.TradeMap({}), now, changed)  # the trader rests 挂跌 65¢ and 挂涨 5¢, 100 shares each ...
            assert sorted(tid.rpartition("|")[0].rpartition("|")[2] for tid, _ in changed) == ["down", "up"], changed
            for n, (_, t) in enumerate(sorted(changed, key=lambda kv: kv[0])):  # ... as real orders: 跌 on Predict's book, 涨 decided and not sent yet
                if real:
                    t["live"] = {"state": ("open", "pending")[n], "order_id": f"LIVEORDER{n + 1}", "hash": "0xabc"}
            held = taker("down", real and {"state": "done", "order_id": "LIVEORDER3", "final": "filled"},  # ... and holds 300 跌 shares a taker
                         status="filled", shares=300.0, order=300.0, price=0.62, edge=0.12, fair=0.82)  # bought: the per-side total, reached
            bot.sim_save([*changed, held])
        return bot

    async def fetch(bot, name="data.json", extra=""):
        web = m.WebServer(bot, 0, TOKEN); web.CACHE_SECONDS = {}; port = await web.start()
        try:
            st, body = await request(port, f"GET /p/{TOKEN}/{name} HTTP/1.1\r\nHost: x\r\n{extra}\r\n".encode())
        finally:
            await web.stop()
        return st, body.decode()

    card_of = lambda data: next(i for i in data["items"] if i.get("symbol") == "SKHYNIXUSDT")["predict"]
    sim_of = lambda data: next(i for i in data["items"] if i.get("kind") == "sim")
    cards = lambda data: [{k: v for k, v in i.items() if k != "sim"} for i in data["items"]]  # every card, less the paper-trading card's totals
    told = lambda text: [w for w in ("挂单中", "等待发送", "真实订单", "吃单等待成交", "持仓", "已吃", "LIVEORDER", "300 份") if w in text]

    # real trading set up (the key configured, real orders in the records) and WEB_CONTROL_KEY set
    live = build(live_key=True, orders="real")
    full = card_of(live.odds_payload())  # the lines are still built (the keyed row, /sim): this block is what data.json carried
    assert full["maker"] == {"text": "挂涨 等待发送 5.0¢；挂跌 挂单中 65.0¢", "state": "resting"}, full["maker"]
    assert [(s["label"], s["taken"], s["block"]) for s in full["taker"]["sides"]] == [("吃涨", 0, ""), ("吃跌", 300, "这一边持仓 300 份，达到上限 300 份")], full["taker"]
    st, text = await fetch(live)
    data = json.loads(text)
    p, sim = card_of(data), sim_of(data)
    assert st == 200 and "maker" not in p and p["taker"] == stub, (st, p.get("maker"), p.get("taker"))
    assert p == {**{k: v for k, v in full.items() if k not in ("maker", "taker")}, "taker": stub} and p["market_id"] == "201" and p["bids"] and p["asks"]  # the book is all there
    assert sim["near"] == [] and sim["sim"]["private"] is True and sim["sim"]["rows"] == [] and sim["sim"]["total"]["trades"] == 3, sim  # the totals stay, as documented
    assert not told(text), told(text)
    # ... then a real taker order in flight: its Predict id and progress were in the line and its hover text
    live.sim_save([flying()])
    assert "真实订单 #LIVEORDER4 已成交 0/50 份" in card_of(live.odds_payload())["taker"]["text"]
    st, text2 = await fetch(live)
    assert st == 200 and card_of(json.loads(text2)) == p and sim_of(json.loads(text2))["near"] == [] and not told(text2), told(text2)
    # nothing in the file follows the account's orders: the same bot with an empty record serves the very same cards
    st, clean = await fetch(build(live_key=True))
    assert st == 200 and cards(json.loads(clean)) == cards(data) == cards(json.loads(text2))
    # the owner's way to the same facts is as before: why.json, with the key
    st, body = await fetch(live, "why.json")
    assert st == 403 and json.loads(body)["locked"], body[:200]
    st, body = await fetch(live, "why.json", f"X-Control-Key: {key}\r\n")
    why = json.loads(body)["markets"]["201"]
    assert st == 200 and why["maker"]["text"] == full["maker"]["text"] and "持仓 300 份" in why["maker"]["sides"][1]["text"], why["maker"]
    assert "真实订单 #LIVEORDER4 已成交 0/50 份" in why["taker"]["text"] and "这一边持仓 300 份" in why["taker"]["text"], why["taker"]
    # without WEB_CONTROL_KEY the account is as private (CTRL-NO-CONTROL-KEY): the same file
    st, text = await fetch(build(live_key=True, orders="real", control_key=False))
    assert st == 200 and card_of(json.loads(text)) == p and not told(text), told(text)

    # a paper bot (no key, no real order in the records): its page keeps the trader's lines
    paper = build(orders="paper")
    st, text = await fetch(paper)
    data = json.loads(text)
    p2, sim2 = card_of(data), sim_of(data)
    assert st == 200 and p2["maker"] == {"text": "挂涨 挂单中 5.0¢；挂跌 挂单中 65.0¢", "state": "resting"}, p2.get("maker")
    assert p2["taker"]["state"] == "near" and p2["taker"]["sides"][1]["taken"] == 300 and "这一边持仓 300 份" in p2["taker"]["detail"], p2["taker"]
    assert sim2["near"] and sim2["near"][0]["text"] == p2["taker"]["text"] and sim2["sim"]["private"] is False and len(sim2["sim"]["rows"]) == 3, sim2
    # ... until its records hold a real order (a database kept after the key was removed). The cache is on here, as in
    # production (a body a second): the copy built a moment before the order is never served after it
    web = m.WebServer(paper, 0, TOKEN); port = await web.start()
    try:
        st, body = await get(port, "data.json")
        assert st == 200 and "挂单中" in card_of(json.loads(body))["maker"]["text"] and not web.account_private()
        paper.sim_save([flying()])
        assert web.account_private()
        st, body = await get(port, "data.json")
        assert st == 200 and card_of(json.loads(body)) == p and sim_of(json.loads(body))["near"] == [] and not told(body.decode()), told(body.decode())
    finally:
        await web.stop()

    # page_payload itself: the price / market-cap ladders' levels as the cards, the payload given left as it is
    payload = {"items": [{"predict": {"edges": [1], "fair": 0.7, "maker": {"text": "挂涨 挂单中 5.0¢", "state": "resting"}, "taker": {"state": "ready", "sides": [{"taken": 9}]}},
                          "ladder": {"rows": [{"edges": [1], "fair": 0.5, "market_id": "7", "maker": {"text": "挂Yes 等待发送 3.0¢"}, "taker": {"state": "near", "gap": 0.02}},
                                              {"fair": 0.4}, "odd"]}},
                         {"kind": "sim", "sim": {"private": True}, "near": [{"text": "可吃单：吃涨 封顶 59.0¢，可买 128 份（已吃 172 份，上限 300）"}]}, {"missing": "x"}]}
    before = json.dumps(payload, ensure_ascii=False)
    private = m.page_payload(payload, True)
    assert private["items"][0]["predict"] == {"fair": 0.7, "taker": stub}, private["items"][0]
    assert private["items"][0]["ladder"]["rows"] == [{"fair": 0.5, "market_id": "7", "taker": stub}, {"fair": 0.4}, "odd"]  # no stub on a level the trader does not price
    assert private["items"][1:] == [{"kind": "sim", "sim": {"private": True}, "near": []}, {"missing": "x"}] and json.dumps(payload, ensure_ascii=False) == before
    public = m.page_payload(payload)  # a paper bot's: only the edge chips go, as before
    assert public["items"][0]["predict"] == {k: v for k, v in payload["items"][0]["predict"].items() if k != "edges"}
    assert public["items"][0]["ladder"]["rows"][0]["taker"] == {"state": "near", "gap": 0.02} and public["items"][1] == payload["items"][1]

    # the page's side of the stub: neither line is drawn for it, the 为什么挂 / 不挂单 row (keyed) stays; a paper bot's blocks draw both
    src = m.WEB_PAGE
    assert 'if(!t||t.state==="off")return null' in src and "if(!p||!p.market_id||!p.taker)return null" in src and "if(it.near&&it.near.length)" in src
    import shutil, subprocess
    node = shutil.which("node")
    if node:
        lines = src[src.index("function makerLine("):src.index("// 为什么挂 / 不挂单:")]
        row = src[src.index("function whyRow("):src.index("function whyBody(")]
        js = ("const $=(tag,cls,text)=>({tag,cls,text,kids:[],append(...a){this.kids.push(...a)},addEventListener(){},setAttribute(){}}),"
              "cent=v=>(v*100).toFixed(1)+'¢',open=new Set();let whyData=null,whyErr='',whyBusy=false,last=null;"
              "function loadWhy(){}function render(){}function whyBody(){return $('div')}\n" + lines + "\n" + row + "\n"
              "const stub=" + json.dumps(stub) + ",made=whyRow({market_id:'201',taker:stub});"
              "console.log(JSON.stringify([takerLine(stub),makerLine(undefined),made&&made.kids[0].text,whyRow({market_id:'201'}),"
              "!!takerLine({state:'near',text:'距吃单 3.0¢：吃涨 卖1 62.0¢，封顶 59.0¢',gap:0.03}),!!makerLine({text:'挂涨 挂单中 5.0¢',state:'resting'})]))")
        out = subprocess.run([node, "-e", js], capture_output=True, text=True, encoding="utf-8", check=True).stdout.strip()
        assert json.loads(out) == [None, None, "▸ 为什么挂 / 不挂单", None, True, True], out


asyncio.run(ctrl_datajson_leak())


async def ctrl_live_allowed_failopen():
    """CTRL-LIVE-ALLOWED-FAILOPEN: LIVE_TRADING_ALLOWED — the deployment's hard switch, the README's emergency stop — was read
    like an ordinary default-on switch (`not off(...)`): only off / 0 / false / no stopped real orders, and a stop typed any
    other way (关, disabled, pause, a typo, a quoted "off", an emptied value) left LIVE=on sending them, the mode shown as on
    and no 硬开关 line anywhere. Now it fails closed: only the variable left unset, or on / 1 / true / yes, allows real orders;
    every other value gives the very configuration off gives. And the page's save under the switch refuses every word that
    reads as LIVE=on, no longer the bare "on" alone (true / 1 / yes were saved, and came up on at the restart that lifted it)."""
    import dataclasses, datetime as dt
    D = m.D
    live = {**base, "LIVE": "on", "PREDICT_PRIVATE_KEY": "0x" + "a" * 64, "PREDICT_API_KEY": "k", "STATE_DB": ":memory:",  # the SDK test suite's key, never a real one
            "SIM_WAYS": "both", "SIM_MARKETS": "all", "SIM_MAKER_MIN_BID": "0", "SIM_MAKER_SESSION": "off", "SIM_MAKER_POINTS": "off"}
    conf = lambda word=None, **more: m.Config.from_env({**live, **more, **({} if word is None else {"LIVE_TRADING_ALLOWED": word})})

    # (a) the words. Allowed: the variable left unset (the documented default) and on / 1 / true / yes, in any case, blanks around
    for word in (None, "on", "ON", " On ", "on\n", "1", "true", "True", "yes", "YES"):
        c = conf(word)
        assert c.live_allowed is True and c.live_mode == "on" and c.live, (word, c.live_allowed, c.live_mode)
    off = conf("off")
    assert off.live_allowed is False and off.live_mode == "pause" and off.live  # the documented stop: capped at pause, the module loaded
    stops = ("OFF", " off ", "0", "false", "no",  # these stopped before too
             "关", "关闭", "否", "停", "disabled", "disable", "pause", "paused", "stop", "none", "never", "n", "f", "of", "0ff", "offf",  # a stop in other words, a typo
             "", "  ", '"off"', "'off'", "off # stop", "Off.", "false # emergency",  # emptied; quoted (docker --env-file keeps the quotes); a comment kept
             "enabled", "allow", "y", "开", '"on"', "on!")  # meant as allowed but none of the four: a pause the operator is told of, never a guess
    for word in stops:
        c = conf(word)
        assert c.live_allowed is False and c.live_mode == "pause" and c.live, (word, c.live_allowed, c.live_mode)  # 关: was True, on
        assert c == off, word  # every field (the environment itself kept aside): whatever off does downstream, this does
    for word in (None, "on", "off", "关"):  # the switch only caps on: pause and off stay as chosen
        assert conf(word, LIVE="pause").live_mode == "pause" and conf(word, LIVE="off").live_mode == "off" and not conf(word, LIVE="off").live, word

    # (b) the README's emergency stop, end to end: the page saved LIVE=on earlier (it stands over the environment's LIVE), the
    # operator sets the deployment variable and the service restarts; one step on a market with edge both ways
    BJ = lambda h, mi=0: int(dt.datetime(2026, 10, 5, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
    now, slug = BJ(10), "hang-seng-index-up-or-down-on-october-5-2026"
    book = m.PredictBook("HSI", slug, "101", "t", ((D("0.55"), D("300")),), ((D("0.58"), D("400")),), now, None)
    card = m.SimMarket(slug, "恒生指数", "close", "HSI", 0.70, book, 0.03, "", ("涨", "跌"),
                       {"key": "HSI", "target": "2026-10-05", "line": 24600.0, "close_ms": BJ(16, 10)},
                       {"basis": {"fair_up": 0.70, "ref": 24600.0}, "sources": [{"what": "恒指期货", "source": "etnet"}], "proxy": None})
    detail = {"id": 101, "status": "REGISTERED", "isNegRisk": False, "isYieldBearing": False, "feeRateBps": 200, "conditionId": "0x" + "ab" * 32,
              "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": "1011"}, {"name": "Down", "indexSet": 2, "onChainId": "1012"}]}

    class Api(FakeApi):  # Predict with a book of the bot's own orders: every order that reaches it is counted
        async def create_order(self, body):
            self.calls.append(("create", body))
            oid, data = str(100 + len(self.calls)), body["data"]
            self.open[oid] = {"id": oid, "status": "OPEN", "amount": data["order"]["takerAmount"], "amountFilled": "0",
                              "strategy": data["strategy"], "order": {**data["order"]}}
            return {"order_id": oid, "hash": data["order"]["hash"], "code": None}
        async def orders(self, status="OPEN", **params): return [dict(r) for r in self.open.values()] if status == "OPEN" else []
        async def order(self, oid, hash_=""): return dict(self.open[oid]) if oid in self.open else None

    async def restart(word):
        store = m.Store(":memory:")
        store.put("control:env", {"LIVE": "on"})
        bot = L.LiveBot(conf(word, LIVE="off"), store, FM(now), None)  # Bot.__init__ lays the saved LIVE=on over the environment
        bot.live.api, bot.live.chain = Api(), FakeChain()
        bot.live.api.markets["101"] = dict(detail)
        bot.sim_markets = lambda now_ms, only=None: [card]
        await bot.live_prepare()
        await bot.live_prefetch(now)
        bot.sim_ran = -1e9
        await bot.sim_step(now)
        return bot, [body for kind, body in bot.live.api.calls if kind == "create"]

    bot, sent = await restart(None)  # the switch left unset: real orders go out — what follows is not an empty check
    assert bot.live.ready and bot.config.live_mode == "on" and bot.config.live_allowed and len(sent) >= 1, (bot.config.live_mode, len(sent))
    for word in ("off", "关", "disabled", "pause", "", '"off"'):
        bot, sent = await restart(word)
        assert bot.live.ready and bot.config.live_mode == "pause" and bot.config.live_allowed is False, (word, bot.config.live_mode)
        assert sent == [] and not bot.sim_trades(), (word, len(sent))  # 关: was on, real orders sent
        text, status = "\n".join(bot.live.summary_lines()), bot.live_status(now)
        assert "硬开关" in text and "LIVE_TRADING_ALLOWED" in text and status["allowed"] is False and status["mode"] == "pause", (word, text)
        reply = await bot.cmd_live(m.Request("/live", ["mode", "on"], 1, 0, 1))
        assert "LIVE_TRADING_ALLOWED" in reply and bot.config.live_mode == "pause", (word, reply)
        r = bot.control_set({"LIVE": "on"})
        assert not r["ok"] and "LIVE_TRADING_ALLOWED" in r["message"] and bot.config.live_mode == "pause", (word, r)
        r = bot.control_set({"SIM_EDGE_CENTS": "12"})  # a later save reads the environment again: the cap stands
        assert r["ok"] and bot.config.sim_edge == 0.12 and bot.config.live_mode == "pause" and bot.config.live_allowed is False, (word, r)
        bot.apply_config(dataclasses.replace(bot.config, live_mode="on"))  # even forced on in memory, both gates refuse by the switch
        await asyncio.sleep(0)
        assert "LIVE_TRADING_ALLOWED" in bot.live_room({}, card, "up", 0.58, 100), word
        assert "LIVE_TRADING_ALLOWED" in bot.live_send_block({"live": {"want": {"shares": 100}, "events": [{"at": now}]}, "order": 100,
                                                              "price": 0.5, "opened": now}, {}, now), word

    # (c) the page's save under the switch: every word Config.from_env reads as LIVE=on is refused and nothing is saved. A
    # hand-made request's true / 1 / yes (the page's buttons send off / pause / on) was saved, capped at pause while the switch
    # stood, and came up on at the restart that lifted it
    for switch in ("off", "关"):
        for word in ("on", "ON", " on ", "1", "true", "True", "yes", "YES", True, 1):
            store = m.Store(":memory:")
            bot = m.Bot(conf(switch, LIVE="pause"), store, FM(NOW), None)
            r = bot.control_set({"LIVE": word})
            assert not r["ok"] and "LIVE_TRADING_ALLOWED" in r["message"] and store.get("control:env") is None, (switch, word, r)
            assert bot.config.live_mode == "pause", (switch, word, bot.config.live_mode)
            lifted = m.Bot(conf("on", LIVE="pause"), store, FM(NOW), None)  # the switch lifted, the service restarted on the same database
            assert lifted.config.live_mode == "pause", (switch, word, lifted.config.live_mode)  # true / 1 / yes: was on
        # the refusal and Config.from_env agree on the words: one that does not read as on is saved as before
        bot = m.Bot(conf(switch, LIVE="pause"), m.Store(":memory:"), FM(NOW), None)
        for word in ("on", "1", "true", "yes", "y", "t", "enable", "enabled", "开", "live", "pause", "paused", "test", "off", "0", "false", "no"):
            reads, before = conf("on", LIVE=word).live_mode, bot.config.live_mode
            r = bot.control_set({"LIVE": word})
            assert r["ok"] is (reads != "on") and bot.config.live_mode == (before if reads == "on" else reads), (switch, word, r, bot.config.live_mode)


asyncio.run(ctrl_live_allowed_failopen())
print("AUDIT_CONTROL_OK")
