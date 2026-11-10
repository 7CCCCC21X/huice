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
print("AUDIT_CONTROL_OK")
