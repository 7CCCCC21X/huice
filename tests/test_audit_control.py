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
print("AUDIT_CONTROL_OK")
