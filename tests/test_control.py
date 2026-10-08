"""The control page (/p/<token>/control): settings changed while the bot runs (validated like the environment, applied
at once, saved across restarts), and the real-trading switches, every action behind WEB_CONTROL_KEY."""
import asyncio, dataclasses, json, sys, time, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L
D = m.D
WEI = 10 ** 18
KEY = "secret-key-12345"
TOKEN = "t" * 20
BJ = lambda mo, d, h, mi=0: int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
NOW = BJ(10, 5, 10, 0)
base = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080"}

# --- settings ----------------------------------------------------------------------------------------------------------
c = m.Config.from_env({**base, "WEB_CONTROL_KEY": KEY})
assert c.web_control_key == KEY and c.env["WEB_PORT"] == "8080" and "web_control_key" not in repr(c) and "env=" not in repr(c)
assert m.Config.from_env(base).web_control_key == "" and m.Config.from_env(base).env["TELEGRAM_BOT_TOKEN"] == "1:x"
for bad in ("short", "has space in it yes", "x" * 65, "中文口令中文口令中文口令"):
    try: m.Config.from_env({**base, "WEB_CONTROL_KEY": bad}); assert False, bad
    except ValueError: pass
assert [k for k, _, _ in m.CONTROL_KEYS][:3] == ["LIVE", "SIM_EDGE_CENTS", "SIM_SHARES"] and m.CONTROL_KEY_SET >= {"LIVE_MAX_ORDER_USD", "LIVE_TAKER_WAIT_SECONDS"} and not m.CONTROL_KEY_SET & {"LIVE_TAKER", "LIVE_SLIPPAGE_BPS"}
assert all(len(row) == 3 and row[0] == row[0].upper() for row in m.CONTROL_KEYS)


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


async def request(port, raw):
    r, w = await asyncio.open_connection("127.0.0.1", port)
    w.write(raw); await w.drain()
    data = await r.read(); w.close()
    head, _, body = data.partition(b"\r\n\r\n")
    return int(head.split(b" ")[1]), head.decode(), body


async def post(port, body, path=f"/p/{TOKEN}/control", headers=""):
    raw = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False).encode("utf-8")
    st, head, out = await request(port, f"POST {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n{headers}"
                                        f"Content-Length: {len(raw)}\r\n\r\n".encode() + raw)
    try:
        return st, json.loads(out)
    except ValueError:
        return st, {"raw": out.decode(errors="replace")}


async def get_json(port, name, key=KEY, client=""):
    """control.json needs the control key (X-Control-Key) when one is configured; the odds data does not."""
    extra = (f"X-Control-Key: {key}\r\n" if key else "") + (f"X-Forwarded-For: {client}\r\n" if client else "")
    st, _, body = await request(port, f"GET /p/{TOKEN}/{name} HTTP/1.1\r\nHost: x\r\n{extra}\r\n".encode())
    assert st == 200, (name, st, body[:200])
    return json.loads(body)


class FakeApi:
    def __init__(self):
        self.jwt, self.jwt_exp, self.auth_error, self.lookup = "t", 0, "", ""
        self.open, self.closed, self.calls, self.positions_rows, self.markets = {}, {}, [], [], {}
    async def ensure_auth(self): pass
    async def market(self, mid): return self.markets[str(mid)]
    async def remove_orders(self, ids):
        self.calls.append(("remove", list(ids)))
        removed = []
        for i in ids:
            row = self.open.pop(i, None)
            if row:
                row["status"] = "CANCELLED"
                self.closed[i] = row
                removed.append(i)
        return {"removed": removed, "noop": [i for i in ids if i not in removed]}
    async def orders(self, status="OPEN", **params):
        return list(self.open.values()) if status == "OPEN" else [r for r in self.closed.values() if r["status"] == status]
    async def order(self, oid, hash_=""): return self.open.get(oid) or self.closed.get(oid)
    async def positions(self): return self.positions_rows


class FakeChain:
    async def usdt_balance(self, owner=None): return 500 * WEI
    async def bnb_balance(self, owner=None): return 10 ** 17
    async def allowance(self, key, owner=None): return 10 ** 30


async def run():
    store = m.Store(":memory:")
    bot = m.Bot(m.Config.from_env({**base, "WEB_CONTROL_KEY": KEY}), store, FM(NOW), None)
    assert bot.control_value("SIM_EDGE_CENTS") == "10" and bot.control_value("SIM_MARKETS") == "close"
    assert bot.control_value("LIVE_AUTO_REDEEM") == "on" and bot.control_value("nope") == "" and bot.control_value("LIVE") == "off"
    web = m.WebServer(bot, 0, TOKEN); web.CACHE_SECONDS = {}; port = await web.start()
    st, head, body = await request(port, f"GET /p/{TOKEN}/control HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    assert st == 200 and "<title>交易控制台</title>" in body.decode() and "text/html" in head and "Content-Security-Policy" in head
    assert 'data-act="test"' in body.decode() and 'id="testlines"' in body.decode() and 'data-mode="pause"' in body.decode()
    assert 'id="cards"' in body.decode() and 'id="fold"' in body.decode() and 'id="adv"' in body.decode() and 'id="fields"' not in body.decode()  # option cards, no form
    assert 'data-act="cancelaccount"' in body.decode() and 'sessionStorage.getItem("ctlkey")' in body.decode() and 'X-Control-Key' in body.decode()
    st, _, raw = await request(port, f"GET /p/{TOKEN}/control.json HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    assert st == 403 and json.loads(raw)["locked"] and "需要控制口令" in json.loads(raw)["message"]  # the page token alone shows no account data
    st, _, raw = await request(port, f"GET /p/{TOKEN}/control.json HTTP/1.1\r\nHost: x\r\nX-Control-Key: nope-nope-nope\r\n\r\n".encode())
    assert st == 403 and json.loads(raw)["message"] == "口令错误"
    data = await get_json(port, "control.json")
    assert data["enabled"] and data["mode"] == "paper" and data["live"] is None and data["version"] == m.VERSION and data["checks"] == []
    assert 'id="checks"' in body.decode() and 'id="foldchecks"' in body.decode() and "常用开关" in body.decode() and 'id="quickbtns"' in body.decode()
    assert 'id="rectabs"' in body.decode() and 'id="recq"' in body.decode() and 'id="bar"' in body.decode() and 'id="foldrec"' in body.decode() and 'id="foldpos"' in body.decode()
    assert data["records"] == [] and isinstance(data["records"], list)
    # the record rows the page files under its tabs: resting / position / history, newest first, with the reason and the real order
    rb = m.Bot(m.Config.from_env(base), m.Store(":memory:"), FM(NOW), None)
    rec = lambda market, side, label, maker, status, price, shares, opened, **extra: {
        "v": 2, "market": market, "slug": market, "market_id": "1", "item": {"m1": "甲", "m2": "乙", "m3": "丙", "m4": "丁"}[market], "kind": "close", "key": "A",
        "side": side, "label": label, "maker": maker, "fair": 0.6, "opened": opened, "settle": {}, "order": 100.0, "fills": [], "revisions": [],
        "entry": {}, "version": {}, "price": price, "signal": 0.1, "edge": 0.1, "shares": shares, "status": status, "filled": None, **extra}
    rb.store.put("sim:m1|up|挂", rec("m1", "up", "挂涨", True, "resting", 0.21, 0.0, NOW - 60_000, live={"state": "open", "order_id": "77", "events": []}))
    rb.store.put("sim:m1|down|吃", rec("m1", "down", "吃跌", False, "filled", 0.55, 100.0, NOW - 120_000))
    rb.store.put("sim:m2|up|挂#1", rec("m2", "up", "挂涨", True, "cancelled", 0.22, 0.0, NOW - 90_000, note="撤单：买1 被顶到 22.0¢，改挂 23.0¢，一份都没成交",
                                       withdrawn={"at": NOW - 30_000, "why": "买1 被顶到 22.0¢，改挂 23.0¢", "unfilled": 100.0}))
    rb.store.put("sim:m3|up|吃", rec("m3", "up", "吃涨", False, "settled", 0.40, 100.0, NOW - 200_000, payout=1.0, settled=NOW - 10_000, confirm="confirmed", note="收盘涨"))
    rb.store.put("sim:m4|up|挂#1", rec("m4", "up", "挂涨", True, "cancelled", 0.30, 0.0, NOW - 50_000, note="下单失败：HTTP 400",
                                       live={"state": "done", "final": "failed", "error": "下单失败：HTTP 400", "order_id": "", "events": []}))
    rows = rb.control_records(rb.sim_trades())
    assert [r["id"] for r in rows] == ["m3|up|吃", "m2|up|挂#1", "m4|up|挂#1", "m1|up|挂", "m1|down|吃"], [r["id"] for r in rows]
    assert [r["group"] for r in rows] == ["history", "history", "history", "resting", "position"]
    assert [r["status"] for r in rows] == ["赢 +$60.00", "已撤单", "失败", "挂单中（真实订单 #77 已成交 0/100 份）", "持仓"] and rows[0]["state"] == "已确认" and rows[3]["state"] == "", [(r["status"], r["state"]) for r in rows]
    assert [r["why"] for r in rows] == ["", "买1 被顶到 22.0¢，改挂 23.0¢", "下单失败：HTTP 400", "", ""] and rows[2]["failed"] and not rows[1]["failed"]
    assert rows[3]["cancellable"] and rows[3]["order_id"] == "77" and rows[3]["live_state"] == "open" and not rows[4]["cancellable"] and rows[4]["order_id"] == ""
    assert rows[0]["item"] == "丙" and rows[0]["price"] == 0.40 and rows[0]["shares"] == 100.0 and rows[0]["url"].startswith("http")
    assert {r["id"] for r in rb.control_payload()["records"]} == {r["id"] for r in rows}
    assert len(rb.control_records(rb.sim_trades(), history=1)) == 3 and rb.control_records(rb.sim_trades(), history=1)[0]["id"] == "m3|up|吃"  # the history is capped, newest kept
    s = {x["key"]: x for x in data["settings"]}
    assert {k: s["SIM_EDGE_CENTS"][k] for k in ("key", "label", "hint", "value", "saved", "env")} == {"key": "SIM_EDGE_CENTS", "label": "触发买入的净优势（¢/份）", "hint": "0.5～50", "value": "10", "saved": "", "env": ""}
    assert s["SIM_EDGE_CENTS"]["kind"] == "number" and s["SIM_EDGE_CENTS"]["group"] == "策略" and "10" in s["SIM_EDGE_CENTS"]["presets"] and s["SIM_EDGE_CENTS"]["unit"] == "¢"
    assert s["SIM_WAYS"]["kind"] == "choice" and s["SIM_WAYS"]["options"] == [["taker", "只吃单"], ["maker", "只挂单"], ["both", "挂单和吃单"]]
    assert s["SIM_MARKETS"]["kind"] == "multi" and [o[0] for o in s["SIM_MARKETS"]["options"]] == list(m.SIM_KINDS) and s["SIM_MARKETS"]["options"][0][1] == "指数/个股日涨跌"
    assert s["SIM_TAKER_SESSION"]["options"] == [["off", "全天都吃单"], ["on", "只在开盘时段吃单"]] and s["LIVE"]["options"][1][0] == "pause" and s["LIVE_NOTIFY"]["group"] == "高级"
    assert s["SIM_GROUP_USD"]["zero"] == "不限" and s["LIVE_MAX_DAILY_LOSS_USD"]["presets"][0] == "0"
    assert s["SIM_MAKER_AFTER_HOURS_CENTS"]["zero"] == "关" and s["SIM_MAKER_AFTER_HOURS_CENTS"]["value"] == "0" and "10" in s["SIM_MAKER_AFTER_HOURS_CENTS"]["presets"]
    assert s["SIM_MAKER_DEEP_CENTS"]["zero"] == "关" and s["SIM_MAKER_DEEP_CENTS"]["value"] == "0" and "25" in s["SIM_MAKER_DEEP_CENTS"]["presets"]
    assert s["SIM_TAKER_AFTER_HOURS_CENTS"]["zero"] == "关" and s["SIM_TAKER_AFTER_HOURS_CENTS"]["value"] == "0" and s["SIM_AFTER_HOURS_MAX_PRICE_CENTS"]["value"] == "60" and s["SIM_AFTER_HOURS_MAX_PRICE_CENTS"]["zero"] == "不限"
    assert s["SIM_MAKER_DEEP_ONLY"]["options"] == [["off", "两种都挂"], ["on", "只挂低价挂单"]] and s["SIM_MAKER_DEEP_ONLY"]["value"] == "off"
    assert s["SIM_MAKER_SPREAD_CENTS"]["zero"] == "不限" and s["SIM_MAKER_SPREAD_CENTS"]["value"] == "0" and "10" in s["SIM_MAKER_SPREAD_CENTS"]["presets"]
    assert s["SIM_SECONDS"]["kind"] == "number" and s["SIM_SECONDS"]["group"] == "高级" and s["SIM_SECONDS"]["value"] == "5" and s["SIM_SECONDS"]["unit"] == "秒" and "3" in s["SIM_SECONDS"]["presets"]
    assert s["PREDICT_POLL_SECONDS"]["kind"] == "number" and s["PREDICT_POLL_SECONDS"]["value"] == "10" and "5" in s["PREDICT_POLL_SECONDS"]["presets"] and data["cadence"] == {"sim": 5, "book": 10}
    assert 'id="checksnote"' in body.decode() and "renderCadence" in body.decode()
    assert s["SIM_MAKER_DEEP_MARKETS"]["kind"] == "detail" and s["SIM_MAKER_DEEP_MARKETS"]["value"] == "" and s["SIM_MAKER_AFTER_HOURS_MARKETS"]["kind"] == "detail"
    assert "全部市场" in body.decode() and ".sw.on" in body.decode() and "绿色 = 开" in body.decode() and "挂单没开，不生效" in body.decode()
    assert s["SIM_SKIP"]["kind"] == "detail" and s["SIM_SKIP"]["value"] == "" and s["SIM_SKIP"]["group"] == "策略"
    assert isinstance(data["catalog"], list) and all({"kind", "key", "name", "skipped"} <= set(r) for r in data["catalog"]) and "展开详细选项" in body.decode()
    # every setting has a widget, every group is one the page knows, and every preset / option is a value the Config accepts
    assert set(m.CONTROL_FORMS) == set(m.CONTROL_KEY_SET) and {f["group"] for f in m.CONTROL_FORMS.values()} == {"策略", "风控", "高级"}
    withkey = {**base, "PREDICT_PRIVATE_KEY": "a" * 64}
    for key, form in m.CONTROL_FORMS.items():
        if form["kind"] == "detail":  # SIM_SKIP: drawn inside the SIM_MARKETS card from the catalog, no presets or options of its own
            assert "presets" not in form and "options" not in form and key in {"SIM_SKIP", "SIM_MAKER_AFTER_HOURS_MARKETS", "SIM_MAKER_DEEP_MARKETS"}, key
            continue
        assert form["kind"] in {"choice", "multi", "number"} and (form["kind"] == "number") == ("presets" in form) and (form["kind"] != "number") == ("options" in form), key
        for value in (form.get("presets") or [o[0] for o in form.get("options", [])]):
            m.Config.from_env({**withkey, key: value})  # raises when a preset is out of range
        if form["kind"] == "multi":
            m.Config.from_env({**withkey, key: ",".join(o[0] for o in form["options"])})
    assert data["scope"] == ["只吃单", "指数/个股日涨跌"] and data["sim"]["trades"] == 0 and len(data["settings"]) == len(m.CONTROL_KEYS)
    # the token still gates everything; other POSTs stay refused; the key is required and compared, the body must be JSON
    st, _, _ = await request(port, f"GET /p/{'u' * 20}/control.json HTTP/1.1\r\nHost: x\r\n\r\n".encode()); assert st == 404
    st, _, _ = await request(port, f"POST /p/{TOKEN}/journal HTTP/1.1\r\nHost: x\r\n\r\n".encode()); assert st == 405
    st, _, _ = await request(port, f"POST /p/{'u' * 20}/control HTTP/1.1\r\nHost: x\r\n\r\n".encode()); assert st == 405
    st, j = await post(port, {"action": "set", "values": {"SIM_EDGE_CENTS": "12"}}); assert (st, j) == (403, {"ok": False, "message": "口令错误"})
    st, j = await post(port, {"key": "wrong-key-123456", "action": "set"}); assert st == 403 and j["message"] == "口令错误"
    st, j = await post(port, b"not json"); assert st == 400 and "JSON" in j["message"]
    st, j = await post(port, b"[1,2]"); assert st == 400
    assert bot.config.sim_edge == 0.10 and store.get("control:env") is None
    # set: validated like the environment, applied at once, saved; a bad value changes nothing
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_EDGE_CENTS": "12", "SIM_WAYS": "both", "LIVE_MAX_ORDER_USD": "40", "SIM_SHARES": ""}})
    assert st == 200 and j["ok"] and "SIM_EDGE_CENTS=12（原 10）" in j["message"] and "SIM_WAYS=both（原 taker）" in j["message"], j
    assert bot.config.sim_edge == 0.12 and bot.config.sim_ways == "both" and bot.config.live_max_order_usd == 40 and bot.config.sim_shares == 100
    assert store.get("control:env") == {"SIM_EDGE_CENTS": "12", "SIM_WAYS": "both", "LIVE_MAX_ORDER_USD": "40"}
    assert m.sim_scope(bot.config) == ("挂单和吃单", "指数/个股日涨跌") and bot.sim_version()["sim_edge"] == 0.12  # the loops read self.config
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_EDGE_CENTS": "99"}})
    assert st == 400 and not j["ok"] and "SIM_EDGE_CENTS" in j["message"] and bot.config.sim_edge == 0.12
    assert store.get("control:env")["SIM_EDGE_CENTS"] == "12"
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"TELEGRAM_BOT_TOKEN": "x"}})
    assert st == 400 and "不能在网页上改的变量" in j["message"] and store.get("control:env") == {"SIM_EDGE_CENTS": "12", "SIM_WAYS": "both", "LIVE_MAX_ORDER_USD": "40"}
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_WAYS": "maker", "SIM_MARKETS": "range"}})
    assert st == 400 and "价格阶梯" in j["message"] or st == 200  # a combination the environment would accept is accepted too
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_WAYS": "", "SIM_MARKETS": "all"}})
    assert st == 200 and bot.config.sim_ways == "taker" and bot.config.sim_markets == frozenset(m.SIM_KINDS), (st, j, bot.config.sim_ways, sorted(bot.config.sim_markets), store.get("control:env"))
    data = await get_json(port, "control.json")
    s = {x["key"]: x for x in data["settings"]}
    assert s["SIM_WAYS"]["saved"] == "" and s["SIM_WAYS"]["value"] == "taker" and s["SIM_EDGE_CENTS"]["saved"] == "12" and s["SIM_MARKETS"]["value"] == "all"
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_MARKETS": "all"}})
    assert st == 200 and j["message"] == "已保存，设置没有变化。"
    # the cadence: the trader and the orderbook feed (its own reference to the config) both run on the new numbers at once
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_SECONDS": "3", "PREDICT_POLL_SECONDS": "5"}})
    assert st == 200 and bot.config.sim_seconds == 3 and bot.predict.config.predict_poll == 5 and (await get_json(port, "control.json"))["cadence"] == {"sim": 3, "book": 5}, (st, j)
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_SECONDS": "2"}})
    assert st == 400 and j == {"ok": False, "message": "SIM_SECONDS 必须在 3～60 之间"} and bot.config.sim_seconds == 3, (st, j)
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_SECONDS": "", "PREDICT_POLL_SECONDS": ""}})
    assert st == 200 and bot.config.sim_seconds == 5 and bot.predict.config.predict_poll == 10, (st, j)
    # the detail chips: SIM_SKIP names single markets left out (upper-cased, deduplicated), the catalog flags them, "" clears it
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_SKIP": " hsi, bnb ,HSI"}})
    assert st == 200 and bot.config.sim_skip == {"HSI", "BNB"} and bot.control_value("SIM_SKIP") == "BNB,HSI", (st, j)
    data = await get_json(port, "control.json")
    assert {x["key"]: x for x in data["settings"]}["SIM_SKIP"]["value"] == "BNB,HSI" and all(r["skipped"] == (r["key"].upper() in {"HSI", "BNB"}) for r in data["catalog"])
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_SKIP": "x" * 41}})
    assert st == 400 and "SIM_SKIP" in j["message"] and bot.config.sim_skip == {"HSI", "BNB"}
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_SKIP": ""}})
    assert st == 200 and bot.config.sim_skip == frozenset() and bot.control_value("SIM_SKIP") == ""
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_MAKER_DEEP_MARKETS": "hsi, kospi", "SIM_MAKER_AFTER_HOURS_MARKETS": "sse"}})
    assert st == 200 and bot.config.sim_maker_deep_markets == {"HSI", "KOSPI"} and bot.control_value("SIM_MAKER_DEEP_MARKETS") == "HSI,KOSPI" and bot.config.sim_maker_after_hours_markets == {"SSE"}
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_MAKER_DEEP_MARKETS": "", "SIM_MAKER_AFTER_HOURS_MARKETS": ""}})
    assert st == 200 and bot.config.sim_maker_deep_markets == frozenset() and bot.config.sim_maker_after_hours_markets == frozenset()
    # 一键搭配: the strategy settings only, each combination valid on its own, saved like any set; the page lists them
    data = await get_json(port, "control.json")
    assert [p["name"] for p in data["presets"]] == ["steady", "balanced", "bold"] and all({"name", "label", "hint", "values"} <= set(p) for p in data["presets"])
    assert 'id="presetbtns"' in body.decode() and "renderPresets" in body.decode()
    for name, label, hint, values in m.CONTROL_PRESETS:
        assert set(values) <= m.CONTROL_KEY_SET and not any(k.startswith("LIVE") for k in values), name
        assert not ({"SIM_MARKETS", "SIM_SKIP", "SIM_SHARES", "SIM_GROUP_USD", "SIM_MAKER_POINTS"} & set(values)), name
        m.Config.from_env({**base, **{k: x for k, x in values.items() if x}})
    st, j = await post(port, {"key": KEY, "action": "preset", "name": "balanced"})
    assert st == 200 and j["message"].startswith("已套用「均衡」。已保存并生效：") and bot.config.sim_ways == "both" and bot.config.sim_maker_after_hours == 0.15, (st, j)
    assert bot.config.sim_maker_after_hours_markets == {"HSI", "KOSPI", "SSE"} and bot.config.sim_taker_after_hours == 0.20 and bot.config.sim_maker_min_bid == 50
    assert bot.config.sim_quiet_minutes == 10 and bot.config.sim_maker_deep == 0.25 and not bot.config.sim_maker_deep_only and bot.config.sim_edge == 0.10
    assert store.get("control:env")["SIM_MAKER_AFTER_HOURS_MARKETS"] == "HSI,KOSPI,SSE" and "SIM_MAKER_DEEP_MARKETS" not in store.get("control:env")  # "" clears
    st, j = await post(port, {"key": KEY, "action": "preset", "name": "balanced"}); assert st == 200 and j["message"] == "已套用「均衡」。已保存，设置没有变化。"
    st, j = await post(port, {"key": KEY, "action": "preset", "name": "steady"})
    assert st == 200 and bot.config.sim_ways == "maker" and bot.config.sim_maker_after_hours == 0 and bot.config.sim_taker_after_hours == 0
    assert bot.config.sim_quiet_minutes == 15 and bot.config.sim_maker_min_bid == 100 and bot.config.sim_maker_after_hours_markets == frozenset()
    st, j = await post(port, {"key": KEY, "action": "preset", "name": "bold"})
    assert st == 200 and bot.config.sim_edge == 0.08 and bot.config.sim_maker_exit == 0.04 and bot.config.sim_seconds == 3 and bot.predict.config.predict_poll == 5
    st, j = await post(port, {"key": KEY, "action": "preset", "name": "nope"}); assert st == 400 and j == {"ok": False, "message": "没有这个搭配"}
    st, j = await post(port, {"key": KEY, "action": "preset"}); assert st == 400 and not j["ok"]
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_EDGE_CENTS": "12", "SIM_WAYS": "both", "SIM_SECONDS": "", "PREDICT_POLL_SECONDS": ""}}); assert st == 200  # what the checks below expect
    # a long detail list (every market but one) is accepted; the cards' numbers are plain, never 1e+06
    many = ",".join(f"MARKET{i:02d}X" for i in range(30))  # 30 keys, 269 characters
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_MAKER_DEEP_MARKETS": many}}); assert st == 200 and len(bot.config.sim_maker_deep_markets) == 30, (st, j)
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_MAKER_DEEP_MARKETS": "", "LIVE_MAX_OPEN_USD": "100000000"}})
    assert st == 200 and bot.control_value("LIVE_MAX_OPEN_USD") == "100000000" and "1e+08" not in j["message"], j
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"LIVE_MAX_OPEN_USD": "40"}}); assert st == 200
    # a saved value that stopped validating (a bound tightened since) is dropped on its own: the rest stays, later saves go on
    store.put("control:env", {**store.get("control:env"), "SIM_SECONDS": "2"})
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_SHARES": "50"}})
    assert st == 200 and bot.config.sim_shares == 50 and "SIM_SECONDS" not in store.get("control:env") and bot.config.sim_seconds == 5, (st, j)
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_SHARES": ""}}); assert st == 200
    store.put("control:env", {**store.get("control:env"), "SIM_SECONDS": "2"})
    pruned = m.Bot(m.Config.from_env({**base, "WEB_CONTROL_KEY": KEY}), store, FM(NOW), None)
    assert pruned.config.sim_edge == 0.12 and pruned.config.sim_seconds == 5 and "SIM_SECONDS" not in store.get("control:env")  # only the bad value dropped, and forgotten
    assert store.get("control:env")["SIM_EDGE_CENTS"] == "12"
    # the page's poll is served the change, not a copy built just before it
    web.CACHE_SECONDS = {"control.json": 60.0}
    assert {x["key"]: x for x in (await get_json(port, "control.json"))["settings"]}["SIM_QUIET_MINUTES"]["value"] != "15"  # a copy is built and cached now
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_QUIET_MINUTES": "15"}}); assert st == 200
    assert {x["key"]: x for x in (await get_json(port, "control.json"))["settings"]}["SIM_QUIET_MINUTES"]["value"] == "15"
    web.CACHE_SECONDS = {}
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"SIM_QUIET_MINUTES": ""}}); assert st == 200
    assert "keyBad" in body.decode() and 'if(!data)return' in body.decode()
    # a new bot on the same store starts with the saved settings; a saved value that stopped validating is ignored
    again = m.Bot(m.Config.from_env({**base, "WEB_CONTROL_KEY": KEY}), store, FM(NOW), None)
    assert again.config.sim_edge == 0.12 and again.config.live_max_order_usd == 40 and again.config.sim_markets == frozenset(m.SIM_KINDS)
    assert again.config.env["WEB_PORT"] == "8080" and again.config.web_control_key == KEY
    store.put("control:env", {"SIM_EDGE_CENTS": "nonsense", "SIM_SHARES": "5"})
    assert m.Bot(m.Config.from_env(base), store, FM(NOW), None).config.sim_edge == 0.10  # nothing of it applied
    store.put("control:env", {"SIM_EDGE_CENTS": "12", "LIVE_MAX_ORDER_USD": "40"})
    # reset: back to the environment, nothing saved
    st, j = await post(port, {"key": KEY, "action": "reset"})
    assert st == 200 and j["ok"] and store.get("control:env") is None and bot.config.sim_edge == 0.10 and bot.config.live_max_order_usd == 100
    # a live action on a paper bot; the mode cannot be switched on without a key
    st, j = await post(port, {"key": KEY, "action": "pause"}); assert st == 400 and "LIVE=off" in j["message"]
    st, j = await post(port, {"key": KEY, "action": "set", "values": {"LIVE": "pause"}}); assert st == 400 and "PREDICT_PRIVATE_KEY" in j["message"]
    st, j = await post(port, {"key": KEY, "action": "bogus"}); assert st == 400
    # too many wrong keys lock the route for a while, the right key included
    for _ in range(5):
        st, _ = await post(port, {"key": "wrong-key-123456", "action": "reset"}); assert st == 403
    st, j = await post(port, {"key": KEY, "action": "reset"}); assert st == 429 and "口令错误次数过多" in j["message"]
    st, _, raw = await request(port, f"GET /p/{TOKEN}/control.json HTTP/1.1\r\nHost: x\r\nX-Control-Key: {KEY}\r\n\r\n".encode()); assert st == 429  # the same client, the read too
    assert (await get_json(port, "control.json", client="10.9.8.7"))["enabled"]  # another client (X-Forwarded-For) is not locked
    st, j = await post(port, {"key": KEY, "action": "reset"}, headers="X-Forwarded-For: 10.9.8.7\r\n"); assert st == 200
    web.control_lock_until = {}  # the lock lapses
    st, j = await post(port, {"key": KEY, "action": "reset"}); assert st == 200
    # a request body over the limit, and one that lies about its length
    st, j = await post(port, json.dumps({"key": KEY, "action": "set", "values": {"SIM_SHARES": "9" * 20000}}).encode()); assert st == 400 and "过大" in j["message"]
    await web.stop()
    # without a key the page is served but every action is refused with the hint
    off_bot = m.Bot(m.Config.from_env(base), m.Store(":memory:"), FM(NOW), None)
    web2 = m.WebServer(off_bot, 0, TOKEN); web2.CACHE_SECONDS = {}; port2 = await web2.start()
    assert not (await get_json(port2, "control.json"))["enabled"]
    st, j = await post(port2, {"key": "anything-at-all", "action": "reset"}); assert st == 403 and "WEB_CONTROL_KEY" in j["message"]
    await web2.stop()
    # /web names the control page only when a key is set
    bot.web_token, bot.config = TOKEN, dataclasses.replace(bot.config, web_base="https://x.example")
    off_bot.web_token, off_bot.config = TOKEN, dataclasses.replace(off_bot.config, web_base="https://x.example")
    assert bot.cmd_web(None).endswith(f"https://x.example/p/{TOKEN}/control") and "/control" not in off_bot.cmd_web(None)

    # --- the live bot: its block on the page, the switches, settings reaching the trader ---------------------------------
    lstore = m.Store(":memory:")
    lbot = L.LiveBot(m.Config.from_env({**base, "WEB_CONTROL_KEY": KEY, "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "PREDICT_API_KEY": "k",
                                        "SIM_WAYS": "both", "SIM_MARKETS": "all"}), lstore, FM(NOW), None)
    fake = lbot.live.api = FakeApi()
    lbot.live.chain = FakeChain()
    await lbot.live_prepare()
    web3 = m.WebServer(lbot, 0, TOKEN); web3.CACHE_SECONDS = {}; port3 = await web3.start()
    data = await get_json(port3, "control.json")
    live = data["live"]
    assert data["mode"] == "live" and live["ready"] and live["paused"] == "" and live["killed"] == "" and live["orders"] == []
    assert [r["k"] for r in live["info"]][:2] == ["钱包", "API"] and live["info"][0]["v"].startswith("普通钱包，下单账户") and all({"k", "v", "tone"} <= set(r) for r in live["info"])
    assert live["lines"][0].startswith("钱包：普通钱包") and live["caps"] == {"order": 100, "open": 500, "daily_loss": 200} and live["exposure"] == 0
    assert live["account"] == "0x8fd3…7A03" and live["positions"] == [] and live["errors"] == []
    st, j = await post(port3, {"key": KEY, "action": "pause", "why": "维护"})
    assert st == 200 and "已暂停（维护）" in j["message"] and lbot.live_paused() == "维护" and (await get_json(port3, "control.json"))["live"]["paused"] == "维护"
    st, j = await post(port3, {"key": KEY, "action": "resume"}); assert st == 200 and lbot.live_paused() == ""
    st, j = await post(port3, {"key": KEY, "action": "set", "values": {"LIVE_MAX_OPEN_USD": "123", "LIVE_TAKER_WAIT_SECONDS": "30"}})
    assert st == 200 and lbot.config.live_max_open_usd == 123 and lbot.live.config.live_max_open_usd == 123 and lbot.live.config.live_taker_wait == 30
    assert (await get_json(port3, "control.json"))["live"]["caps"]["open"] == 123 and "30 秒未成交撤单" in (await get_json(port3, "control.json"))["live"]["lines"][-1]
    # a resting real order appears with its cancel switch; cancelling through the page reaches Predict
    trade = {"v": 2, "market": "x", "slug": "x", "market_id": "9", "item": "恒生指数", "kind": "close", "key": "HSI", "side": "up", "label": "挂涨",
             "maker": True, "fair": 0.70, "opened": NOW - 60_000, "settle": {}, "driver": "HSI@2026-10-05", "driver_name": "恒生指数",
             "order": 100.0, "fills": [], "revisions": [], "entry": {}, "version": {}, "price": 0.55, "signal": 0.15, "shares": 0.0,
             "status": "resting", "filled": None, "queue_ahead": 0.0, "queue_min": 0.0, "edge": 0.15,
             "live": {"state": "open", "order_id": "77", "hash": "0xh", "placed_at": NOW - 60_000, "attempt": 1, "events": [],
                      "want": {"maker": True, "fee_bps": 200}}}
    lbot.sim_save([("x|up|挂", trade)])
    fake.open["77"] = {"id": "77", "status": "OPEN", "amount": str(100 * WEI), "amountFilled": "0", "order": {"hash": "0xh"}}
    rows = (await get_json(port3, "control.json"))["live"]["orders"]
    assert len(rows) == 1 and rows[0]["cancellable"] and rows[0]["order_id"] == "77" and rows[0]["status"] == "挂单中（真实订单 #77 已成交 0/100 份）"
    assert rows[0]["price"] == 0.55 and rows[0]["order"] == 100 and rows[0]["maker"] and rows[0]["url"].startswith(m.PREDICT_SITE)
    st, j = await post(port3, {"key": KEY, "action": "cancel", "id": "99"}); assert st == 200 and "没有订单号为 99" in j["message"]
    st, j = await post(port3, {"key": KEY, "action": "cancel", "id": "77"})
    assert st == 200 and "策略挂单 1 笔：Predict 确认撤掉 1 笔" in j["message"] and ("remove", ["77"]) in fake.calls
    st, j = await post(port3, {"key": KEY, "action": "cancel", "id": "account"}); assert st == 200 and "没有真实挂单可撤" in j["message"], j  # the account-wide scope, nothing open now
    t = lbot.sim_trades()["x|up|挂"]
    assert t["status"] == "cancelled" and "管理员撤单" in t["note"] and t["live"]["state"] == "done"
    rows = (await get_json(port3, "control.json"))["live"]["orders"]
    assert rows[0]["cancellable"] is False and rows[0]["status"] == "已撤单"
    st, j = await post(port3, {"key": KEY, "action": "cancel", "id": "all"}); assert st == 200 and "没有真实挂单可撤" in j["message"]
    fake.positions_rows = [{"id": "p1", "market": {"id": 9, "title": "恒生指数"}, "outcome": {"name": "Up", "indexSet": 1}, "amount": str(3 * WEI)}]
    st, j = await post(port3, {"key": KEY, "action": "positions"}); assert st == 200 and "恒生指数｜Up 3 份" in j["message"]
    assert (await get_json(port3, "control.json"))["live"]["positions"] == ["恒生指数｜Up 3 份"]
    st, j = await post(port3, {"key": KEY, "action": "check"}); assert st == 200 and "自检" in j["message"]
    st, j = await post(port3, {"key": KEY, "action": "redeem"}); assert st == 200 and "没有可领取" in j["message"]
    st, j = await post(port3, {"key": KEY, "action": "bogus"}); assert st == 400 and j["message"] == "未知操作"
    st, j = await post(port3, {"key": KEY, "action": "mode", "value": "pause"})
    assert st == 200 and "on → pause" in j["message"] and lbot.config.live_mode == "pause" and (await get_json(port3, "control.json"))["live"]["mode"] == "pause"
    st, j = await post(port3, {"key": KEY, "action": "mode", "value": "nope"}); assert st == 400 and "用法" in j["message"]
    st, j = await post(port3, {"key": KEY, "action": "mode", "value": "on"}); assert st == 200 and lbot.config.live_mode == "on"
    assert {x["key"]: x["value"] for x in (await get_json(port3, "control.json"))["settings"]}["LIVE"] == "on"
    st, j = await post(port3, {"key": KEY, "action": "test"}); assert st == 200 and "没有可用的市场" in j["message"]  # no fresh two-sided book here
    assert (await get_json(port3, "control.json"))["live"]["last_test"] == ""
    lbot.sim_markets = lambda now: [m.SimMarket("hsi", "恒生指数", "close", "HSI", 0.7, m.PredictBook("HSI", "hsi", "9", "t", ((D("0.55"), D("300")),), ((D("0.58"), D("400")),), now),
                                                0.03, "", ("涨", "跌"), {})]
    fake.markets["9"] = {"id": 9, "status": "REGISTERED", "isNegRisk": False, "isYieldBearing": False, "feeRateBps": 200, "conditionId": "0x" + "ab" * 32,
                         "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": "91"}, {"name": "Down", "indexSet": 2, "onChainId": "92"}]}

    async def create_order(body):
        fake.open["500"] = {"id": "500", "status": "OPEN", "amount": body["data"]["order"]["takerAmount"], "amountFilled": "0", "order": {**body["data"]["order"]}}
        return {"order_id": "500", "hash": body["data"]["order"]["hash"], "code": None}
    fake.create_order = create_order
    st, j = await post(port3, {"key": KEY, "action": "test", "args": ["恒生", "2"]})
    assert st == 200 and "1/4 下单成功：订单 #500" in j["message"] and "3/4 撤单：已撤" in j["message"] and "2.0¢×50 份" in j["message"] and "2 份不足 Predict 最低订单金额 $1，改为 50 份" in j["message"], j
    assert (await get_json(port3, "control.json"))["live"]["last_test"] == j["message"]
    assert [(r["item"], r["kind"]) for r in (await get_json(port3, "control.json"))["checks"]] == [("恒生指数", "指数/个股日涨跌")]  # the order check lists the market
    # the Telegram command still works through the same code
    reply = await lbot.cmd_live(m.Request("/live", [], 1, 0, 1))
    assert isinstance(reply, m.Reply) and "真实交易" in reply.text
    assert "已暂停（看看）" in await lbot.cmd_live(m.Request("/live", ["pause", "看看"], 1, 0, 1)) and lbot.live_paused() == "看看"
    assert "▶️" in await lbot.cmd_live(m.Request("/live", ["resume"], 1, 0, 1))
    # a live bot on a store with saved settings starts with them, trader included
    lstore.put("control:env", {"LIVE_MAX_ORDER_USD": "33"})
    lbot2 = L.LiveBot(m.Config.from_env({**base, "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64}), lstore, FM(NOW), None)
    assert lbot2.config.live_max_order_usd == 33 and lbot2.live.config.live_max_order_usd == 33
    await web3.stop()
    print("CONTROL_OK")


asyncio.run(run())
