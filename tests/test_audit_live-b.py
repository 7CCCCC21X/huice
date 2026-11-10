"""Audit cluster live-b: regression tests for the verified findings fixed in this cluster (fakes only, offline)."""
import asyncio, pathlib, sys
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

# test_trading's fakes (FakeApi, make_bot, step, hsi …) without running its suites: only the definitions before "async def run()"
_src = (ROOT / "tests" / "test_trading.py").read_text(encoding="utf-8").split("\nasync def run():")[0]
T: dict = {"__file__": str(ROOT / "tests" / "test_trading.py")}
exec(compile(_src, "test_trading_head", "exec"), T)
make_bot, step, hsi, NOW, HSI_SLUG = T["make_bot"], T["step"], T["hsi"], T["NOW"], T["HSI_SLUG"]


async def live_stop_ok():
    """LIVE-STOP-OK: the emergency stop's ok is its cancels' verdict — a stop whose cancels failed is not reported as a success
    (the page showed a green toast), and its message no longer says "已有挂单保留"."""
    async def no_remove(ids): raise m.RemoteError("HTTP 500: boom")
    for fail_list in (True, False):
        bot = make_bot(SIM_WAYS="maker")
        await bot.live_prepare(); await bot.live_prefetch(NOW)
        await step(bot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
        if fail_list:
            bot.live.api.fail_open = "HTTP 500: boom"
        bot.live.api.remove_orders = no_remove
        r = await bot.live_control("stop", {})
        assert not r["ok"], r
        assert bot.live_paused() == "紧急停止" and "已有挂单保留" not in r["message"] and "❌" in r["message"], r
    # still ok: a successful stop, and a stop with nothing to cancel
    bot = make_bot(SIM_WAYS="maker")
    await bot.live_prepare(); await bot.live_prefetch(NOW)
    await step(bot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    r = await bot.live_control("stop", {})
    assert r["ok"] and "Predict 确认撤掉 1 笔" in r["message"] and "已有挂单保留" not in r["message"], r
    bot = make_bot(SIM_WAYS="maker")
    await bot.live_prepare()
    r = await bot.live_control("stop", {})
    assert r["ok"] and "没有真实挂单可撤" in r["message"] and bot.live_paused() == "紧急停止", r


asyncio.run(live_stop_ok())

async def live_4xx_filled():
    """LIVE-4XX-FILLED: a bare "HTTP 409: Conflict" on the order POST is Predict holding that hash (a duplicate without the
    wording): the record stays "result unknown" with its hash and the sync adopts the order — a taker Predict took and filled
    at once used to be failed and freed (no order id, so never looked at again: 100 real shares outside exposure and P&L).
    Other 4xx answers are still definite refusals."""
    tid = f"{HSI_SLUG}|up|吃"
    mkt = lambda at: hsi(0.70, [("0.55", "300")], [("0.58", "400")], at)
    bot = make_bot(SIM_WAYS="taker")
    await bot.live_prepare(); await bot.live_prefetch(NOW)
    fake = bot.live.api
    real_create = fake.create_order
    async def taken_then_409(body):
        got = await real_create(body)
        fake.fill(got["order_id"], 100, done=True, price=0.58)
        raise m.RemoteError("HTTP 409: Conflict")
    fake.create_order = taken_then_409
    await step(bot, NOW, mkt(NOW))
    t = bot.sim_trades()[tid]
    # kept "result unknown" with its hash (never failed), and the same step's sync adopts it by that hash
    assert t["live"].get("final") != "failed" and t["live"]["hash"] and any("下单请求结果未知" in e["what"] for e in t["live"]["events"]), t["live"]
    fake.create_order = real_create
    for k in range(1, 8):
        await step(bot, NOW + k * 10_000, mkt(NOW + k * 10_000))
    rows = {k: v for k, v in bot.sim_trades().items() if k.startswith(tid)}
    filled = [v for v in rows.values() if v["status"] == "filled"]
    assert len(filled) == 1 and filled[0]["shares"] == 100 and filled[0]["live"].get("order_id"), {k: (v["status"], v.get("shares"), v["live"].get("final")) for k, v in rows.items()}
    assert bot.live_exposure(bot.sim_trades()) > 0
    assert len([c for c in fake.calls if c[0] == "create"]) == 1, "never sent again"
    # a 400 is still a definite refusal: failed and backed off
    bot = make_bot(SIM_WAYS="taker")
    await bot.live_prepare(); await bot.live_prefetch(NOW)
    bot.live.api.fail_create = "HTTP 400: insufficient balance"
    await step(bot, NOW, mkt(NOW))
    t = bot.sim_trades()[f"{tid}#1"]
    assert t["status"] == "cancelled" and t["live"]["final"] == "failed" and "下单失败：HTTP 400" in t["note"], t["live"]


asyncio.run(live_4xx_filled())

async def live_yield_allowance():
    """LIVE-YIELD-ALLOWANCE: the yield-bearing exchanges' USDT allowances are read, gate live_room, show on the page, and
    --approve sets them. Before: only the two standard keys were read, so an order signed for YIELD_BEARING_CTF_EXCHANGE
    passed the gate with no allowance, and the page showed every approval ✓."""
    import live as L
    class YieldlessChain(T["FakeChain"]):
        async def allowance(self, key, owner=None): return 0 if key.startswith("YIELD") else self.allow
    bot = make_bot(SIM_WAYS="taker")
    bot.live.chain = YieldlessChain()
    await bot.live_prepare()
    assert bot.live.approvals == {"CTF_EXCHANGE": True, "NEG_RISK_CTF_EXCHANGE": True,
                                  "YIELD_BEARING_CTF_EXCHANGE": False, "YIELD_BEARING_NEG_RISK_CTF_EXCHANGE": False}, bot.live.approvals
    mk = hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)
    # a standard market still passes; the same market flagged yield-bearing is refused, naming the track
    await bot.live_prefetch(NOW)
    assert bot.live_room({}, mk, "up", 0.58, 100) == ""
    bot.live.markets["101"].yield_bearing = True
    r = bot.live_room({}, mk, "up", 0.58, 100)
    assert r.startswith("未授权收益型交易所使用 USDT"), r
    # the page: the standard track stays ok, the yield-bearing one shows ✗ with a warn tone (not bad: not every account trades it)
    row = [x for x in bot.live.summary_info() if x["k"] == "授权"][0]
    assert "收益型交易所 ✗" in row["v"] and "交易所 USDT 额度 ✓" in row["v"] and row["tone"] == "warn", row
    line = [x for x in bot.live.summary_lines() if x.startswith("授权")][0]
    assert "收益型 ✗" in line and "收益型市场不会真实下单" in line, line
    bot.live.chain = T["FakeChain"]()
    await bot.live.refresh_approvals()
    row = [x for x in bot.live.summary_info() if x["k"] == "授权"][0]
    assert row["tone"] == "ok" and "收益型交易所 ✓" in row["v"], row
    # --approve sets both tracks (and --live-check reads both)
    calls = []
    class ApproveChain:
        async def bnb_balance(self, owner=None): return 10 ** 17
        async def set_approvals(self, yb=False, log=None):
            calls.append(("set", yb)); return [("x", "已授权")]
        async def check_approvals(self, yb=False):
            calls.append(("check", yb)); return [("x", True)]
    real_trader = L.LiveTrader
    class FakeTrader(real_trader):
        def __init__(self, config):
            super().__init__(config); self.chain = ApproveChain()
    L.LiveTrader = FakeTrader
    try:
        assert await L.live_approve(bot.config) == 0
        assert [c for c in calls if c[0] == "set"] == [("set", False), ("set", True)], calls
    finally:
        L.LiveTrader = real_trader


asyncio.run(live_yield_allowance())

async def live_double_rekey():
    """LIVE-DOUBLE-REKEY: sim_decide takes its copy of the records, then yields once (sleep(0)). An express placement whose
    store flush returns in that slice fails its gate (decision 91 s old) -> live_fail -> sim_rekey(slot -> slot#1); the step
    then found the record still under the slot in its stale copy, spent, and re-keyed it AGAIN (slot -> slot#2): one
    attempt stored twice. sim_rekey now returns the key the record already moved to."""
    import live as L, types
    bot = make_bot(SIM_WAYS="maker")
    await bot.live_prepare(); await bot.live_prefetch(NOW)
    release = asyncio.Event()
    async def slow_to_thread(fn, *a, **k):  # the store flush the placement waits for (a busy writer)
        await release.wait()
        return fn(*a, **k)
    real_asyncio = L.asyncio
    L.asyncio = types.SimpleNamespace(**{n: getattr(asyncio, n) for n in dir(asyncio) if not n.startswith("__")})
    L.asyncio.to_thread = slow_to_thread
    try:
        bot.market.now = NOW
        T["world"]["markets"] = [hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)]
        express = asyncio.create_task(bot.sim_express("HSI"))  # decides the maker, sends it: waits in the flush
        for _ in range(500):
            if bot.live_placing:
                break
            await asyncio.sleep(0.01)
        assert bot.live_placing, "the express placement never reached the flush"
        tid = next(iter(bot.live_placing))
        later = NOW + 91_000  # the decision is too old when the flush returns
        bot.market.now = later
        T["world"]["markets"] = [hsi(0.70, [("0.55", "300")], [("0.58", "400")], later)]
        release.set()  # the placement task resumes at the step's next yield: sim_decide's sleep(0)
        async with bot.sim_lock:
            confirm = await bot.sim_decide(later)
        await confirm
        await express
    finally:
        L.asyncio = real_asyncio
    keys = sorted(k for k in bot.store.keys("sim:") if k.startswith(f"sim:{tid}"))
    assert f"sim:{tid}#2" not in keys and f"sim:{tid}#2" not in [f"sim:{k}" for k in bot.sim_trades()], keys
    one = bot.store.get(f"sim:{tid}#1")
    assert one is not None and one["live"].get("final") == "failed" and one["live"].get("rekeyed") == f"{tid}#1", (keys, one and one["live"])
    assert any("决定已过时" in e["what"] for e in one["live"]["events"]), one["live"]["events"]


asyncio.run(live_double_rekey())


async def live_auth_singleflight():
    """LIVE-AUTH-SINGLEFLIGHT: the sign-in is single-flight — concurrent callers that find no fresh token sign in once, and
    concurrent requests refused with a 401 on a shared token sign in once again and all succeed (each used to blank the token,
    whoever had just obtained it, and sign in on its own)."""
    import live as L, base64, json as J

    def token(n):
        return "x." + base64.urlsafe_b64encode(J.dumps({"exp": 1800000000, "n": n}).encode()).decode().rstrip("=") + ".y"

    class StubWallet:
        maker = "0x8fd379246834eac74B8419FfdA202CF8051F7A03"
        def sign_text(self, message): return "sig:" + message

    def make(revoke_old: bool):
        api = L.PredictApi(m.PREDICT_REST, "key-1", StubWallet())
        st = {"signins": 0, "valid": set(), "no_auth": 0, "orders": 0}

        async def send(method, path, payload, timeout):
            auth = api.headers().get("Authorization", "")  # read as the request leaves, like PredictApi.send
            await asyncio.sleep(0.01)  # in flight long enough for the others to join
            if path == "/auth/message":
                return J.dumps({"success": True, "data": {"message": "Sign in\nNonce: 1"}}).encode()
            if path == "/auth":
                st["signins"] += 1
                new = token(st["signins"])
                st["valid"] = {new} if revoke_old else st["valid"] | {new}
                return J.dumps({"success": True, "data": {"token": new}}).encode()
            if not auth:
                st["no_auth"] += 1
            if auth.removeprefix("Bearer ") not in st["valid"]:
                raise m.RemoteError("HTTP 401: expired")
            st["orders"] += 1
            return J.dumps({"success": True, "data": {"ok": True}}).encode()
        api.send = send
        return api, st

    # cold start: five concurrent ensure_auth → one sign-in
    api, st = make(False)
    await asyncio.gather(*(api.ensure_auth() for _ in range(5)))
    assert st["signins"] == 1 and api.jwt == token(1), st
    # a token the server refuses, shared by five requests in flight → one re-sign-in, all five succeed
    for revoke_old in (False, True):
        api, st = make(revoke_old)
        api.jwt, api.jwt_exp = "stale", 1800000000
        out = await asyncio.gather(*(api.authed("POST", "/orders", {}, priority=True) for _ in range(5)), return_exceptions=True)
        assert all(isinstance(r, dict) and r.get("success") for r in out), (revoke_old, out, st)
        assert st["signins"] == 1 and st["orders"] == 5 and st["no_auth"] == 0, (revoke_old, st)
    # authenticate() (the background renewal) still signs in itself, queued behind a sign-in already running
    api, st = make(False)
    await asyncio.gather(api.ensure_auth(), api.authenticate())
    assert st["signins"] == 2 and api.jwt == token(2), st


asyncio.run(live_auth_singleflight())

async def live_notify_priority():
    """LIVE-NOTIFY-PRIORITY: the unkeyed per-order / per-fill notices (📤 ✅ 💵 ℹ️) were one Telegram message each, and
    Telegram paces a chat at about one message per 1.1 s: a burst of them queued every critical keyed alert (🛑 kill switch,
    ❓ unknown order …) behind all of them. Now they are coalesced per chat into one batch message at a time, so the kill alert
    goes out among the first messages and nothing is lost."""
    class SlowTelegram:  # one message at a time per chat, each takes a while (Telegram.paced's per-chat lock and gap)
        def __init__(self):
            self.sent, self.lock = [], asyncio.Lock()
        async def send(self, chat, thread, text, reply_markup=None, parse_mode=None):
            async with self.lock:
                await asyncio.sleep(0.01)
                self.sent.append((chat, thread, text))
    bot = make_bot()
    bot.telegram = tg = SlowTelegram()
    bot.store.put("subscriptions", {"7:0": {"chat": 7, "thread": 0, "active": True},
                                    "8:0": {"chat": 8, "thread": 0, "active": False}})
    assert bot.config.live_notify
    for k in range(50):
        bot.live_notify(f"📤 order {k}")
        await asyncio.sleep(0)
    bot.live_notify("🛑 KILL switch", key="kill")
    await bot.drain_deliveries()
    texts = [t for chat, _, t in tg.sent]
    assert all(chat == 7 for chat, _, _ in tg.sent), tg.sent  # an inactive subscription gets nothing
    kill_at = next(i for i, t in enumerate(texts) if "KILL" in t)
    assert kill_at < 3, (kill_at, len(texts))
    assert len(texts) <= 5, len(texts)  # far fewer than 51 messages
    joined = "\n\n".join(t for t in texts if "KILL" not in t)
    assert all(f"📤 order {k}" in joined for k in range(50)), joined  # every notice still delivered, in order
    assert joined.index("📤 order 3") < joined.index("📤 order 49")
    assert not bot.live_note_buffer, bot.live_note_buffer
    # a notice after the batch finished starts a new one
    bot.live_notify("✅ filled 9")
    await bot.drain_deliveries()
    assert tg.sent[-1][2] == "✅ filled 9", tg.sent[-1]
    # a flood beyond the buffer's cap keeps the newest and says how many were left out
    tg.sent.clear()
    bot.telegram = tg
    for k in range(bot.LIVE_NOTE_BUFFER + 30):
        bot.live_notify(f"📤 flood {k}")  # no yield: all of them land before the batch takes the buffer
    await bot.drain_deliveries()
    assert len(tg.sent) == 1 and "另有 30 条" in tg.sent[0][2] and "📤 flood 79" in tg.sent[0][2] and "📤 flood 29\n" not in tg.sent[0][2], tg.sent
    # a keyed notice is still deduplicated for LIVE_NOTE_SECONDS
    tg.sent.clear()
    bot.live_notify("🛑 KILL switch", key="kill")
    await bot.drain_deliveries()
    assert not tg.sent, tg.sent


asyncio.run(live_notify_priority())
async def live_send_after_pause():
    """LIVE-SEND-AFTER-PAUSE: the last check (live_send_gate) ran before create_order's wait for a request slot; an order held
    there (the client's five slots busy with three reads and two cancels) went out after /live pause, and after a withdrawal it
    went out too (cancelled only once its answer came — a taker may have filled). Now it is judged again inside the slot: no
    POST /orders reaches the transport, the record never gets an order id. The real PredictApi over a fake transport."""
    import json as _json, threading, time as _time
    L = T["L"]
    tid = f"{HSI_SLUG}|up|吃"
    real_get = m._http_get
    for how in ("pause", "withdraw", "none"):
        held = {"reads": threading.Event(), "writes": threading.Event()}
        posts = []

        def transport(url, payload=None, timeout=15, headers=None):
            path = url.replace(L.API_BASES[56], "")
            if path.startswith("/orders?"):
                held["reads"].wait(5)
                return _json.dumps({"success": True, "data": [], "cursor": None}).encode()
            if path == "/orders/remove":
                held["writes"].wait(5)
                return _json.dumps({"success": True, "removed": [], "noop": payload["data"]["ids"]}).encode()
            if path == "/orders":
                posts.append(payload["data"]["order"]["hash"])
                return _json.dumps({"success": True, "data": {"orderId": "555", "orderHash": payload["data"]["order"]["hash"]}}).encode()
            raise AssertionError(path)

        bot = make_bot(SIM_WAYS="taker")
        await bot.live_prepare()
        mk = hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)
        T["world"]["markets"] = [mk]
        await bot.live_prefetch(NOW)
        async with bot.sim_lock:  # a decision (as the step / the express lane make it), not sent yet
            trades, changed = bot.sim_trades(), []
            bot.sim_take(mk, trades, NOW, changed)
            bot.sim_save(changed)
        assert bot.sim_trades()[tid]["live"]["state"] == "pending"
        api = L.PredictApi(L.API_BASES[56], "k", bot.live.wallet)
        api.jwt, api.jwt_exp = "jwt", int(_time.time()) + 3600
        bot.live.api = api
        m._http_get = transport
        try:
            busy = [asyncio.create_task(api.orders("OPEN")) for _ in range(3)] + [asyncio.create_task(api.remove_orders([str(i)])) for i in range(2)]
            await asyncio.sleep(0.05)
            place = asyncio.create_task(bot.live_place_pending(NOW))
            await asyncio.sleep(0.2)
            trade = bot.sim_trades()[tid]
            assert trade["live"]["state"] == "placing" and not posts, (trade["live"]["state"], posts)  # past every gate, waiting for a slot
            if how == "pause":
                await bot.live_action("pause", ["测试暂停"], NOW)
            elif how == "withdraw":
                async with bot.sim_lock:
                    bot.sim_withdraw(trade, "测试撤单", NOW)
                    bot.sim_save([(tid, trade)])
            held["reads"].set(); held["writes"].set()
            await asyncio.gather(place, *busy)
        finally:
            m._http_get = real_get
        rows = {k: v for k, v in bot.sim_trades().items() if k.startswith(tid)}
        if how == "none":  # nothing changed meanwhile: sent as before
            assert len(posts) == 1 and any(v["live"].get("order_id") == "555" and v["live"]["state"] == "open" for v in rows.values()), {k: v["live"]["state"] for k, v in rows.items()}
            continue
        assert not posts, (how, posts)
        assert all(not v["live"].get("order_id") and v["live"]["state"] not in {"open", "placing", "cancelling", "unknown"} for v in rows.values()), \
            {k: (v["live"]["state"], v["live"].get("order_id")) for k, v in rows.items()}
        done = [v for v in rows.values() if v["live"]["state"] == "done"]
        assert len(done) == 1 and not done[0]["live"]["hash"], {k: v["live"] for k, v in rows.items()}
        if how == "pause":
            assert done[0]["live"]["final"] == "failed" and "真实交易已暂停" in done[0]["note"], done[0]["live"]
        else:
            assert done[0]["live"]["final"] == "cancelled" and "这张单没有发出" in done[0]["live"]["events"][-1]["what"], done[0]["live"]


asyncio.run(live_send_after_pause())


async def live_unknown_open_block():
    """LIVE-UNKNOWN-OPEN-BLOCK: any unrecorded open order on the account held the 1.104.9 evidence release of every unknown
    record — a manual order on a market this bot never touched (never swept) kept a daily maker's side blocked until the close
    plus two hours. Now only an unrecorded order that could be this record's (same market, same token, or a row that says
    neither) holds it; one on another market and another token does not."""
    async def _none(*a, **k): return None
    async def _empty(*a, **k): return []
    async def _noop(ids): return {"removed": [], "noop": list(ids)}
    tid = f"{HSI_SLUG}|up|挂"
    mkt = lambda at: hsi(0.70, [("0.55", "300")], [("0.58", "400")], at)
    for ghost in ("other", "same-market", "same-token", "blind"):
        bot = make_bot(SIM_WAYS="maker")
        await bot.live_prepare(); fake = bot.live.api
        await bot.live_prefetch(NOW)
        await step(bot, NOW, mkt(NOW))
        fake.orders, fake.order, fake.remove_orders = _empty, _none, _noop
        for at in (NOW + 10_000, NOW + 20_000, NOW + 30_000, NOW + 65_000):
            await step(bot, at, mkt(at))
        t = bot.sim_trades()[tid]
        assert t["live"]["state"] == "unknown", (ghost, t["live"])
        want = t["live"]["want"]
        row = {"id": "777", "status": "OPEN", "amount": "1", "amountFilled": "0", "order": {"hash": "0xabc"}}
        if ghost == "other":
            row = {**row, "market": {"id": 999}, "order": {"hash": "0xabc", "tokenId": "99991"}}
        elif ghost == "same-market":
            row = {**row, "market": {"id": 101}, "order": {"hash": "0xabc", "tokenId": "99991"}}
        elif ghost == "same-token":
            row = {**row, "order": {"hash": "0xabc", "tokenId": str(want["token"])}}
        async def ghost_list(status="OPEN", **params): return [dict(row)]
        fake.orders = ghost_list
        for k in range(1, 8):  # well past LIVE_UNKNOWN_LOOKS complete lists and LIVE_UNKNOWN_SETTLE_MS, long before the expiry
            at = NOW + 65_000 + k * 60_000
            await step(bot, at, mkt(at))
        t = bot.sim_trades()[tid]
        assert [u["id"] for u in bot.live_unknown_open] == ["777"], (ghost, bot.live_unknown_open)
        if ghost == "other":  # cannot be this record's order: the evidence releases it, the side quotes again
            first = bot.sim_trades().get(f"{tid}#1") or t
            assert first["status"] == "cancelled" and first["live"]["state"] == "done", (ghost, first["live"], sorted(bot.sim_trades()))
            assert any("按未成交结束，这一边可以再挂" in e["what"] for e in first["live"]["events"]), first["live"]["events"]
        else:  # may be this very order: still held for an operator
            assert t["live"]["state"] == "unknown" and bot.live_reserved(t) > 0 and "没有记录的挂单（#777）" in t["live"]["last_error"], (ghost, t["live"])
            assert f"{tid}#1" not in bot.sim_trades(), (ghost, sorted(bot.sim_trades()))


asyncio.run(live_unknown_open_block())
async def live_pnl_display():
    """LIVE-PNL-DISPLAY: /live's 今日已结算盈亏 and the page's 今日 (live_status day_pnl) show the sum the day's stop compares
    against LIVE_MAX_DAILY_LOSS_USD (the model trader's day plus the pre-open mode's, 1.125.0), not only the model trader's part;
    and the pre-open status in /live is escaped once (by Bot.tell's to_html), not twice."""
    import types
    bot = make_bot()
    await bot.live_prepare()
    bot.live_daily_pnl = lambda trades, now_ms: -15.0
    bot.preopen = types.SimpleNamespace(alive=True, risk=lambda: 0.0, daily_pnl=lambda: -40.0, status=lambda: "预挂 最近问题：HTTP 400 a & <b>",
                                        filled_by_outcome=lambda days=3: {}, ledger=types.SimpleNamespace(rows={}))
    text = bot.live_text(NOW)
    assert "今日已结算盈亏 −$55.00（原策略 −$15.00 + 预挂 −$40.00）（上限 −$50）" in text, [l for l in text.split("\n") if "今日已结算盈亏" in l]
    st = bot.live_status(NOW)
    assert st["day_pnl"] == -55.0 and st["daily_pnl"] == -15.0 and st["preopen_pnl"] == -40.0, (st["day_pnl"], st["daily_pnl"], st["preopen_pnl"])
    assert "HTTP 400 a & <b>" in text, [l for l in text.split("\n") if "HTTP 400" in l]
    out = m.to_html(text)  # what Bot.tell sends for the Reply(html=True)
    assert "HTTP 400 a &amp; &lt;b&gt;" in out and "&amp;amp;" not in out and "&amp;lt;" not in out, [l for l in out.split("\n") if "HTTP 400" in l]
    # a pre-open mode that is not running (crashed: alive False) does not count: the line keeps its old shape, no split
    bot.preopen.alive = False
    text = bot.live_text(NOW)
    assert "今日已结算盈亏 −$15.00（上限 −$50）" in text and "原策略" not in text, [l for l in text.split("\n") if "今日已结算盈亏" in l]
    assert bot.live_status(NOW)["day_pnl"] == -15.0


asyncio.run(live_pnl_display())
async def live_orphan_known():
    """LIVE-ORPHAN-KNOWN: a market the bot only priced (live_prefetch / /live test read its details into the cache, no order,
    no record) is not one of its own markets: the operator's manual order resting there is only reported as an unrecorded
    order, never withdrawn by the orphan sweep (it was withdrawn after LIVE_ORPHAN_GRACE_MS)."""
    WEI = 10 ** 18
    bot = make_bot(SIM_WAYS="taker")
    await bot.live_prepare()
    fake = bot.live.api
    await bot.live.market("101")  # what live_prefetch does for a card near the bar: details cached, nothing placed
    assert "101" in bot.live.markets and not any(isinstance(t.get("live"), dict) for t in bot.sim_trades().values())
    fake.open["900"] = {"id": "900", "status": "OPEN", "amount": str(50 * WEI), "amountFilled": "0", "market": {"id": "101"},
                        "order": {"hash": "0xmanual", "side": 0, "tokenId": "1011", "makerAmount": str(20 * WEI), "takerAmount": str(50 * WEI)}}
    assert bot.live_market_of(fake.open["900"]) == "", bot.live_known_markets()
    for t in (NOW + 60_000, NOW + 130_000, NOW + 200_000, NOW + 330_000):  # well past the grace and the sweep interval
        await step(bot, t, hsi(0.70, [("0.50", "100")], [("0.95", "100")], t))
    assert not any(k == "remove" and "900" in ids for k, ids in fake.calls), fake.calls
    assert "900" in fake.open and not any(o.get("id") == "900" for o in bot.live_orphans), bot.live_orphans
    assert [u["id"] for u in bot.live_unknown_open] == ["900"], bot.live_unknown_open  # still reported


asyncio.run(live_orphan_known())
print("AUDIT_LIVE_B_OK")
