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
print("AUDIT_LIVE_B_OK")
