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
print("AUDIT_LIVE_B_OK")
