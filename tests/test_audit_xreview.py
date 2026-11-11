"""Cross-cluster review (audit round 5): regression tests for the findings fixed on the integrated branch (fakes only, offline)."""
import asyncio, pathlib, sys
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L

# test_trading's fakes (FakeApi, make_bot, step, hsi …) without running its suites: only the definitions before "async def run()"
_src = (ROOT / "tests" / "test_trading.py").read_text(encoding="utf-8").split("\nasync def run():")[0]
T: dict = {"__file__": str(ROOT / "tests" / "test_trading.py")}
exec(compile(_src, "test_trading_head", "exec"), T)
make_bot, step, hsi, NOW, HSI_SLUG, world = T["make_bot"], T["step"], T["hsi"], T["NOW"], T["HSI_SLUG"], T["world"]


async def hash_read_error_409():
    """XR-409-HASH-ERROR: a bare 409 on the order POST is "result unknown" (LIVE-4XX-FILLED): the record stays "placing" with its
    hash. When Predict never took the order and the read by that hash keeps raising (a 4xx other than 404, success=false, a 5xx),
    live_reconcile — and live_cancel's hash-only branch for one withdrawn meanwhile — aborted on the error before the grace test,
    every step: the record stayed placing / cancelling for good, $55 reserved and its side blocked, no ❓ alert, out of /live
    release's reach (it takes "unknown" only) and of live_recheck's expiry backstop (LIVE-RECHECK-ERROR). Past the placing grace the
    error now makes it "unknown" as a "no such order" answer does: still reserved, never re-sent, released by an operator or by the
    positions once its expiry plus the margin has passed — never sooner."""
    card = lambda at: hsi(0.70, [("0.55", "300")], [("0.58", "400")], at)

    async def placed(withdraw):
        bot = make_bot(SIM_WAYS="maker")
        bot.live_market_filter = False
        await bot.live_prepare()
        api = bot.live.api
        world["markets"] = [card(NOW)]
        await bot.live_prefetch(NOW)

        async def conflict(body):  # a bare 409 for an order Predict did not take
            api.calls.append(("create", body))
            raise m.RemoteError("HTTP 409: Conflict")
        api.create_order = conflict

        async def read(oid, hash_=""):
            raise m.RemoteError("HTTP 400: invalid order hash")
        api.order = read
        await step(bot, NOW, card(NOW))
        tid = f"{HSI_SLUG}|up|挂"
        key = next(k for k in bot.sim_trades() if k.startswith(tid))
        t = bot.sim_trades()[key]
        assert t["live"]["state"] == "placing" and t["live"].get("hash"), t["live"]
        reserved = bot.live_reserved(t)
        assert reserved > 0, reserved
        if withdraw:
            bot.sim_withdraw(t, "测试撤单", NOW + 30_000)
        await step(bot, NOW + 60_000, card(NOW + 60_000))
        # inside the grace the error is no evidence either way: it propagates as before (one() notes it), nothing moves
        assert t["live"]["state"] == ("cancelling" if withdraw else "placing"), t["live"]["state"]
        assert "HTTP 400" in str(t["live"].get("last_error")), t["live"].get("last_error")
        for k in range(3, 6):
            await step(bot, NOW + k * 60_000, card(NOW + k * 60_000))
        assert t["live"]["state"] == "unknown", (withdraw, t["live"]["state"], t["live"].get("last_error"))
        assert abs(bot.live_reserved(t) - reserved) < 1e-9, (bot.live_reserved(t), reserved)  # nothing released on the error
        assert len([c for c in api.calls if c[0] == "create"]) == 1  # never re-sent
        return bot, key, t, reserved

    for withdraw in (False, True):
        bot, key, t, reserved = await placed(withdraw)
        at = NOW + 6 * 60_000
        out = bot.live_release_command([key.lower()], at)
        assert out.startswith("已释放"), out
        assert t["live"]["state"] == "done" and bot.live_reserved(t) == 0, (t["live"]["state"], bot.live_reserved(t))

    # left alone: held while the order could still fill, freed by the positions once its expiry plus the margin has passed
    bot, key, t, reserved = await placed(False)
    deadline = bot.live_order_deadline(t)
    assert deadline > NOW
    edge = deadline + bot.LIVE_UNKNOWN_MARGIN_MS
    for at in range(NOW + 3_600_000, edge - 60_000, 3_600_000):
        await step(bot, at, card(at))
    await step(bot, edge - 60_000, card(edge - 60_000))
    assert t["live"]["state"] == "unknown" and abs(bot.live_reserved(t) - reserved) < 1e-9, (t["live"]["state"], bot.live_reserved(t))
    for k in range(0, 4):
        await step(bot, edge + k * 60_000, card(edge + k * 60_000))
    assert t["live"]["state"] == "done" and bot.live_reserved(t) == 0, (t["live"]["state"], bot.live_reserved(t), t["live"].get("last_error"))


async def sendrefused_reconcile():
    """XR-SENDREFUSED-RECONCILE: an order waited past LIVE_PLACING_GRACE_MS for its request slot; the step's sync reads it by its
    hash (live_reconcile while "placing", live_cancel's hash-only branch once withdrawn) and, while that read is on its way, the
    in-slot check refuses it (a pause, a withdrawal: LIVE-SEND-AFTER-PAUSE) and the record ends unsent. The read answers "no such
    order": the sync used to turn the ended record "unknown" — $ reserved, the side held, a false ❓ alert. "stale": no pause and
    no withdrawal, the in-slot check refuses the decision as too old after the long wait — the same end, the same race."""
    tid = f"{HSI_SLUG}|up|吃"

    async def run(how):  # how: pause / withdraw (during the read) / withdrawn (before the sync) / stale (nothing: the decision aged)
        bot = make_bot(SIM_WAYS="taker")
        await bot.live_prepare()
        api = bot.live.api
        mk = hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)
        world["markets"] = [mk]
        await bot.live_prefetch(NOW)
        async with bot.sim_lock:
            trades, changed = bot.sim_trades(), []
            bot.sim_take(mk, trades, NOW, changed)
            bot.sim_save(changed)
        slot, in_slot, read_out, read_go = asyncio.Event(), asyncio.Event(), asyncio.Event(), asyncio.Event()
        real_create, real_order, sent = api.create_order, api.order, []

        async def create(body):  # the wait for the client's request slot, then the check PredictApi.send makes there
            in_slot.set()
            await slot.wait()
            check = L.ORDER_SEND_CHECK.get()
            if check is not None and (why := check()):
                raise L.SendRefused(why)
            sent.append(body)
            return await real_create(body)

        async def order(oid, hash_=""):  # the sync's read by the hash: asked before Predict had anything, answered later
            if not oid and not read_out.is_set():
                read_out.set()
                await read_go.wait()
                return None
            return await real_order(oid, hash_)
        api.create_order, api.order = create, order
        place = asyncio.create_task(bot.live_place_pending(NOW))
        await asyncio.wait_for(in_slot.wait(), 5)
        t = bot.sim_trades()[tid]
        at = NOW + bot.LIVE_PLACING_GRACE_MS + 5_000
        bot.market.now = at
        withdraw = lambda: (bot.sim_withdraw(t, "测试撤单", at), t["withdrawn"].update(rule=True), bot.sim_save([(tid, t)]))
        if how == "withdrawn":
            async with bot.sim_lock:
                withdraw()
        sync = asyncio.create_task(bot.live_sync_orders({tid: t}, at))
        await asyncio.wait_for(read_out.wait(), 5)
        if how == "pause":
            await bot.live_action("pause", ["测试暂停"], at)
        elif how == "withdraw":
            async with bot.sim_lock:
                withdraw()
        slot.set()
        await place
        after = (t["live"]["state"], t["live"].get("order_id"))
        read_go.set()
        await sync
        return bot, t, sent, after

    for how in ("pause", "withdraw", "withdrawn", "stale"):
        bot, t, sent, after = await run(how)
        st = t["live"]
        assert not sent and after[0] == "done", (how, after)
        assert st["state"] == "done" and bot.live_reserved(t) == 0, (how, st["state"], bot.live_reserved(t), st["events"][-1])
        assert not any("状态未知" in e["what"] for e in st["events"]), (how, st["events"])


asyncio.run(hash_read_error_409())
asyncio.run(sendrefused_reconcile())
print("AUDIT_XREVIEW_OK")
