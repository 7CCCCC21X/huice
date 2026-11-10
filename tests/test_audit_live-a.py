"""Audit cluster live-a: regression tests for the verified findings fixed in this cluster (fakes only, offline)."""
import asyncio, pathlib, sys, time
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

# test_trading's fakes (FakeApi, make_bot, step, hsi …) without running its suites: only the definitions before "async def run()"
_src = (ROOT / "tests" / "test_trading.py").read_text(encoding="utf-8").split("\nasync def run():")[0]
T: dict = {"__file__": str(ROOT / "tests" / "test_trading.py")}
exec(compile(_src, "test_trading_head", "exec"), T)
make_bot, step, hsi, NOW, HSI_SLUG, world = T["make_bot"], T["step"], T["hsi"], T["NOW"], T["HSI_SLUG"], T["world"]


async def place_timeout_race():
    """LIVE-PLACE-TIMEOUT-RACE: an express-lane placement (its own task, sent outside sim_lock) that Predict processed but whose
    answer is lost (the HTTP layer's 20 s timeout) while a whole step runs beside it — the step's sync finds the order by its
    hash meanwhile (live_reconcile, or live_cancel for one withdrawn in flight). The answer's error says nothing about an order
    Predict has: the record keeps what the sync found. It used to be failed (live_fail): a filled taker booked "cancelled" with
    every share unfilled at slot#1 (outside the share caps), a resting maker untracked and unreserved, the side backed off with a
    "下单失败" alert — and, once the backoff ran out, a second real order on the side."""
    mkt = lambda at: hsi(0.70, [("0.55", "300")], [("0.58", "400")], at)
    timeout = lambda: m._net_error(TimeoutError("timed out"))  # what the HTTP layer raises when POST /orders gets no answer
    creates = lambda bot: len([c for c in bot.live.api.calls if c[0] == "create"])

    async def in_flight(ways, fill=0.0, withdraw="", sync=True, processed=True, error=timeout, **env):
        bot = make_bot(SIM_WAYS=ways, **env)
        world["markets"] = [mkt(NOW)]  # the cards the prefetch and the express lane price (make_bot's sim_markets)
        await bot.live_prepare(); await bot.live_prefetch(NOW)
        fake = bot.live.api
        real_create, sent, gate = fake.create_order, asyncio.Event(), asyncio.Event()

        async def answer_lost(body):  # Predict takes the order (a taker fills at once, a maker may fill a part); the answer never comes
            got = await real_create(body) if processed else None
            if got and fill:
                fake.fill(got["order_id"], fill, done=fill >= 100, price=0.58)
            sent.set()
            await gate.wait()
            raise error()
        fake.create_order = answer_lost
        bot.market.now = NOW
        lane = asyncio.create_task(bot.sim_express("HSI"))
        await asyncio.wait_for(sent.wait(), 5)
        tid = next(iter(bot.live_placing))
        if withdraw:  # withdrawn while the request is in flight, as the loss stop (live_check_kill) and /live cancel do
            rec = bot.sim_trades()[tid]
            bot.sim_withdraw(rec, withdraw, NOW + 1_000)
            bot.sim_save([(tid, rec)])
        if sync:
            await step(bot, NOW + 2_000, mkt(NOW + 2_000))  # the step's whole pass beside the lane: its sync looks the order up by its hash
        gate.set()
        await lane
        fake.create_order = real_create
        return bot, tid

    # a taker Predict filled at once: still the filled position at its slot — never "cancelled" at slot#1, no backoff
    tid = f"{HSI_SLUG}|up|吃"
    bot, got = await in_flight("taker", fill=100, SIM_TAKER_MAX_SHARES="100")
    trades = bot.sim_trades()
    t = trades.get(tid)
    assert got == tid and t is not None and not [k for k in trades if k.startswith(tid + "#")], sorted(trades)
    assert (t["status"], t["shares"], t["live"]["state"], t["live"].get("final"), t["live"]["order_id"]) == ("filled", 100, "done", "filled", "101"), \
        (t["status"], t["shares"], t["live"]["state"], t["live"].get("final"), t.get("unfilled"))
    assert not bot.live_backoff and "下单失败" not in str(t.get("note") or ""), (bot.live_backoff, t.get("note"))
    assert any("下单应答出错" in e["what"] and "#101" in e["what"] for e in t["live"]["events"]), t["live"]["events"]
    assert m.side_held(trades, t["market"], "up")["positions"] == 100  # counted by every share cap (SIM_SHARES_NET) and the group cap
    for k in list(bot.live_backoff):  # LIVE_RETRY_SECONDS later (none is expected): the side's cap is full, no second taker goes out
        bot.live_backoff[k] = (time.monotonic() - 1, bot.live_backoff[k][1])
    for at in (310_000, 315_000):
        await step(bot, NOW + at, mkt(NOW + at))
    assert creates(bot) == 1, [(k, v["status"], v["shares"]) for k, v in bot.sim_trades().items()]
    assert bot.sim_trades()[tid]["status"] == "filled"

    # a resting maker: tracked and reserved at its slot, the one order on Predict
    tid = f"{HSI_SLUG}|up|挂"
    bot, got = await in_flight("maker")
    trades = bot.sim_trades()
    t = trades.get(tid)
    assert got == tid and t is not None and not [k for k in trades if k.startswith(tid + "#")], sorted(trades)
    assert (t["live"]["state"], t["status"], t["live"]["order_id"]) == ("open", "resting", "101"), (t["live"]["state"], t["status"], t["live"].get("final"))
    assert abs(bot.live_reserved(t) - 55.0) < 1e-9 and not bot.live_backoff, (bot.live_reserved(t), bot.live_backoff)
    await step(bot, NOW + 10_000, mkt(NOW + 10_000))
    t = bot.sim_trades()[tid]
    assert t["live"]["state"] == "open" and sorted(bot.live.api.open) == ["101"] and creates(bot) == 1, (t["live"]["state"], sorted(bot.live.api.open))

    # withdrawn in flight (the loss stop): the sync's live_cancel finds the taker filled by its hash — the position stays counted
    tid = f"{HSI_SLUG}|up|吃"
    bot, got = await in_flight("taker", fill=100, withdraw="触发日亏损上限")
    trades = bot.sim_trades()
    t = trades.get(tid)
    assert t is not None and (t["status"], t["shares"], t["live"]["state"], t["live"].get("final")) == ("filled", 100, "done", "filled"), \
        sorted((k, v["status"], v["shares"], v["live"].get("final")) for k, v in trades.items())
    assert not bot.live_backoff and m.side_held(trades, t["market"], "up")["positions"] == 100, bot.live_backoff

    # withdrawn in flight (/live cancel) after 40 of a maker's 100 shares filled: removed, the 40 stay a position
    tid = f"{HSI_SLUG}|up|挂"
    bot, got = await in_flight("maker", fill=40, withdraw="管理员撤单")
    trades = bot.sim_trades()
    t = trades.get(tid)
    assert t is not None and (t["status"], t["shares"], t.get("unfilled")) == ("filled", 40, 60) and t["live"].get("final") != "failed", \
        sorted((k, v["status"], v["shares"], v.get("unfilled"), v["live"].get("final")) for k, v in trades.items())
    assert not bot.live_backoff and m.side_held(trades, t["market"], "up")["positions"] == 40 and not bot.live.api.open, bot.live_backoff

    # unchanged: the timeout with no sync in between is "result unknown" (kept placing, never re-sent), adopted at the next step
    tid = f"{HSI_SLUG}|up|吃"
    bot, got = await in_flight("taker", fill=100, sync=False)
    t = bot.sim_trades()[tid]
    assert t["live"]["state"] == "placing" and any("下单请求结果未知" in e["what"] for e in t["live"]["events"]), t["live"]["events"]
    await step(bot, NOW + 2_000, mkt(NOW + 2_000))
    t = bot.sim_trades()[tid]
    assert (t["status"], t["shares"], t["live"].get("final")) == ("filled", 100, "filled") and creates(bot) == 1

    # unchanged: a definite refusal of an order Predict never took (the sync found nothing by its hash) still fails at slot#1, backed off
    bot, got = await in_flight("taker", processed=False, error=lambda: m.RemoteError("HTTP 400: insufficient balance"))
    trades = bot.sim_trades()
    t = trades.get(f"{tid}#1")
    assert t is not None and tid not in trades and t["live"]["final"] == "failed" and t["status"] == "cancelled", sorted(trades)
    assert "下单失败：HTTP 400" in t["note"] and bot.live_backoff, (t["note"], bot.live_backoff)


asyncio.run(place_timeout_race())
print("AUDIT_LIVE_A_OK")
