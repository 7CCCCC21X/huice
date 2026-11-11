"""Audit cluster live-a: regression tests for the verified findings fixed in this cluster (fakes only, offline)."""
import asyncio, dataclasses, pathlib, sys, time
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


async def account_check_rpc_down():
    """LIVE-ACCOUNT-VERIFY-RPC: PREDICT_ACCOUNT's on-chain check is one BNB RPC read (at a restart, /live check, /live mode and
    the not-ready retries); a node that does not answer there leaves the bot signed in to Predict but not ready. New orders
    stay refused; the orders already out are still followed — their withdrawals sent, their fills read — through Predict's
    API, which needs the sign-in alone. They used to wait for the node: live_sync returned before reading or cancelling
    anything, and the express lane and the step's cancels were held the same way, for as long as the node stayed down."""
    PA = T["PA"]
    owner_of = lambda bot: bot.live.chain.owner  # FakeChain: the key's own address, what the validator holds when the node answers

    def node_down(bot, empty=False):
        tries = []
        async def read(account):  # Chain.account_owner as a node that times out (Chain.rpc makes one attempt), or answers 0x
            tries.append(account)
            if empty:
                return "0x" + "0" * 40  # an empty eth_call result reads as the zero owner ("链上没有…签名记录")
            raise m.RemoteError("网络错误 (TimeoutError)")
        bot.live.chain.account_owner = read
        return tries

    def node_back(bot):
        async def read(account): return owner_of(bot)
        bot.live.chain.account_owner = read

    def a_minute_on(bot):
        bot.live_last_signin -= 61  # live_ensure_ready's once-a-minute retry is due (monotonic): signed in again, the account checked again

    sent = lambda bot, n, kind: [c[1] for c in bot.live.api.calls[n:] if c[0] == kind]
    by_order = lambda bot, oid: next(t for t in bot.sim_trades().values() if isinstance(t.get("live"), dict) and t["live"].get("order_id") == oid)
    fair70 = lambda at: hsi(0.70, [("0.55", "300")], [("0.58", "400")], at)

    async def down_after_check(ways, empty=False, **env):
        """An order resting on Predict, then the operator's /live check while the node is down."""
        bot = make_bot(SIM_WAYS=ways, PREDICT_ACCOUNT=PA, **env)
        await bot.live_prepare()
        world["markets"] = [fair70(NOW)]
        await bot.live_prefetch(NOW)
        await step(bot, NOW, fair70(NOW))
        tid = f"{HSI_SLUG}|up|{'挂' if ways == 'maker' else '吃'}"
        oid = bot.sim_trades()[tid]["live"]["order_id"]
        assert bot.live.ready and oid in bot.live.api.open, (bot.live.not_ready_why, sorted(bot.live.api.open))
        tries = node_down(bot, empty)
        await bot.live_action("check", [], NOW + 2_000)
        why = "链上没有 Predict 账户" if empty else "无法在链上核对 PREDICT_ACCOUNT 的控制钥匙"
        assert not bot.live.ready and not bot.live.ready_error and why in bot.live.not_ready_why, bot.live.not_ready_why  # signed in, not ready
        assert "未就绪" in bot.live_room({}, fair70(NOW + 2_000), "up", 0.55, 100)  # unchanged: nothing new while not ready
        return bot, tid, oid, tries, len(bot.live.api.calls)

    # a maker whose edge is gone (fair 70¢ → 40¢ under a 55¢ bid, SIM_MAKER_EXIT_CENTS 5): withdrawn and removed, the retry failing again
    for empty in (False, True):
        bot, tid, oid, tries, n = await down_after_check("maker", empty=empty, SIM_MAKER_EXIT_CENTS="5")
        a_minute_on(bot)
        await step(bot, NOW + 10_000, hsi(0.40, [("0.55", "300")], [("0.58", "400")], NOW + 10_000))
        t = by_order(bot, oid)
        assert not bot.live.ready and len(tries) >= 2, (bot.live.ready, tries)
        assert sent(bot, n, "remove") == [[oid]] and oid not in bot.live.api.open, (empty, sent(bot, n, "remove"), t["live"]["state"], t["live"].get("cancel_why"))
        assert (t["status"], t["live"]["state"]) == ("cancelled", "done") and "低于撤单线" in t["live"]["cancel_why"], (t["status"], t["live"])
        assert not sent(bot, n, "create")
    # ...and once the node answers again, the next retry makes the bot ready: the slot is quoted again as usual
    node_back(bot)
    a_minute_on(bot)
    await step(bot, NOW + 75_000, fair70(NOW + 75_000))
    assert bot.live.ready and len(sent(bot, n, "create")) == 1, (bot.live.not_ready_why, sent(bot, n, "create"))

    # a taker's rest: removed at LIVE_TAKER_WAIT_SECONDS, the 40 shares Predict filled meanwhile read and kept a position
    bot, tid, oid, tries, n = await down_after_check("taker")
    bot.live.api.fill(oid, 40, price="0.58")
    a_minute_on(bot)
    await step(bot, NOW + 61_000, fair70(NOW + 61_000))
    t = by_order(bot, oid)
    assert sent(bot, n, "remove") == [[oid]] and oid not in bot.live.api.open and t["live"]["state"] == "done", (sent(bot, n, "remove"), t["live"])
    assert (t["status"], t["shares"], t.get("unfilled")) == ("filled", 40, 60), (t["status"], t["shares"], t.get("unfilled"))
    assert not bot.live.ready and not sent(bot, n, "create")

    # LIVE=off and the loss stop while the node is down: their withdrawals go out at the next step
    for how in ("off", "kill"):
        bot, tid, oid, tries, n = await down_after_check("maker")
        if how == "off":
            await bot.live_set_mode("off")
        else:
            bot.live_daily_pnl = lambda trades, now_ms: -60.0  # past LIVE_MAX_DAILY_LOSS_USD (50)
        a_minute_on(bot)
        await step(bot, NOW + 10_000, fair70(NOW + 10_000))
        t = by_order(bot, oid)
        assert sent(bot, n, "remove") == [[oid]] and oid not in bot.live.api.open and t["live"]["state"] == "done", (how, sent(bot, n, "remove"), t["live"])
        assert not sent(bot, n, "create") and (how == "off" or bot.live_killed()), how

    # the express lane: a book that moves the fair price under the bid — the withdrawal decided and its cancel sent in the lane itself
    bot, tid, oid, tries, n = await down_after_check("maker", SIM_MAKER_EXIT_CENTS="5")
    world["markets"], bot.market.now = [hsi(0.40, [("0.55", "300")], [("0.58", "400")], NOW + 3_000)], NOW + 3_000
    await bot.sim_express("HSI")
    t = by_order(bot, oid)
    assert sent(bot, n, "remove") == [[oid]] and t["live"]["state"] == "done" and oid not in bot.live.api.open, (sent(bot, n, "remove"), t["live"])
    assert "低于撤单线" in t["live"]["cancel_why"] and not sent(bot, n, "create") and not bot.live.ready, t["live"].get("cancel_why")


async def double_cancel():
    """LIVE-DOUBLE-CANCEL: the step's sync, every express pass that changed anything (on any key: live_cancels_unsent is
    global), /live cancel and the emergency stop all reach one withdrawn record side by side (the sync and the lanes run
    outside sim_lock), and cancel_sent was set only once Predict answered: two removals went out for one order. The second
    answer ("noop") undid the first's confirmation, and that path's single-order read — which may not know a just-removed
    order — put the finished record back to "unknown" (its $ reserved, the side blocked, a false ❓ alert); a taker's attempt
    was failed twice (stored at slot#1 and slot#2), and a second final read answering after a fresh taker had taken the slot
    deleted that live order's record: resting on Predict with no record. One path at a time now removes an order and reads
    Predict's final word on it (live_cancels), and what that path records is saved where the record lives, whatever key
    the pass still holds (live_save). Every case here is ordered by the fake's own events — which request has reached
    Predict, which answer is let go — never by the clock."""
    book_, market_json = T["book"], T["market_json"]
    A50 = "ftse-china-a50-up-or-down-on-october-5-2026"
    BIDS, ASKS = [("0.55", "300")], [("0.58", "400")]
    WAIT = 20  # seconds a request may take to show up before the test gives up (a guard against hanging, never a timing)
    card = lambda fair, at, asks=ASKS: hsi(fair, BIDS, asks, at)  # fair 70¢: a 55¢ bid worth joining, a 58¢ ask worth taking; 40¢: the bid's edge gone

    def a50(at):  # a second card on its own key (its own express lane), market 202: a chance there is something a pass changes
        return m.SimMarket(A50, "富时A50", "close", "A50", 0.85, book_(BIDS, ASKS, at, slug=A50, key="A50", mid="202"), 0.03, "", ("涨", "跌"),
                           {"key": "A50", "target": "2026-10-05", "line": 14000.0, "close_ms": T["CLOSE"]},
                           {"basis": {"fair_up": 0.85, "ref": 14000.0}, "sources": [{"what": "A50期货", "source": "sina"}], "proxy": None})

    class Held:
        """Predict's answers to one kind of call, let go by the test: the n-th matching call marks ``came[n]`` and waits for
        ``go[n]`` — its answer ready (the call processed as it arrived: a removal has taken the order off the book, a second
        one finds nothing, "noop"), or, with ``before``, the request still on its way (processed only once let go)."""
        def __init__(self, api, name, match=lambda *a, **k: True, n=2, before=False):
            self.came, self.go = [asyncio.Event() for _ in range(n)], [asyncio.Event() for _ in range(n)]
            real, count = getattr(api, name), 0

            async def call(*a, **k):
                nonlocal count
                if count >= n or not match(*a, **k):
                    return await real(*a, **k)
                i, count = count, count + 1
                out = None if before else await real(*a, **k)
                self.came[i].set()
                await self.go[i].wait()
                return await real(*a, **k) if before else out
            setattr(api, name, call)

    async def until(task, event):
        """``task`` through, or the call ``event`` marks come (the second path sent a request of its own): whichever is first."""
        mark = asyncio.ensure_future(event.wait())
        await asyncio.wait({task, mark}, return_when=asyncio.FIRST_COMPLETED, timeout=WAIT)
        mark.cancel()
        assert task.done() or event.is_set(), "neither through nor waiting on Predict"

    def unknown_once_removed(api):  # GET /orders/{hash} does not know a just-removed order (the case live_cancel's own comments allow)
        known = api.order
        async def order(oid, hash_=""):
            row = api.closed.get(str(oid))
            return None if row is not None and row.get("status") == "CANCELLED" else await known(oid, hash_)
        api.order = order

    def a50_pass(bot, at):  # an express pass on ANOTHER key that changes something (a chance on A50): it sends every unsent cancel, whatever its key
        bot.market.now, world["markets"] = at, [*world["markets"][:1], a50(at)]
        return bot.sim_express("A50")

    def hsi_pass(bot, at, fair):  # the HSI lane, on a push that moved the fair price
        bot.market.now, world["markets"] = at, [card(fair, at)]
        return bot.sim_express("HSI")

    events = lambda t: [e["what"] for e in t["live"]["events"]]
    said = lambda t, word: sum(word in what for what in events(t))
    removals = lambda bot, oid, n: [c for c in bot.live.api.calls[n:] if c == ("remove", [oid])]
    keys = lambda bot, tid: sorted(k for k in bot.sim_trades() if k == tid or k.startswith(tid + "#"))
    stored = lambda bot, tid: sorted(k for k, _ in bot.store.items("sim:") if k == f"sim:{tid}" or k.startswith(f"sim:{tid}#"))
    saved = lambda bot, t: (bot.store.get("sim:" + next(k for k, v in bot.sim_trades().items() if v is t)) or {}).get("live") or {}  # the record as the store holds it, where it lives

    async def resting(ways, **env):
        """A real order resting on HSI (market 101); A50's market (202) known to the bot too, nothing on it yet."""
        bot = make_bot(SIM_WAYS=ways, **env)
        bot.live.api.markets["202"] = market_json("202")
        await bot.live_prepare()
        await bot.live.market("202")
        world["markets"] = [card(0.70, NOW)]
        await bot.live_prefetch(NOW)
        await step(bot, NOW, card(0.70, NOW))
        tid = f"{HSI_SLUG}|up|{'挂' if ways == 'maker' else '吃'}"
        t = bot.sim_trades()[tid]
        oid = t["live"]["order_id"]
        assert t["live"]["state"] == "open" and sorted(bot.live.api.open) == [oid], (t["live"]["state"], sorted(bot.live.api.open))
        return bot, tid, t, oid, len(bot.live.api.calls)

    def ended_once(bot, t, oid, n, at):
        """One removal asked for, the record ended on its confirmation and left so: never "unknown", nothing left claimed."""
        st = t["live"]
        assert removals(bot, oid, n) == [("remove", [oid])], removals(bot, oid, n)
        assert (st["state"], st.get("cancel_confirmed")) == ("done", True), (st["state"], st.get("cancel_confirmed"), events(t)[-4:])
        assert not said(t, "状态未知") and (said(t, "撤单请求已发送"), said(t, "订单结束")) == (1, 1), events(t)
        assert "状态未知" not in bot.live_room(bot.sim_trades(), card(0.70, at), "up", 0.55, 100) and not bot.live_cancels
        assert (saved(bot, t).get("state"), saved(bot, t).get("events")) == ("done", st["events"]), (saved(bot, t).get("state"), st["state"])  # ...and saved so

    # (1) a maker whose edge is gone (fair 70¢ → 40¢ under its 55¢ bid): withdrawn by the step, its sync's removal on its way when an
    # express pass on another key ends — the pass sent the same cancel, and its "noop" and a read that does not know the removed
    # order made the finished record "unknown"
    bot, tid, t, oid, n = await resting("maker", SIM_MAKER_EXIT_CENTS="5")
    unknown_once_removed(bot.live.api)
    rm = Held(bot.live.api, "remove_orders", lambda ids: list(ids) == [oid])
    T1 = NOW + 5_000
    stepping = asyncio.create_task(step(bot, T1, card(0.40, T1)))
    await asyncio.wait_for(rm.came[0].wait(), WAIT)
    unsent = [x for x in bot.live_cancels_unsent().values() if x is t]  # (judged now, while the removal is on its way)
    lane = asyncio.create_task(a50_pass(bot, T1 + 300))
    await until(lane, rm.came[1])
    rm.go[0].set(); await stepping
    rm.go[1].set(); await lane
    await bot.live_fills_wait()
    ended_once(bot, t, oid, n, T1 + 300)
    assert not unsent, "a cancel on its way to Predict is not an unsent one: no lane picks it up"
    assert any(k.startswith(A50) and v["live"].get("order_id") in bot.live.api.open for k, v in bot.sim_trades().items()), sorted(bot.sim_trades())  # the pass's own order went out as usual

    # (2) ...and the other way round: withdrawn by the HSI lane, its removal on its way when a step's sync reaches the record
    bot, tid, t, oid, n = await resting("maker", SIM_MAKER_EXIT_CENTS="5")
    unknown_once_removed(bot.live.api)
    rm = Held(bot.live.api, "remove_orders", lambda ids: list(ids) == [oid])
    lane = asyncio.create_task(hsi_pass(bot, T1, 0.40))
    await asyncio.wait_for(rm.came[0].wait(), WAIT)
    stepping = asyncio.create_task(step(bot, T1 + 300, card(0.40, T1 + 300)))
    await until(stepping, rm.came[1])
    rm.go[0].set(); await lane
    rm.go[1].set(); await stepping
    await bot.live_fills_wait()
    ended_once(bot, t, oid, n, T1 + 300)

    # (3) a taker's unfilled rest at LIVE_TAKER_WAIT_SECONDS: the step's sync removes it while a pass on another key ends — the attempt
    # was failed twice, stored at slot#1 AND slot#2 (one object under both keys, two rows in the store). (4) the worst case: the
    # second path's final read answered only after the short wait (LIVE_TAKER_RETRY_SECONDS, 1 s at the least) had let a fresh taker
    # take the slot — the late second failure deleted that live order's record
    for fresh_first in (False, True):
        bot, tid, t, oid, n = await resting("taker")
        rm = Held(bot.live.api, "remove_orders", lambda ids: list(ids) == [oid])
        T1 = NOW + 61_000
        stepping = asyncio.create_task(step(bot, T1, card(0.70, T1, [("0.75", "400")])))  # nothing under the cap on HSI now: the timer alone ends the rest
        await asyncio.wait_for(rm.came[0].wait(), WAIT)
        lane = asyncio.create_task(a50_pass(bot, T1 + 300))
        await until(lane, rm.came[1])
        rm.go[0].set(); await stepping
        assert keys(bot, tid) == [f"{tid}#1"] and t["live"]["final"] == "failed", (keys(bot, tid), t["live"].get("final"))
        fresh = None
        if fresh_first:  # the short wait over, the chance back: the slot (its first attempt at #1) takes a fresh taker
            way = (t["market"], t["side"], "吃")
            bot.live_backoff[way] = (time.monotonic() - 1, bot.live_backoff[way][1])
            await step(bot, T1 + 2_000, card(0.70, T1 + 2_000))
            fresh = bot.sim_trades().get(tid)
            assert fresh is not None and fresh is not t and fresh["live"]["state"] == "open" and fresh["live"]["order_id"] in bot.live.api.open, keys(bot, tid)
        rm.go[1].set(); await lane
        trades = bot.sim_trades()
        slot = [tid, f"{tid}#1"] if fresh_first else [f"{tid}#1"]
        assert removals(bot, oid, n) == [("remove", [oid])] and said(t, "吃单未成交") == 1, (removals(bot, oid, n), events(t))
        assert keys(bot, tid) == slot and stored(bot, tid) == [f"sim:{k}" for k in slot], (keys(bot, tid), stored(bot, tid))
        recorded = {str(x["live"].get("order_id")) for x in trades.values() if isinstance(x.get("live"), dict)}
        assert len(bot.live.api.open) == 1 + fresh_first and set(bot.live.api.open) <= recorded, (sorted(bot.live.api.open), sorted(recorded))  # every order resting on Predict has its record
        assert trades.get(tid) is fresh and trades[f"{tid}#1"] is t and not bot.live_cancels
    # ...and whatever path fails that attempt once more now, the fresh order at its slot (a read answered late, by the key it held):
    # the attempt stays at #1, the fresh order's record is never replaced by it (sim_rekey's replace of sim:<slot>)
    bot.live_fail(tid, t, "吃单未成交（又一次）", T1 + 3_000, retry=1)
    trades = bot.sim_trades()
    assert trades.get(tid) is fresh and trades.get(f"{tid}#1") is t and keys(bot, tid) == [tid, f"{tid}#1"], keys(bot, tid)
    assert stored(bot, tid) == [f"sim:{tid}", f"sim:{tid}#1"] and saved(bot, t).get("events") == t["live"]["events"], stored(bot, tid)  # saved where it lives
    assert ((bot.store.get(f"sim:{tid}") or {}).get("live") or {}).get("order_id") == fresh["live"]["order_id"], bot.store.get(f"sim:{tid}")

    # (5) /live cancel all (the emergency stop) while a pass has the same cancel on its way: one removal, and the reply counts that
    # request as sent and awaiting Predict's word — not "尚未发出"
    bot, tid, t, oid, n = await resting("maker")
    ls = Held(bot.live.api, "orders", n=1)
    rm = Held(bot.live.api, "remove_orders", lambda ids: list(ids) == [oid])
    T1 = NOW + 5_000
    bot.market.now = T1
    asking = asyncio.create_task(bot.live_cancel_command(["all"], T1))  # withdraws the order, then reads the open list
    await asyncio.wait_for(ls.came[0].wait(), WAIT)
    lane = asyncio.create_task(a50_pass(bot, T1 + 100))
    await asyncio.wait_for(rm.came[0].wait(), WAIT)
    ls.go[0].set()
    await until(asking, rm.came[1])
    rm.go[0].set(); await lane
    rm.go[1].set()
    reply = await asking
    await bot.live_fills_wait()
    assert "1 笔撤单请求已发送、待确认" in reply and "尚未发出" not in reply and reply.startswith("⏳"), reply
    ended_once(bot, t, oid, n, T1 + 100)
    assert abs(bot.live_reserved(t)) < 1e-9, bot.live_reserved(t)  # its fills read (nothing filled): nothing reserved

    # (6) an order Predict's reads lost (the open list and the single-order read) though it still rests: the third look asks for its
    # removal all the same (live_track) — on its way when the lane withdraws the record: that removal is the one, and its "removed"
    # ends the record (the lane's own "noop" and empty read made it "unknown")
    async def lost():
        bot, tid, t, oid, n = await resting("maker", SIM_MAKER_EXIT_CENTS="5")
        async def no_rows(status="OPEN", **params): return []
        async def no_row(o, hash_=""): return None
        bot.live.api.orders, bot.live.api.order = no_rows, no_row
        for at in (NOW + 80_000, NOW + 100_000):  # two looks, a minute and more after the placement
            await step(bot, at, card(0.70, at))
        assert bot.live_misses.get(tid) == 2 and t["live"]["state"] == "open", (bot.live_misses, t["live"]["state"])
        return bot, tid, t, oid, len(bot.live.api.calls), NOW + 120_000

    bot, tid, t, oid, n, T3 = await lost()
    rm = Held(bot.live.api, "remove_orders", lambda ids: list(ids) == [oid])
    stepping = asyncio.create_task(step(bot, T3, card(0.70, T3)))
    await asyncio.wait_for(rm.came[0].wait(), WAIT)
    lane = asyncio.create_task(hsi_pass(bot, T3 + 300, 0.40))
    await until(lane, rm.came[1])
    rm.go[0].set(); await stepping
    rm.go[1].set(); await lane
    await bot.live_fills_wait()
    assert removals(bot, oid, n) == [("remove", [oid])] and t["live"]["state"] == "done", (removals(bot, oid, n), t["live"]["state"])
    assert not said(t, "状态未知") and said(t, "补发的撤单请求撤掉了它") == 1 and not bot.live_cancels, events(t)
    assert (saved(bot, t).get("state"), saved(bot, t).get("events")) == ("done", t["live"]["events"]), saved(bot, t).get("state")  # saved at tid#n, where the lane's withdrawal moved it meanwhile
    # ...and the other way round: the lane's removal on its way when the third look's read comes back empty — left to the lane's
    bot, tid, t, oid, n, T3 = await lost()
    rd = Held(bot.live.api, "order", n=1)
    rm = Held(bot.live.api, "remove_orders", lambda ids: list(ids) == [oid])
    stepping = asyncio.create_task(step(bot, T3, card(0.70, T3)))
    await asyncio.wait_for(rd.came[0].wait(), WAIT)  # the sync at its read of the record, still open
    lane = asyncio.create_task(hsi_pass(bot, T3 + 300, 0.40))
    await asyncio.wait_for(rm.came[0].wait(), WAIT)  # withdrawn by the lane meanwhile, its removal sent
    rd.go[0].set()
    await until(stepping, rm.came[1])
    rm.go[0].set(); await lane
    rm.go[1].set(); await stepping
    await bot.live_fills_wait()
    ended_once(bot, t, oid, n, T3 + 300)

    # (7) a list Predict read before the removal, in a step that began after the lane's pass did (the pass stamps its events with
    # its own start): the record, ended on Predict's confirmation meanwhile, was taken back ("重新出现在开放订单列表": a false alert,
    # its $ reserved and the side held again) and removed a second time
    bot, tid, t, oid, n = await resting("maker", SIM_MAKER_EXIT_CENTS="5")
    rm = Held(bot.live.api, "remove_orders", lambda ids: list(ids) == [oid], before=True)  # the lane's removal still on its way to Predict...
    ls = Held(bot.live.api, "orders", n=1)  # ...when Predict reads the step's list: the order is on it
    E = NOW + 5_000
    lane = asyncio.create_task(hsi_pass(bot, E, 0.40))
    await asyncio.wait_for(rm.came[0].wait(), WAIT)
    stepping = asyncio.create_task(step(bot, E + 50, card(0.40, E + 50)))
    await asyncio.wait_for(ls.came[0].wait(), WAIT)
    bot.market.now = E + 200  # the clock when Predict confirms the removal
    rm.go[1].set()  # (a second removal, should one be sent, goes straight through)
    rm.go[0].set(); await lane
    ls.go[0].set(); await stepping
    await bot.live_fills_wait()
    ended_once(bot, t, oid, n, E + 200)
    assert not said(t, "重新出现") and abs(bot.live_reserved(t)) < 1e-9 and t["live"]["cancel_done_at"] == E + 200, (events(t), bot.live_reserved(t))
    # ...while a list of a later look that still shows the order (a removal answered but not done) takes the record back as before,
    # and the order is removed again
    bot.live.api.open[oid] = {**bot.live.api.closed.pop(oid), "status": "OPEN"}
    for at in (E + 65_000, E + 70_000):  # the idle re-read of the list, then the step that cancels what it took back
        await step(bot, at, card(0.40, at))
    await bot.live_fills_wait()
    assert said(t, "重新出现") == 1 and t["live"]["state"] == "done" and oid not in bot.live.api.open, (events(t), sorted(bot.live.api.open))
    assert len(removals(bot, oid, n)) == 2 and not bot.live_cancels, removals(bot, oid, n)

    # (8) the order's own record, read by the step beside the lane's removal (live_track: the list did not show the order), ends the
    # record while the removal's answer is on its way: that answer changes nothing more (it ended the record a second time; a
    # taker's attempt was failed twice)
    bot, tid, t, oid, n = await resting("maker", SIM_MAKER_EXIT_CENTS="5")
    async def no_rows(status="OPEN", **params): return []
    bot.live.api.orders = no_rows
    rd = Held(bot.live.api, "order", n=1, before=True)  # the step's read on its way to Predict: the lane's removal gets there first...
    rm = Held(bot.live.api, "remove_orders", lambda ids: list(ids) == [oid])  # ...and its answer is the slower of the two
    T1 = NOW + 20_000
    stepping = asyncio.create_task(step(bot, T1, card(0.70, T1)))
    await asyncio.wait_for(rd.came[0].wait(), WAIT)
    lane = asyncio.create_task(hsi_pass(bot, T1 + 300, 0.40))
    await asyncio.wait_for(rm.came[0].wait(), WAIT)
    rd.go[0].set(); await stepping
    assert t["live"]["state"] == "done" and said(t, "订单结束") == 1, (t["live"]["state"], events(t))
    rm.go[0].set(); rm.go[1].set(); await lane
    await bot.live_fills_wait()
    assert removals(bot, oid, n) == [("remove", [oid])] and t["live"]["state"] == "done" and said(t, "订单结束") == 1 and not bot.live_cancels, events(t)
    # ...nor does the final read after a removal Predict did not confirm ("noop": the order had ended on Predict's side already)
    bot, tid, t, oid, n = await resting("maker", SIM_MAKER_EXIT_CENTS="5")
    bot.live.api.closed[oid] = {**bot.live.api.open.pop(oid), "status": "CANCELLED"}
    rd = Held(bot.live.api, "order", before=True)  # the step's read of the order (live_track) and the lane's final read, both on their way
    stepping = asyncio.create_task(step(bot, T1, card(0.70, T1)))
    await asyncio.wait_for(rd.came[0].wait(), WAIT)
    lane = asyncio.create_task(hsi_pass(bot, T1 + 300, 0.40))
    await asyncio.wait_for(rd.came[1].wait(), WAIT)
    rd.go[0].set(); await stepping
    rd.go[1].set(); await lane
    assert removals(bot, oid, n) == [("remove", [oid])] and t["live"]["state"] == "done" and said(t, "订单结束") == 1 and not bot.live_cancels, events(t)

    # (9) a removal request that fails leaves the order to the next pass: nothing stays claimed, the cancel goes out again
    bot, tid, t, oid, n = await resting("maker", SIM_MAKER_EXIT_CENTS="5")
    real = bot.live.api.remove_orders
    async def refused(ids): raise m.RemoteError("HTTP 502: bad gateway")
    bot.live.api.remove_orders = refused
    await hsi_pass(bot, T1, 0.40)
    assert t["live"]["state"] == "cancelling" and not t["live"].get("cancel_sent") and not bot.live_cancels, t["live"]
    bot.live.api.remove_orders = real
    await step(bot, T1 + 5_000, card(0.40, T1 + 5_000))
    await bot.live_fills_wait()
    ended_once(bot, t, oid, n, T1 + 5_000)

    # (10) withdrawn by the HSI lane with nothing filled — the record moves to tid#1, saved there "cancelling" — while a step's sync,
    # begun before (the record open, at its slot), has its open list on its way; and the SYNC is the path that gets to the cancel:
    # it removes the order and ends the record, holding the slot key the record has left. Nothing saved that: the sync wrote
    # nothing under a key the record had left, the lane left the order to the sync (one path at a time), and the fills read does
    # not know the removed order — the store kept the row "cancelling", its cancel unsent, beside a record done in memory, and a
    # restart sent the removal again ("noop") and made the confirmed cancel "unknown". The same with the list read failing: the
    # sync's cancels then go by order id (live_cancel_blind), under the same old key. Held: the lane's own cancels wait until the
    # sync has the order — or nothing held, the real entry points alone: the list's answer let go and the lane started in one
    # loop turn (which of the two then removes the order is the loop's to say; either way the record's end is saved)
    for fails in (False, True):
        for held in (True, False):
            bot, tid, t, oid, n = await resting("maker", SIM_MAKER_EXIT_CENTS="5")
            unknown_once_removed(bot.live.api)
            ls = Held(bot.live.api, "orders", n=1, before=fails)  # (failing: the request still on its way, refused once let go)
            rm = Held(bot.live.api, "remove_orders", lambda ids: list(ids) == [oid])
            T1, moved = NOW + 5_000, f"{tid}#1"
            stepping = asyncio.create_task(step(bot, T1, card(0.70, T1)))  # (nothing for the step itself to withdraw)
            await asyncio.wait_for(ls.came[0].wait(), WAIT)  # the sync's list on its way: the record open, at its slot
            assert t["live"]["state"] == "open" and bot.sim_trades().get(tid) is t, (t["live"]["state"], keys(bot, tid))
            bot.live.api.fail_open = "HTTP 502: bad gateway" if fails else None
            if held:
                blind, lane_at, lane_go = bot.live_cancel_blind, asyncio.Event(), asyncio.Event()

                async def lane_cancels(records, now_ms, blind=blind, lane_at=lane_at, lane_go=lane_go, moved=moved):
                    if moved in records:  # the lane's own cancels, the record it has just moved among them (the sync's hold the slot key)
                        lane_at.set()
                        await lane_go.wait()
                    return await blind(records, now_ms)
                bot.live_cancel_blind = lane_cancels
                lane = asyncio.create_task(hsi_pass(bot, T1 + 300, 0.40))
                await asyncio.wait_for(lane_at.wait(), WAIT)  # withdrawn and moved by the lane, its cancel not begun...
                ls.go[0].set()
                await asyncio.wait_for(rm.came[0].wait(), WAIT)  # ...when the sync, its list answered, has the removal on its way
                lane_go.set()
            else:
                ls.go[0].set()
                lane = asyncio.create_task(hsi_pass(bot, T1 + 300, 0.40))
                await asyncio.wait_for(rm.came[0].wait(), WAIT)
            rm.go[0].set(); rm.go[1].set(); await stepping; await lane
            await bot.live_fills_wait()
            assert keys(bot, tid) == [moved] and bot.sim_trades()[moved] is t and t["live"].get("rekeyed") == moved, (keys(bot, tid), t["live"].get("rekeyed"))
            assert not held or t["live"]["cancel_sent"] == T1, (t["live"]["cancel_sent"], T1)  # the removal was the sync's (the lane's pass began at T1 + 300)
            ended_once(bot, t, oid, n, T1 + 300)  # ...and saved so, where the record lives
            assert stored(bot, tid) == [f"sim:{moved}"], stored(bot, tid)  # nothing written back under the slot's key
            # a restart on that store finds nothing to redo: no second removal, the confirmed cancel never "unknown"
            bot.live.api.fail_open = None
            again = T["L"].LiveBot(bot.config, bot.store, T["FM"](T1 + 60_000), None)
            again.live.api, again.live.chain, again.sim_markets = bot.live.api, bot.live.chain, lambda now: world["markets"]
            await again.live_prepare()
            await step(again, T1 + 60_000, card(0.40, T1 + 60_000))
            await again.live_fills_wait()
            t2 = again.sim_trades()[moved]
            assert removals(again, oid, n) == [("remove", [oid])] and t2["live"]["state"] == "done" and not said(t2, "状态未知"), (removals(again, oid, n), events(t2)[-2:])
    # ...and only the record itself is written at its numbered key: an object that merely names that key (one held from before the
    # view was rebuilt from the store, a record pruned since) is never put back there
    cur = bot.sim_trades()[moved]
    bot.live_save(tid, {**cur, "live": {**cur["live"], "state": "cancelling", "cancel_sent": 0}})
    assert bot.sim_trades()[moved] is cur and bot.store.get(f"sim:{moved}")["live"]["state"] == "done" and stored(bot, tid) == [f"sim:{moved}"], stored(bot, tid)


async def order_test_limits():
    """LIVE-TEST-LIMITS: /live test — and the control endpoint's "test", which passes its request's "args" on — takes a share
    count and a price from whoever asks, and readiness was all that stood before the signature: `/live test 恒生 1000 99` sent
    a $990 buy across the book with LIVE_MAX_ORDER_USD at $5, under LIVE=pause, /live pause, today's loss stop and
    LIVE_TRADING_ALLOWED=off alike (and under LIVE=off once /live check had signed in). Whatever the arguments, the test is
    a resting order now: never at or above the best ask, worth LIVE_MAX_ORDER_USD at the most, and at a price that may trade
    (at or above the best bid) only while a model order could open a position — the hard switch on, LIVE=on, no /live
    pause, today's loss stop not tripped, room under LIVE_MAX_OPEN_USD. Under the hard switch the test is a minimum-size
    order ($2 at the most) whatever LIVE_MAX_ORDER_USD is saved as: no saved setting decides what leaves there. What is
    documented stays: the test without arguments ($1, under the best bid) in every mode — LIVE=pause is where it is run —
    and the operator's own test at the bid while the bot trades."""
    WEI = T["WEI"]
    ASK = 58.0  # ¢: the card below bids 55¢ and asks 58¢
    card = lambda: hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)
    AT_BID = ("恒生", "100", "55")  # $55 at the best bid: it may trade (within make_bot's caps, $100 an order and $250 open)
    today = m.beijing_day(NOW / 1000)

    def orders(bot):  # what the bot sent to Predict: (USDT offered, shares, price in ¢) per order
        return [(int(b["data"]["order"]["makerAmount"]) / WEI, int(b["data"]["order"]["takerAmount"]) / WEI, int(b["data"]["pricePerShare"]) * 100 / WEI)
                for b in (c[1] for c in bot.live.api.calls if c[0] == "create")]

    async def attempt(bot, *args, page=False, cards=None):
        """One order test, by /live test or (page) by the control page's request: (ok, the answer, the order it sent or None)."""
        world["markets"] = cards or [card()]
        before = len(orders(bot))
        if page:  # the request as the web handler hands it on once the key is checked; the page's own button sends no "args"
            got = await bot.control_action({"action": "test", "args": list(args)} if args else {"action": "test"})
            ok, text = got["ok"], got["message"]
        else:
            text = await bot.live_action("test", list(args), NOW)
            ok = None
        sent = orders(bot)[before:]
        assert len(sent) <= 1, sent
        return ok, text, (sent[0] if sent else None)

    def refused(got, *words):  # nothing signed, nothing sent; "❌" is what makes the page report a failure
        ok, text, sent = got
        assert sent is None and text.startswith("❌") and ok is not True and all(w in text for w in words), (ok, text, sent)

    def went(got, usd, shares, cents, *words):  # sent as asked, listed, withdrawn, its final state read
        ok, text, sent = got
        assert sent == (usd, shares, cents) and ok is not False, (ok, text, sent)
        assert text.startswith("🧪 挂单测试") and "1/4 下单成功" in text and "3/4 撤单：已撤" in text and all(w in text for w in words), text

    # every stop at once, as the audit ran it: the hard switch (it caps the mode at pause), /live pause, today's loss stop; the
    # caps at $5 an order and $10 open
    bot = make_bot(LIVE_TRADING_ALLOWED="off", LIVE_MAX_ORDER_USD="5", LIVE_MAX_OPEN_USD="10")
    await bot.live_prepare()
    await bot.live_action("pause", [], NOW)
    bot.store.put("live:killed", {"day": today, "why": "触发日亏损上限", "at": NOW})
    assert bot.live.ready and (bot.config.live_mode, bot.config.live_allowed) == ("pause", False) and bot.live_paused() and bot.live_killed()
    refused(await attempt(bot, "恒生", "1000", "99"))  # the audit's order: $990 across the book
    refused(await attempt(bot, "恒生", "1000", "99", page=True))  # ...and by a hand-made request to the control endpoint
    refused(await attempt(bot, "恒生", "1000", "54"), "LIVE_TRADING_ALLOWED=off", "$2（")  # $540 a cent under the bid: it would rest, but it is no test
    refused(await attempt(bot, "恒生", "2", "55"), "LIVE_TRADING_ALLOWED=off", "可能成交")  # $1.10 at the bid: small enough, but it may trade
    refused(await attempt(bot, "恒生", "2", "57"), "LIVE_TRADING_ALLOWED=off", "可能成交")  # ...as between the bid and the ask
    refused(await attempt(bot, "恒生", "2", "58"), "最低卖价")  # $1.16 at the ask is a purchase all the same
    assert not orders(bot) and bot.store.get("live:test") is None and bot.live_status(NOW)["last_test"] == ""  # a refusal is no test result
    went(await attempt(bot), 1.0, 50, 2.0)  # the test without arguments goes on under every stop: 2¢ × 50 = $1, far under the bid
    went(await attempt(bot, page=True), 1.0, 50, 2.0)  # the page's button
    went(await attempt(bot, "恒生", "100", "1"), 1.0, 100, 1.0)  # README's own example: 100 shares at 1¢
    last = bot.store.get("live:test")["text"]
    refused(await attempt(bot, "恒生", "1000", "99"))
    assert bot.store.get("live:test")["text"] == last  # the last test's result stays on the page
    # the hard switch's bound is no saved setting: the page raises LIVE_MAX_ORDER_USD (it may, whatever the switch says), and the
    # test stays a minimum-size order under the bid
    assert bot.control_set({"LIVE_MAX_ORDER_USD": "1000000"})["ok"] and bot.config.live_max_order_usd == 1_000_000 and not bot.config.live_allowed
    refused(await attempt(bot, "恒生", "1000", "54", page=True), "LIVE_TRADING_ALLOWED=off", "$2（")
    went(await attempt(bot, "恒生", "4", "30"), 1.2, 4, 30.0)  # Predict's minimum at 30¢, well under the bid
    went(await attempt(bot, "恒生", "200", "1"), 2.0, 200, 1.0)  # $2 to the cent
    refused(await attempt(bot, "恒生", "201", "1"), "LIVE_TRADING_ALLOWED=off", "$2（")

    # LIVE=on, nothing stopped: the ask and LIVE_MAX_ORDER_USD hold; the operator's own test at the bid goes out, and says it may trade
    on = make_bot()
    await on.live_prepare()
    went(await attempt(on, *AT_BID), 55.0, 100, 55.0, "可能成交")
    went(await attempt(on, "恒生", "100", "57"), 57.0, 100, 57.0, "可能成交")  # between the bid and the ask
    refused(await attempt(on, "恒生", "100", "58"), "最低卖价")  # at the ask it would trade at once
    refused(await attempt(on, "恒生", "1", "100"), "最低卖价")  # README's example with its two numbers swapped: 99¢ (the tick's ceiling) × 2 shares
    refused(await attempt(on, "恒生", "1000", "57"), "LIVE_MAX_ORDER_USD $100")  # $570
    went(await attempt(on, "恒生", "200", "50"), 100.0, 200, 50.0)  # LIVE_MAX_ORDER_USD to the cent
    refused(await attempt(on, "恒生", "201", "50"), "LIVE_MAX_ORDER_USD $100")  # ...and 50¢ over it
    # each stop by itself keeps a test that may trade in; the test under the bid goes on; /live resume lifts both stops, as for the model
    await on.live_action("pause", ["看看"], NOW)
    refused(await attempt(on, *AT_BID), "已暂停（看看")
    went(await attempt(on), 1.0, 50, 2.0)
    await on.live_action("resume", [], NOW)
    went(await attempt(on, *AT_BID), 55.0, 100, 55.0, "可能成交")
    on.store.put("live:killed", {"day": today, "why": "今日已结算亏损 −$60.00 达到 LIVE_MAX_DAILY_LOSS_USD $50", "at": NOW})
    refused(await attempt(on, *AT_BID), "今日停止开新仓")
    went(await attempt(on), 1.0, 50, 2.0)
    await on.live_action("resume", [], NOW)
    went(await attempt(on, *AT_BID), 55.0, 100, 55.0, "可能成交")
    paused = make_bot(LIVE="pause")
    await paused.live_prepare()
    refused(await attempt(paused, *AT_BID), "LIVE=pause")
    went(await attempt(paused), 1.0, 50, 2.0)  # (test_trading pins this one: "it is how the mode is meant to be used")
    hard = make_bot(LIVE_TRADING_ALLOWED="off")
    await hard.live_prepare()
    hard.config = hard.live.config = dataclasses.replace(hard.config, live_mode="on")  # the hard switch alone: the mode forced on in memory
    refused(await attempt(hard, *AT_BID), "LIVE_TRADING_ALLOWED=off")
    went(await attempt(hard), 1.0, 50, 2.0)
    off = make_bot(LIVE="off")
    world["markets"] = [card()]
    assert "未就绪" in await off.live_action("test", list(AT_BID), NOW) and not orders(off)  # paper only: not even signed in
    await off.live_action("check", [], NOW)  # /live check signs in whatever the mode
    assert off.live.ready and off.config.live_mode == "off"
    refused(await attempt(off, *AT_BID), "LIVE=off")
    went(await attempt(off), 1.0, 50, 2.0)
    # the smallest LIVE_MAX_ORDER_USD the setting takes ($1) still fits the test without arguments, at either price it takes
    least = make_bot(LIVE="pause", LIVE_MAX_ORDER_USD="1")
    await least.live_prepare()
    went(await attempt(least), 1.0, 50, 2.0)
    went(await attempt(least, cards=[hsi(0.70, [("0.02", "300")], [("0.05", "400")], NOW)]), 1.0, 100, 1.0)  # a 2¢ bid: 1¢ × 100
    refused(await attempt(least, "恒生", "4", "30"), "LIVE_MAX_ORDER_USD $1")  # $1.20
    # LIVE_MAX_OPEN_USD: the model's resting order holds $55 of $100 — a test that may trade has the other $45, the $1 test goes on
    full = make_bot(SIM_WAYS="maker", LIVE_MAX_OPEN_USD="100")
    await full.live_prepare()
    world["markets"] = [card()]
    await full.live_prefetch(NOW)
    await step(full, NOW, card())
    assert abs(full.live_exposure(full.sim_trades()) - 55.0) < 1e-9 and orders(full) == [(55.0, 100, 55.0)], (full.live_exposure(full.sim_trades()), orders(full))
    refused(await attempt(full, *AT_BID), "LIVE_MAX_OPEN_USD $100")
    went(await attempt(full, "恒生", "80", "55"), 44.0, 80, 55.0, "可能成交")
    went(await attempt(full), 1.0, 50, 2.0)
    # all in all: no order of any of these bots was worth more than its LIVE_MAX_ORDER_USD, none was priced at the ask or above
    for b, cap in ((bot, 2), (on, 100), (paused, 100), (hard, 2), (off, 100), (least, 1), (full, 100)):
        assert orders(b) and all(usd <= cap + 1e-9 and cents < ASK for usd, _, cents in orders(b)), orders(b)


async def maker_expiry():
    """LIVE-MAKER-EXPIRY: the strategy leaves no order on a market past its end (sim_quiet withdraws every one there, whatever
    the settings: "a bid left on a finished market is a free option for whoever holds the losing side"), but a maker was signed
    to expire two hours after that end, and a taker five minutes after it was built wherever the end fell. The signed expiry is
    the one bound left when the cancel cannot get through (Predict's cancel endpoint erroring, the sign-in broken, a long 429
    cooldown) or the process is down across the end: the bid stayed matchable long after the result was public. An order is now
    signed to expire a minute after its market's end, two minutes after it is decided should that be later, and without an
    expiry only when no end is known (live.order_expiry). What waits on an order's expiry is released no sooner than it was:
    an unknown maker's reservation and its side still wait two hours past the end (live_order_deadline)."""
    L, CLOSE = T["L"], T["CLOSE"]
    END = CLOSE // 1000  # the HSI card's close (16:10), in the seconds an order's expiration counts
    card = lambda at: hsi(0.70, [("0.55", "300")], [("0.58", "400")], at)  # fair 70¢: a 55¢ bid worth joining (挂涨), a 58¢ ask worth taking (吃涨)
    created = lambda bot: [body["data"] for kind, body in bot.live.api.calls if kind == "create"]
    by_order = lambda bot, oid: next(t for t in bot.sim_trades().values() if isinstance(t.get("live"), dict) and t["live"].get("order_id") == oid)

    async def looked(at):
        """One look at the HSI card at ``at`` (SIM_WAYS=both): the bot, and the expiry each order it sent was signed with, by way (挂 / 吃)."""
        bot = make_bot()
        await bot.live_prepare(); await bot.live_prefetch(at)
        await step(bot, at, card(at))
        signed = {d["order"]["hash"].lower(): int(d["order"]["expiration"]) for d in created(bot)}  # what went to Predict, by the order's hash
        trades = bot.sim_trades()
        assert len(signed) == len(trades) and all(t["live"]["state"] == "open" for t in trades.values()), (at - CLOSE, sorted(trades), len(signed))
        assert all(t["live"]["want"]["expires"] == signed[t["live"]["hash"]] for t in trades.values() if t["maker"])  # a maker: as its record says
        return bot, {tid.rsplit("|", 1)[-1]: signed[t["live"]["hash"]] for tid, t in trades.items()}

    # hours before the close: the maker a minute past it (it was two hours past it: END + 7200), the taker its five minutes as ever
    bot, got = await looked(NOW)
    assert got == {"挂": END + 60, "吃": NOW // 1000 + 300}, {k: v - END for k, v in got.items()}
    # the close nearer than that: an order ends a minute past it — a taker's five minutes ran up to five minutes past the close —
    # or two minutes after it is decided, in the market's last minute: never without an expiry while the end is known (a minute's
    # grace with the old guard as it was — "under two minutes away: none" — left every order of the last minute without one)
    for before, maker, taker in ((301, 60, -1),    # (a taker whose five minutes end before the close: as it was)
                                 (240, 60, 60), (200, 60, 60), (61, 60, 60), (60, 60, 60),
                                 (59, 61, 61), (30, 90, 90), (1, 119, 119)):
        at = CLOSE - before * 1000
        _, got = await looked(at)
        assert got == {"挂": END + maker, "吃": END + taker}, (before, {k: v - END for k, v in got.items()})
        assert all(at // 1000 + 120 <= exp < L.FAR_FUTURE for exp in got.values()) and got["吃"] <= at // 1000 + 300, (before, got)
    # ...and an order only goes out before the end (live_send_block, unchanged): so with over a minute left, as the pre-open mode
    # asks of its own orders (SEND_MIN_LEFT_MS); at the end and after it nothing is decided at all
    late = bot.sim_open(card(CLOSE - 50_000), "up", CLOSE - 50_000, maker=m.BookEdge("涨", True, 0.55, 0.15, 300.0, 0.15), shares=100)
    assert late["live"]["want"]["expires"] == END + 70 and bot.live_send_block(late, {}, CLOSE - 1) == "" and "已到结束时间" in bot.live_send_block(late, {}, CLOSE)
    for at in (CLOSE, CLOSE + 5_000):
        quiet = make_bot()
        await quiet.live_prepare(); await quiet.live_prefetch(at)
        await step(quiet, at, card(at))
        assert not created(quiet) and not quiet.sim_trades(), (at - CLOSE, sorted(quiet.sim_trades()))

    # the rule itself, and the end as each kind of market records it (a daily card's close, a touch market's deadline, a window's or
    # a ladder's end); a market whose end is not known keeps an order without an expiry, as before
    assert L.order_expiry(CLOSE, NOW) == L.order_expiry(CLOSE, CLOSE - 60_000) == END + 60 and L.order_expiry(CLOSE, CLOSE - 59_000) == END + 61
    assert L.order_expiry(CLOSE, CLOSE + 3 * 3600_000) == END + 3 * 3600 + 120  # (decided after the end — sim_quiet sees to it that none is: still two minutes, not "none")
    assert L.order_expiry(None, NOW) == L.order_expiry(0, NOW) == L.FAR_FUTURE
    edge = m.BookEdge("涨", True, 0.55, 0.15, 300.0, 0.15)
    opened = lambda kind, settle: bot.sim_open(dataclasses.replace(card(NOW), kind=kind, settle=settle), "up", NOW, maker=edge, shares=100)["live"]["want"]["expires"]
    for kind, settle in (("close", {"key": "HSI", "target": "2026-10-05", "line": 24600.0, "close_ms": CLOSE}), ("touch", {"deadline": CLOSE}), ("flip", {"end": CLOSE}),
                         ("updown", {"end": CLOSE, "start": CLOSE - 14 * 60_000, "window": 15}), ("ladder", {"target": "100", "end": CLOSE})):
        assert opened(kind, settle) == END + 60, (kind, opened(kind, settle) - END)
    assert opened("close", {"key": "HSI", "target": "2026-10-05", "line": 24600.0}) == opened("flip", {}) == L.FAR_FUTURE

    # the harm itself, on an exchange that honours the signed expiry (its contract matches no order past it — the pre-open mode's
    # backstop, PREOPEN.md): a 55¢ bid for 100 涨 rests from the morning on...
    bot = make_bot(SIM_WAYS="maker")
    await bot.live_prepare(); await bot.live_prefetch(NOW)
    await step(bot, NOW, card(NOW))
    fake = bot.live.api
    oid = bot.sim_trades()[f"{HSI_SLUG}|up|挂"]["live"]["order_id"]
    matchable = lambda at: oid in fake.open and int(fake.open[oid]["order"]["expiration"]) >= at // 1000  # resting, its signature not expired
    # ...the process down from here on (stopping cancels nothing, drain_orders), the signature is all that bounds it: good for the
    # market's whole life — the expiry takes nothing from the order — and dead a minute after the close (it was matchable until 18:10)
    assert matchable(NOW) and matchable(CLOSE - 1_000) and matchable(CLOSE + 59_000), int(fake.open[oid]["order"]["expiration"]) - END
    assert not matchable(CLOSE + 61_000) and not matchable(CLOSE + 45 * 60_000), int(fake.open[oid]["order"]["expiration"]) - END
    # ...or the bot up, and every removal failing from the close on: withdrawn at the end as ever, asked for again each look, and
    # still on Predict's book half an hour later, when the losing side's holders sell into whatever bid is left
    real_remove = fake.remove_orders
    async def failing(ids):
        fake.calls.append(("remove", list(ids)))
        raise m.RemoteError("HTTP 502: bad gateway")
    fake.remove_orders = failing
    for at in (CLOSE + 1_000, CLOSE + 61_000, CLOSE + 10 * 60_000, CLOSE + 30 * 60_000):
        await step(bot, at, card(at))
    t = by_order(bot, oid)
    assert (t["status"], t["live"]["state"]) == ("cancelled", "cancelling") and "已到结束时间" in t["live"]["cancel_why"] and oid in fake.open, (t["status"], t["live"])
    assert len([c for c in fake.calls if c == ("remove", [oid])]) >= 3 and len(created(bot)) == 1, fake.calls[-6:]
    if matchable(CLOSE + 30 * 60_000):
        fake.fill(oid, 100)  # (what happened: 100 shares of the side that lost, at 55¢ each)
    fake.remove_orders = real_remove  # Predict's cancel endpoint back: the order is taken off, and nothing of it filled
    await step(bot, CLOSE + 31 * 60_000, card(CLOSE + 31 * 60_000))
    await bot.live_fills_wait()
    t = by_order(bot, oid)
    assert (t["status"], t["shares"], t["live"]["state"]) == ("cancelled", 0, "done") and oid not in fake.open, (t["status"], t["shares"], t["live"]["state"])
    assert abs(bot.live_reserved(t)) < 1e-9 and abs(bot.live_exposure(bot.sim_trades())) < 1e-9, bot.live_exposure(bot.sim_trades())

    # what waits on an order's expiry waits as long as it did. A maker Predict's reads lost (unknown: its $ reserved, its side
    # held) with no whole list of the account's orders to settle it by: still held eleven minutes past the close — where its
    # signature, and the ten-minute margin on it, would have let it go — and until two hours past it, as before
    async def no_rows(status="OPEN", **params): return []
    async def no_row(o, hash_=""): return None
    async def not_removed(ids): return {"removed": [], "noop": list(ids)}
    bot = make_bot(SIM_WAYS="maker")
    await bot.live_prepare(); await bot.live_prefetch(NOW)
    await step(bot, NOW, card(NOW))
    fake = bot.live.api
    tid = f"{HSI_SLUG}|up|挂"
    oid = bot.sim_trades()[tid]["live"]["order_id"]
    fake.orders, fake.order, fake.remove_orders = no_rows, no_row, not_removed
    for at in (NOW + 10_000, NOW + 20_000, NOW + 30_000, NOW + 65_000):
        await step(bot, at, card(at))
    t = bot.sim_trades()[tid]
    HELD = CLOSE + 2 * 3600_000
    assert t["live"]["state"] == "unknown" and t["live"]["want"]["expires"] == END + 60 and bot.live_order_deadline(t) == HELD, (t["live"]["state"], bot.live_order_deadline(t) - CLOSE)
    assert bot.live_status(NOW + 65_000)["unknown"][0]["deadline"] == m.stamp(HELD + bot.LIVE_UNKNOWN_MARGIN_MS, seconds=False)
    fake.orders_complete = False  # a cut list is no look (the pre-open mode's thousands of orders): the evidence cannot end the record
    for at in (CLOSE + 11 * 60_000 + 5_000, CLOSE + 11 * 60_000 + 10_000, HELD + bot.LIVE_UNKNOWN_MARGIN_MS - 5_000):
        await step(bot, at, card(at))
        t = by_order(bot, oid)
        assert t["live"]["state"] == "unknown" and abs(bot.live_reserved(t) - 55.0) < 1e-9, (at - CLOSE, t["live"]["state"], bot.live_reserved(t))
    await step(bot, HELD + bot.LIVE_UNKNOWN_MARGIN_MS + 5_000, card(HELD + bot.LIVE_UNKNOWN_MARGIN_MS + 5_000))
    t = by_order(bot, oid)
    assert t["live"]["state"] == "done" and abs(bot.live_reserved(t)) < 1e-9 and any("有效期已过" in e["what"] for e in t["live"]["events"]), t["live"]
    # ...as do the records of before: a maker signed for its two hours, one of before the expiry was recorded at all; a taker's five
    # minutes from its placement, whatever it is signed for now; a maker with no end to go by, nothing (Predict's word, an operator)
    settle = {"key": "HSI", "target": "2026-10-05", "line": 24600.0, "close_ms": CLOSE}
    deadline = lambda maker, settle, **st: bot.live_order_deadline({"maker": maker, "settle": settle, "live": st})
    assert deadline(True, settle, want={"expires": END + 7200}) == deadline(True, settle, want={}) == deadline(True, settle, want={"expires": L.FAR_FUTURE}) == HELD
    assert deadline(False, settle, want={"expires": END + 60}, placed_at=CLOSE - 30_000) == CLOSE + 270_000
    assert deadline(True, {}, want={"expires": L.FAR_FUTURE}) == 0


async def resume_rearms_loss_stop():
    """LIVE-RESUME-KILL: /live resume — and the control page's 恢复 — lifts today's loss stop (README: 可提前解除). It also took
    the stop away until midnight (Beijing): a day with a record, tripped or resumed, was skipped whole by the check and by the
    send-time recheck, so after one resume no loss of any size stopped the trader (−$294 in a day at a $50 cap in the audit's
    run), and a cap lowered afterwards changed nothing. The stop is now armed again a cap's worth under the P&L the resume
    found (live_loss_floor): the losses the operator went on with never trip it again (test_trading pins that, and it holds),
    the next cap's worth does — refused, withdrawn and announced like the first, and lifted by another resume in turn. The
    answer says that a stop was lifted and where it stands now; the page asks before it lifts one; and a pause no longer hides
    a stop behind it (one command ends both). The pre-open mode's own loss check is not lifted by the resume, as before: the
    conservative side — said in the answer, not changed."""
    import types
    import preopen as P
    BJ, market_json, book = T["BJ"], T["market_json"], T["book"]
    today = m.beijing_day(NOW / 1000)
    creates = lambda bot: len([c for c in bot.live.api.calls if c[0] == "create"])
    total = lambda bot: bot.live_daily_pnl(bot.sim_trades(), bot.market.now_ms()) + bot.preopen_pnl()  # the figure the stop goes by
    check = lambda bot: bot.live_check_kill(bot.sim_trades(), bot.market.now_ms())  # as every step and every sync does
    resume = lambda bot: bot.cmd_live(m.Request("/live", ["resume"], 1, 0, 1))
    RESUMED = "▶️ 真实交易已恢复：满足条件的建议会重新下单"
    lifted = lambda found, line: f"{RESUMED}；同时解除了今日亏损停机：今日已结算 {found}，再亏到 {line} 会再次停机。"

    # --- the whole path, as the audit ran it: real orders, filled by the exchange, settled by the bot, the stop tripped by its step ---
    def card(n, at, close_ms):  # a daily card of its own: 涨 fair at 70¢ and asked at 58¢ (a taker's buy), settled by note_outcome("K<n>")
        return m.SimMarket(f"mkt{n}", f"mkt{n}", "close", f"K{n}", 0.70, book([], [("0.58", "400")], at, f"mkt{n}", f"K{n}", mid=str(200 + n)), 0.03, "",
                           ("涨", "跌"), {"key": f"K{n}", "target": today, "line": 1.0, "close_ms": close_ms})
    # ...and a card with a bid worth joining and nothing worth taking (60¢ bid, 69¢ asked, fair 70¢): a resting order, when both ways are on
    rest = lambda at: m.SimMarket("rest", "rest", "close", "R", 0.70, book([("0.60", "100")], [("0.69", "100")], at, "rest", "R", mid="110"), 0.03, "", ("涨", "跌"), {})

    async def buy(bot, n, at, close_ms, *beside):
        """A look at card n: True when the taker's order went out — filled in full then: 100 shares at 58¢ and the fee, $58.84."""
        bot.live.api.markets[str(200 + n)] = market_json(str(200 + n))
        await step(bot, at, card(n, at, close_ms), *(mk(at) for mk in beside))
        t = bot.sim_trades().get(f"mkt{n}|up|吃")
        if t is None or not t["live"].get("order_id"):
            return False
        bot.live.api.fill(t["live"]["order_id"], 100, done=True, price=0.58)
        await step(bot, at + 10_000, card(n, at + 10_000, close_ms), *(mk(at + 10_000) for mk in beside))
        return True

    async def lose(bot, n, close_ms, *beside):  # card n closes under its line: 涨 pays nothing, settled at the step an hour after the close
        bot.note_outcome(f"K{n}", today, 0.5, "test")
        world["markets"] = [mk(close_ms + 61 * 60_000) for mk in beside]
        await step(bot, close_ms + 61 * 60_000)

    bot = make_bot(SIM_WAYS="taker")  # LIVE_MAX_DAILY_LOSS_USD $50 (make_bot)
    await bot.live_prepare()
    fake = bot.live.api
    fake.markets["110"] = market_json("110")
    assert await buy(bot, 1, BJ(10, 5, 10, 0), BJ(10, 5, 11, 0))
    await lose(bot, 1, BJ(10, 5, 11, 0))
    assert abs(total(bot) + 58.84) < 1e-6 and bot.live_killed() == "今日已结算亏损 −$58.84 达到 LIVE_MAX_DAILY_LOSS_USD $50"  # the day's first stop, as ever
    assert not await buy(bot, 2, BJ(10, 5, 12, 30), BJ(10, 5, 13, 30)) and creates(bot) == 1
    assert RESUMED in await resume(bot) and bot.live_killed() == ""
    bot.config = bot.live.config = dataclasses.replace(bot.config, sim_ways="both")  # ...and a bid resting on another market from here on (60¢ × 100)
    assert await buy(bot, 2, BJ(10, 5, 12, 31), BJ(10, 5, 13, 30), rest) and creates(bot) == 3  # lifted, as documented: the next orders go out
    maker = bot.sim_trades()["rest|up|挂"]
    oid = maker["live"]["order_id"]
    assert (maker["status"], maker["live"]["state"]) == ("resting", "open") and oid in fake.open, (maker["status"], maker["live"]["state"])
    await lose(bot, 2, BJ(10, 5, 13, 30), rest)  # −$117.68 on the day: a cap's worth and more since the resume found −$58.84
    assert abs(total(bot) + 117.68) < 1e-6, total(bot)
    # the stop trips again — it never did, whatever was lost: four more losses went out in the audit's run, −$294 on the day
    assert bot.live_killed() == "今日已结算亏损 −$117.68 达到 LIVE_MAX_DAILY_LOSS_USD $50，手动恢复时 −$58.84、之后又亏 $58.84", bot.live_killed()
    maker = next(t for t in bot.sim_trades().values() if t["live"].get("order_id") == oid)
    assert maker["status"] == "cancelled" and maker["withdrawn"]["why"] == "触发日亏损上限" and oid not in fake.open and ("remove", [oid]) in fake.calls, \
        (maker["status"], maker.get("withdrawn"), sorted(fake.open))  # the resting bid withdrawn on Predict, like the first stop's
    assert not await buy(bot, 3, BJ(10, 5, 14, 35), BJ(10, 5, 15, 30), rest) and creates(bot) == 3, creates(bot)  # nothing new goes out
    assert any(b["market"] == "mkt3" and "今日停止开新仓：今日已结算亏损 −$117.68" in b["why"] for b in bot.sim_blocks()), bot.sim_blocks()[:3]
    bot.market.now = BJ(10, 6, 0, 1)  # the next day (Beijing): no stop, the plain line again
    assert bot.live_kill_record() == {} and bot.live_killed() == "" and bot.live_loss_floor(50.0) == -50.0 and bot.live_resumed_floor() is None

    # --- the line, to the cent: positions settled at exact figures (2 × usd shares bought at 50¢, paid nothing: a loss of usd) ---
    def settled(bot, name, usd, payout=0.0):
        at = bot.market.now_ms()
        bot.sim_save([(f"{name}|up|吃", {"item": name, "label": "吃涨", "market": name, "side": "up", "maker": False, "status": "settled", "shares": 2.0 * usd,
                                         "order": 2.0 * usd, "price": 0.5, "payout": payout, "edge": 0.05, "fair": 0.55, "settled": at, "opened": at,
                                         "fills": [], "revisions": [], "live": {"state": "done", "events": []}})])

    def blocked(bot):  # the send-time recheck of an order decided a moment ago ($5: within every cap)
        at = bot.market.now_ms()
        pending = {"maker": True, "order": 10.0, "price": 0.5, "opened": at, "settle": {}, "live": {"state": "pending", "want": {"shares": 10.0}, "events": [{"at": at}]}}
        return bot.live_send_block(pending, bot.sim_trades(), at)

    class Telegram:  # the administrator's chat: what live_notify sends there
        def __init__(self): self.sent = []
        async def send(self, chat, thread, text, *more): self.sent.append(text)

    async def stops_told(bot):
        for _ in range(5):
            await asyncio.sleep(0)  # the deliveries run beside the step
        return [t for t in bot.telegram.sent if t.startswith("🛑")]

    b = make_bot()
    await b.live_prepare()
    b.telegram = Telegram()
    b.store.put("subscriptions", {"1:0": {"chat": 1, "thread": 0, "active": True}})
    record = lambda bot=b: bot.store.get("live:killed")
    settled(b, "l1", 60); check(b)
    assert b.live_killed() == "今日已结算亏损 −$60.00 达到 LIVE_MAX_DAILY_LOSS_USD $50" and len(await stops_told(b)) == 1 and b.live_resumed_floor() is None
    assert await resume(b) == lifted("−$60.00", "−$110.00")  # the answer says what was lifted, and where the stop stands now
    assert b.live_killed() == "" and (record()["resumed"], record()["resumed_pnl"], record()["resumed_at"]) == (True, -60.0, NOW), record()
    assert b.live_loss_floor(50.0) == b.live_resumed_floor() == -110.0
    check(b)
    assert b.live_killed() == "" and blocked(b) == ""  # the losses the resume found do not trip it again (test_trading's pin, kept)
    settled(b, "l2", 40); check(b)  # −$100
    assert b.live_killed() == "" and blocked(b) == ""
    settled(b, "l3", 9.5); check(b)  # −$109.50: half a dollar short of a cap's worth since the resume
    assert b.live_killed() == "" and blocked(b) == "" and len(await stops_told(b)) == 1
    settled(b, "l4", 0.5)  # −$110.00: the line
    b.sim_save([("held|up|挂", {"item": "held", "label": "挂涨", "market": "held", "side": "up", "maker": True, "status": "resting", "shares": 0.0, "order": 100.0,
                                "price": 0.3, "opened": NOW, "fills": [], "revisions": [],
                                "live": {"state": "open", "order_id": "901", "hash": "0x1", "events": [], "want": {"maker": True}}})])
    assert blocked(b) == "今日已结算亏损 −$110.00 已达 LIVE_MAX_DAILY_LOSS_USD $50"  # at the send, before any step has looked (it was skipped once resumed)
    check(b)
    assert b.live_killed() == "今日已结算亏损 −$110.00 达到 LIVE_MAX_DAILY_LOSS_USD $50，手动恢复时 −$60.00、之后又亏 $50.00", b.live_killed()
    held = b.sim_trades()["held|up|挂"]
    assert (held["status"], held["live"]["state"], held["live"]["cancel_why"]) == ("cancelled", "cancelling", "触发日亏损上限"), (held["status"], held["live"])
    assert "resumed" not in record() and b.live_resumed_floor() is None and "今日停止开新仓" in blocked(b)
    assert "今日停止开新仓" in b.live_room(b.sim_trades(), hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW), "up", 0.58, 100)
    told = await stops_told(b)  # announced again, however soon after the first notice (a keyed notice is held back LIVE_NOTE_SECONDS: the key goes with the stop)
    assert len(told) == 2 and "手动恢复时 −$60.00" in told[1] and "/live resume" in told[1], told
    check(b)
    assert len(await stops_told(b)) == 2 and "resumed" not in record()  # standing: nothing more until the operator lifts it
    assert await resume(b) == lifted("−$110.00", "−$160.00") and record()["resumed_pnl"] == -110.0  # another resume: another cap's worth, from here
    at = record()["resumed_at"]
    # ...and only a standing stop is lifted: a resume that ends a pause later in the day moves nothing (or each one would hand out a cap)
    b.market.now = NOW + 60_000
    settled(b, "l5", 5); check(b)  # −$115.00
    assert "⏸" in await b.cmd_live(m.Request("/live", ["pause", "午休"], 1, 0, 1)) and b.live_paused() == "午休"
    assert await resume(b) == RESUMED + "。" and await resume(b) == RESUMED + "。" and b.live_paused() == ""
    assert (record()["resumed_pnl"], record()["resumed_at"]) == (-110.0, at) and b.live_loss_floor(50.0) == -160.0
    # a cap lowered after the resume counts (it did nothing until midnight): $10 from what the resume found
    assert b.control_set({"LIVE_MAX_DAILY_LOSS_USD": "10"})["ok"] and b.config.live_max_daily_loss_usd == 10
    assert b.live_loss_floor(10.0) == b.live_resumed_floor() == b.live_status(b.market.now_ms())["loss_floor"] == -120.0
    check(b)
    assert b.live_killed() == "" and blocked(b) == ""
    settled(b, "l6", 5)  # −$120.00
    assert "已达 LIVE_MAX_DAILY_LOSS_USD $10" in blocked(b)
    check(b)
    assert b.live_killed() == "今日已结算亏损 −$120.00 达到 LIVE_MAX_DAILY_LOSS_USD $10，手动恢复时 −$110.00、之后又亏 $10.00" and len(await stops_told(b)) == 3
    # no limit (0): nothing to arm, and the answer does not promise a line; a limit set again counts from what the resume found
    assert b.control_set({"LIVE_MAX_DAILY_LOSS_USD": "0"})["ok"] and b.live_killed()  # (the stop that stands is not lifted by the setting)
    assert await resume(b) == f"{RESUMED}；同时解除了今日亏损停机：今日已结算 −$120.00，现在不设亏损上限（LIVE_MAX_DAILY_LOSS_USD=0）。"
    settled(b, "l7", 30); check(b)  # −$150.00
    assert b.live_killed() == "" and blocked(b) == "" and b.live_resumed_floor() is None
    assert b.control_set({"LIVE_MAX_DAILY_LOSS_USD": "50"})["ok"] and b.live_loss_floor(50.0) == -170.0
    check(b)
    assert b.live_killed() == "" and blocked(b) == ""
    settled(b, "l8", 20); check(b)  # −$170.00
    assert b.live_killed().startswith("今日已结算亏损 −$170.00 达到 LIVE_MAX_DAILY_LOSS_USD $50，手动恢复时 −$120.00") and len(await stops_told(b)) == 4

    # a day in profit when the stop is lifted keeps the plain line: never tighter than a day that had no stop
    p = make_bot()
    await p.live_prepare()
    settled(p, "l1", 60); check(p)
    settled(p, "w1", 70, payout=1.0)  # +$70 settles behind the stop: +$10 on the day
    assert p.live_killed() and await resume(p) == lifted("+$10.00", "−$50.00") and p.live_loss_floor(50.0) == -50.0
    settled(p, "l2", 50); check(p)  # −$40.00
    assert p.live_killed() == "" and blocked(p) == ""
    settled(p, "l3", 10); check(p)  # −$50.00
    assert p.live_killed() == "今日已结算亏损 −$50.00 达到 LIVE_MAX_DAILY_LOSS_USD $50，手动恢复时 +$10.00、之后又亏 $60.00", p.live_killed()

    # a stop resumed by the version before (the record has no figure): counted from the first look after the upgrade
    o = make_bot()
    await o.live_prepare()
    settled(o, "l1", 70)
    o.store.put("live:killed", {"day": today, "why": "今日已结算亏损 −$60.00 达到 LIVE_MAX_DAILY_LOSS_USD $50", "at": NOW, "resumed": True, "resumed_at": NOW})
    assert o.live_killed() == "" and blocked(o) == "" and o.live_resumed_floor() is None  # (no figure yet: as that version left it)
    check(o)
    assert o.live_killed() == "" and record(o)["resumed"] and record(o)["resumed_pnl"] == -70.0 and o.live_loss_floor(50.0) == o.live_resumed_floor() == -120.0
    settled(o, "l2", 49); check(o)
    assert o.live_killed() == "" and blocked(o) == ""
    settled(o, "l3", 1); check(o)  # −$120.00
    assert o.live_killed().startswith("今日已结算亏损 −$120.00 达到 LIVE_MAX_DAILY_LOSS_USD $50，手动恢复时 −$70.00"), o.live_killed()
    o.market.now = NOW + 24 * 3_600_000  # the next day: yesterday's record and yesterday's losses are no part of it
    assert o.live_kill_record() == {} and o.live_loss_floor(50.0) == -50.0 and blocked(o) == ""
    settled(o, "n1", 49); check(o)
    assert o.live_killed() == ""
    settled(o, "n2", 1); check(o)
    assert o.live_killed() == "今日已结算亏损 −$50.00 达到 LIVE_MAX_DAILY_LOSS_USD $50"

    # --- the control page: a stop that trips behind a pause is shown beside it, and 恢复 asks before it lifts one ---
    c = make_bot()
    await c.live_prepare()
    assert (await c.control_action({"action": "pause", "why": "午休"}))["ok"]
    settled(c, "l1", 60); check(c)
    st = c.live_status(NOW)
    assert st["paused"] == "午休" and st["killed"] == "今日已结算亏损 −$60.00 达到 LIVE_MAX_DAILY_LOSS_USD $50" and st["loss_floor"] is None
    assert "⏸ 已暂停（午休）｜🛑 今日停开新仓（今日已结算亏损 −$60.00 达到 LIVE_MAX_DAILY_LOSS_USD $50）" in c.live_text(NOW), c.live_text(NOW).split("\n")[0]
    assert "真实交易 已暂停、今日停开新仓｜" in c.status("1:0"), c.status("1:0")
    got = await c.control_action({"action": "resume"})  # the page's 恢复 (the same code as /live resume)
    assert got == {"ok": True, "message": lifted("−$60.00", "−$110.00")}, got
    st = c.live_status(NOW)
    assert (st["paused"], st["killed"], st["loss_floor"], st["caps"]["daily_loss"]) == ("", "", -110.0, 50), (st["paused"], st["killed"], st["loss_floor"])
    text = c.live_text(NOW)
    assert "🟢 运行中" in text.split("\n")[0] and "今日已结算盈亏 −$60.00（上限 −$50；今天手动恢复过，停机线 −$110.00）" in text, text
    assert "真实交易 运行中｜" in c.status("1:0")
    page = m.CONTROL_PAGE
    assert 'else if(a==="resume"){const k=data&&data.live&&data.live.killed;if(!k||confirm("今日亏损停机正在生效（"+k+"）' in page  # asked first while a stop stands
    assert '"已暂停："+L.paused+(L.killed?"｜今日停开新仓："+L.killed:"")' in page and 'L.paused?(L.killed?"已暂停｜今日停开新仓":"已暂停")' in page
    assert 'L.loss_floor!=null?"（今天手动恢复过，停机线 "+money(L.loss_floor)+"）"' in page

    # --- the pre-open mode: its day's P&L is in the figure the stop goes by, before the resume and after; its own check stays ---
    f = make_bot()
    await f.live_prepare()
    day = {"pnl": -30.0}
    f.preopen = types.SimpleNamespace(alive=True, daily_pnl=lambda: day["pnl"], manual_pause=True, ledger=types.SimpleNamespace(pause=lambda value=None: False),
                                      withdrawing=lambda: "已达到今日真实亏损上限")
    settled(f, "l1", 30); check(f)  # −$30 of the model's and −$30 of the mode's
    assert f.live_killed() == "今日已结算亏损 −$60.00 达到 LIVE_MAX_DAILY_LOSS_USD $50"
    assert await resume(f) == lifted("−$60.00", "−$110.00") + "开盘前预挂模式仍不挂单：已达到今日真实亏损上限。"
    assert f.preopen.manual_pause is False and record(f)["resumed_pnl"] == -60.0  # (the mode's manual pause ends with the command, as documented)
    day["pnl"] = -79.5; check(f)
    assert f.live_killed() == "" and blocked(f) == ""
    day["pnl"] = -80.0
    assert "已达 LIVE_MAX_DAILY_LOSS_USD" in blocked(f)
    check(f)
    assert f.live_killed().startswith("今日已结算亏损 −$110.00 达到 LIVE_MAX_DAILY_LOSS_USD $50，手动恢复时 −$60.00"), f.live_killed()
    # the mode itself (preopen.Controller, its loop not started): held by the stop, and by its own check of the day's loss once
    # the stop is lifted — the model trader goes on, the mode stays out for the day, as before (PREOPEN.md: 日亏损上限 withdraws it)
    r = make_bot(PREOPEN_15M="on", PREOPEN_DB=":memory:")
    await r.live_prepare()
    r.preopen = P.Controller(r)
    try:
        settled(r, "l1", 60); check(r)
        assert r.preopen_active() and r.live_killed() and r.preopen.withdrawing() == r.live_killed()
        assert await resume(r) == lifted("−$60.00", "−$110.00") + "开盘前预挂模式仍不挂单：已达到今日真实亏损上限。"
        assert r.live_killed() == "" and blocked(r) == "" and r.preopen.withdrawing() == "已达到今日真实亏损上限"
    finally:
        r.preopen.ledger.close()


asyncio.run(place_timeout_race())
asyncio.run(account_check_rpc_down())
asyncio.run(double_cancel())
asyncio.run(order_test_limits())
asyncio.run(maker_expiry())
asyncio.run(resume_rearms_loss_stop())
print("AUDIT_LIVE_A_OK")
