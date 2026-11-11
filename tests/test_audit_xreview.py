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


async def resume_preopen_anchor():
    """XR-RESUME-PREOPEN-ANCHOR: /live resume keeps the day's counted P&L (the model trader's + the pre-open mode's) and arms the
    stop a cap's worth under it. The pre-open part counts the mode's day while it runs, its loss alone while it has crashed, LIVE≠on
    or it is switched off (PRE-PNL-DROP): a resume taken with the mode at +$60, then a crash, re-trips the stop with no trade, and
    the notice / /live header said "之后又亏 $60.00" — a loss that never happened. The stop stays (conservative); the text now says
    where the change came from. A day whose pre-open part did not move keeps the plain wording."""
    import types

    def bot_with(pre_day):
        bot = make_bot()
        bot.notes = []
        bot.live_notify = lambda text, key="": bot.notes.append(text)
        bot.preopen = types.SimpleNamespace(alive=True, daily_pnl=lambda: pre_day, winding="", manual_pause=False, withdrawing=lambda: "",
                                            ledger=types.SimpleNamespace(pause=lambda value=None: False))
        trades = {}
        bot.sim_trades = lambda: trades
        return bot, trades

    def model_day(trades, pnl, now):  # the model trader's settled day: one record settled now with this P&L
        trades.clear()
        trades["m1"] = {"live": {"state": "done"}, "status": "settled", "settled": now - 1000, "payout": 0.0 if pnl < 0 else 1.0,
                        "price": 0.5, "shares": abs(pnl) / 0.5}

    # the mode running at +$60, the model trader at −$110: −$50 counted, the stop trips; resumed, the line is −$100
    bot, trades = bot_with(60.0)
    model_day(trades, -110.0, NOW)
    bot.live_check_kill(trades, NOW)
    assert bot.live_killed().startswith("今日已结算亏损 −$50.00"), bot.live_killed()
    reply = await bot.live_action("resume", [], NOW)
    assert "再亏到 −$100.00 会再次停机" in reply, reply
    assert bot.live_killed() == "" and bot.live_loss_floor(50.0) == -100.0
    rec = bot.live_kill_record()
    assert rec["resumed_pnl"] == -50.0, rec
    # the mode crashes, no trade: its gain counts 0, −$110 counted — the stop trips again (kept: conservative) ...
    bot.preopen.alive = False
    bot.live_check_kill(trades, NOW + 1000)
    why = bot.live_killed()
    assert why, "the conservative re-trip is kept"
    # ... and says what moved, not a loss that never happened
    assert "之后又亏 $60.00" not in why, why
    assert "原策略 −$110.00" in why and "预挂 +$60.00" in why and "预挂 +$0.00" in why and "预挂未运行" in why, why
    assert why in bot.notes[-1], bot.notes
    assert rec.get("resumed_own") == -110.0 and rec.get("resumed_preopen") == 60.0, rec  # the parts kept with the resume

    # control: the mode running throughout, the model trader −$110 → −$160: the plain wording, unchanged
    bot, trades = bot_with(60.0)
    model_day(trades, -110.0, NOW)
    bot.live_check_kill(trades, NOW)
    await bot.live_action("resume", [], NOW)
    model_day(trades, -159.0, NOW + 1000)
    bot.live_check_kill(trades, NOW + 1000)
    assert bot.live_killed() == ""
    model_day(trades, -160.0, NOW + 2000)
    bot.live_check_kill(trades, NOW + 2000)
    assert bot.live_killed() == "今日已结算亏损 −$100.00 达到 LIVE_MAX_DAILY_LOSS_USD $50，手动恢复时 −$50.00、之后又亏 $50.00", bot.live_killed()


async def hash_read_error_409_adopt():
    """XR-409-HASH-ERROR follow-up: a taker's order Predict DID take answers a bare 409; the open list omits it and its hash read
    errors past the placing grace, so it turns "unknown" (the fix above). Both recover and live_recheck adopts it as open — but
    without a 下单时间 (placed_at): live_taker_due's clock read 0 for ever and the unfilled rest stayed on the book until its signed
    expiry instead of being cancelled LIVE_TAKER_WAIT_SECONDS (60 s) after it was found, as live_reconcile's adoption does."""
    close = T["CLOSE"]
    deep = lambda at: m.SimMarket("deep", "deep", "close", "X", 0.70,
                                  T["book"]([], [("0.58", "100"), ("0.70", "1000")], at, "deep", mid="103"), 0.03, "", ("涨", "跌"),
                                  {"key": "X", "target": "2026-10-05", "line": 1.0, "close_ms": close})
    bot = make_bot(SIM_WAYS="taker")
    await bot.live_prepare()
    api = bot.live.api
    api.markets["103"] = T["market_json"]("103")
    world["markets"] = [deep(NOW)]
    await bot.live_prefetch(NOW)
    real_create, real_orders, real_order = api.create_order, api.orders, api.order
    hide = {"on": True}

    async def create(body):  # Predict took it, the answer is a bare 409
        await real_create(body)
        raise m.RemoteError("HTTP 409: Conflict")

    async def orders(status="OPEN", **p):
        rows = await real_orders(status, **p)
        return [r for r in rows if not hide["on"]]

    async def order(oid, hash_=""):
        if hide["on"]:
            raise m.RemoteError("HTTP 503: Service Unavailable")
        return await real_order(oid, hash_)
    api.create_order, api.orders, api.order = create, orders, order
    await step(bot, NOW, deep(NOW))
    key = next(k for k in bot.sim_trades() if k.startswith("deep|up|吃"))
    st = bot.sim_trades()[key]["live"]
    assert st["state"] == "placing" and not st.get("placed_at"), st
    for s in (30, 60, 90, 130):
        await step(bot, NOW + s * 1000, deep(NOW + s * 1000))
    assert st["state"] == "unknown", (st["state"], st.get("last_error"))
    hide["on"] = False
    found = NOW + 140_000
    await step(bot, found, deep(found))
    assert st["state"] == "open" and st.get("placed_at") == found, (st["state"], st.get("placed_at"))
    removes = lambda: [c for c in api.calls if c[0] == "remove"]
    await step(bot, found + 30_000, deep(found + 30_000))
    assert removes() == [], removes()  # not before the taker wait
    for s in (80, 140):
        await step(bot, found + s * 1000, deep(found + s * 1000))
    # before: still "open", placed_at None, nothing removed — the unfilled rest left on the book until its expiry
    assert removes() == [("remove", [st["order_id"]])], (st["state"], st.get("placed_at"), removes())
    assert st["state"] == "done", st["state"]
    assert len([c for c in api.calls if c[0] == "create"]) == 1  # never re-sent


asyncio.run(hash_read_error_409())
asyncio.run(hash_read_error_409_adopt())
asyncio.run(sendrefused_reconcile())
asyncio.run(resume_preopen_anchor())


def ev_sigma_levels():
    """XR-EV-SIGMA-LEVELS: SIM-EV-SIGMA netted Up against Down inside one record only. With PREOPEN_LEVELS (and re-quotes) one
    window is several records — <window>#<level>, superseded '@<attempt>' — all settled by the same coin flip, so each still
    became its own bet: a window hedged across levels kept the phantom variance and the double count toward EV_MIN_BETS, and
    same-side levels were taken for independent flips. preopen.ev_bets now makes one bet per window: variance (Σup − Σdown)² / 4."""
    import datetime as dt
    import preopen as P
    at = int(dt.datetime(2026, 11, 10, 12, 0, tzinfo=m.BEIJING).timestamp() * 1000)

    def row(i, suffix, price, up, down, result, attempt=0):
        return {"key": "BTC-15M", "slug": f"btc-updown-15m-{i}{suffix}", "name": "BTC 15分涨跌 10:00–10:15", "price": str(price),
                "shares": "20", "start_ms": at - (i + 1) * 900_000, "phase": "closed", "accounted": True, "result": result,
                "attempt": attempt, "orders": {"up": {"state": "final", "filled": up}, "down": {"state": "final", "filled": down}}}

    # level 0 filled Up 20 at 49¢, level 1 filled Down 20 at 48¢, up won: one hedged bet, no variance
    bets = P.ev_bets([row(1, "", 0.49, 20.0, 0.0, "up"), row(1, "#1", 0.48, 0.0, 20.0, "up")])
    assert len(bets) == 1, bets
    b = bets[0]
    assert (b["shares"], b["var"], round(b["ev"], 9), round(b["pnl"], 9)) == (40.0, 0.0, 0.6, 0.6), b
    # two levels both Up: the same flip, perfectly correlated -> 40² / 4, not 2 × 100
    bets = P.ev_bets([row(2, "", 0.49, 20.0, 0.0, "down"), row(2, "#1", 0.48, 20.0, 0.0, "down")])
    assert len(bets) == 1 and (bets[0]["shares"], bets[0]["var"]) == (40.0, 400.0), bets
    # a superseded attempt '@1', level 0 and level '#1' of the same window: still one bet
    assert len(P.ev_bets([row(3, "@1", 0.49, 10.0, 0.0, "up", 1), row(3, "", 0.49, 0.0, 10.0, "up"),
                          row(3, "#1", 0.48, 0.0, 0.0, "up")])) == 1
    # different windows stay separate bets; a one-sided window keeps filled² / 4
    bets = P.ev_bets([row(4, "", 0.49, 20.0, 0.0, "up"), row(5, "", 0.49, 20.0, 0.0, "up")])
    assert len(bets) == 2 and [x["var"] for x in bets] == [100.0, 100.0], bets


ev_sigma_levels()


async def prelate_crashed():
    """XR-PRELATE-CRASHED: PRE-LATE-FILL leaves the final read of an order off the book to the cancellation loop, which a
    crashed controller (attached, alive False) lacks. An operator's withdrawal on it (/preopen pause) left its orders
    "removed", and with the mode off preopen_leftovers counted only orders that may rest: no wind-down controller ever read
    them, their outcomes held the model trader's positions check for 3 days. Now they start one, which reads them a minute
    on, accounts the pairs and stops itself — and is not started again."""
    import tempfile, types
    import preopen as P
    import test_preopen as TP
    with tempfile.TemporaryDirectory() as folder:
        db = str(pathlib.Path(folder) / "x.preopen.sqlite3")
        rows = [TP.market(at=TP.NOW + k * 15 * 60_000, mid=200 + k) for k in (1, 2, 3)]
        env = dict(PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="1440", PREOPEN_SHARES="10", PREOPEN_DB=db)
        ctl, api, clock = TP.controller(rows, **env)
        for r in rows:
            await ctl.place_pair(P.candidate(r, TP.NOW, ctl.owner.config, ctl.settings))
        ctl.alive = False  # crashed with its orders still resting; still attached
        bot = L.LiveBot.__new__(L.LiveBot)
        bot.preopen, bot.store, bot.market = ctl, m.Store(":memory:"), types.SimpleNamespace(now_ms=lambda: clock.ms)
        await bot.cmd_preopen(m.Request("/preopen", ["pause"], 1, 0, 1))
        await asyncio.gather(*list(ctl.settling.values()), return_exceptions=True)
        states = {o["state"] for x in ctl.ledger.rows.values() for o in x["orders"].values()}
        assert states == {"removed"}, states
        ctl.ledger.close()
        clock.ms += 5 * 60_000  # no loop on a dead controller: still unread minutes on
        assert len(P.unverified_outcomes(P.read_rows(db), clock.ms)) == 6
        off = L.LiveBot.__new__(L.LiveBot)
        off.config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "STATE_DB": ":memory:", "LIVE": "on",
                                        "PREDICT_PRIVATE_KEY": "a" * 64, "PREOPEN_15M": "off", "PREOPEN_DB": db})
        off.market = types.SimpleNamespace(now_ms=lambda: clock.ms)
        assert off.preopen_leftovers() == 6, off.preopen_leftovers()  # was 0: nothing would ever read them with the mode off
        # preopen_apply with the mode off starts a wind-down controller for them
        made = []

        class FakeCtl:
            def __init__(self, owner):
                self.owner, self.settings, self.alive, self.stop, self.error, self.winding = owner, owner.config.preopen, True, asyncio.Event(), "", ""
                self.ledger = types.SimpleNamespace(close=lambda: None)
                made.append(self)

            def wind_down(self, reason):
                self.winding = reason

            async def run(self):
                try:
                    await self.stop.wait()
                finally:
                    self.alive = False
        off.stopping, off.preopen, off.preopen_task, off.preopen_factory, off.preopen_lock = asyncio.Event(), None, None, FakeCtl, asyncio.Lock()
        off.preopen_tidied = 1e18  # (no archive pass in this check)
        await off.preopen_apply()
        assert len(made) == 1 and made[0].winding == "预挂模式关着，但上次还有挂单没撤完", [c.winding for c in made]
        made[0].stop.set()
        await asyncio.gather(off.preopen_task, return_exceptions=True)
        # the real wind-down controller over the same file: reads them a minute on, accounts the pairs, stops itself
        ctl2, _, _ = TP.controller(rows, **env)
        ctl2.owner.market.now_ms = lambda: clock.ms
        ctl2.api = ctl2.cancel_api = api
        api.ctl = ctl2
        ctl2.adopt()
        ctl2.wind_down("预挂模式关着，但上次还有挂单没撤完")
        task = asyncio.create_task(ctl2.cancellation_loop())
        await asyncio.sleep(0.3)
        clock.ms += P.LATE_FILL_MS + 1000
        await asyncio.wait_for(ctl2.stop.wait(), 10)
        task.cancel()
        await asyncio.gather(task, *list(ctl2.settling.values()), return_exceptions=True)
        live_rows = list(ctl2.ledger.rows.values())
        assert {o["state"] for x in live_rows for o in x["orders"].values()} == {"final"}
        assert all(x.get("accounted") for x in live_rows) and not P.unverified_outcomes(live_rows, clock.ms)
        ctl2.ledger.close()
        assert off.preopen_leftovers() == 0  # nothing left: not started again


asyncio.run(prelate_crashed())


def daily_pnl_perf():
    """XR-DAILY-PNL-PERF: live_daily_pnl (the daily loss stop's figure, asked ~3 times a second by the running pre-open's
    withdrawing() and on every step and send) read shares and walked a generator over the revisions of EVERY live record settled
    before today — the store keeps them all, ~4.6x the 1.149.0 cost per call. An old record without revisions adds nothing, so it
    is not looked at any more (shares never read), and the figure stays: today's records, today's corrections' losses only."""
    class Counted(dict):
        reads = 0

        def __getitem__(self, k):
            if k == "shares":
                Counted.reads += 1
            return super().__getitem__(k)

    now, day = 1_762_800_000_000, 86_400_000  # 2025-11-11 02:40 Beijing
    bot = L.LiveBot.__new__(L.LiveBot)
    old = lambda **kw: Counted(live={"state": "done"}, status="settled", settled=now - 3 * day, payout=1.0, price=0.5, shares=10.0, **kw)
    trades = {f"old{i}": old() for i in range(1000)}
    trades["old_empty"] = old(revisions=[])
    assert bot.live_daily_pnl(trades, now) == 0.0
    assert Counted.reads == 0, f"shares read {Counted.reads} times for old records without revisions"
    # the figure itself: a record settled today (-4), an old one corrected today 1 -> 0 (-6), one corrected in the bot's favour
    # today (not counted), one corrected yesterday (not counted)
    rec = lambda settled, payout, shares, revisions=None: dict(live={"state": "done"}, status="settled", settled=settled,
                                                               payout=payout, price=0.5 if revisions else 0.4, shares=shares,
                                                               **({"revisions": revisions} if revisions else {}))
    trades.update({
        "today": rec(now - 60_000, 0.0, 10.0),
        "lost": rec(now - day, 0.0, 6.0, [{"at": now - 60_000, "from": 1.0, "to": 0.0}]),
        "gained": rec(now - day, 1.0, 6.0, [{"at": now - 60_000, "from": 0.0, "to": 1.0}]),
        "yday": rec(now - 2 * day, 0.0, 6.0, [{"at": now - day, "from": 1.0, "to": 0.0}]),
    })
    got = bot.live_daily_pnl(trades, now)
    assert abs(got - (-4.0 - 6.0)) < 1e-9, got


daily_pnl_perf()


def driver_windows_perf():
    """XR-DRIVER-WINDOWS-PERF: since every 15-minute window record settles on its pair's driver (BTCUSDT), the driver index
    (TradeMap.of_driver) handed every BTC/ETH/BNB market's group check (sim_group_room) the pair's whole window history —
    settled, expired, withdrawn paper attempts, never pruned — ~5 ms a call at 50k records, ~8 calls per candidate under a
    binding cap. The index now holds only the records that may still count (driver_may_count); the group cap's answers stay
    those of the full walk."""
    import random, time, types
    touch = next(s for s in m.TOUCH_MARKETS if s.symbol == "BTCUSDT")
    card = next(s for s in m.WINDOW_UPDOWN_MARKETS if s.key == "BTC-15M")
    at = 1_794_000_000_000

    def window(i, status, shares=0.0, order=10.0, live=None, side="up"):
        spec = card.window_spec(at - i * 900_000)
        t = {"kind": "updown", "key": "BTC-15M", "market": spec.slug, "side": side, "price": 0.5, "shares": shares, "order": order,
             "status": status, "settle": {"end": spec.end_ms, "start": spec.start_ms, **card.tag(spec)}, "item": spec.name}
        if live is not None:
            t["live"] = live
        return t

    plain = {}
    for i in range(200):
        plain[f"w{i}|up|挂"] = window(i, "settled", shares=10.0)
    for i in range(50):
        plain[f"x{i}|up|挂"] = window(i, "expired")
        plain[f"c{i}|up|挂#1"] = window(i, "cancelled")  # a withdrawn paper attempt: nothing can fill it
    plain["touch|up|吃"] = {"kind": "touch", "key": touch.key, "market": "btc-touch", "side": "up", "price": 0.3, "shares": 20.0,
                           "order": 20.0, "status": "filled", "settle": {"deadline": touch.deadline_ms}}
    assert {m.trade_driver(t)[0] for t in plain.values()} == {"BTCUSDT"}
    tm = m.TradeMap(plain)
    got = m.trades_of_driver(tm, "BTCUSDT")
    assert got == [plain["touch|up|吃"]], f"the driver index walks {len(got)} records, 1 can count"
    assert m.trades_of_driver(dict(plain), "BTCUSDT") == got  # the scan says the same
    # a withdrawn record whose real order is done stays: live_readopt can revive it in place while the index stands
    plain["r|up|挂"] = window(0, "cancelled", live={"state": "done"})
    assert plain["r|up|挂"] in m.TradeMap(plain).of_driver("BTCUSDT")

    # the group cap's answers: the index against the full walk of every record of the driver, over random records
    class Full(dict):
        def of_driver(self, driver):
            return [t for t in self.values() if m.trade_driver(t)[0] == driver]

    rnd = random.Random(7)
    bot = types.SimpleNamespace(config=types.SimpleNamespace(sim_group_usd=40.0), sim_resting_cost=lambda t: float(t.get("price") or 0),
                                sim_group_order=lambda mk, price, shares, taker=None: (price, shares))
    mk = m.SimMarket("t", "BTC 先触", "touch", touch.key, 0.5, None, 0.0, "", ("x", "y"), {"deadline": touch.deadline_ms})
    refusals = 0
    for it in range(1500):
        recs = {}
        for j in range(rnd.randint(0, 12)):
            order = rnd.choice([10.0, 50.0, 100.0])
            live = None
            if rnd.random() < 0.5:
                live = {"state": rnd.choice(["done", "open", "cancelling", "pending", "unknown", "placing"]), "order_id": "1"}
                if rnd.random() < 0.3:
                    live["fills_pending"] = time.time() * 1000
            recs[f"r{j}|挂"] = window(rnd.randint(-3, 3), rnd.choice(["filled", "resting", "cancelled", "settled", "expired"]),
                                      shares=rnd.choice([0.0, order / 2, order]), order=order, live=live, side=rnd.choice(["up", "down"]))
            recs[f"r{j}|挂"]["price"] = rnd.random()
        side, price, shares = rnd.choice(["up", "down"]), rnd.random(), rnd.choice([10.0, 60.0, 100.0])
        want = m.Bot.sim_group_room(bot, Full(recs), mk, side, price, shares)
        assert m.Bot.sim_group_room(bot, m.TradeMap(recs), mk, side, price, shares) == want, (it, recs)
        assert m.Bot.sim_group_room(bot, dict(recs), mk, side, price, shares) == want, (it, recs)
        refusals += bool(want)
    assert refusals > 100, refusals



def stock_close_recalc():
    """XR-STOCK-CLOSE-RECALC: note_outcome measured the index σ factors again (index_sigma_calibs, most of a second on a full
    store, on the event loop) at every new outcome, a stock card's too, although only the three indices' own snapshots and
    outcomes are its inputs. record_predictions now notes the stock cards' closes every second, SIM / PREDICT off as well, so
    each stock close paid a full measurement for the same factors. Only an index's new outcome measures them now; the stock
    outcome and its source are still written, and an index outcome gives exactly a fresh measurement."""
    import datetime as dt, math, os, random, tempfile
    store = m.Store(os.path.join(tempfile.mkdtemp(), "x.db"))
    rnd = random.Random(3)
    t0 = int(dt.datetime(2026, 9, 1, tzinfo=m.BEIJING).timestamp() * 1000)
    rows = []
    for i in range(40):
        day = (dt.date(2026, 9, 1) + dt.timedelta(days=i)).isoformat()
        for key in ("HSI", "KOSPI", "SSE", "SKHYNIXUSDT"):
            if i < 39:
                rows.append((f"outcome:{key}:{day}", 100 * math.exp(rnd.gauss(0, .01))))
            for k in range(6):
                t = t0 + i * 86_400_000 + k * 3_600_000
                rows.append((f"pred:{key}:{t}", {"key": key, "t": t, "target": day, "mode": "盘中" if k % 2 else "盘后", "ref": 100.0,
                                                 "ref_raw": 100.0, "up": .5, "move": .001, "beta": 1.0, "sigma": .01, "R": .5}))
    store.put_many(rows)
    store.flush()

    class Clock:
        config = None

        def now_ms(self):
            return t0 + 41 * 86_400_000

    bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "SSE_INDEX": "off"}),
                store, Clock(), None)
    assert bot.config.sigma_calib and bot.vols.calib, "SIGMA_CALIB on by default: factors measured at the start"
    calls, real = [], bot.index_sigma_calibs
    bot.index_sigma_calibs = lambda: calls.append(1) or real()
    start = dict(bot.vols.calib)
    last = (dt.date(2026, 9, 1) + dt.timedelta(days=39)).isoformat()
    bot.note_outcome("SKHYNIXUSDT", last, 101.0, "t")
    bot.note_outcome("S1", last, 99.0, "t")
    assert len(calls) == 0, f"stock closes measured the index σ factors {len(calls)}x"
    assert bot.vols.calib == start
    assert bot.store.get(f"outcome:SKHYNIXUSDT:{last}") == 101.0 and bot.store.get(f"outcome:S1:{last}") == 99.0
    assert bot.store.get(f"outsrc:SKHYNIXUSDT:{last}")["value"] == 101.0 and bot.store.get(f"outsrc:S1:{last}")["source"] == "t"
    bot.note_outcome("HSI", last, 101.0, "t")
    assert len(calls) == 1 and bot.vols.calib != start and bot.vols.calib == real()
    bot.note_outcome("HSI", last, 101.0, "t")  # the same value again: nothing new to measure
    assert len(calls) == 1


driver_windows_perf()
stock_close_recalc()


async def startup_notice_lost():
    """XR-STARTUP-NOTICE-LOST: with Telegram out of reach at the start the bot now runs on (CTRL-TELEGRAM-STARTUP), and the one-off
    start notices — the saved page settings this start dropped (CTRL-PRUNE, already removed from the store at __init__) and
    LiveBot's 🧪 LIVE=pause / 💰 真实交易已启动 — were tried once during the outage, failed (Bot.tell swallows it) and were never sent
    again. They now wait for the start-up check to pass and go out once Telegram is back; a stop during the outage ends them."""
    import logging, socket, time
    WEI = 10 ** 18
    dns = lambda: m._net_error(socket.gaierror(-3, "Temporary failure in name resolution"))

    class Market(m.Binance):
        def now_ms(self): return int(time.time() * 1000)
        async def sync_clock(self): pass
        async def prices(self): return {"UNITREEUSDT": {"symbol": "UNITREEUSDT", "price": "73.13", "time": self.now_ms()}}
        async def get(self, path, **p): return []

    class Tg:
        def __init__(self):
            self.down, self.calls, self.sent = dns, [], []
        async def call(self, method, payload=None, timeout=15):
            self.calls.append(method)
            if self.down is not None:
                raise self.down()
            if method == "getWebhookInfo": return {"url": ""}
            if method == "getMe": return {"username": "AuditBot"}
            if method == "getUpdates":
                await asyncio.sleep(0.02)
                return []
            return True
        async def send(self, chat, thread, text, reply_markup=None, parse_mode=None):
            if self.down is not None:
                raise self.down()
            self.sent.append((chat, text))

    class Api:
        def __init__(self):
            self.jwt, self.jwt_exp, self.auth_error, self.lookup = "t", 0, "", ""
        async def ensure_auth(self): pass
        async def remove_orders(self, ids): return {"removed": [], "noop": list(ids)}
        async def orders(self, status="OPEN", **params): return []
        async def order(self, oid, hash_=""): return None
        async def positions(self): return []

    class Chain:
        async def usdt_balance(self, owner=None): return 500 * WEI
        async def bnb_balance(self, owner=None): return 10 ** 17
        async def allowance(self, key, owner=None): return 10 ** 30

    quiet = {"TELEGRAM_BOT_TOKEN": "1:x", "ADMIN_USER_ID": "42", "SYMBOLS": "UNITREEUSDT", "BASELINE_MODE": "manual", "HSI_FUTURES": "off",
             "KOSPI_INDEX": "off", "SSE_INDEX": "off", "PREDICT": "off", "BNB_TOUCH": "off", "SIM": "off", "PROBABILITY": "off",
             "WEB": "off", "POLL_SECONDS": "3", "STATE_DB": ":memory:"}

    async def until(cond, seconds=8.0):
        end = time.monotonic() + seconds
        while not cond() and time.monotonic() < end:
            await asyncio.sleep(0.01)
        return cond()

    async def scenario(cls, env, saved, recover=True, live=False):
        tg = Tg()
        cfg = m.Config.from_env({**quiet, **env})
        store = m.Store(":memory:")
        store.put("control:env", saved)
        store.put("subscriptions", {"42:0": {"chat": 42, "thread": 0, "active": True}})
        bot = cls(cfg, store, Market(cfg), tg)
        if live:
            bot.live.api, bot.live.chain = Api(), Chain()
        assert bot.control_dropped, "a saved value that no longer validates is dropped at __init__"
        real_wait = bot.wait

        async def wait(seconds):  # the commands loop's back-off cut to a moment
            if asyncio.current_task().get_name() == "commands":
                seconds = min(seconds, 0.02)
            await real_wait(seconds)
        bot.wait = wait
        runner = asyncio.create_task(bot.run())
        assert await until(lambda: bot.last_cycle > 0 or runner.done()) and not runner.done(), "run() is not blocked by Telegram"
        await asyncio.sleep(0.3)
        assert not tg.sent
        if recover:
            tg.down = None  # Telegram answers again
            assert await until(lambda: bot.telegram_ok and "getUpdates" in tg.calls)
            await until(lambda: any("网页控制台保存的设置" in t for _, t in tg.sent)
                        and (not live or any(t.startswith("🧪 真实交易处于 LIVE=pause") for _, t in tg.sent)), 5)
            await asyncio.sleep(0.3)
        t0 = time.monotonic()
        bot.stopping.set()
        assert await asyncio.wait_for(runner, 30) == 0
        assert time.monotonic() - t0 < 10, "a notice waiting for Telegram never holds the shutdown"
        return tg.sent

    logging.disable(logging.WARNING)
    try:
        sent = await scenario(m.Bot, {}, {"SIM_EDGE_CENTS": "12", "SIM_SECONDS": "2"})
        dropped = [t for c, t in sent if "网页控制台保存的设置" in t]
        assert len(dropped) == 1 and "SIM_SECONDS" in dropped[0] and sent[0][0] == 42, sent
        assert await scenario(m.Bot, {}, {"SIM_SECONDS": "2"}, recover=False) == []  # stopped while still down: nothing, no hang
        sent = await scenario(L.LiveBot, {"LIVE": "pause", "SIM": "on", "PREDICT": "on", "PREDICT_WS": "off", "BINANCE_WS": "off",
                                          "HL_WS": "off", "PREDICT_PRIVATE_KEY": "a" * 64, "PREDICT_API_KEY": "k", "PREOPEN_MARKETS": "btc,eht"},
                              {"PREOPEN_15M": "on", "LIVE_MAX_ORDER_USD": "5"}, live=True)
        assert len([t for c, t in sent if t.startswith("🧪 真实交易处于 LIVE=pause")]) == 1, sent
        assert len([t for c, t in sent if "网页控制台保存的设置" in t and "PREOPEN_15M" in t]) == 1, sent
    finally:
        logging.disable(logging.NOTSET)


asyncio.run(startup_notice_lost())

async def why_no_key():
    """XR-WHY-NO-KEY: a real account (a real-trading key, real orders in the records) with WEB_CONTROL_KEY unset. data.json keeps
    a 距吃单 stub on every priced card so that its 为什么挂 / 不挂单 row stays (CTRL-DATAJSON-LEAK: "the keyed way to the same
    facts"), but why.json and journal.json answer 403 "未设置 WEB_CONTROL_KEY" and count no failure (CTRL-NO-CONTROL-KEY): there is
    no key to type. The row asked for one anyway, stored every value typed, read again, got the same 403 and asked again — only
    Cancel ended it; the review page did the same. The 403 now says "nokey" and both pages show the message once, no prompt.
    With a key configured a missing or wrong key still prompts (nokey false)."""
    import json, os, re, shutil, subprocess, tempfile
    token = "t" * 20

    class FM:
        def __init__(self): self.config = None
        def now_ms(self): return NOW

    def bot_of(control_key):
        env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
               "PREDICT_PRIVATE_KEY": "0x" + "a" * 64, **({"WEB_CONTROL_KEY": "control-key-123456"} if control_key else {})}
        return m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(), None)

    async def get(port, name, extra=""):
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(f"GET /p/{token}/{name} HTTP/1.1\r\nHost: x\r\n{extra}\r\n".encode()); await w.drain()
        data = await r.read(); w.close()
        head, _, body = data.partition(b"\r\n\r\n")
        return int(head.split(b" ")[1]), body.decode("utf-8")

    bodies, pages = {}, {}
    for control_key in (False, True):
        web = m.WebServer(bot_of(control_key), 0, token); web.CACHE_SECONDS = {}
        port = await web.start()
        try:
            assert web.account_private() and bool(web.bot.config.web_control_key) == control_key
            for name in ("why.json", "journal.json"):
                for extra in ("", "X-Control-Key: guess-one-guess\r\n"):
                    st, body = await get(port, name, extra)
                    j = json.loads(body)
                    assert st == 403 and j["locked"] is True, (control_key, name, st, body)
                    assert j.get("nokey") is (not control_key), (control_key, name, extra, j)  # only "no key configured" is final
                    if not control_key:
                        assert "未设置 WEB_CONTROL_KEY" in j["message"], j
                        bodies[name] = body
            assert (web.control_failures == {}) is (not control_key), web.control_failures  # no key: nothing counted, as before
            pages = {"odds": web.pages["page"].decode("utf-8"), "journal": web.pages["journal"].decode("utf-8")}
        finally:
            await web.stop()

    odds, journal = pages["odds"], pages["journal"]
    assert re.search(r"j\.nokey[^\n]*whyAsk\(", odds), "the odds page's row checks nokey before prompting"
    assert re.search(r"j\.nokey[^\n]*askKey\(", journal), "the review page checks nokey before prompting"

    # the pages' own code, run against the server's real 403 body with fetch / prompt stubbed (node; skipped without it)
    node = shutil.which("node") or ("/opt/node22/bin/node" if os.path.exists("/opt/node22/bin/node") else "")
    if not node:
        print("why_no_key: node not found, page-code run skipped")
        return
    why_js = odds[odds.index("let whyData="):odds.index("function whyRow(")]
    jr_js = journal[journal.index("function ctlKey()"):journal.index('["csv","scsv","json"]')]
    harness = r"""
const prompts=[],asks=[];let store={};
globalThis.sessionStorage={getItem:k=>store[k]||null,setItem:(k,v)=>{store[k]=v}};
globalThis.prompt=m=>{prompts.push(m);return prompts.length<=3?"guess-"+prompts.length:null};
globalThis.location={pathname:"/p/T/"};
const BODY=JSON.parse(process.env.BODY);
globalThis.fetch=async(u,o)=>{asks.push(u);return {status:403,ok:false,json:async()=>BODY}};
"""
    odds_run = harness + "let last=null;function render(){}\n" + why_js + """
(async()=>{await loadWhy(true);await loadWhy(false);
console.log(JSON.stringify({prompts:prompts.length,fetches:asks.length,err:whyErr}))})();"""
    jr_run = harness + """
let dead=false,okAt=0,allLoaded=false,openId="",data=null,details={},base="/p/T";
function filterKey(){return ""}function loadAna(){}function render(){}function loadDetail(){}function clock(){return ""}
function $(tag,cls,text){return {tag,cls,text}}
let meta=[];globalThis.document={getElementById:()=>({replaceChildren:(...a)=>{meta=a}}),querySelector:()=>null};
""" + jr_js + """
(async()=>{await load();await load();
console.log(JSON.stringify({prompts:prompts.length,fetches:asks.length,meta:meta.map(x=>x.text).join("")}))})();"""
    results = {}
    with tempfile.TemporaryDirectory() as tmp:
        for label, code, body in (("odds", odds_run, bodies["why.json"]), ("journal", jr_run, bodies["journal.json"])):
            path = pathlib.Path(tmp) / f"{label}.js"
            path.write_text(code, encoding="utf-8")
            out = subprocess.run([node, str(path)], capture_output=True, text=True, timeout=60, env={**os.environ, "BODY": body})
            assert out.returncode == 0, (label, out.stderr[-2000:])
            results[label] = json.loads(out.stdout.strip().splitlines()[-1])
    o, jr = results["odds"], results["journal"]
    assert o["prompts"] == 0 and o["fetches"] == 1, o  # one read, no prompt, no 5 s re-polls while the row is open
    assert "未设置 WEB_CONTROL_KEY" in o["err"] and "重新输入口令" not in o["err"], o
    assert jr["prompts"] == 0 and jr["fetches"] == 1 and "未设置 WEB_CONTROL_KEY" in jr["meta"], jr


asyncio.run(why_no_key())


async def latefill_stale_key():
    """XR-LATEFILL-STALE-KEY: live_late_fills takes its (key, record) pairs, then awaits the account's positions (paged) and the
    orders' own records — in the step's sync, after the step has let go of sim_lock. The express lane runs beside it: taking or
    quoting the same slot again, it moves the record there to tid#1 and opens a fresh order at the slot, sent at once. The pass
    then saved the old record back under the key it took — the trim of a taker's fee in shares (about two minutes after nearly
    every taker fill), the adoption of a late fill, the adopted cost's check — over the fresh order's record, in the store and in memory:
    that real order rested or filled with no record (outside LIVE_MAX_OPEN_USD, the share caps and the loss stop), the old record
    was stored twice, and the slot, free again in the bot's eyes, took another order beside the unrecorded one. Each record is now
    saved where it lives (live_save)."""
    WEI = T["WEI"]
    mkt = lambda fair, at: hsi(fair, [("0.55", "300")], [("0.58", "400")], at)
    rows_of = lambda bot, slot: sorted((k.removeprefix("sim:").rsplit("|", 1)[-1], str((v.get("live") or {}).get("order_id")), round(float(v.get("shares") or 0), 6))
                                       for k, v in bot.store.items("sim:") if k.removeprefix("sim:").split("#")[0] == slot)
    up_row = lambda shares, **kw: [{"id": "p1", "market": {"id": 101, "title": "恒生指数"}, "outcome": {"name": "Up", "indexSet": 1, "onChainId": "1011"},
                                    "amount": str(int(shares * WEI)), **kw}]

    async def lane_in_the_read(bot, at):
        """The step at ``at`` (fair 0.50: it decides nothing) whose late-fill pass is held on the positions read while the express
        lane looks at the card (fair 0.70: the side taken or quoted again). Returns the step, still waiting, and the read's go."""
        api = bot.live.api
        came, go, real = asyncio.Event(), asyncio.Event(), api.positions

        async def positions():  # the account's positions read, held on its way: the pass took its records before it
            rows = await real()
            if not came.is_set():
                came.set()
                await go.wait()
            return rows
        api.positions = positions
        stepping = asyncio.ensure_future(step(bot, at, mkt(0.50, at)))
        await asyncio.wait_for(came.wait(), 20)
        bot.market.now, world["markets"] = at + 300, [mkt(0.70, at + 300)]
        await bot.sim_express("HSI")
        return stepping, go

    def every_order_recorded(bot):
        api, trades = bot.live.api, bot.sim_trades()
        booked = {str(v["live"].get("order_id")) for v in trades.values() if isinstance(v.get("live"), dict)}
        resting = {str(v["live"].get("order_id")) for v in trades.values() if isinstance(v.get("live"), dict) and v["live"].get("state") == "open"}
        filled = {i for i, r in api.closed.items() if r["status"] == "FILLED"}
        assert filled <= booked and set(api.open) <= resting, (sorted(filled), sorted(api.open), sorted(booked), sorted(resting))

    # (a) the trim (the default way, taker; the fee taken in shares): a filled taker at 吃, trimmed while the lane takes the side again
    bot = make_bot(SIM_WAYS="taker", SIM_TAKER_MAX_SHARES="300")
    await bot.live_prepare()
    api = bot.live.api
    real_create = api.create_order

    async def fills_at_once(body):
        got = await real_create(body)
        api.fill(got["order_id"], 100, done=True, price=0.58)
        return got
    api.create_order = fills_at_once
    world["markets"] = [mkt(0.70, NOW)]
    await bot.live_prefetch(NOW)
    await step(bot, NOW, mkt(0.70, NOW))
    slot = f"{HSI_SLUG}|up|吃"
    old = bot.sim_trades()[slot]
    assert old["live"]["order_id"] == "101" and old["status"] == "filled" and old["shares"] == 100, (old["live"].get("order_id"), old["status"], old["shares"])
    api.positions_rows = up_row(98.551724)  # 100 bought, the fee taken in shares
    await step(bot, NOW + 70_000, mkt(0.50, NOW + 70_000))  # the shortfall seen once; it must stand a minute later
    stepping, go = await lane_in_the_read(bot, NOW + 135_000)  # ...and this pass trims
    fresh = bot.sim_trades()[slot]
    assert fresh is not old and fresh["live"]["order_id"] == "102" and bot.sim_trades()[f"{slot}#1"] is old, fresh["live"]  # the old record -> 吃#1
    go.set()
    await stepping
    trades = bot.sim_trades()
    assert old["shares"] == 98.551724, old["shares"]  # trimmed as before ...
    assert trades[slot] is fresh and trades[f"{slot}#1"] is old, [k for k, v in trades.items() if v is old]  # ... where it lives
    assert rows_of(bot, slot) == [("吃", "102", 0.0), ("吃#1", "101", 98.551724)], rows_of(bot, slot)  # one row per order (store = memory)
    every_order_recorded(bot)  # the fresh taker's fill (#102) is booked: it was on Predict with no record

    # (b) a late fill read from an ended maker's own record (an operator's cancel leaves it spent at its slot), while the lane quotes
    # the slot again: live_adopted_status
    bot = make_bot(SIM_WAYS="maker")
    api = bot.live.api
    await bot.live_prepare()
    world["markets"] = [mkt(0.70, NOW)]
    await bot.live_prefetch(NOW)
    await step(bot, NOW, mkt(0.70, NOW))
    slot = f"{HSI_SLUG}|up|挂"
    old = bot.sim_trades()[slot]
    oid = old["live"]["order_id"]
    assert old["live"]["state"] == "open" and sorted(api.open) == [oid], (old["live"]["state"], sorted(api.open))
    t1 = NOW + 5_000
    bot.market.now = t1
    await bot.live_cancel_command([oid], t1)
    await bot.live_fills_wait()
    assert bot.sim_trades()[slot] is old and old["live"]["state"] == "done" and old["status"] == "cancelled", (old["live"]["state"], old["status"])
    api.closed[oid]["amountFilled"] = str(30 * WEI)  # matched as the cancel landed: on the order's own record and on the account
    api.positions_rows = up_row(30)
    t2 = t1 + 70_000
    stepping, go = await lane_in_the_read(bot, t2)
    fresh = bot.sim_trades()[slot]
    assert fresh is not old and fresh["live"]["state"] == "open" and bot.sim_trades()[f"{slot}#1"] is old, fresh["live"]  # the spent record -> 挂#1
    go.set()
    await stepping
    await bot.live_fills_wait()
    trades = bot.sim_trades()
    assert old["shares"] == 30, old["shares"]  # the late fill booked on its own order, as before
    assert trades[slot] is fresh and trades[f"{slot}#1"] is old, [k for k, v in trades.items() if v is old]
    assert rows_of(bot, slot) == [("挂", fresh["live"]["order_id"], 0.0), ("挂#1", oid, 30.0)], rows_of(bot, slot)
    every_order_recorded(bot)
    # ...and the next look at the side places nothing beside it: one order resting, the one the records hold
    t3 = t2 + 1_500
    bot.market.now, world["markets"] = t3, [mkt(0.70, t3)]
    await bot.sim_express("HSI")
    assert sorted(api.open) == [fresh["live"]["order_id"]], sorted(api.open)
    every_order_recorded(bot)

    # (c) the adopted cost's check, the pass after a late fill went onto an ended maker by the position's average price (the order's
    # own record never showed it), while the lane quotes the slot again (the filled record -> 挂#1)
    bot = make_bot(SIM_WAYS="maker")
    api = bot.live.api
    await bot.live_prepare()
    world["markets"] = [mkt(0.70, NOW)]
    await bot.live_prefetch(NOW)
    await step(bot, NOW, mkt(0.70, NOW))
    old = bot.sim_trades()[slot]
    oid = old["live"]["order_id"]
    t1 = NOW + 5_000
    bot.market.now = t1
    await bot.live_cancel_command([oid], t1)
    await bot.live_fills_wait()
    assert bot.sim_trades()[slot] is old and old["live"]["state"] == "done" and old["shares"] == 0, (old["live"]["state"], old["shares"])
    api.positions_rows = up_row(30, avgPrice=str(old["price"]))
    t2 = t1 + 70_000
    await step(bot, t2, mkt(0.50, t2))  # adopted by the position's price, no race here
    ad = old["live"].get("adopted") or {}
    assert old["shares"] == 30 and old["status"] == "filled" and str(ad.get("by")).startswith("按 Predict 持仓均价") and not ad.get("guess"), (old["shares"], old["status"], ad)
    assert not old["live"].get("adopted_price_fixed") and bot.sim_trades()[slot] is old
    stepping, go = await lane_in_the_read(bot, t2 + 65_000)  # the next pass checks the adopted cost against the position's average
    fresh = bot.sim_trades()[slot]
    assert fresh is not old and fresh["live"]["state"] == "open" and bot.sim_trades()[f"{slot}#1"] is old, fresh["live"]
    go.set()
    await stepping
    await bot.live_fills_wait()
    trades = bot.sim_trades()
    assert old["live"].get("adopted_price_fixed") is True  # the check ran and saved its mark ...
    assert trades[slot] is fresh and trades[f"{slot}#1"] is old, [k for k, v in trades.items() if v is old]  # ... where the record lives
    assert rows_of(bot, slot) == [("挂", fresh["live"]["order_id"], 0.0), ("挂#1", oid, 30.0)], rows_of(bot, slot)
    assert bot.store.get(f"sim:{slot}#1")["live"].get("adopted_price_fixed") is True  # stored there, not only in memory
    every_order_recorded(bot)


asyncio.run(latefill_stale_key())


async def place_success_after_end():
    """XR-PLACE-SUCCESS-AFTER-END: an order Predict takes at once but answers late — successfully — while a sync runs beside it
    (the express lane sends outside sim_lock): the step's own, /live cancel all's or the emergency stop's sync finds the order by
    its hash and ENDS the record on Predict's word — withdrawn meanwhile and its removal confirmed, a taker filled at once, a
    market whose result came in flight. The late answer changes nothing then. It used to undo the end: back to "cancelling" with
    cancel_sent=0 — a second removal, its "noop" turning cancel_confirmed False and, with a single-order read that no longer knows
    a removed order, the confirmed cancel "unknown" (the side blocked for minutes, a false ❓ alert); a taker's attempt failed
    twice; a record ended by its market's result tracked "open" again; a 📤 notice after the order's end. A record ended WITHOUT
    its order found (an unknown one released before the answer came) is still tracked and reserved by the answer, as before."""
    mkt = lambda fair, at: hsi(fair, [("0.55", "300")], [("0.58", "400")], at)
    removals = lambda fake, oid: sum(1 for c in fake.calls if c == ("remove", [oid]))
    UP = "1011"  # the Up outcome's token: the order held in flight is the Up side's

    async def race(ways, how, read404=False):
        bot = make_bot(SIM_WAYS=ways, SIM_MAKER_EXIT_CENTS="5")
        bot.notes = []
        bot.live_notify = lambda text, key="": bot.notes.append(text)
        world["markets"] = [mkt(0.70, NOW)]
        await bot.live_prepare(); await bot.live_prefetch(NOW)
        fake = bot.live.api
        if read404:  # Predict's single-order read no longer knows a removed order (GET /orders/{hash}: 404)
            known = fake.order

            async def order(oid, hash_=""):
                row = fake.closed.get(str(oid))
                return None if row is not None and row.get("status") == "CANCELLED" else await known(oid, hash_)
            fake.order = order
        real_create, taken, answer, held = fake.create_order, asyncio.Event(), asyncio.Event(), []

        async def slow(body):  # the Up order: taken at once (a taker filled at once when asked), answered late — successfully
            got = await real_create(body)
            if held or body["data"]["order"]["tokenId"] != UP:
                return got  # any other order (the step's Down quote at 0.40): answered at once
            held.append(got)
            if how == "filled":
                fake.fill(got["order_id"], 100, done=True, price=0.58)
            taken.set()
            await answer.wait()
            return got
        fake.create_order = slow
        bot.market.now = NOW
        lane = asyncio.create_task(bot.sim_express("HSI"))  # the express lane's pass: its orders go out outside sim_lock
        await asyncio.wait_for(taken.wait(), 5)
        slot = f"{HSI_SLUG}|up|{'挂' if ways == 'maker' else '吃'}"
        oid, t = held[0]["order_id"], bot.sim_trades()[slot]
        assert t["live"]["state"] == "placing" and not t["live"].get("order_id"), t["live"]
        if how in {"cancel", "stop"}:  # an operator's /live cancel all, or the emergency stop: its own sync finds, removes and ends it
            bot.market.now = NOW + 1_000
            if how == "cancel":
                await bot.live_cancel_command(["all"], NOW + 1_000)
            else:
                assert (await bot.live_control("stop", {}))["ok"]
        else:
            if how == "result":  # the market's result while the request is in flight: cancelling, no withdrawal
                bot.sim_settle(t, 0.0, "本地结果", NOW + 1_000, "本地数据")
                bot.sim_save([(slot, t)])
            # the step beside the lane ("withdrawn": its own decisions withdraw the order, the edge gone at 0.40): its sync finds the
            # order by its hash
            fair = 0.40 if how in {"withdrawn", "result"} else 0.70
            await step(bot, NOW + 2_000, mkt(fair, NOW + 2_000))
        await bot.live_fills_wait()
        ended, seen = dict(t["live"]), len(bot.notes)
        answer.set()
        await lane  # the answer lands; the pass then sends the cancels it finds unsent (sim_express_after: late)
        await bot.live_fills_wait()
        fake.create_order = real_create
        return bot, t, oid, ended, bot.notes[seen:]

    for read404 in (False, True):
        # a maker withdrawn in flight by the step's own decisions: its sync removed it (confirmed) and ended it — the answer adds an
        # event, nothing else: no second removal, no "unknown", no 📤 after the end, the side free
        bot, t, oid, ended, notes = await race("maker", "withdrawn", read404)
        st, fake = t["live"], bot.live.api
        assert ended["state"] == "done" and ended["final"] == "cancelled" and ended["cancel_confirmed"] and ended["order_id"] == oid, ended
        assert st["state"] == "done" and st["final"] == "cancelled" and st["cancel_confirmed"] and removals(fake, oid) == 1, (read404, st["state"], st.get("cancel_confirmed"), fake.calls)
        assert not any("状态未知" in e["what"] for e in st["events"]) and any("下单应答到达" in e["what"] for e in st["events"]), st["events"]
        assert not any(n.startswith(("📤", "❓")) for n in notes), notes
        assert bot.live_room(bot.sim_trades(), mkt(0.70, NOW + 2_500), "up", 0.55, 100.0) == "", "the side is free: its order is gone"
        await step(bot, NOW + 12_000, mkt(0.40, NOW + 12_000)); await bot.live_fills_wait()
        assert st["state"] == "done" and st["cancel_confirmed"] and removals(fake, oid) == 1, (st["state"], fake.calls)
        key = next(k for k, v in bot.sim_trades().items() if v is t)
        assert bot.store.get(f"sim:{key}")["live"]["state"] == "done", bot.store.get(f"sim:{key}")["live"]  # stored as it lives
        # a taker withdrawn in flight by /live cancel all: removed and failed once (one 吃单未成交, one removal), never "unknown"
        bot, t, oid, ended, notes = await race("taker", "cancel", read404)
        st, fake = t["live"], bot.live.api
        assert ended["state"] == "done" and ended["final"] == "failed" and ended["order_id"] == oid, ended
        assert st["state"] == "done" and st["final"] == "failed" and removals(fake, oid) == 1, (read404, st["state"], fake.calls)
        assert sum("吃单未成交" in e["what"] for e in st["events"]) == 1 and not any("状态未知" in e["what"] for e in st["events"]), st["events"]
    # the emergency stop in flight: its "✅ Predict 确认撤掉" stands — no second removal, no 📤 after it, no ❓; resumed, the side is free
    bot, t, oid, ended, notes = await race("maker", "stop", read404=True)
    st, fake = t["live"], bot.live.api
    assert ended["state"] == "done" and ended["cancel_confirmed"], ended
    assert st["state"] == "done" and st["cancel_confirmed"] and removals(fake, oid) == 1, (st["state"], fake.calls)
    assert not any(n.startswith(("📤", "❓")) for n in notes), notes
    await bot.live_control("resume", {})
    assert bot.live_room(bot.sim_trades(), mkt(0.70, NOW + 3_000), "up", 0.55, 100.0) == "", "the side is free once resumed"
    # a market's result in flight: removed and ended by the sync — not tracked as "open" again
    bot, t, oid, ended, notes = await race("maker", "result")
    assert ended["state"] == "done" and t["status"] == "expired" and t["live"]["state"] == "done" and bot.live_reserved(t) == 0, t["live"]
    # a taker filled at once, found by the sync's reconcile: done/filled stays done/filled
    bot, t, oid, ended, notes = await race("taker", "filled")
    assert ended["state"] == "done" and ended["final"] == "filled", ended
    assert t["live"]["state"] == "done" and t["live"]["final"] == "filled" and t["shares"] == 100 and len(t["fills"]) == 1, t["live"]
    # control: found by the sync but not ended (a maker still wanted, "open"): the answer is taken as before
    bot, t, oid, ended, notes = await race("maker", "open")
    assert ended["state"] == "open" and t["live"]["state"] == "open" and t["live"]["order_id"] == oid, t["live"]
    assert any(e["what"].startswith(f"已下单 #{oid}") for e in t["live"]["events"]) and not any(c[0] == "remove" for c in bot.live.api.calls)

    # control: a record ended WITHOUT its order found — "unknown" (the hash read came before Predict had the order), then released
    # by an operator — learns from the answer that Predict has the order: tracked and reserved by the id, as before (never left
    # resting untracked)
    bot = make_bot(SIM_WAYS="maker", SIM_MAKER_EXIT_CENTS="5")
    world["markets"] = [mkt(0.70, NOW)]
    await bot.live_prepare(); await bot.live_prefetch(NOW)
    fake = bot.live.api
    real_create, sent, answer = fake.create_order, asyncio.Event(), asyncio.Event()

    async def unprocessed(body):  # the Up order reaches Predict only as its answer comes back
        if body["data"]["order"]["tokenId"] == UP and not sent.is_set():
            sent.set()
            await answer.wait()
        return await real_create(body)
    fake.create_order = unprocessed
    bot.market.now = NOW
    lane = asyncio.create_task(bot.sim_express("HSI"))
    await asyncio.wait_for(sent.wait(), 5)
    tid = f"{HSI_SLUG}|up|挂"
    t = bot.sim_trades()[tid]
    at = NOW + bot.LIVE_PLACING_GRACE_MS + 5_000
    await step(bot, at, mkt(0.70, at))  # its sync: not on the open list, unknown to the hash read, past the grace
    assert t["live"]["state"] == "unknown" and not t["live"].get("order_id"), t["live"]
    assert bot.live_release_command([tid.lower()], at).startswith("已释放")
    assert t["live"]["state"] == "done" and bot.live_reserved(t) == 0, t["live"]
    answer.set()
    await lane
    oid = t["live"].get("order_id")
    assert t["live"]["state"] == "open" and oid in fake.open and bot.live_reserved(t) > 0, t["live"]


asyncio.run(place_success_after_end())


async def unknown_takes_latefill():
    """XR-UNKNOWN-TAKES-LATEFILL: maker A (55¢) withdrawn by the express lane, Predict confirms the cancel; 30 shares matched as it
    landed, but A's own record keeps showing 0 (it lags). The slot's next maker B's POST never reaches Predict and its answer is
    lost: placing, then unknown after the grace — no order id, its hash unknown to Predict. The account holds 30 at 55¢.
    LIVE-LATEFILL-ACTIVE's hold (live_late_unread) waited on B (状态未知) every minute, so B's release by the positions
    (live_recheck → live_unexplained) booked the 30 onto the order that never existed, at its limit; once A's record showed its 30
    and a further 10 landed there, A was read again and booked them too: 60 recorded, 40 held, for good. An unknown record without
    an id whose hash Predict says it does not know no longer holds the pass: the position's price names A (as in 1.149.0), B ends
    unfilled. Still held: an unknown record with an id (it reached Predict), and one without an id whose hash read has only ever
    failed (XR-409-HASH-ERROR: Predict never answered — it may hold the order and its fill)."""
    WEI = T["WEI"]
    slot = f"{HSI_SLUG}|up|挂"
    held = lambda n: [{"id": "p1", "market": {"id": 101}, "outcome": {"name": "Up", "indexSet": 1, "onChainId": "1011"},
                       "amount": str(int(n * WEI)), "avgPrice": "0.55"}]
    card = lambda fair, at: hsi(fair, [("0.55", "300")], [("0.58", "400")], at)
    on_up = lambda bot: sorted((str(t["live"].get("order_id") or ""), float(t.get("shares") or 0)) for t in bot.sim_trades().values()
                               if isinstance(t.get("live"), dict) and t["side"] == "up" and float(t.get("shares") or 0) > 0)

    # (a) the record that never reached Predict does not take the ended order's late fill
    bot = make_bot(SIM_WAYS="maker", SIM_MAKER_EXIT_CENTS="5")
    await bot.live_prepare()
    fake = bot.live.api
    world["markets"] = [card(0.70, NOW)]
    await bot.live_prefetch(NOW)
    await step(bot, NOW, card(0.70, NOW))
    a = bot.sim_trades()[slot]
    a_oid = a["live"]["order_id"]
    lag = {"on": True}
    real_order, real_orders, real_create = fake.order, fake.orders, fake.create_order

    async def lagging(oid, hash_=""):  # A's own record: 0 filled although 30 matched
        row = await real_order(oid, hash_)
        return {**row, "amountFilled": "0"} if lag["on"] and row is not None and str(row.get("id")) == a_oid else row

    async def hiding(status="OPEN", **params):  # the list read just before the removal shows A with 0 filled too
        return [{**r, "amountFilled": "0"} if str(r.get("id")) == a_oid else r for r in await real_orders(status, **params)]
    lost = []

    async def create(body):  # the next order on 涨 (B) never reaches Predict; its answer is lost (the HTTP layer's timeout)
        if not lost and str(body["data"]["order"]["tokenId"]) == "1011":
            lost.append(body["data"]["order"]["hash"])
            raise m._net_error(TimeoutError("timed out"))
        return await real_create(body)
    fake.order, fake.orders, fake.create_order = lagging, hiding, create
    t1 = NOW + 5_000
    fake.open[a_oid]["amountFilled"] = str(30 * WEI)
    fake.positions_rows = held(30)
    bot.market.now, world["markets"] = t1, [card(0.40, t1)]  # the edge gone: A withdrawn by the express lane, the cancel confirmed
    await bot.sim_express("HSI")
    await bot.live_fills_wait()
    fake.orders = real_orders
    assert a["live"]["state"] == "done" and float(a["shares"]) == 0, (a["live"]["state"], a["shares"])
    t2 = t1 + 2_000
    await step(bot, t2, card(0.70, t2))  # the slot quoted again: B's POST lost
    b = bot.sim_trades()[slot]
    assert b is not a and b["live"]["state"] == "placing" and b["live"].get("hash") and not b["live"].get("order_id"), b["live"]
    for k in range(1, 33):  # eight minutes of steps, A's record still lagging: B goes unknown (no order id), then is released
        at = t2 + k * 15_000
        await step(bot, at, card(0.70, at))
        await bot.live_fills_wait()
    assert any("状态未知" in e["what"] for e in b["live"]["events"]), [e["what"] for e in b["live"]["events"]]
    assert b["live"]["state"] == "done" and float(b["shares"]) == 0, ("the order that never reached Predict took the fill", b["status"], b["shares"],
                                                                      [e["what"] for e in b["live"]["events"]][-2:])
    assert float(a["shares"]) == 30 and (a["live"].get("adopted") or {}).get("by", "").startswith("按 Predict 持仓均价"), (a["shares"], a["live"].get("adopted"))
    assert on_up(bot) == [(a_oid, 30.0)], on_up(bot)
    assert b["live"].get("hash_not_found") and not b["live"].get("order_id"), b["live"]  # "no such order" by its hash: it held nothing
    # later: A's own record shows its 30, and a further 10 land on the outcome unrecorded: the next check reads A again — counted once
    lag["on"] = False
    fake.closed[a_oid]["amountFilled"] = str(30 * WEI)
    fake.positions_rows = held(40)
    t3 = t2 + 33 * 15_000
    for k in range(6):
        at = t3 + k * 15_000
        await step(bot, at, card(0.70, at))
        await bot.live_fills_wait()
    assert sum(sh for _, sh in on_up(bot)) == 40, ("recorded vs held 40", on_up(bot))

    async def requoted():  # maker A (55¢) withdrawn by a rule with nothing filled, the slot quoted again: B
        bot = make_bot(SIM_WAYS="maker")
        await bot.live_prepare()
        world["markets"] = [card(0.70, NOW)]
        await bot.live_prefetch(NOW)
        await step(bot, NOW, card(0.70, NOW))
        a = bot.sim_trades()[slot]
        bot.market.now = NOW + 5_000
        bot.sim_withdraw(a, "test: 买1 moved", NOW + 5_000)
        a["withdrawn"]["rule"] = True
        bot.sim_save([(slot, a)])
        await bot.live_cancel_blind({slot: a}, NOW + 5_000)
        await bot.live_fills_wait()
        assert a["live"]["state"] == "done" and float(a["shares"]) == 0 and a["status"] == "cancelled", (a["live"]["state"], a["shares"])
        return bot, bot.live.api, a

    # (b) an unknown record WITH an order id (it reached Predict) still holds the check (LIVE-LATEFILL-ACTIVE): found again with its
    # 50, the 50 are its own — not booked onto the ended order as well
    bot, fake, a = await requoted()
    await step(bot, NOW + 10_000, card(0.70, NOW + 10_000))
    b = bot.sim_trades()[slot]
    b_oid = b["live"]["order_id"]
    assert b is not a and b_oid and b_oid != a["live"]["order_id"] and b["live"]["state"] == "open", b["live"]
    gone = fake.open.pop(b_oid)
    real_order, real_remove = fake.order, fake.remove_orders

    async def none(oid, hash_=""): return None
    async def noop(ids): return {"removed": [], "noop": list(ids)}
    fake.order, fake.remove_orders = none, noop
    for at in (NOW + 20_000, NOW + 30_000, NOW + 40_000, NOW + 75_000, NOW + 80_000):
        await step(bot, at, card(0.70, at))
    assert b["live"]["state"] == "unknown" and b["live"].get("order_id") == b_oid, b["live"]["state"]
    fake.positions_rows = held(50)
    bot.live_late_checked = -1e9
    await step(bot, NOW + 85_000, card(0.70, NOW + 85_000))
    assert float(a["shares"]) == 0 and not a["live"].get("adopted"), ("held while B (with an id) is unknown", a["shares"], a["live"].get("adopted"))
    gone["amountFilled"] = str(50 * WEI)
    fake.open[b_oid] = gone
    fake.order, fake.remove_orders = real_order, real_remove
    await step(bot, NOW + 90_000, card(0.70, NOW + 90_000))
    assert on_up(bot) == [(b_oid, 50.0)], on_up(bot)

    # (c) an unknown record WITHOUT an id whose hash read has only ever failed (XR-409-HASH-ERROR) still holds it: Predict never
    # answered — it may hold the order and its fill. Here it does: B reached Predict and filled at once (never on the open list), its
    # answer lost, the read by its hash failing for minutes. A, ended at the same price with room, does not take B's 100 by the
    # position's price; once B's read works, the 100 are B's own — counted once
    bot, fake, a = await requoted()
    real_create, real_order, taken = fake.create_order, fake.order, []

    async def taken_lost(body):
        got = await real_create(body)
        if not taken and str(body["data"]["order"]["tokenId"]) == "1011":
            taken.append(body["data"]["order"]["hash"])
            fake.fill(got["order_id"], 100, done=True)
            raise m._net_error(TimeoutError("timed out"))
        return got

    async def failing(oid, hash_=""):
        if taken and hash_ == taken[0]:
            raise m.RemoteError("HTTP 500: internal error")
        return await real_order(oid, hash_)
    fake.create_order, fake.order = taken_lost, failing
    await step(bot, NOW + 10_000, card(0.70, NOW + 10_000))
    b = bot.sim_trades()[slot]
    assert b is not a and b["live"]["state"] == "placing" and b["live"].get("hash") == taken[0] and not b["live"].get("order_id"), b["live"]
    fake.positions_rows = held(100)
    for k in range(1, 25):  # six minutes of steps, the read by B's hash failing: B goes unknown past the grace, no id, never released
        at = NOW + 10_000 + k * 15_000
        await step(bot, at, card(0.70, at))
        await bot.live_fills_wait()
    assert b["live"]["state"] == "unknown" and not b["live"].get("order_id") and not b["live"].get("hash_not_found"), b["live"]
    assert float(a["shares"]) == 0 and not a["live"].get("adopted"), ("held while B's hash read fails", a["shares"], a["live"].get("adopted"))
    fake.order = real_order  # the read by B's hash works again: found, FILLED 100
    at = NOW + 10_000 + 25 * 15_000
    await step(bot, at, card(0.70, at))
    await bot.live_fills_wait()
    b_oid = b["live"].get("order_id")
    assert b["live"]["state"] == "done" and float(b["shares"]) == 100 and b_oid, (b["live"]["state"], b["shares"])
    for k in range(1, 5):
        await step(bot, at + k * 15_000, card(0.70, at + k * 15_000))
        await bot.live_fills_wait()
    assert on_up(bot) == [(b_oid, 100.0)], on_up(bot)


asyncio.run(unknown_takes_latefill())


async def stop_ok_preopen():
    """XR-STOP-OK-PREOPEN: the emergency stop's verdict (Audit LIVE-STOP-OK) reads "没撤成" in the line of its own pre-open
    withdraw_all, but a book of BULK_MIN_IDS ids or fewer — PREOPEN_AHEAD_MINUTES=60 with BTC alone (PREOPEN.md: about four
    windows, eight ids), or the 30 a crashed controller's first try left — took a branch that answered "" whatever its
    removals did: removals failing (HTTP 500, a cooldown on the key) came back ok=True, a green toast, the bids still up; on a
    crashed controller nothing tries again before their signed expiry. Now that branch says how many it could not withdraw
    and why, and the stop is red; a stop whose removals went through, or found the order already gone ("noop"), stays green."""
    import time
    import preopen as P
    import test_preopen as TP

    async def http500(ids):
        raise m.RemoteError("HTTP 500: internal error")

    async def book(windows, alive, ahead="60"):
        rows = [TP.market(at=TP.NOW + k * 15 * 60_000, mid=200 + k) for k in range(1, windows + 1)]
        ctl, api, _ = TP.controller(rows, PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES=ahead, PREOPEN_SHARES="10")
        for r in rows:
            assert await ctl.place_pair(P.candidate(r, TP.NOW, ctl.owner.config, ctl.settings)) == "", r["id"]
        ctl.alive = alive  # False: crashed, still attached — no loop to try again
        return ctl, api

    def up(api):
        return sum(1 for r in api.rows.values() if r["status"] == "OPEN")

    async def stop(ctl):
        """The real stop path: a LiveBot whose model trader has one resting maker order, its cancel working, over the controller."""
        bot = make_bot(SIM_WAYS="maker")
        await bot.live_prepare(); await bot.live_prefetch(NOW)
        await step(bot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
        bot.preopen = ctl
        out = await bot.live_control("stop", {})
        await asyncio.gather(*list(ctl.settling.values()), return_exceptions=True)  # the reconcile tasks the withdrawal launched
        ctl.ledger.close()
        assert bot.live_paused() == "紧急停止" and out["message"].split("\n")[1].startswith("✅ 策略挂单 1 笔：Predict 确认撤掉 1 笔"), out
        return out

    # (a) a small book (8 ids) whose removals all fail, on a running and on a crashed controller: red, and the line says so
    for alive in (True, False):
        ctl, api = await book(4, alive)
        assert sum(len(P.removable_ids(x)) for x in ctl.ledger.rows.values()) == 8 <= P.BULK_MIN_IDS
        api.remove_orders = http500
        out = await stop(ctl)
        assert not out["ok"] and "还有 8 张没撤成（HTTP 500: internal error" in out["message"] and up(api) == 8, (alive, out)
        assert ("预挂模式已停止：还有 8 张没撤成" in out["message"]) == (not alive), (alive, out)
    # ...a cooldown on the key (another client's 429): nothing sent at all — not withdrawn either
    ctl, api = await book(4, True)
    real = api.remove_orders

    async def cooling(ids):  # (as the real client: nothing goes out during the cooldown)
        if api.blocked_until > time.monotonic():
            raise m.RemoteError("HTTP 429: too many requests", 60)
        return await real(ids)
    api.remove_orders, api.blocked_until = cooling, time.monotonic() + 60
    out = await stop(ctl)
    assert not out["ok"] and "还有 8 张没撤成（Predict 接口限流冷却中" in out["message"] and up(api) == 8, out
    # (b) a crashed controller's book of 50 ids: Predict takes the first batch of 20, then fails — the cancel command's own
    # withdraw_all leaves 30, and the stop's second one (30 <= BULK_MIN_IDS, the small branch) must say so as well
    ctl, api = await book(25, False, ahead="1440")
    real, budget = api.remove_orders, [20]

    async def flaky(ids):
        if budget[0] < len(ids):
            raise m.RemoteError("HTTP 500: internal error")
        budget[0] -= len(ids)
        return await real(ids)
    api.remove_orders = flaky
    out = await stop(ctl)
    assert not out["ok"] and up(api) == 30 and "预挂模式已停止：还有 30 张没撤成（HTTP 500: internal error" in out["message"], out
    # (c) still green: removals that work (running and crashed), nothing to withdraw — and an order Predict answers "noop" for
    # (gone already): not a miss, though the ledger says "open" until the reconcile reads it (a recount would have said 1 left)
    for alive in (True, False):
        ctl, api = await book(4, alive)
        out = await stop(ctl)
        assert out["ok"] and "没撤成" not in out["message"] and up(api) == 0, (alive, out)
    ctl, api = await book(0, True)
    out = await stop(ctl)
    assert out["ok"] and "没撤成" not in out["message"], out
    ctl, api = await book(4, False)
    gone = next(iter(api.rows.values()))
    gone["status"] = "CANCELLED"
    api.stuck.add(gone["id"])  # answered "noop", its record left as it is
    out = await stop(ctl)
    assert out["ok"] and "没撤成" not in out["message"] and up(api) == 0, out


asyncio.run(stop_ok_preopen())


# --- XR-DISPUTE-HOLD-FREEZE: a flip / market-cap level's dispute hold and the orders that exist when it begins -------------------
# MOD-BOOK-DISPUTES judges "the book does not agree with our data" on the best Yes bid alone (was max(bid, ask)): a thin book bid
# 60¢ / offered 95¢ now holds a market our data has settled Yes ("数据显示已触及 / 已反超，但盘口仍低于 90¢") where 1.149.0 took the
# data's word. A held card's resting order stands unjudged (sim_requote_reason) — the pattern SIM-TOUCH-DECIDED-HOLD's fix-up
# removed for a first-touch market's book hold alone (mk.decided). On a market-cap level sim_result waits for Predict while the book
# disputes, so a real No bid (worth 0 by our data, at most 10¢ under such a book) stayed on Predict for the half hour (for good with
# SIM_NO_ODDS_WITHDRAW_MINUTES=0) where 1.149.0's exit line cancelled it at the next look; and on both kinds a decision an API
# cooldown had held back went out under the hold when the cooldown ended (a flip's before the step's next look settled it), where
# 1.149.0's send-time re-check voided the No bid. No market-cap ladder is listed now (CAP_MARKETS): one is registered by hand, as
# tests/test_cap.py does; the flip is this section's own (FLIP_MARKETS' line goes with its market).
import datetime as dt  # noqa: E402
import itertools  # noqa: E402
from decimal import Decimal as D  # noqa: E402

DH_NOW = int(dt.datetime(2026, 11, 10, 12, 0, tzinfo=m.BEIJING).timestamp() * 1000)
DH_ENV = {"TELEGRAM_BOT_TOKEN": "1:x", "SIM_WAYS": "maker", "SIM_MARKETS": "all", "SIM_MAKER_POINTS": "off",
          "SIM_MAKER_FLOOR_SHARES": "0", "SIM_MAKER_DEEP_CENTS": "0"}  # the defaults otherwise (exit line 5¢, the half hour)
DH_HOLD = {"ladder": "数据显示已触及，但盘口仍低于 90¢", "flip": "数据显示已反超，但盘口仍低于 90¢；以 Hyperliquid 1 分钟 K 为准，请核实"}
DH_GONE = "净优势降到 -5.0¢，低于撤单线 5.0¢"


async def dispute_bot(kind: str, live_on: bool = False):
    """A paper Bot, or the real trader over the stand-ins (LIVE=on with the SDK test suite's key, never a real one), with one
    market of ``kind`` on Predict's market 401 (Yes / No): a market-cap level ($200M, "ladder") or a flip. (the bot, its spec,
    look(at, settled, bid="0.60", fair=0.70): one look's data — our data's verdict, Yes reached or not, and the Yes book bid
    ``bid``, offered at 95¢; until our data settles it the model gives Yes 70¢, a market-cap level ``fair``)."""
    env = {**DH_ENV, **({"LIVE": "on", "PREDICT_PRIVATE_KEY": T["KEY"], "PREDICT_API_KEY": "k"} if live_on else {})}
    bot = (L.LiveBot if live_on else m.Bot)(m.Config.from_env(env), m.Store(":memory:"), T["FM"](DH_NOW), None)
    if live_on:
        bot.live.api, bot.live.chain = T["FakeApi"](), T["FakeChain"]()
        bot.live.api.markets["401"] = T["market_json"]("401", names=("Yes", "No"))
        await bot.live_prepare()
        assert bot.live.ready, bot.live.ready_error
        await bot.live.market("401")
    start, end = DH_NOW - 9 * m.DAY_MS, DH_NOW + 20 * m.DAY_MS
    book = lambda key, slug, at, bid: m.PredictBook(key, slug, "401", "t", ((D(bid), D("300")),), ((D("0.95"), D("300")),), at, 200)
    if kind == "ladder":
        spec = m.CapSpec("XRCAP", "what-marketcap-will-xrcap-hit", "$XRCAP 市值", "0x" + "cd" * 20, start, end, (D("200000000"),))
        cap, seen = m.CapMarket(bot.store, spec), {"high": D("150000000"), "fair": 0.70}
        cap.probability = lambda target, now_ms: 1.0 if seen["high"] >= target else seen["fair"]
        cap.window_high = lambda: (seen["high"], DH_NOW)
        cap.coverage = lambda: ""
        cap.model_swing = lambda target, now_ms, fair: 0.0
        bot.caps = {spec.key: cap}
        bot.predict.market_meta["401"] = ({"outcomes": ["Yes", "No"], "status": "REGISTERED"}, 0)

        def look(at, settled, bid="0.60", fair=0.70):
            seen.update(high=D("210000000") if settled else D("150000000"), fair=fair)
            bot.predict.ladders[spec.key] = [m.LadderRow(D("200000000"), "401", "$200M", book(spec.key, spec.slug, at, bid))]
            bot.market.now = at
    else:
        spec = m.FlipSpec("XR-FLIP", "will-xr-flip-test", "XR 反超测试", "HYPE", "SOL", start, end)
        fm = m.FlipMarket(bot.store, spec)
        fm.odds = lambda now_ms: 1.0 if fm.history.get("kind") == "flip" else 0.70
        fm.model_swing = lambda now_ms: 0.0
        bot.flips = {spec.key: fm}
        bot.predict.info[spec.slug] = {"outcomes": ["Yes", "No"]}

        def look(at, settled, bid="0.60", fair=0.70):
            fm.sigma_ms = at  # σ current and the path read to the minute before: the market's own reasons hold nothing
            bot.store.put(f"flip:{spec.slug}", {"kind": "flip", "time": at - 60_000, "a": 181.0, "b": 180.0, "start": start, "top": 1.005,
                                                "top_at": at - 60_000} if settled else
                          {"kind": "clear", "through": at - 60_000, "start": start, "top": 0.99, "top_at": at - 3_600_000})
            bot.predict.books[spec.key] = book(spec.key, spec.slug, at, bid)
            bot.market.now = at
    return bot, spec, look


async def dispute_step(bot, at: int, key: str = "", lane: bool = False) -> None:
    """One look of the trader at ``at``: the step's, or the express lane's on ``key``."""
    if lane:
        await bot.sim_express(key)
    else:
        bot.sim_ran = -1e9
        await bot.sim_step(at)
    if isinstance(bot, L.LiveBot):
        await bot.live_fills_wait()


async def dispute_hold_resting():
    """A market-cap level: a No bid rests at 5¢ (the model gives No 30¢) beside a Yes bid at 60¢; then our data sees the level
    reached while the book still bids Yes at 60¢. The No bid — worth 0 by our data — is withdrawn at the exit line at that very
    look, by the step or by the express lane, on paper and for real (its cancel sent to Predict); the Yes bid, worth more by our
    data, stands as judged; nothing is placed while the card holds. Before: both stood unjudged, the real No bid open on Predict
    for the half hour (for good with SIM_NO_ODDS_WITHDRAW_MINUTES=0)."""
    for live_on, lane in itertools.product((False, True), (False, True)):
        bot, spec, look = await dispute_bot("ladder", live_on)
        market, api = f"{spec.slug}#401", bot.live.api if live_on else None
        look(DH_NOW, False)
        await dispute_step(bot, DH_NOW)
        trades = bot.sim_trades()
        no, yes = trades[f"{market}|down|挂"], trades[f"{market}|up|挂"]
        assert (no["status"], no["price"], yes["status"], yes["price"]) == ("resting", 0.05, "resting", 0.6), (no, yes)
        if live_on:
            ids = {side: t["live"].get("order_id") for side, t in (("no", no), ("yes", yes))}
            assert sorted(api.open) == sorted(ids.values()) and [c[0] for c in api.calls] == ["create", "create"], (ids, api.calls)
        at = DH_NOW + 5_000
        look(at, True)
        (mk,) = [x for x in bot.sim_markets(at) if x.key == spec.key]
        assert (mk.fair_up, mk.hold) == (1.0, DH_HOLD["ladder"]), (mk.fair_up, mk.hold)
        assert bot.sim_maker_gate(mk, at).startswith("卡片暂不建议") and bot.sim_result(no, at) is None  # held; Predict settles it
        await dispute_step(bot, at, spec.key, lane)
        trades = bot.sim_trades()
        states = {tid.partition("|")[2]: (t["status"], (t.get("withdrawn") or {}).get("why", "")) for tid, t in trades.items()}
        assert states == {"down|挂#1": ("cancelled", DH_GONE), "up|挂": ("resting", "")}, (live_on, lane, states)  # before: both resting
        if live_on:
            assert api.calls[2:] == [("remove", [ids["no"]])] and sorted(api.open) == [ids["yes"]], (lane, api.calls[2:], api.open)
        for later in (6_000, 10_000):  # nothing takes its place while the card holds
            look(DH_NOW + later, True)
            await dispute_step(bot, DH_NOW + later, spec.key, lane)
        assert {tid.partition("|")[2] for tid in bot.sim_trades()} == {"down|挂#1", "up|挂"}, (live_on, lane, list(bot.sim_trades()))
        if live_on:
            assert [c[0] for c in api.calls] == ["create", "create", "remove"], (lane, api.calls)


async def dispute_hold_unsent():
    """A resting-order decision made on the open market and held back by an API cooldown (a 429 answered to another request between
    the decision and its send); four seconds on, our data has settled the market Yes while the book still bids it at 60¢, and the
    cooldown is over. What is pending goes out after any express look that acts, on whichever market (live_place_pending): neither
    order is sent now — not the No bid (worth 0 by our data; before: sent, and on a market-cap level left there for the half hour)
    nor the Yes bid (no hold lets a new order out). With our data as it was, both go out."""
    for kind, settled in itertools.product(("ladder", "flip"), (True, False)):
        bot, spec, look = await dispute_bot(kind, True)
        api, send = bot.live.api, bot.sim_after_decide

        async def cooled(now_ms, api=api, send=send):
            api.blocked_until = m.time.monotonic() + 3600
            await send(now_ms)
        bot.sim_after_decide = cooled
        look(DH_NOW, False)
        await dispute_step(bot, DH_NOW)
        bot.sim_after_decide = send
        waiting = {t["label"]: (t["live"]["state"], t["price"]) for t in bot.sim_trades().values()}
        assert waiting == {"挂No": ("pending", 0.05), "挂Yes": ("pending", 0.6)} and api.calls == [], (kind, waiting, api.calls)
        api.blocked_until = 0.0
        at = DH_NOW + 4_000
        look(at, settled)
        (mk,) = [x for x in bot.sim_markets(at) if x.key == spec.key]
        assert mk.hold == (DH_HOLD[kind] if settled else ""), (kind, settled, mk.hold)
        await bot.live_place_pending(at)
        sent = [c for c in api.calls if c[0] == "create"]
        if not settled:
            assert len(sent) == 2 and len(api.open) == 2, (kind, api.calls)
            continue
        assert sent == [], (kind, [c[1]["data"]["order"]["tokenId"] for c in sent])  # before: both sent, the No bid at 5¢ first
        notes = {t["label"]: (t["status"], t["live"]["state"], t.get("note") or (t.get("withdrawn") or {}).get("why", ""))
                 for t in bot.sim_trades().values()}
        assert all(status == "cancelled" and state == "done" and f"卡片暂不建议（{DH_HOLD[kind]}）" in note and note.endswith("4 秒前的挂单决定作废")
                   for status, state, note in notes.values()) and len(notes) == 2, (kind, notes)
        look(at + 1_000, True)
        await dispute_step(bot, at + 1_000)  # the step that follows: nothing new under the hold
        assert not [c for c in api.calls if c[0] == "create"] and not api.open, (kind, api.calls)


async def dispute_hold_scope():
    """Only the dispute is told apart (SimMarket.decided "up": our data settled Yes, the book bids under 90¢). The market's own
    holds leave it empty, and an order resting under them stands as on every other held card, to the half hour (a market-cap
    level whose σ is the prior, our data short of the level: the No bid's edge gone); a book bidding 90¢ or more agrees with our
    data — no hold, and the market is settled on our data (sim_result) as before; the card's hold is the trader's, its words
    unchanged (flip_payload)."""
    bot, spec, look = await dispute_bot("ladder")
    cap, at = bot.caps[spec.key], DH_NOW + 5_000
    look(DH_NOW, False)
    await dispute_step(bot, DH_NOW)
    no = bot.sim_trades()[f"{spec.slug}#401|down|挂"]
    assert no["status"] == "resting" and no["price"] == 0.05, no
    for settled, bid, sigma_kind, fair, want in ((True, "0.60", "", 0.70, (DH_HOLD["ladder"], "up")), (True, "0.92", "", 0.70, ("", "")),
                                                 (False, "0.60", "prior", 0.97, ("σ 是先验值", ""))):  # last: the No side worth 3¢
        cap.sigma_kind = sigma_kind
        look(at, settled, bid, fair)
        (mk,) = [x for x in bot.sim_markets(at) if x.key == spec.key]
        assert (mk.hold, mk.decided) == want, (settled, bid, sigma_kind, mk.hold, mk.decided)
    assert bot.sim_requote_reason(no, mk, at) == ""  # under its 5¢ bid, and it stands under σ's hold, as before
    cap.sigma_kind = ""
    look(at, True, "0.92")
    assert bot.sim_result(no, at)[0] == 1.0  # the book agrees: settled on our data, as before
    bot, spec, look = await dispute_bot("flip")
    fm = bot.flips[spec.key]
    for settled, bid, sigma_age, want in ((True, "0.60", 0, (DH_HOLD["flip"], "up")), (True, "0.92", 0, ("", "")),
                                          (False, "0.60", 25 * 3_600_000, ("波动率超过一天未更新；暂不给建议", ""))):
        look(DH_NOW, settled, bid)
        fm.sigma_ms = DH_NOW - sigma_age
        (mk,) = [x for x in bot.sim_markets(DH_NOW) if x.key == spec.key]
        assert (mk.hold, mk.decided) == want and bot.flip_payload(fm, DH_NOW)["flip"]["hold"] == mk.hold, (settled, bid, mk.hold, mk.decided)


for dispute_case in (dispute_hold_resting, dispute_hold_unsent, dispute_hold_scope):
    asyncio.run(dispute_case())


# --- XR-STALE-HOLD-FREEZE: the two 1.150.0 holds for a price too old to trade on, and the order resting when they begin ------------
# FEED-HSI-FUTURES-STALE holds the HSI after-hours card's suggestions while the futures quote in use has not changed for three
# minutes in a session (futures_hold); MOD-NVDA-OPEN-GAP holds NVDA first-touch's while its price in the session is over five
# minutes old by its own time, or from before the open (StockTouchMarket.advice_problem). A held card's resting order stands
# unjudged (sim_requote_reason): its 买1 not followed, the valid 买1 gone not seen. So the bid resting when the price stalled stayed
# at its price from before the stall while the book, which had seen the move, came down to it — and was filled there, for real with
# LIVE on — until the odds paused and SIM_NO_ODDS_WITHDRAW_MINUTES after that; 1.149.0 had no hold in that state and followed the
# book down. The up/down card's own stale-price hold (MOD-WINDOW-STALE-PRICE) withdraws the order instead (SimMarket.price_problem);
# these two now do too. Real Bot / LiveBot over the stand-ins, the real feed and card code (the etnet page parsed and dated; the
# NVDA card's fields as Yahoo's reads leave them), the trader's step.
def sh_at(hour: int, minute: int, second: int = 0) -> int:  # Wednesday 2026-09-23, Beijing time: the night session 17:15–03:00
    return int(dt.datetime(2026, 9, 23, hour, minute, second, tzinfo=m.BEIJING).timestamp() * 1000)


SH_ENV = {"TELEGRAM_BOT_TOKEN": "1:x", "SIM_WAYS": "maker", "SIM_MAKER_AFTER_HOURS_CENTS": "15", "SIM_MAKER_AFTER_HOURS_MARKETS": "HSI",
          "SIM_MAKER_MIN_BID": "50", "SIM_MAKER_POINTS": "off"}  # resting orders after hours on HSI (the 均衡 preset's mode), no points gate
SH_STUCK = "恒指期货报价停在 09-23 21:00，已超 3 分钟未更新"
SH_UNMOVED = "恒指期货 etnet 页面启动后尚未见变动，报价时间无法确认"
SH_QUIET = (((D("0.58"), D("300")),), ((D("0.70"), D("300")),))  # others bid 58¢ × 300; "涨" worth ~78¢ on the card: 20¢ over
SH_PULLED = (((D("0.58"), D("20")), (D("0.50"), D("300"))), ((D("0.70"), D("300")),))  # the book saw the futures fall: 58¢ pulled
SH_SWEPT = (((D("0.50"), D("300")),), ((D("0.58"), D("500")),))  # ...and a seller offers 58¢: a bid still there is filled
SH_STRIKE = "24595.09"  # Predict's own target price for the market (its startPrice), a little off the local dated close 24,595.00
sh_feed = {"last": 24840}


def sh_page() -> bytes:
    """etnet in the night session: the running 夜市 block carries no time (dated by when its values were first seen)."""
    last = sh_feed["last"]
    return (f"""<div><h3>恒生指數期貨(10/2026)</h3><span>日市</span><div>&#9650;24,610 +90 (+0.37%) 高水15</div>
<div>最高: 24,650 最低: 24,500 前收市: 24,520 開市: 24,530</div></div>
<div><h3>恒生指數期貨(10/2026)</h3><span>夜市</span><div>&#9650;{last:,} +{last - 24610} (+0.96%) 高水{last - 24595}</div>
<div>最高: 24,900 最低: 24,600 前收市: 24,610 開市: 24,615</div></div>
<div>恒生指數現貨 &#9650;24,595.00 +84.91 (+0.35%) 前收市: 24,510.09 開市: 24,520</div>""").encode()


async def sh_get(url, timeout=15, headers=None):
    if "etnet" in url:
        return sh_page()
    raise m.RemoteError("HTTP 502")  # Eastmoney and the Sina CFD down: etnet alone prices the card


async def sh_bot(live_on: bool = False, store=None):
    """A paper Bot (or the real trader over the stand-ins: LIVE=on with the SDK test suite's key, never a real one) as it starts
    that evening — the HSI closed at 24,595, the futures printed 24,600 at 16:10 (the exact anchor, recorded that afternoon) —
    on ``store`` (another process's: a restart) or a new one; Predict's market 902 is the next session's HSI up/down."""
    if store is None:
        store = m.Store(":memory:")
        close = sh_at(16, 10)
        store.put("anchor:HSI", [close, "24600", "16:10 现货收市时", "10/2026", close + 3_000, close + 3_000])
    env = {**SH_ENV, **({"LIVE": "on", "PREDICT_PRIVATE_KEY": T["KEY"], "PREDICT_API_KEY": "k"} if live_on else {})}
    bot = (L.LiveBot if live_on else m.Bot)(m.Config.from_env(env), store, T["FM"](sh_at(20, 59)), None)
    bot.hsi_daily.daily = {dt.date(2026, 9, 23): D("24595.00")}

    async def details(mid):
        return {"outcomes": ["Up", "Down"], "status": "REGISTERED", "resolved": None}
    bot.predict.market_details = details
    if live_on:
        bot.live.api, bot.live.chain = T["FakeApi"](), T["FakeChain"]()
        bot.live.api.markets["902"] = T["market_json"]("902")
        await bot.live_prepare()
        assert bot.live.ready, bot.live.ready_error
        await bot.live.market("902")
    return bot


async def sh_look(bot, now: int, book: tuple, step: bool = True, strike: str | None = None):
    """The feed read at ``now`` (the etnet page as it stands), Predict's book for the card's market — with ``strike``, Predict's own
    target price for it too (settle_on_strike then measures the card against that, the odds rebuilt) —, the trader's step
    (``step``): (the market as the trader sees it, the records — label, status, price, filled, why withdrawn)."""
    bot.market.now = now
    await bot.hsi.refresh(now, force=True)
    odds = bot.hsi_odds(now)
    assert isinstance(odds, m.CloseOdds), odds
    slug = m.predict_slug(bot.config.predict_slugs["HSI"], odds.target)
    if strike is not None:
        bot.predict.strikes[slug] = (D(strike), m.time.monotonic())
    bot.predict.books["HSI"] = m.PredictBook("HSI", slug, "902", "t", *book, now)
    (mk,) = [x for x in bot.sim_markets(now) if x.key == "HSI"]
    assert mk.settle["line"] == float(strike or odds.ref), (mk.settle, strike)  # the line the trader sees: Predict's figure, given one
    if step:
        bot.sim_ran = -1e9
        await bot.sim_step(now)
        if isinstance(bot, L.LiveBot):
            await bot.live_fills_wait()
    return mk, sorted((t["label"], t["status"], round(t["price"], 2), t["shares"], (t.get("withdrawn") or {}).get("why", ""))
                      for t in bot.sim_trades().values())


async def stale_hold_hsi(live_on: bool, strike: str | None = None):
    """The page moves until 21:00, then stands still. Three minutes on (futures_hold's INDEX_STALE_MS) the card holds — and the
    跟买1 resting at 58¢ is withdrawn at that look, its cancel sent to Predict. The book that saw the futures fall pulls its 58¢
    bids and a seller comes at 58¢: nothing of ours is there, nothing is placed while the card holds. The page moves: quoted afresh
    at the valid 买1 (50¢). (Before: the bid stood unjudged at 58¢ and was filled, 100 @ 58¢, the real order open on Predict.)
    ``strike``: Predict's own target price for the market known and a little off the local close — the card measured against it
    (settle_on_strike, at every look), all the same. (Before the review's follow-up: the odds rebuilt there kept the hold and lost
    the stale price — the bid frozen at 58¢ and filled, the real order left open on Predict, as with no fix.)"""
    sh_feed["last"] = 24840
    bot = await sh_bot(live_on)
    api = bot.live.api if live_on else None
    await sh_look(bot, sh_at(20, 59, 56), SH_QUIET, strike=strike)
    sh_feed["last"] = 24846  # the page moves (a live one) and stands still from 21:00
    mk, recs = await sh_look(bot, sh_at(21, 0), SH_QUIET, strike=strike)
    assert not mk.hold and not mk.price_problem and recs == [("挂涨", "resting", 0.58, 0.0, "")], (mk.hold, recs)
    if live_on:
        assert [c[0] for c in api.calls] == ["create"] and len(api.open) == 1, api.calls
        oid = next(iter(api.open))
    mk, recs = await sh_look(bot, sh_at(21, 3), SH_QUIET, strike=strike)  # three minutes: a short gap, nothing held, the order rests
    assert not mk.hold and not mk.price_problem and recs == [("挂涨", "resting", 0.58, 0.0, "")], (mk.hold, recs)
    mk, recs = await sh_look(bot, sh_at(21, 3, 1), SH_QUIET, strike=strike)  # past them: the card holds — and the order goes
    assert mk.hold == mk.price_problem == SH_STUCK and 0.77 < mk.fair_up < 0.79, (mk.hold, mk.price_problem, mk.fair_up)
    assert recs == [("挂涨", "cancelled", 0.58, 0.0, f"{SH_STUCK}，撤单")], recs  # before: ("挂涨", "resting", 0.58, 0.0, "")
    if live_on:
        assert api.calls[1:] == [("remove", [oid])] and not api.open, api.calls
    for at, book in ((sh_at(21, 4), SH_PULLED), (sh_at(21, 5), SH_SWEPT)):  # before: filled at 21:05, 100 @ 58¢
        mk, recs = await sh_look(bot, at, book, strike=strike)
        assert mk.hold == SH_STUCK and recs == [("挂涨", "cancelled", 0.58, 0.0, f"{SH_STUCK}，撤单")], (at, mk.hold, recs)
    assert all(t["shares"] == 0 for t in bot.sim_trades().values())
    if live_on:
        assert [c[0] for c in api.calls] == ["create", "remove"] and not api.open, api.calls
    sh_feed["last"] = 24850  # the page moves again: priced, and quoted afresh where the book now is
    mk, recs = await sh_look(bot, sh_at(21, 6), SH_SWEPT, strike=strike)
    assert not mk.hold and not mk.price_problem and ("挂涨", "resting", 0.5, 0.0, "") in recs, (mk.hold, recs)
    if live_on:
        assert [c[0] for c in api.calls] == ["create", "remove", "create"] and len(api.open) == 1, api.calls


async def stale_hold_hsi_restart(moves: bool):
    """A restart in the night session with an order resting on Predict's book. The first read of the page shows nothing of its
    age (futures_hold: no suggestion until it moves) — that is no reason to withdraw: a live page moves within seconds, and every
    order resting at a restart would be cancelled and placed again (``moves``: it moves; the order stays as it was, never
    withdrawn). A page that does not move is stale three minutes after that read, and the order goes then."""
    sh_feed["last"] = 24840
    first = await sh_bot()
    await sh_look(first, sh_at(20, 59, 56), SH_QUIET)
    sh_feed["last"] = 24846
    _, recs = await sh_look(first, sh_at(21, 0), SH_QUIET)
    assert recs == [("挂涨", "resting", 0.58, 0.0, "")], recs
    bot = await sh_bot(store=first.store)  # the restart: the records and the anchor kept, the feed's memory of the page not
    mk, recs = await sh_look(bot, sh_at(21, 0, 30), SH_QUIET)
    assert mk.hold == SH_UNMOVED and mk.price_problem == "" and recs == [("挂涨", "resting", 0.58, 0.0, "")], (mk.hold, mk.price_problem, recs)
    if moves:
        sh_feed["last"] = 24848
        mk, recs = await sh_look(bot, sh_at(21, 0, 34), SH_QUIET)
        assert not mk.hold and not mk.price_problem and recs == [("挂涨", "resting", 0.58, 0.0, "")], (mk.hold, recs)
        assert list(bot.sim_trades()) == list(first.sim_trades()), list(bot.sim_trades())  # the same record, never withdrawn
        return
    mk, recs = await sh_look(bot, sh_at(21, 3, 30), SH_QUIET)  # three minutes after the first read: still a short gap
    assert mk.hold == SH_UNMOVED and mk.price_problem == "" and recs == [("挂涨", "resting", 0.58, 0.0, "")], (mk.price_problem, recs)
    mk, recs = await sh_look(bot, sh_at(21, 3, 31), SH_QUIET)  # past them: stale like any page (dated by that read: 21:00:30)
    assert mk.hold == SH_UNMOVED and mk.price_problem == SH_STUCK, (mk.hold, mk.price_problem)
    assert recs == [("挂涨", "cancelled", 0.58, 0.0, f"{SH_STUCK}，撤单")], recs


async def stale_hold_hsi_unsent(strike: str | None = None):
    """A resting-order decision made at 21:02:30 on a page last seen to move at 21:00, held back by an API cooldown; the cooldown
    over at 21:03:05, the page still as it was: the send-time re-check (live_taker_recheck → sim_requote_reason) voids it — no
    new order under the hold, as none is decided there (sim_maker_gate). (Before: sent into the held card, and left there.)
    ``strike``: as in stale_hold_hsi (before the review's follow-up: sent, as with no fix)."""
    sh_feed["last"] = 24840
    bot = await sh_bot(True)
    api, send = bot.live.api, bot.sim_after_decide
    await sh_look(bot, sh_at(20, 59, 56), SH_QUIET, step=False, strike=strike)
    sh_feed["last"] = 24846
    await sh_look(bot, sh_at(21, 0), SH_QUIET, step=False, strike=strike)

    async def cooled(now_ms, api=api, send=send):
        api.blocked_until = m.time.monotonic() + 3600
        await send(now_ms)
    bot.sim_after_decide = cooled
    mk, _ = await sh_look(bot, sh_at(21, 2, 30), SH_QUIET, strike=strike)
    bot.sim_after_decide = send
    waiting = {t["label"]: (t["live"]["state"], t["price"]) for t in bot.sim_trades().values()}
    assert not mk.hold and waiting == {"挂涨": ("pending", 0.58)} and api.calls == [], (mk.hold, waiting, api.calls)
    api.blocked_until = 0.0
    mk, _ = await sh_look(bot, sh_at(21, 3, 5), SH_QUIET, step=False, strike=strike)
    assert mk.hold == mk.price_problem == SH_STUCK, (mk.hold, mk.price_problem)
    await bot.live_place_pending(sh_at(21, 3, 5))
    assert not [c for c in api.calls if c[0] == "create"] and not api.open, api.calls  # before: sent, open on Predict at 58¢
    (t,) = bot.sim_trades().values()
    note = t.get("note") or (t.get("withdrawn") or {}).get("why", "")
    assert (t["status"], t["live"]["state"]) == ("cancelled", "done") and f"{SH_STUCK}，撤单" in note and note.endswith("35 秒前的挂单决定作废"), (
        t["status"], t["live"]["state"], note)


async def stale_hold_hsi_scope():
    """futures_stale is futures_hold's second rule, word for word, and only that: an untimed page counted from its first sighting,
    a timed source by its own time, nothing while the market is shut. The approximate anchor's hold alone withdraws nothing."""
    bot = await sh_bot()
    hsi_feed = bot.hsi
    timed = m.FuturesQuote("恒指主连", D("24846"), D("24610"), None, None, None, sh_at(21, 0), "东方财富", fetched_ms=sh_at(21, 3, 1))
    assert hsi_feed.futures_stale(timed, sh_at(21, 3)) == "" and hsi_feed.futures_stale(timed, sh_at(21, 3, 1)) == SH_STUCK
    assert hsi_feed.futures_hold(timed, sh_at(21, 3, 1)) == SH_STUCK
    final = m.dataclasses.replace(timed, quoted_ms=sh_at(16, 29))  # 16:50: between the day and the night session
    assert hsi_feed.futures_stale(final, sh_at(16, 50)) == hsi_feed.futures_hold(final, sh_at(16, 50)) == ""
    assert m.CloseOdds.__dataclass_fields__["stale"].default == ""  # every other card's odds: no stale price to withdraw for
    sh_feed["last"] = 24840
    await sh_look(bot, sh_at(20, 59, 56), SH_QUIET, step=False)
    sh_feed["last"] = 24846
    await sh_look(bot, sh_at(21, 0), SH_QUIET, step=False)  # the page moves at 21:00, then stands still
    for at, want in ((sh_at(21, 0), ("", "")), (sh_at(21, 3), ("", "")), (sh_at(21, 3, 1), (SH_STUCK, SH_STUCK))):
        odds = bot.hsi_odds(at)
        assert (odds.warn, odds.stale) == want, (at, odds.warn, odds.stale)
    # the approximate anchor's hold (as in 1.149.0) withdraws nothing by itself; a still quote under it does, as under any anchor
    bot.store.put("anchor:HSI", [sh_at(16, 10), "24600", "日市 16:30 收市（近似：缺 16:10 现货收市时的期货价，未含 16:10–16:30 期货变动）",
                                 "10/2026", sh_at(16, 31), sh_at(16, 31)])
    for at, stale in ((sh_at(21, 1), ""), (sh_at(21, 3, 1), SH_STUCK)):
        odds = bot.hsi_odds(at)
        assert (odds.warn, odds.stale) == ("期货锚点是近似值，暂不给建议", stale), (at, odds.warn, odds.stale)
    # Predict's own target price for the market known: settle_on_strike measures the card against it, the odds rebuilt — the stale
    # price goes along with the hold (before the review's follow-up: the hold alone, so nothing withdrawn on that path); a figure
    # too far from the local close (another unit) is not used, the odds kept as they were, the stale price with them
    slug = m.predict_slug(bot.config.predict_slugs["HSI"], odds.target)
    for strike, ref in ((SH_STRIKE, D(SH_STRIKE)), ("20000", odds.ref)):
        bot.predict.strikes[slug] = (D(strike), m.time.monotonic())
        shown = bot.settle_on_strike("恒生指数", odds)
        assert shown.ref == ref and (shown.warn, shown.stale) == ("期货锚点是近似值，暂不给建议", SH_STUCK), (strike, shown.ref, shown.warn, shown.stale)


async def stale_hold_nvda():
    """NVDA first-touch in the session, a 跟买1 resting on "$210 first": Yahoo's trade stands still while its reads go on answering
    (the index refused, 2% from it). Over five minutes old by its own time the card holds — and the order is withdrawn at that
    look; the book falls to where the bid was and nothing of ours is there; the trade moves, the order is placed afresh. A read
    that fails is an outage, not a trade standing still: the hold as before, the order left to the odds' pause and the generic
    clock (MOD-NVDA-CLOSE-RESTING). Out of the session (the close's own rule) and on a decided market there is no stale price."""
    day = dt.date(2026, 11, 10)  # a Tuesday
    t1 = m.et_wall_ms(day, 11, 0)
    nvda = next(s for s in m.TOUCH_MARKETS if s.key == "NVDA-2027")
    bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                   "SIM_MARKETS": "touch", "SIM_WAYS": "maker", "SIM_MAKER_POINTS": "off"}),
                m.Store(":memory:"), T["FM"](t1), None)
    card = bot.touches["NVDA-2027"]
    assert isinstance(card, m.StockTouchMarket)
    bot.predict.info[nvda.slug] = {"outcomes": ["$210", "$160"], "created_ms": m.et_wall_ms(dt.date(2026, 11, 2), 10, 0)}
    card.start_ms = bot.predict.info[nvda.slug]["created_ms"]
    card.save({"kind": "clear", "through": m.et_wall_ms(day, 0, 0)})
    card.sigma, card.sigma_ms = 0.44, t1
    traded = t1 - 20_000  # Yahoo's trade at 10:59:40, 185.00

    async def look(now: int, priced: int, read: int, bid: float, ask: float, failing: str = ""):
        bot.market.now = now
        card.yahoo = (D("185.0000"), priced)
        card.price, card.priced_ms, card.index_live = D("185.0000"), priced, False
        card.fetched_ms = card.minutes_ms = read
        card.failures["price"] = failing
        bot.predict.books["NVDA-2027"] = m.PredictBook("NVDA-2027", nvda.slug, "77", "t", ((D(f"{bid:.2f}"), D("300")),),
                                                       ((D(f"{ask:.2f}"), D("300")),), now)
        mk = next(x for x in bot.sim_markets(now) if x.key == "NVDA-2027")
        bot.sim_ran = -1e9
        await bot.sim_step(now)
        return mk, sorted((t["label"], t["status"], round(t["price"], 2), t["shares"], (t.get("withdrawn") or {}).get("why", ""))
                          for t in bot.sim_trades().values())

    fair = m.first_touch(185.0, 160.0, 210.0, 0.44, 0.0, card.remaining_years(t1)).fair_upper
    bid = round(fair - 0.12, 2)  # others bid 12¢ under the card's "$210 first"
    mk, recs = await look(t1, traded, t1, bid, bid + 0.15)
    assert not mk.hold and not mk.price_problem and recs == [("挂210", "resting", bid, 0.0, "")], (mk.hold, recs)
    t2 = traded + 5 * 60_000  # five minutes by the trade's own time: still fine
    mk, recs = await look(t2, traded, t2 - 5_000, bid, bid + 0.15)
    assert not mk.hold and recs == [("挂210", "resting", bid, 0.0, "")], (mk.hold, recs)
    t3 = t2 + 1  # over five: the reads answer, the trade stands still
    why = f"美股交易中，现价还停在 {m.stamp(traded, seconds=False)}（超过 5 分钟没更新）"
    mk, recs = await look(t3, traded, t3 - 5_000, bid, bid + 0.15)
    assert card.advice_problem(t3) == mk.hold == f"{why}；暂不给建议", mk.hold  # the card's words as before
    assert isinstance(card.odds(t3), m.TouchOdds) and mk.price_problem == why, mk.price_problem
    assert recs == [("挂210", "cancelled", bid, 0.0, f"{why}，撤单")], recs  # before: ("挂210", "resting", bid, 0.0, "")
    # the stock fell, the card cannot see it: the book comes down to where the bid was — nothing of ours there, nothing placed
    mk, recs = await look(t3 + 60_000, traded, t3 + 55_000, bid - 0.10, bid)
    assert all(t["shares"] == 0 for t in bot.sim_trades().values()) and [r[1] for r in recs] == ["cancelled"], recs
    # the trade moves again: quoted afresh
    fresh = t3 + 88_000
    mk, recs = await look(t3 + 90_000, fresh, t3 + 90_000, bid, bid + 0.15)
    assert not mk.hold and not mk.price_problem and ("挂210", "resting", bid, 0.0, "") in recs, (mk.hold, recs)
    # an outage: the reads fail from here, the last trade five minutes old a few seconds before the last read is — the hold as
    # before, and the order stands (the odds pause at the read's five minutes; then SIM_NO_ODDS_WITHDRAW_MINUTES)
    t4 = fresh + 5 * 60_000 + 1
    mk, recs = await look(t4, fresh, t3 + 90_000, bid, bid + 0.15, failing="价格：HTTP 502: Bad Gateway")
    old = f"美股交易中，现价还停在 {m.stamp(fresh, seconds=False)}（超过 5 分钟没更新）"
    assert mk.hold == f"{old}；暂不给建议" and mk.price_problem == "" and ("挂210", "resting", bid, 0.0, "") in recs, (mk.hold, recs)
    assert card.stale_price(t4) == old and card.price_problem(t4) == ""
    card.failures["price"] = ""
    assert card.price_problem(t4) == old
    # the session's open on yesterday's price (the other rule), likewise
    card.price, card.priced_ms = D("185.0000"), m.us_session(day)[0] - 60_000
    opened = f"美股已开盘，现价还是开盘前的（停在 {m.stamp(card.priced_ms, seconds=False)}），今天的价格还没读到"
    assert card.price_problem(t1) == opened and card.advice_problem(t1) == f"{opened}；暂不给建议"
    # out of the session (the close's own rule: MOD-NVDA-CLOSE-RESTING), and on a market the minutes decided: nothing stale
    card.price, card.priced_ms = D("185.0000"), traded
    assert card.price_problem(m.et_wall_ms(day, 17, 0)) == "" and card.advice_problem(m.et_wall_ms(day, 17, 0)).startswith("美股休市中")
    card.save({"kind": "high", "time": t1, "hi": 210.5, "lo": 184.5})
    assert card.price_problem(t3) == "" and card.advice_problem(t3) == ""


_sh_get, _sh_health = m.http_get, m.SOURCE_HEALTH
m.http_get, m.SOURCE_HEALTH = sh_get, m.SourceHealth()
try:
    for live_on in (False, True):
        for strike in (None, SH_STRIKE):
            asyncio.run(stale_hold_hsi(live_on, strike))
    for moves in (True, False):
        asyncio.run(stale_hold_hsi_restart(moves))
    for strike in (None, SH_STRIKE):
        asyncio.run(stale_hold_hsi_unsent(strike))
    asyncio.run(stale_hold_hsi_scope())
finally:
    m.http_get, m.SOURCE_HEALTH = _sh_get, _sh_health
asyncio.run(stale_hold_nvda())


# --- XR-SELFCAP-GHOST-CLOCK: SIM-SELF-MATCH's ghost clause read by the time the book's levels were taken (FEED-FETCHED-MS) ----------
# sim_taker_self_cap (SIM-SELF-MATCH) keeps a taker's cap a cent under our own real buy of the other side, and still counts one whose
# cancel Predict confirmed after the book was read (the book may still show it). "Read" was the book's fetched_ms, which FEED-FETCHED-MS
# made the book's age alone: an unchanged re-send (refresh_age) and the heartbeat (touch) restart it over the same old levels
# (book_restarted), and a REST read asked before the confirmation is stamped with its landing after it. On such a book the maker side
# (sim_own_resting, ghost_unsure) took the cancelled order as maybe still shown, while the taker side released the cap and bought it: a
# real LIMIT order sized on liquidity gone at Predict, and 距吃单 / the order check read 可买 for it. Now read by book_content_ms, the
# time the levels were taken, as sim_own_order / ghost_unsure / own_on_book read it.
def selfcap_ghost_clock():
    """Our real 涨 bid 60¢ × 50, the book's only bid, was withdrawn and Predict confirmed the cancel at t0 (a 挂涨; or what a 吃涨 at a
    60¢ LIMIT did not fill, 40 of 50); the fair 涨 is 30¢, so the 跌 taker's cap is 59¢ and a book whose levels were taken before t0
    shows our cancelled bid as a 40¢ "ask" of 跌."""
    now = NOW
    bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_TAKER_HOLD_SECONDS": "0"}), m.Store(":memory:"), T["FM"](now), None)
    slug, t0 = "hsi-selfcap-ghost", now - 2_000
    done = {"state": "done", "final": "cancelled", "order_id": "9", "answered_ms": now - 50_000, "answered_at": now - 50_000,
            "cancel_sent_at": t0 - 120, "cancel_done_at": t0, "cancel_done_ms": t0}
    maker = {"v": 2, "market": slug, "slug": slug, "item": "恒生指数", "kind": "close", "key": "HSI", "side": "up", "label": "挂涨",
             "maker": True, "status": "cancelled", "shares": 0.0, "order": 50.0, "price": 0.60, "fills": [], "revisions": [],
             "opened": now - 60_000, "withdrawn": {"at": t0 - 150, "why": "撤单线", "rule": True}, "live": done}
    rest = {**maker, "label": "吃涨", "maker": False, "status": "filled", "shares": 10.0, "price": 0.6096, "cap": 0.61,
            "live": {**done, "want": {"maker": False, "limit": 0.60, "cap": 0.61, "shares": 50.0}}}
    ours, asks = ((D("0.60"), D("50")),), ((D("0.62"), D("40")),)
    card = lambda book: m.SimMarket(slug, "恒生指数", "close", "HSI", 0.30, book, 0.02, "", ("涨", "跌"), T["SETTLE_HSI"])
    pushed = m.PredictBook("HSI", slug, "1", "t", ours, asks, t0 - 1_000, 200, content_ms=t0 - 1_000)  # a changed push of before t0
    restamped = {"re-sent unchanged / heartbeat after the confirmation": m.book_restarted(pushed, t0 + 500),  # refresh_age, touch
                 "REST read asked before the confirmation, landing after it": m.PredictBook("HSI", slug, "1", "t", ours, asks, t0 + 300, 200,
                                                                                             content_ms=t0 - 200)}
    for rec, tid in ((maker, f"{slug}|up|挂#1"), (rest, f"{slug}|up|吃")):
        trades = lambda: m.TradeMap({tid: {**rec, "live": dict(rec["live"])}})

        def take(book):
            bot.sim_paper_used.clear()  # (a paper Bot: what an earlier case "took" of these asks is not this case's matter)
            changed: list = []
            bot.sim_take(card(book), trades(), now, changed)
            return [(t["label"], t["shares"]) for _, t in changed if not t["maker"]]

        for what, book in restamped.items():
            what = (rec["label"], what)
            assert book.fetched_ms > t0 >= m.book_content_ms(book) and not book.stale(now), (what, book.fetched_ms, m.book_content_ms(book))
            if rec["maker"]:  # the maker side (FEED-FETCHED-MS) reads this very book as maybe still showing the cancelled order ...
                assert m.ghost_unsure(trades()[tid], book) and m.Bot.sim_own_resting(trades(), slug, "up", book) == (50.0, 0.6), what
            # ... and so does the taker side now: the cap a cent under our old level, nothing bought from the ghost
            cap = bot.sim_taker_self_cap(trades(), card(book), "down", 0.59)
            assert cap == 0.39, (what, cap)  # before: 0.59
            assert take(book) == [], (what, take(book))  # before: [('吃跌', 50.0)], "bought" from our own cancelled bid
            # the 距吃单 card and the order check use the trader's cap: no 可吃单 / 可买 for the ghost's shares
            bot.sim_paper_used.clear()
            gap = bot.sim_taker_gap(card(book), trades(), now)
            down = next(s for s in gap["sides"] if s["side"] == "down")
            assert gap["state"] != "ready" and "可买" not in gap["text"] + gap["detail"], (what, gap["state"], gap["text"])  # before: ready, 可买 50 份
            assert not down["got"] and down["block"] == bot.sim_taker_self_text(0.39) and "40.0¢" in down["block"], (what, down)
            bot.sim_trades, bot.sim_markets = trades, (lambda now_ms, book=book: [card(book)])
            try:
                (row,) = bot.sim_checks(now)
            finally:
                del bot.sim_trades, bot.sim_markets
            assert "自己的" in row["taker"] and "可买" not in row["taker"], (what, row["taker"])  # before: 跌：封顶 59.0¢，可买 50 份
        # the same levels not restamped hold the cap, as SIM-SELF-MATCH made them; a book whose levels were taken after the
        # confirmation (the push that drops our bid, a REST read asked after it) releases it: what bids there is somebody else's
        assert bot.sim_taker_self_cap(trades(), card(pushed), "down", 0.59) == 0.39 and take(pushed) == []
        after = m.PredictBook("HSI", slug, "1", "t", ((D("0.55"), D("30")),), asks, t0 + 300, 200, content_ms=t0 + 300)
        assert bot.sim_taker_self_cap(trades(), card(after), "down", 0.59) == 0.59 and take(after) == [("吃跌", 30.0)], take(after)
        # a book built without content_ms (its levels' time is its fetched_ms): SIM-SELF-MATCH's own comparison, unchanged
        plain = lambda at: m.PredictBook("HSI", slug, "1", "t", ours, asks, at, 200)
        assert [bot.sim_taker_self_cap(trades(), card(plain(at)), "down", 0.59) for at in (t0 - 1, t0, t0 + 1)] == [0.39, 0.39, 0.59]
        # never more than the fetched_ms reading allowed: whatever the two stamps (levels taken no later than the book is stamped)
        for content in (t0 - 1, t0, t0 + 1):
            for fetched in (t0 - 1, t0, t0 + 1, t0 + 500):
                if fetched >= content:
                    bk = m.PredictBook("HSI", slug, "1", "t", ours, asks, fetched, 200, content_ms=content)
                    assert bot.sim_taker_self_cap(trades(), card(bk), "down", 0.59) <= (0.39 if fetched <= t0 else 0.59), (content, fetched)


async def selfcap_ghost_clock_live(how: str) -> tuple[list, list]:
    """The same end to end: the real trader (LiveBot over test_trading's stand-ins and the SDK test suite's key: nothing real is
    contacted) and the real PredictStream, the books' stamps and live_cancel's cancel_done_ms on one clock. Our real 挂涨 rests at
    60¢ × 50 (SIM_WAYS=both, the 均衡 / 进取 presets), the book's only bid. At T a changed push shows it while the fair 涨 has fallen
    to 30¢: the express lane withdraws it and Predict confirms the cancel 100 ms later. At T+300 the server re-sends the same levels
    or the heartbeat comes (``how``; "" nothing comes); at T+400 a price move wakes the express lane. Returns the 跌 taker records
    and the orders sent after the cancel."""
    import json
    real_time = m.time

    class Clock:  # main's and live's wall clock in the test's hands; monotonic moved on with it
        def __init__(self, ms): self.ms, self.skew = ms, 0.0
        def time(self): return (self.ms + 0.5) / 1000  # int(time.time() * 1000) is self.ms exactly
        def monotonic(self): return real_time.monotonic() + self.skew
        def pass_ms(self, ms): self.ms, self.skew = self.ms + ms, self.skew + ms / 1000
        def __getattr__(self, name): return getattr(real_time, name)

    class Ws:  # the connection the heartbeat is answered on
        async def send(self, text): pass

    async def looked(feed):  # the express looks a push started, to their end
        while feed.express_tasks:
            await asyncio.gather(*list(feed.express_tasks))
            await asyncio.sleep(0)

    clock = Clock(NOW)
    m.time = L.time = clock
    try:
        # the settings as deployed but for the resting orders' sessions (SIM_WAYS=both: the 均衡 / 进取 presets); not make_bot's,
        # whose resting orders rest as placed (SIM_MAKER_EXIT_CENTS=0): this one has to be withdrawn by the bot's own look
        bot = L.LiveBot(m.Config.from_env({**T["base"], "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
                                           "SIM_WAYS": "both", "SIM_MARKETS": "all", "SIM_MAKER_SESSION": "off", "LIVE": "on",
                                           "PREDICT_PRIVATE_KEY": T["KEY"], "PREDICT_API_KEY": "k", "LIVE_MAX_ORDER_USD": "100",
                                           "LIVE_MAX_OPEN_USD": "250", "LIVE_MAX_DAILY_LOSS_USD": "50"}), m.Store(":memory:"), T["FM"](NOW), None)
        bot.market.now_ms = lambda: clock.ms  # the market clock: the same one
        bot.live.api, bot.live.chain = T["FakeApi"](), T["FakeChain"]()
        bot.live.api.markets["101"] = T["market_json"]("101")
        api, feed, stream = bot.live.api, bot.predict, bot.predict.stream
        cancel = api.remove_orders

        async def remove_orders(ids):  # Predict confirms a cancel 100 ms after it goes out
            clock.pass_ms(100)
            return await cancel(ids)
        api.remove_orders = remove_orders
        fair = {"up": 0.62}
        bot.sim_markets = lambda now_ms: [m.SimMarket(HSI_SLUG, "恒生指数", "close", "HSI", fair["up"], feed.books["HSI"], 0.03, "", ("涨", "跌"),
                                                      T["SETTLE_HSI"], {"basis": {"fair_up": fair["up"], "ref": 24600.0}, "proxy": None,
                                                                        "sources": [{"what": "恒指期货", "source": "etnet"}]})]
        # the book as last read (somebody's 58¢ × 10 then): the push at T is a change
        feed.books["HSI"] = m.PredictBook("HSI", HSI_SLUG, "101", "t", ((D("0.58"), D("10")),), ((D("0.62"), D("40")),), clock.ms - 2_000, 200)
        await bot.live_prepare()
        await bot.live_prefetch(clock.ms)
        assert bot.live.ready, bot.live.ready_error
        await bot.live.market("101")  # its outcome tokens and fee in hand: a decision can go out at once
        stream.subscribed = {"101": ("HSI", False)}
        stream.ws, stream.connected_at, stream.last_frame = Ws(), clock.monotonic(), clock.monotonic()
        # our real 挂涨 at 60¢ × 50, placed a minute ago, open on Predict
        api.open["9"] = {"id": "9", "status": "OPEN", "amount": str(50 * T["WEI"]), "amountFilled": "0", "strategy": "LIMIT",
                         "order": {"tokenId": "1011", "hash": "0x" + "9" * 64}}
        bot.store.put(f"sim:{HSI_SLUG}|up|挂", {
            "v": 2, "market": HSI_SLUG, "slug": HSI_SLUG, "item": "恒生指数", "kind": "close", "key": "HSI", "side": "up", "label": "挂涨",
            "maker": True, "status": "resting", "shares": 0.0, "order": 50.0, "price": 0.60, "fills": [], "revisions": [],
            "opened": clock.ms - 60_000, "settle": T["SETTLE_HSI"], "fair": 0.70, "edge": 0.10, "version": bot.sim_version(),
            "queue_ahead": 0.0, "queue_min": 0.0,
            "live": {"state": "open", "order_id": "9", "hash": "0x" + "9" * 64, "answered_ms": clock.ms - 59_000, "answered_at": clock.ms - 59_000,
                     "placed_at": clock.ms - 59_500, "sent_at": clock.ms - 59_500, "attempt": 1, "events": [],
                     "want": {"maker": True, "side": "up", "shares": 50.0, "price": 0.60, "token": "1011", "outcome": "Up", "index_set": 1,
                              "fee_bps": 200, "neg_risk": False, "yield_bearing": False, "condition_id": "0x" + "ab" * 32}}})
        levels = {"bids": [["0.60", "50"]], "asks": [["0.62", "40"]]}
        fair["up"], t0 = 0.30, clock.ms
        stream.book("101", {"version": 7, **levels})  # T: the change, our bid on it; the fair 涨 has fallen to 30¢
        await looked(feed)
        (ghost,) = [t for t in bot.sim_trades().values() if (t.get("live") or {}).get("order_id") == "9"]
        assert (ghost["status"], ghost["live"]["state"], ghost["live"]["cancel_done_ms"]) == ("cancelled", "done", t0 + 100), ghost["live"]
        assert [c[0] for c in api.calls] == ["remove"], api.calls  # withdrawn by the bot's own look; nothing taken meanwhile (SIM-SELF-MATCH)
        sent = len(api.calls)
        clock.pass_ms(t0 + 300 - clock.ms)
        if how == "heartbeat":
            await stream.handle(stream.ws, json.dumps({"type": "M", "topic": m.PREDICT_WS_HEARTBEAT, "data": 1}))
        elif how == "resend":
            stream.book("101", {"version": 8, **levels})
            await looked(feed)
        book = feed.books["HSI"]
        assert (m.book_content_ms(book), book.fetched_ms) == (t0, t0 + 300 if how else t0), (how, m.book_content_ms(book) - t0, book.fetched_ms - t0)
        clock.pass_ms(100)
        await bot.sim_express("HSI")  # T+400: a price move wakes the express lane on the book as stored
        await looked(feed)
        takers = [(t["label"], t["price"], t["order"]) for t in bot.sim_trades().values() if not t.get("maker") and t["side"] == "down"]
        orders = [(c[0], c[1]["data"]["order"]["tokenId"], round(int(c[1]["data"]["order"]["makerAmount"]) / int(c[1]["data"]["order"]["takerAmount"]), 2))
                  if c[0] == "create" else c for c in api.calls[sent:]]
        return takers, orders
    finally:
        m.time = L.time = real_time


selfcap_ghost_clock()
for how in ("resend", "heartbeat", ""):
    takers, orders = asyncio.run(selfcap_ghost_clock_live(how))
    assert (takers, orders) == ([], []), (how, takers, orders)
    # before ("resend", "heartbeat"): ([('吃跌', 0.408, 100.0)], [('create', '1012', 0.4)]) — a real LIMIT BUY of 跌 (token 1012) at
    # 40¢ × 100, sized and priced on our own cancelled 60¢ bid; with nothing re-stamped ("") nothing was bought before either
print("AUDIT_XREVIEW_OK")
