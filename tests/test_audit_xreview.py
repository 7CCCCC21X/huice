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


asyncio.run(hash_read_error_409())
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
print("AUDIT_XREVIEW_OK")
