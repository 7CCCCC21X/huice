"""Audit cluster live-a2: regression tests for the verified findings fixed in this cluster (fakes only, offline)."""
import asyncio, pathlib, sys, types
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import main as m  # noqa: F401
import preopen as P

# test_trading's fakes (FakeApi, make_bot, step, hsi …) without running its suites: only the definitions before "async def run()"
_src = (ROOT / "tests" / "test_trading.py").read_text(encoding="utf-8").split("\nasync def run():")[0]
T: dict = {"__file__": str(ROOT / "tests" / "test_trading.py")}
exec(compile(_src, "test_trading_head", "exec"), T)
make_bot, step, hsi, NOW, HSI_SLUG, WEI = T["make_bot"], T["step"], T["hsi"], T["NOW"], T["HSI_SLUG"], T["WEI"]


async def live_unexplained_preopen():
    """LIVE-UNEXPLAINED-PREOPEN: an order of unknown state settled by the account's positions (the evidence path, or past its
    expiry) took the pre-open mode's shares on its outcome for its own fill: booked filled at its limit, the same shares then in
    the model trader's records and in the pre-open ledger (exposure, the P&L and the loss stop counting them twice). The pre-open
    mode's read fills now explain their shares there; while that mode's order on the outcome is not read, or a pair of it may
    have been merged into USDT with the market's other side, a surplus is not attributed: the record waits, reserved — the
    positions read for it once a minute at most, past its expiry too ((i)–(k), the review's follow-up)."""
    async def _none(*a, **k): return None
    async def _empty(*a, **k): return []
    async def _noop(ids): return {"removed": [], "noop": list(ids)}
    mkt = lambda at: hsi(0.70, [("0.55", "300")], [("0.58", "400")], at)
    po_order = lambda side, state, filled: {"state": state, "filled": filled, "index_set": 1 if side == "up" else 2, "hash": f"0xp{side}", "id": f"p-{side}"}

    def attach(bot, up, down):
        """The pre-open controller as far as LiveBot reads it: a ledger of one window on market 101 (the pair row's real shape,
        read through preopen.fills_by_outcome / unverified_outcomes); its own orders are not unrecorded ones."""
        pair = {"slug": "w1", "key": "HSI", "name": "恒生指数 w1", "market_id": "101", "start_ms": NOW - 300_000, "price": "0.49", "shares": "10",
                "phase": "closed", "orders": {"up": up, "down": down}}
        bot.preopen = types.SimpleNamespace(
            alive=True, winding="", manual_pause=False, limited_at=(), account_rows=None, account_at=0,
            ledger=types.SimpleNamespace(rows={"w1": pair}), filled_by_outcome=lambda days=30: P.fills_by_outcome([pair], NOW, days),
            unmanaged_rows=lambda rows: [r for r in rows if not str(r.get("id")).startswith("p-")], known=lambda: (set(), set()),
            daily_pnl=lambda: 0.0, risk=lambda: 0.0, status=lambda: "开盘前预挂（测试替身）", request_rate=lambda: None, now=lambda: NOW)
        return pair

    def held(**shares):
        return [{"id": f"pos-{side}", "market": {"id": 101, "title": "恒生指数"}, "outcome": {"name": side.title(), "indexSet": 1 if side == "up" else 2,
                 "onChainId": f"101{1 if side == 'up' else 2}"}, "amount": str(int(n * WEI))} for side, n in shares.items()]

    async def lost(up, down, way="maker"):
        """A model order on 101 / Up that Predict lost (off the open list, the hash read finding nothing, the cancel a noop): unknown."""
        bot = make_bot(SIM_WAYS=way)
        await bot.live_prepare(); fake = bot.live.api
        await bot.live_prefetch(NOW)
        pair = attach(bot, up, down)
        tid = f"{HSI_SLUG}|up|{'挂' if way == 'maker' else '吃'}"
        await step(bot, NOW, mkt(NOW))
        t = bot.sim_trades()[tid]
        assert (str(t["market_id"]), int(t["live"]["want"]["index_set"])) == ("101", 1), (t.get("market_id"), t["live"]["want"])
        oid = t["live"]["order_id"]
        fake.open.pop(oid, None)
        fake.orders, fake.order, fake.remove_orders = _empty, _none, _noop
        for at in (NOW + 10_000, NOW + 20_000, NOW + 30_000, NOW + 65_000):
            await step(bot, at, mkt(at))
        assert bot.sim_trades()[tid]["live"]["state"] == "unknown", bot.sim_trades()[tid]["live"]
        return bot, fake, pair, oid

    async def settle_by_evidence(bot, *offsets):
        for dt_ms in offsets or (125_000, 185_000, 245_000, 305_000):  # three whole lists without it, three minutes unknown: the positions decide
            await step(bot, NOW + dt_ms, mkt(NOW + dt_ms))

    rec = lambda bot, oid: next(t for t in bot.sim_trades().values() if t["live"].get("order_id") == oid)
    see = lambda t: (t["live"]["state"], t["status"], t["shares"], t["live"].get("last_error", ""), [e["what"] for e in t["live"]["events"][-2:]])

    # (a) the pre-open mode's 49¢ order on 101 / Up read (10 filled), the account holds exactly those 10: the lost order filled nothing —
    # it ends unfilled, its reservation freed (before: "持仓比记录多 10 份：按成交 10 份记", 20 recorded against 10 held)
    bot, fake, pair, oid = await lost(po_order("up", "final", 10.0), po_order("down", "final", 0.0))
    fake.positions_rows = held(up=10)
    await settle_by_evidence(bot)
    t = rec(bot, oid)
    assert t["live"]["state"] == "done" and t["status"] == "cancelled" and t["shares"] == 0 and bot.live_reserved(t) == 0, see(t)
    assert any("账户持仓也没有多出的份数：按未成交结束" in e["what"] for e in t["live"]["events"]), see(t)
    assert bot.live_recorded_by_outcome().get(("101", 1)) == 10.0, bot.live_recorded_by_outcome()  # the ledger's 10 alone: as held

    # (b) the pre-open order there not read yet (open; the ledger knows of no fill): the holding may be its fill — the record waits,
    # reserved, saying why; read (10 filled), the next look releases it
    bot, fake, pair, oid = await lost(po_order("up", "open", 0.0), po_order("down", "final", 0.0))
    fake.positions_rows = held(up=10)
    await settle_by_evidence(bot)
    t = rec(bot, oid)
    assert t["live"]["state"] == "unknown" and t["shares"] == 0 and bot.live_reserved(t) > 0 and "预挂" in t["live"].get("last_error", ""), see(t)
    assert "持仓读取失败" not in t["live"]["last_error"], see(t)
    pair["orders"]["up"].update(state="final", filled=10.0)
    await settle_by_evidence(bot, 365_000)
    t = rec(bot, oid)
    assert t["live"]["state"] == "done" and t["status"] == "cancelled" and t["shares"] == 0 and bot.live_reserved(t) == 0, see(t)

    # (c) the lost order did fill: 30 of its own beside the pre-open mode's 10 (40 held) — 30 booked, not 40
    bot, fake, pair, oid = await lost(po_order("up", "final", 10.0), po_order("down", "final", 0.0))
    fake.positions_rows = held(up=40)
    await settle_by_evidence(bot)
    t = rec(bot, oid)
    assert t["live"]["state"] == "done" and t["status"] == "filled" and abs(t["shares"] - 30) < 1e-9, see(t)
    assert any("持仓比记录多 30 份：按成交 30 份记" in e["what"] for e in t["live"]["events"]), see(t)
    assert bot.live_recorded_by_outcome().get(("101", 1)) == 40.0, bot.live_recorded_by_outcome()

    # (d) the pre-open pair filled both ways (10 Up, 10 Down): Predict merges the two sides into USDT, so the 10 Up held may be the
    # lost order's own fill — taking the ledger's 10 Up off would book nothing, keeping them would book the pair's: the record waits
    # for Predict's word or an operator, reserved (never a guess either way)
    bot, fake, pair, oid = await lost(po_order("up", "final", 10.0), po_order("down", "final", 10.0))
    fake.positions_rows = held(up=10)
    await settle_by_evidence(bot)
    t = rec(bot, oid)
    assert t["live"]["state"] == "unknown" and t["shares"] == 0 and bot.live_reserved(t) > 0 and "合并" in t["live"].get("last_error", ""), see(t)
    result = await bot.live_control("release", {"id": oid})  # the way out its notice names: an operator checked the site
    assert result["ok"] and rec(bot, oid)["live"]["state"] == "done" and bot.live_reserved(rec(bot, oid)) == 0, (result, see(rec(bot, oid)))
    # ...but nothing beyond the records at all: unfilled, whatever the pre-open mode holds there (no wait)
    bot, fake, pair, oid = await lost(po_order("up", "final", 10.0), po_order("down", "final", 10.0))
    fake.positions_rows = []
    await settle_by_evidence(bot)
    t = rec(bot, oid)
    assert t["live"]["state"] == "done" and t["status"] == "cancelled" and t["shares"] == 0 and bot.live_reserved(t) == 0, see(t)

    # (e) the pre-open mode's fill on the other side only (10 Down held): nothing changes for this side — released as before
    bot, fake, pair, oid = await lost(po_order("up", "final", 0.0), po_order("down", "final", 10.0))
    fake.positions_rows = held(down=10)
    await settle_by_evidence(bot)
    t = rec(bot, oid)
    assert t["live"]["state"] == "done" and t["status"] == "cancelled" and t["shares"] == 0 and bot.live_reserved(t) == 0, see(t)

    # (f) a taker of unknown state past its expiry, the holding the pre-open mode's: EXPIRED unfilled (before: filled 10 at its limit)
    bot, fake, pair, oid = await lost(po_order("up", "final", 10.0), po_order("down", "final", 0.0), way="taker")
    fake.positions_rows = held(up=10)
    at = NOW + T["L"].TAKER_ORDER_SECONDS * 1000 + bot.LIVE_UNKNOWN_MARGIN_MS + 5_000
    await step(bot, at, mkt(at))
    t = rec(bot, oid)
    assert t["live"]["state"] == "done" and t["status"] == "cancelled" and t["shares"] == 0 and bot.live_reserved(t) == 0, see(t)
    assert any("有效期已过" in e["what"] and "按未成交结束" in e["what"] for e in t["live"]["events"]), see(t)

    # (g) the step rechecks its unknown records side by side: one record's wait on the pre-open mode is not another's reason —
    # A's read fails while B's look (answered first) waits on the unread pre-open order; A still reads as a failed read
    bot = make_bot(SIM_WAYS="maker")
    attach(bot, po_order("up", "open", 0.0), po_order("down", "final", 0.0))
    gate, calls = asyncio.Event(), []
    async def positions():
        calls.append(1)
        if len(calls) == 1:  # A's read: answered last, and it fails
            await gate.wait()
            raise T["m"].RemoteError("HTTP 503")
        gate.set()  # B's read: answered at once — the 10 Up there may be the pre-open order's fill
        return held(up=10)
    bot.live.api.positions = positions
    async def look(trade):
        got = await bot.live_unexplained(trade)
        return got, bot.live_positions_busy, bot.live_positions_cut  # what live_recheck reads for it right after
    a, b = {"market_id": "101", "live": {"want": {"index_set": 1}}}, {"market_id": "101", "live": {"want": {"index_set": 1}}}
    got_a, got_b = await asyncio.gather(look(a), look(b))
    assert got_b[0] is None and "预挂" in got_b[1], got_b
    assert got_a == (None, "", False), got_a

    # (h) the pre-open mode's 10 Up beside a model record's 10 Down on the same market: merged into USDT, the 10 Up held may be the
    # lost order's own — it waits; without that record on the other side the ledger's 10 explain the 10 held (nothing filled)
    bot = make_bot(SIM_WAYS="maker")
    attach(bot, po_order("up", "final", 10.0), po_order("down", "final", 0.0))
    bot.live.api.positions_rows = held(up=10)
    gone, down = {"market_id": "101", "live": {"want": {"index_set": 1}}}, {"market_id": "101", "shares": 10.0, "live": {"want": {"index_set": 2}}}
    bot.sim_trades = lambda: {"gone": gone, "down": down}
    assert await bot.live_unexplained(gone) is None and "合并" in bot.live_positions_busy, bot.live_positions_busy
    bot.sim_trades = lambda: {"gone": gone}
    assert await bot.live_unexplained(gone) == 0.0 and bot.live_positions_busy == "", bot.live_positions_busy

    # --- review follow-up: a look that settles nothing is made once a minute at most past the order's expiry too. An expired record
    # looked every step, which the waits above turned into a standing flood: every page of /positions read each step for as long as
    # the surplus cannot be told apart (days; until /live release), inside the step's sync and on the rate limit the orders share
    def counted(bot):
        """Every read of the account's positions from here on: the bot's clock at each."""
        reads, whole = [], bot.live.api.positions
        async def positions():
            reads.append(bot.market.now)
            return await whole()
        bot.live.api.positions = positions
        return reads

    async def steps(bot, first, last):
        for at in range(first, last + 1, 5_000):  # the trader's look every five seconds
            await step(bot, at, mkt(at))

    EVERY = T["L"].LiveBot.LIVE_UNEXPLAINED_EVERY_MS
    EXPIRED = NOW + T["L"].TAKER_ORDER_SECONDS * 1000 + T["L"].LiveBot.LIVE_UNKNOWN_MARGIN_MS  # a taker placed at NOW: given up from here (900 s)
    GONE = EXPIRED + 5_000  # ...and the first step past it, as in (f)

    # (i) the pre-open pair filled both ways (the wait of (d), which only an operator ends), five-second steps from the evidence
    # path to two minutes past the order's expiry: looked at once a minute throughout (before: every step from the expiry on)
    bot, fake, pair, oid = await lost(po_order("up", "final", 10.0), po_order("down", "final", 10.0), way="taker")
    fake.positions_rows = held(up=10)
    reads = counted(bot)
    await settle_by_evidence(bot, 125_000, 185_000, 245_000)
    assert reads == [NOW + 245_000] and rec(bot, oid)["live"]["state"] == "unknown", ([r - NOW for r in reads], see(rec(bot, oid)))
    await steps(bot, NOW + 250_000, EXPIRED + 120_000)
    t = rec(bot, oid)
    assert t["live"]["state"] == "unknown" and t["shares"] == 0 and bot.live_reserved(t) > 0, see(t)
    assert "有效期已过" in t["live"]["last_error"] and "合并" in t["live"]["last_error"], see(t)  # the last looks were the expiry path's
    assert reads == [NOW + 245_000 + i * EVERY for i in range(13)], [r - NOW for r in reads]  # 245 s, 305 s, … 965 s: straight across the expiry
    result = await bot.live_control("release", {"id": oid})
    assert result["ok"] and rec(bot, oid)["live"]["state"] == "done" and bot.live_reserved(rec(bot, oid)) == 0, (result, see(rec(bot, oid)))

    # (j) past the expiry, the pre-open order there not read yet: the first look is made at once (none within the last minute) and
    # the record waits, reserved; the ledger reads its order (the 10 held are its own) and the next look — a minute after the
    # first, not sooner — ends the record unfilled: the wait still ends by itself
    bot, fake, pair, oid = await lost(po_order("up", "open", 0.0), po_order("down", "final", 0.0), way="taker")
    fake.positions_rows = held(up=10)
    reads = counted(bot)
    await steps(bot, GONE, GONE + 25_000)
    t = rec(bot, oid)
    assert reads == [GONE] and t["live"]["state"] == "unknown" and bot.live_reserved(t) > 0 and "预挂" in t["live"]["last_error"], ([r - GONE for r in reads], see(t))
    pair["orders"]["up"].update(state="final", filled=10.0)
    await steps(bot, GONE + 30_000, GONE + 55_000)
    t = rec(bot, oid)
    assert reads == [GONE] and t["live"]["state"] == "unknown" and bot.live_reserved(t) > 0, ([r - GONE for r in reads], see(t))
    await step(bot, GONE + EVERY, mkt(GONE + EVERY))
    t = rec(bot, oid)
    assert reads == [GONE, GONE + EVERY], [r - GONE for r in reads]
    assert t["live"]["state"] == "done" and t["status"] == "cancelled" and t["shares"] == 0 and bot.live_reserved(t) == 0, see(t)
    assert any("有效期已过" in e["what"] and "按未成交结束" in e["what"] for e in t["live"]["events"]), see(t)

    # (k) the older looks that settle nothing past the expiry — the list cut at the page cap (every page read to learn that), the
    # read failing — are spaced the same (before: each step too); readable and whole again, the next look ends the record
    for case, why in (("cut", "持仓列表没读全"), ("unreadable", "持仓读取失败")):
        bot, fake, pair, oid = await lost(po_order("up", "final", 0.0), po_order("down", "final", 0.0), way="taker")
        readable = fake.positions
        async def cut():
            fake.positions_complete = False
            return held(down=5)
        async def broken(): raise T["m"].RemoteError("HTTP 503")
        fake.positions = cut if case == "cut" else broken
        reads = counted(bot)
        await steps(bot, GONE, GONE + 115_000)
        t = rec(bot, oid)
        assert t["live"]["state"] == "unknown" and bot.live_reserved(t) > 0 and why in t["live"]["last_error"], (case, see(t))
        assert reads == [GONE, GONE + EVERY], (case, [r - GONE for r in reads])
        fake.positions, fake.positions_complete = readable, True
        await step(bot, GONE + 2 * EVERY, mkt(GONE + 2 * EVERY))
        t = rec(bot, oid)
        assert t["live"]["state"] == "done" and t["status"] == "cancelled" and t["shares"] == 0 and bot.live_reserved(t) == 0, (case, see(t))
    print("LIVE_UNEXPLAINED_PREOPEN_OK")


asyncio.run(live_unexplained_preopen())


async def live_latefill_active():
    """LIVE-LATEFILL-ACTIVE: the once-a-minute positions check (live_late_fills (a)) put the account's excess on an outcome onto an
    ENDED order there whose price matched (a maker re-quoted at the same price; an ended taker whose limit is at or over the
    position's average) while the excess was a WORKING order's own fill that its record did not show yet — a resting maker the
    bounded read left for later, a cancel confirmed whose fills were still being read. A price match is not a guess, so it was never
    checked again: once the working order's read landed, the same shares sat on two records (50 held, 100 recorded) for good. The
    orders resting there are now read in the check, and while a record there may still hold fills not read, the outcome waits."""
    L = T["L"]
    slot = f"{HSI_SLUG}|up|挂"
    mkt = lambda at, *bids: hsi(0.70, list(bids or (("0.55", "300"),)), [("0.58", "400")], at)
    rec = lambda bot, oid: next(t for t in bot.sim_trades().values() if t["live"].get("order_id") == oid)
    total = lambda bot: sum(float(t.get("shares") or 0) for t in bot.sim_trades().values() if isinstance(t.get("live"), dict))
    see = lambda bot: [(t["live"].get("order_id"), t["status"], t["live"].get("state"), t.get("shares"), (t["live"].get("adopted") or {}).get("by"))
                       for t in bot.sim_trades().values() if isinstance(t.get("live"), dict)]
    held = lambda n: [{"market": {"id": 101, "title": "恒生指数"}, "outcome": {"name": "Up", "indexSet": 1, "onChainId": "1011"},
                       "amount": str(int(n * WEI)), "avgPrice": "0.55"}]

    def hide(bot):
        """The pre-open mode runs: its complete snapshot stands for the whole account, so the step reads one page of the open list —
        the mode's own orders, none of the model trader's — and Predict does not filter it by market: a resting maker is read by its
        hash in rotation (at most every LIVE_TRACK_READ_MS), its fills unread in between."""
        fake = bot.live.api
        bot.preopen = types.SimpleNamespace(
            alive=True, winding="", error="", manual_pause=False, limited_at=(), account_rows=[], account_at=0,
            ledger=types.SimpleNamespace(rows={}), filled_by_outcome=lambda days=30: {}, unmanaged_rows=lambda rows: list(rows),
            known=lambda: (set(), set()), daily_pnl=lambda: 0.0, risk=lambda: 0.0, status=lambda: "开盘前预挂（测试替身）",
            request_rate=lambda: None, now=lambda: NOW)
        bot.live_market_filter = False
        whole = fake.orders

        async def orders(status="OPEN", stop_ids=None, max_pages=None, **params):
            fake.orders_complete = not max_pages
            return [] if max_pages else await whole(status)
        fake.orders = orders

    async def go(bot, at, *bids):
        if getattr(bot, "preopen", None) is not None:  # the mode's whole read of the account, fresh
            bot.preopen.account_rows, bot.preopen.account_at = [dict(r) for r in bot.live.api.open.values()], at
        await step(bot, at, mkt(at, *bids))

    async def requoted(**env):
        """Maker A at 55¢ withdrawn by the strategy (its cancel confirmed, nothing filled), the slot quoted again at 55¢: maker B."""
        bot = make_bot(SIM_WAYS="maker", **env)
        await bot.live_prepare()
        assert bot.live.ready, bot.live.not_ready_why
        await step(bot, NOW, mkt(NOW))
        a = bot.sim_trades()[slot]
        a_oid = a["live"]["order_id"]
        bot.market.now = NOW + 5_000
        bot.sim_withdraw(a, "测试：买1 变了", NOW + 5_000)
        a["withdrawn"]["rule"] = True
        bot.sim_save([(slot, a)])
        await bot.live_cancel_blind({slot: a}, NOW + 5_000)
        await bot.live_fills_wait()
        await step(bot, NOW + 10_000, mkt(NOW + 10_000))
        b_oid = bot.sim_trades()[slot]["live"]["order_id"]
        assert b_oid != a_oid and rec(bot, a_oid)["live"]["state"] == "done" and rec(bot, a_oid)["shares"] == 0, see(bot)
        assert rec(bot, b_oid)["live"]["state"] == "open" and rec(bot, b_oid)["price"] == rec(bot, a_oid)["price"] == 0.55, see(bot)
        return bot, bot.live.api, a_oid, b_oid

    # (a) B rests unread between its hash reads: it fills 50, the account shows them, and the minute's check falls in a step that
    # leaves B for later. Its read in the check fails: the outcome waits (before: A took the 50 at once, "按 Predict 持仓均价 55.0¢
    # 对上这张单", and B's own read put them on B too — 100 recorded against 50 held, for good)
    bot, fake, a_oid, b_oid = await requoted()
    hide(bot)
    await go(bot, NOW + 20_000)
    assert bot.live_sync_detail["bounded"] and bot.live_sync_detail["reads"] == 1, bot.live_sync_detail  # B's first read by its hash
    fake.fill(b_oid, 50)
    fake.positions_rows = held(50)
    readable = fake.order

    async def failing(oid, hash_=""):
        if str(oid) == b_oid:
            raise T["m"].RemoteError("HTTP 503")
        return await readable(oid, hash_)
    fake.order = failing
    bot.live_late_checked = NOW + 30_000 - 61_000  # the minute's check falls in this step
    await go(bot, NOW + 30_000)
    assert bot.live_sync_detail["later"] == 1 and bot.live_late_checked == NOW + 30_000, bot.live_sync_detail
    assert rec(bot, a_oid)["shares"] == 0 and not rec(bot, a_oid)["live"].get("adopted") and rec(bot, b_oid)["shares"] == 0, see(bot)
    fake.order = readable
    await go(bot, NOW + 36_000)  # B's own read, in rotation
    assert rec(bot, b_oid)["shares"] == 50 and rec(bot, a_oid)["shares"] == 0 and total(bot) == 50, see(bot)
    # ...B fills 10 more and the check again falls in a step that leaves B for later: the check reads B itself, the excess is B's
    fake.fill(b_oid, 60)
    fake.positions_rows = held(60)
    bot.live_late_checked = NOW + 40_000 - 61_000
    await go(bot, NOW + 40_000)
    assert bot.live_sync_detail["later"] == 1, bot.live_sync_detail
    assert rec(bot, b_oid)["shares"] == 60 and rec(bot, a_oid)["shares"] == 0 and total(bot) == 60, see(bot)
    # ...while a fill of A's own that its record never showed (the 1.119.0 case) is still booked onto A at once, B resting there
    # (read: it holds nothing back — a re-quote on the side must not keep a genuine late fill unbooked)
    fake.positions_rows = held(70)
    bot.live_late_checked = NOW + 45_000 - 61_000
    await go(bot, NOW + 45_000)
    a = rec(bot, a_oid)
    assert abs(a["shares"] - 10) < 1e-9 and a["live"]["adopted"]["by"].startswith("按 Predict 持仓均价 55.0¢") and not a["live"]["adopted"]["guess"], see(bot)
    assert rec(bot, b_oid)["shares"] == 60 and abs(total(bot) - 70) < 1e-9, see(bot)

    # (b) all inside one step: the strategy withdraws B (the others left 55¢), its cancel is confirmed in the sync and the 50 shares
    # that matched as it landed are read beside the step (fills_pending) — still on their way when the minute's check runs
    bot, fake, a_oid, b_oid = await requoted(SIM_MAKER_MIN_BID="100", SIM_MAKER_EXIT_CENTS="5", SIM_MAKER_SESSION="on")
    gate, readable, b_hash = asyncio.Event(), fake.order, str(rec(bot, b_oid)["live"]["hash"]).lower()

    async def slow(oid, hash_=""):
        if str(oid) == b_oid or str(hash_ or "").lower() == b_hash:
            await gate.wait()
        return await readable(oid, hash_)
    fake.order = slow
    fake.fill_on_cancel[b_oid] = L.to_wei(50)
    fake.positions_rows = held(50)
    bot.live_late_checked = -1e9
    await step(bot, NOW + 40_000, mkt(NOW + 40_000, ("0.55", "100"), ("0.53", "200")))
    b = rec(bot, b_oid)
    assert b["live"]["state"] == "done" and T["m"].fills_pending(b["live"]) and bot.live_late_checked == NOW + 40_000, (b["live"], see(bot))
    assert rec(bot, a_oid)["shares"] == 0 and not rec(bot, a_oid)["live"].get("adopted"), see(bot)  # before: A took the 50 here
    gate.set()
    await bot.live_fills_wait()
    assert rec(bot, b_oid)["shares"] == 50 and rec(bot, a_oid)["shares"] == 0 and total(bot) == 50, see(bot)
    await step(bot, NOW + 105_000, mkt(NOW + 105_000))  # the next minute's check: explained, nothing adopted
    await bot.live_fills_wait()
    assert rec(bot, a_oid)["shares"] == 0 and rec(bot, b_oid)["shares"] == 50 and total(bot) == 50, see(bot)

    # (c) an ended TAKER matches any position average at or under its limit: T (limit 58¢), its rest withdrawn unfilled after the
    # taker wait, took the resting maker M's 55¢ fill (costed as a taker fill, with a fee never paid) — then M's read booked it again
    bot = make_bot(SIM_WAYS="both", LIVE_TAKER_WAIT_SECONDS="5")
    await bot.live_prepare(); fake = bot.live.api
    hide(bot)
    await go(bot, NOW)
    trades = bot.sim_trades()
    m_tid = slot
    m_oid, t_oid = trades[m_tid]["live"]["order_id"], trades[f"{HSI_SLUG}|up|吃"]["live"]["order_id"]
    at = NOW
    for i in range(1, 8):
        at = NOW + i * 5_000
        await go(bot, at)
    taker = rec(bot, t_oid)
    assert taker["live"]["state"] == "done" and taker["shares"] == 0 and float(taker["live"]["want"]["limit"]) >= 0.55 + 0.0051, (taker["live"].get("want"), see(bot))
    for _ in range(10):  # until M was just read by its hash: its next read is LIVE_TRACK_READ_MS away
        if bot.live_track_reads.get(m_tid) == at:
            break
        at += 5_000
        await go(bot, at)
    assert bot.live_track_reads.get(m_tid) == at, bot.live_track_reads
    fake.fill(m_oid, 50)
    fake.positions_rows = held(50)
    at += 5_000
    bot.live_late_checked = at - 61_000
    await go(bot, at)
    assert bot.live_sync_detail["later"] == 1 and bot.live_late_checked == at, bot.live_sync_detail
    takers = [t for t in bot.sim_trades().values() if isinstance(t.get("live"), dict) and not t.get("maker") and float(t.get("shares") or 0) > 0]
    assert not takers and rec(bot, m_oid)["shares"] == 50, see(bot)
    for _ in range(4):
        at += 5_000
        await go(bot, at)
    assert rec(bot, m_oid)["shares"] == 50 and total(bot) == 50, see(bot)
    print("LIVE_LATEFILL_ACTIVE_OK")


asyncio.run(live_latefill_active())


async def live_taker_fee_double():
    """LIVE-TAKER-FEE-DOUBLE: a taker's record is costed at Predict's average price plus the fee per share (live_apply), but
    Predict takes that fee out of the shares bought, not in USDT (the signed order gives price × shares at most). The minute's
    positions check trims the record to the shares held (份数校正) — and since 1.122.0 kept price × shares whole over the fewer
    shares, so the fee was counted twice: the shares gone and their worth still in the cost. Every trimmed taker's P&L, a win's
    and a loss's alike, was low by fee × the shares bought ($0.84 on 100 shares at 58¢) in the journal, the exposure and the
    day's loss figure. A trimmed taker now costs the money it spent — its average price × the shares it bought — never above
    the cost it had and never by lowering its per-share price; a maker's cost is kept, as before."""
    L = T["L"]
    slot = f"{HSI_SLUG}|up|吃"
    mkt = lambda at, ask="0.58", fair=0.70, bid="0.55": hsi(fair, [(bid, "300")], [(ask, "400")], at)
    taken = lambda price, shares, bps=200: bps / 10_000 * min(price, 1 - price) * shares / price  # the exchange's BUY fee, out of the shares
    holding = lambda n, **more: [{"market": {"id": 101, "title": "恒生指数"}, "outcome": {"name": "Up", "indexSet": 1, "onChainId": "1011"},
                                  "amount": str(L.to_wei(round(n, 6))), **more}]
    cost = lambda t: float(t["price"]) * float(t["shares"])
    see = lambda bot: [(k, "挂" if t.get("maker") else "吃", t["status"], t.get("shares"), t.get("price"), t.get("avg"), t.get("fee_shares"))
                       for k, t in bot.sim_trades().items() if isinstance(t.get("live"), dict)]
    rec = lambda bot, oid: next(t for t in bot.sim_trades().values() if isinstance(t.get("live"), dict) and t["live"].get("order_id") == oid)
    slot_order = lambda bot: str(((bot.sim_trades().get(slot) or {}).get("live") or {}).get("order_id") or "")  # the order in the taker's slot now

    async def trim(bot, at):
        """Two of the minute's positions checks: the same shortfall standing a minute later, the records are trimmed."""
        await bot.live_late_fills(at)
        await bot.live_late_fills(at + 65_000)

    def noted(bot):
        """The 份数校正 notices from here on (the administrator's Telegram line), as they would be sent."""
        notes = []
        bot.live_notify = lambda text, key="": notes.append(text) if text.startswith("📐 份数校正") else None
        return notes

    async def taker(price, fair=0.70, bid="0.55", average=True, **env):
        """A taker of 100 shares, filled whole at ``price`` and read back: (bot, its record)."""
        bot = make_bot(SIM_WAYS="taker", **env)
        bot.notes = noted(bot)
        await bot.live_prepare(); fake = bot.live.api
        await bot.live_prefetch(NOW)
        await step(bot, NOW, mkt(NOW, str(price), fair, bid))
        signed = [b for k, b in fake.calls if k == "create"][-1]["data"]["order"]
        assert int(signed["makerAmount"]) == round(price * 100) * WEI and int(signed["takerAmount"]) == 100 * WEI and signed["side"] == 0, signed
        fake.fill(bot.sim_trades()[slot]["live"]["order_id"], 100, done=True, price=price if average else None)
        await step(bot, NOW + 10_000, mkt(NOW + 10_000, str(price), fair, bid))
        t = bot.sim_trades()[slot]
        fee = 0.02 * min(price, 1 - price)
        assert not t.get("maker") and t["shares"] == 100 and t["live"]["state"] == "done" and t["status"] == "filled", see(bot)
        assert abs(t["avg"] - price) < 1e-9 and abs(t["fee"] - fee) < 1e-9 and abs(t["price"] - price - fee) < 1e-9, see(bot)  # as before
        return bot, t

    def day(bot, t, up, at):
        """The day's settled P&L (the loss stop's figure) with this record settled on ``up`` — a copy: the record stays open."""
        done = {**t, "live": dict(t["live"]), "revisions": []}
        bot.sim_settle(done, up, "测试", at, "Predict 最终结果")
        return bot.live_daily_pnl({"x": done}, at)

    # (a) the finding: 100 shares at 58¢ — $58.00 leaves the account (the order signed for no more), 98.551724 shares arrive. Trimmed
    # to the holding, the record costs the $58.00 spent (before: $58.84 — the fee as the shares gone and again as money): a loss
    # books −$58.00 and a win the shares held less $58.00, the cash that really moved (before: both $0.84 lower)
    bot, t = await taker(0.58)
    held = round(100 - taken(0.58, 100), 6)
    assert abs(held - 98.551724) < 1e-9 and abs(cost(t) - 58.84) < 1e-9 and abs(bot.live_reserved(t) - 58.84) < 1e-9
    bot.live.api.positions_rows = holding(held)
    await bot.live_late_fills(NOW + 75_000)  # seen once: not yet
    assert bot.sim_trades()[slot]["shares"] == 100 and abs(cost(bot.sim_trades()[slot]) - 58.84) < 1e-9, see(bot)
    await bot.live_late_fills(NOW + 140_000)
    t = bot.sim_trades()[slot]
    assert abs(t["shares"] - held) < 1e-6 and abs(t["fee_shares"] - (100 - held)) < 1e-6 and t["status"] == "filled", see(bot)
    assert abs(cost(t) - 58.00) < 1e-3, ("a trimmed taker costs the USDT it spent, not that plus the fee again", cost(t), see(bot))
    assert abs(t["avg"] - 0.58) < 1e-9 and abs(t["fee"] - 0.0084) < 1e-9, see(bot)  # the fill's own average and fee per share stay as read
    assert abs(bot.live_reserved(t) - 58.00) < 1e-3 and abs(bot.live_exposure(bot.sim_trades()) - 58.00) < 1e-3, bot.live_reserved(t)
    assert abs(day(bot, t, 0.0, NOW + 600_000) + 58.00) < 1e-3, ("a loss is the money spent", day(bot, t, 0.0, NOW + 600_000))
    assert abs(day(bot, t, 1.0, NOW + 600_000) - (held - 58.00)) < 1e-3, ("a win is the shares held less the money spent", day(bot, t, 1.0, NOW + 600_000))
    what = [e["what"] for e in t["live"]["events"] if e["what"].startswith("按 Predict 持仓校正份数：100 → 98.5517")]
    assert len(what) == 1 and "成本 $58.84 → $58.00" in what[0] and "不再另算进成本" in what[0], t["live"]["events"]
    assert len(bot.notes) == 1 and bot.notes[0].endswith("1 笔记录按比例改为持有的份数（吃单的手续费就是少掉的这些份额，不再另算进成本：成本共减 $0.84）"), bot.notes
    await trim(bot, NOW + 210_000)  # explained now: nothing more moves
    assert abs(bot.sim_trades()[slot]["shares"] - held) < 1e-6 and abs(cost(bot.sim_trades()[slot]) - 58.00) < 1e-3, see(bot)
    # ...and the day's loss stop still trips on the money really lost: $58.00 against a $57.90 line
    stop = make_bot(SIM_WAYS="taker", LIVE_MAX_DAILY_LOSS_USD="57.9")
    lost = {**t, "live": dict(t["live"]), "revisions": []}
    stop.sim_settle(lost, 0.0, "测试", NOW + 600_000, "Predict 最终结果")
    stop.market.now = NOW + 600_000
    stop.live_check_kill({"x": lost}, NOW + 600_000)
    assert stop.live_killed().startswith("今日已结算亏损 −$58.00"), stop.live_killed()

    # (b) the cheap side and the dear side: the fee is the same rate × min(p, 1 − p) of the shares — 2 of 100 at 25¢, half a share at 80¢
    for price, fair, bid in ((0.25, 0.45, "0.20"), (0.80, 0.97, "0.30")):
        bot, t = await taker(price, fair, bid)
        held = round(100 - taken(price, 100), 6)
        bot.live.api.positions_rows = holding(held)
        await trim(bot, NOW + 75_000)
        t = bot.sim_trades()[slot]
        assert abs(t["shares"] - held) < 1e-6 and abs(cost(t) - price * 100) < 1e-3, (price, cost(t), see(bot))
        assert abs(day(bot, t, 0.0, NOW + 600_000) + price * 100) < 1e-3 and abs(day(bot, t, 1.0, NOW + 600_000) - (held - price * 100)) < 1e-3, price

    # (c) Predict's record states no average price: the fill is costed at the order's limit (待对账) — trimmed, at the limit × the
    # shares bought, the most the signed order could take ($58.00), not that plus the fee
    bot, t = await taker(0.58, average=False)
    assert t["cost_confirmed"] is False and abs(cost(t) - 58.84) < 1e-9, see(bot)
    bot.live.api.positions_rows = holding(98.551724)
    await trim(bot, NOW + 75_000)
    assert abs(bot.sim_trades()[slot]["shares"] - 98.551724) < 1e-6 and abs(cost(bot.sim_trades()[slot]) - 58.00) < 1e-3, see(bot)

    # (d) the holding first shows a part of the fee gone (the list following late), the rest a few minutes on: cut by less than
    # its fee the record is never cheaper than the money spent, nor dearer than its price × the shares left — and it ends at the
    # money spent (the shares bought are those held and every share cut, not the last trim's alone)
    bot, t = await taker(0.58)
    bot.live.api.positions_rows = holding(98.551724 + 0.9)
    await trim(bot, NOW + 75_000)
    t = bot.sim_trades()[slot]
    assert abs(t["shares"] - 99.451724) < 1e-6 and 58.00 - 1e-3 <= cost(t) <= 0.5884 * 99.451724 + 1e-3, (cost(t), see(bot))
    bot.live.api.positions_rows = holding(98.551724)
    await trim(bot, NOW + 215_000)
    t = bot.sim_trades()[slot]
    assert abs(t["shares"] - 98.551724) < 1e-6 and abs(t["fee_shares"] - 1.448276) < 1e-6 and abs(cost(t) - 58.00) < 1e-3, (cost(t), see(bot))

    # (e) a maker's record is as before (1.122.0): no fee in its price — the money spent is kept, over the shares held
    bot = make_bot(SIM_WAYS="maker")
    notes = noted(bot)
    await bot.live_prepare(); fake = bot.live.api
    await bot.live_prefetch(NOW)
    await step(bot, NOW, mkt(NOW))
    mslot = f"{HSI_SLUG}|up|挂"
    fake.fill(bot.sim_trades()[mslot]["live"]["order_id"], 100, done=True)
    await step(bot, NOW + 10_000, mkt(NOW + 10_000))
    t = bot.sim_trades()[mslot]
    assert t["maker"] and t["shares"] == 100 and t["price"] == 0.55 and t["live"]["state"] == "done", see(bot)
    fake.positions_rows = holding(98)
    await trim(bot, NOW + 75_000)
    t = bot.sim_trades()[mslot]
    assert abs(t["shares"] - 98) < 1e-9 and abs(cost(t) - 55.00) < 1e-3 and abs(t["price"] - 0.561224) < 1e-6, see(bot)
    assert not any("成本 $" in e["what"] for e in t["live"]["events"]), t["live"]["events"]
    assert len(notes) == 1 and notes[0].endswith("1 笔记录按比例改为持有的份数（成本不变）"), notes

    # (f) a maker and a taker on the outcome (SIM_WAYS=both): the taker's 1.448276 shares of fee are spread over both records in
    # proportion. The maker's cost is kept ($55.00); the taker, cut by half its fee only, costs no less than the $58.00 it spent
    # and no more than its price × the shares left (before: $58.84 — the two records $0.84 over what was spent)
    bot = make_bot(SIM_WAYS="both")
    notes = noted(bot)
    await bot.live_prepare(); fake = bot.live.api
    await bot.live_prefetch(NOW)
    await step(bot, NOW, mkt(NOW))
    trades = bot.sim_trades()
    assert trades[mslot]["live"]["order_id"] and trades[slot]["live"]["order_id"], see(bot)
    fake.fill(trades[mslot]["live"]["order_id"], 100, done=True)
    fake.fill(trades[slot]["live"]["order_id"], 100, done=True, price=0.58)
    await step(bot, NOW + 10_000, mkt(NOW + 10_000))
    fake.positions_rows = holding(200 - taken(0.58, 100))
    await trim(bot, NOW + 75_000)
    mk_, tk_ = bot.sim_trades()[mslot], bot.sim_trades()[slot]
    assert abs(mk_["shares"] + tk_["shares"] - 198.551724) < 1e-5 and abs(mk_["shares"] - tk_["shares"]) < 1e-6, see(bot)
    assert abs(cost(mk_) - 55.00) < 1e-3, ("a maker's cost is kept", cost(mk_), see(bot))
    assert 58.00 - 1e-3 <= cost(tk_) <= 0.5884 * tk_["shares"] + 1e-3, ("never cheaper than spent, nor dearer than its price", cost(tk_), see(bot))
    assert len(notes) == 1 and "2 笔记录按比例改为持有的份数（吃单的手续费就是少掉的这些份额，不再另算进成本：成本共减 $" in notes[0] and notes[0].endswith("；挂单的成本不变）"), notes

    # (g) the default flow (SIM_TAKER_MAX_SHARES): a second taker on the outcome after the first was trimmed. Its fee's shortfall
    # is spread over both records; the first — already at the money it spent — is cut in shares, not in cost, and the second is
    # never cheaper than the $55.00 it spent: together no less than what left the account (before: $1.74 over it)
    bot, t1 = await taker(0.58, SIM_TAKER_MAX_SHARES="200")
    fake, oid1 = bot.live.api, t1["live"]["order_id"]
    fake.positions_rows = holding(98.551724)
    await trim(bot, NOW + 75_000)
    assert abs(cost(rec(bot, oid1)) - 58.00) < 1e-3, see(bot)
    at = NOW + 150_000
    for _ in range(40):  # the asks come down to 55¢: the side buys again (its holds and waits first)
        await step(bot, at, mkt(at, "0.55", bid="0.50"))
        if slot_order(bot) not in ("", oid1):
            break
        at += 5_000
    oid2 = slot_order(bot)
    assert oid2 not in ("", oid1) and float(rec(bot, oid2)["order"]) == 100 and not rec(bot, oid2).get("maker"), see(bot)
    fake.fill(oid2, 100, done=True, price=0.55)
    await step(bot, at + 5_000, mkt(at + 5_000, "0.55", bid="0.50"))
    assert rec(bot, oid2)["shares"] == 100 and abs(rec(bot, oid2)["price"] - 0.559) < 1e-9 and abs(rec(bot, oid1)["shares"] - 98.551724) < 1e-6, see(bot)
    fake.positions_rows = holding(98.551724 + 100 - taken(0.55, 100))
    await trim(bot, at + 70_000)
    first, second = rec(bot, oid1), rec(bot, oid2)
    assert abs(first["shares"] + second["shares"] - (198.551724 - taken(0.55, 100))) < 1e-5 and first["shares"] < 98.551724 - 0.5, see(bot)
    assert abs(cost(first) - 58.00) < 1e-3, ("the record already at the money it spent is not cut cheaper", cost(first), see(bot))
    assert 55.00 - 1e-3 <= cost(second) <= 0.559 * second["shares"] + 1e-3, (cost(second), see(bot))

    # (h) a taker's fill its own record never showed, booked from the holding (撤单后的补记): the shares are those the account
    # holds — already net of the fee — at the position's average plus the fee, the record's own "average" still its limit.
    # Trimmed with a second taker's shortfall, such a record is neither costed cheaper per share (the shares it bought are not
    # known: the rule would take a fee off shares that carry none — position at 58¢) nor dearer in all than it was (its limit
    # is far over what the position says it paid — position at 40¢)
    for average in (0.58, 0.40):
        bot = make_bot(SIM_WAYS="taker", LIVE_TAKER_WAIT_SECONDS="5", SIM_TAKER_MAX_SHARES="200")
        await bot.live_prepare(); fake = bot.live.api
        await bot.live_prefetch(NOW)
        await step(bot, NOW, mkt(NOW))
        oid1, at = bot.sim_trades()[slot]["live"]["order_id"], NOW
        for _ in range(8):  # the taker wait over: the order is withdrawn, nothing filled as far as its record says
            at += 5_000
            await step(bot, at, mkt(at))
            await bot.live_fills_wait()
            if rec(bot, oid1)["live"]["state"] == "done":
                break
        assert rec(bot, oid1)["status"] == "cancelled" and rec(bot, oid1)["shares"] == 0, see(bot)
        fake.positions_rows = holding(98.551724, avgPrice=str(average))  # ...but the account holds its 100 shares less the fee
        await bot.live_late_fills(at + 70_000)
        t1 = rec(bot, oid1)
        fee = 0.02 * min(average, 1 - average)
        assert t1["status"] == "filled" and abs(t1["shares"] - 98.551724) < 1e-6 and t1["live"]["adopted"]["by"].startswith("按 Predict 持仓均价"), see(bot)
        assert abs(t1["price"] - average - fee) < 1e-9 and abs(t1["avg"] - 0.58) < 1e-9 and not t1.get("fee_shares"), see(bot)
        price1, cost1 = float(t1["price"]), cost(t1)
        bot.live_backoff.clear()  # (the short wait after a taker that filled nothing runs on the real clock)
        at += 75_000
        for _ in range(40):
            await step(bot, at, mkt(at, "0.55", bid="0.50"))
            if slot_order(bot) not in ("", oid1):
                break
            at += 5_000
        oid2 = slot_order(bot)
        assert oid2 not in ("", oid1) and float(rec(bot, oid2)["order"]) == 100, see(bot)
        fake.fill(oid2, 100, done=True, price=0.55)
        await step(bot, at + 5_000, mkt(at + 5_000, "0.55", bid="0.50"))
        assert rec(bot, oid2)["shares"] == 100 and abs(rec(bot, oid1)["shares"] - 98.551724) < 1e-6 and abs(rec(bot, oid1)["price"] - price1) < 1e-9, see(bot)
        fake.positions_rows = holding(98.551724 + 100 - taken(0.55, 100))
        await trim(bot, at + 70_000)
        t1, t2 = rec(bot, oid1), rec(bot, oid2)
        assert abs(t1["shares"] + t2["shares"] - (198.551724 - taken(0.55, 100))) < 1e-5 and t1["shares"] < 98.551724 - 0.5, see(bot)
        assert t1["price"] >= price1 - 1e-9, ("a record booked from the holding is not costed cheaper per share", average, t1["price"], price1)
        assert cost(t1) <= cost1 + 1e-3, ("...nor dearer in all than it was", average, cost(t1), cost1)
        assert 55.00 - 1e-3 <= cost(t2) <= 0.559 * t2["shares"] + 1e-3, (average, cost(t2), see(bot))
    print("LIVE_TAKER_FEE_DOUBLE_OK")


asyncio.run(live_taker_fee_double())


async def live_fill_after_local_settle():
    """LIVE-FILL-AFTER-LOCAL-SETTLE: a real order's fill read only after its market's result. The step decides before it reads
    the orders back, so a result the bot's own data gives in that very step (a first-touch line crossed; a flip, a range or cap
    level alike) finds a just-filled order's record at 0 shares and lapses it ("expired": 市场已出结果，挂单一直没成交); the cancel
    that follows — or the positions check minutes later — then reads the fill, and nothing settled the record: real shares shown
    as "未成交" with no payout, out of the day's settled loss (LIVE_MAX_DAILY_LOSS_USD did not see the money) until Predict's own
    result hours later, which then kept the lapse's time: after Beijing midnight the loss was on no day's stop. And a position
    pre-settled on the bot's data before midnight that Predict resolved the other way after it was booked to the day already
    over. Now the record is settled on the result it already has the moment the shares are read, stamped then; and what a
    correction made today took from a record settled on an earlier day counts today. Real paths only (no direct sim_settle)."""
    world, market_json, BJ, book, clock = T["world"], T["market_json"], T["BJ"], T["book"], T["time"]
    spec = next((s for s in m.TOUCH_MARKETS if s.key == "BNB-2027"), None) or next(s for s in m.TOUCH_MARKETS if not s.stock)
    slug, mid, start = spec.slug, "201", BJ(11, 1, 0)
    low, high = f"${int(spec.low)}", f"${int(spec.high)}"  # the outcomes' names: 涨 / up is the high line's side
    quiet = ([("0.55", "300")], [("0.58", "400")])  # before the touch: the high side's fair price 70¢, its 买1 55¢, its ask 58¢
    lost = ([("0.01", "500")], [("0.02", "500")])   # the low line touched first: the high side is worth nothing
    won = ([("0.97", "500")], [("0.99", "500")])    # the high line touched first
    late_event, late_note = "市场出结果后才读到成交", "市场出结果后才读到这笔真实成交"

    def touch(fair, bids, asks, at):
        return m.SimMarket(slug, "先触", "touch", spec.key, fair, book(bids, asks, at, slug, spec.key, None, mid), 0.03, "", (high, low),
                           {"deadline": spec.deadline_ms}, {})

    def predict_says(bot, name):
        """Predict's own word on the market from now on, asked at the next step: resolved on ``name``, or (None) still open."""
        async def details(market_id):
            resolved = {"index": 0 if name == high else 1, "name": name, "split": False, "how": "outcomes.status"} if name else None
            return {"status": "RESOLVED" if name else "REGISTERED", "resolved": resolved, "outcomes": []}
        bot.predict.market_details = details
        bot.sim_checked.clear()

    async def setup(at, ways, **env):
        bot = make_bot(SIM_WAYS=ways, LIVE_MAX_DAILY_LOSS_USD="50", **env)
        bot.market.now = at
        bot.live.api.markets[mid] = market_json(mid, names=(high, low))
        bot.touches[spec.key].start_ms = start
        predict_says(bot, None)
        await bot.live_prepare()
        world["markets"] = [touch(0.70, *quiet, at)]
        await bot.live_prefetch(at)
        return bot, bot.live.api

    async def look(bot, at, *markets):
        """One step and the fills reads it started; with no market given, no card prices the market in this look."""
        world["markets"] = list(markets)
        await step(bot, at)
        await bot.live_fills_wait()

    def touched(bot, kind, at):
        """The bot's own data decides the market: what TouchMarket.scan writes when a line is reached."""
        hi, lo = (float(spec.high) + 1, float(spec.high) - 40) if kind == "high" else (float(spec.low) + 12, float(spec.low) - 1)
        bot.store.put(f"touch:{slug}", {"kind": kind, "time": at, "hi": hi, "lo": lo, "start": start})

    def holds(api, t, shares, **more):
        want = t["live"]["want"]
        api.positions_rows = [{"id": "p1", "market": {"id": int(mid), "title": "先触"}, "amount": str(int(shares * WEI)), **more,
                               "outcome": {"name": want["outcome"], "indexSet": want["index_set"], "onChainId": want["token"]}}]

    rec = lambda bot, oid: next(t for t in bot.sim_trades().values() if (t.get("live") or {}).get("order_id") == oid)
    kept = lambda t: (t["status"], t["shares"], t.get("payout"), t.get("settled"))
    stored = lambda bot, oid: kept(next(v for _, v in bot.store.items("sim:") if (v.get("live") or {}).get("order_id") == oid))  # as a restart reads it
    day = lambda bot, at: round(bot.live_daily_pnl(bot.sim_trades(), at), 4)
    whats = lambda t: [e["what"] for e in t["live"]["events"]]
    see = lambda t: (t["status"], t["shares"], t.get("payout"), t.get("confirm"), t["live"]["state"], m.stamp(int(t.get("settled") or 0)), t.get("note"), whats(t)[-3:])

    async def express_taker():
        """(a) An express-lane taker fills at once on Predict; the touch is recorded; the NEXT step settles the market on the bot's
        own data before its sync reads the fill (the record lapses at 0 shares), then the cancel's read finds the 100 shares."""
        t0 = BJ(11, 10, 23, 59)
        bot, api = await setup(t0, "taker")
        await bot.sim_express(spec.key)
        t = bot.sim_trades()[f"{slug}|up|吃"]
        oid = t["live"]["order_id"]
        assert t["status"] == "resting" and t["live"]["state"] == "open" and oid, see(t)
        api.fill(oid, 100, done=True, price=0.58)  # matched at once; not read back yet
        touched(bot, "low", t0 - 60_000)
        t2 = t0 + 3_000
        await look(bot, t2, touch(0.0, *lost, t2))
        t = rec(bot, oid)
        assert t["status"] == "settled" and t["payout"] == 0.0 and t["shares"] == 100 and t["confirm"] == "local" and t["live"]["state"] == "done", see(t)
        assert t["settled"] == t2 and m.sim_status(t) == "输 −$58.84" and "先触及" in t["note"] and late_note in t["note"], see(t)
        assert f"{late_event} 100/100 份：按已有的结果（本地预结算）结算，计入今日已结算盈亏" in whats(t) and whats(t).count("撤单：市场已出结果") == 1, whats(t)
        assert day(bot, t2) == -58.84 and m.sim_stats([t])["losses"] == 1 and m.sim_stats([t])["expired"] == 0, (day(bot, t2), see(t))
        assert stored(bot, oid) == kept(t) == ("settled", 100.0, 0.0, t2), ("saved so, not only in the step's view", stored(bot, oid))
        # the loss stop sees it from this moment: nothing more is sent (the gate recomputes the day's loss); the switch trips at the next step
        assert "已达 LIVE_MAX_DAILY_LOSS_USD $50" in bot.live_send_block(t, bot.sim_trades(), t2), bot.live_send_block(t, bot.sim_trades(), t2)
        await look(bot, t2 + 5_000, touch(0.0, *lost, t2 + 5_000))
        assert "今日已结算亏损 −$58.84" in bot.live_killed(), bot.live_kill_record()
        # Predict's result after midnight confirms it: the loss stays on the day it became known (counted and stopped then), once
        predict_says(bot, low)
        t3 = BJ(11, 11, 0, 30)
        await look(bot, t3, touch(0.0, *lost, t3))
        t = rec(bot, oid)
        assert t["status"] == "settled" and t["confirm"] == "confirmed" and t["payout"] == 0.0 and t["settled"] == t2 and not t["revisions"], see(t)
        assert day(bot, t3) == 0 and day(bot, t2 + 30_000) == -58.84, (day(bot, t3), day(bot, t2 + 30_000))

    async def blind_maker(view):
        """(b) A resting bid swept by the dump at the touch. With a fresh book the exit line withdraws it first (the normal path,
        the control); a look without a fresh view of the market — its book stale, crossed, or no card for it — settles the
        market first: the record lapses, then the cancel's read finds the fill."""
        t0 = BJ(11, 10, 14, 0)
        bot, api = await setup(t0, "maker", SIM_MAKER_EXIT_CENTS="5")
        await look(bot, t0, touch(0.70, *quiet, t0))
        t = bot.sim_trades()[f"{slug}|up|挂"]
        oid = t["live"]["order_id"]
        assert t["status"] == "resting" and t["live"]["state"] == "open" and t["price"] == 0.55 and oid, see(t)
        api.fill(oid, 100, done=True, price=0.55)
        touched(bot, "low", t0 + 30_000)
        views = lambda at: {"fresh": [touch(0.0, *lost, at)], "stale": [touch(0.0, *quiet, t0)], "gone": [],
                            "crossed": [touch(0.0, [("0.03", "500")], [("0.02", "500")], at)]}[view]
        t2 = t0 + 100_000
        await look(bot, t2, *views(t2))
        t = rec(bot, oid)
        if view == "fresh":
            assert t["status"] == "filled" and t["shares"] == 100 and t.get("withdrawn") and not any(w.startswith(late_event) for w in whats(t)), see(t)
        else:
            assert t["status"] == "settled" and t["payout"] == 0.0 and t["shares"] == 100 and t["settled"] == t2 and t["live"]["state"] == "done", (view, see(t))
            assert day(bot, t2) == -55 and m.sim_status(t) == "输 −$55.00" and late_note in t["note"], (view, day(bot, t2), see(t))
        await look(bot, t2 + 5_000, *views(t2 + 5_000))
        t = rec(bot, oid)
        assert t["status"] == "settled" and t["payout"] == 0.0 and day(bot, t2 + 5_000) == -55, (view, see(t))
        assert "今日已结算亏损 −$55.00" in bot.live_killed(), (view, bot.live_kill_record())

    async def winner():
        """(c) The bid on the side that wins, filled just before the touch: it stands (a fresh book, the edge is there), the
        market is settled first, the fill read after — a win counted as any pre-settled win is (before: "未成交" until Predict)."""
        t0 = BJ(11, 10, 14, 0)
        bot, api = await setup(t0, "maker", SIM_MAKER_EXIT_CENTS="5")
        await look(bot, t0, touch(0.40, *quiet, t0))  # the low side's fair price 60¢: its 买1 is 1 − 58¢ = 42¢
        t = bot.sim_trades()[f"{slug}|down|挂"]
        oid = t["live"]["order_id"]
        assert t["status"] == "resting" and abs(t["price"] - 0.42) < 1e-9 and oid, see(t)
        api.fill(oid, 100, done=True, price=0.42)
        touched(bot, "low", t0 + 30_000)
        t2 = t0 + 100_000
        await look(bot, t2, touch(0.0, *lost, t2))
        t = rec(bot, oid)
        assert t["status"] == "settled" and t["payout"] == 1.0 and t["shares"] == 100 and t["settled"] == t2 and m.sim_status(t) == "赢 +$58.00", see(t)
        assert day(bot, t2) == 58 and not bot.live_kill_record(), day(bot, t2)

    async def late_backfill(t0, by_record):
        """(d) The cancel's read says 0 filled (the record lapsed for good, its order ended); Predict's record of the order and
        the account's positions show the 100 shares a minute later — the 1.122.0 backfill (live_late_fills: the order's own
        record read again, or the position's price naming the order) puts them on the record, which is settled then. A lapse
        at 23:59 whose fill is read after midnight counts on the new day: its stop trips."""
        bot, api = await setup(t0, "maker", SIM_MAKER_EXIT_CENTS="5")
        await look(bot, t0, touch(0.70, *quiet, t0))
        oid = bot.sim_trades()[f"{slug}|up|挂"]["live"]["order_id"]
        touched(bot, "low", t0 + 30_000)
        t2 = t0 + 60_000
        await look(bot, t2)  # no card for it in this look: lapsed on the bot's own result, the cancel confirmed, nothing filled as far as it shows
        t = rec(bot, oid)
        assert t["status"] == "expired" and t["shares"] == 0 and t["live"]["state"] == "done" and t["settled"] == t2 and day(bot, t2) == 0, see(t)
        if by_record:
            api.closed[oid]["amountFilled"] = str(100 * WEI)
            holds(api, t, 100)
        else:
            holds(api, t, 100, avgPrice="0.55")  # the order's record still shows nothing: the position's average price names the order
        t4 = t2 + 70_000
        await look(bot, t4)
        t = rec(bot, oid)
        assert t["status"] == "settled" and t["shares"] == 100 and t["payout"] == 0.0 and t["settled"] == t4 and t["confirm"] == "local", (by_record, see(t))
        assert any(w.startswith("结束后核对持仓：") for w in whats(t)) and any(w.startswith(late_event) for w in whats(t)), whats(t)
        assert by_record or t["live"]["adopted"]["by"].startswith("按 Predict 持仓均价"), t["live"].get("adopted")
        assert stored(bot, oid) == kept(t) == ("settled", 100.0, 0.0, t4), stored(bot, oid)
        assert day(bot, t4) == -55, day(bot, t4)  # (t0 23:58: t4 is 00:00:10 of the next day — that day's figure)
        await look(bot, t4 + 5_000)
        assert "今日已结算亏损 −$55.00" in bot.live_killed() and bot.live_kill_record()["day"] == m.beijing_day(t4 / 1000), bot.live_kill_record()
        await look(bot, t4 + 70_000)  # explained now: nothing more is adopted, nothing settled twice
        t = rec(bot, oid)
        assert t["shares"] == 100 and t["settled"] == t4 and len(t["fills"]) == 1 and not t["revisions"], see(t)

    async def partial_then_more():
        """(e) Part of the bid matched just before the result (unread), more as the cancel landed: settled on the 30 shares the
        open list shows at the cancel, and the 20 the order's own record shows after the confirmed removal count as well."""
        t0 = BJ(11, 10, 14, 0)
        bot, api = await setup(t0, "maker", SIM_MAKER_EXIT_CENTS="5")
        await look(bot, t0, touch(0.70, *quiet, t0))
        oid = bot.sim_trades()[f"{slug}|up|挂"]["live"]["order_id"]
        api.fill(oid, 30)  # still open on Predict, 30 matched
        api.fill_on_cancel[oid] = T["L"].to_wei(50)
        touched(bot, "low", t0 + 30_000)
        t2 = t0 + 60_000
        await look(bot, t2)
        t = rec(bot, oid)
        assert t["status"] == "settled" and t["payout"] == 0.0 and t["shares"] == 50 and t["settled"] == t2 and t["live"]["state"] == "done", see(t)
        assert [f["shares"] for f in t["fills"]] == [30, 20] and sum(w.startswith(late_event) for w in whats(t)) == 1, (t["fills"], whats(t))
        assert day(bot, t2) == -27.5 and bot.live_reserved(t) == 0 and not t["live"].get("fills_pending"), (day(bot, t2), bot.live_reserved(t))
        assert stored(bot, oid) == kept(t) == ("settled", 50.0, 0.0, t2), stored(bot, oid)

    async def predict_first():
        """(f) The result on the lapsed record is Predict's own (as a backfill undone leaves it: live_undo_preopen_adoptions): the
        late fill is settled on that, not on the bot's earlier figure, and a win is redeemed soon."""
        t0 = BJ(11, 10, 14, 0)
        bot, api = await setup(t0, "maker", SIM_MAKER_EXIT_CENTS="5")
        await look(bot, t0, touch(0.70, *quiet, t0))
        oid = bot.sim_trades()[f"{slug}|up|挂"]["live"]["order_id"]
        touched(bot, "low", t0 + 30_000)
        t2 = t0 + 60_000
        await look(bot, t2)
        tid, t = next((k, x) for k, x in bot.sim_trades().items() if (x.get("live") or {}).get("order_id") == oid)
        assert t["status"] == "expired" and t["shares"] == 0 and t["local"]["up"] == 0.0, see(t)
        t.update(final={"up": 1.0, "name": high, "how": "outcomes.status", "at": t2 + 1_000, "status": "RESOLVED", "outcomes": []}, confirm="mismatch")
        bot.sim_save([(tid, t)])
        api.closed[oid]["amountFilled"] = str(100 * WEI)
        holds(api, t, 100)
        t4 = t2 + 70_000
        bot.live_last_redeem = clock.monotonic()  # a redeem round just ran: the next would be LIVE_REDEEM_SECONDS away
        await look(bot, t4)
        t = rec(bot, oid)
        assert t["status"] == "settled" and t["payout"] == 1.0 and t["shares"] == 100 and t["settled"] == t4 and t["note"].startswith(f"Predict 结算：{high}"), see(t)
        assert f"{late_event} 100/100 份：按已有的结果（Predict 最终结果）结算，计入今日已结算盈亏" in whats(t) and t["confirm"] == "mismatch", whats(t)
        assert day(bot, t4) == 45 and clock.monotonic() - bot.live_last_redeem >= bot.LIVE_REDEEM_SECONDS - bot.LIVE_REDEEM_SOON_SECONDS - 1, day(bot, t4)

    async def stuck_before():
        """(g) A record an earlier version left lapsed with its shares (the fill read after the result, never settled): Predict's
        result, after midnight, settles it — at that time, on that day's stop; no longer at the lapse's time, a day already over."""
        t0 = BJ(11, 10, 23, 58)
        bot, api = await setup(t0, "maker", SIM_MAKER_EXIT_CENTS="5")
        await look(bot, t0, touch(0.70, *quiet, t0))
        oid = bot.sim_trades()[f"{slug}|up|挂"]["live"]["order_id"]
        api.fill(oid, 100, done=True, price=0.55)
        touched(bot, "low", t0 + 30_000)
        t2 = t0 + 60_000
        bot.live_settle_late = lambda trade, now_ms: None  # as before 1.150.0: nothing settled a fill read after the result
        await look(bot, t2)
        del bot.live_settle_late
        tid, t = next((k, x) for k, x in bot.sim_trades().items() if (x.get("live") or {}).get("order_id") == oid)
        assert t["status"] == "expired" and t["shares"] == 100 and t.get("payout") is None and t["settled"] == t2, see(t)
        # a read of its order that shows nothing new settles nothing (no caller would save it: settled in the step's view only,
        # it would be lapsed again after a restart) — Predict's result does
        assert await bot.live_reread_fills(tid, t, t2 + 20_000) == 0 and kept(t) == stored(bot, oid) == ("expired", 100.0, None, t2), (see(t), stored(bot, oid))
        predict_says(bot, low)
        t3 = BJ(11, 11, 0, 30)
        await look(bot, t3)
        t = rec(bot, oid)
        assert t["status"] == "settled" and t["payout"] == 0.0 and t["confirm"] == "confirmed" and t["settled"] == t3, see(t)
        assert day(bot, t3) == -55, day(bot, t3)
        await look(bot, t3 + 5_000)
        assert "今日已结算亏损 −$55.00" in bot.live_killed() and bot.live_kill_record()["day"] == "2026-11-11", bot.live_kill_record()

    async def correction(first, final, when, counted, tripped):
        """(h) A position (its fill read in time) pre-settled on the bot's own data at 23:59; Predict's result at ``when`` differs
        (or agrees). The record keeps its first settlement's time (the journal's); what a correction made on a later day took
        away counts on that day, what it gave back does not."""
        t0 = BJ(11, 10, 23, 59)
        bot, api = await setup(t0, "taker")
        await bot.sim_express(spec.key)
        oid = bot.sim_trades()[f"{slug}|up|吃"]["live"]["order_id"]
        api.fill(oid, 100, done=True, price=0.58)
        await bot.live_sync(t0 + 1_000)  # read back in time: a position
        assert rec(bot, oid)["status"] == "filled" and rec(bot, oid)["shares"] == 100, see(rec(bot, oid))
        touched(bot, first, t0 - 60_000)
        t2 = t0 + 3_000
        up1, up2 = first == "high", final == "high"
        await look(bot, t2, touch(float(up1), *(won if up1 else lost), t2))
        t = rec(bot, oid)
        assert t["status"] == "settled" and t["payout"] == float(up1) and t["settled"] == t2 and day(bot, t2) == (41.16 if up1 else -58.84), see(t)
        predict_says(bot, high if up2 else low)
        await look(bot, when, touch(float(up2), *(won if up2 else lost), when))
        t = rec(bot, oid)
        assert t["status"] == "settled" and t["payout"] == float(up2) and t["confirm"] == ("confirmed" if first == final else "mismatch"), see(t)
        assert t["settled"] == t2 and [r["at"] for r in t["revisions"]] == [when] * (first != final), see(t)  # the journal's time stays; the revision has its own
        assert day(bot, when) == counted, (first, final, m.stamp(when), day(bot, when))
        await look(bot, when + 5_000, touch(float(up2), *(won if up2 else lost), when + 5_000))
        assert bool(bot.live_killed()) == tripped and day(bot, when + 5_000) == counted, (first, final, bot.live_kill_record())

    async def corrected_twice():
        """(i) Two corrections on one later day: the bot's own data turns yesterday's win into a loss at 00:10 (the 100 it took
        counts, the stop trips), Predict says it was a win after all at 00:30: the day's figure is their net, nothing — not the
        100 of a loss that did not happen."""
        t0 = BJ(11, 10, 23, 59)
        bot, api = await setup(t0, "taker")
        await bot.sim_express(spec.key)
        oid = bot.sim_trades()[f"{slug}|up|吃"]["live"]["order_id"]
        api.fill(oid, 100, done=True, price=0.58)
        await bot.live_sync(t0 + 1_000)
        touched(bot, "high", t0 - 60_000)
        t2 = t0 + 3_000
        await look(bot, t2, touch(1.0, *won, t2))
        assert rec(bot, oid)["payout"] == 1.0 and day(bot, t2) == 41.16, see(rec(bot, oid))
        touched(bot, "low", t0 - 120_000)  # the bot's own data corrected
        t3 = BJ(11, 11, 0, 10)
        await look(bot, t3, touch(0.0, *lost, t3))
        t = rec(bot, oid)
        assert t["payout"] == 0.0 and t["settled"] == t2 and [(r["by"], r["at"]) for r in t["revisions"]] == [("本地数据更正", t3)], see(t)
        assert day(bot, t3) == -100 and "今日已结算亏损 −$100.00" in bot.live_killed(), (day(bot, t3), bot.live_kill_record())
        predict_says(bot, high)
        t4 = BJ(11, 11, 0, 30)
        await look(bot, t4, touch(1.0, *won, t4))
        t = rec(bot, oid)
        assert t["payout"] == 1.0 and t["confirm"] == "mismatch" and t["settled"] == t2 and len(t["revisions"]) == 2, see(t)
        assert day(bot, t4) == 0, day(bot, t4)

    next_day, same_day = BJ(11, 11, 0, 30), BJ(11, 10, 23, 59) + 30_000
    cases = [(express_taker,), (blind_maker, "fresh"), (blind_maker, "stale"), (blind_maker, "crossed"), (blind_maker, "gone"), (winner,),
             (late_backfill, BJ(11, 10, 14, 0), True), (late_backfill, BJ(11, 10, 14, 0), False), (late_backfill, BJ(11, 10, 23, 58), True),
             (partial_then_more,), (predict_first,), (stuck_before,),
             # a local win at 23:59, Predict says the other line at 00:30: the +41.16 was 11-10's, the 100 the correction took is 11-11's
             (correction, "high", "low", next_day, -100.0, True),
             # ...a local loss Predict turns into a win the next day gives that day's stop no room (the −58.84 was 11-10's)
             (correction, "low", "high", next_day, 0.0, False),
             # ...Predict agreeing the next day changes nothing
             (correction, "low", "low", next_day, 0.0, False),
             # ...and a correction on the day of the settlement is the record's P&L as it stands, once (as before)
             (correction, "high", "low", same_day, -58.84, True), (corrected_twice,)]
    for case, *args in cases:
        await case(*args)
    print("LIVE_FILL_AFTER_LOCAL_SETTLE_OK")


asyncio.run(live_fill_after_local_settle())

print("AUDIT_LIVE_A2_OK")
