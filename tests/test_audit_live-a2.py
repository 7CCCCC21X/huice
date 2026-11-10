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
    have been merged into USDT with the market's other side, a surplus is not attributed: the record waits, reserved."""
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

print("AUDIT_LIVE_A2_OK")
