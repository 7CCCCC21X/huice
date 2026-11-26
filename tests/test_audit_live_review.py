"""1.154.33 — an outside review of the real-trading code (live.py): regression tests for the findings fixed there (fakes only,
offline). R02 a confirmed cancel's lagging fills read, R03 the stop and /live cancel with orders already on their way out, CR-09
the rule taker's orders in /live cancel account and the orphan sweep, R05 a taker's price inside the request slot, R06 a maker
that would cross inside it, R08 list and order answers that say nothing, R10 Predict's merge of the two sides, R12 /live test's
lost answer, R16 the orphan sweep's unanswered removals."""
import asyncio, copy, json, pathlib, sys, threading, time
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
WEI = 10 ** 18
MAKER, TAKER = f"{HSI_SLUG}|up|挂", f"{HSI_SLUG}|up|吃"
market = lambda at, fair=0.70, bids=(("0.55", "300"),), asks=(("0.58", "400"),): hsi(fair, list(bids), list(asks), at)


def record_of(bot, oid: str) -> dict:
    return next(v for v in bot.sim_trades().values() if (v.get("live") or {}).get("order_id") == oid)


def sizes(fake, since: int = 0) -> list[float]:
    return [int(b["data"]["order"]["takerAmount"]) / WEI for k, b in fake.calls[since:] if k == "create"]


async def maker_out(**env):
    """A bot with one resting maker order of 100 at 55¢ on Predict (the fake): (bot, fake, its record, its order id)."""
    bot = make_bot(SIM_WAYS="maker", **env)
    await bot.live_prepare()
    fake = bot.live.api
    await bot.live_prefetch(NOW)
    await step(bot, NOW, market(NOW))
    t = bot.sim_trades()[MAKER]
    assert t["live"]["state"] == "open" and float(t["order"]) == 100, t["live"]
    return bot, fake, t, t["live"]["order_id"]


async def r02_lagging_fills_read():
    """R02: a maker's removal confirmed, the order's own record then read for what filled before it (live_cancel_fills_read) — a read
    that still said OPEN with nothing filled (a record behind the matching: 90 of the 100 had filled after the last read) was taken
    as the last word: the reservation freed at once, the record at 0 shares, and the slot quoted again in full past
    LIVE_MAX_OPEN_USD. Now only a final record frees it; one still open is applied and read again later (live_fills_again)."""
    async def cancelled_after_fill(lagging: bool):
        bot, fake, t, oid = await maker_out(LIVE_MAX_OPEN_USD="100", LIVE_MAX_ORDER_USD="100")
        stale = json.loads(json.dumps(fake.open[oid]))  # its record as it stood before the fill: OPEN, nothing filled
        fake.fill(oid, 90)  # 90 of the 100 match after the bot's last read
        if lagging:
            async def behind(oid_, hash_=""):
                return dict(stale)
            fake.order = behind
        bot.sim_withdraw(t, "买1 变了（测试）", NOW + 1_000)
        bot.sim_save([(MAKER, t)])
        await bot.live_cancel_blind({MAKER: t}, NOW + 1_000)  # the express lane's cancel: no list read, the removal confirmed at once
        await bot.live_fills_wait()
        return bot, fake, oid

    # the record current (CANCELLED, 90 filled): booked and freed at once — the re-quote after a cancel as fast as before
    bot, fake, oid = await cancelled_after_fill(False)
    t = record_of(bot, oid)
    assert t["shares"] == 90 and not t["live"].get("fills_pending") and abs(bot.live_reserved(t) - 49.5) < 1e-9, (t["shares"], t["live"])
    n = len(fake.calls)
    await step(bot, NOW + 3_000, market(NOW + 3_000))
    assert sizes(fake, n) == [91.0], sizes(fake, n)  # LIVE_MAX_OPEN_USD $100: $49.50 held, 91 shares at 55¢ fit beside it
    # the record lagging: what it shows is taken (nothing), the 100 stay reserved — never re-quoted past the cap on it
    bot, fake, oid = await cancelled_after_fill(True)
    t = record_of(bot, oid)
    assert t["shares"] == 0 and t["live"].get("fills_pending") and abs(bot.live_reserved(t) - 55.0) < 1e-9, (t["shares"], t["live"])
    assert sum("按哈希读到的订单记录仍是 OPEN" in e["what"] for e in t["live"]["events"]) == 1, t["live"]["events"]
    n = len(fake.calls)
    await step(bot, NOW + 3_000, market(NOW + 3_000))  # its read again due (3 s on): still behind — read again later, said once
    await bot.live_fills_wait()
    assert sizes(fake, n) == [81.0], sizes(fake, n)  # $55 held for it: 81 at 55¢ fit — with the 90 that really filled, $94.05 (91 would be $99.55)
    t = record_of(bot, oid)
    assert t["live"].get("fills_pending") and t["live"]["fills_reads"] == 2, t["live"]
    assert sum("仍是 OPEN" in e["what"] for e in t["live"]["events"]) == 1 and abs(bot.live_reserved(t) - 55.0) < 1e-9, t["live"]["events"]
    reads = len([c for c in fake.calls if c[0] == "remove"])
    await step(bot, NOW + 5_000, market(NOW + 5_000))  # not due yet (10 s after the second read): no third read
    await bot.live_fills_wait()
    assert record_of(bot, oid)["live"]["fills_reads"] == 2 and len([c for c in fake.calls if c[0] == "remove"]) == reads
    # ...nor does the minute's positions check free it with a read of its own (live_reread_fills) while that read lags
    tid = next(k for k, v in bot.sim_trades().items() if v is t)
    assert await bot.live_reread_fills(tid, t, NOW + 5_000) == 0 and t["live"].get("fills_pending"), t["live"]
    del fake.order  # Predict's record catches up: CANCELLED, 90 filled
    await step(bot, NOW + 15_000, market(NOW + 15_000))
    await bot.live_fills_wait()
    t = record_of(bot, oid)
    assert t["shares"] == 90 and not t["live"].get("fills_pending") and not t["live"].get("fills_reread_at"), t["live"]
    assert abs(bot.live_reserved(t) - 49.5) < 1e-9 and any("撤单确认后读到成交 90 份" in e["what"] for e in t["live"]["events"]), t["live"]["events"]
    # a read that fails (or finds nothing) keeps the reservation too, and is read again — never more than once at a time
    bot, fake, t, oid = await maker_out()
    async def down(oid_, hash_=""):
        raise m.RemoteError("HTTP 502: bad gateway")
    fake.order = down
    sleep = asyncio.sleep
    async def quick(seconds, *a, **kw):  # (the read's own retries, 1 s and 2 s apart: not waited for here)
        return await sleep(0)
    asyncio.sleep = quick
    try:
        bot.sim_withdraw(t, "买1 变了（测试）", NOW + 1_000)
        bot.sim_save([(MAKER, t)])
        await bot.live_cancel_blind({MAKER: t}, NOW + 1_000)
        await bot.live_fills_wait()
        t = record_of(bot, oid)
        assert t["live"].get("fills_pending") and t["live"]["fills_reads"] == 1 and t["live"]["fills_reread_at"] == NOW + 3_000, t["live"]
        bot.live_fills_reread(NOW + 3_000)
        bot.live_fills_reread(NOW + 3_000)  # (the first read still on its way: not a second one beside it)
        assert len(bot.live_fill_tasks) == 1, bot.live_fill_tasks
        del fake.order
        await bot.live_fills_wait()
    finally:
        asyncio.sleep = sleep
    t = record_of(bot, oid)
    assert not t["live"].get("fills_pending") and t["shares"] == 0 and bot.live_reserved(t) == 0.0, t["live"]
    print("R02_LAGGING_FILLS_READ_OK")


async def r03_stop_takes_orders_on_their_way_out():
    """R03: the emergency stop (and /live cancel all|account) took only resting records: one already withdrawn whose cancel had failed,
    gone unconfirmed or turned "unknown" was neither cancelled nor named — "没有真实挂单可撤。" with a green toast while Predict could
    still match it. Such records are now part of the command: their removal sent by its own pass, the reply counting them."""
    for case in ("failed", "unconfirmed", "unknown", "still failing"):
        bot, fake, t, oid = await maker_out()
        real_remove, real_order = fake.remove_orders, fake.order

        async def bad(ids, case=case):
            fake.calls.append(("remove-bad", list(ids)))
            if case in ("failed", "still failing"):
                raise m.RemoteError("HTTP 502: bad gateway")
            return {"removed": [], "noop": list(ids)}  # "nothing to remove" — yet the order still rests
        fake.remove_orders = bad
        if case == "unknown":
            async def nothing(oid_, hash_=""):
                return None
            fake.order = nothing  # the removal unconfirmed and the order not found: unknown
        bot.sim_withdraw(t, "买1 变了（测试）", NOW + 1_000)
        bot.sim_save([(MAKER, t)])
        await bot.live_cancel_blind({MAKER: t}, NOW + 1_000)
        assert t["live"]["state"] == ("unknown" if case == "unknown" else "cancelling") and t["status"] != "resting" and oid in fake.open, t["live"]
        if case != "still failing":
            fake.remove_orders = real_remove
        if case == "unknown":
            del fake.order
        n = len(fake.calls)
        answer = await bot.live_control("stop", {})
        text = answer["message"]
        assert "没有真实挂单可撤" not in text and "之前已在撤单" in text, (case, text)
        assert any(c[0].startswith("remove") and c[1] == [oid] for c in fake.calls[n:]), (case, fake.calls[n:])  # sent by the stop itself
        if case == "still failing":
            assert answer["ok"] is False and "❌" in text and "出错" in text and oid in fake.open, (case, answer)
        else:
            assert answer["ok"] is True and "✅ 策略挂单 1 笔" in text and "确认撤掉 1 笔" in text and oid not in fake.open, (case, answer)
            assert t["live"]["state"] == "done", (case, t["live"])
    # unchanged: nothing resting and nothing on its way out
    bot, fake, t, oid = await maker_out()
    assert "✅" in await bot.live_cancel_command(["all"], NOW + 1_000) and oid not in fake.open
    assert "没有真实挂单可撤。" in await bot.live_cancel_command(["all"], NOW + 2_000)
    print("R03_STOP_ON_THEIR_WAY_OUT_OK")


async def cr09_rule_orders_are_own():
    """CR-09: /live cancel account listed the rule taker's real orders as the account's unrecorded ones — sent a second removal
    beside the taker's own (rules_withdraw) and named "账户上无记录的挂单" — and the orphan sweep took a market that only the rule taker
    traded for nobody's (a lost order of its own there never swept)."""
    bot = make_bot(SIM_WAYS="maker")
    await bot.live_prepare()
    fake = bot.live.api
    rule_hash = "0x" + "ab" * 32
    bot.store.put("rule:btc-updown-15m-test", {"slug": "btc-updown-15m-test", "mode": "on", "start_ms": NOW, "market_id": "555",
                                                "name": "BTC 15 分钟", "orders": [{"id": "9001", "hash": rule_hash, "state": "open"}]})
    fake.open["9001"] = {"id": "9001", "status": "OPEN", "amount": "1", "amountFilled": "0", "order": {"hash": rule_hash}, "market": {"id": "555"}}
    fake.open["9002"] = {"id": "9002", "status": "OPEN", "amount": "1", "amountFilled": "0", "order": {"hash": "0x" + "cd" * 32}, "market": {"id": "556"}}
    ids, _ = bot.live_known_markets()
    assert ids.get("555") == "BTC 15 分钟" and "556" not in ids, ids
    text = await bot.live_cancel_command(["account"], NOW)
    removed = [c[1] for c in fake.calls if c[0] == "remove"]
    assert ["9002"] in removed and not any("9001" in r for r in removed), removed
    assert "账户上无记录的挂单 1 张" in text and "9001" in fake.open and "9002" not in fake.open, text
    # (the review of the fix) a rule order its record has ended — or taken as removed — while it still rests is no order of the rule
    # taker's own withdrawal: the account's command takes it, as it did before
    bot.store.put("rule:btc-updown-15m-test", {"slug": "btc-updown-15m-test", "mode": "on", "start_ms": NOW, "market_id": "555",
                                                "name": "BTC 15 分钟", "orders": [{"id": "9001", "hash": rule_hash, "state": "final"}]})
    assert bot.rules_withdrawable() == (set(), set()) and "9001" in bot.rules_known()[0]
    fake.calls.clear()
    text = await bot.live_cancel_command(["account"], NOW)
    removed = [c[1] for c in fake.calls if c[0] == "remove"]
    assert ["9001"] in removed and "9001" not in fake.open, (removed, text)
    print("CR09_RULE_ORDERS_OK")


def slots_held(how: str, posts: list, held: dict):
    """Predict over a fake transport: the client's reads and cancels hold its five slots until ``held`` is set (as in
    test_audit_live-b's live_send_after_pause); an order's POST is logged and answered."""
    def transport(url, payload=None, timeout=15, headers=None):
        path = url.replace(L.API_BASES[56], "")
        if path.startswith("/orders?"):
            held["reads"].wait(5)
            return json.dumps({"success": True, "data": [], "cursor": None}).encode()
        if path == "/orders/remove":
            held["writes"].wait(5)
            return json.dumps({"success": True, "removed": [], "noop": payload["data"]["ids"]}).encode()
        if path == "/orders":
            posts.append(payload["data"]["order"]["hash"])
            return json.dumps({"success": True, "data": {"orderId": "555", "orderHash": payload["data"]["order"]["hash"]}}).encode()
        raise AssertionError(path)
    return transport


async def in_slot(ways: str, decide, later):
    """A decision sent while the client's slots are busy: ``later()`` changes the world while it waits, then the slots free.
    (bot, the POSTs that went out, the notices)."""
    real_get = m._http_get
    held, posts, notes = {"reads": threading.Event(), "writes": threading.Event()}, [], []
    bot = make_bot(SIM_WAYS=ways)
    await bot.live_prepare()
    mk = market(NOW)
    world["markets"] = [mk]
    await bot.live_prefetch(NOW)
    async with bot.sim_lock:
        trades, changed = bot.sim_trades(), []
        decide(bot, mk, trades, changed)
        bot.sim_save(changed)
    api = L.PredictApi(L.API_BASES[56], "k", bot.live.wallet)
    api.jwt, api.jwt_exp = "jwt", int(time.time()) + 3600
    bot.live.api = api
    bot.live_notify = lambda text, key="": notes.append(text)
    m._http_get = slots_held(ways, posts, held)
    try:
        busy = [asyncio.create_task(api.orders("OPEN")) for _ in range(3)] + [asyncio.create_task(api.remove_orders([str(i)])) for i in range(2)]
        await asyncio.sleep(0.05)
        place = asyncio.create_task(bot.live_place_pending(NOW))
        await asyncio.sleep(0.2)
        assert not posts and any(t["live"]["state"] == "placing" for t in bot.sim_trades().values()), [t["live"]["state"] for t in bot.sim_trades().values()]
        later(bot)
        held["reads"].set(); held["writes"].set()
        await asyncio.gather(place, *busy)
    finally:
        m._http_get = real_get
    return bot, posts, notes


async def r05_taker_priced_in_slot():
    """R05: inside the request slot (ORDER_SEND_CHECK) the stops, the caps and a withdrawal were judged again, the price not: a taker
    that waited seconds for the key's pace or a slot went out at a limit set on a fair price that had moved, and filled at once.
    Judged again now when the gate's look is over LIVE_TAKER_RECHECK_MS old: void, no POST; not to be judged (no fresh book), it
    waits pending."""
    take = lambda bot, mk, trades, changed: bot.sim_take(mk, trades, NOW, changed)
    for how in ("moved", "stale", "fresh", "none"):
        def later(bot, how=how):
            bot.market.now = NOW + (500 if how == "fresh" else 3_000)  # the slot came that much after the gate
            if how == "moved":
                world["markets"] = [market(NOW + 3_000, fair=0.60)]  # the fair price fell: the 58¢ limit is over the cap now
            elif how == "stale":
                world["markets"] = [market(NOW - 200_000)]  # no fresh book to judge it on
            elif how == "fresh":
                world["markets"] = [market(NOW + 500, fair=0.60)]  # moved, but within LIVE_TAKER_RECHECK_MS of the gate: sent as judged
        bot, posts, notes = await in_slot("taker", take, later)
        rows = {k: v for k, v in bot.sim_trades().items() if k.startswith(TAKER)}
        if how in ("none", "fresh"):
            assert len(posts) == 1 and any(v["live"].get("order_id") == "555" for v in rows.values()), (how, posts, rows)
            continue
        assert not posts, (how, posts)
        if how == "moved":
            done = [v for v in rows.values() if v["live"]["state"] == "done"]
            assert len(done) == 1 and done[0]["live"]["final"] == "failed" and "公平价变了" in done[0]["note"] and not done[0]["live"]["hash"], rows
            wait = bot.live_backoff[(HSI_SLUG, "up", "吃")][0] - time.monotonic()
            assert 0 < wait <= bot.config.live_taker_retry, wait  # the taker's short wait, as the gate's own re-check
        else:
            t = rows[TAKER]
            assert t["live"]["state"] == "pending" and not t["live"]["hash"] and "等新盘口" in t["live"]["last_error"], t["live"]
            assert "sent_at" not in t["live"] and (HSI_SLUG, "up", "吃") not in bot.live_backoff, t["live"]
    print("R05_TAKER_PRICED_IN_SLOT_OK")


async def r06_maker_crossing_in_slot():
    """R06 (the partial fix): a resting order whose ask came down to its price while it waited for its request slot is not sent —
    it would have taken the book at the taker's fee on a price chosen for a maker. A void decision: no backoff, no alert (the next
    look quotes the new book). Post-only is not used: what moves after the slot is not covered."""
    quote = lambda bot, mk, trades, changed: bot.sim_quote(mk, trades, NOW, changed)
    for how in ("error", "crossing", "through", "stale", "none"):
        def later(bot, how=how):
            bot.market.now = NOW + 1_000
            if how == "error":  # (the review of the fix) a fault in the re-check: sent as decided, never "result unknown" for an order not sent
                def broken(trade, at):
                    raise RuntimeError("boom")
                bot.live_maker_crossing = broken
            if how == "crossing":
                world["markets"] = [market(NOW + 1_000, bids=(("0.53", "300"),), asks=(("0.55", "200"),))]  # the ask came down to 55¢
            elif how == "through":
                world["markets"] = [market(NOW + 1_000, bids=(("0.50", "300"),), asks=(("0.54", "200"),))]  # ...and under it
            elif how == "stale":
                world["markets"] = [market(NOW - 200_000, bids=(("0.50", "300"),), asks=(("0.54", "200"),))]  # nothing fresh to judge by
        bot, posts, notes = await in_slot("maker", quote, later)
        rows = {k: v for k, v in bot.sim_trades().items() if k.startswith(MAKER)}
        if how in ("none", "stale", "error"):
            assert len(posts) == 1 and any(v["live"].get("order_id") == "555" for v in rows.values()), (how, posts, rows)
            assert not any(v["live"]["state"] == "unknown" for v in rows.values()), rows
            continue
        assert not posts, (how, posts)
        done = [v for v in rows.values() if v["live"]["state"] == "done"]
        assert len(done) == 1 and done[0]["live"]["final"] == "failed" and "会直接吃单" in done[0]["note"] and not done[0]["live"]["hash"], rows
        assert bot.live_backoff[(HSI_SLUG, "up", "挂")][0] <= time.monotonic() and not any("会直接吃单" in n for n in notes), (bot.live_backoff, notes)
    for kind in ("floor", "sweep"):  # (the review of the fix) placed at their price wherever the asks stand, by design: never refused here
        assert bot.live_maker_crossing({kind: True, "key": "HSI", "market": "x", "side": "up", "price": 0.99}, NOW) == ""
    print("R06_MAKER_CROSSING_IN_SLOT_OK")


async def r08_answers_that_say_nothing():
    """R08: an empty 2xx body (call reads it as {}) read as an empty list — and a whole one, the pager seeing no cursor; the order
    read took it for "Predict knows nothing of it". Now an error in both. What stays (merged, narrowed): an answer that says
    something without a list (success with no data, data null or {}) is still an empty list, and an empty page offering a cursor,
    or one handing back the cursor it was asked with, still ends the list — Predict's way with either has not been seen here."""
    def api_with(*answers):
        api = L.PredictApi("https://example.invalid/v1", "k", None)
        seq = list(answers)

        async def authed(method, path, payload=None, timeout=0, priority=False):
            return seq.pop(0) if len(seq) > 1 else seq[0]
        api.authed = authed
        return api

    try:
        await api_with({}).paged("/orders", {"status": "OPEN"}); assert False
    except m.RemoteError as error:
        assert "空的" in str(error), error
    try:
        await api_with({}).positions(); assert False
    except m.RemoteError:
        pass
    for said in ({"success": True}, {"success": True, "data": None}, {"data": {}}):
        assert await api_with(said).paged("/orders", {"status": "OPEN"}) == ([], True), said
    assert await api_with({"success": True, "data": [], "cursor": None}).paged("/orders", {}) == ([], True)  # an empty list said as one
    assert await api_with([{"id": "1"}]).paged("/orders", {}) == ([{"id": "1"}], True)  # a bare list
    assert await api_with({"data": {"items": [{"id": "1"}]}}).paged("/orders", {}) == ([{"id": "1"}], True)
    assert await api_with({"success": True, "data": [], "cursor": "c1"}).paged("/orders", {}) == ([], True)  # (as ever: see above)
    two = api_with({"success": True, "data": [{"id": "1"}], "cursor": "c1"}, {"success": True, "data": [{"id": "2"}], "cursor": None})
    assert await two.paged("/orders", {}) == ([{"id": "1"}, {"id": "2"}], True)
    try:
        await api_with({}).order("101", "0xaa"); assert False
    except m.RemoteError as error:
        assert "空的" in str(error), error
    assert await api_with({"success": True, "data": {}}).order("101", "0xaa") is None  # an empty answer is still no order (as ever)
    assert await api_with({"success": True, "data": {"id": "7", "status": "OPEN"}}).order("7", "0xaa") == {"id": "7", "status": "OPEN"}
    print("R08_ANSWERS_THAT_SAY_NOTHING_OK")


async def r10_merge_is_no_fee():
    """R10: Predict merges a market's 涨 and 跌 shares into USDT. The positions check took the gap a merge leaves on one side for the
    fee taken in shares (≤ 5%: trimmed — "手续费以份额扣除"), and the release of an order of unknown state took a fill of it that merged
    with the other side for "filled nothing"."""
    async def two_sides():
        bot, fake, up, oid = await maker_out()
        fake.fill(oid, 100, done=True)
        await step(bot, NOW + 2_000, market(NOW + 2_000))
        up = record_of(bot, oid)
        assert up["shares"] == 100 and up["live"]["state"] == "done", (up["shares"], up["live"]["state"])
        down = copy.deepcopy(up)
        down.update(side="down", label="挂跌", order=3.0, shares=3.0, price=0.40)
        down["live"].update(order_id="999", hash="0x" + "9" * 64)
        down["live"]["want"].update(index_set=2, side="down")
        bot.sim_save([(f"{HSI_SLUG}|down|挂#1", down)])
        return bot, fake, up, down

    held = lambda up, down=0.0: [{"market": {"id": "101"}, "outcome": {"indexSet": 1}, "amount": str(int(up * WEI))}] + (
        [{"market": {"id": "101"}, "outcome": {"indexSet": 2}, "amount": str(int(down * WEI))}] if down else [])
    # 3 跌 bought against 100 涨: merged, the account holds 97 涨 and no 跌 — no fee there, nothing trimmed
    bot, fake, up, down = await two_sides()
    fake.positions_rows = held(97)
    for at in (NOW + 200_000, NOW + 270_000, NOW + 340_000):
        await bot.live_late_fills(at)
    assert up["shares"] == 100 and down["shares"] == 3 and not any("校正份数" in e["what"] for e in up["live"]["events"]), up["live"]["events"]
    # ...a fee beside the merge still is one: 95 涨 held — 3 merged, 2 the fee
    bot, fake, up, down = await two_sides()
    fake.positions_rows = held(95)
    for at in (NOW + 200_000, NOW + 270_000):
        await bot.live_late_fills(at)
    trims = [e["what"] for e in up["live"]["events"] if "校正份数" in e["what"]]
    assert abs(up["shares"] - 98) < 1e-6 and len(trims) == 1 and "另有 3 份已和另一边合并成 USDT" in trims[0] and "少 2 份" in trims[0], (up["shares"], trims)
    # unchanged (1.119.1): one side alone, the gap is the fee
    bot, fake, up, oid = await maker_out()
    fake.fill(oid, 100, done=True)
    await step(bot, NOW + 2_000, market(NOW + 2_000))
    up = record_of(bot, oid)
    fake.positions_rows = held(98)
    for at in (NOW + 200_000, NOW + 270_000):
        await bot.live_late_fills(at)
    assert abs(up["shares"] - 98) < 1e-6, up["shares"]
    # an order of unknown state on the 跌 side whose fill merged with the 涨 shares: as before (the half of R10 that held such an
    # order undecided was withdrawn after the review of the fix — a winning side redeemed, or a fee not trimmed yet, read as a
    # merge and held the order and its money until /live release); the minute's trim no longer takes a merge for a fee (above)
    bot, fake, up, down = await two_sides()
    lost = copy.deepcopy(down)
    lost.update(shares=0.0, status="resting", order=10.0)
    lost["live"].update(state="unknown", unknown_since=NOW, order_id="998", hash="0x" + "8" * 64)
    fake.positions_rows = held(92)
    assert await bot.live_unexplained(lost) == 0.0 and not bot.live_positions_busy, bot.live_positions_busy
    fake.positions_rows = held(97)
    assert await bot.live_unexplained(lost) == 0.0 and not bot.live_positions_busy, bot.live_positions_busy
    # unchanged: an order lost on the side the account holds, nothing on the other side — its fill is the surplus there
    down["shares"] = 0.0  # (no 跌 bought)
    lost_up = copy.deepcopy(up)
    lost_up.update(shares=0.0, status="resting", order=10.0)
    lost_up["live"].update(state="unknown", unknown_since=NOW, order_id="997", hash="0x" + "7" * 64)
    fake.positions_rows = held(104)
    assert await bot.live_unexplained(lost_up) == 4.0 and not bot.live_positions_busy, bot.live_positions_busy
    print("R10_MERGE_IS_NO_FEE_OK")


async def r12_test_order_lost_answer():
    """R12: /live test kept its order's hash in memory alone until the end, and an answer lost after Predict took the order read
    "下单失败" — the order left resting for its ten minutes, a second test beside it when tried again. Now the record is written
    first and the order looked for by its hash: found, it is withdrawn as every test is."""
    bot = make_bot(SIM_WAYS="both")
    await bot.live_prepare()
    fake = bot.live.api
    world["markets"] = [market(NOW)]
    real_create, seen = fake.create_order, {}

    async def taken_then_lost(body):
        seen["stored"] = bot.store.get("live:test")  # what the store holds as the request goes out
        await real_create(body)  # Predict takes it...
        raise m._net_error(TimeoutError("timed out"))  # ...and its answer never comes
    fake.create_order = taken_then_lost
    text = await bot.live_action("test", [], NOW)
    stored = seen["stored"]
    assert stored and stored["hash"] and stored["hash"] == bot.store.get("live:test")["hash"] and "发送中" in stored["text"], stored
    oid = bot.store.get("live:test")["order_id"]
    assert oid and "但按哈希找到了订单 #" + oid in text and "3/4 撤单：已撤" in text and "下单失败" not in text, text
    assert oid not in fake.open and [c for c in fake.calls if c[0] == "create"].__len__() == 1, fake.open
    # not taken (the answer lost on the way out): looked for, not found — said so
    async def lost(body):
        raise m._net_error(TimeoutError("timed out"))
    fake.create_order = lost
    text = await bot.live_action("test", [], NOW)
    assert "1/4 下单结果未知" in text and "按哈希查不到这张单" in text and "2/4" not in text, text
    # unchanged: Predict's refusal is a failure, with the request body
    fake.create_order = real_create
    fake.fail_create = "HTTP 400: order value below minimum"
    text = await bot.live_action("test", [], NOW)
    assert "1/4 下单失败：HTTP 400" in text and "请求体（不含签名）" in text and "2/4" not in text, text
    # 1.154.72 (an outside audit, P1-4): the record — its hash — not written: nothing sent (it used to be sent all the same)
    fake.fail_create = None
    creates, real_put = len([c for c in fake.calls if c[0] == "create"]), bot.store.put

    def broken(key, value, *args, **kwargs):
        if key == "live:test":
            raise OSError("disk full")
        return real_put(key, value, *args, **kwargs)
    bot.store.put = broken
    text = await bot.live_action("test", [], NOW)
    bot.store.put = real_put
    assert text.startswith("❌ 挂单测试没有发出：记录写不进数据库（") and "disk full" in text, text
    assert len([c for c in fake.calls if c[0] == "create"]) == creates, "no request"
    print("R12_TEST_ORDER_LOST_ANSWER_OK")


async def r16_orphan_unanswered():
    """R16: the orphan sweep counted an id Predict's removal answer left out of both lists (unanswered) as "称无需撤": dropped from the
    unrecorded list and timed afresh from the next one (LIVE_ORPHAN_GRACE_MS more) while it may still rest. Kept now, asked again
    at the next sweep, said apart."""
    bot = make_bot()
    await bot.live_prepare()
    fake = bot.live.api
    answers = [{"removed": [], "noop": []}]

    async def remove(ids):
        fake.calls.append(("remove", list(ids)))
        answer = answers[0]
        return {"removed": [i for i in ids if i in answer["removed"]], "noop": [i for i in ids if i in answer["noop"]]}
    fake.remove_orders = remove
    bot.live_known_markets = lambda: ({"101": "恒生指数"}, {})
    row = {"id": "777", "status": "OPEN", "amount": "1", "market": {"id": "101"}, "order": {"tokenId": "1011", "hash": "0xabc"}}
    bot.live_note_unknown([row], NOW - 200_000)
    notes, errors = [], []
    bot.live_notify = lambda text, key="": notes.append(text)
    bot.live_note_error = lambda text: errors.append(text)
    await bot.live_orphan_sweep(NOW)
    assert bot.live_unknown_first == {"777": NOW - 200_000} and [u["id"] for u in bot.live_unknown_open] == ["777"], bot.live_unknown_first
    assert errors and "没有 #777" in errors[-1] and not any("无需撤" in e for e in errors), errors
    answers[0] = {"removed": ["777"], "noop": []}
    await bot.live_orphan_sweep(NOW + bot.LIVE_ORPHAN_EVERY_MS)  # the next sweep: asked again, no new grace
    assert [c for c in fake.calls if c[0] == "remove"] == [("remove", ["777"])] * 2 and not bot.live_unknown_first, fake.calls
    assert notes and "撤掉 1 张" in notes[-1], notes
    print("R16_ORPHAN_UNANSWERED_OK")


async def main():
    await r02_lagging_fills_read()
    await r03_stop_takes_orders_on_their_way_out()
    await cr09_rule_orders_are_own()
    await r05_taker_priced_in_slot()
    await r06_maker_crossing_in_slot()
    await r08_answers_that_say_nothing()
    await r10_merge_is_no_fee()
    await r12_test_order_lost_answer()
    await r16_orphan_unanswered()


asyncio.run(main())
print("AUDIT_LIVE_REVIEW_OK")
