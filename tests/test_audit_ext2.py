"""1.154.76 — a second outside white-box audit (of 1.154.74; the user pasted its report): each finding that held, replayed with
its own order of events, and what happens now. One function a finding (or a part of one); ``python tests/test_audit_ext2.py``
runs them all.

P0-1 an order of a record its market had ended (settled / expired: cancelled, nothing marked withdrawn) taken back as open
     (live_recheck, the open list) and passed by /live cancel all.  P0-2 sim_confirm writing a moved record back at the slot.
P0-3 the stop and /live cancel all ended by a pause write that failed, before any order was withdrawn.  P0-4 a pre-open pair
     withdrawn while its own requests were still out.  P0-5 a taker's fee trim taken for an unfilled part (re-read and adopted).
P1-1 the rule taker's withdrawal marking its sends late.  P1-2 the day's loss stop: a rule loss, a file day, a write that failed.
P1-3 "UNKNOWN" a final word.  P1-4 a confirmed cancel ended on a lagging read (a taker's, the third look's re-cancel).
P1-5 the order paths after a wait.  P1-6 the rule journal's outcome.  P1-7 the rule taker's money after a removal.
P1-8 the chain's answers without a result.  P1-9 the redeem's winner by its bit mask, a revert's hour.  P1-10 the expiry at
     the build.  P1-11 the stop's order.  P1-12 the streams' marks.  P1-13 a sweep order on a book a cent wide.  P1-14 a window
     closed on an empty answer.  P1-15 a REST failure read as "not listed".  P2-1 408.  P2-5 the balance's dust.  P2-6 上涨/下跌,
     a settled pair closed.  P2-8 an unknown order's own shares.

Run: python tests/test_audit_ext2.py
"""
import asyncio
import contextlib
import dataclasses
import json
import sqlite3
import sys
import tempfile
import threading
import time
import types
from decimal import Decimal as D
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L
import preopen as P
import rules as R
import test_preopen as TP
import test_rules as TR

# test_trading's fakes (FakeApi, make_bot, step, hsi …) without running its suites: only the definitions before "async def run():"
_src = (ROOT / "tests" / "test_trading.py").read_text(encoding="utf-8").split("\nasync def run():")[0]
T: dict = {"__file__": str(ROOT / "tests" / "test_trading.py")}
exec(compile(_src, "test_trading_head", "exec"), T)
make_bot, step, hsi, NOW, HSI_SLUG, world = T["make_bot"], T["step"], T["hsi"], T["NOW"], T["HSI_SLUG"], T["world"]
card = lambda at: hsi(0.70, [("0.55", "300")], [("0.58", "400")], at)  # fair 70¢: a 55¢ bid worth joining, a 58¢ ask worth taking


async def resting(ways: str = "maker", **env):
    """A real order resting on HSI (market 101), placed by the step: the bot, its slot's key, the record, the order id."""
    bot = make_bot(SIM_WAYS=ways, **env)
    await bot.live_prepare()
    world["markets"] = [card(NOW)]
    await bot.live_prefetch(NOW)
    await step(bot, NOW, card(NOW))
    tid = f"{HSI_SLUG}|up|{'挂' if ways == 'maker' else '吃'}"
    t = bot.sim_trades()[tid]
    assert t["live"]["state"] == "open" and t["live"]["order_id"], t["live"]
    return bot, tid, t, t["live"]["order_id"]


def by_order(bot, oid: str) -> dict:
    return next(t for t in bot.sim_trades().values() if isinstance(t.get("live"), dict) and t["live"].get("order_id") == oid)


# --- P0-1: a record its market ended, its order found open ------------------------------------------------------------------------
async def p0_1_settled_found_open() -> None:
    bot, tid, t, oid = await resting()
    st = t["live"]
    t["status"] = "settled"  # the result came (sim_settle asks the order cancelled; nothing is marked withdrawn)
    st.update(state="unknown", unknown_since=NOW)
    await bot.live_recheck(tid, t, {oid: dict(bot.live.api.open[oid])}, {}, None, NOW + 1_000)
    assert st["state"] == "cancelling", st["state"]  # was "open": a bid tracked on a market already decided
    # /live cancel all reaches a settled record whose order is open too (it was passed by: neither resting nor being withdrawn)
    bot, tid, t, oid = await resting()
    t["status"] = "settled"
    said = await bot.live_cancel_command(["all"], NOW + 2_000)
    await bot.live_fills_wait()
    assert oid not in bot.live.api.open and t["live"]["state"] == "done", (said, sorted(bot.live.api.open), t["live"]["state"])


# --- P1-3: "UNKNOWN" is no final word -------------------------------------------------------------------------------------------------
def p1_3_unknown_not_final() -> None:
    assert "UNKNOWN" not in L.FINAL_STATUSES
    assert not L.order_final({"status": "UNKNOWN", "amount": str(L.to_wei(100)), "amountFilled": "0"}, 100.0)
    assert L.order_final({"status": "CANCELLED", "amount": str(L.to_wei(100)), "amountFilled": "0"}, 100.0)


# --- P1-4: a confirmed cancel whose order's own record lags ----------------------------------------------------------------------------
async def p1_4_taker_cancel_lagging_read() -> None:
    bot, tid, t, oid = await resting("taker")
    api, st = bot.live.api, t["live"]
    st.update(state="cancelling", cancel_sent=0, cancel_why="测试撤单")
    api.fill_on_cancel[oid] = L.to_wei(60)  # 60 matched as the removal landed: its record says so once it catches up
    plain, lag = api.order, [True]

    async def lagging(o, h=""):
        row = await plain(o, h)
        if lag[0] and row is not None:
            lag[0] = False
            return {**row, "status": "OPEN", "amountFilled": "0"}  # the first read after the removal: not caught up yet
        return row
    api.order = lagging
    await bot.live_cancel_order(tid, t, None, None, NOW + 30_000)
    assert st["state"] == "done" and st.get("fills_pending"), st
    assert bot.live_reserved(t) > 50, bot.live_reserved(t)  # the unfilled part still held: it was freed at once
    await bot.live_fills_wait()
    t = by_order(bot, oid)
    assert t["shares"] == 60 and t["status"] == "filled" and not t["live"].get("fills_pending"), (t["shares"], t["status"], t["live"].get("fills_pending"))


async def p1_4_third_look_recancel() -> None:
    bot, tid, t, oid = await resting()
    api, st = bot.live.api, t["live"]
    api.fill_on_cancel[oid] = L.to_wei(40)
    real = api.order

    async def lost(o, h=""):
        return None  # Predict's reads have lost the order (it still rests)
    api.order = lost
    bot.live_misses[tid] = bot.LIVE_MISSES - 1
    bot.live_counts_miss = lambda st, since: True
    await bot.live_track(tid, t, {}, None, NOW + 120_000)
    assert st["state"] == "done" and st.get("fills_pending") and st.get("cancel_done_at"), st
    api.order = real
    await bot.live_fills_wait()
    t = by_order(bot, oid)
    assert t["shares"] == 40 and t["status"] == "filled", (t["shares"], t["status"])  # was: ended with none of the 40 booked


# --- P1-5: the order paths after a wait ------------------------------------------------------------------------------------------
async def p1_5_track_withdrawn_meanwhile() -> None:
    bot, tid, t, oid = await resting()
    api, st = bot.live.api, t["live"]
    bot.live_misses[tid] = bot.LIVE_MISSES - 1
    bot.live_counts_miss = lambda st, since: True

    async def withdrawn_meanwhile(o, h=""):
        st["state"] = "cancelling"  # the express lane withdrew it while this read was on its way
        return None

    async def noop(ids):
        return {"removed": [], "noop": list(ids)}
    api.order, api.remove_orders = withdrawn_meanwhile, noop
    await bot.live_track(tid, t, {}, None, NOW + 120_000)
    assert st["state"] == "cancelling" and bot.live_misses.get(tid) == bot.LIVE_MISSES - 1, (st["state"], bot.live_misses)  # was "unknown"


async def p1_5_recheck_answered_meanwhile() -> None:
    bot, tid, t, oid = await resting()
    st = t["live"]
    st.update(state="unknown", unknown_since=NOW, unknown_misses=bot.LIVE_UNKNOWN_LOOKS)

    async def answered_meanwhile(o, h=""):
        st["state"] = "open"  # the placement's answer came while this read was on its way: open on Predict's word
        return None
    bot.live.api.order = answered_meanwhile
    await bot.live_recheck(tid, t, {}, {}, None, NOW + bot.LIVE_UNKNOWN_SETTLE_MS + 1_000)
    assert st["state"] == "open", st["state"]  # was: ended as "nothing filled" on the positions — an order just found resting


async def p1_5_misses_move_with_the_record() -> None:
    bot, tid, t, oid = await resting()
    bot.live_misses[tid] = 2
    new = bot.sim_rekey(tid, t)
    assert new != tid and bot.live_misses.get(new) == 2 and tid not in bot.live_misses, (new, bot.live_misses)


async def p1_5_answer_keeps_a_removal_out() -> None:
    """The placement's answer after a cancel found the order by its hash and sent its removal: that removal stays the one."""
    bot, tid, t, oid = await resting()
    st = t["live"]
    # the state live_place_order's answer meets: withdrawn, the order found and its removal confirmed, the final read awaited
    st.update(state="cancelling", cancel_sent=NOW + 500, cancel_confirmed=True)
    out = st.get("state") == "cancelling" and st.get("cancel_sent") and str(st.get("order_id") or "") == oid
    assert out  # (the condition the answer path checks before it resets cancel_sent)
    src = (ROOT / "live.py").read_text(encoding="utf-8")
    assert '**({} if out else {"cancel_sent": 0})' in src


# --- P1-7: the rule taker's money after a removal ------------------------------------------------------------------------------------
def p1_7_removed_unread() -> None:
    t, api, clock, x = TR.taker("on")
    rec = {"mode": "on", "name": "黄金", "rule": "gold_shut", "side": "up", "orders": [
        {"state": "cancelling", "removed": clock.ms, "shares": 50.0, "filled": 0.0, "limit": 0.58, "expires": clock.ms // 1000 + 600, "id": "1"}]}
    t.recs["w-removed"] = rec
    assert abs(t.pending_usd() - 29.0) < 1e-9, t.pending_usd()  # was 0: its fills not read yet, their USDT gone meanwhile
    rec["orders"][0]["state"] = "final"
    assert t.pending_usd() == 0.0
    del t.recs["w-removed"]
    x.owner.store.close()


# --- P1-6: the rule journal's outcome ---------------------------------------------------------------------------------------------------
def p1_6_recover_index_set() -> None:
    with tempfile.TemporaryDirectory() as folder:
        t, api, clock, x = TR.taker("on")
        path = str(Path(folder) / "rule-orders.jsonl")
        t.journal_path = lambda: path
        key = "xau-updown-15m-1|on"
        watch = {"slug": "xau-updown-15m-1", "mode": "on", "market_id": "901", "index_set": 0, "start_ms": clock.ms, "orders": []}
        t.recs[key] = dict(watch)  # the store's copy: the one its watch made (no outcome yet)
        with open(path, "w", encoding="utf-8") as f:
            f.write(json.dumps({"key": key, "rec": {**watch, "index_set": 2}, "order": {"hash": "0xabc", "shares": 10.0}}) + "\n")
        t.recover()
        assert t.recs[key]["index_set"] == 2 and t.recs[key]["orders"][0]["state"] == "unknown", t.recs[key]  # was 0
        with contextlib.suppress(Exception):
            import os
            os.close(t.journal_fd)
        x.owner.store.close()


# --- P1-8: the chain's answers without a result ------------------------------------------------------------------------------------
async def p1_8_rpc_result() -> None:
    chain = L.Chain("https://rpc.example", L.Wallet(T["KEY"], 56, ""))
    answers = [b'{"jsonrpc":"2.0","id":1}', b'{"jsonrpc":"2.0","id":1,"result":null}', b'{"jsonrpc":"2.0","id":1,"result":"0x"}']
    plain = m._http_get
    m._http_get = lambda url, payload, timeout, headers: answers.pop(0)
    try:
        with contextlib.suppress(m.RemoteError):
            await chain.known("0x" + "11" * 32)
            assert False, "an answer without a result read as 'the node does not know it'"
        assert await chain.known("0x" + "11" * 32) is False  # a null result is the node's answer
        try:
            await chain.usdt_balance()
            assert False, "an empty eth_call read as a balance of 0"
        except m.RemoteError as error:
            assert "不足一个字" in str(error), error
    finally:
        m._http_get = plain


# --- P1-9: the redeem's winner by its own index set; a revert's hour ---------------------------------------------------------------
async def p1_9_winner_and_revert() -> None:
    rows = [{"name": n, "indexSet": s, "status": "WON" if n == "C" else "LOST"} for n, s in (("A", 1), ("B", 2), ("C", 4))]
    won = m.predict_resolution({"status": "RESOLVED", "outcomes": rows})
    assert won["index"] == 2 and won["index_set"] == 4, won  # the third outcome: its bit mask, not its place + 1 (3)
    assert m.predict_resolution({"status": "RESOLVED", "outcomes": rows[:2] and [{**rows[0], "status": "WON"}, rows[1]]})["index_set"] == 1
    # a sent redeem's receipt found reverted: its hour starts now, whatever an earlier failure left
    bot = make_bot()
    key, sent_key = "live:redeemed:903:1", bot.REDEEM_SENT + "903:1"
    bot.store.put(sent_key, {"at": NOW, "tx": "0x" + "22" * 32, "amount": str(L.to_wei(10)), "title": "w", "outcome": "Up"})
    bot.live_redeem_failed[key] = time.monotonic() - 2 * bot.LIVE_REDEEM_RETRY_SECONDS  # an old failure's time, long over

    class Reverted:
        async def receipt(self, tx):
            return {"blockNumber": "0x10", "status": "0x0", "gasUsed": "0x5208", "effectiveGasPrice": "0x3b9aca00"}
    bot.live.chain = Reverted()
    errors: dict = {}
    booked, out = await bot.live_redeem_sent(NOW + 60_000, [], errors, {})
    assert booked[key]["kind"] == "failed" and time.monotonic() - bot.live_redeem_failed[key] < 5, bot.live_redeem_failed[key]  # was the old time: retried at once


# --- P1-10: the expiry floor as the order is built -----------------------------------------------------------------------------------
async def p1_10_floor_at_build() -> None:
    stale = L.order_expiry
    L.order_expiry = lambda end_ms, now_ms: int(now_ms) // 1000 + 60  # as if decided long before its send: 60 s left at the build
    try:
        bot = make_bot(SIM_WAYS="maker")
        await bot.live_prepare()
        world["markets"] = [card(NOW)]
        await bot.live_prefetch(NOW)
        await step(bot, NOW, card(NOW))
    finally:
        L.order_expiry = stale
    signed = [int(body["data"]["order"]["expiration"]) for kind, body in bot.live.api.calls if kind == "create"]
    assert signed and all(e >= NOW // 1000 + L.ORDER_MIN_SECONDS for e in signed), [e - NOW // 1000 for e in signed]  # was NOW + 60: refused
    assert L.ORDER_MIN_SECONDS == 130


# --- P1-11: the stop's order --------------------------------------------------------------------------------------------------------
async def p1_11_stop_order() -> None:
    pool = m.predict_pool()
    m.predict_pools_close()
    assert m._PREDICT_POOL is None and m.predict_pool() is not pool  # a fresh pool, never the shut one ("cannot schedule new futures")
    m.predict_pools_close()
    # the live trader's side work stops before the pools go (Bot.run calls stop_beside first), side by side
    bot = make_bot()
    sleeper = asyncio.create_task(asyncio.sleep(60))
    bot.live_fill_tasks.add(sleeper)
    started = time.monotonic()
    await bot.stop_beside()
    assert sleeper.cancelled() and time.monotonic() - started < 5, (sleeper, time.monotonic() - started)
    await bot.stop_beside()  # (a second call — run()'s own end — finds nothing left to stop)
    src = (ROOT / "main.py").read_text(encoding="utf-8")
    run_end = src[src.index("            await asyncio.gather(*tasks, *pending, stopper, return_exceptions=True)"):]
    assert run_end.index("await self.stop_beside()") < run_end.index("predict_pools_close()") < run_end.index("store_wait_close()")
    # a viewer that stopped reading: its write waits DRAIN_SECONDS, never for ever
    web = m.WebServer.__new__(m.WebServer)
    web.DRAIN_SECONDS = 0.2

    class Stuck:
        async def drain(self):
            await asyncio.sleep(60)
    started = time.monotonic()
    with contextlib.suppress(asyncio.TimeoutError):
        await web.drain(Stuck())
    assert time.monotonic() - started < 2
    # a store whose writes keep failing as it closes: the writer gives up, close() returns, its connection not closed under it
    with tempfile.TemporaryDirectory() as folder:
        store = m.Store(str(Path(folder) / "s.sqlite3"))
        store._apply = lambda conn, rows: (_ for _ in ()).throw(sqlite3.OperationalError("disk I/O error"))
        store.put("k", {"v": 1})
        started = time.monotonic()
        store.close()  # (its flush waits FLUSH_SECONDS for the failing writer, then the writer gives up at once)
        assert time.monotonic() - started < store.FLUSH_SECONDS + 4 and not store.writer.is_alive(), time.monotonic() - started


# --- P1-12: the streams' marks set before their requests -------------------------------------------------------------------------
async def p1_12_stream_marks() -> None:
    feed = m.PredictFeed(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}))
    stream = feed.stream

    class Ws:  # a refusal read while the send awaits (the frames are read beside the sync)
        def __init__(self, on_send):
            self.on_send = on_send

        async def send(self, text):
            msg = json.loads(text)
            await asyncio.sleep(0)
            self.on_send(msg)
    stream.wanted = {"101": ("HSI", False)}
    await stream.sync(Ws(lambda msg: stream.answered({"type": "R", "requestId": msg["requestId"], "success": False, "error": {"code": "nope"}})))
    assert "101" not in stream.subscribed and "101" in stream.refused, (stream.subscribed, stream.refused)  # was: marked for good
    stream.book("101", {"version": float("nan"), "bids": [["0.5", "10"]]})  # (no NaN version taken: it raised)
    # the rule taker's stream the same
    rs = R.RuleStream.__new__(R.RuleStream)
    R.RuleStream.__init__(rs, *([] if R.RuleStream.__init__.__code__.co_argcount == 1 else [types.SimpleNamespace(config=m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}))]))
    rs.wanted, rs.subscribed = {"202"}, set()

    def refuse(msg):
        if msg.get("method") == "subscribe":
            asyncio.get_running_loop().create_task(rs.handle(None, json.dumps({"type": "R", "requestId": msg["requestId"], "success": False, "error": "nope"})))

    class RWs:
        async def send(self, text):
            refuse(json.loads(text))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
    rs.SYNC_S = 0.01
    task = asyncio.create_task(rs.sync_loop(RWs()))
    await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert "202" not in rs.subscribed or rs.refused.get("202"), (rs.subscribed, rs.refused)


# --- P1-13: a sweep order on a book a cent wide ---------------------------------------------------------------------------------
def p1_13_sweep_cent_wide() -> None:
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_WAYS": "maker", "SIM_MAKER_POINTS": "off", "SIM_MAKER_SESSION": "off",
                             "SIM_MAKER_EXIT_CENTS": "0", "SIM_MAKER_DEEP_CENTS": "10", "SIM_MAKER_DEEP_ONLY": "on",
                             "SIM_MAKER_FOLLOW_SETTLE_SECONDS": "0", "SIM_MAKER_SWEEP_SETTLE_SECONDS": "0"})
    bot = m.Bot(cfg, m.Store(":memory:"), T["FM"](NOW), None)
    lv = lambda rows: tuple((D(p), D(q)) for p, q in rows)
    # somebody's 45¢ the 买1, the best ask a cent over it; fair 55¢ (the front leaves 10¢ = the deep bar), others' 30¢ under
    book = m.PredictBook("HSI", HSI_SLUG, "101", "t", lv([["0.45", "300"], ["0.30", "200"]]), lv([["0.46", "300"]]), NOW, None)
    mk = m.SimMarket(HSI_SLUG, "恒生指数", "close", "HSI", 0.55, book, 0.03, "", ("涨", "跌"), T["SETTLE_HSI"],
                     {"basis": {"fair_up": 0.55, "ref": 24600.0}, "sources": [{"what": "恒指期货", "source": "etnet"}], "proxy": None})
    # the fresh order on this book: no cent over the 买1 to be had — a sweep at 31¢ (a cent over the others' 30¢)
    assert m.Bot.sweep_price(book, "up", 0.55, 0.10)[0] == 0.31
    sweep = {"market": HSI_SLUG, "side": "up", "maker": True, "price": 0.31, "order": 100.0, "shares": 0.0, "status": "resting",
             "deep": True, "sweep": True, "settle": T["SETTLE_HSI"], "kind": "close", "key": "HSI", "version": bot.sim_version()}
    why = bot.sim_requote_reason(sweep, mk, NOW + 1_000)
    assert why == "", why  # was "盘口离公平价远了，击穿单改挂到买1 之上 1¢（45.0¢）": withdrawn, placed back as the same sweep, for ever


# --- P1-14: a window closed on an empty answer ------------------------------------------------------------------------------------
async def p1_14_range_empty_tail() -> None:
    spec = next(s for s in m.RANGE_MARKETS if s.venue == "spot")
    spec = dataclasses.replace(spec, start_ms=NOW - 10 * 3_600_000 - NOW % 3_600_000, end_ms=NOW - 3 * 3_600_000 - NOW % 3_600_000 - 60_000)
    rm = m.RangeMarket(m.Store(":memory:"), spec)
    start, end = rm.start_ms, rm.window_end
    hours = [[h, "100", "101", "99", "100", "1", h + 3_599_999] for h in range(start, end - 3 * 3_600_000, 3_600_000)]  # the last 3 hours missing

    async def get(path, **params):
        rows = hours if params.get("interval") == "1h" else []
        return [r for r in rows if params["startTime"] <= r[0] <= params["endTime"]]
    rm.get = get
    await rm.scan(NOW)
    assert int(rm.history["through"]) < end, (rm.history["through"] - end) / 3_600_000  # was stamped to the end: its last hours never read
    assert rm.scanned_ms != NOW  # (and not counted as read up to now)
    # read in full: complete
    hours[:] = [[h, "100", "101", "99", "100", "1", h + 3_599_999] for h in range(start, end, 3_600_000)]
    await rm.scan(NOW + 1)
    assert int(rm.history["through"]) >= end


def p1_14_cap_unread_after_end() -> None:
    spec = m.CapSpec("TEST", "test-market-cap", "TEST 市值", "0x" + "ab" * 20, NOW - 3 * 86_400_000, NOW - 3_600_000, (D("1e9"),))
    assert spec.gecko  # (a bars source: GeckoTerminal's hourly bars, none read here)
    cap = m.CapMarket(m.Store(":memory:"), spec)
    after = spec.end_ms + 120_000
    assert cap.coverage() and cap.probability(D("1e30"), after) is None  # was 0.0: NO on a record never read to its end


# --- P1-15: a REST failure read as "not listed" ------------------------------------------------------------------------------------
async def p1_15_find_rest_failure() -> None:
    feed = m.PredictFeed(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "PREDICT_GRAPHQL": "off"}))

    async def fail(url, *a, **k):
        raise m.RemoteError("HTTP 500: internal error")
    feed.fetch = fail
    try:
        await feed.find("btc-updown-15m-1700000000")
        assert False, "a 500 read as 'not listed'"
    except m.RemoteError as error:
        assert "500" in str(error)

    async def missing(url, *a, **k):
        raise m.RemoteError("HTTP 404: not found")
    feed.fetch = missing
    assert await feed.find("btc-updown-15m-1700000000") == []  # a 404 is Predict's answer: not listed


# --- P2-1: a 408 is no refusal --------------------------------------------------------------------------------------------------
def p2_1_408() -> None:
    assert not L.refused_status(408) and L.refused_status(400) and L.refused_status(409) and not L.refused_status(500)
    assert L.remove_failure(m.RemoteError("HTTP 408: Request Timeout")) == "transient"


# --- P2-5: the balance's dust ---------------------------------------------------------------------------------------------------
async def p2_5_balance_dust() -> None:
    bot = make_bot()
    await bot.live_prepare()
    bot.live.balance = (L.to_wei(28), 10 ** 17, time.time())
    notional = 0.14 * 200
    assert notional > 28.0  # 28.000000000000004
    src = (ROOT / "live.py").read_text(encoding="utf-8")
    assert src.count("usd < notional - 1e-9") == 2 and "usd < notional:" not in src


# --- P2-6: 上涨 / 下跌; a settled pair closed ------------------------------------------------------------------------------------
async def p2_6_words_and_closed() -> None:
    bot = make_bot()
    assert bot.resolution_up({"kind": "close"}, {"name": "下跌", "index": 0, "split": False}) == 0.0  # was 1.0 (the first outcome taken as 涨)
    assert bot.resolution_up({"kind": "updown"}, {"name": "上涨", "index": 0, "split": False}) == 1.0  # was None
    row = TP.market(at=TP.NOW + 15 * 60_000, mid=201)
    ctl, api, clock = TP.controller([row], PREOPEN_MARKETS="btc", PREOPEN_SHARES="10")
    assert await ctl.place_pair(P.candidate(row, TP.NOW, ctl.owner.config, ctl.settings)) == ""
    pair = next(iter(ctl.ledger.rows.values()))
    pair["phase"] = "closing"
    for o in pair["orders"].values():
        o.update(state="final", filled=0.0)
    ctl.ledger.save(pair)

    async def market(mid):
        return {"status": "RESOLVED", "outcomes": [{"name": "Up", "indexSet": 1, "status": "WON"}, {"name": "Down", "indexSet": 2, "status": "LOST"}]}
    ctl.cancel_api.market = market

    async def read_final(order, pair):
        return None
    ctl.read_final = read_final
    clock.ms = P.settle_from(pair) + 1
    await ctl.settle_pair(pair)
    assert pair["accounted"] and pair["phase"] == "closed", (pair["accounted"], pair["phase"])  # was "closing" for good
    ctl.ledger.close()


# --- P2-8: an unknown order's own booked shares -----------------------------------------------------------------------------------
async def p2_8_unknown_own_shares() -> None:
    bot, tid, t, oid = await resting()
    t["shares"] = 40.0  # filled 40 of 100 before it went unknown
    want = t["live"]["want"]
    mid = str(want.get("market_id") or t.get("market_id") or "")
    if not mid:
        t["market_id"] = mid = "101"
    bot.live.api.positions_rows = [{"market": {"id": mid}, "outcome": {"indexSet": want["index_set"]}, "amount": str(L.to_wei(40))}]
    extra = await bot.live_unexplained(t)
    assert extra == 0.0, extra  # was 40: its own shares read as the account holding 40 more


# --- P1-2: the day's loss stop ------------------------------------------------------------------------------------------------------
def p1_2_file_day_and_index() -> None:
    day, next_day = "2026-11-26", "2026-11-27"
    rows = [{"accounted": True, "accounted_day": day, "pnl": 0.0, "late": [{"day": next_day, "pnl": -5.0}]}]  # +$5 that day, −$5 learned the next
    assert P.file_day_pnl(rows, next_day) == -5.0 and P.file_day_pnl(rows, day) == 5.0, (P.file_day_pnl(rows, next_day), P.file_day_pnl(rows, day))


async def p1_2_rule_loss_checks_the_stop() -> None:
    src = (ROOT / "rules.py").read_text(encoding="utf-8")
    settle = src[src.index("    async def settle(self) -> None:"):]
    settle = settle[:settle.index("\n    async def ", 10) if "\n    async def " in settle[10:] else len(settle)]
    assert "live_check_kill" in settle


# --- P0-2: a record moved while sim_confirm waited ---------------------------------------------------------------------------------
def p0_2_moved_record() -> None:
    bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), m.Store(":memory:"), T["FM"](NOW), None)
    tid = f"{HSI_SLUG}|up|挂"
    rec = {"market": HSI_SLUG, "side": "up", "status": "filled", "shares": 10.0, "price": 0.5, "order": 10.0, "maker": True}
    bot.sim_save([(tid, rec)])
    view = dict(bot.sim_trades())  # the step's view, taken before its wait for Predict's results
    assert bot.sim_still(view, tid)
    bot.sim_rekey(tid, rec)  # moved meanwhile (the express lane's withdrawal: the attempt kept at tid#1)
    assert not bot.sim_still(view, tid)  # was "still there" (tid in view): saved back at the slot — one fill under two keys
    bot.sim_save([(tid, {**rec, "shares": 0.0, "status": "resting"})])  # ...or a fresh order took the slot: not that one either
    assert not bot.sim_still(view, tid)


# --- P0-3: a pause write that fails stops no withdrawal ----------------------------------------------------------------------------
async def p0_3_pause_write_fails() -> None:
    bot, tid, t, oid = await resting()
    rows = [TP.market(at=TP.NOW + k * 15 * 60_000, mid=200 + k) for k in (1, 2)]
    ctl, api, _ = TP.controller(rows, PREOPEN_MARKETS="btc", PREOPEN_SHARES="10")
    for r in rows:
        assert await ctl.place_pair(P.candidate(r, TP.NOW, ctl.owner.config, ctl.settings)) == ""
    bot.preopen = ctl

    def full(*a, **k):
        raise sqlite3.OperationalError("database or disk is full")
    ctl.ledger.pause, ctl.ledger.save_many = full, full
    said = await bot.live_cancel_command(["all"], NOW + 2_000)  # was: the pause's error ended it before a single order was withdrawn
    await bot.live_fills_wait()
    await asyncio.gather(*list(ctl.settling.values()), return_exceptions=True)
    assert oid not in bot.live.api.open, said  # the model trader's order withdrawn
    assert sum(1 for r in api.rows.values() if r["status"] == "OPEN") == 0, said  # ...and the pre-open mode's (save_many failing too)
    assert ctl.manual_pause and "⚠️ 预挂的暂停没写进数据库" in said and bot.live_cancel_failed(said), said  # paused in memory, said, red
    ctl.ledger.close()


# --- P0-4: a pre-open pair withdrawn while its requests are out ----------------------------------------------------------------------
async def p0_4_placing_pair() -> None:
    row = TP.market(at=TP.NOW + 15 * 60_000, mid=201)
    ctl, api, clock = TP.controller([row], PREOPEN_MARKETS="btc", PREOPEN_SHARES="10")
    assert await ctl.place_pair(P.candidate(row, TP.NOW, ctl.owner.config, ctl.settings)) == ""
    pair = next(iter(ctl.ledger.rows.values()))
    at = int(pair["cancel_ms"]) + 1
    assert pair in ctl.due(at)
    ctl.placing.add(pair["slug"])  # its two requests still out (place_pair withdraws it itself as they are answered)
    assert pair not in ctl.due(at), "taken by the cancellation loop while its requests were still out"
    ctl.placing.discard(pair["slug"])
    assert pair in ctl.due(at)
    ctl.ledger.close()


# --- P0-5: a trimmed taker read again ------------------------------------------------------------------------------------------------
async def p0_5_trimmed_taker_reread() -> None:
    bot, tid, t, oid = await resting("taker")
    api = bot.live.api
    api.fill(oid, 100, done=True, price=0.58)
    row = dict(api.closed[oid])
    bot.live_apply(tid, t, row, None, NOW + 5_000)
    assert t["shares"] == 100 and abs(float(t["avg"]) - 0.58) < 1e-9, (t["shares"], t.get("avg"))
    fills = len(t["fills"])
    t["fee_shares"], t["shares"] = 2.0, 98.0  # the fee taken in shares (live_late_fills' trim): 2 of the 100 bought not held
    t["price"] = price = round(float(t["avg"]) * 100 / 98, 6)
    bot.live_apply(tid, t, dict(row), None, NOW + 70_000)  # its own record read again: 100 filled, as before
    assert (t["shares"], len(t["fills"]), t["price"]) == (98.0, fills, price), (t["shares"], len(t["fills"]), t["price"])  # was a fill of 2 more


# --- P1-1: the rule taker's withdrawal marks its sends before it waits ---------------------------------------------------------------
async def p1_1_cancel_asked_first() -> None:
    t, api, clock, x = TR.taker("on")
    resting_order = {"state": "open", "id": "1", "shares": 10.0, "filled": 0.0, "limit": 0.5, "expires": clock.ms // 1000 + 600}
    flying = {"state": "placing", "shares": 10.0, "filled": 0.0, "limit": 0.5, "hash": "0xfly", "expires": clock.ms // 1000 + 600}
    t.recs["w"] = {"mode": "on", "slug": "w", "name": "黄金", "orders": [resting_order, flying]}
    seen = []

    async def remove_ids(ids, clients=None):
        seen.append(bool(flying.get("cancel_asked")))  # an answer to the flying order could come during this wait
        return {"removed": list(ids), "noop": []}
    t.remove_ids = remove_ids
    await t.withdraw_all("管理员撤单")
    assert seen == [True], seen  # was [False]: marked after the waits — an answer during them found no mark
    del t.recs["w"]
    x.owner.store.close()
    bot = live_text_bot = types.SimpleNamespace(__dict__={})
    assert L.LiveBot.live_cancel_failed(types.SimpleNamespace(), "✅ 策略挂单 1 笔：Predict 确认撤掉 1 笔\n规则吃单：1 张没撤成（HTTP 500）")
    assert not L.LiveBot.live_cancel_failed(types.SimpleNamespace(), "✅ 策略挂单 1 笔：Predict 确认撤掉 1 笔")


TESTS = [p0_1_settled_found_open, p0_2_moved_record, p0_3_pause_write_fails, p0_4_placing_pair, p0_5_trimmed_taker_reread,
         p1_1_cancel_asked_first, p1_3_unknown_not_final, p1_4_taker_cancel_lagging_read, p1_4_third_look_recancel,
         p1_5_track_withdrawn_meanwhile, p1_5_recheck_answered_meanwhile, p1_5_misses_move_with_the_record, p1_5_answer_keeps_a_removal_out,
         p1_7_removed_unread, p1_6_recover_index_set, p1_8_rpc_result, p1_9_winner_and_revert, p1_10_floor_at_build, p1_11_stop_order,
         p1_12_stream_marks, p1_13_sweep_cent_wide, p1_14_range_empty_tail, p1_14_cap_unread_after_end, p1_15_find_rest_failure,
         p2_1_408, p2_5_balance_dust, p2_6_words_and_closed, p2_8_unknown_own_shares, p1_2_file_day_and_index, p1_2_rule_loss_checks_the_stop]


async def run(only: list[str] | None = None) -> list[str]:
    failed = []
    for test in TESTS:
        if only and test.__name__ not in only:
            continue
        try:
            out = test()
            if asyncio.iscoroutine(out):
                await out
        except Exception as error:
            if not only and len(sys.argv) < 2:
                raise
            failed.append(f"{test.__name__}: {type(error).__name__}: {str(error)[:160]}")
    return failed


if __name__ == "__main__":
    picked = [a for a in sys.argv[1:] if a != "--each"]
    failures = asyncio.run(run(picked or None)) if "--each" not in sys.argv else asyncio.run(run([t.__name__ for t in TESTS]))
    if failures:
        print("\n".join(failures))
        sys.exit(1)
    print("AUDIT_EXT2_OK")
