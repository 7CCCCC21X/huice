"""Audit regression tests for the pre-open cluster. Entirely offline."""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import offline  # noqa: F401

# PRE-DOC-MAXOPEN: since 1.132.1 the pre-open reservation is capped only by PREOPEN_MAX_OPEN_USD (default 0 = none,
# 1.132.3); LIVE_MAX_OPEN_USD is the model trader's cap and preopen.budget_problem never reads it (pinned by
# test_preopen.test_own_budget_only_the_model_traders_cap_and_exposure_are_not_this_modes). PREOPEN.md must not tell
# the operator that LIVE_MAX_OPEN_USD bounds this mode's reservation.
# The Docker image copies only the code and tests/ (Dockerfile), not the docs: the check runs where PREOPEN.md is (the
# repository, the local suite) and is skipped in the image build.
_doc_path = ROOT / "PREOPEN.md"
if _doc_path.exists():
    _doc = _doc_path.read_text(encoding="utf-8")
    # Per clause (split on 。；，), so the correct "本模式的预留只受 PREOPEN_MAX_OPEN_USD 约束，原策略只受 LIVE_MAX_OPEN_USD 约束"
    # (two clauses) passes while "预留仍受 PREOPEN_MAX_OPEN_USD 和 LIVE_MAX_OPEN_USD 约束" (one clause) fails.
    _clauses = re.split(r"[。；，\n]", _doc)
    _bad = [c for c in _clauses if "预留" in c and "`LIVE_MAX_OPEN_USD` 约束" in c]
    assert not _bad, f"PREOPEN.md still says LIVE_MAX_OPEN_USD caps the pre-open reservation: {_bad[0][:200]}"

# PRE-MONITOR-SAVES: monitor_pair ran ledger.save (one fsync'd commit, synchronous=FULL) for every resting pair every
# MONITOR_MS even when the fresh account snapshot covered all its orders and nothing was read. Now: no read, no commit
# (checked_ms still advances in memory); a read that changes a fill is still saved.
import asyncio
import tempfile
sys.path.insert(0, str(ROOT / "tests"))
import preopen as _p
import test_preopen as _T


async def _monitor_saves(folder):
    db = str(Path(folder) / "s.sqlite3")
    ctl, api, clock = _T.controller(STATE_DB=db)
    c = _p.candidate(_T.market(), _T.NOW, ctl.owner.config, ctl.settings)
    await ctl.place_pair(c)
    pair = ctl.ledger.rows[c["slug"]]
    saves = [0]
    real = ctl.ledger.save
    def counted(row):
        saves[0] += 1
        real(row)
    ctl.ledger.save = counted
    clock.ms = _T.NOW + 20_000
    ctl.listed_ids = {o["id"] for o in pair["orders"].values()}
    ctl.account_at = clock.ms
    await ctl.monitor_pair(pair)
    assert saves[0] == 0, f"a look that read nothing still committed the pair ({saves[0]} saves)"
    assert pair["checked_ms"] == clock.ms, "the look must still be paced (checked_ms advanced in memory)"
    # a read that changes something is still durable
    ctl.listed_ids = set()
    up = pair["orders"]["up"]
    api.rows[up["hash"]]["amountFilled"] = str(int(api.rows[up["hash"]]["amount"]) // 2)
    clock.ms += 5_000
    ctl.account_at = clock.ms
    await ctl.monitor_pair(pair)
    assert saves[0] == 1, f"a read that changed a fill must be saved once ({saves[0]} saves)"
    filled = pair["orders"]["up"]["filled"]
    assert filled > 0
    ctl.ledger.close()
    again = _p.Ledger(ctl.settings.db)  # (the mode keeps its own file next to STATE_DB)
    assert again.rows[c["slug"]]["orders"]["up"]["filled"] == filled, "the partial fill must be on disk"
    again.close()
    for task in ctl.settling.values():
        task.cancel()
    await asyncio.gather(*ctl.settling.values(), return_exceptions=True)


with tempfile.TemporaryDirectory() as _folder:
    asyncio.run(_monitor_saves(_folder))

# PRE-ZHIPU-HALT: check_cards — the only code that fills card_holds, which discovery, place_pair and before_send read —
# passed over every card without a daily price limit (limit_pct 0), a filter from 1.130.0 when only the limited stock
# cards had an exchange. 1.134.0 added 智谱 (HKEX 2513: no daily limit, "只看临停" — the halt rule alone) and card_hold
# judges it so, but check_cards never asked: through a halt card_holds stayed empty, the resting pair stayed active and
# the next windows were quoted. Now only a card without an exchange is passed over.
import types
import main as _m


async def _zhipu_halt_is_held():
    t10 = _T.NOW + 3600_000  # 10:00 Beijing = HKT (Friday 2026-11-06): HKEX's morning session, past its first minutes
    first, second = (_T.market("zhipu", at=t10 + k * 15 * 60_000, mid=500 + k) for k in (2, 3))  # the 10:30 and 10:45 windows
    btc = _T.market("btc", at=t10 + 30 * 60_000, mid=600)
    ctl, api, clock = _T.controller([first, second, btc], PREOPEN_MARKETS="zhipu,btc,xau")
    clock.ms = ctl.account_at = t10
    for mk in (first, btc):
        await ctl.place_pair(_p.candidate(mk, clock.ms, ctl.owner.config, ctl.settings))
    pair, other = ctl.ledger.rows[first["categorySlug"]], ctl.ledger.rows[btc["categorySlug"]]
    assert (pair["phase"], other["phase"], len(api.sent)) == ("active", "active", 4), (pair["phase"], other["phase"], len(api.sent))
    quote = lambda quoted: types.SimpleNamespace(last=_m.D("119"), prev_close=_m.D("100"), quoted_ms=quoted, fetched_ms=clock.ms)
    # 智谱 up 19% on the day (HKEX has no limit: no hold by itself), its quote's own time 4 minutes still in continuous
    # trading while the feed itself is fresh: a halt. 黄金's quote an hour still: no exchange, nothing judged.
    ctl.owner.updowns = {"ZHIPU-15M": types.SimpleNamespace(lead=quote(clock.ms - 4 * 60_000)),
                         "XAU-15M": types.SimpleNamespace(lead=quote(clock.ms - 3600_000))}
    ctl.card_checked_ms = 0
    await ctl.check_cards(clock.ms)
    assert "智谱 交易时段里成交时间 4 分钟没动" in ctl.card_holds.get("ZHIPU-15M", ""), f"the halted HK card is not held: {ctl.card_holds}"
    assert set(ctl.card_holds) == {"ZHIPU-15M"}, f"only the card with an exchange is judged: {ctl.card_holds}"
    ids = {o["id"] for o in pair["orders"].values()}
    assert pair["phase"] != "active" and ids and ids <= set(api.removed), (pair["phase"], ids, api.removed)
    assert not _p.counts_against(pair), "a halt is not the window's doing: the withdrawal must not use up its quotes"
    assert ctl.before_send(pair["orders"]["up"]["body"]), "the held card's order must not be sent"
    assert other["phase"] == "active" and not {o["id"] for o in other["orders"].values()} & set(api.removed), "BTC is not touched"
    assert "股票卡暂停：智谱" in ctl.status()
    sent = len(api.sent)
    assert await ctl.place_pair(_p.candidate(second, clock.ms, ctl.owner.config, ctl.settings)) == "held"
    assert len(api.sent) == sent, "no new 智谱 window quoted while it is halted"
    api.calls.clear()
    await ctl.discover()
    assert not [x for x in api.calls if "zhipu" in x], f"held: its next hour's windows are not even asked for ({api.calls})"
    # the quote prints again: the hold lifts
    clock.ms += _p.CARD_CHECK_MS
    ctl.owner.updowns["ZHIPU-15M"].lead = quote(clock.ms)
    await ctl.check_cards(clock.ms)
    assert ctl.card_holds == {}, ctl.card_holds
    for task in ctl.settling.values():
        task.cancel()
    await asyncio.gather(*ctl.settling.values(), return_exceptions=True)
    ctl.ledger.close()


asyncio.run(_zhipu_halt_is_held())

# PRE-PNL-DROP: live_check_kill and live_send_block add preopen_pnl() — the pre-open mode's accounted day — to the model
# trader's day against LIVE_MAX_DAILY_LOSS_USD, but it was 0.0 whenever the mode was not running: a crash (the dead
# controller stays attached until the next apply), /preopen off (wound down, then cleared) or a restart with the mode off
# took the day's realised pre-open loss out of the stop and gave the model trader its whole cap back. Now a stopped mode's
# loss of the day still counts (its controller's records, else the ledger file, read once a day while no controller holds
# it); its gain still counts 0 then, and the running mode counts net as before.
import copy
import live as _L

_PROBE = {"live": {"want": {"shares": 10}, "events": [{"at": 0}]}, "maker": True, "price": 0.5, "order": 10}


def _pnl_config(db, on=True, **env):
    return _m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "STATE_DB": ":memory:", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64,
                               "LIVE_MAX_DAILY_LOSS_USD": "100", "PREOPEN_DB": db, "PREOPEN_15M": "on" if on else "off", **env})


def _pnl_bot(clock, db, on, stopping):
    bot = _L.LiveBot.__new__(_L.LiveBot)
    bot.config, bot.store, bot.stopping = _pnl_config(db, on), _m.Store(":memory:"), stopping
    bot.market = types.SimpleNamespace(now_ms=lambda: clock.ms)
    bot.live_notify, bot.live_note_error = (lambda text, key="": None), (lambda text: None)
    bot.live = types.SimpleNamespace(ready=False, not_ready_why="past the loss check", usd=lambda: None)
    bot.preopen, bot.preopen_task, bot.preopen_lock, bot.preopen_factory = None, None, asyncio.Lock(), None
    return bot


def _stop_hits(bot, clock, model_loss=60.0):
    """(the send-time recheck refuses, the kill switch trips) with the model trader's settled day at −model_loss."""
    trades = {"m1": {"live": {"state": "done"}, "status": "settled", "settled": clock.ms - 1000, "payout": 0.0, "price": 0.5,
                     "shares": model_loss / 0.5}}
    bot.store.delete_keys(["live:killed"])
    refused = "已达 LIVE_MAX_DAILY_LOSS_USD $100" in bot.live_send_block(_PROBE, trades, clock.ms)
    bot.live_check_kill(trades, clock.ms)
    tripped = bool(bot.live_kill_record())
    bot.store.delete_keys(["live:killed"])
    return refused, tripped


async def _pnl_drop(folder):
    db = str(Path(folder) / "p.preopen.sqlite3")
    # a real loss of the day, booked by the controller in its ledger file: Up filled in full, withdrawn, Down won (−$49);
    # and a bigger loss booked yesterday, which today's stop must not count
    ctl, api, clock = _T.controller(PREOPEN_DB=db, PREOPEN_SHARES="100", LIVE_MAX_DAILY_LOSS_USD="100")
    c = _p.candidate(_T.market(), _T.NOW, ctl.owner.config, ctl.settings)
    await ctl.place_pair(c)
    pair = ctl.ledger.rows[c["slug"]]
    up = api.rows[pair["orders"]["up"]["hash"]]
    up["amountFilled"] = up["amount"]
    await ctl.cancel_many([pair], "cancel")
    await asyncio.gather(*list(ctl.settling.values()), return_exceptions=True)
    clock.ms = c["start_ms"] + 20 * 60_000  # 10:20 Beijing, the same day
    api.markets["123"]["outcomes"][0]["status"] = "WON"
    await ctl.settle_pair(pair)
    assert pair["accounted_day"] == "2026-11-06" and abs(pair["pnl"] + 49) < 1e-9, (pair.get("accounted_day"), pair.get("pnl"))
    ctl.ledger.save({**copy.deepcopy(pair), "slug": pair["slug"] + "-yesterday", "accounted_day": "2026-11-05", "pnl": -500.0})
    # running (started by the bot itself): its day counts, as before — −60 + −49 is past the $100 stop, −40 + −49 is not
    bot = _pnl_bot(clock, db, True, ctl.owner.stopping)
    bot.preopen_factory = lambda owner: ctl
    await bot.preopen_apply()
    await asyncio.sleep(0.2)
    assert bot.preopen is ctl and bot.preopen_active() and abs(bot.preopen_pnl() + 49) < 1e-9, bot.preopen_pnl()
    assert _stop_hits(bot, clock) == (True, True) and _stop_hits(bot, clock, 40.0) == (False, False)
    # a crash: an exception escapes its cancellation loop; the bot's own done-callback marks it dead, still attached
    async def fault(now_ms):
        raise RuntimeError("injected fault")
    ctl.check_cards = fault
    await asyncio.wait([bot.preopen_task], timeout=5)
    await asyncio.sleep(0)
    assert bot.preopen is ctl and not ctl.alive and not bot.preopen_active(), (bot.preopen, ctl.alive)
    assert abs(bot.preopen_pnl() + 49) < 1e-9, f"crashed: the day's realised pre-open loss left the stop ({bot.preopen_pnl():+.2f})"
    assert _stop_hits(bot, clock) == (True, True), "crashed: the model trader got its whole cap back"
    # switched off on the page: the dead controller is cleared (nothing left to wind down) — no controller, the file counts
    bot.config = _pnl_config(db, on=False)
    await bot.preopen_apply()
    assert bot.preopen is None
    reads, real = [], _p.read_rows
    _p.read_rows = lambda path: (reads.append(path), real(path))[1]
    try:
        assert abs(bot.preopen_pnl() + 49) < 1e-9, f"off and cleared: today's loss off the file ({bot.preopen_pnl():+.2f})"
        assert _stop_hits(bot, clock) == (True, True) and _stop_hits(bot, clock, 40.0) == (False, False)
        assert len(reads) == 1, f"the file is read once, not at every check ({len(reads)} reads)"
        clock.ms += 86_400_000
        assert bot.preopen_pnl() == 0.0 and len(reads) == 2, "the next Beijing day: read again, nothing booked on it"
        clock.ms -= 86_400_000
        # a restart the same day with the mode off: nothing is started, the loss still counts
        again = _pnl_bot(clock, db, False, asyncio.Event())
        await again.preopen_apply()
        assert again.preopen is None and abs(again.preopen_pnl() + 49) < 1e-9 and _stop_hits(again, clock) == (True, True)
    finally:
        _p.read_rows = real
    # a stopped mode's gain never loosens the stop (it counted 0 before too); running, its day counts net as before; with
    # LIVE≠on the mode is not running for real, and its real loss of the day still counts
    bot.preopen = types.SimpleNamespace(alive=False, daily_pnl=lambda: 60.0)
    assert bot.preopen_pnl() == 0.0
    bot.config, bot.preopen.alive = _pnl_config(db), True
    assert bot.preopen_pnl() == 60.0
    bot.config, bot.preopen = _pnl_config(db, LIVE="pause"), types.SimpleNamespace(alive=True, daily_pnl=lambda: -49.0)
    assert not bot.preopen_active() and bot.preopen_pnl() == -49.0


with tempfile.TemporaryDirectory() as _folder:
    asyncio.run(_pnl_drop(_folder))

# PRE-PAUSE-STRIKES: withdraw_all — /preopen pause, /live cancel all, the emergency stop — withdrew a book of BULK_MIN_IDS
# ids or fewer (or any book of a crashed controller) here and now under the operator's reason without the free mark: one
# failure against every withdrawn window's level, so after three pauses the window was never quoted again ("失败满 3 次"),
# though PREOPEN.md says a pause does not count and a bigger book's pause (the loop's "到时撤单或真实交易暂停") never did.
# place_pair's tail did the same to a pair whose two POSTs an operator's stop overtook. Now the resting (active) pairs an
# operator's stop withdraws are free; a pair already closing on its own failure, and a side not confirmed at placement,
# still count (marking every pair free — cancel_all(reason, free=True) — would erase those, and the cap with them).


async def _quote_all(ctl):
    """One discovery pass (a full scan), each queued level placed as the placement workers would: the keys placed."""
    ctl.full_scan_ms = 0
    await ctl.discover()
    keys = []
    while not ctl.queue.empty():
        _, _, c = ctl.queue.get_nowait()
        ctl.account_at = ctl.now()
        assert await ctl.place_pair(c) == "", c["slug"]
        ctl.queued.discard(c["slug"])
        keys.append(c["slug"])
    return sorted(keys)


async def _settled(ctl):
    await asyncio.gather(*list(ctl.settling.values()), return_exceptions=True)


async def _read_finals(ctl, clock, pairs=None):
    """The cancellation loop's next try for the withdrawn pairs not cleared yet (all of them, or those given), the clock moved
    to it. Audit PRE-LATE-FILL: an order off the book is ended by a read of its record made LATE_FILL_MS after it left, not
    by one right after the removal — so a pair withdrawn with nothing filled is accounted at that try, a minute on. The
    blocks here that go on to what follows the accounting (written when it came on the spot) make the try with this;
    nothing to do, it does nothing."""
    waiting = [x for x in (ctl.ledger.rows.values() if pairs is None else pairs) if not x.get("accounted") and x["phase"] != "active"]
    if waiting:
        clock.ms = max(clock.ms, *(int(x.get("retry_ms") or 0) for x in waiting))
        for x in waiting:
            ctl.bookkeep(x)
            ctl.launch_reconcile(x)
        await _settled(ctl)


def _operator(ctl, clock):
    """A LiveBot with the real command handlers over the controller; the model trader has nothing resting."""
    bot = _L.LiveBot.__new__(_L.LiveBot)
    bot.preopen, bot.store, bot.market = ctl, _m.Store(":memory:"), types.SimpleNamespace(now_ms=lambda: clock.ms)

    async def model_cancel(words, now_ms):
        return "没有真实挂单可撤。"
    bot._live_cancel_command = model_cancel
    return bot


async def _operator_stops_are_free():
    # PREOPEN_AHEAD_MINUTES=60 (PREOPEN.md: "60 = 大约 4 场"), BTC alone: four windows, eight ids — the small path
    rows = [_T.market(at=_T.NOW + k * 15 * 60_000, mid=200 + k) for k in range(1, 5)]
    ctl, api, clock = _T.controller(rows, PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="60", PREOPEN_SHARES="10")
    bot = _operator(ctl, clock)
    pause, resume = (_m.Request("/preopen", [a], 1, 0, 1) for a in ("pause", "resume"))
    stops = [("/preopen pause", lambda: bot.cmd_preopen(pause), lambda: bot.cmd_preopen(resume)),
             ("/live cancel all", lambda: bot.live_cancel_command(["all"], clock.ms), lambda: bot.cmd_preopen(resume)),
             ("emergency stop", lambda: bot.live_control("stop", {}), lambda: bot.live_action("resume", [], clock.ms)),
             ("/preopen pause", lambda: bot.cmd_preopen(pause), lambda: bot.cmd_preopen(resume))]  # four stops: past MAX_ATTEMPTS
    windows = None
    for name, stop, again in stops:
        keys = await _quote_all(ctl)
        windows = windows or keys
        assert len(keys) == 4 and keys == windows, f"before {name}: {len(keys)} of 4 windows quoted ({keys})"
        assert sum(len(_p.removable_ids(x)) for x in ctl.ledger.rows.values() if x["phase"] != "closed") <= _p.BULK_MIN_IDS
        await stop()
        await _settled(ctl)
        await _read_finals(ctl, clock)
        withdrawn = [x for x in ctl.ledger.rows.values() if x["phase"] != "active"]
        assert ctl.manual_pause and not [x for x in ctl.ledger.rows.values() if x["phase"] == "active"], name
        assert all(x.get("accounted") for x in withdrawn), f"{name}: the withdrawn pairs' records not all read"
        strikes = _p.window_strikes(ctl.ledger.rows.values())
        assert strikes == {}, f"{name}: an operator's stop counted against the windows: {strikes}"
        await again()
        assert not ctl.manual_pause, name
        clock.ms += 30_000
    assert await _quote_all(ctl) == windows, "after four operator stops every window is still quoted (new salts)"
    assert "不再报价" not in ctl.status()
    assert len({x["orders"]["up"]["hash"] for x in ctl.ledger.rows.values()}) == 5 * 4, "every quote a new order"
    for task in ctl.settling.values():
        task.cancel()
    await _settled(ctl)
    ctl.ledger.close()

    # a pair already closing on its own failure (a side cancelled by someone else; the other side's removal not answered
    # yet) when the operator pauses: it still counts — only the resting pair is the operator's doing
    rows = [_T.market(at=_T.NOW + 30 * 60_000, mid=301), _T.market(at=_T.NOW + 45 * 60_000, mid=302)]
    ctl, api, clock = _T.controller(rows, PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="60", PREOPEN_SHARES="10")
    bot = _operator(ctl, clock)
    a, b = (_p.candidate(r, _T.NOW, ctl.owner.config, ctl.settings) for r in rows)
    for c in (a, b):
        await ctl.place_pair(c)
    failed, resting = ctl.ledger.rows[a["slug"]], ctl.ledger.rows[b["slug"]]
    api.rows[failed["orders"]["up"]["hash"]]["status"] = "CANCELLED"
    api.stuck = {failed["orders"]["down"]["id"]}
    clock.ms += 15_000
    ctl.account_at, ctl.listed_ids = clock.ms, set()
    await ctl.monitor_pair(failed)
    await _settled(ctl)
    assert (failed["phase"], failed.get("free"), resting["phase"]) == ("closing", None, "active"), (failed["phase"], failed.get("free"), resting["phase"])
    await bot.cmd_preopen(pause)
    api.stuck = set()
    clock.ms += 20_000
    for pair in (failed, resting):
        if not pair.get("accounted"):
            await ctl.cancel_pair(pair, pair.get("reason") or "撤单状态核对")  # the reconciliation's next try
    await _settled(ctl)
    await _read_finals(ctl, clock)
    assert failed.get("accounted") and resting.get("accounted"), (failed.get("accounted"), resting.get("accounted"))
    assert _p.counts_against(failed), "the window's own failure must still count after the pause"
    assert not _p.counts_against(resting), "the resting pair the pause withdrew must be free"
    assert _p.window_strikes(ctl.ledger.rows.values()) == {a["slug"]: 1}, _p.window_strikes(ctl.ledger.rows.values())
    ctl.ledger.close()

    # a crashed controller (no loop to leave a big book to): the operator's pause withdraws all 50 ids here — free too
    rows = [_T.market(at=_T.NOW + k * 15 * 60_000, mid=400 + k) for k in range(1, 26)]
    ctl, api, clock = _T.controller(rows, PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="1440", PREOPEN_SHARES="10")
    bot = _operator(ctl, clock)
    assert len(await _quote_all(ctl)) == 25
    ctl.alive = False
    reply = await bot.cmd_preopen(pause)
    assert reply.startswith("预挂模式已停止：50 张预挂单已撤"), reply
    await _settled(ctl)
    await _read_finals(ctl, clock)  # (a crashed controller has no loop of its own: the next start's)
    assert all(x.get("accounted") for x in ctl.ledger.rows.values())
    assert _p.window_strikes(ctl.ledger.rows.values()) == {}, _p.window_strikes(ctl.ledger.rows.values())
    ctl.ledger.close()

    # the operator's stop lands while a pair's two POSTs are on their way, after Predict took both: place_pair's tail
    # withdraws it — free, as the loop would; a side that was not confirmed (the second POST timed out) still counts
    for fail in (0, 2):
        ctl, api, clock = _T.controller([_T.market(at=_T.NOW + 30 * 60_000, mid=501)], PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="60", PREOPEN_SHARES="10")
        api.fail_side = fail
        real = api.create_order

        async def create(body, real=real, api=api, ctl=ctl):
            try:
                return await real(body)
            finally:
                if len(api.sent) == 2:
                    ctl.manual_pause = ctl.ledger.pause(True)  # /preopen pause lands during the round trip
        api.create_order = create
        c = _p.candidate(_T.market(at=_T.NOW + 30 * 60_000, mid=501), _T.NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        await _settled(ctl)
        await _read_finals(ctl, clock)
        pair = ctl.ledger.rows[c["slug"]]
        states = sorted(o["state"] for o in pair["orders"].values())
        assert pair["phase"] != "active" and ctl.manual_pause and len(api.sent) == 2, (pair["phase"], len(api.sent))
        if not fail:
            assert pair.get("accounted") and not _p.counts_against(pair), (states, pair.get("reason"), pair.get("free"))
            assert _p.window_strikes(ctl.ledger.rows.values()) == {}
        else:
            assert "unknown" in states and not pair.get("free") and _p.counts_against(pair), (states, pair.get("free"))
        for task in ctl.settling.values():
            task.cancel()
        await _settled(ctl)
        ctl.ledger.close()


asyncio.run(_operator_stops_are_free())

# PRE-CRASH-WITHDRAW: a crash withdraws the mode's orders (PREOPEN.md, on_stop), but a crash on a ledger it cannot write (a
# full disk, an I/O error) withdrew nothing: on_stop's withdrawal raised at its first write (cancel_many saves every pair
# before it sends a removal, each batch answer is saved inside the withdrawal), contextlib.suppress swallowed it, and not
# one removal went out — every order stayed up until its signed expiry, unmonitored, while LiveBot's crash notice said
# "本模式的挂单已撤" and /preopen said "0 条挂着". adopt() ran outside the guard that calls on_stop, so a failure there
# withdrew nothing either. Now a failed withdrawal falls back to removals that write nothing, adopt() is inside the guard,
# and the notice and the status count what may still rest. The ledger here refuses every write from a moment on.
import sqlite3


def _ledger_full(*args, **kwargs):
    raise sqlite3.OperationalError("database or disk is full")


async def _crash(ctl, then=None):
    """The controller started through LiveBot.preopen_start (whose done-callback sends the crash notice); ``then()`` once it
    runs healthy — adopt() done, every loop going. The crash notice."""
    bot = _L.LiveBot.__new__(_L.LiveBot)
    notes = []
    bot.preopen, bot.preopen_factory, bot.stopping = None, (lambda owner: ctl), ctl.owner.stopping
    bot.live_note_error, bot.live_notify = (lambda text: None), (lambda text, key="": notes.append(text))
    bot.preopen_start()
    if then is not None:
        await asyncio.sleep(0.2)
        assert not bot.preopen_task.done(), "the mode must start healthy"
        then()
    await asyncio.wait([bot.preopen_task], timeout=10)
    await asyncio.sleep(0)
    assert bot.preopen_task.done() and isinstance(bot.preopen_task.exception(), sqlite3.OperationalError), bot.preopen_task
    crash = [n for n in notes if n.startswith("⚠️ 开盘前预挂模式异常停止")]
    assert len(crash) == 1 and not ctl.alive, notes
    return crash[0]


def _resting(ctl, api):
    """The mode's orders still OPEN on the fake book, their signed expiry ahead."""
    expiry = {o["id"]: int(x["expires"]) * 1000 for x in ctl.ledger.rows.values() for o in x["orders"].values() if o.get("id")}
    return sorted(r["id"] for r in api.rows.values() if r["status"] == "OPEN" and expiry.get(r["id"], 0) > ctl.now())


async def _crash_withdraws():
    rows = [_T.market(at=_T.NOW + k * 15 * 60_000, mid=200 + k) for k in (1, 2, 3)]

    async def three():  # three windows quoted, six orders resting
        ctl, api, clock = _T.controller(rows, PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="1440", PREOPEN_SHARES="10")
        for r in rows:
            await ctl.place_pair(_p.candidate(r, _T.NOW, ctl.owner.config, ctl.settings))
        assert len(_resting(ctl, api)) == 6
        return ctl, api, clock

    # the ledger stops taking writes, then the soonest window reaches its cancel time: the cancellation loop crashes on
    # its first save (the path a full disk takes), and the crash withdraws all six orders
    ctl, api, clock = await three()

    def fail():
        ctl.ledger.save = ctl.ledger.save_many = _ledger_full
        clock.ms = min(x["cancel_ms"] for x in ctl.ledger.rows.values())
    note = await _crash(ctl, fail)
    assert _resting(ctl, api) == [], f"a crash on a ledger it cannot write left {_resting(ctl, api)} resting (removals {api.removed})"
    assert "本模式的挂单已撤" in note, note
    assert any("改为不写记录直接撤" in e for _, e in ctl.errors), "the failed withdrawal must be recorded, not swallowed"
    assert "还可能挂着 0 张" in ctl.status()
    # a book bigger than one round of batches (200 orders): all of it, not just the first answers' worth
    ctl, api, clock = _T.controller()
    pairs = await _T.PreopenTests.book_of(None, ctl, api, 100)

    def fail():
        ctl.ledger.save = ctl.ledger.save_many = _ledger_full
        clock.ms = pairs[0]["cancel_ms"]
    await _crash(ctl, fail)
    assert _resting(ctl, api) == [], f"{len(_resting(ctl, api))} of 200 orders left resting"
    # every removal refused as well (401): the notice and the status say what may still rest, never "已撤"
    ctl, api, clock = await three()

    async def refused(ids):
        raise _m.RemoteError("HTTP 401: unauthorized")

    def fail():
        ctl.ledger.save = ctl.ledger.save_many = _ledger_full
        api.remove_orders = refused
        clock.ms = min(x["cancel_ms"] for x in ctl.ledger.rows.values())
    note = await _crash(ctl, fail)
    assert len(_resting(ctl, api)) == 6 and "已撤" not in note and "还有 6 张预挂单可能还挂着" in note, note
    assert "还可能挂着 6 张" in ctl.status(), ctl.status()
    # adopt() fails at a start (a window started during the restart: its record must change, the ledger cannot be
    # written): the orders it took over are withdrawn on the way out like any crash's
    ctl, api, clock = await three()
    clock.ms = _T.NOW + 16 * 60_000  # past the first window's start, before the other two's cancel times
    ctl.ledger.save = ctl.ledger.save_many = _ledger_full
    note = await _crash(ctl)
    assert _resting(ctl, api) == [], f"an adopt() failure left {_resting(ctl, api)} resting"
    assert "本模式的挂单已撤" in note, note


asyncio.run(_crash_withdraws())

# PRE-LEDGER-GROWTH: nothing ever left the ledger's working records — one a window, a level and an attempt, for good
# (twenty cards at four levels: thousands a day). Every start loaded them all, the loops walked them all many times a
# second, before_send walked them from the oldest for the pair being sent (the newest) four times a pair, and with the
# mode off the positions check read the whole file twice a minute: at 50,000 records seconds-long freezes of the bot's
# one event loop, a quarter of it gone to the walks, half a gigabyte of memory, all growing. Now a finished quote that
# filled nothing moves to the ledger's archive two days after its window (same file, the record whole); only the review
# page reads it, and counts it as before. Everything that carries money or a doubt stays a working record.
import json
import shutil

_DAY = 86_400_000
_ENV = dict(PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="1440", PREOPEN_SHARES="10", PREOPEN_LEVELS="48x20", LIVE_MAX_DAILY_LOSS_USD="0")


def _controller_at(now_ms, **env):
    """test_preopen's controller built with the clock already at ``now_ms`` (a restart then: its constructor reads the time)."""
    real = _T.NOW
    _T.NOW = now_ms
    try:
        return _T.controller(**env)
    finally:
        _T.NOW = real


def _tables(db):
    """(the working records' keys, the archive's records by key) as the file holds them."""
    conn = sqlite3.connect(db)
    try:
        pairs = {slug for (slug,) in conn.execute("SELECT slug FROM pairs")}
        archive = {slug: json.loads(body) for slug, body in conn.execute("SELECT slug, body FROM archive")}
    finally:
        conn.close()
    return pairs, archive


async def _at_cancel_time(ctl, clock, pair):
    """A window reaching its cancel time, as the cancellation loop takes it: the active pairs due, withdrawn free."""
    clock.ms = pair["cancel_ms"]
    active = [x for x in ctl.due(clock.ms) if x["phase"] == "active"]
    assert pair in active, pair["slug"]
    await ctl.cancel_many(active, "到时撤单或真实交易暂停", free=True)
    await _settled(ctl)


async def _ledger_growth(folder):
    db = str(Path(folder) / "g.preopen.sqlite3")
    at = lambda minutes: _T.NOW + minutes * 60_000
    mA, mB, mC, mD, mE = (_T.market(at=at(30 + 15 * k), mid=700 + k) for k in range(5))  # 09:30 … 10:30, Friday 11-06
    mU, mR = _T.market(at=at(24 * 60 + 45), mid=710), _T.market(at=at(25 * 60), mid=711)  # Saturday 09:45 and 10:00
    A, B, C, D, E, U, R = (x["categorySlug"] for x in (mA, mB, mC, mD, mE, mU, mR))
    ctl, api, clock = _T.controller([mA, mB, mC], PREOPEN_DB=db, **_ENV)
    for x in (mD, mE, mU, mR):
        api.markets[str(x["id"])] = x
        api.unlisted.add(x["categorySlug"])  # (placed one by one below, not by the discovery)
    alone = lambda market: ctl.place_pair(_p.candidate(market, clock.ms, ctl.owner.config, ctl.settings))  # its first level
    # A, B, C quoted at both levels by the discovery; C withdrawn as a settings change withdraws and quoted again: its
    # first records step aside under '@0'
    assert await _quote_all(ctl) == sorted([A, A + "#1", B, B + "#1", C, C + "#1"])
    await ctl.cancel_many([ctl.ledger.rows[C], ctl.ledger.rows[C + "#1"]], "设置不同，撤掉后重新报价", free=True)
    await _settled(ctl)
    await _read_finals(ctl, clock)
    clock.ms += 30_000
    assert await _quote_all(ctl) == [C, C + "#1"] and {C + "@0", C + "#1@0"} <= set(ctl.ledger.rows)
    # B's first level fills on Up before its cancel; D's second order times out and is never found (given up); E's Down
    # order is one Predict never removes and still lists as open days later
    up = api.rows[ctl.ledger.rows[B]["orders"]["up"]["hash"]]
    up["amountFilled"] = up["amount"]
    api.fail_side = len(api.sent) + 2
    await alone(mD)
    await _settled(ctl)
    await alone(mE)
    api.stuck = {ctl.ledger.rows[E]["orders"]["down"]["id"]}
    for slug in (A, B, C, E):
        await _at_cancel_time(ctl, clock, ctl.ledger.rows[slug])
    clock.ms = at(120)  # 11:00: every window over, past the give-up; Down won B
    for slug in (D, E):
        await ctl.cancel_pair(ctl.ledger.rows[slug], ctl.ledger.rows[slug].get("reason") or "撤单状态核对")
    api.markets[str(mB["id"])]["outcomes"][0]["status"] = "WON"
    await ctl.settle_pair(ctl.ledger.rows[B])
    await _read_finals(ctl, clock)  # (the pairs withdrawn at their cancel times with nothing filled: their records, long since due)
    # Saturday: U's second order times out too, and the process stops before it is given up — in doubt, its money
    # reserved (its withdrawn side read a minute on: nothing filled); R is withdrawn at its cancel time like any other
    clock.ms = ctl.account_at = at(24 * 60 + 30)
    api.fail_side = len(api.sent) + 2
    await alone(mU)
    await _settled(ctl)
    clock.ms = ctl.account_at = clock.ms + 60_000  # (LATE_FILL_MS)
    await ctl.cancel_pair(ctl.ledger.rows[U], ctl.ledger.rows[U].get("reason") or "撤单状态核对")
    await alone(mR)
    await _at_cancel_time(ctl, clock, ctl.ledger.rows[R])
    await _read_finals(ctl, clock, [ctl.ledger.rows[R]])  # (R's records read LATE_FILL_MS on; U's own next try has not come)
    # ...and an earlier attempt at A that ended with nothing filled but with Predict's result on it (the tally reads it)
    res = copy.deepcopy(ctl.ledger.rows[A])
    res.update(slug=A + "@7", attempt=7, result="up")
    for n, order in enumerate(res["orders"].values()):
        order.update(hash=f"0xres{n}", api_hash=f"0xres{n}", id=f"res{n}")
    ctl.ledger.save(res)
    done = lambda x: bool(x.get("accounted")) and x["phase"] == "closed"
    assert [k for k, x in ctl.ledger.rows.items() if not done(x)] == [U], {k: (x["phase"], x.get("accounted")) for k, x in ctl.ledger.rows.items()}
    assert abs(ctl.ledger.rows[B]["pnl"] + 4.9) < 1e-9 and ctl.ledger.rows[B]["result"] == "down"
    assert ctl.ledger.rows[D]["orders"]["down"]["state"] == "not_found" and ctl.ledger.rows[U]["orders"]["down"]["state"] == "unknown"
    assert "仍不是终态" in ctl.ledger.rows[E]["orders"]["down"]["note"] and api.rows[ctl.ledger.rows[E]["orders"]["down"]["hash"]]["status"] == "OPEN"
    ctl.ledger.close()
    old = {A, A + "#1", B + "#1", C + "@0", C + "#1@0", C, C + "#1"}  # finished, nothing filled, no doubt, no result
    kept = {B, D, E, U, R, A + "@7"}  # a fill and its P&L; two orders given up on; one in doubt; a young one; a result
    later = at(25 * 60 + 15) + 2 * _DAY - 30 * 60_000  # Monday 09:45: R's window ended two days less half an hour ago
    for copy_of in ("off", "fail", "half", "last"):
        shutil.copy(db, str(Path(folder) / f"{copy_of}.preopen.sqlite3"))
    # what the code before the fix worked on and showed: every record, off the file
    full = _p.read_rows(db)
    assert {x["slug"] for x in full} == old | kept
    by_slug = {x["slug"]: x for x in full}
    ref = {"review": _p.review(full, later), "fills": _p.fills_by_outcome(full, later, 30), "unverified": _p.unverified_outcomes(full, later),
           "ev": _p.ev_bets(full), "cards": _p.cards_today(full, later)}
    total = ref["review"]["total"]
    assert (total["records"], total["windows"], total["withdrawn"], total["pnl"]) == (13, 7, 11, -4.9)
    assert ref["fills"] == {(str(mB["id"]), 2): 10.0} and ref["unverified"] == {(str(mU["id"]), 1)} and len(ref["ev"]) == 1

    # --- a restart two days after the windows: the old finished quotes are no working records any more
    again, _, clock2 = _controller_at(later, PREOPEN_DB=db, **_ENV)
    again.api = again.cancel_api = api
    api.ctl = again
    assert set(again.ledger.rows) == kept, f"the old finished quotes are still working records: {sorted(set(again.ledger.rows) - kept)}"
    pairs, archive = _tables(db)
    assert pairs == kept and set(archive) == old, (sorted(pairs), sorted(archive))
    assert all(archive[slug] == by_slug[slug] for slug in old), "an archived record is the record as it was, whole"
    assert again.ledger.archived_count == 7 and "6 条（0 条挂着，5 条已结清），另有 7 条已归档" in again.status(), again.status()
    assert set(again.ledger.by_hash) == {o["hash"] for x in again.ledger.rows.values() for o in x["orders"].values()}, "the hash index holds the working records' hashes alone"
    # the review page and the daily summary: as before, the archive counted in (B's first level is a working record, its
    # second an archived one: one window all the same)
    bot = _L.LiveBot.__new__(_L.LiveBot)
    bot.preopen, bot.config, bot.market, bot.sim_trades = again, again.owner.config, again.owner.market, (lambda: {})
    assert bot.preopen_review() == ref["review"], "the review changed with the archive"
    assert [(x["label"], x["records"], x["markets"], round(x["pnl"], 4)) for x in bot.summary_rows([], {})] == [("11-07 周六", 2, 2, 0), ("11-06 周五", 11, 5, -4.9), ("累计", 13, 7, -4.9)]
    # what carries money, a doubt or a result is where it was: the fills the positions check counts and the outcomes it
    # leaves alone, the reservation, the EV bets, today's cards, the tally's rows, the day's P&L in the loss stop, the
    # orders known as this mode's, the listing horizon
    rows = list(again.ledger.rows.values())
    assert (again.filled_by_outcome(30), bot.preopen_filled(), bot.preopen_unverified()) == (ref["fills"], ref["fills"], ref["unverified"])
    assert abs(again.risk() - 9.8) < 1e-9 and (_p.ev_bets(rows), _p.cards_today(rows, later)) == (ref["ev"], ref["cards"])
    card = next(r for r in _m.preopen_cards() if r.key == "BTC-15M")
    starts = {int(x["start_ms"]) for x in full}
    assert bot.tally_fill_rows(card, starts) == [{"s": at(45), "up": 10.0, "down": 0.0, "price": 0.49, "result": "d"}]
    assert bot.tally_official(card, starts) == {at(45): "d", at(30): "u"}
    clock2.ms = at(180)  # (Friday noon again: the day B was settled on)
    assert abs(again.daily_pnl() + 4.9) < 1e-9
    clock2.ms = later
    ids, hashes = again.known()
    assert by_slug[E]["orders"]["down"]["id"] in ids and {by_slug[D]["orders"]["down"]["hash"], by_slug[U]["orders"]["down"]["hash"]} <= hashes
    listed = await api.orders("OPEN")
    assert [r["id"] for r in listed] == [by_slug[E]["orders"]["down"]["id"]] and again.unmanaged_rows(listed) == [], "the order Predict still lists is still this mode's"
    assert (again.listed_until_ms, again.listed_by_card) == (at(25 * 60), {"BTC-15M": at(25 * 60)})

    # --- before_send finds the pair being sent — the newest record — by its hash, without a walk through the records
    mF = _T.market(at=later + 150 * 60_000, mid=720)  # Monday 12:15
    api.markets[str(mF["id"])] = mF
    await again.place_pair(_p.candidate(mF, clock2.ms, again.owner.config, again.settings))
    F = mF["categorySlug"]
    pairF = again.ledger.rows[F]
    assert pairF["phase"] == "active" and [o["state"] for o in pairF["orders"].values()] == ["open", "open"]

    class Walked(dict):
        walks = 0

        def values(self):
            Walked.walks += 1
            return super().values()
    again.ledger.rows = Walked(again.ledger.rows)
    assert again.before_send(pairF["orders"]["up"]["body"]) == "" and again.before_send(pairF["orders"]["down"]["body"]) == ""
    assert Walked.walks == 0, f"before_send walked every record {Walked.walks} times for one pair's two orders"
    assert again.before_send(by_slug[B]["orders"]["up"]["body"]) == "市场已进入撤单阶段，不再发送"  # a closed pair's order: found, refused
    assert again.before_send({"data": {"order": {"hash": "0xnobody"}}}) == "市场已进入撤单阶段，不再发送"  # nobody's: refused as before
    again.ledger.rows = dict(again.ledger.rows)

    # --- an hour on, the loop's own pass takes R, two days over by now; F rests on; U, given up meanwhile, stays (a doubt)
    clock2.ms = later + 3_600_000
    loop = asyncio.create_task(again.cancellation_loop())
    for _ in range(300):
        if R not in again.ledger.rows and again.ledger.rows[U].get("accounted"):
            break
        await asyncio.sleep(0.01)
    again.stop.set()
    await asyncio.gather(loop, return_exceptions=True)
    for task in again.settling.values():
        task.cancel()
    await _settled(again)
    assert not loop.cancelled() and loop.exception() is None, loop
    assert set(again.ledger.rows) == (kept - {R}) | {F} and pairF["phase"] == "active", sorted(again.ledger.rows)
    assert again.ledger.rows[U]["orders"]["down"]["state"] == "not_found" and again.ledger.rows[U]["accounted"]
    assert set(_tables(db)[1]) == old | {R} and again.ledger.archived_count == 8
    everything = lambda: [*again.ledger.rows.values(), *(by_slug[slug] for slug in old | {R})]  # the working records, and the archived ones as they were
    assert bot.preopen_review() == _p.review(everything(), clock2.ms)

    # a pass that cannot be written stops nothing: said, every record where it was
    def full_disk(now_ms):
        raise sqlite3.OperationalError("database or disk is full")
    real_pass, again.ledger.archive_old = again.ledger.archive_old, full_disk
    again.archive(clock2.ms + _DAY)
    assert "旧记录归档失败：database or disk is full" in again.error and len(again.ledger.rows) == 6
    again.ledger.archive_old = real_pass
    after = _p.review(everything(), clock2.ms)
    again.ledger.close()
    assert bot.preopen_review() is None, "an archive that cannot be read (the ledger closed here): no figures rather than short ones"
    # ...the mode off: the review and the fills read off the file, the archive with it
    bot.preopen = None
    assert bot.preopen_review() == after and bot.preopen_filled() == ref["fills"]

    # --- a controller on a ledger that cannot take the move starts all the same, with every record (as before the fix);
    # opening the ledger wrote nothing new — the archive's table is made by the first move — and no archive reads as none
    fail_db = str(Path(folder) / "fail.preopen.sqlite3")
    real_pass = _p.Ledger.archive_old
    _p.Ledger.archive_old = lambda self, now_ms: full_disk(now_ms)
    try:
        hurt, _, _ = _controller_at(later, PREOPEN_DB=fail_db, **_ENV)
    finally:
        _p.Ledger.archive_old = real_pass
    assert set(hurt.ledger.rows) == old | kept and "旧记录归档失败" in hurt.error
    assert not hurt.ledger.has_archive() and hurt.ledger.archived_count == 0 and list(hurt.ledger.archived()) == [] and list(_p.read_archive(fail_db)) == []
    assert "已归档" not in hurt.status()
    hurt.archive(later)
    assert set(hurt.ledger.rows) == kept and hurt.ledger.has_archive() and hurt.ledger.archived_count == 7
    hurt.ledger.close()
    # the move goes a chunk a transaction — in the archive and out of the working records together, or neither: the second
    # chunk's delete fails (a full disk), so the first chunk has moved, the second not at all, and the next pass finishes
    half_db = str(Path(folder) / "half.preopen.sqlite3")
    half = _p.Ledger(half_db)

    class SecondDeleteFails:
        deletes = 0

        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn.__enter__()

        def __exit__(self, *exc):
            return self.conn.__exit__(*exc)

        def execute(self, *args):
            return self.conn.execute(*args)

        def executemany(self, sql, rows):
            if sql.startswith("DELETE"):
                SecondDeleteFails.deletes += 1
                if SecondDeleteFails.deletes == 2:
                    raise sqlite3.OperationalError("database or disk is full")
            return self.conn.executemany(sql, rows)
    real_conn, half.conn = half.conn, SecondDeleteFails(half.conn)
    real_chunk, _p.ARCHIVE_CHUNK = _p.ARCHIVE_CHUNK, 3
    try:
        half.archive_old(later)
        raise AssertionError("the failed chunk must raise")
    except sqlite3.OperationalError:
        pass
    finally:
        half.conn = real_conn
    pairs, archive = _tables(half_db)
    assert len(archive) == 3 and set(archive) < old and half.archived_count == 3, sorted(archive)
    assert pairs == set(half.rows) == (old | kept) - set(archive), "what is in the archive is in the working records no more, on disk and in memory alike"
    try:
        assert half.archive_old(later) == 4 and half.archived_count == 7  # the rest, two chunks
    finally:
        _p.ARCHIVE_CHUNK = real_chunk
    assert _tables(half_db) == (kept, {slug: by_slug[slug] for slug in old}) and set(half.rows) == kept
    half.close()
    # a card's latest window stays whatever its age (R, a month old here): the listing horizon a start reads off the records
    # is as it was
    last, _, _ = _controller_at(later + 30 * _DAY, PREOPEN_DB=str(Path(folder) / "last.preopen.sqlite3"), **_ENV)
    assert set(last.ledger.rows) == kept and (last.listed_until_ms, last.listed_by_card) == (at(25 * 60), {"BTC-15M": at(25 * 60)})
    last.ledger.close()

    # --- the mode off: nobody else would ever move them — the positions check reads that file whole twice a minute
    off_db = str(Path(folder) / "off.preopen.sqlite3")
    clock3 = types.SimpleNamespace(ms=later)
    off = _pnl_bot(clock3, off_db, False, asyncio.Event())
    passes, real_file = [], _p.archive_file
    _p.archive_file = lambda path, now_ms: (passes.append(path), real_file(path, now_ms))[1]
    try:
        await off.preopen_apply()
        await off.preopen_apply()  # (a settings apply a moment later: not again within the hour)
    finally:
        _p.archive_file = real_file
    assert off.preopen is None and passes == [off_db], passes
    assert {x["slug"] for x in _p.read_rows(off_db)} == kept and set(_tables(off_db)[1]) == old
    assert off.preopen_review() == ref["review"] and off.preopen_filled() == ref["fills"] and off.preopen_unverified() == ref["unverified"]
    nowhere = str(Path(folder) / "none.preopen.sqlite3")
    await _pnl_bot(clock3, nowhere, False, asyncio.Event()).preopen_apply()
    assert not Path(nowhere).exists(), "no ledger file is made for a mode that never ran"

    # --- the index answers as the walk did, whatever the records: a hash two records hold, a record renamed, replaced, gone
    led = _p.Ledger(":memory:")
    walk = lambda h: next((x for x in led.rows.values() if any(o.get("hash") == h for o in x["orders"].values())), None)
    row = lambda slug, *hs: {"slug": slug, "phase": "closed", "orders": {f"s{n}": {"hash": h, "state": "final"} for n, h in enumerate(hs)}}
    same = lambda: all(led.holder(h) is walk(h) for h in ("h1", "h2", "h3", "h9", "nobody"))
    led.save(row("a", "h1", "h2"))
    led.save(row("b", "h3"))
    assert same() and led.holder("h1") is led.rows["a"] and led.holder("nobody") is None
    led.save(row("c", "h1"))  # a cloned record: two hold h1 — the first holder, as the walk says
    assert same() and led.holder("h1") is led.rows["a"]
    led.rename("a", "a@0")  # ...which steps aside to the end
    assert same() and led.holder("h1") is led.rows["c"]
    led.delete("c")
    assert same() and led.holder("h1") is led.rows["a@0"]
    led.save(row("b", "h9"))  # a record replaced under its key: its old hash is nobody's
    assert same() and led.holder("h3") is None and led.holder("h9") is led.rows["b"]
    led.save_many([row("d", "h3")])  # ...until another record has it
    assert same() and led.holder("h3") is led.rows["d"]
    led.close()


with tempfile.TemporaryDirectory() as _folder:
    asyncio.run(_ledger_growth(_folder))

# PRE-LATE-FILL: the read made right after a removal ended the order on whatever it showed, and a pair whose two records
# said CANCELLED with nothing filled was accounted on the spot (pnl 0) — after which nothing reads its orders again (due
# and settle_pair pass an accounted pair by, the account snapshot shows open orders alone). That record can lag the
# matching (README 1.119.0: the model trader's order "撤掉、0 成交" by the same GET /orders/{hash}, 299.99 shares on the
# account), so shares matched in the seconds before a withdrawal stayed out of the ledger for good: no P&L, nothing in
# the day's loss stop, the outcome neither in fills_by_outcome nor in unverified_outcomes — the two the model trader's
# positions check goes by (test_trading's preopen_fills_not_adopted: an unverified outcome is left alone, anything else
# beyond the records is booked onto its own ended order on that side) — and a window withdrawn early through no fault of
# its own was quoted again on top of the fill. Now an order off the book is ended only by a read of its record made
# LATE_FILL_MS after it left: "removed" till then — unverified, its pair reserved and not accounted, its window not
# quoted again. The read right after a confirmed removal is not made at all, so a withdrawal costs no request more; a
# pair withdrawn with nothing filled is released a minute on. (A fill a record shows later than that is not covered.)


def _order_reads(api, clock):
    """Every read of an order's own record from here on, as [(when, order id)]."""
    reads, real = [], api.order

    async def order(oid, digest=""):
        reads.append((clock.ms, oid))
        return await real(oid, digest)
    api.order = order
    return reads


async def _until(cond, timeout=5.0):
    for _ in range(int(timeout / 0.01)):
        if cond():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"waited {timeout:g}s")


async def _placed(**env):
    """A controller with the 10:00 BTC window quoted (50 a side at 49¢): (ctl, api, clock, candidate, pair, up, down, reads)."""
    ctl, api, clock = _T.controller(**env)
    reads = _order_reads(api, clock)
    c = _p.candidate(_T.market(), _T.NOW, ctl.owner.config, ctl.settings)
    await ctl.place_pair(c)
    pair = ctl.ledger.rows[c["slug"]]
    return ctl, api, clock, c, pair, pair["orders"]["up"], pair["orders"]["down"], reads


async def _done(ctl):
    for task in ctl.settling.values():
        task.cancel()
    await _settled(ctl)
    ctl.ledger.close()


async def _late_fill(folder):
    shares = lambda n: str(_L.to_wei(n))
    minute = getattr(_p, "LATE_FILL_MS", 60_000)  # (a minute; the name is the fix's)
    # --- (1) the routine cancel, made by the cancellation loop itself. 20 of Up's 50 shares are matched in the last seconds:
    # as the removal is confirmed the record says CANCELLED with nothing filled, and it shows them half a minute later
    db = str(Path(folder) / "late.preopen.sqlite3")
    ctl, api, clock, c, pair, up, down, reads = await _placed(PREOPEN_DB=db)
    k_up, k_down = ("123", up["index_set"]), ("123", down["index_set"])
    bot = _L.LiveBot.__new__(_L.LiveBot)  # the model trader's reading of this mode's records (its positions check goes by these two)
    bot.preopen, bot.config, bot.market = ctl, ctl.owner.config, ctl.owner.market
    assert (bot.preopen_filled(), bot.preopen_unverified()) == ({}, {k_up, k_down})
    at = clock.ms = ctl.account_at = c["cancel_ms"]
    loop = asyncio.create_task(ctl.cancellation_loop())
    try:
        await _until(lambda: pair["phase"] == "closed")
        await asyncio.sleep(0.6)  # (a pass of the loop later)
        assert set(api.removed) == {up["id"], down["id"]}
        assert (up["state"], down["state"]) == ("removed", "removed"), f"ended by a read right after the removal: {up['state']}/{down['state']}"
        assert not pair.get("accounted") and pair.get("pnl") is None, f"accounted on the spot with pnl {pair.get('pnl')}: nothing reads its orders again"
        assert ctl.risk() == 49.0 and not ctl.supersedable(pair) and (pair["retry_ms"], pair["tries"]) == (at + minute, 1)
        assert reads == [], f"a read right after the removal cannot end the order: not made ({reads})"
        assert bot.preopen_unverified() == {k_up, k_down} and bot.preopen_filled() == {}, "the positions check must leave both outcomes alone meanwhile"
        assert _p.unverified_outcomes(_p.read_rows(db), clock.ms) == {k_up, k_down}, "on file too (the mode off: the model trader reads the file)"
        assert f"撤掉的场次在撤单 {minute // 1000} 秒后读到一份没成交才释放" in ctl.status()
        api.rows[up["hash"]]["amountFilled"] = shares(20)  # Predict's record catches up
        clock.ms = at + minute - 1_000
        await asyncio.sleep(0.6)
        assert reads == [] and up["state"] == "removed", "a second short of the minute: not read yet"
        clock.ms = at + minute
        await _until(lambda: up["state"] == "final" and down["state"] == "final")
    finally:
        ctl.stop.set()
        await asyncio.gather(loop, return_exceptions=True)
    assert not loop.cancelled() and loop.exception() is None, loop
    assert (up["filled"], down["filled"], pair.get("accounted")) == (20.0, 0.0, None), "the fill is booked; a pair with a fill waits for the official result"
    assert [(t - at, oid) for t, oid in reads] == [(minute, up["id"]), (minute, down["id"])], f"one read an order, as before — a minute on: {reads}"
    assert (bot.preopen_filled(), bot.preopen_unverified()) == ({k_up: 20.0}, set()), "read: the ledger explains the position"
    clock.ms = c["start_ms"] + 20 * 60_000
    api.markets["123"]["outcomes"][0]["status"] = "WON"  # Down won: the 20 Up shares bought at 49¢ are lost
    await ctl.settle_pair(pair)
    assert pair["accounted"] and pair["result"] == "down" and abs(pair["pnl"] + 9.8) < 1e-9, (pair.get("accounted"), pair.get("pnl"))
    assert abs(ctl.daily_pnl() + 9.8) < 1e-9, "the loss is in the day's figure the loss stop reads"
    await _done(ctl)

    # --- (2) withdrawn early through no fault of its own (a settings change ten minutes before the start), 20 shares matched
    # just before: the window waits for its final reads, and with the fill read it is not quoted again (PREOPEN.md: 有成交的不再报)
    ctl, api, clock, c, pair, up, down, reads = await _placed()
    at = clock.ms = ctl.account_at = c["start_ms"] - 600_000
    await ctl.cancel_many([pair], "设置不同，撤掉后重新报价", free=True)
    await _settled(ctl)
    api.rows[up["hash"]]["amountFilled"] = shares(20)
    ctl.full_scan_ms = 0
    await ctl.discover()
    assert not ctl.supersedable(pair) and ctl.queue.qsize() == 0, "quoted again before its withdrawn orders' records were read"
    assert ctl.scan_state["pending"] == 1 and "1 档在等撤单核对" in ctl.scan_note, ctl.scan_note
    await _read_finals(ctl, clock)
    assert (clock.ms - at, up["state"], up["filled"], down["state"], pair.get("accounted")) == (minute, "final", 20.0, "final", None)
    ctl.full_scan_ms, ctl.account_at = 0, clock.ms
    await ctl.discover()
    assert not ctl.supersedable(pair) and ctl.queue.qsize() == 0 and len(api.sent) == 2, "a window holding a fill was quoted again"
    await _done(ctl)

    # --- (3) nothing filled at all: released, and quoted again, a minute after the removal — one read an order, as before
    ctl, api, clock, c, pair, up, down, reads = await _placed()
    at = clock.ms = ctl.account_at = c["start_ms"] - 600_000
    await ctl.cancel_many([pair], "设置不同，撤掉后重新报价", free=True)
    await _settled(ctl)
    assert (pair.get("accounted"), ctl.risk(), reads, ctl.due(at + minute - 1)) == (None, 49.0, [], [])
    await _read_finals(ctl, clock)
    assert (pair["accounted"], pair["pnl"], ctl.risk(), ctl.supersedable(pair)) == (True, 0.0, 0.0, True)
    assert [(t - at, oid) for t, oid in reads] == [(minute, up["id"]), (minute, down["id"])], reads
    ctl.full_scan_ms, ctl.account_at = 0, clock.ms
    await ctl.discover()
    assert ctl.queue.qsize() == 1 and ctl.queue.get_nowait()[2]["attempt"] == 1, "its window is quoted again once the records say nothing filled"
    await _done(ctl)

    # --- (4) an order that ended outside this mode's cancel (monitor_pair: CANCELLED, nothing filled as yet): the other side is
    # withdrawn there and then, as before — and neither record ends its order before the read a minute on, which shows 15
    ctl, api, clock, c, pair, up, down, reads = await _placed()
    at = clock.ms = ctl.account_at = _T.NOW + 20_000
    ctl.listed_ids = {down["id"]}  # (the fresh snapshot lists Down; Up is gone from it)
    api.rows[up["hash"]]["status"] = "CANCELLED"
    await ctl.monitor_pair(pair)
    await _settled(ctl)
    assert (up["state"], down["state"], pair["phase"], pair.get("accounted"), api.removed) == ("removed", "removed", "closed", None, [down["id"]])
    assert _p.window_strikes(ctl.ledger.rows.values()) == {} and not ctl.supersedable(pair), "counted as a failure and quoted again on one read"
    api.rows[up["hash"]]["amountFilled"] = shares(15)
    await _read_finals(ctl, clock)
    assert (up["state"], up["filled"], down["state"], pair.get("accounted"), ctl.supersedable(pair)) == ("final", 15.0, "final", None, False)
    assert [(t - at, oid) for t, oid in reads] == [(0, up["id"]), (minute, up["id"]), (minute, down["id"])], reads  # (the one read more: Up's, seen ended)
    assert _p.fills_by_outcome(list(ctl.ledger.rows.values()), clock.ms) == {("123", up["index_set"]): 15.0}
    await _done(ctl)

    # --- (5) ...ended outside with part of it filled, the pair resting on: the same monitor reads it again a minute later
    ctl, api, clock, c, pair, up, down, reads = await _placed()
    at = clock.ms = ctl.account_at = _T.NOW + 20_000
    ctl.listed_ids = {down["id"]}
    api.rows[up["hash"]].update(status="CANCELLED", amountFilled=shares(4))
    await ctl.monitor_pair(pair)
    assert (up["state"], up["filled"], pair["phase"], api.removed) == ("removed", 4.0, "active", [])
    assert ("123", up["index_set"]) in _p.unverified_outcomes(list(ctl.ledger.rows.values()), clock.ms)
    api.rows[up["hash"]]["amountFilled"] = shares(6)
    clock.ms = ctl.account_at = at + minute - 6_000
    await ctl.monitor_pair(pair)
    assert (up["state"], up["filled"], len(reads)) == ("removed", 4.0, 1), "not read again before the minute is up"
    clock.ms = ctl.account_at = at + minute
    await ctl.monitor_pair(pair)
    assert (up["state"], up["filled"], pair["phase"], len(reads), api.removed) == ("final", 6.0, "active", 2, [])
    await _done(ctl)

    # --- (6) the whole order filled: nothing more can show, its record is its last word at once (a removal answers "noop")
    ctl, api, clock, c, pair, up, down, reads = await _placed()
    api.rows[up["hash"]].update(status="FILLED", amountFilled=api.rows[up["hash"]]["amount"])
    api.stuck = {up["id"]}
    clock.ms = ctl.account_at = c["cancel_ms"]
    await ctl.cancel_many([pair], "到时撤单或真实交易暂停", free=True)
    await _settled(ctl)
    assert (up["state"], up["filled"], down["state"], pair.get("accounted")) == ("final", 50.0, "removed", None)
    await _done(ctl)

    # --- (7) the process stops inside the minute: the withdrawn orders stay unverified on file, and the next controller reads
    # them when the minute is up (the time they left the book is on the record) — the fill is booked
    db = str(Path(folder) / "restart.preopen.sqlite3")
    ctl, api, clock, c, pair, up, down, reads = await _placed(PREOPEN_DB=db)
    at = clock.ms = ctl.account_at = c["cancel_ms"]
    await ctl.cancel_many([pair], "到时撤单或真实交易暂停", free=True)
    await _done(ctl)
    api.rows[up["hash"]]["amountFilled"] = shares(20)
    assert _p.unverified_outcomes(_p.read_rows(db), at + 10_000) == {("123", 1), ("123", 2)} and _p.fills_by_outcome(_p.read_rows(db), at + 10_000) == {}
    again, _, clock2 = _controller_at(at + 10_000, PREOPEN_DB=db)
    again.api = again.cancel_api = api
    api.ctl = again
    again.adopt()
    pair = again.ledger.rows[c["slug"]]
    assert (pair["phase"], pair.get("accounted"), again.due(at + minute - 1), [x["slug"] for x in again.due(at + minute)]) == ("closed", None, [], [c["slug"]])
    await _read_finals(again, clock2)
    assert (clock2.ms, pair["orders"]["up"]["state"], pair["orders"]["up"]["filled"], pair["orders"]["down"]["state"], pair.get("accounted")) == (at + minute, "final", 20.0, "final", None)
    await _done(again)

    # --- (8) a record an older version left "removed" with no time on it (its read in flight at the stop): read at the next
    # try as before, and ended by the read a minute after that one; long past the window any read ends an order
    ctl, api, clock, c, pair, up, down, reads = await _placed()
    at = clock.ms = ctl.account_at = c["start_ms"] - 600_000
    await ctl.cancel_many([pair], "设置不同，撤掉后重新报价", free=True)
    await _settled(ctl)
    for order in (up, down):
        del order["gone_ms"]
    pair["retry_ms"] = 0
    await _read_finals(ctl, clock)
    assert (clock.ms, len(reads), up["state"], down["state"], pair.get("accounted"), pair["retry_ms"]) == (at, 2, "removed", "removed", None, at + minute)
    await _read_finals(ctl, clock)
    assert (clock.ms, len(reads), up["state"], down["state"], pair["accounted"]) == (at + minute, 4, "final", "final", True)
    assert ctl.final_read_ms({"gone_ms": c["start_ms"] + _p.GIVE_UP_MS - 1}, pair) > 0
    clock.ms = c["start_ms"] + _p.GIVE_UP_MS
    assert ctl.final_read_ms({"gone_ms": clock.ms}, pair) == 0, "past the give-up nothing waits"
    await _done(ctl)


with tempfile.TemporaryDirectory() as _folder:
    asyncio.run(_late_fill(_folder))

# PRE-LEDGER-GROWTH, the follow-up (the review of 1e0d641). With the old finished quotes moved to the archive the review
# page still counts them all, and LiveBot.preopen_review did so by reading and parsing every archived record off the file
# at every call — two calls a build of journal.json, a build a minute while the page is open, all of it on the bot's
# event loop: with the mode on a call took twice as long as it had with the same records in memory (2.0 s against 1.0 s
# at 42,000 archived), and longer with every day of history — the one cost of the ledger left that grew without bound.
# Now the archive is read once and its share of every table kept as counts (preopen.ArchiveShare: an archived record is a
# finished quote that filled nothing — a record, a withdrawn one, its window, an order sent on a side); a call reads the
# rows moved in since the last one, none when there are none, and the page's figures are the same to the byte. The
# archive is read whole again only when its rows did not only grow, and record by record, as before, when it holds
# something that is no plain withdrawn quote.


class _Parses:
    """preopen's json with its loads counted: how many records a call read and parsed off the ledger file."""
    def __init__(self):
        self.n = 0

    def __getattr__(self, name):
        return getattr(json, name)

    def loads(self, text, *args, **kwargs):
        self.n += 1
        return json.loads(text, *args, **kwargs)


def _old_quote(stem, card, start, level=0, attempt=None, **over):
    """A finished quote that filled nothing, as the ledger keeps one (what the review and the archive read of it)."""
    slug = f"{stem}-{start // 1000}" + (f"#{level}" if level else "") + (f"@{attempt}" if attempt is not None else "")
    order = lambda side: {"hash": f"0x{slug}/{side}", "id": f"{slug}/{side}", "state": "final", "filled": 0.0}
    row = {"slug": slug, "key": stem.split("-")[0].upper() + "-15M", "market_id": str(start // 1000), "name": f"{card} 15分涨跌 {_p.hms(start)[:5]}",
           "start_ms": start, "end_ms": start + 900_000, "level": level, "attempt": attempt or 0, "price": ("0.49", "0.48")[level], "shares": ("10", "20")[level],
           "created_ms": start - 3_600_000, "cancel_sent_ms": start - 20_000, "cancel_done_ms": start - 19_600, "phase": "closed", "accounted": True, "pnl": 0.0,
           "orders": {"up": order("up"), "down": order("down")}}
    row.update(over)
    return row


async def _review_by_counts(folder):
    db = str(Path(folder) / "r.preopen.sqlite3")
    day = 24 * 60
    at = lambda minutes: _T.NOW + minutes * 60_000  # (_T.NOW: Friday 11-06 09:00 in Beijing)
    btc = lambda minutes, *args, **over: _old_quote("btc-updown-15m", "BTC", at(minutes), *args, **over)
    uni = lambda minutes, *args, **over: _old_quote("unitree-updown-15m", "宇树", at(minutes), *args, **over)
    filled = btc(60)  # Friday 10:00, the first level: Up filled whole, Down won
    filled["orders"]["up"]["filled"] = 10.0
    filled.update(result="down", pnl=-4.9)
    one_side = uni(240)  # 宇树 Friday 13:00: one order sent, the other never
    one_side["orders"]["down"]["state"] = "not_sent"
    never = btc(120)  # Friday 11:00: no order of it ever placed — no quote to the review
    never["orders"]["up"]["state"], never["orders"]["down"]["state"] = "not_sent", "rejected"
    closing = btc(150, 1, phase="closing", accounted=False)  # Friday 11:30, the second level: still being withdrawn on Monday
    resting = btc(5 * day, phase="active", accounted=False)  # Wednesday 09:00: resting (BTC's latest window)
    for row, state in ((closing, "removed"), (resting, "open")):
        for order in row["orders"].values():
            order["state"] = state
    led = _p.Ledger(db)
    for row in (filled, btc(60, 1),                       # ...and that window's second level: nothing filled
                btc(75, attempt=0), btc(75), btc(75, 1),  # 10:15: quoted twice at the first level, once at the second
                one_side, never, btc(150), closing,       # (11:30: the first level withdrawn)
                btc(day + 45),                            # Saturday 09:45: the weekend
                btc(3 * day + 60), uni(3 * day + 240),    # Monday 10:00; 宇树's latest window, Monday 13:00
                resting):
        led.save(row)
    later = at(3 * day + 180)  # Monday 12:00: Friday's and Saturday's windows are two days over
    assert led.archive_old(later) == 8 and len(led.rows) == 5  # (the fill, the level being withdrawn, Monday's two, the resting one stay)
    clock = types.SimpleNamespace(ms=later)
    bot = _L.LiveBot.__new__(_L.LiveBot)
    bot.preopen, bot.market, bot.sim_trades = types.SimpleNamespace(ledger=led, manual_pause=False), types.SimpleNamespace(now_ms=lambda: clock.ms), (lambda: {})
    bot.config, bot.live = types.SimpleNamespace(preopen=types.SimpleNamespace(db=db)), types.SimpleNamespace(markets={})

    def archived(path=db):
        conn = sqlite3.connect(path)
        try:
            return [json.loads(body) for (body,) in conn.execute("SELECT body FROM archive ORDER BY rowid")]
        finally:
            conn.close()

    def want(working):
        """The page as it was before the follow-up: the review of every record — the working ones, then the archived ones that
        are no working records too — as JSON."""
        keys = {x["slug"] for x in working}
        return json.dumps(_p.review([*working, *(x for x in archived() if x["slug"] not in keys)], clock.ms), ensure_ascii=False)

    def look():
        """(the page's section as JSON, how many records the call read and parsed off the file)"""
        before = parses.n
        out = bot.preopen_review()
        return json.dumps(out, ensure_ascii=False), parses.n - before
    parses = _p.json = _Parses()
    try:
        # --- the archive is read once: a call with nothing new in it reads none of it (it read all eight records, every call)
        got, read = look()
        assert got == want(list(led.rows.values())) and read == 8, read
        again, read = look()
        assert read == 0, f"a call with nothing new in the archive read and parsed the whole of it again ({read} records)"
        assert again == got
        before = parses.n
        summary = bot.summary_rows([], {})  # a build of journal.json: the daily summary's call, and the analysis's own
        bot.preopen_review()
        assert parses.n == before, "a build of journal.json read the archive"
        page = json.loads(got)
        total = page["total"]
        assert (total["records"], total["windows"], total["withdrawn"], total["resting"], total["pnl"]) == (12, 8, 9, 1, -4.9)  # (10:00 and 11:30: a level here, a level there — one window each)
        assert [(x["label"], x["records"], x["markets"]) for x in summary] == [("11-09 周一", 2, 2), ("11-07 周六", 1, 1), ("11-06 周五", 8, 4), ("累计", 11, 7)]
        assert [(x["name"], x["records"], x["windows"]) for x in page["cards"]] == [("BTC 15分涨跌", 10, 6), ("宇树 15分涨跌", 2, 2)]
        assert [(x["name"], x["records"]) for x in page["sides"]] == [("涨", 12), ("跌", 11)]
        assert [(x["records"], x["filled"]) for x in page["weekly"]] == [(1, 0.0), (11, 10.0)] and list(page["daily"]["days"]) == ["2026-11-06", "2026-11-09", "2026-11-07"]
        share = bot.preopen_share
        assert set(share.known) == {_p.window_of(x) for x in led.rows.values()} and len(share.known) == 5, "of the windows, the working records' alone are kept"

        # --- two days on the 11:30 window's second level is withdrawn too and Monday's window is two days over: the pass
        # moves the two, and the page's next call reads those two rows and no other
        clock.ms = at(5 * day + 360)  # Wednesday 15:00
        for order in closing["orders"].values():
            order["state"] = "final"
        closing.update(phase="closed", accounted=True)
        led.save(closing)
        assert led.archive_old(clock.ms) == 2
        got, read = look()
        assert got == want(list(led.rows.values())) and read == 2, read
        assert (json.loads(got)["total"]["records"], json.loads(got)["total"]["windows"], len(bot.preopen_share.known)) == (12, 8, 3)
        # a window quoted and archived between two looks at the page, and one more record of a window all of whose records
        # are archived (nothing is kept of such a window: its three archived records are found by their keys)
        led.save(btc(3 * day + 90))  # Monday 10:30
        led.save(btc(75, attempt=1))
        assert led.archive_old(clock.ms) == 2
        got, read = look()
        assert got == want(list(led.rows.values())) and read == 2 + 3, read
        assert (json.loads(got)["total"]["records"], json.loads(got)["total"]["windows"]) == (14, 9)

        # --- a record saved again after its move by a task that still held it. A settlement in flight saved it with the
        # result: a working record for good, its old copy in the archive — one record to the page; the archive is read
        # whole for it, once
        count = len(archived())
        saturday = next(x for x in archived() if x["start_ms"] == at(day + 45))
        led.save({**saturday, "result": "up"})
        got, read = look()
        assert got == want(list(led.rows.values())) and (read, bot.preopen_share.skip, count) == (12, {saturday["slug"]}, 12), read
        assert look() == (got, 0)
        # ...a reconcile saved one with an order's state read anew: the next pass puts it back, in place of the old copy —
        # the page looked at in between (the record a working one for the while), and not
        back = copy.deepcopy(next(x for x in archived() if x["key"] == "UNITREE-15M"))
        back["orders"]["down"]["state"] = "final"
        led.save(back)
        assert look()[0] == want(list(led.rows.values()))
        rewrites = _p.Ledger.rewrites
        assert led.archive_old(clock.ms) == 1 and _p.Ledger.rewrites == rewrites + 1 and len(archived()) == count
        got, read = look()
        assert got == want(list(led.rows.values())) and read == count, "a record replaced in the archive: the counts are the new copy's"
        assert [(x["name"], x["records"]) for x in json.loads(got)["sides"]] == [("涨", 14), ("跌", 14)]
        back = copy.deepcopy(next(x for x in archived() if x["slug"] == btc(60, 1)["slug"]))
        back["orders"]["up"]["state"] = "not_sent"
        led.save(back)
        assert led.archive_old(clock.ms) == 1 and _p.Ledger.rewrites == rewrites + 2 and len(archived()) == count
        got, read = look()
        assert got == want(list(led.rows.values())) and read == count, "a record replaced in the archive between two looks: the counts are the new copy's"
        assert [(x["name"], x["records"]) for x in json.loads(got)["sides"]] == [("涨", 13), ("跌", 14)]

        # --- the mode off: the working records come off the file at every call, as they did; of the archive nothing is read
        working = len(led.rows)
        bot.preopen = None
        led.close()
        got, read = look()
        assert got == want(_p.read_rows(db)) and (read, working) == (4, 4), read
        assert look() == (got, working)
        # something in the archive that is no plain withdrawn quote (put there by another hand): counts cannot stand for
        # it — the archive is read record by record, as before the follow-up; gone again, the counts are read afresh
        odd = btc(-day)
        odd["orders"]["up"]["filled"] = 3.0
        odd.update(result="up", pnl=1.53)
        conn = sqlite3.connect(db)
        with conn:
            conn.execute("INSERT INTO archive VALUES (?,?)", (odd["slug"], json.dumps(odd, ensure_ascii=False)))
        got, read = look()
        assert got == want(_p.read_rows(db)) and not bot.preopen_share.plain and read >= working + count + 1, read
        assert json.loads(got)["total"]["filled"] == 13.0
        with conn:
            conn.execute("DELETE FROM archive WHERE slug=?", (odd["slug"],))
        got, read = look()
        assert got == want(_p.read_rows(db)) and bot.preopen_share.plain and read == working + count, read
        assert look() == (got, working)
        # ...that hand takes a row out of the archive (the rows no longer add up to the count: read whole); then its last row
        # out and two in — one row more than was counted and one row after the last one counted, as if a pass had moved
        # one in, but the last row counted is not what it was: read whole
        with conn:
            conn.execute("DELETE FROM archive WHERE rowid=(SELECT MIN(rowid) FROM archive)")
        count -= 1
        got, read = look()
        assert got == want(_p.read_rows(db)) and read == working + count, read
        with conn:
            conn.execute("DELETE FROM archive WHERE rowid=(SELECT MAX(rowid) FROM archive)")
            for row in (btc(-day + 15), btc(-day + 30)):
                conn.execute("INSERT INTO archive VALUES (?,?)", (row["slug"], json.dumps(row, ensure_ascii=False)))
        conn.close()
        count += 1
        got, read = look()
        assert got == want(_p.read_rows(db)) and read == working + count, read
        assert look() == (got, working)
        # the clock set back before an archived window's day (the daily summary takes today's and earlier): record by record
        clock.ms = at(-day)
        got, read = look()
        assert got == want(_p.read_rows(db)) and not bot.preopen_share.fits(clock.ms) and read == working + count, read
        clock.ms = at(5 * day + 360)

        # --- the two readers of the working records in live.py that find an archived record no more (Ledger.archive_old's
        # own words say so, and why neither is missed): /live cancel <an archived record's order id> is nobody's order and
        # pauses nothing; a market whose records are all archived is none of this bot's markets
        doc = _p.Ledger.archive_old.__doc__
        assert "live_cancel_command" in doc and "live_known_markets" in doc, "archive_old does not say which readers of the working records no longer find an archived record"
        led = _p.Ledger(db)
        bot.preopen = types.SimpleNamespace(ledger=led, manual_pause=False)
        gone = next(x for x in archived() if x["key"] == "UNITREE-15M")
        text = await bot.live_cancel_command([gone["orders"]["up"]["id"]], clock.ms)
        assert text.startswith("没有订单号为") and not led.pause() and bot.preopen.manual_pause is False, text
        ids, _ = bot.live_known_markets()
        assert gone["market_id"] not in ids and filled["market_id"] in ids
        assert look() == (want(list(led.rows.values())), 0)  # (the mode on again, on the same file: the kept counts go on)
        led.close()

        # --- a window's archived records are found by their keys — <window>, <window>#<level>, either with @<attempt> —
        # and no other window's: a key that only begins the same way is another window's (window_of)
        led = _p.Ledger(str(Path(folder) / "k.preopen.sqlite3"))
        w = "btc-updown-15m-1"
        own = [btc(60, slug=w), btc(60, 1, slug=w + "#1"), btc(60, 1, slug=w + "#1@0"), btc(60, slug=w + "@2")]  # Friday's
        others = [btc(day + 60, slug=w + "0"), btc(day + 60, 1, slug=w + "0#1"), btc(day + 60, slug=w + "A"), btc(day + 60, slug=w + "$"), btc(day + 60, slug=w[:-1])]  # Saturday's
        for row in (*own, *others, btc(5 * day)):
            led.save(row)
        assert led.archive_old(clock.ms) == 9
        share = _p.archive_share(led, list(led.rows.values()))
        assert w not in share.known and (share.lines["all"][""].records, share.lines["all"][""].windows) == (9, 5)
        share.learn(led.conn, w)
        friday, saturday = (share.lines["date"][_p.quote_of(x)["date"]] for x in (own[0], others[0]))
        assert friday in share.known[w] and saturday not in share.known[w] and len(share.known[w]) == 8, "the lines of its own four records: the total, a date, a day, a card, two levels, an hour, the week"

        def both():
            """(the review by the archive's counts, the review of every record) of this ledger, as JSON"""
            working = list(led.rows.values())
            by_counts = _p.review(working, clock.ms, _p.archive_share(led, working, share))
            return json.dumps(by_counts, ensure_ascii=False), json.dumps(_p.review([*working, *archived(led.path)], clock.ms), ensure_ascii=False)
        by_counts, every = both()  # one card in this ledger: its hours stand without its name, the archive's too
        assert by_counts == every and [x["name"] for x in json.loads(by_counts)["hours"]] == ["09 时开始", "10 时开始"]
        led.save(uni(5 * day))  # a second card with more working records than the first — which has more records, the archived counted
        led.save(uni(5 * day + 15))
        by_counts, every = both()
        assert by_counts == every and [(x["name"], x["records"]) for x in json.loads(by_counts)["cards"]] == [("BTC 15分涨跌", 10), ("宇树 15分涨跌", 2)]
        led.delete(btc(5 * day)["slug"])  # ...and the first card with no working record at all: two cards all the same
        by_counts, every = both()
        assert by_counts == every and [x["name"] for x in json.loads(by_counts)["hours"]] == ["BTC 15分涨跌 10 时开始", "宇树 15分涨跌 09 时开始"]
        led.close()
    finally:
        _p.json = json


with tempfile.TemporaryDirectory() as _folder:
    asyncio.run(_review_by_counts(_folder))

print("AUDIT_PREOPEN_OK")
