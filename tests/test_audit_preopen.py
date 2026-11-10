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
_doc = (ROOT / "PREOPEN.md").read_text(encoding="utf-8")
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

print("AUDIT_PREOPEN_OK")
