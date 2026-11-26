"""1.154.53 — the pre-open archive is never read whole on the event loop.

/diag of 11-26 08:32 (v1.154.51, 8 hours up): 主循环最长 20783 ms，卡在 preopen.read ← preopen.archive_share ←
live.preopen_review_build. The review counts the archived records (finished quotes that filled nothing: 187,104 of them,
some 14,000 more a day) by counts kept in memory, and after a start the first call made those by reading the archive
whole — on the bot's one event loop, whatever asked first: the page, /journal, /summary, /export. Twenty seconds in which
no book was read and no order sent or withdrawn.

Here: the work in steps (preopen.ShareWork) is the one read it was, to the figure; a call on the event loop reads only
what is little and says ShareBehind before it reads anything else; the counts are kept in the ledger's file and stand
after a restart (not once a row of the archive was replaced in place: the file's generation). The real trader's bot
(LiveBot.preopen_share_keeper): the working records alone and a note while a large archive is counted in the
background — a step at a time, not while the rule taker goes first — then the exact figures; a restart reads only the
records of the windows that have a working one too; an hour's pass is read on the caller's own turn, or by the
keeper's own look when nobody asks; a row replaced, a pass, a closed ledger, a stop and a failure under a round; the
note's words for each state and /preopen's line; the same with the mode off (the ledger's file by its path); a small
archive and a ledger in memory as they always were; a bot nothing runs in the background of. And the exports: /export 全部
makes the review once for all its files (its own task's alone), /summary and /export 预挂 say the note. Entirely offline.

Run: python tests/test_share.py
"""
import asyncio, contextlib, contextvars, copy, json, pathlib, sqlite3, sys, tempfile, time, types
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L
import preopen as P

BJ = lambda *a: int(m.dt.datetime(2026, *a, tzinfo=m.BEIJING).timestamp() * 1000)
NOW = BJ(11, 26, 8, 30)  # a Thursday morning
DAY = m.DAY_MS
LEVELS = [(0.47, 30.0), (0.43, 160.0), (0.40, 160.0), (0.37, 160.0)]
CARDS = [("BTC-5M", "btc-updown-5m-", 300_000, "BTC 5分涨跌"), ("ETH-15M", "eth-updown-15m-", 900_000, "ETH 15分涨跌"), ("XAU-15M", "xau-updown-15m-", 900_000, "黄金 15分涨跌")]


class Clock:
    def __init__(self, ms): self.ms = ms
    def now_ms(self): return self.ms


def pair(card, start, level, state="final", phase="closed", accounted=True, up=0.0, down=0.0, result="", attempt=1):
    key, prefix, span, name = CARDS[card]
    price, shares = LEVELS[level]
    row = {"key": key, "name": f"{name} {start}", "slug": f"{prefix}{start // 1000}" + (f"#{level}" if level else "") + f"@{attempt}", "price": price, "shares": shares,
           "start_ms": start, "end_ms": start + span, "phase": phase, "accounted": accounted, "result": result, "attempt": attempt,
           "orders": {"up": {"state": state, "filled": up}, "down": {"state": state, "filled": down}}}
    if accounted and result and (up or down):
        payout = {"up": 1.0, "down": 0.0, "split": 0.5}[result]
        row["pnl"] = round((payout - price) * up + ((1 - payout) - price) * down, 6)
    return row


def build(path, windows=120, parse=None):
    """A ledger at NOW: per card ``windows`` windows of five days ago and before, every level a finished quote that filled
    nothing (the archive's, once a pass has run) — but on every tenth window the first level filled and settled (a working
    record for good: a window with a record in both); yesterday's and today's windows (working: too young to move);
    tomorrow's resting. (the ledger, every record as it was saved)."""
    led = P.Ledger(str(path))
    saved = []
    for card, (_, _, span, _) in enumerate(CARDS):
        for k in range(windows):
            start = NOW - 5 * DAY - k * span
            for level in range(len(LEVELS)):
                filled = level == 0 and k % 10 == 0
                saved.append(pair(card, start, level, up=LEVELS[0][1] if filled else 0.0, result="up" if filled and k % 20 == 0 else "down" if filled else ""))
        for k in range(12):
            for level in range(len(LEVELS)):
                saved.append(pair(card, NOW - DAY - k * span, level))                                          # yesterday's: finished, not two days old
                saved.append(pair(card, NOW + DAY - k * span, level, "open", "active", False))                   # tomorrow's: resting
    led.save_many(saved)
    moved = led.archive_old(NOW)
    return led, saved, moved


def exact(saved, now=NOW):
    """The review of every record there is (the working ones and the archived ones as they were saved)."""
    return dumped(P.review(list(saved), now))


dumped = lambda review: json.dumps({k: v for k, v in (review or {}).items() if k != "archive_note"}, ensure_ascii=False, sort_keys=True, default=str)


def counting():
    """preopen's json.loads counted: how many records (and kept counts) a call parsed."""
    seen = {"n": 0}
    real = json.loads

    class Json:
        dumps, JSONDecodeError = staticmethod(json.dumps), json.JSONDecodeError

        @staticmethod
        def loads(text, *a, **kw):
            seen["n"] += 1
            return real(text, *a, **kw)
    P.json = Json
    return seen


def the_steps_are_the_one_read(folder) -> None:
    led, saved, moved = build(folder / "steps.sqlite3")
    rows = list(led.rows.values())
    # per card 120 old windows × 4 levels; the 12 filled first levels stay (a fill is no history only)
    assert moved == len(CARDS) * (120 * 4 - 12) == led.archived_count == 1404, moved
    whole = P.ArchiveShare.read(led.conn, led.path, rows)
    assert (whole.count, whole.top, whole.plain, whole.fits(NOW)) == (1404, 1404, True, True)
    by_counts = dumped(P.review(rows, NOW, whole))
    assert by_counts == exact(saved), "the counts stand for the archived records"
    for n in (1, 7, 100, 5000):
        work = P.ShareWork(led.conn, led.path, rows)
        assert (work.fresh, work.rows_left, work.windows_left, work.total, work.done, work.stale) == (True, 1404, 0, 1404, False, False)
        steps = 0
        while work.step(n):
            steps += 1
            assert steps < 5000
        assert work.done and work.rows_left == 0 and work.share.count == 1404 and dumped(P.review(rows, NOW, work.share)) == by_counts, n
        assert set(work.share.known) == {P.window_of(p) for p in rows}, "of the windows, the working records' alone are kept"
        assert steps >= 1404 // n, (n, steps)
    # brought up to date in steps too: a pass moves yesterday's windows in two days on, and a window quoted and archived since
    later = NOW + 2 * DAY
    extra = pair(0, NOW - 4 * DAY, 1)
    led.save(extra)
    saved.append(extra)
    assert led.archive_old(later) == len(CARDS) * 12 * 4 + 1 == 145  # (a card's latest window — tomorrow's, resting — is what stays whatever its age)
    rows = list(led.rows.values())
    work = P.ShareWork(led.conn, led.path, rows, whole)
    assert (work.fresh, work.rows_left, work.share is whole) == (False, 145, True)
    while work.step(10):
        pass
    assert dumped(P.review(rows, later, whole)) == exact(saved, later) and whole.count == 1404 + 145
    # 1.154.66: a step cut short by its time (budget_s: none at all here — one row, one window a step) comes to the same counts
    for kept in (None,):
        cut = P.ShareWork(led.conn, led.path, rows, kept)
        steps = 0
        while cut.step(100, budget_s=0.0):
            steps += 1
            assert steps < 100_000
        assert cut.done and dumped(P.review(rows, later, cut.share)) == exact(saved, later), "a step cut short by its time: the same read"
        assert steps >= cut.share.count, (steps, cut.share.count)
    assert dumped(P.review(rows, later, P.ArchiveShare.read(led.conn, led.path, rows, whole))) == exact(saved, later), "and nothing new: as it stands"
    # a row replaced in place while the steps go: said (the work is begun again by whoever runs it)
    work = P.ShareWork(led.conn, led.path, rows, whole)
    assert not work.stale
    P.Ledger.rewrites += 1
    assert work.stale
    P.Ledger.rewrites -= 1
    led.close()
    print("THE_STEPS_ARE_THE_ONE_READ_OK")


def little_on_the_loop(folder) -> None:
    led, saved, _ = build(folder / "loop.sqlite3")
    rows = list(led.rows.values())
    seen = counting()
    try:
        old = P.SHARE_LOOP_ROWS, P.SHARE_LOOP_WINDOWS
        # an archive of more rows than a call may read: said before anything of it is read
        P.SHARE_LOOP_ROWS = 1000
        try:
            P.ArchiveShare.read(led.conn, led.path, rows, None, whole=False)
        except P.ShareBehind as behind:
            assert behind.total == 1404 and "1404 条里还有 1404 条没计数" in str(behind) and seen["n"] == 0, (str(behind), seen)
        else:
            raise AssertionError("the archive was read whole on the caller's turn")
        try:
            P.archive_share(led, rows, None, whole=False)
        except P.ShareBehind:
            pass
        else:
            raise AssertionError("archive_share passes the word on")
        # ...read whole where the caller says so (the default: as before), and little enough is read on the spot
        share = P.archive_share(led, rows)
        assert share.count == 1404 and seen["n"] == 1404
        P.SHARE_LOOP_ROWS = 2000
        seen["n"] = 0
        small = P.ArchiveShare.read(led.conn, led.path, rows, None, whole=False)
        assert small.count == 1404 and seen["n"] == 1404, "an archive within what a call may read: read, as it always was"
        # kept and nothing new: nothing read; a pass's rows: those alone
        seen["n"] = 0
        assert P.ArchiveShare.read(led.conn, led.path, rows, share, whole=False) is share and seen["n"] == 0
        extra = pair(1, NOW - 6 * DAY - 7_000, 2)
        led.save(extra)
        assert led.archive_old(NOW) == 1
        rows = list(led.rows.values())
        assert P.ArchiveShare.read(led.conn, led.path, rows, share, whole=False).count == 1405 and seen["n"] == 1
        # far behind (a share from the file after a long stop), or many windows to look up: the background's
        P.SHARE_LOOP_ROWS = 0
        led.save(pair(1, NOW - 6 * DAY - 9_000, 2))
        led.archive_old(NOW)
        rows = list(led.rows.values())
        for limits in ((0, 10 ** 6), (10 ** 6, 3)):
            P.SHARE_LOOP_ROWS, P.SHARE_LOOP_WINDOWS = limits
            kept = copy.copy(share)
            kept.known = {} if limits[1] == 3 else share.known
            before = (kept.count, kept.top, dict(kept.known))
            seen["n"] = 0
            try:
                P.ArchiveShare.read(led.conn, led.path, rows, kept, whole=False)
            except P.ShareBehind:
                assert (kept.count, kept.top, dict(kept.known)) == before and seen["n"] == 0, "the share in hand is as it was"
            else:
                raise AssertionError(limits)
    finally:
        P.SHARE_LOOP_ROWS, P.SHARE_LOOP_WINDOWS = old
        P.json = json
    led.close()
    print("LITTLE_ON_THE_LOOP_OK")


def kept_in_the_file(folder) -> None:
    db = folder / "kept.sqlite3"
    led, saved, _ = build(db)
    rows = list(led.rows.values())
    share = P.archive_share(led, rows)
    assert P.share_load(led.conn, led.path) is None and P.share_gen(led.conn) == 0
    assert P.share_save(led.conn, share)
    state = json.loads(led.conn.execute("SELECT body FROM settings WHERE key=?", (P.SHARE_KEY,)).fetchone()[0])
    assert (state["v"], state["kinds"], state["gen"], state["top"], state["count"], state["plain"], state["sent"]) == (
        P.SHARE_VERSION, list(P.REVIEW_KINDS), 0, 1404, 1404, True, {"up": 1404, "down": 1404}) and "known" not in state
    led.close()
    # a start: the ledger opened anew, the counts read off its file — every line under the key it had (tuples, a bool, numbers)
    led = P.Ledger(str(db))
    rows = list(led.rows.values())
    back = P.share_load(led.conn, led.path)
    assert back is not None and (back.top, back.count, back.last, back.skip, back.plain, back.sent, back.path) == (share.top, share.count, share.last, share.skip, True, share.sent, led.path)
    for kind in P.REVIEW_KINDS:
        assert {k: (x.records, x.windows, x.latest) for k, x in back.lines[kind].items()} == {k: (x.records, x.windows, x.latest) for k, x in share.lines[kind].items()}, kind
    assert all(type(k) is type(j) for kind in P.REVIEW_KINDS for k, j in zip(sorted(back.lines[kind], key=repr), sorted(share.lines[kind], key=repr)))
    assert (0.47, 30.0) in back.lines["level"] and ("BTC 5分涨跌", 8) in back.lines["hour"] and set(back.lines["week"]) <= {True, False} and back.known == {}
    # brought up to date it reads no record of the archive: the working records' windows are looked up by their keys
    seen = counting()
    try:
        work = P.ShareWork(led.conn, led.path, rows, back)
        assert (work.fresh, work.rows_left, work.windows_left) == (False, 0, len({P.window_of(p) for p in rows}))
        while work.step(50):
            pass
        assert dumped(P.review(rows, NOW, back)) == exact(saved)
        parsed = seen["n"]
        whole = P.ArchiveShare.read(led.conn, led.path, rows)
        # (read by their keys: the archived levels — three each — of the 36 windows that have a working record too, a filled first level)
        assert seen["n"] - parsed == 1404 and parsed == 36 * 3, (parsed, seen["n"])
    finally:
        P.json = json
    # a record of the archive replaced in place by a pass (saved again after its move by a task that still held it): the file's
    # generation goes on with those very rows, and the counts kept before are no more the archive's
    assert P.share_save(led.conn, back) and P.share_load(led.conn, led.path) is not None
    held = next(json.loads(body) for (body,) in led.conn.execute("SELECT body FROM archive LIMIT 1"))
    held["orders"]["down"]["state"] = "not_sent"
    led.save(held)
    rewrites = P.Ledger.rewrites
    assert led.archive_old(NOW) == 1 and P.Ledger.rewrites == rewrites + 1 and P.share_gen(led.conn) == 1
    assert P.share_load(led.conn, led.path) is None, "the counts of an earlier generation were taken for the archive's"
    fresh = P.archive_share(led, list(led.rows.values()))
    assert P.share_save(led.conn, fresh) and P.share_load(led.conn, led.path).count == 1404 and fresh.sent == {"up": 1404, "down": 1403}, fresh.sent
    # what is no state of this code's is none: another version, other tables, a broken row, no row, a file without the table
    for change in ({"v": P.SHARE_VERSION + 1}, {"kinds": ["all"]}, {"lines": {}}, {"top": "x"}, {"sent": {}}):
        assert P.ArchiveShare.restored(led.path, {**fresh.state(1), **change}, 1) is None, change
    assert P.ArchiveShare.restored(led.path, fresh.state(1), 2) is None and P.ArchiveShare.restored(led.path, "x", 1) is None
    assert P.ArchiveShare.restored(led.path, fresh.state(1), 1).count == 1404
    with led.conn:
        led.conn.execute("UPDATE settings SET body='{' WHERE key=?", (P.SHARE_KEY,))
    assert P.share_load(led.conn, led.path) is None
    bare = sqlite3.connect(":memory:")
    assert P.share_load(bare, ":memory:") is None and P.share_gen(bare) == 0 and P.share_save(bare, fresh) is False
    led.close()
    print("KEPT_IN_THE_FILE_OK")


def bot_on(led, clock, **env):
    """The real trader's bot on a controller's ledger ``led`` — or, None, with the mode off: the file PREOPEN_DB names."""
    bot = L.LiveBot.__new__(L.LiveBot)
    bot.preopen_share, bot.preopen_share_away = None, False  # (as its __init__ leaves them)
    bot.preopen = types.SimpleNamespace(ledger=led) if led is not None else None
    bot.config, bot.market = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", **env}), clock
    bot.rules_clear = lambda seconds: True
    bot.PREOPEN_ARCHIVE_WALK = 1000  # (an archive of 1,404 records is a large one here)
    return bot


async def settled(bot, turns=200_000) -> None:
    """Until the keeper has the counts ready (its round over)."""
    for _ in range(turns):
        if getattr(bot, "preopen_share", None) is not None and not bot.preopen_share_away:
            return
        await REAL_SLEEP(0)
    raise AssertionError("the keeper never had the counts ready")


async def until(what, turns=200_000) -> None:
    for _ in range(turns):
        if what():
            return
        await REAL_SLEEP(0)
    raise AssertionError("never came")


async def stopped(*bots) -> None:
    for bot in bots:
        task = getattr(bot, "preopen_share_task", None)
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


NOTE = lambda n, word: (f"归档的 {n:,} 条旧记录（都是没成交、已撤的挂单）{word}：这里暂时只算了账本里的工作记录——"
                        "较早日子的挂出记录数、场次数、已撤数会偏少，成交和盈亏不受影响")
COUNTING, WAITING = "还在后台计数，一两分钟后再看就全了", "要等规则吃单的优先时段过去，才在后台接着计数（计完就全了）"
LATER, NOT_READ, UNFIT = "这次没计入（后台计数上次没做成，5 分钟内会自动再试）", "这次没计入（归档的计数没读成）", "这次没计入（归档里有按条数算不了的记录）"
REAL_SLEEP = asyncio.sleep


class Naps:
    """asyncio.sleep stood in for: what was slept is noted, and the loop has its turn."""
    def __init__(self):
        self.seen: list[float] = []

    async def __call__(self, seconds, *a):
        self.seen.append(seconds)
        await REAL_SLEEP(0)

    def __enter__(self):
        asyncio.sleep = self
        return self.seen

    def __exit__(self, *a):
        asyncio.sleep = REAL_SLEEP


async def the_real_trader(folder) -> None:
    db = folder / "bot.sqlite3"
    led, saved, _ = build(db)
    clock = Clock(NOW)
    old = P.SHARE_LOOP_ROWS, P.SHARE_LOOP_WINDOWS
    P.SHARE_LOOP_ROWS = 1000
    seen = counting()
    bots: list = []
    try:
        with Naps() as naps:
            bot = bot_on(led, clock)
            bots.append(bot)
            # --- the first call after a start, nothing kept: the working records alone, said — and not a record of the archive read
            first = bot.preopen_review()
            working = dumped(P.review(list(led.rows.values()), NOW))
            assert dumped(first) == working and seen["n"] == 0, seen
            assert first["archive_note"] == NOTE(1404, COUNTING), first["archive_note"]
            assert first["total"]["pnl"] == json.loads(exact(saved))["total"]["pnl"] and first["total"]["filled"] == json.loads(exact(saved))["total"]["filled"], "every fill and every P&L is a working record's"
            task = bot.preopen_share_task
            assert task is not None and not task.done() and bot.preopen_share is None and bot.preopen_share_away
            assert bot.preopen_share_line() == "\n归档计数：后台准备中（启动后约半分钟开始，一两分钟内好）；这期间复盘只算工作记录（成交和盈亏不受影响）"
            assert bot.preopen_review()["archive_note"] == first["archive_note"] and bot.preopen_share_task is task and seen["n"] == 0, "asked again meanwhile: the same, one keeper"
            rows, share = bot.preopen_range("按市场")
            assert share is None and len(rows) == len(led.rows) and seen["n"] == 0, "按市场 over a range: its older days short for the while, as the note says"
            # --- the keeper: a step, the loop's turn, a step — then the exact figures, and the counts in the file
            await settled(bot)
            assert bot.preopen_share.count == 1404 and seen["n"] == 1404 and not task.done()
            assert bot.preopen_share_line() == "\n归档计数：已备好（1,404 条；后台每分钟跟进新归档的，复盘不再整读归档）"
            steps = [x for x in naps if 0 < x < 1.0]
            assert len(steps) >= 1404 // P.SHARE_STEP + 1 and min(steps) >= 0.01, (len(steps), min(steps))  # (a hundred rows a step, the loop's turn after each)
            ready = bot.preopen_review()
            assert dumped(ready) == exact(saved) and "archive_note" not in ready and seen["n"] == 1404
            assert bot.preopen_share_saved == (1404, 1404) and P.share_load(led.conn, led.path).count == 1404 and bot.preopen_share_seen == (led.path, 1404)
            seen["n"] = 0  # (share_load's own parse above)
            assert bot.preopen_range("按价格档位")[1] is bot.preopen_share and not bot.preopen_share_due()
            # --- an hour's pass moves two records in. Asked for first: read on the caller's own turn, and the file follows
            for k in (3, 5):
                extra = pair(2, NOW - 7 * DAY - k * 1_000, 3)
                led.save(extra)
                saved.append(extra)
            assert led.archive_old(NOW) == 2 and bot.preopen_share_due()
            assert dumped(bot.preopen_review()) == exact(saved) and seen["n"] == 2 and bot.preopen_share_saved == (1406, 1406) and not bot.preopen_share_due()
            assert json.loads(led.conn.execute("SELECT body FROM settings WHERE key=?", (P.SHARE_KEY,)).fetchone()[0])["count"] == 1406
            # ...and nobody asking: the keeper's own look (every PREOPEN_SHARE_EVERY_S) counts them in, in one piece — the counts are
            # never away for it — and the file follows
            await stopped(bot)
            bot.PREOPEN_SHARE_EVERY_S = 0.02
            bot.preopen_share_start(delay=0.02)
            for k in (7, 9):
                extra = pair(2, NOW - 7 * DAY - k * 1_000, 3)
                led.save(extra)
                saved.append(extra)
            assert led.archive_old(NOW) == 2
            seen["n"], kept = 0, bot.preopen_share
            del naps[:]
            await until(lambda: bot.preopen_share_saved == (1408, 1408))
            assert bot.preopen_share is kept and kept.count == 1408 and seen["n"] == 2 and not naps and not bot.preopen_share_away
            assert dumped(bot.preopen_review()) == exact(saved) and seen["n"] == 2
            await REAL_SLEEP(0.1)  # (several looks on: nothing more to do, nothing done)
            assert seen["n"] == 2 and not naps
            # --- the rule taker goes first: the keeper waits (the note says so), and no step of its falls into those seconds
            bot2 = bot_on(led, clock)
            bots.append(bot2)
            asked: list = []

            def rules_clear(seconds):
                asked.append(seconds)
                return len(asked) > 3
            bot2.rules_clear = rules_clear
            seen["n"] = 0
            bot2.preopen_share_start()
            await until(lambda: naps)
            assert set(naps) == {1.0} and bot2.preopen_share_waiting and seen["n"] == 0
            assert bot2.preopen_review()["archive_note"] == NOTE(1408, WAITING)
            assert bot2.preopen_share_line() == "\n归档计数：后台准备中，在等规则吃单的优先时段过去；这期间复盘只算工作记录（成交和盈亏不受影响）"
            await settled(bot2)
            assert naps.count(1.0) == 3 and set(asked) == {bot2.PREOPEN_SHARE_CLEAR_S} and not bot2.preopen_share_waiting
            assert dumped(bot2.preopen_review()) == exact(saved)
            await stopped(bot, bot2)
            led.close()

            # --- a restart: the counts are in the file — nothing of the archive is read again but the records of the windows that
            # have a working one too (looked up by their keys); and somebody asking cuts the start's delay short
            led = P.Ledger(str(db))
            bot = bot_on(led, clock)
            bots.append(bot)
            seen["n"], began = 0, time.monotonic()
            bot.preopen_share_start(delay=30.0)
            await REAL_SLEEP(0.05)
            assert bot.preopen_share is None and seen["n"] == 0, "the start's own work first: nothing before the delay is over"
            assert bot.preopen_review()["archive_note"] == NOTE(1408, COUNTING), "before the counts are ready: the working records, said"
            await settled(bot)
            assert time.monotonic() - began < 10 and seen["n"] == 1 + 36 * 3, seen  # (the kept counts, and three archived levels of each of 36 windows)
            assert dumped(bot.preopen_review()) == exact(saved) and seen["n"] == 1 + 36 * 3
            await stopped(bot)
            led.close()
            # ...the same where the windows to look up are many (a start with thousands of working records): a step at a time
            P.SHARE_LOOP_WINDOWS = 50
            led = P.Ledger(str(db))
            bot = bot_on(led, clock)
            bots.append(bot)
            seen["n"] = 0
            del naps[:]
            bot.preopen_share_start()
            await until(lambda: bot.preopen_share_away)
            assert bot.preopen_review()["archive_note"] == NOTE(1408, COUNTING)
            await settled(bot)
            assert len(naps) >= 3 and seen["n"] == 1 + 36 * 3 and dumped(bot.preopen_review()) == exact(saved)
            P.SHARE_LOOP_WINDOWS = old[1]

            # --- a record in the archive that counts cannot stand for (put there by another hand): the working records and a note,
            # not a walk of the archive record by record
            odd = pair(0, NOW - 9 * DAY - 1_000, 0, up=3.0, result="up")
            with led.conn:
                led.conn.execute("INSERT INTO archive VALUES (?,?)", (odd["slug"], json.dumps(odd, ensure_ascii=False)))
            led.archived_count += 1
            seen["n"] = 0
            unfit = bot.preopen_review()
            assert seen["n"] == 1 and not bot.preopen_share.plain and unfit["archive_note"] == NOTE(1409, UNFIT), (seen, unfit.get("archive_note"))
            assert dumped(unfit) == dumped(P.review(list(led.rows.values()), NOW))
            with led.conn:
                led.conn.execute("DELETE FROM archive WHERE slug=?", (odd["slug"],))
            led.archived_count -= 1
            # ...the rows no longer what was counted: read whole again — by the keeper (a note meanwhile), then exact
            again = bot.preopen_review()
            assert again["archive_note"] == NOTE(1408, COUNTING) and bot.preopen_share_away
            await settled(bot)
            assert dumped(bot.preopen_review()) == exact(saved) and bot.preopen_share.plain
            await stopped(bot)

            # --- a row of the archive replaced in place while the keeper reads it whole: begun again, and exact
            with led.conn:
                led.conn.execute("DELETE FROM settings WHERE key=?", (P.SHARE_KEY,))
            bot = bot_on(led, clock)
            bots.append(bot)
            seen["n"] = 0
            bot.preopen_share_start()
            await until(lambda: seen["n"] >= 300)
            assert bot.preopen_share_away and seen["n"] < 1408
            held = json.loads(led.conn.execute("SELECT body FROM archive ORDER BY rowid DESC LIMIT 1").fetchone()[0])
            held["orders"]["down"]["state"] = "not_sent"
            led.save(held)
            saved[next(i for i, x in enumerate(saved) if x["slug"] == held["slug"])] = held
            gen = P.share_gen(led.conn)
            assert led.archive_old(NOW) == 1 and P.share_gen(led.conn) == gen + 1
            await settled(bot)
            # (the three steps read before it are let go — no further step of a read gone stale — and the archive is read once more)
            assert seen["n"] == 300 + 1408 and bot.preopen_share.count == 1408 and bot.preopen_share.sent == {"up": 1408, "down": 1407}, (seen, bot.preopen_share.sent)
            assert dumped(bot.preopen_review()) == exact(saved) and P.share_load(led.conn, led.path).sent == {"up": 1408, "down": 1407}
            await stopped(bot)

            # --- a pass moves records in while the keeper reads the archive whole: read with the rest, and the next look sees
            # that the ledger's count moved under the round (nothing left for it to read here)
            with led.conn:
                led.conn.execute("DELETE FROM settings WHERE key=?", (P.SHARE_KEY,))
            bot = bot_on(led, clock)
            bots.append(bot)
            seen["n"] = 0
            bot.preopen_share_start()
            await until(lambda: seen["n"] >= 300)
            for k in (11, 13):
                extra = pair(2, NOW - 7 * DAY - k * 1_000, 3)
                led.save(extra)
                saved.append(extra)
            assert led.archive_old(NOW) == 2
            await settled(bot)
            assert seen["n"] == 1410 and bot.preopen_share.count == 1410 and bot.preopen_share_seen == (led.path, 1408) and bot.preopen_share_due()
            assert dumped(bot.preopen_review()) == exact(saved) and seen["n"] == 1410 and not bot.preopen_share_due()
            await stopped(bot)

            # --- the ledger closed under the steps (the controller restarted): the round goes on with the one there is
            with led.conn:
                led.conn.execute("DELETE FROM settings WHERE key=?", (P.SHARE_KEY,))
            bot = bot_on(led, clock)
            bots.append(bot)
            seen["n"] = 0
            bot.preopen_share_start()
            await until(lambda: seen["n"] >= 300)
            led.close()
            led = P.Ledger(str(db))
            bot.preopen = types.SimpleNamespace(ledger=led)
            await settled(bot)
            assert dumped(bot.preopen_review()) == exact(saved) and bot.preopen_share_saved == (bot.preopen_share.top, 1410)

            # --- stopped in the middle of a round (the bot stops): nothing said to have failed, no counts left half made
            with led.conn:
                led.conn.execute("DELETE FROM settings WHERE key=?", (P.SHARE_KEY,))
            await stopped(bot)
            bot = bot_on(led, clock)
            bots.append(bot)
            seen["n"] = 0
            bot.preopen_share_start()
            await until(lambda: seen["n"] >= 300)
            await stopped(bot)
            assert bot.preopen_share is None and getattr(bot, "preopen_share_failed", -1.0) < 0 and P.share_load(led.conn, led.path) is None

            # --- a round that fails: said once, the keeper lives, the note says when it is tried again — and it is
            bot = bot_on(led, clock)
            bots.append(bot)

            async def boom():
                raise OSError("disk I/O error")
            bot.preopen_share_turn = boom
            m.LOG.disabled = True
            try:
                bot.preopen_share_start()
                await until(lambda: getattr(bot, "preopen_share_failed", -1.0) > 0)
            finally:
                m.LOG.disabled = False
            task = bot.preopen_share_task
            assert bot.preopen_share is None and not task.done()
            seen["n"] = 0
            later = bot.preopen_review()
            assert later["archive_note"] == NOTE(1410, LATER) and dumped(later) == dumped(P.review(list(led.rows.values()), NOW)) and seen["n"] == 0
            assert bot.preopen_share_line() == "\n归档计数：⚠️ 后台上次没做成，5 分钟内自动再试；这期间复盘只算工作记录（成交和盈亏不受影响）"
            assert bot.preopen_review()["archive_note"] == NOTE(1410, LATER) and bot.preopen_share_task is task
            await REAL_SLEEP(0.05)
            assert bot.preopen_share is None and seen["n"] == 0, "not again sooner than PREOPEN_SHARE_RETRY_S"
            del bot.preopen_share_turn
            bot.preopen_share_failed = time.monotonic() - bot.PREOPEN_SHARE_RETRY_S - 1
            assert bot.preopen_review()["archive_note"] == NOTE(1410, COUNTING)
            bot.preopen_share_wanted.set()  # (the keeper's next look, now)
            await settled(bot)
            assert dumped(bot.preopen_review()) == exact(saved)
            # --- the counts could not be had on the caller's own turn: a large archive is not walked for it either
            real = bot.preopen_share_now

            def broken(rows):
                raise sqlite3.OperationalError("database is locked")
            bot.preopen_share_now = broken
            seen["n"] = 0
            m.LOG.disabled = True
            try:
                got = bot.preopen_review()
                assert got["archive_note"] == NOTE(1410, NOT_READ) and seen["n"] == 0 and bot.preopen_share is None
                # ...nor an archive of a size nobody can say
                bot.preopen = types.SimpleNamespace(ledger=types.SimpleNamespace(conn=led.conn, path=led.path, rows=led.rows, archived=led.archived))
                got = bot.preopen_review()
                assert got["archive_note"].startswith("归档的旧记录（都是没成交、已撤的挂单）" + NOT_READ + "：") and seen["n"] == 0
            finally:
                m.LOG.disabled = False
            bot.preopen, bot.preopen_share_now = types.SimpleNamespace(ledger=led), real
            assert bot.preopen_share_due(), "dropped: the keeper's to make again at its next look"
            assert bot.preopen_review()["archive_note"] == NOTE(1410, COUNTING)  # (...or at once, when somebody asks)
            await settled(bot)
            assert dumped(bot.preopen_review()) == exact(saved)
            await stopped(*bots)
            led.close()
    finally:
        P.SHARE_LOOP_ROWS, P.SHARE_LOOP_WINDOWS = old
        P.json = json
    print("THE_REAL_TRADER_OK")


async def the_mode_off(folder) -> None:
    """No controller on the ledger's file (the mode off): the same by the file's path — its working records read off it at
    each call, as before; the archive never whole on a caller's turn."""
    db = folder / "off.sqlite3"
    led, saved, _ = build(db)
    working = len(led.rows)
    led.close()
    clock = Clock(NOW)
    old = P.SHARE_LOOP_ROWS
    P.SHARE_LOOP_ROWS = 1000
    seen = counting()
    try:
        with Naps():
            bot = bot_on(None, clock, PREOPEN_DB=str(db))
            first = bot.preopen_review()
            assert first["archive_note"] == NOTE(1404, COUNTING) and seen["n"] == working == 324, seen  # (the file's working records; none of its archive)
            await settled(bot)
            assert seen["n"] == 2 * working + 1404 and bot.preopen_share.count == 1404 and bot.preopen_share_saved == (1404, 1404) and bot.preopen_share_seen is None
            assert dumped(bot.preopen_review()) == exact(saved) and seen["n"] == 3 * working + 1404
            conn = sqlite3.connect(str(db))
            assert json.loads(conn.execute("SELECT body FROM settings WHERE key=?", (P.SHARE_KEY,)).fetchone()[0])["count"] == 1404
            conn.close()
            assert not bot.preopen_share_due(), "nothing counts the file's archive for the keeper while the mode is off: a caller says when it is behind"
            await stopped(bot)
            # a restart, the mode still off: the counts kept in the file
            bot = bot_on(None, clock, PREOPEN_DB=str(db))
            seen["n"] = 0
            bot.preopen_share_start()
            await settled(bot)
            assert seen["n"] == working + 1 + 36 * 3, seen
            assert dumped(bot.preopen_review()) == exact(saved)
            await stopped(bot)
            # no file at all: nothing recorded, nothing to keep — and nothing goes wrong
            bot = bot_on(None, clock, PREOPEN_DB=str(folder / "none.sqlite3"))
            assert bot.preopen_review() is None and bot.preopen_archive_rows() == 0
            bot.preopen_share_start()
            await REAL_SLEEP(0.05)
            assert not bot.preopen_share_task.done() and not bot.preopen_share_away and bot.preopen_review() is None and not (folder / "none.sqlite3").exists()
            await stopped(bot)
    finally:
        P.SHARE_LOOP_ROWS = old
        P.json = json
    print("THE_MODE_OFF_OK")


async def a_small_archive_as_it_was(folder) -> None:
    """Within what a call may read nothing changes: the figures at once and exact, no keeper, no note — and what counts cannot
    stand for is read record by record, as before."""
    led, saved, _ = build(folder / "small.sqlite3", windows=20)
    clock = Clock(NOW)
    bot = L.LiveBot.__new__(L.LiveBot)
    bot.preopen, bot.config, bot.market = types.SimpleNamespace(ledger=led), m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), clock
    assert led.archived_count == len(CARDS) * (20 * 4 - 2) == 234 and bot.preopen_archive_rows() == 234
    got = bot.preopen_review()
    assert dumped(got) == exact(saved) and "archive_note" not in got and getattr(bot, "preopen_share_task", None) is None and bot.preopen_share.count == 234
    assert bot.preopen_share_saved == (234, 234), "its counts are kept in the file all the same"
    odd = pair(0, NOW - 9 * DAY - 1_000, 0, up=3.0, result="up")
    with led.conn:
        led.conn.execute("INSERT INTO archive VALUES (?,?)", (odd["slug"], json.dumps(odd, ensure_ascii=False)))
    led.archived_count += 1
    got = bot.preopen_review()
    assert dumped(got) == exact([*saved, odd]) and "archive_note" not in got and got["total"]["filled"] == json.loads(exact(saved))["total"]["filled"] + 3.0
    assert getattr(bot, "preopen_share_task", None) is None
    led.close()
    print("A_SMALL_ARCHIVE_AS_IT_WAS_OK")


async def a_ledger_in_memory() -> None:
    """A ledger in memory (no bot that trades for real has one): nothing of it is kept, so it is read whole by whoever asks,
    as it always was — exact at once, no note, and the keeper leaves it alone."""
    led, saved, _ = build(":memory:")
    old = P.SHARE_LOOP_ROWS
    P.SHARE_LOOP_ROWS = 1000
    seen = counting()
    try:
        bot = bot_on(led, Clock(NOW))
        for n in (1, 2):
            got = bot.preopen_review()
            assert dumped(got) == exact(saved) and "archive_note" not in got and seen["n"] == n * 1404 and not bot.preopen_share_away
        assert getattr(bot, "preopen_share_task", None) is None and not bot.preopen_share_due()
        bot.PREOPEN_SHARE_EVERY_S = 0.02
        bot.preopen_share_start()
        await REAL_SLEEP(0.1)
        assert seen["n"] == 2 * 1404 and not bot.preopen_share_task.done(), "the keeper read a ledger in memory"
        await stopped(bot)
    finally:
        P.SHARE_LOOP_ROWS = old
        P.json = json
    led.close()
    print("A_LEDGER_IN_MEMORY_OK")


def without_a_loop(folder) -> None:
    """A bot nothing runs in the background of (a one-off call, a test's) and a large archive: the working records and the
    note — nothing raised, nothing walked, nothing promised."""
    led, saved, _ = build(folder / "noloop.sqlite3")
    old = P.SHARE_LOOP_ROWS
    P.SHARE_LOOP_ROWS = 1000
    seen = counting()
    try:
        bot = bot_on(led, Clock(NOW))
        for _ in range(2):
            got = bot.preopen_review()
            assert got["archive_note"] == NOTE(1404, NOT_READ) and getattr(bot, "preopen_share_task", None) is None and seen["n"] == 0
            assert bot.preopen_share_line() == "", "nothing keeps the counts here: /preopen says nothing of them"
            assert dumped(got) == dumped(P.review(list(led.rows.values()), NOW))
    finally:
        P.SHARE_LOOP_ROWS = old
        P.json = json
    led.close()
    print("WITHOUT_A_LOOP_OK")


async def one_review_for_an_export(folder) -> None:
    """What /export 全部 shares among its files (REVIEW_SHARED) is its own task's: the real trader's review is made once for
    it — in and between its builds — and anew for whoever else asks meanwhile, and afterwards."""
    led, saved, _ = build(folder / "shared.sqlite3", windows=20)
    bot = bot_on(led, Clock(NOW))
    a = bot.preopen_review()
    assert bot.preopen_review() is not a and bot.review_held() is None, "nothing kept outside a build"
    with bot.review_scope():
        b = bot.preopen_review()
        assert bot.preopen_review() is b and bot.review_held() == {"preopen": b}
    assert bot.review_held() is None
    others: list = []

    async def another() -> None:
        others.append(bot.review_held())
        with bot.review_scope():
            others.append(bot.preopen_review())
        others.append(bot.review_held())
    late: list = []

    async def begun_under_it(go: asyncio.Event) -> None:
        late.append(bot.review_held())
        await go.wait()
        late.append(bot.review_held())
        late.append(bot.preopen_review())
    shared: list = [{}]
    token = m.REVIEW_SHARED.set(shared)
    try:
        with bot.review_scope():
            c = bot.preopen_review()
        task = asyncio.get_running_loop().create_task(another(), context=contextvars.Context())  # (a page built meanwhile: its own task)
        await task
        go = asyncio.Event()
        child = asyncio.get_running_loop().create_task(begun_under_it(go))  # (a task the command starts and does not wait for: the command's context, copied)
        await asyncio.sleep(0)
        with bot.review_scope():
            assert bot.preopen_review() is c
        assert bot.preopen_review() is c and bot.review_held() == {"preopen": c} and getattr(bot, "review_kept", None) is None
    finally:
        shared[0] = None  # (as /export 全部 leaves it)
        m.REVIEW_SHARED.reset(token)
    assert others[0] is None and others[2] is None and others[1] is not c and dumped(others[1]) == dumped(c) == exact(saved)
    assert bot.review_held() is None and bot.preopen_review() is not c
    go.set()
    await child
    assert late[0] == {"preopen": c} and late[1] is None and late[2] is not c, "what outlives the command found its review still kept"
    led.close()
    print("ONE_REVIEW_FOR_AN_EXPORT_OK")


async def the_exports() -> None:
    """/export 预挂 and /summary say the note under their tables; /export 全部 makes the pre-open review once for all its files,
    with a breath between two."""
    src = (ROOT / "tests" / "test_journal.py").read_text(encoding="utf-8").split("\nasync def run():")[0]
    space: dict = {"__file__": str(ROOT / "tests" / "test_journal.py"), "__name__": "test_journal"}
    exec(compile(src, "test_journal_head", "exec"), space)
    bot = await space["build"]()  # (test_journal's bot: trades to make a journal and a summary of)
    now = bot.market.now_ms()
    rows = [pair(0, now - DAY, 0, up=30.0, result="up"), pair(0, now - DAY - 300_000, 0, down=30.0, result="up")]
    made, word = [], ["归档的 1,404 条旧记录还在后台计数"]

    def review():
        kept = bot.review_held()
        if kept is not None and "preopen" in kept:
            return kept["preopen"]
        made.append(1)
        out = P.review(rows, now)
        if word[0]:
            out["archive_note"] = word[0]
        if kept is not None:
            kept["preopen"] = out
        return out
    bot.preopen_rows, bot.preopen_review = (lambda: rows), review
    req = lambda *args: m.Request("/export", list(args), 1, 0, 42)
    records, tables = await bot.cmd_export(req("预挂"))
    assert tables.caption.startswith("预挂统计表（全部日期）：合计、按天") and tables.caption.endswith("｜⚠️ 归档的 1,404 条旧记录还在后台计数"), tables.caption
    assert "归档的旧记录也计入" not in tables.caption and bot.archive_note() == word[0]
    summary = await bot.cmd_export(req("汇总"))
    assert summary.caption.endswith("被拦下的机会；第一列是表名｜⚠️ 其中开盘前预挂的部分：归档的 1,404 条旧记录还在后台计数"), summary.caption
    assert "⚠️" not in (await bot.cmd_export(req("汇总", "恒生"))).caption, "a filter leaves the pre-open quotes out: nothing of theirs to say"
    word[0] = ""
    assert "⚠️" not in (await bot.cmd_export(req("汇总"))).caption and "归档的旧记录也计入" in (await bot.cmd_export(req("预挂")))[1].caption and bot.archive_note() == ""
    word[0] = "归档的 1,404 条旧记录还在后台计数"
    bot.TALLY_PACE_S = 0.0

    async def none(path, **params):
        return []
    m.binance_spot, bot.binance_futures = none, none
    del made[:]
    with Naps() as naps:
        parts = await bot.cmd_export(req("全部"))
    names = [x.filename for x in parts if isinstance(x, m.Document)]
    assert [n.split("-2026")[0] for n in names[:4]] == ["journal", "summary", "preopen-records", "preopen-summary"], names
    assert len(made) == 1, f"the pre-open review was made {len(made)} times for the files of one export"
    assert all("⚠️" in x.caption for x in parts if isinstance(x, m.Document) and x.filename.split("-2026")[0] in ("summary", "preopen-summary"))
    # (1.154.68: six files and the calibration's own breath between its two)
    assert naps.count(bot.EXPORT_BREATH_S) == 7 and bot.EXPORT_BREATH_S == 1.0 and m.REVIEW_SHARED.get() is None and bot.review_held() is None, naps
    del made[:]
    await bot.cmd_export(req("预挂"))
    assert len(made) == 1, "nothing kept beyond the export"
    print("THE_EXPORTS_OK")


async def run() -> None:
    with tempfile.TemporaryDirectory() as name:
        folder = pathlib.Path(name)
        the_steps_are_the_one_read(folder)
        little_on_the_loop(folder)
        kept_in_the_file(folder)
        await the_real_trader(folder)
        await the_mode_off(folder)
        await a_small_archive_as_it_was(folder)
        await a_ledger_in_memory()
        await one_review_for_an_export(folder)
    await the_exports()
    print("SHARE_OK")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as name:
        without_a_loop(pathlib.Path(name))  # (before any loop runs: a one-off call)
    asyncio.run(run())
