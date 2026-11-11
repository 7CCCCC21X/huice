"""Audit of the automatic redeem (自动领取): regression tests for the verified findings fixed here (fakes only, offline —
never Predict, a BNB node or Telegram)."""
import asyncio, json, pathlib, sys, tempfile, time, urllib.parse
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import main as m  # noqa: F401
import live as L

# test_trading's fakes (FakeApi, FakeChain, make_bot …) without running its suites: only the definitions before "async def run()"
_src = (ROOT / "tests" / "test_trading.py").read_text(encoding="utf-8").split("\nasync def run():")[0]
T: dict = {"__file__": str(ROOT / "tests" / "test_trading.py")}
exec(compile(_src, "test_trading_head", "exec"), T)
make_bot, market_json, NOW, WEI = T["make_bot"], T["market_json"], T["NOW"], T["WEI"]
MIN = 60_000


def cond(mid) -> str:
    return "0x" + f"{int(mid):064x}"


def mk(mid, won=0, status=None):
    """A two-outcome market by Predict's word: unresolved, or resolved with index set ``won`` the winner."""
    raw = {**market_json(str(mid), status=status or ("RESOLVED" if won else "REGISTERED")), "conditionId": cond(mid)}
    if won:
        raw["outcomes"] = [{**o, "status": "WON" if o["indexSet"] == won else "LOST"} for o in raw["outcomes"]]
    return raw


def held(mid, index_set=1, shares=10):
    """``shares`` held on a market's outcome (a row in the documented shape: no time on it)."""
    return {"id": f"p{mid}:{index_set}", "market": {"id": int(mid), "title": f"window {mid}"},
            "outcome": {"name": "Up" if index_set == 1 else "Down", "indexSet": index_set}, "amount": str(shares * WEI)}


class Canned:
    """Predict's REST API in miniature, under the REAL PredictApi: GET /positions paged by first / after (a cursor per page,
    none on the last), GET /markets/{id}. Every request is logged."""
    def __init__(self):
        self.rows, self.markets, self.log = [], {}, []

    def __call__(self, url, payload=None, timeout=15, headers=None):
        path = url.replace(m.PREDICT_REST, "")
        self.log.append(path)
        if path.startswith("/positions?"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
            first, start = int(q["first"][0]), int(q["after"][0][1:]) if "after" in q else 0
            more = start + first < len(self.rows)
            return json.dumps({"success": True, "data": self.rows[start:start + first], "cursor": f"c{start + first}" if more else None}).encode()
        if path.startswith("/markets/"):
            return json.dumps({"success": True, "data": self.markets[path.split("/")[2]]}).encode()
        raise m.RemoteError(f"HTTP 404: {path}")


async def ready_bot(**env):
    bot = make_bot(**env)
    await bot.live_prepare()
    assert bot.live.ready, bot.live.not_ready_why
    return bot


def notices(bot) -> list:
    """The bot's Telegram notices, kept instead of sent: [(key, text)]."""
    sent: list = []
    bot.live_notify = lambda text, key="": sent.append((key, text))
    return sent


# --- RDM-POSITIONS-CUT ----------------------------------------------------------------------------------------------------------
async def positions_cut():
    """RDM-POSITIONS-CUT: the redeem read the positions through api.positions() — ORDERS_PAGES_MAX pages (1000 rows at 50 a page)
    — and never looked whether that was the whole list. The bot never burns a losing token, so the pre-open mode's account grows
    by hundreds of rows a day: a winner listed past the cut was never redeemed by any round, the round said 持仓 1000 个：输掉的
    1000 and /live redeem check 赢的都领完了. The redeem now reads LIVE_REDEEM_PAGES deep with its own answer (positions_read),
    the minute readers keep their twenty pages and their flag, and a list longer still is said — the summary, the check, /live
    redeem, one notice an hour — never taken for the whole."""
    bot = await ready_bot()
    server, plain = Canned(), m._http_get
    m._http_get = server
    try:
        api = L.PredictApi(m.PREDICT_REST, "k", None)
        api.jwt, api.ORDERS_PAGE = "t", 5  # five rows a page: the shared cap (ORDERS_PAGES_MAX = 20 pages) ends at row 100
        bot.live.api = api
        dead = [str(9000 + i) for i in range(120)]  # 120 old positions resolved against the bot, marked lost long ago
        for mid in dead:
            bot.store.put(f"live:redeemed:{mid}:1", {"at": NOW - 86_400_000, "lost": True})
        server.rows = [held(mid) for mid in dead] + [held(500)]  # the winner is row 121, Predict lists the oldest first
        server.markets["500"] = mk(500, won=1)
        # the once-a-minute readers keep their twenty pages and their flag
        assert len(await api.positions()) == 100 and api.positions_complete is False
        # (a) the automatic round reads its own way deeper: the winner past the cut redeemed — once
        server.log.clear()
        done = await bot.live_redeem(NOW)
        assert len(done) == 1 and "已领取 window 500 Up 10 份：0xredeem" in done[0], (done, bot.live_redeem_last)
        assert [cid for cid, *_ in bot.live.chain.redeemed] == [cond(500)] and bot.store.get("live:redeemed:500:1")["tx"] == "0xredeem"
        pages = [p for p in server.log if p.startswith("/positions")]
        assert len(pages) == 25 and server.log.count("/markets/500") == 1, server.log  # 121 rows at 5 a page; the lost ones not read
        assert api.positions_complete is False, "the redeem's read writes no flag of the minute readers"
        assert bot.live_redeem_last["cut"] == 0 and "其余没检查" not in bot.live_redeem_summary(bot.live_redeem_last), bot.live_redeem_last
        assert bot.live_redeem_last["counts"] == {"redeemed": 1, "lost": 120}, bot.live_redeem_last
        assert await bot.live_redeem(NOW + 10 * MIN) == [] and len(bot.live.chain.redeemed) == 1, "never twice"
        report = await bot.live_redeem_report(NOW + 11 * MIN)
        assert "其余没检查" not in report and "结论：有持仓已经发过领取交易并上链" in report, report  # the whole list read: no cut words
        # (b) a list longer than even the redeem's own read (here 10 pages = 50 rows): said everywhere, never an all-clear
        bot.LIVE_REDEEM_PAGES = 10
        sent = notices(bot)
        assert await bot.live_redeem(NOW + 20 * MIN) == []
        last = bot.live_redeem_last
        assert last["cut"] == 50 and last["counts"] == {"lost": 50}, last
        assert bot.live_redeem_summary(last) == "持仓 50 个：输掉的 50；⚠️ 持仓列表超过 50 条，只读到前 50 条，其余没检查", bot.live_redeem_summary(last)
        assert [k for k, _ in sent] == ["redeem:cut"] and "超过 50 条，只读到前 50 条" in sent[0][1] and "Predict 网站" in sent[0][1], sent
        assert "持仓列表超过 50 条" in bot.live_errors[-1], bot.live_errors[-1:]
        assert "持仓列表超过 50 条，只读到前 50 条，其余没检查" in bot.live_redeem_status_line(), bot.live_redeem_status_line()
        report = await bot.live_redeem_report(NOW + 21 * MIN)
        verdict = next(line for line in report.split("\n") if line.startswith("结论："))
        assert verdict.startswith("结论：持仓列表超过 50 条，只读到前 50 条，其余没检查") and "赢的都领完了" not in report, report
        assert "持仓 50 个：输掉的 50（⚠️ 持仓列表超过 50 条，只读到前 50 条，其余没检查）" in report, report
        answer = await bot.live_action("redeem", [], NOW + 22 * MIN)
        assert answer.startswith("读到的持仓里没有可领取的：持仓 50 个：输掉的 50；⚠️ 持仓列表超过 50 条") and "没有可领取的已结算持仓" not in answer, answer
        assert [k for k, _ in sent] == ["redeem:cut"], ("one notice an hour, on its own timer", sent)
        # (c) a stand-in client without positions_read: its own read and its own flag, as before
        bot.LIVE_REDEEM_PAGES = L.LiveBot.LIVE_REDEEM_PAGES
        fake = T["FakeApi"]()
        fake.positions_rows, fake.markets["500"], fake.positions_complete = [held(500)], mk(500, won=1), False
        bot.live.api = fake
        bot.store.delete_keys(["live:redeemed:500:1"])
        done = await bot.live_redeem(NOW + 80 * MIN)
        assert len(done) == 1 and bot.live_redeem_last["cut"] == 1, bot.live_redeem_last
    finally:
        m._http_get = plain
    print("RDM_POSITIONS_CUT_OK")


asyncio.run(positions_cut())


# --- RDM-ROUND-VOID -------------------------------------------------------------------------------------------------------------
def due(bot) -> float:
    """Seconds until the next automatic redeem round is due (live_redeem_tick's own test)."""
    return bot.LIVE_REDEEM_SECONDS - (time.monotonic() - bot.live_last_redeem)


async def auto_round(bot):
    """One automatic round, as the redeem's own timer starts it when due."""
    bot.live_last_redeem = -1e9
    bot.live_redeem_tick()
    await bot.live_redeem_task


async def round_void():
    """RDM-ROUND-VOID: the tick stamps the clock before the round runs, and a round that could not look — the key's 429 cooldown
    (shared with the pre-open mode's orders and cancels), one failed positions read, the sign-in not there yet — or whose market
    reads a rate limit cut short, left every waiting winner ten more minutes (the round a win had brought included). Such a round
    now brings the next one soon: after the cooldown, 30 / 60 / 120 s, three times in a row at most, never later than a round
    already due; a round that looked, a mid-pass failure, low gas and the dry run keep the ten minutes."""
    cooling_text = "Predict 接口限流冷却中（15 秒后重试）"

    async def account(*winners):
        bot = await ready_bot()
        fake = bot.live.api
        fake.blocked_until, plain = 0.0, fake.positions
        fake.positions_rows = [held(mid) for mid in winners]
        fake.markets.update({str(mid): mk(mid, won=1) for mid in winners})
        return bot, fake, plain

    # (a) the key cooling when the round starts: nothing read — the next round the cooldown's end + 30 s on, not 600
    bot, fake, plain = await account(500)

    async def refused():
        raise m.RemoteError(cooling_text, 15)
    fake.positions, fake.blocked_until = refused, time.monotonic() + 15
    await auto_round(bot)
    assert bot.live_redeem_last["error"] == "读取持仓失败：" + cooling_text and bot.live.chain.redeemed == [], bot.live_redeem_last
    assert 40 <= due(bot) <= 46, due(bot)
    task = bot.live_redeem_task
    bot.live_redeem_tick()
    assert bot.live_redeem_task is task, "inside the wait: no round"
    fake.positions, fake.blocked_until = plain, 0.0
    bot.live_last_redeem -= due(bot) + 1  # the wait is over
    bot.live_redeem_tick()
    assert bot.live_redeem_task is not task
    await bot.live_redeem_task
    assert [cid for cid, *_ in bot.live.chain.redeemed] == [cond(500)] and bot.live_redeem_again_n == 0, bot.live_redeem_last
    assert due(bot) > 595, ("a round that looked keeps the ten minutes", due(bot))
    # (b) the positions read failing round after round: 30, 60, 120 s, then the usual cadence; a round that looks starts over
    bot, fake, plain = await account(500)

    async def down():
        raise m.RemoteError("HTTP 500: Internal Server Error")
    fake.positions, waits = down, []
    for _ in range(5):
        await auto_round(bot)
        waits.append(round(due(bot)))
    assert waits == [30, 60, 120, 600, 600] and bot.live_redeem_last["error"] == "读取持仓失败：HTTP 500: Internal Server Error", (waits, bot.live_redeem_last)
    fake.positions = plain
    await auto_round(bot)
    assert len(bot.live.chain.redeemed) == 1 and due(bot) > 595 and bot.live_redeem_again_n == 0
    fake.positions = down
    await auto_round(bot)
    assert round(due(bot)) == 30, due(bot)
    # (c) never inside the cooldown, and never later than a round already due (min, as live_redeem_soon)
    fake.blocked_until = time.monotonic() + 100
    bot.live_last_redeem = time.monotonic()
    bot.live_redeem_pull(5)
    assert 129 <= due(bot) <= 131, due(bot)
    fake.blocked_until = 0.0
    bot.live_redeem_soon()
    assert due(bot) <= 20, due(bot)
    bot.live_redeem_pull(45)
    assert due(bot) <= 20, ("a sooner round is not put off", due(bot))
    # (d) not signed in yet: the next round past the sign-in's once-a-minute throttle, not ten minutes on
    bot, fake, plain = await account(500)
    bot.live.account_error = "账户校验失败：RPC 超时"
    await auto_round(bot)
    assert bot.live_redeem_last["error"].startswith("真实交易未就绪") and 58 <= due(bot) <= 66, (bot.live_redeem_last, due(bot))
    # (e) a rate limit in the middle of the market reads: the rest once the cooldown is over (7 s + 30 s), not ten minutes on
    bot, fake, plain = await account(501, 502, 503)
    reads, market = [], fake.market

    async def limited(mid):
        reads.append(str(mid))
        if len(reads) == 2:
            fake.blocked_until = time.monotonic() + 7
            raise m.RemoteError("HTTP 429: Too Many Requests", 7)
        return await market(mid)
    fake.market = limited
    await auto_round(bot)
    assert bot.live_redeem_last["counts"] == {"redeemed": 1, "unread": 2} and bot.live_redeem_last["limited"], bot.live_redeem_last
    assert 35 <= due(bot) <= 38, due(bot)
    fake.blocked_until = 0.0
    await auto_round(bot)
    assert sorted(cid for cid, *_ in bot.live.chain.redeemed) == [cond(501), cond(502), cond(503)] and due(bot) > 595, bot.live_redeem_last
    # (f) the usual cadence: a failure in the middle of the pass (its own label), no gas, a dry run, /live redeem
    bot, fake, plain = await account(500)
    bot.position_time = lambda row: 1 // 0
    await auto_round(bot)
    assert bot.live_redeem_last["error"].startswith("这一轮中途出错：") and due(bot) > 595, (bot.live_redeem_last, due(bot))
    del bot.position_time
    bot.live.chain.bnb = 10 ** 13
    await bot.live.refresh_balance()
    await auto_round(bot)
    assert bot.live_redeem_last["counts"] == {"claimable": 1} and bot.live.chain.redeemed == [] and due(bot) > 595, bot.live_redeem_last
    bot.live.chain.bnb = 10 ** 17
    await bot.live.refresh_balance()
    fake.positions = down
    bot.live_last_redeem = stamp = time.monotonic()
    try:
        await bot.live_redeem_report(NOW)
        assert False, "the check's read failed"
    except m.RemoteError as error:
        assert "HTTP 500" in str(error)
    try:
        await bot.live_redeem(NOW)
        assert False, "/live redeem's read failed"
    except m.RemoteError as error:
        assert "HTTP 500" in str(error)
    assert bot.live_last_redeem == stamp, "/live redeem and the check never move the automatic clock"
    # (g) no round has looked for half an hour (a cause that does not pass): said on Telegram, once an hour
    sent = notices(bot)
    bot.live_redeem_whole_at = time.monotonic() - bot.LIVE_REDEEM_STUCK_SECONDS - 1
    for _ in range(3):
        await auto_round(bot)
    assert [k for k, _ in sent] == ["redeem:read"] and "分钟没能看完持仓（读取持仓失败：HTTP 500" in sent[0][1], sent
    fake.positions = plain
    await auto_round(bot)
    assert time.monotonic() - bot.live_redeem_whole_at < 5 and len(bot.live.chain.redeemed) == 1
    print("RDM_ROUND_VOID_OK")


asyncio.run(round_void())


# --- RDM-UNSENT-RETRY-GRID ------------------------------------------------------------------------------------------------------
async def unsent_retry_grid():
    """RDM-UNSENT-RETRY-GRID: a redeem that never left (TxNotSent: the chain a moment behind Predict's result, an RPC blip) waits
    five minutes and the bot said 5 分钟后自动再试 — but nothing brought a round when the wait ended: the retry came with the next
    ten-minute round (up to fifteen when another round came inside the wait). For its first three such failures in a row the
    position now brings the next round to the end of its wait (the shared pull: never later than a round already due, never
    inside the key's cooldown); then the usual rounds, and the text says so. A sent failure, low gas and the check pull nothing."""
    bot = await ready_bot()
    fake, chain, key = bot.live.api, bot.live.chain, "live:redeemed:500:1"
    fake.positions_rows, fake.markets["500"] = [held(500)], mk(500, won=1)
    chain.fail_unsent = "RPC 错误：execution reverted"
    sent = notices(bot)
    # (a) the first try never left: the next round at the wait's end, five minutes on — not the ten-minute round
    bot.live_last_redeem = time.monotonic()  # (as the tick stamps it when the round starts)
    assert await bot.live_redeem(NOW) == [] and chain.redeemed == []
    assert 295 < due(bot) <= 302, due(bot)
    assert bot.live_errors[-1].endswith("（交易没发出，5 分钟后再试）") and bot.live_redeem_unsent == {key: 1}, (bot.live_errors[-1:], bot.live_redeem_unsent)
    assert [k for k, _ in sent] == ["redeem:fail"] and "5 分钟后自动再试" in sent[0][1], sent
    # (b) another round inside the wait (a win elsewhere) finds it waiting: the pull to the wait's end stands
    bot.live_last_redeem = time.monotonic()
    assert await bot.live_redeem(NOW + MIN) == [] and bot.live_redeem_last["counts"] == {"waiting": 1}, bot.live_redeem_last
    assert 290 < due(bot) <= 302, due(bot)
    # (c) /live redeem check moves nothing
    stamp = bot.live_last_redeem
    await bot.live_redeem_report(NOW + MIN + 1000)
    assert bot.live_last_redeem == stamp
    # (d) the second and third such failures in a row pull too; the fourth waits for the usual round, and says so
    for runs in (2, 3, 4):
        bot.live_redeem_failed.clear()  # (the wait over)
        bot.live_last_redeem = time.monotonic()
        assert await bot.live_redeem(NOW + runs * 6 * MIN) == [] and bot.live_redeem_unsent == {key: runs}, bot.live_redeem_unsent
        if runs <= bot.LIVE_REDEEM_UNSENT_PULLS:
            assert 295 < due(bot) <= 302 and bot.live_errors[-1].endswith("（交易没发出，5 分钟后再试）"), (runs, due(bot), bot.live_errors[-1:])
    assert due(bot) > 595 and bot.live_errors[-1].endswith("（交易没发出，下一轮再试，最迟约 10 分钟）"), (due(bot), bot.live_errors[-1:])
    bot.live_last_redeem = time.monotonic()
    assert await bot.live_redeem(NOW + 30 * MIN) == [] and bot.live_redeem_last["counts"] == {"waiting": 1} and due(bot) > 595, "past the bound: not even from the wait"
    # (e) it goes out: the run of failures forgotten
    chain.fail_unsent = ""
    bot.live_redeem_failed.clear()
    done = await bot.live_redeem(NOW + 40 * MIN)
    assert len(done) == 1 and bot.live_redeem_unsent == {} and [cid for cid, *_ in chain.redeemed] == [cond(500)], (done, bot.live_redeem_unsent)
    # (f) a sent failure (its gas paid, its transaction may yet be mined): the hour's wait, nothing pulled
    fake.positions_rows.append(held(501)); fake.markets["501"] = mk(501, won=1)
    chain.fail = "交易失败（已回滚）"
    bot.live_last_redeem = time.monotonic()
    assert await bot.live_redeem(NOW + 50 * MIN) == [] and due(bot) > 595 and bot.live_errors[-1].endswith("（一小时后再试）"), (due(bot), bot.live_errors[-1:])
    assert bot.live_redeem_unsent == {}
    # (g) low gas: nothing tried, nothing pulled
    chain.fail = ""
    fake.positions_rows.append(held(502)); fake.markets["502"] = mk(502, won=1)
    chain.bnb = 10 ** 13
    await bot.live.refresh_balance()
    bot.live_last_redeem = time.monotonic()
    assert await bot.live_redeem(NOW + 51 * MIN) == [] and bot.live_redeem_last["counts"].get("claimable") == 1 and due(bot) > 595, bot.live_redeem_last
    # (h) the key cooling when the wait ends: the pulled round after the cooldown, not into it
    chain.bnb = 10 ** 17
    await bot.live.refresh_balance()
    chain.fail_unsent = "HTTP 503: Service Unavailable"
    bot.live_redeem_failed.clear()
    fake.blocked_until = time.monotonic() + 400
    bot.live_last_redeem = time.monotonic()
    assert await bot.live_redeem(NOW + 52 * MIN) == []
    assert 429 <= due(bot) <= 431, due(bot)
    print("RDM_UNSENT_RETRY_GRID_OK")


asyncio.run(unsent_retry_grid())


# --- RDM-SOON-NO-TARGET (the name kept through a pulled retry) ------------------------------------------------------------------
async def soon_names_the_market():
    """RDM-SOON-NO-TARGET: live_redeem_soon only moved the clock. With more than forty markets unjudged, the round a win brought
    read the forty longest unread — and the market that had just settled, read a round before as still open, waited for a later
    round. The win's market is now named (sim_settle, the pre-open settle_pair) and read first by the next real round, inside the
    cap; once read it goes back into the rotation. Kept named while its redeem never left and that retry is pulled, so the retry
    at the wait's end reads it first too; the check reads it first and leaves the name."""
    bot = await ready_bot()
    fake, chain, CAP = bot.live.api, bot.live.chain, bot.LIVE_REDEEM_READS
    newer = [str(600 + i) for i in range(45)]
    fake.markets.update({mid: mk(mid) for mid in newer})
    fake.markets["500"] = mk(500)
    fake.positions_rows = [held(mid) for mid in newer] + [held(500)]  # no time on the rows: Predict's own order, window 500 last
    reads, market = [], fake.market

    async def counting(mid):
        reads.append(str(mid))
        return await market(mid)
    fake.market = counting

    async def look(at):
        reads.clear()
        done = await bot.live_redeem(at)
        assert len(reads) <= CAP, ("never more than the cap a round", len(reads))
        return done, list(reads)

    def won(mid, at):
        """Predict's final result on a real record of this market, in the bot's favour (what asks for a round within seconds)."""
        fake.markets[mid] = mk(mid, won=1)
        record = {"shares": 10.0, "order": 10.0, "side": "up", "status": "filled", "maker": False, "revisions": [], "market_id": mid,
                  "live": {"state": "done", "events": [], "want": {"market_id": mid, "index_set": 1}}}
        bot.sim_settle(record, 1.0, "Predict 结算：Up", at, "Predict 最终结果")

    done, r1 = await look(NOW)
    assert done == [] and r1 == newer[:CAP], r1
    done, r2 = await look(NOW + 10 * MIN)  # the six not reached first — 500 read, still open — then the rest
    assert done == [] and r2[:6] == newer[CAP:] + ["500"], r2
    won("500", NOW + 11 * MIN)
    reads.clear()
    report = await bot.live_redeem_report(NOW + 11 * MIN + 1000)  # the check reads it first too, and leaves the name
    assert reads[0] == "500" and "window 500 / Up 10 份，约 $10.00" in report, (reads[:3], report)
    assert bot.live_redeem_first.keys() == {"500"}, bot.live_redeem_first
    done, r3 = await look(NOW + 11 * MIN + 20_000)  # the round the win brought
    assert r3[0] == "500" and len(done) == 1 and "已领取 window 500 Up 10 份：0xredeem" in done[0], (done, r3)
    assert bot.live_redeem_first == {} and [cid for cid, *_ in chain.redeemed] == [cond(500)], bot.live_redeem_first
    # a named market still unresolved: read once, first, then back in the rotation
    bot.live_redeem_soon(newer[CAP])
    done, r4 = await look(NOW + 20 * MIN)
    assert r4[0] == newer[CAP] and bot.live_redeem_first == {}, (r4[:3], bot.live_redeem_first)
    # the chain behind Predict's result: the named market read first, its redeem never left — still named; inside the wait not
    # read and still named; at the wait's end read first and redeemed, the name gone
    chain.fail_unsent = "RPC 错误：execution reverted"
    won(newer[0], NOW + 21 * MIN)
    done, r5 = await look(NOW + 21 * MIN + 20_000)
    assert r5[0] == newer[0] and done == [] and newer[0] in bot.live_redeem_first, (r5[:3], bot.live_redeem_last)
    done, r6 = await look(NOW + 24 * MIN)
    assert newer[0] not in r6 and newer[0] in bot.live_redeem_first and bot.live_redeem_last["counts"]["waiting"] == 1, (r6[:3], bot.live_redeem_last)
    chain.fail_unsent = ""
    bot.live_redeem_failed.clear()  # (the wait over)
    done, r7 = await look(NOW + 27 * MIN)
    assert r7[0] == newer[0] and len(done) == 1 and f"已领取 window {newer[0]}" in done[0] and bot.live_redeem_first == {}, (done, r7[:3])
    # a name with nothing to judge there is forgotten by the next real round
    bot.live_redeem_soon("999")
    await look(NOW + 30 * MIN)
    assert bot.live_redeem_first == {}
    # a win settled without a market (an older record): the clock alone, as before
    bot.live_last_redeem = time.monotonic()
    bot.live_redeem_soon()
    assert bot.live_redeem_first == {} and due(bot) <= 20
    print("RDM_SOON_NO_TARGET_OK")


asyncio.run(soon_names_the_market())


# --- RDM-ROUND-LOCK-STALL -------------------------------------------------------------------------------------------------------
class Node(L.Chain):
    """The REAL Chain (send, wait, redeem; the real signing with the SDK's test key) over a scripted JSON-RPC node that takes
    every transaction; ``mines`` False: it mines none. A node that mines answers the first receipt poll; one that does not is
    given no time at all (the real wait would poll 120 s)."""
    def __init__(self, wallet, mines=False):
        super().__init__("https://rpc.invalid", wallet)
        self.mines, self.sent = mines, []

    async def rpc(self, method, params):
        if method == "eth_sendRawTransaction":
            self.sent.append(params[0])
            return "0x" + f"{len(self.sent):064x}"
        if method == "eth_getTransactionReceipt":
            return {"blockNumber": "0x10", "status": "0x1"} if self.mines else None
        return {"eth_getTransactionCount": "0x1", "eth_gasPrice": "0x3b9aca00", "eth_estimateGas": "0x5208", "eth_getBalance": hex(10 ** 17),
                "eth_call": "0x" + "0" * 63 + "1"}[method]

    async def wait(self, tx_hash, timeout=120.0):
        return await super().wait(tx_hash, timeout=5 if self.mines else 0)


async def round_lock_stall():
    """RDM-ROUND-LOCK-STALL: a round holds the redeem lock for its whole pass, each winner waiting up to 120 s for its receipt —
    with a node that takes the transactions and mines none, N × two minutes. /live redeem check waited for that lock with no bound
    and said nothing until the round ended, and /live showed 正在领取 with no start. The check now waits a minute at most and then
    answers without its dry run (never beside the round), /live says since when; and after one receipt that did not come in time
    the round sends nothing more — the rest stay 可领取 for the next round, the pending one keeps its hour."""
    # F1: the check while a round holds the lock (its one redeem waiting for a receipt)
    bot = await ready_bot()
    fake, chain = bot.live.api, bot.live.chain
    fake.positions_rows, fake.markets["500"] = [held(500)], mk(500, won=1)
    receipt, plain = asyncio.Event(), chain.redeem

    async def slow(*args):
        await receipt.wait()
        return await plain(*args)
    chain.redeem = slow
    bot.live_last_redeem = -1e9
    bot.live_redeem_tick()
    while not bot.redeem_lock().locked():
        await asyncio.sleep(0)
    bot.REDEEM_CHECK_WAIT_SECONDS = 0
    report = await asyncio.wait_for(bot.live_redeem_report(NOW + MIN), 2)  # (before: no answer until the round ended)
    assert "结论：有一轮领取正在进行（10-05 10:00 起，已 0 分钟）" in report and "持仓 1 个" not in report, report
    assert "｜正在领取（10-05 10:00 起）｜" in bot.live_redeem_status_line(), bot.live_redeem_status_line()
    assert "｜正在领取（10-05 10:00 起）" in report and "签名钱包" in report, report  # what it has: the switches, the status, the gas
    # ...a round that ends within the wait: the full report, as before
    bot.REDEEM_CHECK_WAIT_SECONDS = 5
    later = asyncio.create_task(bot.live_redeem_report(NOW + MIN))
    await asyncio.sleep(0.2)
    receipt.set()
    report = await asyncio.wait_for(later, 4)
    await bot.live_redeem_task
    assert "持仓 1 个：已领仍列着 1" in report and "正在进行" not in report, report
    assert [cid for cid, *_ in chain.redeemed] == [cond(500)] and bot.live_redeem_since == 0, "redeemed once"
    assert "正在领取" not in bot.live_redeem_status_line(), bot.live_redeem_status_line()
    # F2: a node that takes every transaction and mines none — one transaction, the rest left 可领取 for the next round
    bot = await ready_bot()
    fake = bot.live.api
    fake.positions_rows = [held(mid) for mid in (501, 502, 503)]
    fake.markets.update({str(mid): mk(mid, won=1) for mid in (501, 502, 503)})
    node = bot.live.chain = Node(bot.live.wallet)
    try:
        await node.wait("0xabc")
        assert False, "no receipt"
    except L.TxPending as error:
        assert str(error) == "交易 0xabc 在 0 秒内未上链" and error.tx_hash == "0xabc" and isinstance(error, m.RemoteError)
    sent = notices(bot)
    assert await bot.live_redeem(NOW) == []
    assert len(node.sent) == 1, ("one transaction into a node that confirms none", len(node.sent))
    assert bot.live_redeem_last["counts"] == {"failed": 1, "claimable": 2}, bot.live_redeem_last
    assert list(bot.live_redeem_failed) == ["live:redeemed:501:1"], bot.live_redeem_failed
    assert bot.LIVE_REDEEM_RETRY_SECONDS - 5 < bot.LIVE_REDEEM_RETRY_SECONDS - (time.monotonic() - bot.live_redeem_failed["live:redeemed:501:1"]), "its hour"
    assert not any(bot.store.get(f"live:redeemed:{mid}:1") for mid in (501, 502, 503)), "no mark"
    assert "这一轮其余的先不发" in bot.live_redeem_summary(bot.live_redeem_last) and [k for k, _ in sent] == ["redeem:fail"], (bot.live_redeem_last, sent)
    assert "一小时后自动再试" in sent[0][1] and "在 0 秒内未上链" in sent[0][1], sent
    # ...the node mining again: the next round sends the other two, and the pending one is looked up by its receipt first and
    # booked with its own transaction — never sent again (1.150.0, RDM-RECEIPT-POLL: it used to wait its hour unannounced, mined)
    node.mines = True
    done = await bot.live_redeem(NOW + 10 * MIN)
    assert len(done) == 3 and len(node.sent) == 3 and bot.live_redeem_last["counts"] == {"redeemed": 3}, (done, bot.live_redeem_last)
    assert done[0].startswith("💵 已领取 window 501 Up 10 份：") and bot.live_redeem_failed == {}, (done, bot.live_redeem_failed)
    # control: a node that mines — every winner, one transaction each, as before
    bot = await ready_bot()
    fake = bot.live.api
    fake.positions_rows = [held(mid) for mid in (501, 502, 503)]
    fake.markets.update({str(mid): mk(mid, won=1) for mid in (501, 502, 503)})
    node = bot.live.chain = Node(bot.live.wallet, mines=True)
    assert len(await bot.live_redeem(NOW)) == 3 and len(node.sent) == 3 and all(bot.store.get(f"live:redeemed:{mid}:1") for mid in (501, 502, 503))
    print("RDM_ROUND_LOCK_STALL_OK")


asyncio.run(round_lock_stall())


# --- RDM-ROW-ABORT --------------------------------------------------------------------------------------------------------------
async def row_abort():
    """RDM-ROW-ABORT: an index set int() cannot read ('YES' on a row, '1.0' on a market's outcome) raised out of the pass — every
    round ended there (读取持仓失败 …invalid literal…), /live redeem check answered one line, and under the rotation the odd row's
    market, never read, sorted first every round: nothing at all redeemed. A row whose amount is no whole number, or that names no
    market, was dropped without a word. Each is now 缺字段 with its reason — no read, no transaction, no mark, never a guessed
    side — and the round goes on; an emptied row (amount 0) stays out of the count as before."""
    bot = await ready_bot()
    fake, chain = bot.live.api, bot.live.chain
    odd_market = mk(902, won=1)
    odd_market["outcomes"][0]["indexSet"] = "1.0"  # the market's own outcome: no whole number
    fake.markets.update({"900": mk(900, won=1), "902": odd_market, "903": mk(903, won=1), "901": mk(901, won=1), "905": mk(905)})
    fake.positions_rows = [{**held(900), "outcome": {"name": "Up", "indexSet": "YES"}},  # the row's index set: no number
                           held(902),
                           {**held(903), "amount": "7.5"},  # an amount that is no whole number of wei
                           {"id": "pX", "market": "?", "outcome": {"name": "Up", "indexSet": 1}, "amount": str(4 * WEI)},  # no market id
                           {**held(904), "amount": "0"},  # emptied: nothing held
                           held(905),  # open
                           held(901, shares=25)]  # an ordinary winner, listed last
    sent = notices(bot)
    await auto_round(bot)
    last = bot.live_redeem_last
    assert not last.get("error"), last  # (before: 读取持仓失败 / 这一轮中途出错：invalid literal for int() with base 10: 'YES')
    assert [(cid, index_set, amount) for cid, index_set, amount, *_ in chain.redeemed] == [(cond(901), 1, 25 * WEI)], chain.redeemed
    assert last["counts"] == {"redeemed": 1, "fields": 4, "open": 1}, last
    assert not any(bot.store.get(f"live:redeemed:{mid}:{i}") for mid in ("900", "902", "903") for i in (0, 1, 2)), "no mark, never a guess"
    assert bot.live_redeem_summary(last) == "持仓 6 个：领了 1，缺字段 4，未结算 1", bot.live_redeem_summary(last)
    assert [k for k, _ in sent] == [""] and "已领取 window 901 Up 25 份" in sent[0][1], sent  # the 💵 line, and nothing else
    report = await bot.live_redeem_report(NOW + MIN)
    assert report.startswith("🔎 自动领取自检") and "缺字段：" in report, report
    for why in ("持仓的 indexSet 读不懂（'YES'），分不出是哪一边", "市场详情读不懂（", "持仓的份数（amount）读不懂（'7.5'）", "持仓没写市场编号"):
        assert why in report, (why, report)
    # the round after: the open market resolved in the bot's favour is redeemed (the odd row before it holds nothing up)
    fake.markets["905"] = mk(905, won=1)
    await auto_round(bot)
    assert [cid for cid, *_ in chain.redeemed] == [cond(901), cond(905)] and not bot.live_redeem_last.get("error"), (chain.redeemed, bot.live_redeem_last)
    # a row with no index set at all keeps its old words
    fake.positions_rows = [{**held(906), "outcome": {"name": "Up"}}]
    fake.markets["906"] = mk(906, won=1)
    assert "持仓没写 indexSet，分不出是哪一边" in await bot.live_redeem_report(NOW + 2 * MIN)
    print("RDM_ROW_ABORT_OK")


asyncio.run(row_abort())


# --- RDM-OPEN-SHAPES ------------------------------------------------------------------------------------------------------------
async def open_shapes():
    """RDM-OPEN-SHAPES: a position is redeemed (or written off) only when the market says both — the settled status word and a
    winner it can read — and that stays: a named result without the settled word may still be a proposal (a redeem then can be
    mined for nothing; never a lost mark on less than settled). But half an answer read 未结算 for ever: the check said
    结算后自动领取 beside 市场状态 RESOLVED, nothing was pushed. It is now its own kind, 结算读不出, with the missing half named,
    and told on Telegram once it has stood half an hour (six hours for a named result without the settled word), an hour apart.
    Nothing is sent and nothing written for it."""
    def advance(bot, seconds):
        """``seconds`` pass for the unclear positions' clocks (the round that first met each)."""
        for k in bot.live_redeem_unclear:
            bot.live_redeem_unclear[k] -= seconds

    # (1) RESOLVED, no winner readable (outcomes without a status, no resolution object)
    bot = await ready_bot()
    fake, chain = bot.live.api, bot.live.chain
    fake.markets["101"] = {**market_json("101", status="RESOLVED"), "conditionId": cond(101)}
    fake.positions_rows = [held(101, 1), held(101, 2)]
    sent = notices(bot)
    await bot.live_redeem(NOW)
    assert bot.live_redeem_last["counts"] == {"unclear": 2}, bot.live_redeem_last  # (before: {"open": 2})
    assert chain.redeemed == [] and not any(bot.store.get(f"live:redeemed:101:{i}") for i in (1, 2)), "nothing sent, nothing written"
    assert bot.live_redeem_summary(bot.live_redeem_last) == "持仓 2 个：结算读不出 2"
    report = await bot.live_redeem_report(NOW + MIN)
    assert "Predict 的结算读不出（机器人不会去领）：" in report and "Predict 写了已结算，但读不出哪一边赢（市场状态 RESOLVED）" in report, report
    assert "结算后自动领取" not in report and "赢的都领完了" not in report and "结论：有持仓的市场 Predict 的结算读不出" in report, report
    for _ in range(2):  # ten, twenty minutes on: not yet
        advance(bot, 600)
        await bot.live_redeem(NOW + MIN)
    assert sent == [], sent
    advance(bot, 1200)  # forty minutes
    await bot.live_redeem(NOW + 40 * MIN)
    assert [k for k, _ in sent] == ["redeem:unclear"] and "2 个持仓 Predict 的结算读不出（window 101：Predict 写了已结算，但读不出哪一边赢，市场状态 RESOLVED）" in sent[0][1], sent
    assert "不会去领、也不记成输" in sent[0][1] and "结算读不出" in bot.live_errors[-1], sent
    advance(bot, 600)
    await bot.live_redeem(NOW + 50 * MIN)
    assert len(sent) == 1, "an hour apart"
    # ...the winner readable later: redeemed once, the loser written off, nothing left remembered
    fake.markets["101"]["outcomes"] = [{**o, "status": "WON" if o["indexSet"] == 2 else "LOST"} for o in fake.markets["101"]["outcomes"]]
    done = await bot.live_redeem(NOW + 60 * MIN)
    assert len(done) == 1 and [(cid, i) for cid, i, *_ in chain.redeemed] == [(cond(101), 2)] and bot.store.get("live:redeemed:101:1")["lost"] is True
    assert bot.live_redeem_unclear == {}, bot.live_redeem_unclear
    # (2) a named result without the settled word (CLOSED; a proposal looks so): not redeemed, the other side NOT written off
    bot = await ready_bot()
    fake, chain = bot.live.api, bot.live.chain
    fake.markets["201"] = mk(201, won=2, status="CLOSED")
    fake.positions_rows = [held(201, 1), held(201, 2)]
    sent = notices(bot)
    await bot.live_redeem(NOW)
    assert bot.live_redeem_last["counts"] == {"unclear": 2} and chain.redeemed == [], bot.live_redeem_last
    assert bot.store.get("live:redeemed:201:1") is None and bot.store.get("live:redeemed:201:2") is None, "no lost mark on less than settled"
    report = await bot.live_redeem_report(NOW + MIN)
    assert "Predict 写了结果（Down），但市场状态不是已结算（市场状态 CLOSED）" in report, report
    advance(bot, 40 * 60)
    await bot.live_redeem(NOW + 40 * MIN)
    assert sent == [], ("a proposal may stand for hours: not yet", sent)
    advance(bot, 6 * 3600)
    await bot.live_redeem(NOW + 7 * 60 * MIN)
    assert [k for k, _ in sent] == ["redeem:unclear"] and "Predict 写了结果（Down），但市场状态不是已结算" in sent[0][1], sent
    # (3) guards: the documented settled shape as before; a market still open stays 未结算 and is never told
    bot = await ready_bot()
    fake, chain = bot.live.api, bot.live.chain
    fake.markets.update({"301": mk(301, won=2), "302": mk(302)})
    fake.positions_rows = [held(301, 1), held(301, 2), held(302, 1), held(302, 2)]
    sent = notices(bot)
    done = await bot.live_redeem(NOW)
    assert len(done) == 1 and bot.live_redeem_last["counts"] == {"redeemed": 1, "lost": 1, "open": 2}, bot.live_redeem_last
    assert bot.store.get("live:redeemed:301:1")["lost"] is True and bot.store.get("live:redeemed:301:2")["tx"] == "0xredeem"
    fake.positions_rows = [row for row in fake.positions_rows if row["id"] != "p301:2"]  # (redeemed: Predict lists it no more)
    report = await bot.live_redeem_report(NOW + MIN)
    assert "结论：现在没有可领取的：赢的都领完了，剩下的还没结算，结算后自动领取" in report and "结算读不出" not in report, report
    await bot.live_redeem(NOW + 7 * 60 * MIN)
    assert [k for k, _ in sent] == [""] and bot.live_redeem_unclear == {}, sent  # the 💵 line alone
    print("RDM_OPEN_SHAPES_OK")


asyncio.run(open_shapes())


# --- RDM-YB-NEGRISK-APPROVAL ----------------------------------------------------------------------------------------------------
KEY, PA = T["KEY"], T["PA"]
SEL_APPROVED = L.Chain.encode("isApprovedForAll(address,address)", ["address", "address"], [PA, PA])[:4]
SEL_SET = L.Chain.encode("setApprovalForAll(address,bool)", ["address", "bool"], [PA, True])[:4]
# both SDKs' address tables, as data (predict_sdk constants.py / @predictdotfun/sdk Constants.js)
SDK = {56: {"CONDITIONAL_TOKENS": "0x22DA1810B194ca018378464a58f6Ac2B10C9d244", "NEG_RISK_CONDITIONAL_TOKENS": "0x22DA1810B194ca018378464a58f6Ac2B10C9d244",
            "YIELD_BEARING_CONDITIONAL_TOKENS": "0x9400F8Ad57e9e0F352345935d6D3175975eb1d9F",
            "YIELD_BEARING_NEG_RISK_CONDITIONAL_TOKENS": "0xF64b0b318AAf83BD9071110af24D24445719A07F"},
       97: {"CONDITIONAL_TOKENS": "0x2827AAef52D71910E8FBad2FfeBC1B6C2DA37743", "NEG_RISK_CONDITIONAL_TOKENS": "0x2827AAef52D71910E8FBad2FfeBC1B6C2DA37743",
            "YIELD_BEARING_CONDITIONAL_TOKENS": "0x38BF1cbD66d174bb5F3037d7068E708861D68D7f",
            "YIELD_BEARING_NEG_RISK_CONDITIONAL_TOKENS": "0x26e865CbaAe99b62fbF9D18B55c25B5E079A93D5"}}


class ApprovalChain(L.Chain):
    """The REAL check_approvals / set_approvals over a chain where nothing is approved: every isApprovedForAll read and every
    setApprovalForAll sent is recorded as (tokens contract, operator)."""
    def __init__(self, wallet):
        super().__init__("https://rpc.invalid", wallet)
        self.reads, self.sets = [], []

    async def call(self, to, data):
        if data[:4] == SEL_APPROVED:
            self.reads.append((to.lower(), "0x" + data[4 + 32 + 12:4 + 64].hex()))
        return b"\x00" * 32  # nothing approved, no allowance

    async def send(self, to, data, value=0, **kw):
        if data[:4] == SEL_SET:
            self.sets.append((to.lower(), "0x" + data[4 + 12:4 + 32].hex()))
        return "0x" + "11" * 32

    async def wait(self, tx_hash, timeout=120.0):
        return {"blockNumber": "0x1", "status": "0x1", "logs": []}


async def yb_negrisk_approval():
    """RDM-YB-NEGRISK-APPROVAL: on the yield-bearing track all three outcome-token approvals were read and set on
    YIELD_BEARING_CONDITIONAL_TOKENS; both SDKs put the multi-outcome exchange and the adapter on the track's multi-outcome
    tokens, YIELD_BEARING_NEG_RISK_CONDITIONAL_TOKENS — a different contract on chains 56 and 97. --approve signed two grants
    neither SDK sends (and never the one a yield-bearing multi-outcome redeem needs), --live-check read OK for it. The steps
    now name the SDKs' pairs; the labels, the USDT steps and the standard track (one address) are as they were."""
    assert SDK[56]["YIELD_BEARING_NEG_RISK_CONDITIONAL_TOKENS"].lower() != SDK[56]["YIELD_BEARING_CONDITIONAL_TOKENS"].lower()
    for chain_id in (56, 97):
        for account in ("", PA):
            for yb in (True, False):
                chain = ApprovalChain(L.Wallet(KEY, chain_id, account))
                a = L.ADDRESSES[chain_id]
                p = "YIELD_BEARING_" if yb else ""
                pairs = [(SDK[chain_id][p + "CONDITIONAL_TOKENS"].lower(), a[p + "CTF_EXCHANGE"].lower()),
                         (SDK[chain_id][p + "NEG_RISK_CONDITIONAL_TOKENS"].lower(), a[p + "NEG_RISK_CTF_EXCHANGE"].lower()),
                         (SDK[chain_id][p + "NEG_RISK_CONDITIONAL_TOKENS"].lower(), a[p + "NEG_RISK_ADAPTER"].lower())]
                word = "收益型" if yb else ""
                assert [label for label, _, _ in chain.approval_steps(yb)] == [
                    f"{word}结果代币 → 交易所", f"{word}结果代币 → 多结果交易所", f"{word}结果代币 → 多结果适配器",
                    f"{word}USDT 额度 → 交易所", f"{word}USDT 额度 → 多结果交易所"]
                assert [ok for _, ok in await chain.check_approvals(yb)] == [False] * 5
                assert chain.reads == pairs, (chain_id, account, yb, chain.reads)  # (before: the yield-bearing track read 0x9400… three times)
                steps = await chain.set_approvals(yb)
                assert [outcome.startswith("已完成") for _, outcome in steps] == [True] * 5, steps
                assert chain.sets == pairs, (chain_id, account, yb, chain.sets)
        assert L.ADDRESSES[chain_id]["NEG_RISK_CONDITIONAL_TOKENS"] == SDK[chain_id]["NEG_RISK_CONDITIONAL_TOKENS"]
        assert L.tokens_key(True, False) in L.ADDRESSES[chain_id] and L.tokens_key(True, True) in L.ADDRESSES[chain_id]
    print("RDM_YB_NEGRISK_APPROVAL_OK")


asyncio.run(yb_negrisk_approval())


# --- RDM-REVERT-REASON-DROPPED --------------------------------------------------------------------------------------------------
class RpcNode:
    """A BNB JSON-RPC node in miniature under the REAL Chain.rpc (main._http_get replaced while ``with node:``): it answers
    by method — a value is the result, {"error": {...}} the error object, bytes a raw body, an exception is raised (the
    request failed on the way), a callable decides per call from the params — and logs every (method, params)."""
    def __init__(self, **answers):
        self.answers, self.log, self.plain = dict(answers), [], None

    def __call__(self, url, payload=None, timeout=15, headers=None):
        method, params = payload["method"], payload["params"]
        self.log.append((method, params))
        answer = self.answers[method]
        if callable(answer):
            answer = answer(params)
        if isinstance(answer, BaseException):
            raise answer
        if isinstance(answer, bytes):
            return answer
        if isinstance(answer, dict) and set(answer) == {"error"}:
            return json.dumps({"jsonrpc": "2.0", "id": 1, "error": answer["error"]}).encode()
        return json.dumps({"jsonrpc": "2.0", "id": 1, "result": answer}).encode()

    def methods(self):
        return [method for method, _ in self.log]

    def __enter__(self):
        self.plain, m._http_get = m._http_get, self
        return self

    def __exit__(self, *exc):
        m._http_get = self.plain


def revert(data, wording="upstream", reason=""):
    """eth_estimateGas reverting: go-ethereum keeps the revert bytes in ``data`` (and a reason string in the message);
    BNB Chain's client (1.5.16+) also repeats them after the message."""
    message = "execution reverted" + (f": {reason}" if reason else "")
    return {"error": {"code": 3, "message": message + (f": {data}" if wording == "bsc" else ""), "data": data}}


async def revert_reason():
    """RDM-REVERT-REASON-DROPPED: Chain.rpc kept only the error's message and dropped its data, so a redeem reverting in
    simulation with a custom error read the bare 「RPC 错误：execution reverted」 whatever the cause (missing approval, amount
    over the balance, the yield vault) — the only evidence there is, since such a redeem never leaves the bot. The selector
    is now kept, after the 120-character cut, with the error's name when both SDKs' ABIs declare it; a reason string, a
    Panic and the node's other answers read as before; the exception classes are untouched."""
    approval = "0xe237d922" + "00" * 12 + "ab" * 20 + "00" * 12 + "cd" * 20
    balance = "0x03dee4c5" + "00" * 128
    string = "0x08c379a0" + L.Chain.encode("x(string)", ["string"], ["result for condition not received yet"])[4:].hex()
    chain = L.Chain("https://rpc.example", L.Wallet(KEY, 56, PA))

    async def estimate_fails(answer) -> str:
        with RpcNode(eth_getTransactionCount="0x5", eth_gasPrice="0x5", eth_estimateGas=answer) as node:
            try:
                await chain.redeem("0x" + "ab" * 32, 1, 7 * WEI, False, False)
                assert False, "the estimate reverted"
            except L.TxNotSent as error:
                assert node.methods() == ["eth_getTransactionCount", "eth_gasPrice", "eth_estimateGas"], node.methods()
                assert error.retry_after == 0 and str(error) == chain.error
                return str(error)

    for wording in ("upstream", "bsc"):
        text = await estimate_fails(revert(approval, wording))
        assert text.startswith("RPC 错误：execution reverted（合约错误 0xe237d922 ERC1155MissingApprovalForAll") and text.count("e237d922") == 1, (wording, text)
        other = await estimate_fails(revert(balance, wording))
        assert "0x03dee4c5 ERC1155InsufficientBalance" in other and other != text, (wording, other)
        assert await estimate_fails(revert(string, wording, "result for condition not received yet")) == "RPC 错误：execution reverted: result for condition not received yet"
        assert await estimate_fails(revert("0xdeadbeef" + "00" * 32, wording)) == "RPC 错误：execution reverted（合约错误 0xdeadbeef）"
    # unchanged: no data, empty data, the node's other refusals; a long message keeps the selector (cut before it, not through it)
    assert await estimate_fails({"error": {"code": 3, "message": "execution reverted"}}) == "RPC 错误：execution reverted"
    assert await estimate_fails({"error": {"code": 3, "message": "execution reverted", "data": "0x"}}) == "RPC 错误：execution reverted"
    assert await estimate_fails({"error": {"code": -32000, "message": "nonce too low: next nonce 6, tx nonce 5"}}) == "RPC 错误：nonce too low: next nonce 6, tx nonce 5"
    long = await estimate_fails({"error": {"code": 3, "message": "execution reverted" + " x" * 150, "data": approval}})
    assert "0xe237d922 ERC1155MissingApprovalForAll" in m.clean_error(long) and len(long) <= 280, long
    assert await estimate_fails({"error": {"code": -32000}}) == "RPC 错误：code -32000"  # (before: 「RPC 错误：None」)
    # a read (eth_call) answered with a revert: a plain RemoteError carrying the same words
    with RpcNode(eth_call=revert(approval, "bsc")):
        try:
            await chain.usdt_balance()
            assert False
        except m.RemoteError as error:
            assert not isinstance(error, L.TxNotSent) and "（合约错误 0xe237d922" in str(error), str(error)
    print("RDM_REVERT_REASON_DROPPED_OK")


asyncio.run(revert_reason())


# --- RDM-UNSENT-AS-SENT ---------------------------------------------------------------------------------------------------------
def keccak_of(raw_hex: str) -> str:
    """The hash a node gives a raw transaction: keccak of its signed bytes."""
    return "0x" + L._eth()[5].keccak(bytes.fromhex(raw_hex.removeprefix("0x"))).hex()


def node_for(**over):
    """A healthy node for the REAL Chain: reads answered, every raw transaction taken (its hash) and mined at once."""
    answers = dict(eth_getTransactionCount="0x1", eth_gasPrice="0x3b9aca00", eth_estimateGas="0x30d40", eth_getBalance=hex(10 ** 17),
                   eth_call="0x" + "0" * 64, eth_sendRawTransaction=lambda params: keccak_of(params[0]),
                   eth_getTransactionReceipt={"blockNumber": "0x10", "status": "0x1", "logs": []}, eth_getTransactionByHash=None)
    answers.update(over)
    return RpcNode(**answers)


def refusal(message):
    return {"error": {"code": -32000, "message": message}}


async def unsent_as_sent():
    """RDM-UNSENT-AS-SENT: Chain.send turned only its three reads into TxNotSent; the node's ANSWER refusing the raw
    transaction (insufficient funds, underpriced, a full pool…) — nothing broadcast, nothing spent — was a plain RemoteError,
    i.e. a sent failure: an hour's wait and 一小时后自动再试, also when the wallet was topped up minutes later. A refusal is now
    TxNotSent (five minutes); "already known" is this very transaction (sent), "nonce too low" is looked up by its hash; a
    request that failed on the way stays a sent failure (it may have been taken); never a second raw transaction in one call.
    A refusal for the gas makes the round's winners 可领取 under the ⛽ note, not one refused send and 没领成 per winner."""
    chain = L.Chain("https://rpc.invalid", L.Wallet(KEY, 56, PA))
    tokens = L.ADDRESSES[56]["CONDITIONAL_TOKENS"]

    async def send(node):
        with node:
            try:
                return await chain.send(tokens, b"\xaa\xbb")
            finally:
                assert node.methods().count("eth_sendRawTransaction") == 1, ("one raw transaction per call", node.methods())

    for said in ("insufficient funds for gas * price + value: balance 1, tx cost 2, overshot 1", "replacement transaction underpriced",
                 "transaction underpriced: tip needed 1, tip permitted 0", "intrinsic gas too low", "txpool is full"):
        try:
            await send(node_for(eth_sendRawTransaction=refusal(said)))
            assert False, said
        except L.TxNotSent as error:  # (before: a plain RemoteError — the hour)
            assert str(error) == f"RPC 错误：{said}", str(error)
    # "already known": this very transaction is in the node — sent; its hash is the keccak of the signed bytes
    node = node_for(eth_sendRawTransaction=refusal("already known"))
    tx = await send(node)
    raw = next(params[0] for method, params in node.log if method == "eth_sendRawTransaction")
    assert tx == keccak_of(raw), tx
    # "nonce too low": looked up by its hash — this transaction (taken: sent) or another one (never left: TxNotSent)
    node = node_for(eth_sendRawTransaction=refusal("nonce too low: next nonce 6, tx nonce 5"), eth_getTransactionByHash={"hash": "0x…"})
    assert await send(node) == keccak_of(next(p[0] for meth, p in node.log if meth == "eth_sendRawTransaction"))
    assert node.methods()[-1] == "eth_getTransactionByHash" and node.log[-1][1] == [keccak_of(next(p[0] for meth, p in node.log if meth == "eth_sendRawTransaction"))]
    try:
        await send(node_for(eth_sendRawTransaction=refusal("nonce too low: next nonce 6, tx nonce 5")))
        assert False
    except L.TxNotSent as error:
        assert "nonce too low" in str(error)
    # unchanged: a request that failed on the way (it may have been taken) and an answer not known to be a refusal — sent failures
    for answer in (m.RemoteError("网络错误 (TimeoutError: timed out)"), m.RemoteError("HTTP 503: Service Unavailable"), b"<html>bad gateway</html>",
                   refusal("internal error")):
        try:
            await send(node_for(eth_sendRawTransaction=answer))
            assert False, answer
        except m.RemoteError as error:
            assert not isinstance(error, L.TxNotSent), (answer, error)
    assert (await send(node_for(eth_sendRawTransaction="0x" + "ab" * 32))) == "0x" + "ab" * 32  # the node's answer, as before
    node = node_for(eth_sendRawTransaction=None)  # a node answering null: the hash of what it was given (before: "None")
    assert await send(node) == keccak_of(next(p[0] for meth, p in node.log if meth == "eth_sendRawTransaction"))
    for read in ("eth_getTransactionCount", "eth_estimateGas"):  # a read answering null: nothing sent (before: ValueError — the hour)
        with node_for(**{read: None}) as node:
            try:
                await chain.send(tokens, b"\xaa\xbb")
                assert False
            except L.TxNotSent:
                assert "eth_sendRawTransaction" not in node.methods()

    # the round: the node refuses the first redeem for its gas (the wallet over the 0.0005 floor, but short at this gas price)
    bot = await ready_bot()
    fake = bot.live.api
    fake.positions_rows = [held(mid) for mid in (601, 602, 603)]
    fake.markets.update({str(mid): mk(mid, won=1) for mid in (601, 602, 603)})
    bot.live.chain = L.Chain("https://rpc.invalid", bot.live.wallet)
    sent = notices(bot)
    dry = refusal("insufficient funds for gas * price + value: balance 600000000000000, tx cost 750000000000000, overshot 150000000000000")
    with node_for(eth_sendRawTransaction=dry) as node:
        assert await bot.live_redeem(NOW) == []
    assert node.methods().count("eth_sendRawTransaction") == 1, ("one refused send, not one per winner", node.methods())
    last = bot.live_redeem_last
    assert last["counts"] == {"claimable": 3} and last["low_gas"] and bot.live_redeem_failed == {}, (last, bot.live_redeem_failed)
    assert not any(bot.store.get(f"live:redeemed:{mid}:1") for mid in (601, 602, 603)), "no mark"
    assert [k for k, _ in sent] == ["redeem:gas"] and "节点拒收" in sent[0][1] and bot.live.wallet.signer in sent[0][1], sent  # not 没领成…一小时后
    assert "不够付领取交易的 gas（节点拒收）" in bot.live_redeem_summary(last), bot.live_redeem_summary(last)
    with node_for() as node:
        report = await bot.live_redeem_report(NOW + MIN)
    assert "结论：上一轮节点以 gas 不够拒收了领取交易" in report and bot.live.wallet.signer in report, report
    with node_for() as node:  # topped up: the next round redeems all three, at once (no hour)
        done = await bot.live_redeem(NOW + 10 * MIN)
    assert len(done) == 3 and node.methods().count("eth_sendRawTransaction") == 3, (done, bot.live_redeem_last)
    # a stale pending-nonce read (another transaction took the nonce): never left — five minutes, not an hour
    bot = await ready_bot()
    fake = bot.live.api
    fake.positions_rows, fake.markets["604"] = [held(604)], mk(604, won=1)
    bot.live.chain = L.Chain("https://rpc.invalid", bot.live.wallet)
    sent = notices(bot)
    with node_for(eth_sendRawTransaction=refusal("nonce too low: next nonce 6, tx nonce 5")):
        assert await bot.live_redeem(NOW) == []
    assert bot.live_redeem_last["counts"] == {"failed": 1} and bot.live_errors[-1].endswith("（交易没发出，5 分钟后再试）"), bot.live_errors[-1:]
    left = bot.LIVE_REDEEM_RETRY_SECONDS - (time.monotonic() - bot.live_redeem_failed["live:redeemed:604:1"])
    assert left <= bot.LIVE_REDEEM_UNSENT_RETRY_SECONDS, left
    assert [k for k, _ in sent] == ["redeem:fail"] and "5 分钟后自动再试" in sent[0][1] and bot.store.get("live:redeemed:604:1") is None, sent
    bot.live_redeem_failed.clear()  # (the five minutes over)
    with node_for():
        assert len(await bot.live_redeem(NOW + 6 * MIN)) == 1
    print("RDM_UNSENT_AS_SENT_OK")


asyncio.run(unsent_as_sent())


# --- RDM-RECEIPT-POLL (the failed poll) -----------------------------------------------------------------------------------------
def polls(*answers):
    """eth_getTransactionReceipt answering ``answers`` one after another (the last one for ever after)."""
    queue = list(answers)
    return lambda params: queue.pop(0) if len(queue) > 1 else queue[0]


MINED = {"blockNumber": "0x10", "status": "0x1", "logs": []}


async def receipt_poll():
    """RDM-RECEIPT-POLL: one receipt poll that failed (a timeout, HTTP 429 / 503, a body that is no JSON, a JSON-RPC error answer
    — a POST the HTTP layer never repeats) ended Chain.wait, and a redeem that had been sent and was mined a block later was booked
    as failed: no mark, no 💵 line, a 没领成 alert, an hour's back-off. A failed poll now means "no receipt yet": asked again to the
    same deadline. A revert still ends the wait at once, the deadline is still TxPending, a cancel still ends it."""
    chain = L.Chain("https://rpc.invalid", L.Wallet(KEY, 56, PA))
    chain.RECEIPT_POLL_SECONDS = 0
    for trouble in (m.RemoteError("网络错误 (TimeoutError: timed out)"), m.RemoteError("HTTP 503: 接口请求失败"),
                    m.RemoteError("HTTP 429: 接口限流，等待后重试"), b"<html>502</html>", {"error": {"code": -32005, "message": "limit exceeded"}}):
        with RpcNode(eth_getTransactionReceipt=polls(trouble, MINED)) as node:
            assert (await chain.wait("0xtxhash"))["status"] == "0x1", trouble  # (before: the RemoteError of poll 1)
        assert node.methods() == ["eth_getTransactionReceipt"] * 2, node.methods()
    # unchanged: a revert at once; no receipt to the deadline is TxPending (now saying why it was not read); a cancel ends the wait
    with RpcNode(eth_getTransactionReceipt=polls(m.RemoteError("HTTP 503: 接口请求失败"), {"blockNumber": "0x10", "status": "0x0"})):
        try:
            await chain.wait("0xtxhash")
            assert False
        except m.RemoteError as error:
            assert str(error) == "交易失败（已回滚）：0xtxhash" and not isinstance(error, L.TxPending)
    with RpcNode(eth_getTransactionReceipt=polls(None, m.RemoteError("HTTP 503: 接口请求失败"))) as node:
        try:
            await chain.wait("0xtxhash", timeout=0.05)
            assert False
        except L.TxPending as error:
            assert str(error).startswith("交易 0xtxhash 在 0 秒内未上链（回执读取失败：HTTP 503") and error.tx_hash == "0xtxhash", str(error)
    with RpcNode(eth_getTransactionReceipt=None):
        task = asyncio.create_task(chain.wait("0xtxhash"))
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
            assert False
        except asyncio.CancelledError:
            pass
    # the round: sent, mined, poll 1 failed — 💵 with the hash and the mark, no failure, no alert, one transaction
    bot = await ready_bot()
    fake = bot.live.api
    fake.positions_rows, fake.markets["701"] = [held(701)], mk(701, won=1)
    bot.live.chain = L.Chain("https://rpc.invalid", bot.live.wallet)
    bot.live.chain.RECEIPT_POLL_SECONDS = 0
    sent = notices(bot)
    with node_for(eth_getTransactionReceipt=polls(m.RemoteError("HTTP 503: 接口请求失败"), MINED)) as node:
        done = await bot.live_redeem(NOW)
    tx = keccak_of(next(p[0] for meth, p in node.log if meth == "eth_sendRawTransaction"))
    assert done == [f"💵 已领取 window 701 Up 10 份：{tx}"], (done, bot.live_errors[-1:])  # (before: [] — 领取失败, an hour)
    assert bot.store.get("live:redeemed:701:1")["tx"] == tx and bot.live_redeem_failed == {} and node.methods().count("eth_sendRawTransaction") == 1
    assert [k for k, _ in sent] == [""] and "redeem:fail" not in [k for k, _ in sent], sent
    print("RDM_RECEIPT_POLL_OK")


asyncio.run(receipt_poll())


# --- RDM-PAYOUT-UNCHECKED -------------------------------------------------------------------------------------------------------
TOKENS_TOPIC = "0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d"   # keccak of both SDK ABIs' PayoutRedemption
ADAPTER_TOPIC = "0x9140a6a270ef945260c03894b3c6b3b2695e9d5101feef0ff24fec960cfd3224"


def pad(address):
    return "0x" + "0" * 24 + address[2:].lower()


def payout_log(contract, redeemer, payout, cid, neg_risk=False, index_sets=(1,), amounts=(0, 0)):
    """A PayoutRedemption as the contract emits it: the tokens contract's (redeemer, collateral, parent indexed; data
    conditionId, indexSets, payout) or the adapter's (redeemer, conditionId indexed; data amounts, payout)."""
    if neg_risk:
        data = L.Chain.encode("x(uint256[],uint256)", ["uint256[]", "uint256"], [list(amounts), payout])[4:]
        return {"address": contract, "topics": [ADAPTER_TOPIC, pad(redeemer), cid], "data": "0x" + data.hex()}
    data = L.Chain.encode("x(bytes32,uint256[],uint256)", ["bytes32", "uint256[]", "uint256"], [bytes.fromhex(cid[2:]), list(index_sets), payout])[4:]
    return {"address": contract, "topics": [TOKENS_TOPIC, pad(redeemer), pad(L.ADDRESSES[56]["USDT"]), "0x" + "00" * 32], "data": "0x" + data.hex()}


async def payout_unchecked():
    """RDM-PAYOUT-UNCHECKED: 💵 已领取 was said, and the position closed for good, on a mined receipt alone — a redeem of a balance
    already gone (claimed on the website a moment before, an earlier transaction the bot did not book) is mined and pays nothing,
    yet read as paid. The payout is read from the receipt the redeem has anyway (its PayoutRedemption for this account, from the
    contract called: no extra request): a zero is said as such (and still marked: unmarked it would be redeemed again, for
    nothing); a payout says 到账 $X; a receipt that says nothing reads as before."""
    ct, adapter = L.ADDRESSES[56]["CONDITIONAL_TOKENS"], L.ADDRESSES[56]["NEG_RISK_ADAPTER"]
    other = "0x" + "77" * 20

    async def one_round(mid, logs, neg=False, rounds=1):
        bot = await ready_bot()
        fake = bot.live.api
        fake.positions_rows, fake.markets[str(mid)] = [held(mid)], {**mk(mid, won=1), "isNegRisk": neg}
        bot.live.chain = L.Chain("https://rpc.invalid", bot.live.wallet)
        sent = notices(bot)
        with node_for(eth_getTransactionReceipt={**MINED, "logs": logs}) as node:
            done = await bot.live_redeem(NOW)
            for i in range(1, rounds):
                assert await bot.live_redeem(NOW + i * 10 * MIN) == []
        txs = [keccak_of(p[0]) for meth, p in node.log if meth == "eth_sendRawTransaction"]
        return bot, done, sent, txs

    maker = (await ready_bot()).live.wallet.maker
    # (a) mined, paid nothing (a decoy: another redeemer's event, 999 USDT) — said, not 💵; marked; never sent again
    bot, done, sent, txs = await one_round(801, [payout_log(ct, other, 999 * WEI, cond(801)), payout_log(ct, maker, 0, cond(801))], rounds=3)
    assert len(txs) == 1 and len(done) == 1 and "💵" not in done[0] and "没领到 USDT" in done[0] and txs[0] in done[0], done  # (before: 💵 已领取 …)
    assert [text for _, text in sent] == done and "没领到 USDT" in bot.live_errors[-1], (sent, bot.live_errors[-1:])
    assert bot.store.get("live:redeemed:801:1") == {"at": NOW, "tx": txs[0], "amount": str(10 * WEI), "title": "window 801", "paid": "0"}
    assert bot.live_redeem_last["counts"] == {"kept": 1}, bot.live_redeem_last
    with node_for():
        report = await bot.live_redeem_report(NOW + 31 * MIN)
    assert "没领到 USDT" in report, report
    # (b) paid: 到账 $X in the 💵 line
    bot, done, sent, txs = await one_round(802, [payout_log(ct, maker, 10 * WEI, cond(802), index_sets=(1,))])
    assert done == [f"💵 已领取 window 802 Up 10 份，到账 $10.00：{txs[0]}"] and bot.store.get("live:redeemed:802:1")["paid"] == str(10 * WEI), done
    # (c) a receipt that says nothing: the line and the mark as before (None is never 0)
    bot, done, sent, txs = await one_round(803, [])
    assert done == [f"💵 已领取 window 803 Up 10 份：{txs[0]}"] and "paid" not in bot.store.get("live:redeemed:803:1"), done
    # (d) multi-outcome: the adapter's event (the tokens contract's own one names the adapter as redeemer and pays it — not ours)
    bot, done, sent, txs = await one_round(804, [payout_log(ct, adapter, 10 * WEI, cond(804)),
                                                 payout_log(adapter, maker, 0, cond(804), neg_risk=True, amounts=(10 * WEI, 0))], neg=True)
    assert len(done) == 1 and "没领到 USDT" in done[0], done
    bot, done, sent, txs = await one_round(805, [payout_log(adapter, maker, 10 * WEI, cond(805), neg_risk=True, amounts=(10 * WEI, 0))], neg=True)
    assert done == [f"💵 已领取 window 805 Up 10 份，到账 $10.00：{txs[0]}"], done
    # the decoder: the event's last FIELD (a last-word reading gives the index set 2, or the amount 0)
    chain = L.Chain("https://rpc.invalid", L.Wallet(KEY, 56, PA))
    receipt = {"logs": [payout_log(ct, PA, 40 * WEI, cond(1), index_sets=(2,))]}
    assert chain.redeem_payout(receipt, ct, False) == 40 * WEI and chain.redeem_payout(receipt, adapter, False) is None
    receipt = {"logs": [payout_log(adapter, PA, 60 * WEI, cond(1), neg_risk=True, amounts=(60 * WEI, 0))]}
    assert chain.redeem_payout(receipt, adapter, True) == 60 * WEI and chain.redeem_payout(receipt, ct, False) is None
    assert chain.redeem_payout({"logs": [{"address": ct, "topics": [TOKENS_TOPIC, pad(PA)], "data": "0x12"}]}, ct, False) is None
    assert chain.redeem_payout({"status": "0x1"}, ct, False) is None and chain.redeem_payout(None, ct, False) is None
    print("RDM_PAYOUT_UNCHECKED_OK")


asyncio.run(payout_unchecked())


# --- RDM-RECEIPT-POLL (the deadline, the lost answer) + RDM-RESTART-MID-SEND: the sent note -------------------------------------
class QuickChain(L.Chain):
    """The REAL Chain whose receipt wait ends after ``wait_s`` (the real one polls 120 s) and polls every 10 ms."""
    RECEIPT_POLL_SECONDS = 0.01

    def __init__(self, wallet, wait_s=0.05):
        super().__init__("https://rpc.invalid", wallet)
        self.wait_s = wait_s

    async def wait(self, tx_hash, timeout=120.0):
        return await super().wait(tx_hash, timeout=self.wait_s if self.wait_s is not None else timeout)


def make_bot_on(store, **env):
    """make_bot over ``store`` (a database file: a new Store over the same file is the next process's)."""
    plain = m.Store
    m.Store = lambda path: store
    try:
        return make_bot(**env)
    finally:
        m.Store = plain


async def sent_note():
    """RDM-RECEIPT-POLL + RDM-RESTART-MID-SEND: nothing was recorded while a redeem transaction was out. A receipt that did not
    come within the wait, or an answer to the raw transaction lost on the way, was booked as a failure with the hash forgotten
    (no mark, no 💵, an hour later a second redeem paying nothing); a restart (every deploy) before the mark made the next
    process redeem the still-listed position again — the empty second transaction announced 💵 (neg-risk: 没领成 for a paid
    position). The hash is noted on disk before the broadcast, and every round asks for its receipt before anything is sent for
    that position: status 1 is booked with THAT hash (listed or not), 0 is a failure, none leaves the position alone; only a
    note an hour old whose transaction the node does not know at all is let go, and the position redeemed again."""
    sends = lambda node: [keccak_of(p[0]) for meth, p in node.log if meth == "eth_sendRawTransaction"]
    # (a) the receipt did not come in time: failed this round (its hour), the note kept; mined since — the next round books it
    #     with ITS hash and what it paid, though Predict no longer lists the row; one transaction in all
    bot = await ready_bot()
    fake = bot.live.api
    fake.positions_rows, fake.markets["901"] = [held(901)], mk(901, won=1)
    bot.live.chain = QuickChain(bot.live.wallet)
    sent = notices(bot)
    with node_for(eth_getTransactionReceipt=None) as node:
        assert await bot.live_redeem(NOW) == []
    first = sends(node)
    assert len(first) == 1 and bot.live_redeem_last["counts"] == {"failed": 1} and "在 0 秒内未上链" in bot.live_errors[-1], bot.live_errors[-1:]
    note = bot.store.get("live:redeem-sent:901:1")
    assert note["tx"] == first[0] and note["at"] == NOW and note["amount"] == str(10 * WEI) and not note["neg_risk"], note  # (before: nothing)
    fake.positions_rows = []  # (redeemed: Predict lists it no more)
    paid = {**MINED, "logs": [payout_log(L.ADDRESSES[56]["CONDITIONAL_TOKENS"], bot.live.wallet.maker, 10 * WEI, cond(901))]}
    with node_for(eth_getTransactionReceipt=paid) as node:
        done = await bot.live_redeem(NOW + 10 * MIN)
    assert done == [f"💵 已领取 window 901 Up 10 份，到账 $10.00：{first[0]}"] and sends(node) == [], done  # (before: [] — no 💵, ever)
    assert bot.store.get("live:redeemed:901:1")["tx"] == first[0] and bot.store.get("live:redeem-sent:901:1") is None
    assert bot.live_redeem_failed == {} and bot.live_redeem_errors == {}, bot.live_redeem_failed
    # (b) the answer to the raw transaction lost on the way (the node took it): failed (an hour, as before), the note kept with the
    #     hash of the signed bytes; the next round finds it mined and books it — never a second transaction
    bot = await ready_bot()
    fake = bot.live.api
    fake.positions_rows, fake.markets["902"] = [held(902)], mk(902, won=1)
    bot.live.chain = QuickChain(bot.live.wallet)
    with node_for(eth_sendRawTransaction=lambda params: m.RemoteError("网络错误 (TimeoutError: timed out)")) as node:
        assert await bot.live_redeem(NOW) == []
    raw = sends(node)
    assert len(raw) == 1 and bot.store.get("live:redeem-sent:902:1")["tx"] == raw[0] and bot.live_errors[-1].endswith("（一小时后再试）")
    with node_for() as node:
        assert await bot.live_redeem(NOW + 10 * MIN) == [f"💵 已领取 window 902 Up 10 份：{raw[0]}"] and sends(node) == []
    # (c) no receipt yet: nothing sent beside it — the hour, and while the node still has it in its pool after that; an hour on
    #     and unknown to the node (dropped / never broadcast): let go, redeemed once more
    bot = await ready_bot()
    fake = bot.live.api
    fake.positions_rows, fake.markets["903"] = [held(903)], mk(903, won=1)
    bot.live.chain = QuickChain(bot.live.wallet)
    with node_for(eth_getTransactionReceipt=None) as node:
        await bot.live_redeem(NOW)
        old = sends(node)[0]
        bot.live_redeem_failed.clear()  # (as after a restart: the back-offs live in memory, the note on disk)
        assert await bot.live_redeem(NOW + MIN) == [] and len(sends(node)) == 1
        assert bot.live_redeem_last["counts"] == {"sent": 1}, bot.live_redeem_last
        node.answers["eth_getTransactionByHash"] = {"hash": old}  # an hour on, still in the node's pool
        assert await bot.live_redeem(NOW + 61 * MIN) == [] and len(sends(node)) == 1 and bot.live_redeem_last["counts"] == {"sent": 1}
        report = await bot.live_redeem_report(NOW + 61 * MIN + 1000)
        assert f"领取交易已发出、还没确认上链（下一轮先查回执，不会再发）：" in report and old in report and "节点里还在排队" not in report, report
        assert "结论：有领取交易已经发出、还没确认上链" in report and bot.store.get("live:redeem-sent:903:1")["tx"] == old, report  # (the check reads no receipt, writes nothing)
        node.answers["eth_getTransactionByHash"] = None  # dropped
        node.answers["eth_getTransactionReceipt"] = lambda params: None if params[0] == old else MINED  # (the new one is mined)
        node.answers["eth_getTransactionCount"] = "0x2"  # (the pending nonce moved on: a new transaction, not the same bytes again)
        done = await bot.live_redeem(NOW + 62 * MIN)
    assert len(sends(node)) == 2 and done == [f"💵 已领取 window 903 Up 10 份：{sends(node)[1]}"] and bot.store.get("live:redeem-sent:903:1") is None, (done, bot.live_redeem_last)
    assert sends(node)[1] != old and "一小时没上链、节点里也查不到：重新领取" in bot.live_errors[-1], bot.live_errors[-2:]
    # (d) a note whose transaction was mined and reverted: a failure (its hour), the note gone; a revert seen by the wait: the same
    bot = await ready_bot()
    fake = bot.live.api
    fake.positions_rows, fake.markets["904"] = [held(904)], mk(904, won=1)
    bot.live.chain = QuickChain(bot.live.wallet)
    with node_for(eth_getTransactionReceipt=None):
        await bot.live_redeem(NOW)
    bot.live_redeem_failed.clear()
    with node_for(eth_getTransactionReceipt={"blockNumber": "0x10", "status": "0x0"}) as node:
        assert await bot.live_redeem(NOW + 10 * MIN) == [] and sends(node) == []
    assert bot.live_redeem_last["counts"] == {"failed": 1} and "交易失败（已回滚）" in bot.live_errors[-1] and bot.store.get("live:redeem-sent:904:1") is None
    assert "live:redeemed:904:1" in bot.live_redeem_failed and bot.store.get("live:redeemed:904:1") is None
    bot.live_redeem_failed.clear()
    with node_for(eth_getTransactionReceipt={"blockNumber": "0x10", "status": "0x0"}) as node:
        assert await bot.live_redeem(NOW + 80 * MIN) == [] and len(sends(node)) == 1
    assert bot.store.get("live:redeem-sent:904:1") is None and bot.live_errors[-1].endswith("（一小时后再试）")
    # ...and a raw transaction the node refused (nothing left the bot): no note either
    bot.live_redeem_failed.clear()
    with node_for(eth_sendRawTransaction=refusal("replacement transaction underpriced")):
        await bot.live_redeem(NOW + 90 * MIN)
    assert bot.store.get("live:redeem-sent:904:1") is None and bot.live_errors[-1].endswith("（交易没发出，5 分钟后再试）")
    # (e) a restart while the receipt was awaited: the next process finds the note and books the first transaction (standard), or
    #     sends nothing and books it (neg-risk: before, a 没领成 for a paid position) — one transaction in all
    for neg in (False, True):
        with tempfile.TemporaryDirectory() as folder:
            path = str(pathlib.Path(folder) / "bot.sqlite3")
            store1 = m.Store(path)
            bot1 = make_bot_on(store1)
            await bot1.live_prepare()
            mid = "905" if not neg else "906"
            bot1.live.api.positions_rows, bot1.live.api.markets[mid] = [held(mid)], {**mk(mid, won=1), "isNegRisk": neg}
            bot1.live.chain = QuickChain(bot1.live.wallet, wait_s=None)  # the real 120 s wait, polled every 10 ms
            with node_for(eth_getTransactionReceipt=None) as node:
                task = asyncio.create_task(bot1.live_redeem(NOW))
                while "eth_getTransactionReceipt" not in node.methods():
                    await asyncio.sleep(0.01)
                task.cancel()  # the deploy: the process stops mid-wait
                try:
                    await task
                    assert False
                except asyncio.CancelledError:
                    pass
            first = sends(node)
            assert len(first) == 1 and store1.get(f"live:redeemed:{mid}:1") is None
            store2 = m.Store(path)  # the next process, the same database file
            bot2 = make_bot_on(store2)
            await bot2.live_prepare()
            bot2.live.api.positions_rows, bot2.live.api.markets[mid] = [held(mid)], {**mk(mid, won=1), "isNegRisk": neg}
            bot2.live.chain = QuickChain(bot2.live.wallet)
            sent = notices(bot2)
            target = L.ADDRESSES[56]["NEG_RISK_ADAPTER" if neg else "CONDITIONAL_TOKENS"]
            log = payout_log(target, bot2.live.wallet.maker, 10 * WEI, cond(mid), neg_risk=neg, amounts=(10 * WEI, 0))
            with node_for(eth_getTransactionReceipt={**MINED, "logs": [log]}) as node:
                done = await bot2.live_redeem(NOW + 30_000)
            assert done == [f"💵 已领取 window {mid} Up 10 份，到账 $10.00：{first[0]}"] and sends(node) == [], (neg, done)  # (before: a 2nd transaction)
            assert "eth_estimateGas" not in node.methods() and store2.get(f"live:redeemed:{mid}:1")["tx"] == first[0], node.methods()
            assert not any("没领成" in text for _, text in sent), sent
            store1.close()
            store2.close()
    print("RDM_RECEIPT_POLL_RESTART_MID_SEND_OK")


asyncio.run(sent_note())


# --- RDM-NEGRISK-ADAPTER-UNCHECKED ----------------------------------------------------------------------------------------------
async def adapter_unchecked():
    """RDM-NEGRISK-ADAPTER-UNCHECKED: a neg-risk redeem goes through the adapter, which needs the account's ERC-1155 approval on
    the track's multi-outcome tokens (both SDKs' REDEEM scope); nothing read or set it, and a missing one reverted the estimate
    every round as the bare 「RPC 错误：execution reverted」. After such a failure the approval is read once and, missing, named
    with the remedy (python main.py --approve) — never sent automatically; a redeem that goes through reads nothing."""
    chain = L.Chain("https://rpc.invalid", L.Wallet(KEY, 56, PA))
    blob = "0xe237d922" + "00" * 64
    word = lambda bit: "0x" + str(bit).rjust(64, "0")

    def reads(node):
        return [p[0] for meth, p in node.log if meth == "eth_call"]

    for yb, tokens, adapter in ((False, "CONDITIONAL_TOKENS", "NEG_RISK_ADAPTER"),
                                (True, "YIELD_BEARING_NEG_RISK_CONDITIONAL_TOKENS", "YIELD_BEARING_NEG_RISK_ADAPTER")):
        with node_for(eth_estimateGas=revert(blob), eth_call=word(0)) as node:
            try:
                await chain.redeem("0x" + "ab" * 32, 2, 7 * WEI, True, yb)
                assert False
            except L.TxNotSent as error:
                text = str(error)  # (before: 「RPC 错误：execution reverted（合约错误 0xe237d922 …）」 alone, no read)
            assert text.startswith("多结果适配器没有这个账户结果代币的操作权") and "python main.py --approve" in text and "0xe237d922" in text, text
            assert len(reads(node)) == 1 and reads(node)[0]["to"] == L.ADDRESSES[56][tokens], reads(node)
            data = reads(node)[0]["data"]
            assert data.startswith("0xe985e9c5") and PA[2:].lower() in data and data.endswith(L.ADDRESSES[56][adapter][2:].lower()), data
            assert "eth_sendRawTransaction" not in node.methods()
        # in place, or the read fails: the error as it was
        for answer in (word(1), m.RemoteError("HTTP 503: 接口请求失败")):
            with node_for(eth_estimateGas=revert(blob), eth_call=answer):
                try:
                    await chain.redeem("0x" + "ab" * 32, 2, 7 * WEI, True, yb)
                    assert False
                except L.TxNotSent as error:
                    assert str(error).startswith("RPC 错误：execution reverted（合约错误 0xe237d922"), str(error)
        # a redeem that goes through reads nothing; a binary redeem whose estimate reverts reads nothing either
        with node_for() as node:
            await chain.redeem("0x" + "ab" * 32, 2, 7 * WEI, True, yb)
            assert reads(node) == [] and node.methods().count("eth_sendRawTransaction") == 1
        with node_for(eth_estimateGas=revert(blob)) as node:
            try:
                await chain.redeem("0x" + "ab" * 32, 2, 7 * WEI, False, yb)
                assert False
            except L.TxNotSent:
                assert reads(node) == []
    # the round: the five-minute retry as before (nothing sent, no mark), the notice naming the remedy
    bot = await ready_bot()
    fake = bot.live.api
    fake.positions_rows, fake.markets["951"] = [held(951)], {**mk(951, won=1), "isNegRisk": True}
    bot.live.chain = QuickChain(bot.live.wallet)
    sent = notices(bot)
    with node_for(eth_estimateGas=revert(blob), eth_call=word(0)) as node:
        assert await bot.live_redeem(NOW) == [] and "eth_sendRawTransaction" not in node.methods()
    assert bot.live_errors[-1].endswith("（交易没发出，5 分钟后再试）") and "多结果适配器没有这个账户结果代币的操作权" in bot.live_errors[-1]
    assert [k for k, _ in sent] == ["redeem:fail"] and "python main.py --approve" in sent[0][1] and bot.store.get("live:redeemed:951:1") is None, sent
    print("RDM_NEGRISK_ADAPTER_UNCHECKED_OK")


asyncio.run(adapter_unchecked())


# --- RDM-NEGRISK-AMOUNT ---------------------------------------------------------------------------------------------------------
class HeldChain(T["FakeChain"]):
    """test_trading's FakeChain with the account's on-chain balances of outcome tokens (``held``: token id -> wei; a value
    that is an exception is raised); every read logged."""
    def __init__(self, held=None):
        super().__init__()
        self.held, self.reads = dict(held or {}), []

    async def outcome_balance(self, token, neg_risk, yield_bearing, owner=None):
        self.reads.append((str(token), neg_risk, yield_bearing))
        found = self.held.get(str(token), 0)
        if isinstance(found, BaseException):
            raise found
        return found


async def negrisk_amount():
    """RDM-NEGRISK-AMOUNT: a multi-outcome redeem named Predict's listed amount; the adapter pulls exactly that — one wei more
    than held reverted the estimate every round, less left the rest on the chain behind the mark for good. The redeem now
    names the account's on-chain balance of the winning token (one free read, neg-risk only); any doubt (no id, two ids, a
    failed or zero read) keeps the listed amount; a binary redeem, the check and a round without gas read nothing."""
    async def round_with(chain, listed, neg=True, row_id=True, market_token=True, dry=False):
        bot = await ready_bot()
        fake = bot.live.api
        raw = {**mk(961, won=1), "isNegRisk": neg}
        if not market_token:
            raw["outcomes"] = [{k: v for k, v in o.items() if k != "onChainId"} for o in raw["outcomes"]]
        row = held(961)
        row["amount"] = str(listed)
        if row_id:
            row["outcome"] = {**row["outcome"], "onChainId": "9611"}
        fake.positions_rows, fake.markets["961"] = [row], raw
        bot.live.chain = chain
        if dry:
            return bot, await bot.live_redeem_report(NOW)
        return bot, await bot.live_redeem(NOW)

    # (a) listed one wei over the chain's balance: the balance is redeemed (before: the listed amount — a revert every round)
    chain = HeldChain({"9611": 100 * WEI})
    bot, done = await round_with(chain, 100 * WEI + 1)
    assert [amount for _, _, amount, *_ in chain.redeemed] == [100 * WEI] and chain.reads == [("9611", True, False)], (chain.redeemed, chain.reads)
    # (b) listed under it: the whole balance, and the line and the mark say what was redeemed (before: 100, 30 stranded)
    chain = HeldChain({"9611": 130 * WEI})
    bot, done = await round_with(chain, 100 * WEI)
    assert [amount for _, _, amount, *_ in chain.redeemed] == [130 * WEI] and done == ["💵 已领取 window 961 Up 130 份：0xredeem"], (chain.redeemed, done)
    assert bot.store.get("live:redeemed:961:1")["amount"] == str(130 * WEI)
    # (c) no id on the row: the market's token at that index set is read
    chain = HeldChain({"9611": 130 * WEI})
    await round_with(chain, 100 * WEI, row_id=False)
    assert chain.reads == [("9611", True, False)] and chain.redeemed[0][2] == 130 * WEI
    # (d) any doubt: the listed amount as before — a failed read, a zero, two different ids, no id at all
    for found in (m.RemoteError("HTTP 503: 接口请求失败"), 0):
        chain = HeldChain({"9611": found})
        await round_with(chain, 100 * WEI)
        assert chain.redeemed[0][2] == 100 * WEI, found
    chain = HeldChain({"9611": 130 * WEI})
    bot = await ready_bot()
    raw = {**mk(962, won=1), "isNegRisk": True}
    row = {**held(962), "outcome": {"name": "Up", "indexSet": 1, "onChainId": "9999"}}  # the row names another token than the market
    bot.live.api.positions_rows, bot.live.api.markets["962"] = [row], raw
    bot.live.chain = chain
    await bot.live_redeem(NOW)
    assert chain.reads == [] and chain.redeemed[0][2] == 10 * WEI, (chain.reads, chain.redeemed)
    chain = HeldChain({"9611": 130 * WEI})
    await round_with(chain, 100 * WEI, row_id=False, market_token=False)
    assert chain.reads == [] and chain.redeemed[0][2] == 100 * WEI
    # (e) a binary redeem reads nothing (the whole balance is burnt whatever the amount); the check and no gas read nothing
    chain = HeldChain({"9611": 130 * WEI})
    await round_with(chain, 100 * WEI, neg=False)
    assert chain.reads == [] and chain.redeemed[0][2] == 100 * WEI
    chain = HeldChain({"9611": 130 * WEI})
    bot, report = await round_with(chain, 100 * WEI, dry=True)
    assert chain.reads == [] and chain.redeemed == [] and "可领取" in report, report
    chain = HeldChain({"9611": 130 * WEI})
    chain.bnb = 10 ** 13
    bot = await ready_bot()
    bot.live.chain = chain
    await bot.live.refresh_balance()
    bot.live.api.positions_rows, bot.live.api.markets["961"] = [held(961)], {**mk(961, won=1), "isNegRisk": True}
    assert await bot.live_redeem(NOW) == [] and chain.reads == [] and chain.redeemed == []
    # (f) the read itself: balanceOf(maker, id) on the track's multi-outcome tokens (standard: the CONDITIONAL_TOKENS address)
    for account in ("", PA):
        real = L.Chain("https://rpc.invalid", L.Wallet(KEY, 56, account))
        for yb, tokens in ((False, "CONDITIONAL_TOKENS"), (True, "YIELD_BEARING_NEG_RISK_CONDITIONAL_TOKENS")):
            with node_for(eth_call="0x" + hex(7 * WEI)[2:].rjust(64, "0")) as node:
                assert await real.outcome_balance("987654321", True, yb) == 7 * WEI
            call = node.log[-1][1][0]
            assert call["to"] == L.ADDRESSES[56][tokens] and call["data"] == "0x00fdd58e" + pad(real.wallet.maker)[2:] + hex(987654321)[2:].rjust(64, "0"), call
        with node_for(eth_call="0x"):
            assert await real.outcome_balance("1", True, False) is None
    print("RDM_NEGRISK_AMOUNT_OK")


asyncio.run(negrisk_amount())


# --- RDM-MARKS-PERMANENT --------------------------------------------------------------------------------------------------------
class LongHash(T["FakeChain"]):
    """test_trading's FakeChain with a real-length transaction hash (its 0xredeem is too short to show a cut)."""
    TX = "0x" + "5e" * 32

    async def redeem(self, cid, index_set, amount, neg, yb):
        await super().redeem(cid, index_set, amount, neg, yb)
        return self.TX


async def marks_permanent():
    """RDM-MARKS-PERMANENT: a redeemed / lost mark skips its position for good (1.131.3: no second redeem of a row Predict still
    lists, no re-read of every listed loser) — and that stays. But nothing could lift a wrong lost mark (one market read), the
    check cut the hash to 14 characters with no age and no market id, and said 过 10 分钟还在 for ever. The check now says the
    whole hash, the mark's age and the market (lost rows too), its conclusion goes by the age, and /live redeem forget <市场号>
    lifts that market's LOST marks (the next round judges it again: a loser re-marked, a winner redeemed once) — never a kept
    one (a second redeem would pay nothing)."""
    bot = await ready_bot()
    fake, chain = bot.live.api, LongHash()
    bot.live.chain = chain
    fake.markets.update({"101": mk(101, won=2), "102": mk(102, won=2)})
    fake.positions_rows = [held(101, 2, 100), held(102, 1, 40)]
    reads, market = [], fake.market

    async def counting(mid):
        reads.append(str(mid))
        return await market(mid)
    fake.market = counting
    done = await bot.live_redeem(NOW)
    assert done == [f"💵 已领取 window 101 Down 100 份：{LongHash.TX}"] and bot.store.get("live:redeemed:102:1")["lost"] is True, done
    lost = bot.store.get("live:redeemed:102:1")
    reads.clear()
    assert await bot.live_redeem(NOW + 10 * MIN) == [] and reads == [] and len(chain.redeemed) == 1, reads  # still for good: no read, no 2nd redeem
    # the check: the whole hash, the age, the market — by the age, list lag or a look on bscscan
    report = await bot.live_redeem_report(NOW + 2 * MIN)
    assert "结论：有持仓已经发过领取交易并上链，Predict 还列着：可能只是列表没更新，过半小时再查" in report, report
    before = bot.store.items("live:")
    report = await bot.live_redeem_report(NOW + 40 * MIN)
    assert bot.store.items("live:") == before, "the check writes nothing"
    assert f"window 101 / Down 100 份（市场 101，40 分前领取，交易 {LongHash.TX}）" in report, report  # (before: （交易 0x5e5e5e5e5e5e…）)
    assert "输掉的（不再读市场；判断错了可发 /live redeem forget <市场号> 重新判断）：" in report and "window 102 / Up 40 份（市场 102，40 分前判定）" in report, report
    assert "结论：有 1 个持仓 40 分前就发过领取交易并上链，Predict 一直还列着：在 bscscan.com 打开上面的交易哈希" in report and "列表没更新" not in report, report
    assert (lost.get("status"), lost.get("winner"), lost.get("how")) == ("RESOLVED", "Down", "outcomes.status"), lost  # the read a lost mark rests on
    # forget: only lost marks, only that market (10 is not 101 / 102), nothing sent
    sent = notices(bot)
    assert (await bot.live_action("redeem", ["forget"], NOW)).startswith("用法：/live redeem forget <市场号>")
    assert await bot.live_action("redeem", ["forget", "10"], NOW) == "市场 10 没有领取记录"
    answer = await bot.live_action("redeem", ["forget", "101"], NOW)
    assert "没有清除" in answer and "已清除" not in answer and bot.store.get("live:redeemed:101:2")["tx"] == LongHash.TX, answer  # a kept mark stays
    answer = await bot.live_action("redeem", ["forget", "102"], NOW)
    assert answer.startswith("已清除市场 102 的 1 条“输掉的”记录") and bot.store.get("live:redeemed:102:1") is None, answer
    assert len(chain.redeemed) == 1 and sent == [], "forget sends nothing"
    # the next round judges 102 again: still lost — marked again, no transaction, 101 not read
    reads.clear()
    assert await bot.live_redeem(NOW + 50 * MIN) == [] and reads == ["102"] and bot.store.get("live:redeemed:102:1")["lost"] is True, reads
    # a wrong lost mark: forgotten, and Predict now names Up — redeemed once, then never again
    await bot.live_action("redeem", ["forget", "102"], NOW)
    fake.markets["102"] = mk(102, won=1)
    done = await bot.live_redeem(NOW + 60 * MIN)
    assert done == [f"💵 已领取 window 102 Up 40 份：{LongHash.TX}"] and [i for _, i, *_ in chain.redeemed] == [2, 1], (done, chain.redeemed)
    assert await bot.live_redeem(NOW + 70 * MIN) == [] and len(chain.redeemed) == 2
    print("RDM_MARKS_PERMANENT_OK")


asyncio.run(marks_permanent())


# --- RDM-GAS-STALE --------------------------------------------------------------------------------------------------------------
async def gas_stale():
    """RDM-GAS-STALE: the gas gate was judged on a BNB reading of any age — USDT and BNB are re-read together, one failing half
    stores neither, and an old LOW reading kept every round at BNB 不足 (and the ⛽ note asking for BNB already sent) while /live
    redeem check, reading on its own, said 够付 gas. When the joint re-read fails the BNB is read alone; with no fresh figure at
    all a reading older than LIVE_BALANCE_STALE_S is not taken for the present — no 不足, no ⛽, nothing sent blind, said as
    unread. The stored pair (the order gate's) is never touched; a fresh low reading, a recent one, a never-read one: as before."""
    async def refused(owner=None):
        raise m.RemoteError("RPC 错误：limit exceeded")

    async def account(bnb):
        bot = await ready_bot()
        fake, chain = bot.live.api, bot.live.chain
        fake.positions_rows, fake.markets["301"] = [held(301)], mk(301, won=1)
        chain.bnb = bnb
        await bot.live.refresh_balance()
        return bot, chain

    # (a) read low, topped up since, the USDT read failing: the BNB read alone — redeemed (before: 可领取 1；签名钱包 BNB 不足)
    bot, chain = await account(10 ** 13)
    chain.bnb, chain.usdt_balance = 10 ** 17, refused
    bot.live.balance = (*bot.live.balance[:2], time.time() - 700)
    sent = notices(bot)
    assert len(await bot.live_redeem(NOW)) == 1 and len(chain.redeemed) == 1 and not bot.live_redeem_last["low_gas"], bot.live_redeem_last
    assert "redeem:gas" not in [k for k, _ in sent], sent
    assert bot.live.balance[1] == 10 ** 13 and "余额刷新失败" in bot.live_balance_gate(), "the order gate's pair untouched"
    # (b) nothing readable and the last reading 700 s old: not taken for the present — nothing sent, no 不足, no ⛽, said as unread
    bot, chain = await account(10 ** 13)
    chain.usdt_balance = chain.bnb_balance = refused
    bot.live.balance = (*bot.live.balance[:2], time.time() - 700)
    sent = notices(bot)
    assert await bot.live_redeem(NOW) == [] and chain.redeemed == [] and not bot.live_redeem_last["low_gas"], bot.live_redeem_last
    assert sent == [] and bot.live_redeem_last["counts"] == {"claimable": 1}, sent
    summary = bot.live_redeem_summary(bot.live_redeem_last)
    assert "签名钱包的 BNB 读不到（RPC 错误：limit exceeded），上次读到是 11 分钟前，这一轮先不发" in summary and "BNB 不足" not in summary, summary
    # (c) as before: a fresh low reading holds back with the ⛽ note; a recent one (300 s) with nothing readable still judges
    bot, chain = await account(10 ** 13)
    sent = notices(bot)
    assert await bot.live_redeem(NOW) == [] and chain.redeemed == [] and bot.live_redeem_last["low_gas"] and [k for k, _ in sent] == ["redeem:gas"], sent
    bot, chain = await account(10 ** 13)
    chain.usdt_balance = chain.bnb_balance = refused
    bot.live.balance = (*bot.live.balance[:2], time.time() - 300)
    assert await bot.live_redeem(NOW) == [] and chain.redeemed == [] and bot.live_redeem_last["low_gas"] and not bot.live_redeem_last["gas_unread"]
    # ...and never read at all, nothing readable: not low, tried as before
    bot, chain = await account(10 ** 17)
    chain.usdt_balance = chain.bnb_balance = refused
    bot.live.balance = None
    assert len(await bot.live_redeem(NOW)) == 1 and len(chain.redeemed) == 1
    print("RDM_GAS_STALE_OK")


asyncio.run(gas_stale())


print("AUDIT_REDEEM_OK")
