"""Audit of the automatic redeem (自动领取): regression tests for the verified findings fixed here (fakes only, offline —
never Predict, a BNB node or Telegram)."""
import asyncio, json, pathlib, sys, time, urllib.parse
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
    # ...the node mining again: the next round sends the other two (the pending one waits its hour)
    node.mines = True
    done = await bot.live_redeem(NOW + 10 * MIN)
    assert len(done) == 2 and len(node.sent) == 3 and bot.live_redeem_last["counts"] == {"redeemed": 2, "waiting": 1}, (done, bot.live_redeem_last)
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


print("AUDIT_REDEEM_OK")
