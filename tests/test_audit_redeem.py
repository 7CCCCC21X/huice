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


print("AUDIT_REDEEM_OK")
