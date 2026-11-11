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


print("AUDIT_REDEEM_OK")
