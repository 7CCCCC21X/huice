"""1.154.81 — the owner, on two lines of /preopen check (先加核对，再决定 / 改):

- 923 BTC 5-minute levels invalidated by Predict (INVALIDATED, no reason given). Not the balance against one order (the largest is
  $138, the balance $1,029) — the guess now is the market's bids together: seven levels on both sides are $1,388. Each refusal
  now keeps what stood beside it (refusal_context: the money this mode's orders on that market were bidding, the USDT balance
  of the last account snapshot), and /preopen and /preopen check say how many refusals came with the market's bids past the
  balance — the placing itself is unchanged.
- A filled hourly window waiting on Predict to call its market settled (1.154.77) was "stuck" half an hour past its end: it is
  shown apart now ("等 Predict 结算", with the market's status and the result named if any), and a problem only
  SETTLE_WAIT_MAX_MS (6 hours) past the end.

Run: python tests/test_refusal_check.py
"""
import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import offline  # noqa: F401
import preopen as p
import test_preopen as tp  # (its controller, market and Api stand-ins: a suite of its own, run only as __main__)


async def read_finals(ctl, clock, *pairs):
    """The withdrawn pairs' next try as the cancellation loop makes it, the clock moved to it (test_preopen's own): an order off
    the book ended by a read of its record LATE_FILL_MS after it left."""
    clock.ms = max(clock.ms, *(int(pair["retry_ms"]) for pair in pairs))
    for pair in pairs:
        if pair in ctl.due(clock.ms):
            ctl.bookkeep(pair)
            ctl.launch_reconcile(pair)
    await asyncio.gather(*list(ctl.settling.values()), return_exceptions=True)


class RefusalCheckTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_refusal_keeps_the_market_bids_and_the_balance(self):
        ctl, api, clock = tp.controller()
        c = p.candidate(tp.market(), tp.NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        price, shares = p.pair_quote(pair)
        up = pair["orders"]["up"]
        ctl.balance = 30.0
        api.rows[up["hash"]].update(status="INVALIDATED")
        clock.ms += 10_000
        ctl.account_at = clock.ms
        ctl.listed_ids = set()
        await ctl.monitor_pair(pair)
        await asyncio.gather(*list(ctl.settling.values()), return_exceptions=True)
        ctx = up.get("refused_ctx")
        self.assertTrue(up.get("refused") and ctx, up)
        # both sides of the market's one level were out: the refused one counted as it stood, its other side still open
        self.assertEqual((ctx["usd"], ctx["balance"], ctx["age_s"], ctx["level"]), (round(2 * price * shares, 2), 30.0, 0, 0))
        self.assertEqual(list(ctl.refusals), [ctx])
        # an order of another market is not this market's bid
        ctl.ledger.working["elsewhere"] = {"market_id": "999", "slug": "elsewhere", "price": "0.4", "shares": "100",
                                          "orders": {"up": {"state": "open", "filled": 0.0}, "down": {"state": "open", "filled": 0.0}}}
        # (counted now, after the refusal: the other side was withdrawn there and then — the refused order alone, never the other market's $80)
        self.assertEqual(ctl.refusal_context(pair, up)["usd"], round(price * shares, 2))
        del ctl.ledger.working["elsewhere"]
        line = ctl.refusal_line()
        self.assertIn("作废核对（本次启动后 1 次）", line)
        self.assertIn(f"合计中位 ${2 * price * shares:,.0f}，当时余额中位 $30", line)
        self.assertIn("合计超过余额的 1 次（100%）——次数还少，再看一阵", line)
        status, check = ctl.status(), ctl.check()
        self.assertIn("作废核对（本次启动后 1 次）", status)
        self.assertIn("作废核对（本次启动后 1 次）", check)
        # once the refused level is booked it is listed to be quoted again — its example with what stood beside it
        await read_finals(ctl, clock, pair)
        self.assertTrue(pair.get("accounted"), pair)
        self.assertIn(f"（这场两边挂单合计 ${2 * price * shares:,.0f}，余额 $30）", ctl.status())
        # what the line concludes, with enough of them
        ctl.refusals.clear()
        ctl.refusals.extend({"usd": 1388.2, "balance": 1029.11, "at": 0, "age_s": 5, "name": "x", "level": 5} for _ in range(10))
        self.assertIn("合计超过余额的 10 次（100%）——基本可以确定：Predict 不许同一场的挂单合计超过余额", ctl.refusal_line())
        ctl.refusals.clear()
        ctl.refusals.extend({"usd": 588.2, "balance": 1029.11, "at": 0, "age_s": 5, "name": "x", "level": 5} for _ in range(10))
        self.assertIn("合计超过余额的 0 次（0%）——和余额关系不大，原因另找", ctl.refusal_line())
        ctl.refusals.extend({"usd": 1388.2, "balance": 1029.11, "at": 0, "age_s": 5, "name": "x", "level": 5} for _ in range(12))
        self.assertIn("合计超过余额的 12 次（55%）——还说不准，再看一阵", ctl.refusal_line())
        ctl.refusals.clear()
        self.assertEqual(ctl.refusal_line(), "")
        ctl.ledger.close()

    async def test_a_filled_window_waiting_on_predict_is_shown_apart(self):
        ctl, api, clock = tp.controller()
        c = p.candidate(tp.market(), tp.NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        for row in api.rows.values():
            row["amountFilled"] = row["amount"]  # both filled before the cancel
        await ctl.cancel_many([pair], "cancel")
        await asyncio.gather(*list(ctl.settling.values()), return_exceptions=True)
        await read_finals(ctl, clock, pair)
        self.assertTrue(all(o["state"] == "final" and o["filled"] > 0 for o in pair["orders"].values()), pair["orders"])
        end = p.settle_from(pair)
        # the result not written yet: waited on, its words said
        clock.ms = end + 31 * 60_000
        await ctl.settle_pair(pair)
        self.assertFalse(pair.get("accounted"))
        wait = pair["settle_wait"]
        self.assertEqual((wait["status"], wait["result"], wait["since"]), ("REGISTERED", "", clock.ms))
        self.assertEqual(ctl.stuck_pairs(clock.ms), [], "half an hour past its end, waiting on Predict: not stuck (1.154.80 called it so)")
        self.assertEqual(ctl.settle_waiting(clock.ms), [pair])
        self.assertIn("还没写结果，市场状态 REGISTERED，结束后已 31 分钟", p.settle_wait_text(pair, clock.ms))
        # a result named, the market not called settled (1.154.77 waits for that word): still waited on, the result said
        api.markets["123"]["outcomes"][1]["status"] = "WON"  # Up
        clock.ms = end + 95 * 60_000
        await ctl.settle_pair(pair)
        self.assertFalse(pair.get("accounted"))
        self.assertEqual((pair["settle_wait"]["result"], pair["settle_wait"]["since"]), ("Up", end + 31 * 60_000), "the wait counted from its first look")
        ctl.account_at = clock.ms  # (the fixture's account snapshot kept fresh: the clock jumped hours)
        check = ctl.check()
        self.assertTrue(check.startswith("开盘前预挂体检") and "：正常" in check.splitlines()[0], check.splitlines()[0])
        self.assertIn("等 Predict 结算（有成交、两边已读完，等 Predict 把市场写成已结算；超过结束后 6 小时才算有问题）：1 场", check)
        self.assertIn("已写结果 Up，市场状态 REGISTERED，结束后已 1 小时 35 分", check)
        self.assertIn("撤单后超过 15 分钟未结清：0 场", check)
        # six hours on: a problem
        late = end + p.SETTLE_WAIT_MAX_MS + 60_000
        self.assertEqual(ctl.stuck_pairs(late), [pair])
        self.assertEqual(ctl.settle_waiting(late), [])
        # ...and once Predict calls it settled, booked as ever
        api.markets["123"]["status"] = "RESOLVED"
        clock.ms = end + 3 * 3600_000
        ctl.owner.live_redeem_soon = lambda market_id="": None
        await ctl.settle_pair(pair)
        self.assertTrue(pair["accounted"])
        self.assertEqual(ctl.settle_waiting(clock.ms), [])
        ctl.ledger.close()

    async def test_a_filled_pair_never_looked_at_is_stuck_as_before(self):
        ctl, api, clock = tp.controller()
        c = p.candidate(tp.market(), tp.NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        for row in api.rows.values():
            row["amountFilled"] = row["amount"]
        await ctl.cancel_many([pair], "cancel")
        await asyncio.gather(*list(ctl.settling.values()), return_exceptions=True)
        await read_finals(ctl, clock, pair)
        end = p.settle_from(pair)
        self.assertEqual(ctl.stuck_pairs(end + 29 * 60_000), [])
        self.assertEqual(ctl.stuck_pairs(end + 31 * 60_000), [pair], "no read of its market: the settlement itself is not running — stuck as before")
        ctl.ledger.close()


if __name__ == "__main__":
    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(RefusalCheckTests))
    if not result.wasSuccessful():
        raise SystemExit(1)
    print("REFUSAL_CHECK_OK")
