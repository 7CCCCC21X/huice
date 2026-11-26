"""1.154.72 — an outside white-box audit of 1.154.71 (the user pasted its report): the P1 findings it reproduced, each replayed
here with its own amounts and order of events, and what each does now.

- P1-1 a pre-open pair settled on Predict's result, then a slower account snapshot stating more shares filled: the P&L and
  the day's loss stop stayed at the fill it was settled with (Controller.late_fill; Ledger.day_pnl counts a change learned
  on a later day on that day).
- P1-2 a model-trader record settled at 23:59 Beijing, its final read at 00:01 stating ten times the shares: the loss learned
  after midnight was on no day's stop (LiveBot.live_note_late; live_daily_pnl).
- P1-3 the rule taker: a fill's USDT still in the balance until its next read, the filled part no longer counted as pending —
  the next window's order sized on the whole balance again (RuleTaker.pending_usd).
- P1-5 /live cancel all while a rule order's request is out: "应答一到就撤", withdrawn only 1.5 s later unless real trading
  was stopped too (withdraw_all marks it; send withdraws it at the answer).
- P1-6 a pre-open pair whose first save failed: held in memory with its money reserved, never sent, the retry dropping the
  window (Ledger.unhold).
- P2 (rule taker) a lagging order record's average price taken over the shares of a newer one (take_fills).

Run: python tests/test_audit_ext.py
"""
import asyncio
import datetime as dt
import sqlite3
import sys
import tempfile
import time
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

DAY = 86_400_000


def day_of(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, m.BEIJING).date().isoformat()


def settled_pair(slug: str, day: str, up_filled: float, start_ms: int) -> dict:
    """A pre-open pair settled on Predict's result "down": 涨 bought at 49¢, ``up_filled`` of its 20 shares known filled then."""
    orders = {}
    for side, index_set in (("up", 2), ("down", 1)):
        orders[side] = {"hash": f"0x{slug}{side}", "id": f"{slug}-{side}", "state": "final", "index_set": index_set,
                        "filled": up_filled if side == "up" else 0.0,
                        "body": {"data": {"order": {"hash": f"0x{slug}{side}", "tokenId": f"{slug}{side}", "side": 0,
                                                    "makerAmount": str(L.to_wei(9.8)), "takerAmount": str(L.to_wei(20))}}}}
    return {"slug": slug, "key": "BTC-15M", "window": slug, "level": 0, "attempt": 0, "price": "0.49", "shares": "20",
            "phase": "closed", "accounted": True, "accounted_day": day, "result": "down", "pnl": (0.0 - 0.49) * up_filled,
            "start_ms": start_ms, "market_id": "777", "name": slug, "orders": orders}


def snapshot_row(pair: dict, side: str, filled: float) -> dict:
    """The account snapshot's row of a pair's order: still listed (the read began before it left the book), ``filled`` of 20."""
    order = pair["orders"][side]
    return {"id": order["id"], "order": {"hash": order["hash"], "side": 0, "makerAmount": str(L.to_wei(9.8))},
            "amount": str(L.to_wei(20)), "amountFilled": str(L.to_wei(filled)), "status": "OPEN"}


# --- P1-1: the pre-open pair settled, then more shares stated ---------------------------------------------------------------------
async def preopen_late_fill(folder: str) -> None:
    db = str(Path(folder) / "late.sqlite3")
    ctl, api, clock = TP.controller(STATE_DB=db, LIVE_MAX_DAILY_LOSS_USD="6")
    day1 = day_of(TP.NOW)
    pair = settled_pair("w1", day1, 10.0, TP.NOW - 3_600_000)
    ctl.ledger.save(pair)
    assert abs(ctl.daily_pnl() - (-4.9)) < 1e-9 and ctl.withdrawing() == "", (ctl.daily_pnl(), ctl.withdrawing())
    # the report's path: account -> apply_fill -> save_many, the snapshot's 20 shares
    api.rows[pair["orders"]["up"]["hash"]] = snapshot_row(pair, "up", 20.0)
    await ctl.account()
    pair = ctl.ledger.rows["w1"]
    assert pair["orders"]["up"]["filled"] == 20.0 and abs(pair["pnl"] - (-9.8)) < 1e-9, pair
    assert "late" not in pair, "learned the day it was settled: its own day's P&L is simply the new one"
    assert abs(ctl.daily_pnl() - (-9.8)) < 1e-9, ctl.daily_pnl()
    assert ctl.withdrawing() == "已达到今日真实亏损上限", ctl.withdrawing()  # was "" — −$4.90 under a $6 stop
    # the same rows read again: nothing new
    await ctl.account()
    assert abs(ctl.ledger.rows["w1"]["pnl"] - (-9.8)) < 1e-9 and abs(ctl.daily_pnl() - (-9.8)) < 1e-9
    ctl.ledger.close()

    # learned on the next day: counted on that day, taken off the pair's own — each day's sum, and each pair's one P&L
    db2 = str(Path(folder) / "late2.sqlite3")
    ctl, api, clock = TP.controller(STATE_DB=db2, LIVE_MAX_DAILY_LOSS_USD="6")
    w2 = settled_pair("w2", day1, 10.0, TP.NOW - 3_600_000)
    ctl.ledger.save(w2)
    clock.ms = TP.NOW + DAY
    day2 = day_of(clock.ms)
    assert ctl.daily_pnl() == 0.0
    api.rows[w2["orders"]["up"]["hash"]] = snapshot_row(w2, "up", 20.0)
    await ctl.account()
    w2 = ctl.ledger.rows["w2"]
    assert abs(w2["pnl"] - (-9.8)) < 1e-9 and len(w2["late"]) == 1 and w2["late"][0]["day"] == day2, w2
    assert abs(w2["late"][0]["pnl"] - (-4.9)) < 1e-9, w2["late"]
    assert abs(ctl.daily_pnl() - (-4.9)) < 1e-9, ctl.daily_pnl()  # today's stop: what became known today
    assert abs(ctl.ledger.day_pnl(day1) - (-4.9)) < 1e-9, ctl.ledger.day_pnl(day1)  # the pair's own day as it was then
    assert abs(ctl.ledger.day_pnl(day1) + ctl.ledger.day_pnl(day2) - w2["pnl"]) < 1e-9
    assert ctl.withdrawing() == "", "−$4.90 today under a $6 stop"
    # one more share known later that day: in the same day's figure
    api.rows[w2["orders"]["up"]["hash"]] = snapshot_row(w2, "up", 20.0)
    w2["orders"]["up"]["filled"] = 19.0  # (as if 19 had been known: the snapshot's 20 adds one)
    w2["pnl"] = -0.49 * 19
    w2["late"][0]["pnl"] = -0.49 * 9
    ctl.ledger.save(w2)
    await ctl.account()
    w2 = ctl.ledger.rows["w2"]
    assert len(w2["late"]) == 2 and abs(ctl.daily_pnl() - (-0.49 * 10)) < 1e-9, (w2["late"], ctl.daily_pnl())
    path = ctl.ledger.path
    ctl.ledger.close()
    # after a restart: read back and indexed the same
    again = P.Ledger(path)
    assert abs(again.day_pnl(day2) - (-4.9)) < 1e-9 and abs(again.day_pnl(day1) - (-4.9)) < 1e-9, (again.day_pnl(day2), again.day_pnl(day1))
    assert set(again.late) == {day2} and again.late_days == {"w2": (day2,)}
    again.unindex("w2", again.rows.pop("w2"))
    assert again.late == {} and again.late_days == {} and again.day_pnl(day2) == 0.0
    again.close()


# --- P1-6: the first save of a pair fails ------------------------------------------------------------------------------------------
async def preopen_first_save_fails(folder: str) -> None:
    db = str(Path(folder) / "save.sqlite3")
    # one pair's worth (50 shares at 49¢ a side = $49) the whole budget
    ctl, api, clock = TP.controller(STATE_DB=db, PREOPEN_MAX_OPEN_USD="49", PREOPEN_PRICE_CENTS="49", PREOPEN_SHARES="50")
    c = P.candidate(TP.market(), TP.NOW, ctl.owner.config, ctl.settings)
    real, fails = ctl.ledger.save, [1]

    def flaky(row):
        if fails[0]:
            fails[0] -= 1
            raise sqlite3.OperationalError("disk I/O error")
        real(row)
    ctl.ledger.save = flaky
    try:
        await ctl.place_pair(c)
    except sqlite3.OperationalError:
        pass
    else:
        raise AssertionError("the save's failure must reach the worker (its retry)")
    assert api.sent == [], "nothing sent before the record is on disk"
    assert c["slug"] not in ctl.ledger.rows and not ctl.ledger.working and not ctl.ledger.by_hash, "out of the records again"
    assert ctl.risk() == 0.0, ctl.risk()  # was $49: the budget spoken for by an order never sent
    assert real.__self__.conn.execute("SELECT COUNT(*) FROM pairs").fetchone()[0] == 0
    # the worker's retry: placed afresh, both sides sent
    assert await ctl.place_pair(c) == ""
    pair = ctl.ledger.rows[c["slug"]]
    assert len(api.sent) == 2 and {o["state"] for o in pair["orders"].values()} == {"open"}, pair["orders"]
    assert real.__self__.conn.execute("SELECT COUNT(*) FROM pairs").fetchone()[0] == 1
    for task in ctl.settling.values():
        task.cancel()
    await asyncio.gather(*ctl.settling.values(), return_exceptions=True)
    ctl.ledger.close()


# --- P1-2: the model trader's record settled at 23:59, its shares stated at 00:01 ---------------------------------------------------
def bj(y, mo, d, h, mi, s=0):
    return int(dt.datetime(y, mo, d, h, mi, s, tzinfo=m.BEIJING).timestamp() * 1000)


def live_bot() -> L.LiveBot:
    bot = L.LiveBot.__new__(L.LiveBot)
    bot.notes = []
    bot.live_notify = lambda text, key="": bot.notes.append(text)
    return bot


def resting_record(settled: int, payout: float, shares: float) -> dict:
    return {"live": {"state": "done", "want": {"fee_bps": 0, "cap": 0.5, "limit": 0.5}, "events": [], "order_id": "9"},
            "status": "settled", "settled": settled, "payout": payout, "price": 0.5, "shares": shares, "order": 100.0,
            "fills": [], "revisions": [], "side": "up", "fair": 0.5, "maker": True, "item": "恒指", "label": "挂涨", "filled": settled - 60_000}


def final_row(shares: float) -> dict:
    return {"id": "9", "amount": "100", "amountFilled": str(int(shares)), "status": "CANCELLED"}


def model_late_fill() -> None:
    before_midnight, after = bj(2026, 11, 10, 23, 59), bj(2026, 11, 11, 0, 1)
    bot, t = live_bot(), resting_record(before_midnight, 0.0, 10.0)
    trades = {"t": t}
    assert bot.live_daily_pnl(trades, before_midnight + 30_000) == -5.0  # the day it was settled: −$5
    bot.live_apply("t", t, final_row(100), None, after)
    assert t["shares"] == 100.0 and t["settled"] == before_midnight, t  # the journal's day stays
    assert len(t["late"]) == 1 and abs(t["late"][0]["pnl"] - (-45.0)) < 1e-9 and t["late"][0]["at"] == after, t["late"]
    assert abs(bot.live_daily_pnl(trades, after) - (-45.0)) < 1e-9, bot.live_daily_pnl(trades, after)  # was 0: on no day's stop
    assert abs(bot.live_daily_pnl(trades, after + DAY) - 0.0) < 1e-9, "the day after: counted on the day it was learned only"
    assert any("计入今日" in e["what"] for e in t["live"]["events"]), t["live"]["events"]
    # the same read again: nothing more
    bot.live_apply("t", t, final_row(100), None, after + 60_000)
    assert len(t["late"]) == 1 and abs(bot.live_daily_pnl(trades, after) - (-45.0)) < 1e-9
    # settled today: counted whole as ever, no note
    today = resting_record(after + 60_000, 0.0, 10.0)
    bot.live_apply("u", today, final_row(100), None, after + 120_000)
    assert "late" not in today and abs(bot.live_daily_pnl({"u": today}, after + 120_000) - (-50.0)) < 1e-9
    # a win learned late: a correction in the bot's favour gives today's stop no room (as a revision's does not)
    won = resting_record(before_midnight, 1.0, 10.0)
    bot.live_apply("w", won, final_row(100), None, after)
    assert abs(won["late"][0]["pnl"] - 45.0) < 1e-9 and bot.live_daily_pnl({"w": won}, after) == 0.0

    # a revision today weighs the shares it was made on: shares stated after it are priced at its result already
    def revised_then_filled() -> dict:
        r = resting_record(before_midnight, 1.0, 10.0)  # a win of yesterday (+$5)
        bot.sim_settle(r, 0.0, "本地数据更正", after, "本地数据更正")  # today: the other way
        assert r["revisions"][-1]["shares"] == 10.0 and r["settled"] == before_midnight
        bot.live_apply("r", r, final_row(100), None, after + 1_000)  # then 90 more shares stated: −$45 at the new result
        return r

    def filled_then_revised() -> dict:
        r = resting_record(before_midnight, 1.0, 10.0)
        bot.live_apply("r", r, final_row(100), None, after)  # 90 more shares stated while it stood a win: +$45
        bot.sim_settle(r, 0.0, "本地数据更正", after + 1_000, "本地数据更正")  # then the other way, on 100
        assert r["revisions"][-1]["shares"] == 100.0
        return r
    truth = (0.0 - 0.5) * 100 - (1.0 - 0.5) * 10  # today's change: −$50 now against the +$5 counted yesterday
    for make in (revised_then_filled, filled_then_revised):
        r = make()
        assert abs(bot.live_daily_pnl({"r": r}, after + 2_000) - truth) < 1e-9, (make.__name__, bot.live_daily_pnl({"r": r}, after + 2_000))
    # an older revision (no shares on it): the shares as they stand, as before
    old = resting_record(before_midnight, 0.0, 10.0)
    old["revisions"] = [{"at": after, "by": "Predict 最终结果", "from": 1.0, "to": 0.0, "note": ""}]
    assert bot.live_daily_pnl({"o": old}, after + 1_000) == -10.0


# --- P1-5: /live cancel all while a rule order's request is out, the taker going on ----------------------------------------------
async def rule_cancel_asked() -> None:
    slug = f"xau-updown-15m-{TR.GOLD_0600 // 1000}"
    t, api, clock, x = TR.taker("on", RULE_SHARES="40")
    api.markets["901"] = TR.gold_market(TR.GOLD_0600)
    api.listed.add(slug)
    api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
    api.fill[1], api.price = 30.0, 0.51
    plain, said, seq = api.create_order, [], []
    plain_read, plain_remove = api.order, api.remove_orders

    async def asked_meanwhile(body):
        got = await plain(body)  # the bytes left...
        said.append(await t.withdraw_all("管理员撤单"))  # ...and /live cancel all came before the answer — no stop
        return got

    async def read(oid, digest=""):
        seq.append("read")
        return await plain_read(oid, digest)

    async def remove(ids):
        seq.append("remove")
        return await plain_remove(ids)
    api.create_order, api.order, api.remove_orders = asked_meanwhile, read, remove
    await t.tick()
    order = t.record(slug)["orders"][0]
    assert said == ["规则吃单：1 张还在发送途中，应答一到就撤。"], said
    assert not t.halted(), "real trading not stopped: the taker goes on"
    assert api.removed == ["1"] and order["state"] == "cancelling" and order["removed"] == clock.ms, (api.removed, order)
    assert seq[:1] == ["remove"], seq  # at the answer — was: no request at all, the order left to its own course 1.5 s on
    assert "cancel_asked" not in order
    x.owner.store.close()


# --- P1-3 and P2: the rule taker's balance and its fills -----------------------------------------------------------------------
def rule_spent_and_cost() -> None:
    t, api, clock, x = TR.taker("on")
    x.state.usd = 30.0
    now_wall = time.time()
    x.owner.live.balance = (L.to_wei(30), 0, now_wall - 5)  # the chain's balance, read 5 s ago
    rec = {"mode": "on", "name": "黄金", "rule": "gold_shut", "side": "up", "orders": [
        {"state": "final", "shares": 50.0, "filled": 0.0, "limit": 0.58, "expires": (clock.ms // 1000) + 600}]}
    t.recs["w-spent"] = rec
    order = rec["orders"][0]
    # filled since the balance was read: its USDT still in that balance, and no longer an unfilled rest — counted
    t.take_fills(rec, order, {"amount": str(L.to_wei(50)), "amountFilled": str(L.to_wei(50)), "averagePrice": "0.58", "status": "FILLED"})
    assert order["filled"] == 50.0 and order["fill_wall"] >= int(now_wall * 1000) - 1, order
    assert abs(t.pending_usd() - 29.0) < 1e-9, t.pending_usd()
    assert t.block(5.0, net=True) == "USDT 余额 $30.00，在途的规则单还要 $29.00，剩下的不足本单 $5.00", t.block(5.0, net=True)
    # the balance read again after the fill: in it already, nothing more counted
    x.owner.live.balance = (L.to_wei(1), 0, time.time() + 1)
    assert t.pending_usd() == 0.0
    # no read time known (a stand-in, a balance never read): as before
    x.owner.live.balance = None
    assert t.pending_usd() == 0.0
    del t.recs["w-spent"]

    # P2: a lagging record's average price is not taken over a newer record's shares
    rec = {"mode": "on", "name": "黄金", "rule": "gold_shut", "side": "up", "orders": [{"state": "open", "shares": 100.0, "filled": 0.0, "limit": 0.58}]}
    order = rec["orders"][0]
    t.take_fills(rec, order, {"amount": str(L.to_wei(100)), "amountFilled": str(L.to_wei(40)), "averagePrice": "0.54", "status": "OPEN"})
    assert order["filled"] == 40.0 and order["avg"] == 0.54
    t.take_fills(rec, order, {"amount": str(L.to_wei(100)), "amountFilled": str(L.to_wei(10)), "averagePrice": "0.45", "status": "CANCELLED"})
    assert order["filled"] == 40.0 and order["avg"] == 0.54, order  # was 0.45: $21.60 spent booked as $18.00
    assert abs(R.cost(rec) - 21.6) < 1e-9, R.cost(rec)
    t.take_fills(rec, order, {"amount": str(L.to_wei(100)), "amountFilled": str(L.to_wei(40)), "averagePrice": "0.55", "status": "CANCELLED"})
    assert order["avg"] == 0.55, "the same shares stated again with their price: taken"
    x.owner.store.close()


async def main() -> None:
    with tempfile.TemporaryDirectory() as folder:
        await preopen_late_fill(folder)
        await preopen_first_save_fails(folder)
    model_late_fill()
    await rule_cancel_asked()
    rule_spent_and_cost()


asyncio.run(main())
print("AUDIT_EXT_OK")
