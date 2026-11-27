"""1.154.77 — the second outside audit's follow-up (the user: "两件都做"):

- the exchange holiday tables carry 2027, each from its official notice: CN (the State Council's arrangement of 2026-11-05),
  KR (KASA's 2027 월력요항: 노동절 / 제헌절 public holidays, seven substitute days, and KRX's year-end closure), HK (the general
  holidays gazetted 2026-05-15), SG (MOM's list of 2026-06-18), the half days of HKEX and SGX; each table reaches 2028-01-01;
- a result is booked only on a market Predict calls settled — the model trader's confirmation, the pre-open mode's settlement
  and the rule taker's, as the redeem has it (the audit's "to verify" 1: a result named before that may still be a proposal);
- the review's EV bets take the probability the edge was judged by (SIM_MAX_FAIR_CENTS): the spread and the fills' expectation
  were the uncapped model's (P2-8), and the EV line counts independent events.

Run: python tests/test_audit_ext3.py
"""
import asyncio
import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import preopen as P
import rules as R
import test_preopen as TP
import test_rules as TR

d = dt.date.fromisoformat


# --- the 2027 holiday tables -----------------------------------------------------------------------------------------------------
def holidays_2027() -> None:
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"})
    H = cfg.holidays
    closed = {
        "sh": ["2027-01-01", "2027-02-05", "2027-02-08", "2027-02-09", "2027-02-10", "2027-02-11", "2027-02-12", "2027-04-05",
               "2027-05-03", "2027-05-04", "2027-05-05", "2027-06-09", "2027-09-15", "2027-10-01", "2027-10-04", "2027-10-07"],
        "kr": ["2027-01-01", "2027-02-08", "2027-02-09", "2027-03-01", "2027-05-03", "2027-05-05", "2027-05-13", "2027-07-19",
               "2027-08-16", "2027-09-14", "2027-09-15", "2027-09-16", "2027-10-04", "2027-10-11", "2027-12-27", "2027-12-31"],
        "hk": ["2027-01-01", "2027-02-08", "2027-02-09", "2027-03-26", "2027-03-29", "2027-04-05", "2027-05-13", "2027-06-09",
               "2027-07-01", "2027-09-16", "2027-10-01", "2027-10-08", "2027-12-27"],
        "sg": ["2027-01-01", "2027-02-08", "2027-03-10", "2027-03-26", "2027-05-17", "2027-05-20", "2027-08-09", "2027-10-28"],
    }
    for market, days in closed.items():
        missing = [x for x in days if d(x) not in H[market]]
        assert not missing, (market, missing)
    assert H["sz"] == H["sh"]
    # ...and trading days on either side stay trading days (a holiday written wrong skips a real one)
    open_days = {"sh": ["2027-02-04", "2027-02-15", "2027-04-06", "2027-05-06", "2027-10-08"],
                 "kr": ["2027-02-10", "2027-05-04", "2027-07-16", "2027-09-17", "2027-12-30"],
                 "hk": ["2027-02-05", "2027-02-10", "2027-03-30", "2027-12-28"],
                 "sg": ["2027-02-05", "2027-02-09", "2027-05-18", "2027-12-24"]}
    for market, days in open_days.items():
        wrong = [x for x in days if d(x) in H[market]]
        assert not wrong, (market, wrong)
    assert m.hk_trading_day(d("2027-02-09"), H["hk"]) is False and m.hk_trading_day(d("2027-02-10"), H["hk"]) is True
    assert not m.a50_trading_day(d("2027-02-08")) and m.a50_trading_day(d("2027-02-09")) and not m.a50_trading_day(d("2027-10-28"))
    # the half days: the eves of Lunar New Year, Christmas and New Year (all Fridays in 2027)
    for day in ("2027-02-05", "2027-12-24", "2027-12-31"):
        assert m.CALENDAR.half_day("hk", d(day)) and m.CALENDAR.half_day("sg", d(day)), day
    assert m.a50_day_end(d("2027-02-05")) == m.A50_HALF_DAY_END and not m.a50_night(d("2027-12-31"))
    # each table reaches the next New Year's Day: no reminder this December, one in December 2027
    assert m.calendar_until(H) == d("2028-01-01")
    assert m.calendar_warning(H, d("2026-11-27")) == "" and m.calendar_warning(H, d("2026-12-31")) == ""
    assert "2028-01-01" in m.calendar_warning(H, d("2027-12-05"))


# --- a result is booked only on a market Predict calls settled ---------------------------------------------------------------------
class FM:
    def __init__(self, now: int):
        self.now, self.config = now, None

    def now_ms(self) -> int:
        return self.now


def resolved_market(status: str, winner: int = 0) -> dict:
    return {"id": 101, "status": status, "outcomes": [{"name": "Up", "indexSet": 1, "status": "WON" if winner == 0 else "LOST"},
                                                       {"name": "Down", "indexSet": 2, "status": "WON" if winner == 1 else "LOST"}]}


async def model_trader_waits_for_settled() -> None:
    now = int(dt.datetime(2026, 11, 27, 18, 0, tzinfo=m.BEIJING).timestamp() * 1000)
    bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), m.Store(":memory:"), FM(now), None)
    tid = "hang-seng-index-up-or-down-on-november-27-2026|up|挂"
    rec = {"v": 2, "market": tid.split("|")[0], "market_id": "101", "item": "恒生指数", "kind": "close", "key": "HSI", "side": "up",
           "label": "挂涨", "maker": True, "fair": 0.7, "price": 0.55, "edge": 0.15, "shares": 100.0, "order": 100.0,
           "status": "settled", "payout": 1.0, "opened": now - 6 * 3_600_000, "settled": now - 3_600_000,
           "settle": {"key": "HSI", "target": "2026-11-27", "line": 26000.0, "close_ms": now - 2 * 3_600_000},
           "fills": [{"at": now - 6 * 3_600_000, "shares": 100.0, "fair": 0.7}], "revisions": [], "confirm": "local",
           "local": {"up": 1.0, "note": "11-27 收盘 26,100，对 26,000", "at": now - 3_600_000}}
    bot.sim_save([(tid, rec)])
    answer = {"status": "PENDING_RESOLUTION"}

    async def details(mid: str) -> dict:
        market = resolved_market(answer["status"])
        return {"outcomes": ["Up", "Down"], "status": market["status"], "resolved": m.predict_resolution(market)}
    bot.predict.market_details = details
    await bot.sim_confirm(bot.sim_trades(), now)
    held = bot.sim_trades()[tid]
    assert not held.get("final") and held["confirm"] == "local", held.get("final")  # was "confirmed" on a market not settled yet
    assert held["final_check"]["status"] == "PENDING_RESOLUTION"  # (the page says 待确认（市场状态：…）)
    answer["status"] = "RESOLVED"
    bot.sim_checked.clear()
    await bot.sim_confirm(bot.sim_trades(), now + 600_000)
    held = bot.sim_trades()[tid]
    assert held["final"]["up"] == 1.0 and held["confirm"] == "confirmed", (held.get("final"), held.get("confirm"))


async def preopen_waits_for_settled() -> None:
    row = TP.market(at=TP.NOW + 15 * 60_000, mid=201)
    ctl, api, clock = TP.controller([row], PREOPEN_MARKETS="btc", PREOPEN_SHARES="10")
    assert await ctl.place_pair(P.candidate(row, TP.NOW, ctl.owner.config, ctl.settings)) == ""
    pair = next(iter(ctl.ledger.rows.values()))
    pair["phase"] = "closing"
    for side, o in pair["orders"].items():
        o.update(state="final", filled=10.0 if side == "up" else 0.0)
    ctl.ledger.save(pair)
    answer = {"status": "CLOSED"}

    async def market(mid):
        return resolved_market(answer["status"], winner=1)  # Down won: Up's 10 shares lost
    ctl.cancel_api.market = market

    async def read_final(order, pair):
        return None
    ctl.read_final = read_final
    clock.ms = P.settle_from(pair) + 1
    await ctl.settle_pair(pair)
    assert not pair.get("accounted"), "settled on a result named before Predict called the market settled"
    answer["status"] = "RESOLVED"
    clock.ms += 61_000
    await ctl.settle_pair(pair)
    assert pair["accounted"] and pair["result"] == "down" and pair["pnl"] < 0, (pair.get("accounted"), pair.get("result"), pair.get("pnl"))
    ctl.ledger.close()


async def rule_taker_waits_for_settled() -> None:
    t, api, clock, x = TR.taker("on")
    rec = {"mode": "on", "slug": "xau-updown-15m-1", "name": "黄金", "rule": "gold_shut", "side": "up", "market_id": "901",
           "start_ms": clock.ms - 30 * 60_000, "end_ms": clock.ms - 15 * 60_000, "pnl": None, "result": "",
           "orders": [{"state": "final", "id": "1", "shares": 10.0, "filled": 10.0, "limit": 0.5, "avg": 0.5, "fee_bps": 0}]}
    t.recs["w"] = rec
    answer = {"status": "PROPOSED"}

    async def market(mid):
        return resolved_market(answer["status"], winner=0)
    t.api.market = market
    clock.ms = rec["end_ms"] + R.SETTLE_AFTER_MS
    await t.settle()
    assert rec["pnl"] is None and "还不是已结算" in str(rec.get("settle_error")), (rec["pnl"], rec.get("settle_error"))
    answer["status"] = "RESOLVED"
    clock.ms += R.SETTLE_EVERY_MS
    await t.settle()
    assert rec["result"] == "up" and abs(rec["pnl"] - (10 * 1.0 - 10 * 0.5)) < 1e-9, (rec["result"], rec["pnl"])
    del t.recs["w"]
    x.owner.store.close()


# --- the review's EV bets by the probability the edge was judged at --------------------------------------------------------------
def ev_capped() -> None:
    rec = {"status": "settled", "shares": 10.0, "payout": 0.0, "price": 0.85, "edge": 0.10, "fair": 0.99, "maker": False, "side": "up",
           "market": "x", "market_id": "7", "opened": 0, "entry": {"fair_cap": 0.95}, "version": {"sim_max_fair": 0.95},
           "fills": [{"shares": 10.0, "fair": 0.99}]}
    bet = m.sim_ev_bets([rec])[0]
    assert bet["fair"] == 0.95 and abs(bet["var"] - 100 * 0.95 * 0.05) < 1e-9, bet  # was p = 0.99: a fifth of the spread
    assert abs(bet["ev"] - 1.0) < 1e-9
    assert abs(m.sim_fill_expectation(rec) - 1.0) < 1e-9, m.sim_fill_expectation(rec)  # was (0.99 − 0.85) × 10 = 1.40 against 1.00
    # a record the cap did not cut: as before
    plain = {**rec, "fair": 0.90, "edge": 0.05, "entry": {}, "fills": [{"shares": 10.0, "fair": 0.90}]}
    assert m.sim_ev_bets([plain])[0]["fair"] == 0.90 and abs(m.sim_fill_expectation(plain) - 0.5) < 1e-9
    # a maker filled later, when the model said more than the cap: its fill judged at the cap too (version.sim_max_fair)
    later = {**plain, "fills": [{"shares": 10.0, "fair": 0.99}]}
    assert abs(m.sim_fill_expectation(later) - (0.95 - 0.85) * 10) < 1e-9
    # (an old record without the version's cap: the model's figure, as before)
    old = {**later, "version": {}}
    assert abs(m.sim_fill_expectation(old) - (0.99 - 0.85) * 10) < 1e-9
    src = (ROOT / "main.py").read_text(encoding="utf-8")
    assert "独立场次" in src[src.index("    def sim_ev_line"):src.index("    def sim_notify")]


TESTS = [holidays_2027, model_trader_waits_for_settled, preopen_waits_for_settled, rule_taker_waits_for_settled, ev_capped]


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
            if not only:
                raise
            failed.append(f"{test.__name__}: {type(error).__name__}: {str(error)[:200]}")
    return failed


if __name__ == "__main__":
    names = [a for a in sys.argv[1:] if a != "--each"]
    failures = asyncio.run(run([t.__name__ for t in TESTS] if "--each" in sys.argv else names or None))
    if failures:
        print("\n".join(failures))
        sys.exit(1)
    print("AUDIT_EXT3_OK")
