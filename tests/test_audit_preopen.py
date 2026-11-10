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

print("AUDIT_PREOPEN_OK")
