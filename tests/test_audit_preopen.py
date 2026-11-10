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

print("AUDIT_PREOPEN_OK")
