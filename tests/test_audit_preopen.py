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

print("AUDIT_PREOPEN_OK")
