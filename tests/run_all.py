#!/usr/bin/env python3
"""Run every offline test suite (tests/test_*.py). Each suite is a script that ends by printing "..._OK".

No network access is needed: all feeds are replaced by fixtures. The browser check in test_web.py
runs only when Playwright + Chromium are installed and is skipped otherwise.
Exit status is non-zero when any suite fails, so `python tests/run_all.py` can gate a Docker build.
"""
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main() -> int:
    only = set(sys.argv[1:])
    suites = sorted(p for p in HERE.glob("test_*.py") if not only or p.stem.removeprefix("test_") in only)
    failed = []
    for path in suites:
        started = time.monotonic()
        try:
            result = subprocess.run([sys.executable, str(path)], cwd=HERE.parent, capture_output=True, text=True,
                                    timeout=300, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
            stdout, output, code = result.stdout, result.stdout + result.stderr, result.returncode
        except subprocess.TimeoutExpired:
            stdout, output, code = "", "TIMEOUT after 300 s", -1
        lines = output.strip().splitlines()
        ok = code == 0 and result_ok(stdout)
        print(f"{'PASS' if ok else 'FAIL'} {path.name} ({time.monotonic() - started:.1f}s)")
        if not ok:
            failed.append((path.name, cause(lines)))
            print("\n".join("    " + line for line in lines[-25:]))
    print(f"\n{len(suites) - len(failed)}/{len(suites)} suites passed")
    for name, why in failed:  # repeated at the very end: a build log's tail (all a screenshot shows) names what failed
        print(f"FAILED {name}: {why}")
    return 1 if failed or not suites else 0


def cause(lines: list[str]) -> str:
    """Why a suite failed, in one line: its failing tests' names (unittest's FAIL: / ERROR: lines, three at most) and
    the last error line of its output."""
    tests = [line.split("(")[0].strip() for line in lines if line.startswith(("FAIL: ", "ERROR: "))][:3]
    error = next((line.strip() for line in reversed(lines) if line.strip().startswith(("AssertionError", "TIMEOUT"))
                  or line.strip().split(":")[0].endswith(("Error", "Exception"))), lines[-1].strip() if lines else "no output")
    return "; ".join(tests + [error[:300]])


def result_ok(stdout: str) -> bool:
    out = stdout.strip().splitlines()
    return bool(out) and out[-1].strip().endswith("_OK")


if __name__ == "__main__":
    sys.exit(main())
