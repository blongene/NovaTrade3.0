# apply_phase26a_step4_patch.py
"""Bullet-proof patcher: adds Alpha Approvals Requests Mirror schedule into main.py.

Usage:
  python apply_phase26a_step4_patch.py

Idempotent:
  - If schedule line already exists, no changes.

Adds:
  _schedule("Alpha Approval Requests Mirror", "alpha_approvals_requests_mirror", "run_alpha_approvals_requests_mirror", every=15, unit="minutes")
"""

from __future__ import annotations

import os
import re

TARGET = "main.py"

INSERT_AFTER_PATTERNS = [
    r'_schedule\("Unified Snapshot".*',
    r'_schedule\("Alpha Proposals Mirror".*',
    r'_schedule\("Sentiment Log \(Advisory\)".*',
]

SCHEDULE_LINE = '    _schedule("Alpha Approval Requests Mirror","alpha_approvals_requests_mirror","run_alpha_approvals_requests_mirror", every=15, unit="minutes")'


def main() -> int:
    if not os.path.exists(TARGET):
        print(f"Patch failed: {TARGET} not found in current directory.", flush=True)
        return 2

    raw = open(TARGET, "r", encoding="utf-8", errors="ignore").read()

    if "alpha_approvals_requests_mirror" in raw and "run_alpha_approvals_requests_mirror" in raw:
        print("Phase 26A Step 4 schedule already present in main.py (no changes).", flush=True)
        return 0

    lines = raw.splitlines(True)

    insert_at = None
    for i, line in enumerate(lines):
        for pat in INSERT_AFTER_PATTERNS:
            if re.search(pat, line):
                insert_at = i + 1

    if insert_at is None:
        # fallback: insert after any _set_schedules() line block start
        for i, line in enumerate(lines):
            if "def _set_schedules" in line:
                insert_at = i + 1
                break

    if insert_at is None:
        insert_at = len(lines)

    lines.insert(insert_at, SCHEDULE_LINE + "\n")
    open(TARGET, "w", encoding="utf-8").write("".join(lines))
    print("Phase 26A Step 4 schedule inserted into main.py successfully.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
