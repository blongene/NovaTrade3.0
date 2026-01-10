# apply_phase26b_approvals_patch.py
"""Bullet-proof patcher: inserts Phase 26B Approvals Sync schedule into main.py.

Usage:
  python apply_phase26b_approvals_patch.py

Idempotent:
  - If the schedule line already exists, it does nothing.

Adds:
  _schedule("Alpha Approvals Sync", "alpha_approvals_sync", "run_alpha_approvals_sync", every=1, unit="hours")
"""

from __future__ import annotations

import os
import re

TARGET = "main.py"

# Insert after other Alpha schedules if possible
INSERT_AFTER_PATTERNS = [
    r'_schedule\("Alpha Preview Proposals".*',
    r'_schedule\("Alpha Proposals Mirror".*',
]

SCHEDULE_LINE = '    _schedule("Alpha Approvals Sync",        "alpha_approvals_sync",     "run_alpha_approvals_sync",    every=1, unit="hours")'


def main() -> int:
    if not os.path.exists(TARGET):
        print(f"Patch failed: {TARGET} not found in current directory.", flush=True)
        return 2

    raw = open(TARGET, "r", encoding="utf-8", errors="ignore").read()

    if "alpha_approvals_sync" in raw and "run_alpha_approvals_sync" in raw:
        print("Phase 26B schedule already present in main.py (no changes).", flush=True)
        return 0

    lines = raw.splitlines(True)

    insert_at = None
    for i, line in enumerate(lines):
        for pat in INSERT_AFTER_PATTERNS:
            if re.search(pat, line):
                insert_at = i + 1

    if insert_at is None:
        # fallback: try to insert after Phase 26A block marker (module names)
        for i, line in enumerate(lines):
            if "alpha_proposals_mirror" in line:
                insert_at = i + 1
                break

    if insert_at is None:
        # last fallback: append near end of schedule section
        for i, line in enumerate(lines):
            if "def start_schedules" in line or "Schedules" in line:
                insert_at = i + 1
                break

    if insert_at is None:
        insert_at = len(lines)

    lines.insert(insert_at, SCHEDULE_LINE + "\n")

    open(TARGET, "w", encoding="utf-8").write("".join(lines))
    print("Phase 26B schedule inserted into main.py successfully.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
