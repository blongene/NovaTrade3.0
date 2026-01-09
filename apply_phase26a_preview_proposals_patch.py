# apply_phase26a_preview_proposals_patch.py
"""
Bullet-proof patcher: inserts Phase 26A Preview Proposals schedules into main.py
without overwriting your file manually.

Usage:
  python apply_phase26a_preview_proposals_patch.py

Idempotent:
- If the schedule lines already exist, it does nothing.
"""
from __future__ import annotations
import os, sys, re

TARGET = "main.py"

INSERT_AFTER_PATTERNS = [
    r'_schedule\("Unified Snapshot".*',
    r'_schedule\("Daily Telemetry Digest".*',
]

SCHEDULE_LINES = [
    '    _schedule("Alpha Preview Proposals",      "alpha_proposal_runner",      "run_alpha_proposal_runner",     every=6, unit="hours")',
    '    _schedule("Alpha Proposals Mirror",       "alpha_proposals_mirror",     "run_alpha_proposals_mirror",    every=6, unit="hours")',
]

def main() -> int:
    if not os.path.exists(TARGET):
        print(f"Patch failed: {TARGET} not found in current directory.", flush=True)
        return 2

    raw = open(TARGET, "r", encoding="utf-8", errors="ignore").read()

    if "alpha_proposal_runner" in raw and "alpha_proposals_mirror" in raw:
        print("Phase 26A schedules already present in main.py (no changes).", flush=True)
        return 0

    lines = raw.splitlines(True)

    insert_at = None
    for i, line in enumerate(lines):
        for pat in INSERT_AFTER_PATTERNS:
            if re.search(pat, line):
                insert_at = i + 1
    if insert_at is None:
        # fallback: insert near end of schedule section by finding "# Vault suite" or "# Wallet + telemetry"
        for i, line in enumerate(lines):
            if "Wallet + telemetry" in line or "Vault suite" in line:
                insert_at = i
                break
    if insert_at is None:
        print("Patch failed: could not find schedule insertion point in main.py.", flush=True)
        return 3

    block = "".join([l + "\n" for l in SCHEDULE_LINES])
    lines.insert(insert_at, block)

    out = "".join(lines)
    open(TARGET, "w", encoding="utf-8").write(out)

    print("Phase 26A schedules inserted into main.py successfully.", flush=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
