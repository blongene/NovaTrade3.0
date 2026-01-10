#!/usr/bin/env python3
"""
apply_phase26e_patch.py

Idempotently inserts Phase 26E schedule into main.py.

Adds:
    _schedule("Alpha Dryrun OrderPlace", "alpha_outbox_orderplace_dryrun", "run", every=30, unit="minutes")
"""

from __future__ import annotations
from pathlib import Path

MAIN = Path(__file__).resolve().parent / "main.py"
LABEL = "Alpha Dryrun OrderPlace"
SCHEDULE_LINE = '    _schedule("Alpha Dryrun OrderPlace", "alpha_outbox_orderplace_dryrun", "run", every=30, unit="minutes")\n'

def main():
    txt = MAIN.read_text(encoding="utf-8", errors="ignore")

    if LABEL in txt:
        print("Phase 26E schedule already present in main.py")
        return

    lines = txt.splitlines(True)
    out = []
    inserted = False

    # Insert right after Telemetry Digest schedule if possible
    for ln in lines:
        out.append(ln)
        if (not inserted) and ('_schedule("Telemetry Digest"' in ln):
            out.append(SCHEDULE_LINE)
            inserted = True

    if not inserted:
        # Fallback: insert near the top of the schedule block (first indented _schedule call)
        out = []
        for ln in lines:
            out.append(ln)
            if (not inserted) and ln.startswith("    _schedule("):
                out.append(SCHEDULE_LINE)
                inserted = True

    if not inserted:
        raise RuntimeError("Could not locate insertion point in main.py")

    MAIN.write_text("".join(out), encoding="utf-8")
    print("Phase 26E schedule inserted into main.py successfully.")

if __name__ == "__main__":
    main()
