#!/usr/bin/env python3
# apply_phase26d_patch.py — inserts Phase 26D-preview schedule into main.py (idempotent)

from __future__ import annotations
from pathlib import Path

MAIN = Path(__file__).resolve().parent / "main.py"
SCHEDULE_LINE = '_schedule("Alpha Outbox Preview", "alpha_outbox_preview", "run_alpha_outbox_preview", every=30, unit="minutes")\n'

def main():
    txt = MAIN.read_text(encoding="utf-8", errors="ignore")

    if "Alpha Outbox Preview" in txt:
        print("Phase 26D-preview schedule already present in main.py")
        return

    # Prefer inserting after Alpha Translation Preview schedule if present
    anchor = "Alpha Translation Preview"
    if anchor in txt:
        lines = txt.splitlines(True)
        out = []
        inserted = False
        for ln in lines:
            out.append(ln)
            if (not inserted) and (anchor in ln) and ("_schedule(" in ln):
                out.append(SCHEDULE_LINE)
                inserted = True
        if inserted:
            MAIN.write_text("".join(out), encoding="utf-8")
            print("Phase 26D-preview schedule inserted into main.py successfully (after Alpha Translation Preview).")
            return

    # Fallback: after Telemetry Digest schedule
    anchor2 = "Telemetry Digest"
    if anchor2 in txt:
        lines = txt.splitlines(True)
        out = []
        inserted = False
        for ln in lines:
            out.append(ln)
            if (not inserted) and (anchor2 in ln) and ("_schedule(" in ln):
                out.append("\n" + SCHEDULE_LINE)
                inserted = True
        if inserted:
            MAIN.write_text("".join(out), encoding="utf-8")
            print("Phase 26D-preview schedule inserted into main.py successfully (after Telemetry Digest).")
            return

    raise RuntimeError("Could not locate insertion point in main.py")

if __name__ == "__main__":
    main()
