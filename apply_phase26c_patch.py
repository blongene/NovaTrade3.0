#!/usr/bin/env python3
# apply_phase26c_patch.py — inserts Phase 26C schedule into main.py (safe, idempotent)

from __future__ import annotations
import re
from pathlib import Path

MAIN = Path(__file__).resolve().parent / "main.py"

SCHEDULE_LINE = '_schedule("Alpha Translation Preview", "alpha_translation_preview", "run_alpha_translation_preview", every=60, unit="minutes")\n'

def main():
    txt = MAIN.read_text(encoding="utf-8", errors="ignore")

    if "Alpha Translation Preview" in txt:
        print("Phase 26C schedule already present in main.py")
        return

    # Insert after Telemetry Digest schedule if present, else after imports block
    anchor = '_schedule("Telemetry Digest", "telemetry_digest", "run_telemetry_digest", when="13:10")'
    if anchor in txt:
        txt = txt.replace(anchor, anchor + "\n\n" + SCHEDULE_LINE.strip() )
        txt += "\n" if not txt.endswith("\n") else ""
        MAIN.write_text(txt, encoding="utf-8")
        print("Phase 26C schedule inserted into main.py successfully (after Telemetry Digest).")
        return

    # Fallback: insert near first _schedule definition call usage (after helper def _schedule)
    m = re.search(r"def _schedule\\(.*?\\):", txt, flags=re.S)
    if m:
        # place after the helper block by finding next blank line after function definition block end heuristic
        insert_at = txt.find("# --- Optional background loop", m.end())
        if insert_at == -1:
            insert_at = m.end()
        txt = txt[:insert_at] + "\n" + SCHEDULE_LINE + "\n" + txt[insert_at:]
        MAIN.write_text(txt, encoding="utf-8")
        print("Phase 26C schedule inserted into main.py successfully (fallback position).")
        return

    raise RuntimeError("Could not locate insertion point in main.py")

if __name__ == "__main__":
    main()
