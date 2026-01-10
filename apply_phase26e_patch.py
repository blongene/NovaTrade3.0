#!/usr/bin/env python3
"""Apply Phase 26E patch: schedule dryrun order.place outbox job.

Usage:
  cd /opt/render/project/src
  python apply_phase26e_patch.py

Idempotent: if the block already exists, nothing changes.
"""

from __future__ import annotations

import re
from pathlib import Path

MAIN_PY = Path(__file__).resolve().parent / "main.py"
MARKER = "Alpha 26E Dryrun Order.Place"


def main() -> int:
    if not MAIN_PY.exists():
        print(f"ERROR: main.py not found at {MAIN_PY}")
        return 1

    txt = MAIN_PY.read_text(encoding="utf-8")

    if MARKER in txt:
        print("Phase 26E schedule already present in main.py (noop).")
        return 0

    # We insert right after the Telemetry Digest schedule block.
    # If your main.py differs, this still works by finding the text label.
    anchor_pat = r"(_schedule\(\s*"Daily Telemetry Digest"[\s\S]*?\)\s*)"
    m = re.search(anchor_pat, txt)
    if not m:
        print("ERROR: could not find Telemetry Digest schedule anchor in main.py")
        return 2

    insert = "
" + (
        "    # Phase 26E — dryrun BUY/SELL order.place intents from Alpha translations
"
        f"    _schedule("{MARKER}", "alpha_outbox_orderplace_dryrun", every_min=15)
"
    )

    new_txt = txt[: m.end(1)] + insert + txt[m.end(1) :]
    MAIN_PY.write_text(new_txt, encoding="utf-8")
    print("Phase 26E schedule inserted into main.py successfully (after Telemetry Digest).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
