#!/usr/bin/env python3
"""
apply_phase26a_step5_patch.py

Adds a scheduled Step 5 tick to main.py so Alpha_CommandPreviews stays fresh.
"""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MAIN = ROOT / "main.py"

STEP5_SENTINEL = "Phase 26A Step 5 schedule inserted into main.py successfully."

def main() -> None:
    if not MAIN.exists():
        raise SystemExit("main.py not found")

    src = MAIN.read_text(encoding="utf-8")

    if STEP5_SENTINEL in src:
        print("Step 5 already applied.")
        return

    insert_block = """
    # ---------------------------
    # Phase 26A Step 5: Command Previews mirror (NO enqueue)
    # ---------------------------
    try:
        from alpha_command_previews_mirror import run_alpha_command_previews_mirror
        sched.add_job(
            run_alpha_command_previews_mirror,
            "interval",
            minutes=int(os.getenv("ALPHA_COMMAND_PREVIEWS_MIRROR_MINUTES", "15")),
            id="alpha_command_previews_mirror",
            replace_existing=True,
        )
    except Exception as e:
        logger.warning(f"Phase26A Step5 scheduler: failed to add job: {e}")
"""

    # Insert right before the app starts (best-effort), else append.
    marker = "if __name__ == "__main__":"
    if marker in src:
        parts = src.split(marker)
        src = parts[0] + insert_block + "\n\n" + marker + parts[1]
    else:
        src = src + "\n\n" + insert_block

    src += "\n\n# " + STEP5_SENTINEL + "\n"
    MAIN.write_text(src, encoding="utf-8")
    print(STEP5_SENTINEL)

if __name__ == "__main__":
    main()
