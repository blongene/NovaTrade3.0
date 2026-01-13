# alpha_phase26_tick.py
"""\
Phase 26A — Tick Orchestrator (Bus-only)

This module exists to keep Phase 26A scheduling *boring* and *predictable*.

Cadence
-------
The tick is typically scheduled every 10–15 minutes. It is safe to run more
frequently; both generator and mirror are gated by environment flags and
dedupe rules.

Sequence
--------
1) Generate preview-only WOULD_* proposals via the SQL-native runner.
2) Mirror today's proposals to Sheets (presentation only).

Safety
------
- Bus-only (no Edge changes)
- Preview-only (requires PREVIEW_ENABLED=1 and ALPHA_PREVIEW_PROPOSALS_ENABLED=1)
- Never enqueues commands / never executes trades
"""

from __future__ import annotations

import os
import logging
logger = logging.getLogger(__name__)


def _truthy(v: str | None) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes", "y", "on")


def run_alpha_phase26_tick() -> None:
    """Run Phase 26A generator then mirror. Safe to call at any time."""

    # Hard gates — keep this boring.
    if not (_truthy(os.getenv("PREVIEW_ENABLED")) and _truthy(os.getenv("ALPHA_PREVIEW_PROPOSALS_ENABLED"))):
        # Quiet skip; upstream scheduler already logs its label.
        return

    # 1) Generate proposals (SQL-native runner)
    try:
        from alpha_proposal_runner import run_alpha_proposal_runner

        run_alpha_proposal_runner()
    except Exception as e:
        try:
            from utils import warn

            warn(f"alpha_phase26_tick: proposal runner failed: {e}")
        except Exception:
            print(f"alpha_phase26_tick: proposal runner failed: {e}", flush=True)

    # 2) Mirror proposals to Sheets (presentation-only)
    try:
        from alpha_proposals_mirror import run_alpha_proposals_mirror

        run_alpha_proposals_mirror()
    except Exception as e:
        try:
            from utils import warn

            warn(f"alpha_phase26_tick: proposals mirror failed: {e}")
        except Exception:
            print(f"alpha_phase26_tick: proposals mirror failed: {e}", flush=True)
            
    # --- Phase 26E Step 1: enqueue approved intents into commands outbox (schema-adaptive)
    try:
        from alpha_phase26e_enqueue import enqueue_from_approvals
        p, n = enqueue_from_approvals(limit=25)
        if n:
            logger.info(f"alpha26e_enqueue: processed={p} enqueued_new={n}")
    except Exception as e:
        logger.warning(f"alpha26e_enqueue: skipped/failed: {e}")
