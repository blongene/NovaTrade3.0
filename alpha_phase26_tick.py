# alpha_phase26_tick.py
"""
Phase 26A — Tick Orchestrator (Bus-only)

Cadence
-------
Run every 10–15 minutes (safe to run more frequently).

Sequence
--------
1) Generate preview-only WOULD_* proposals via the SQL-native runner.
2) Mirror today's proposals to Sheets (presentation only).
3) (Optional) Phase 26E: enqueue approved intents into canonical commands outbox.

Safety
------
- Bus-only (no Edge changes)
- Preview-only proposal generation is gated
- Optional enqueue is separately gated
"""

from __future__ import annotations

import os
import logging


def _truthy(v: str | None) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes", "y", "on")


def run_alpha_phase26_tick() -> None:
    """
    Run Phase 26A generator then mirror.
    Optionally run Phase 26E enqueue (separately gated).
    """
    log = logging.getLogger("alpha_phase26_tick")

    # ----------------------------
    # Phase 26A gates (preview)
    # ----------------------------
    preview_ok = _truthy(os.getenv("PREVIEW_ENABLED")) and _truthy(os.getenv("ALPHA_PREVIEW_PROPOSALS_ENABLED"))
    if not preview_ok:
        # Quiet skip; upstream scheduler already prints its label.
        return

    # 1) Generate proposals (SQL-native runner)
    try:
        from alpha_proposal_runner import run_alpha_proposal_runner
        run_alpha_proposal_runner()
    except Exception as e:
        # Prefer utils.warn if available, but never fail the tick because warn isn't available.
        try:
            from utils import warn
            warn(f"alpha_phase26_tick: proposal runner failed: {e}")
        except Exception:
            log.exception("alpha_phase26_tick: proposal runner failed")
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
            log.exception("alpha_phase26_tick: proposals mirror failed")
            print(f"alpha_phase26_tick: proposals mirror failed: {e}", flush=True)

    # ----------------------------
    # Phase 26E gate (enqueue)
    # ----------------------------
    # Keep enqueue OFF unless explicitly enabled.
    # Use any one of these envs as an ON switch:
    #   PHASE26E_ENQUEUE_ENABLED=1  (recommended)
    #   ALPHA_PHASE26E_ENQUEUE_ENABLED=1
    #   ALPHA_EXECUTION_ENABLED=1   (if you prefer this as the “go” flag)
    enqueue_ok = (
        _truthy(os.getenv("PHASE26E_ENQUEUE_ENABLED"))
        or _truthy(os.getenv("ALPHA_PHASE26E_ENQUEUE_ENABLED"))
        or _truthy(os.getenv("ALPHA_EXECUTION_ENABLED"))
    )
    if not enqueue_ok:
        return

    try:
        # Support either function name depending on your current module
        from alpha_phase26e_enqueue import enqueue_from_approvals
        processed, enq_new = enqueue_from_approvals(limit=25)
        if enq_new:
            log.info("alpha26e_enqueue: processed=%s enqueued_new=%s", processed, enq_new)
    except Exception as e:
        # Never fail the tick because enqueue had a problem.
        log.warning("alpha26e_enqueue: skipped/failed: %s", e)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    run_alpha_phase26_tick()
