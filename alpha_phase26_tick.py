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
2b) Mirror Alpha "Why Nothing Happened" to shared WNH surface (presentation only).
3) (Optional) Phase 26E: enqueue approved intents into canonical commands outbox.

Safety
------
- Bus-only (no Edge changes)
- Preview-only proposal generation is gated
- Optional enqueue is separately gated
"""

from __future__ import annotations

import os
import json
import logging


def _load_db_read_json() -> dict:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _cfg_get(cfg: dict, dotted: str, default=None):
    cur = cfg
    for p in dotted.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return default if cur is None else cur


def _truthy(v: str | None) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes", "y", "on")


def run_alpha_phase26_tick() -> None:
    """
    Run Phase 26A generator then mirror.
    Optionally run Phase 26E enqueue (separately gated).
    """
    log = logging.getLogger("alpha_phase26_tick")

    cfg = _load_db_read_json()

    # ----------------------------
    # Phase 26A gates (preview)
    # ----------------------------
    # JSON-first (Render env-var slots are scarce):
    #   DB_READ_JSON.phases.phase26.alpha.planning_enabled == 1
    # Fallback to legacy envs if JSON isn't present.
    planning_enabled = _truthy(_cfg_get(cfg, "phases.phase26.alpha.planning_enabled", None))
    if _cfg_get(cfg, "phases.phase26", None) is None:
        preview_ok = _truthy(os.getenv("PREVIEW_ENABLED")) and _truthy(os.getenv("ALPHA_PREVIEW_PROPOSALS_ENABLED"))
    else:
        preview_ok = planning_enabled
    if not preview_ok:
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

    # 2b) Mirror Alpha "Why Nothing Happened" explanations (presentation-only)
    try:
        from alpha_wnh_mirror import run_alpha_wnh_mirror
        run_alpha_wnh_mirror()
    except Exception as e:
        try:
            from utils import warn
            warn(f"alpha_phase26_tick: alpha WNH mirror failed: {e}")
        except Exception:
            log.warning("alpha_phase26_tick: alpha WNH mirror failed: %s", e)

    # ----------------------------
    # Phase 26E gate (enqueue)
    # ----------------------------
    # Keep enqueue OFF unless explicitly enabled.
    # Use any one of these envs as an ON switch:
    #   PHASE26E_ENQUEUE_ENABLED=1  (recommended)
    #   ALPHA_PHASE26E_ENQUEUE_ENABLED=1
    #   ALPHA_EXECUTION_ENABLED=1   (if you prefer this as the “go” flag)
    execution_enabled = _truthy(_cfg_get(cfg, "phases.phase26.alpha.execution_enabled", None))
    if _cfg_get(cfg, "phases.phase26", None) is None:
        enqueue_ok = (
            _truthy(os.getenv("PHASE26E_ENQUEUE_ENABLED"))
            or _truthy(os.getenv("ALPHA_PHASE26E_ENQUEUE_ENABLED"))
            or _truthy(os.getenv("ALPHA_EXECUTION_ENABLED"))
        )
    else:
        enqueue_ok = execution_enabled
    if not enqueue_ok:
        return

    try:
        from alpha_phase26e_enqueue import enqueue_from_approvals
        processed, enq_new = enqueue_from_approvals(limit=25)
        if enq_new:
            log.info("alpha26e_enqueue: processed=%s enqueued_new=%s", processed, enq_new)
    except Exception as e:
        log.warning("alpha26e_enqueue: skipped/failed: %s", e)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    run_alpha_phase26_tick()
