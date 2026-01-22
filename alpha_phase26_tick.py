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
3) Mirror Alpha WNH explanations (presentation only).
4) Emit WNH daily summary + weekly digest (presentation only).
5) (Optional) Phase 26E: enqueue approved intents into canonical commands outbox.

Safety
------
- Bus-only (no Edge changes)
- Preview-only proposal generation is gated
- Optional enqueue is separately gated
- Every step is best-effort (never breaks the tick)
"""

from __future__ import annotations

import os
import json
import logging
from typing import Any, Dict


def _load_db_read_json() -> Dict[str, Any]:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _cfg_get(cfg: Dict[str, Any], dotted: str, default=None):
    cur: Any = cfg
    for p in dotted.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return default if cur is None else cur


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes", "y", "on")


def run_alpha_phase26_tick() -> None:
    """Run Phase 26A generator then mirrors; optionally run Phase 26E enqueue."""
    log = logging.getLogger("alpha_phase26_tick")
    cfg = _load_db_read_json()

    # ----------------------------
    # Phase 26A gate (preview)
    # ----------------------------
    # JSON-first:
    #   DB_READ_JSON.phases.phase26.alpha.planning_enabled == 1
    # Legacy env fallback:
    #   PREVIEW_ENABLED=1 and ALPHA_PREVIEW_PROPOSALS_ENABLED=1
    planning_enabled = _truthy(_cfg_get(cfg, "phases.phase26.alpha.planning_enabled", None))
    json_phase26_present = _cfg_get(cfg, "phases.phase26", None) is not None

    if not json_phase26_present:
        preview_ok = _truthy(os.getenv("PREVIEW_ENABLED")) and _truthy(os.getenv("ALPHA_PREVIEW_PROPOSALS_ENABLED"))
    else:
        preview_ok = planning_enabled

    if not preview_ok:
        return  # quiet skip

    # 1) Generate proposals (SQL-native runner)
    try:
        from alpha_proposal_runner import run_alpha_proposal_runner  # type: ignore
        run_alpha_proposal_runner()
    except Exception as e:
        try:
            from utils import warn  # type: ignore
            warn(f"alpha_phase26_tick: proposal runner failed: {e}")
        except Exception:
            log.exception("alpha_phase26_tick: proposal runner failed")
            print(f"alpha_phase26_tick: proposal runner failed: {e}", flush=True)

    # 2) Mirror proposals to Sheets (presentation-only)
    try:
        from alpha_proposals_mirror import run_alpha_proposals_mirror  # type: ignore
        run_alpha_proposals_mirror()
    except Exception as e:
        try:
            from utils import warn  # type: ignore
            warn(f"alpha_phase26_tick: proposals mirror failed: {e}")
        except Exception:
            log.exception("alpha_phase26_tick: proposals mirror failed")
            print(f"alpha_phase26_tick: proposals mirror failed: {e}", flush=True)

    # 3) Mirror Alpha WNH (presentation-only; deduped inside mirror)
    try:
        from alpha_wnh_mirror import run_alpha_wnh_mirror  # type: ignore
        run_alpha_wnh_mirror()
    except Exception as e:
        try:
            from utils import warn  # type: ignore
            warn(f"alpha_phase26_tick: alpha WNH mirror failed: {e}")
        except Exception:
            log.warning("alpha_phase26_tick: alpha WNH mirror failed: %s", e)

    # 4) WNH daily summary + weekly digest (presentation-only; both deduped)
    try:
        from wnh_daily_summary import run_wnh_daily_summary  # type: ignore
        run_wnh_daily_summary()
    except Exception:
        pass

    try:
        from wnh_weekly_digest import run_wnh_weekly_digest  # type: ignore
        run_wnh_weekly_digest()
    except Exception:
        pass

    # ----------------------------
    # Phase 26E gate (enqueue)
    # ----------------------------
    # Keep enqueue OFF unless explicitly enabled.
    # JSON-first:
    #   DB_READ_JSON.phases.phase26.alpha.execution_enabled == 1
    # Legacy env fallback:
    #   PHASE26E_ENQUEUE_ENABLED=1  (recommended)
    #   ALPHA_PHASE26E_ENQUEUE_ENABLED=1
    #   ALPHA_EXECUTION_ENABLED=1
    execution_enabled = _truthy(_cfg_get(cfg, "phases.phase26.alpha.execution_enabled", None))

    if not json_phase26_present:
        enqueue_ok = (
            _truthy(os.getenv("PHASE26E_ENQUEUE_ENABLED"))
            or _truthy(os.getenv("ALPHA_PHASE26E_ENQUEUE_ENABLED"))
            or _truthy(os.getenv("ALPHA_EXECUTION_ENABLED"))
        )
    else:
        enqueue_ok = execution_enabled

    if not enqueue_ok:
        return

    # Enqueue approved intents into canonical outbox
    try:
        from alpha_phase26e_enqueue import enqueue_from_approvals  # type: ignore
        processed, enq_new = enqueue_from_approvals(limit=25)
        if enq_new:
            log.info("alpha26e_enqueue: processed=%s enqueued_new=%s", processed, enq_new)
    except Exception as e:
        log.warning("alpha26e_enqueue: skipped/failed: %s", e)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    run_alpha_phase26_tick()
