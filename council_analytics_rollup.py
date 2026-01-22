# council_analytics_rollup.py
"""Council Analytics Rollup (Bus-driven)

This is the Bus replacement for legacy Apps Script 'syncCouncilAnalytics' / 'syncCouncilAll'
at a *minimal viable* level:

- Emits a single Decision_Analytics row summarizing recent WNH outcomes
- Keeps it cheap: reads the last N rows from the Why_Nothing_Happened sheet
- Designed to be scheduled from main.py (no Render cron required)

NOTE
----
Your Decision_Analytics tab is already being written to by wnh_daily_summary and wnh_weekly_digest
(via decision_analytics_rollup / wnh_weekly_digest). This module is optional.
If you already see rollups landing in Decision_Analytics, you can skip scheduling this.

Config (DB_READ_JSON):
  {
    "council_rollups": {
      "analytics": { "enabled": 1, "limit": 400 }
    }
  }
"""

from __future__ import annotations

import os, json, logging
from typing import Any, Dict

log = logging.getLogger("council_analytics_rollup")

def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).strip().lower() in {"1","true","yes","y","on"}

def _load_db_read_json() -> dict:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        o = json.loads(raw)
        return o if isinstance(o, dict) else {}
    except Exception:
        return {}

def _cfg() -> dict:
    cfg = _load_db_read_json()
    roll = cfg.get("council_rollups") or {}
    if isinstance(roll, dict):
        sub = roll.get("analytics") or {}
        return sub if isinstance(sub, dict) else {}
    return {}

def run_council_analytics_rollup(force: bool = False) -> Dict[str, Any]:
    cfg = _cfg()
    if not _truthy(cfg.get("enabled", 0)) and not force:
        return {"ok": False, "skipped": True, "reason": "disabled"}

    # If your existing decision_analytics_rollup has a function, prefer it.
    try:
        from decision_analytics_rollup import emit_wnh_daily_rollup  # type: ignore
        # Calling with utc_day=today makes it idempotent for the day (dedupe should handle repeats)
        return emit_wnh_daily_rollup()
    except Exception as e:
        log.warning("council_analytics_rollup: delegate failed: %s", e)
        return {"ok": False, "reason": f"delegate_failed:{e.__class__.__name__}"}
