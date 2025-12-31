"""edge_authority.py — Phase 24C (Bus)

Purpose
- Formalize the trust boundary for Edge command leasing (/api/commands/pull).
- Only lease commands when the requesting agent is considered *healthy*.

Design goals
- No new Bus env vars required (Render env var limits).
- Optional configuration via DB_READ_JSON:
    {
      "edge_authority": {
        "enabled": 1,
        "max_age_sec": 300,
        "allow_agents": ["edge-primary","edge-nl1"]
      }
    }

Fail-safe posture
- If DB is unavailable or telemetry missing, we treat the agent as NOT trusted
  (deny lease) but return ok=true with empty commands to avoid retry storms.
- If you prefer permissive behavior, set edge_authority.enabled=0.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional, Tuple


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _load_db_read_json() -> Dict[str, Any]:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _cfg() -> Dict[str, Any]:
    cfg = _load_db_read_json()
    ea = cfg.get("edge_authority") or {}
    return ea if isinstance(ea, dict) else {}


def authority_enabled() -> bool:
    ea = _cfg()
    # default ON once Phase 24C is installed; can be disabled explicitly
    if "enabled" in ea:
        return _truthy(ea.get("enabled"))
    return True


def max_age_sec() -> int:
    ea = _cfg()
    try:
        v = int(ea.get("max_age_sec") or 300)
        return max(30, min(v, 3600))
    except Exception:
        return 300


def allow_agents() -> Optional[list]:
    ea = _cfg()
    v = ea.get("allow_agents") or ea.get("agents") or None
    if not v:
        return None
    if isinstance(v, str):
        return [s.strip() for s in v.split(",") if s.strip()]
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    return None


def _latest_telemetry_age_seconds(agent_id: str) -> Optional[int]:
    """Return age in seconds of latest nova_telemetry row for agent_id, or None."""
    try:
        from db_backbone import _get_conn  # type: ignore
    except Exception:
        return None
    try:
        conn = _get_conn()
        if not conn:
            return None
        cur = conn.cursor()
        cur.execute(
            """
            SELECT EXTRACT(EPOCH FROM (now() - MAX(created_at)))::int
            FROM nova_telemetry
            WHERE agent_id = %s
            """,
            (agent_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        age = row[0]
        if age is None:
            return None
        return int(age)
    except Exception:
        return None


def evaluate_agent(agent_id: str) -> Tuple[bool, str, Optional[int]]:
    """Return (trusted, reason, age_sec)."""
    if not authority_enabled():
        return True, "edge_authority_disabled", None

    allow = allow_agents()
    if allow is not None and agent_id not in allow:
        return False, "agent_not_allowed", None

    age = _latest_telemetry_age_seconds(agent_id)
    if age is None:
        return False, "no_telemetry", None

    if age > max_age_sec():
        return False, f"stale_telemetry>{max_age_sec()}s", age

    return True, "ok", age


def lease_block_response(agent_id: str) -> Dict[str, Any]:
    trusted, reason, age = evaluate_agent(agent_id)
    return {
        "ok": True,
        "commands": [],
        "hold": True,
        "reason": reason,
        "agent_id": agent_id,
        "age_sec": age,
    }
