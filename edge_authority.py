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

DB-first note
- Preferred trust signal is Postgres table nova_telemetry.
- If nova_telemetry is empty/unavailable (common during Sheets→DB migration),
  we gracefully degrade to Wallet_Monitor and then NovaHeartbeat freshness.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
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
    if "enabled" in ea:
        return _truthy(ea.get("enabled"))
    return True


def max_age_sec() -> int:
    ea = _cfg()
    try:
        v = int(ea.get("max_age_sec") or 300)
        return max(30, min(v, 3600 * 24))
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


def _parse_ts(ts: Any) -> Optional[datetime]:
    try:
        s = str(ts).strip()
        if not s:
            return None
        # Sheets format: "YYYY-MM-DD HH:MM:SS"
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _age_seconds_from_sheet(tab: str, agent_id: str) -> Optional[int]:
    """
    Fallback: compute age from latest row in a Sheets/Cache tab.
    get_records_cached() in this codebase already abstracts DB-first vs Sheets mirror.
    """
    try:
        from utils import get_records_cached  # type: ignore
    except Exception:
        return None

    try:
        rows = get_records_cached(tab, ttl_s=0) or []
        if not rows:
            return None

        cand = []
        for r in rows:
            if not isinstance(r, dict):
                continue

            a = (r.get("Agent") or r.get("agent_id") or r.get("agent") or "").strip()
            if a and a != agent_id:
                continue

            ts = r.get("Timestamp") or r.get("ts") or r.get("created_at")
            dt = _parse_ts(ts)
            if dt:
                cand.append(dt)

        if not cand:
            return None

        latest = max(cand)
        now = datetime.now(timezone.utc)
        return int((now - latest).total_seconds())
    except Exception:
        return None


def _latest_telemetry_age_seconds(agent_id: str) -> Optional[int]:
    """
    Preferred: Postgres nova_telemetry.
    Fallbacks:
      - Wallet_Monitor freshness
      - NovaHeartbeat freshness
    """
    # 1) DB telemetry (best)
    try:
        from db_backbone import _get_conn  # type: ignore

        conn = _get_conn()
        if conn:
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
            if row and row[0] is not None:
                return int(row[0])
    except Exception:
        pass

    # 2) Graceful degradation: these are real “edge freshness” surfaces today
    age = _age_seconds_from_sheet("Wallet_Monitor", agent_id)
    if age is not None:
        return age

    age = _age_seconds_from_sheet("NovaHeartbeat", agent_id)
    if age is not None:
        return age

    return None


def evaluate_agent(agent_id: str) -> Tuple[bool, str, Optional[int]]:
    """Return (trusted, reason, age_sec).

    Supports comma-separated agent_id (e.g., "edge-primary,edge-nl1") by taking the freshest allowed.
    """
    if not authority_enabled():
        return True, "edge_authority_disabled", None

    ids = [s.strip() for s in str(agent_id).split(",") if s.strip()] or [str(agent_id).strip()]

    allow = allow_agents()
    if allow is not None:
        ids_allowed = [i for i in ids if i in allow]
        if not ids_allowed:
            return False, "agent_not_allowed", None
        ids = ids_allowed

    ages = []
    for aid in ids:
        age = _latest_telemetry_age_seconds(aid)
        if age is not None:
            ages.append((aid, age))

    if not ages:
        return False, "no_telemetry", None

    best_agent, best_age = sorted(ages, key=lambda x: x[1])[0]

    if best_age > max_age_sec():
        return False, f"stale_telemetry>{max_age_sec()}s", best_age

    return True, "ok", best_age


def lease_block_response(agent_id: str) -> Dict[str, Any]:
    """Standard response shape for /api/commands/pull when dispatch is blocked."""
    trusted, reason, age = evaluate_agent(agent_id)
    return {
        "ok": True,
        "commands": [],
        "hold": (not trusted),
        "reason": reason,
        "agent_id": agent_id,
        "age_sec": age,
    }
