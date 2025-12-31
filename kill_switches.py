"""kill_switches.py — Phase 24A Dual Kill Switches (Bus)

Goals
- Preserve existing env-based holds (CLOUD_HOLD, NOVA_KILL).
- Add umbrella config support via DB_READ_JSON (no new env vars required).
- Never raise; always fail-safe (if anything is odd, treat hold as OFF unless env says otherwise).

Config (optional):
DB_READ_JSON:
  {
    "kill_switch": {
      "cloud_hold": 0
    }
  }
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict


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


def cloud_hold_active() -> bool:
    """Cloud-side hold (Bus). ACTIVE unless halted.

    Sources (OR):
      - NOVA_KILL env
      - CLOUD_HOLD env
      - DB_READ_JSON.kill_switch.cloud_hold (optional umbrella)
    """
    nova_kill = _truthy(os.getenv("NOVA_KILL"))
    cloud_hold_env = _truthy(os.getenv("CLOUD_HOLD"))
    if nova_kill or cloud_hold_env:
        return True

    cfg = _load_db_read_json()
    ks = cfg.get("kill_switch") or cfg.get("kill_switches") or {}
    if isinstance(ks, dict) and _truthy(ks.get("cloud_hold")):
        return True

    return False


def cloud_hold_reason() -> str:
    """Best-effort reason string for observability."""
    if _truthy(os.getenv("NOVA_KILL")):
        return "NOVA_KILL"
    if _truthy(os.getenv("CLOUD_HOLD")):
        return "CLOUD_HOLD"
    cfg = _load_db_read_json()
    ks = cfg.get("kill_switch") or cfg.get("kill_switches") or {}
    if isinstance(ks, dict) and _truthy(ks.get("cloud_hold")):
        return "DB_READ_JSON.kill_switch.cloud_hold"
    return ""
