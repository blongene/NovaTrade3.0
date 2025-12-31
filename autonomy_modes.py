#!/usr/bin/env python3
"""
autonomy_modes.py

Phase 20B – Autonomy Modes & Safety Rails

Single source of truth for "how autonomous" NovaTrade is allowed to be,
based on environment variables.

Usage:
    from autonomy_modes import get_autonomy_state, format_autonomy_status

    state = get_autonomy_state()
    notes = notes + "; " + format_autonomy_status(state)
"""

from __future__ import annotations

import os
import json
from typing import Any, Dict


def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _float_env(name: str) -> float | None:
    v = os.getenv(name)
    if not v:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _json_env(name: str) -> Any:
    v = os.getenv(name)
    if not v:
        return None
    try:
        return json.loads(v)
    except Exception:
        return None


def get_autonomy_state() -> Dict[str, Any]:
    """
    Compute a structured autonomy state snapshot from environment vars.

    Returned shape (stable contract):

        {
          "mode": "MANUAL_ONLY" | "SEMI_AUTO" | "AUTO_WITH_BRAKES",
          "edge_mode": "dryrun" | "live" | "unknown",
          "holds": {
            "cloud": bool,
            "edge": bool,
            "nova": bool,
          },
          "switches": {
            "nt_enqueue_live": bool,
            "auto_enable_kraken": bool,
          },
          "limits": {
            "canary_max_usd": float | null,
            "quote_floors": dict | null,
          },
        }

    This never raises – if anything looks odd it falls back to safe defaults.
    """
    edge_mode = (os.getenv("EDGE_MODE") or "dryrun").strip().lower()
    if edge_mode not in {"dryrun", "live"}:
        edge_mode = "unknown"

    holds = {
        "cloud": (_bool_env("CLOUD_HOLD", False) or _dbread_cloud_hold()),
        "edge": _bool_env("EDGE_HOLD", False),
        "nova": _bool_env("NOVA_KILL", False),
    }

    switches = {
        "nt_enqueue_live": _bool_env("NT_ENQUEUE_LIVE", True),
        "auto_enable_kraken": _bool_env("AUTO_ENABLE_KRAKEN", False),
    }

    limits = {
        "canary_max_usd": _float_env("POLICY_CANARY_MAX_USD"),
        "quote_floors": _json_env("QUOTE_FLOORS_JSON"),
    }

    # ---- derive coarse autonomy mode --------------------------------------
    # Priority of brakes:
    #   1) NOVA_KILL or CLOUD_HOLD -> MANUAL_ONLY
    #   2) Edge not fully live OR EDGE_HOLD OR NT_ENQUEUE_LIVE=0 -> SEMI_AUTO
    #   3) Else -> AUTO_WITH_BRAKES (policy + canary + floors govern)
    if holds["nova"] or holds["cloud"]:
        mode = "MANUAL_ONLY"
    elif edge_mode != "live" or holds["edge"] or not switches["nt_enqueue_live"]:
        mode = "SEMI_AUTO"
    else:
        mode = "AUTO_WITH_BRAKES"

    return {
        "mode": mode,
        "edge_mode": edge_mode,
        "holds": holds,
        "switches": switches,
        "limits": limits,
    }


def format_autonomy_status(state: Dict[str, Any] | None = None) -> str:
    """
    Compact, human-readable summary string for logs / Notes fields.

    Example:
        "autonomy=AUTO_WITH_BRAKES; edge=live; holds=none; canary<=11"

    If `state` is None, this calls get_autonomy_state() internally.
    """
    if state is None:
        state = get_autonomy_state()

    mode = state.get("mode", "UNKNOWN")
    edge_mode = state.get("edge_mode", "unknown")

    holds = state.get("holds") or {}
    active_holds = [name for name, on in holds.items() if on]
    holds_str = ",".join(active_holds) if active_holds else "none"

    limits = state.get("limits") or {}
    canary = limits.get("canary_max_usd")
    if isinstance(canary, (int, float)) and canary > 0:
        canary_str = f"canary<={canary:g}"
    else:
        canary_str = "canary=off"

    return f"autonomy={mode}; edge={edge_mode}; holds={holds_str}; {canary_str}"


if __name__ == "__main__":  # simple local debug
    import json as _json
    s = get_autonomy_state()
    print(_json.dumps(s, indent=2, default=str))
    print()
    print(format_autonomy_status(s))
