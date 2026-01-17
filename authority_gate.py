# bus/authority_gate.py
# Phase 28 — Authority Gate (Bus-side)
# Single source of truth for Edge trust & command eligibility

from __future__ import annotations
import os
import time
from typing import Tuple, Dict

# -----------------------------------------------------------------------------
# Configuration (env-driven, DB-first compatible)
# -----------------------------------------------------------------------------

DEFAULT_LEASE_SECONDS = int(os.getenv("OUTBOX_LEASE_SECONDS", "90"))
AUTHORITY_TTL_SEC = int(os.getenv("EDGE_AUTHORITY_TTL_SEC", "600"))  # 10 min
REQUIRE_EDGE_AUTHORITY = os.getenv("REQUIRE_EDGE_AUTHORITY", "true").lower() in ("1","true","yes","on")

# -----------------------------------------------------------------------------
# In-memory authority cache (DB will replace this later)
# -----------------------------------------------------------------------------

# agent_id -> {"trusted": bool, "reason": str, "ts": int}
_AUTH_CACHE: Dict[str, Dict] = {}

# -----------------------------------------------------------------------------
# Authority evaluation
# -----------------------------------------------------------------------------

def evaluate_agent(agent_id: str) -> Tuple[bool, str, int]:
    """
    Returns:
      trusted (bool)
      reason (str)
      age_sec (int)
    """

    now = int(time.time())
    agent_id = (agent_id or "").strip() or "edge"

    entry = _AUTH_CACHE.get(agent_id)

    if entry:
        age = now - entry["ts"]
        if age <= AUTHORITY_TTL_SEC:
            return entry["trusted"], entry["reason"], age

    # --- DEFAULT TRUST LOGIC (Phase 28) -------------------------------------
    # You can later replace this with:
    #   - DB-backed agent registry
    #   - Council decisions
    #   - Signed capability grants
    # -----------------------------------------------------------------------

    trusted = True
    reason = "default_trusted"

    # Hard safety: explicit holds
    if os.getenv("EDGE_HOLD", "").lower() in ("1","true","yes","on"):
        trusted = False
        reason = "edge_hold_env"

    if os.getenv("NOVA_KILL", "").lower() in ("1","true","yes","on"):
        trusted = False
        reason = "nova_kill"

    # Cache result
    _AUTH_CACHE[agent_id] = {
        "trusted": trusted,
        "reason": reason,
        "ts": now,
    }

    return trusted, reason, 0


# -----------------------------------------------------------------------------
# Lease-block response helper
# -----------------------------------------------------------------------------

def lease_block_response(agent_id: str) -> Dict:
    """
    Standardized response when an agent is not allowed to receive commands.
    IMPORTANT: We return 200 with empty commands to prevent retry storms.
    """
    trusted, reason, age = evaluate_agent(agent_id)

    return {
        "ok": True,
        "commands": [],
        "lease_seconds": DEFAULT_LEASE_SECONDS,
        "hold": True,
        "reason": reason,
        "agent_id": agent_id,
        "age_sec": age,
    }
