#!/usr/bin/env python3
"""
council_influence.py

Phase 20D – Council Influence Tagging

Lightweight helper that annotates a policy decision with which
Council voices influenced the outcome.

Voices:
  - soul   : Brett / Captain (explicit human intent)
  - nova   : Heart / momentum / manual rebuys
  - orion  : Hands / execution feasibility
  - ash    : Mind / structural rules & invariants
  - lumen  : Light / visibility & diagnostics
  - vigil  : Shadow / risk containment & objections
"""

from __future__ import annotations

from typing import Any, Dict, Optional


VOICE_KEYS = ("soul", "nova", "orion", "ash", "lumen", "vigil")

try:
    from ace_bias import apply_ace_bias  # Phase 21 – ACE bias
except Exception:  # hard fallback if module is missing
    def apply_ace_bias(council):
        return council

def _base_council() -> Dict[str, float]:
    return {k: 0.0 for k in VOICE_KEYS}


def apply_council_influence(
    intent: Dict[str, Any],
    decision: Dict[str, Any],
    autonomy_ctx: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    """
    Build a simple council influence model for a given intent/decision.

    intent : normalized intent dict
    decision : policy decision (ok, reason, patched_intent, flags, ...)
    autonomy_ctx : optional {"autonomy": "...", "mode": "live|dryrun"}

    Returns {voice: weight} for each council voice.
    """
    council = _base_council()

    if not isinstance(intent, dict):
        intent = {}
    if not isinstance(decision, dict):
        decision = {}

    # Manual rebuys always start as a Soul + Nova act.
    council["soul"] = 1.0
    council["nova"] = 1.0

    reason = str(decision.get("reason") or "").lower()
    ok = bool(decision.get("ok"))

    # Flags (if present) help refine influence.
    flags = decision.get("flags") or []
    if isinstance(flags, str):
        flags = [flags]
    if not isinstance(flags, list):
        flags = []

    # --- Structural / Ash influences ---------------------------------------
    structural_markers = (
        "min_notional",
        "min volume",
        "min qty",
        "min quantity",
        "blocked symbol",
        "blocked_symbols",
        "cooldown",
    )
    if any(m in reason for m in structural_markers) or any(
        "min_notional" in f or "min_qty" in f or "cooldown" in f for f in flags
    ):
        council["ash"] = max(council["ash"], 1.0)

    # --- Risk / Vigil influences -------------------------------------------
    risk_markers = (
        "insufficient quote",
        "below min reserve",
        "reserve_unknown",
        "canary",
        "max_per_coin",
        "risk",
    )
    if any(m in reason for m in risk_markers) or any(
        "insufficient_quote" in f or "below_min_reserve" in f or "clamped" in f
        for f in flags
    ):
        council["vigil"] = max(council["vigil"], 1.0)

    # Hard denials: Vigil definitely had a say.
    if not ok:
        council["vigil"] = max(council["vigil"], 0.7)

    # --- Execution / Orion influences --------------------------------------
    if ok:
        council["orion"] = max(council["orion"], 1.0)

    # --- Visibility / Lumen influences -------------------------------------
    visibility_markers = ("price unknown", "price_unknown", "reserve_unknown")
    if any(m in reason for m in visibility_markers) or any(
        "price_unknown" in f or "reserve_unknown" in f for f in flags
    ):
        council["lumen"] = max(council["lumen"], 1.0)

    # Autonomy context tweaks
    if isinstance(autonomy_ctx, dict):
        auto = str(autonomy_ctx.get("autonomy") or "").lower()
        mode = str(autonomy_ctx.get("mode") or "").lower()

        if "dryrun" in auto or mode == "dryrun":
            council["lumen"] = max(council["lumen"], 0.7)

        if "live_enqueued" in auto:
            council["orion"] = max(council["orion"], 1.0)

        if "blocked" in auto or "manual_only" in auto:
            council["vigil"] = max(council["vigil"], 0.7)

    try:
        council = apply_ace_bias(council)
    except Exception:
        # If anything goes wrong, keep original weights
        pass

    return council
