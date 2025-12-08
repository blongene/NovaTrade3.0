#!/usr/bin/env python3
"""
decision_story.py

Phase 20C – Decision Stories

Generate short, human-readable explanations for policy decisions.

This is intentionally conservative and backwards-compatible:
- If we can't understand the decision dict, we fall back to reason.
- Callers can store the story in decision["story"] and/or logs.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def _safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def generate_decision_story(
    intent: Dict[str, Any],
    decision: Dict[str, Any],
    autonomy_state: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Build a concise narrative based on:
      - intent (token/venue/quote/amount_usd/side)
      - decision (ok/status/reason/patched_intent)
      - autonomy_state (optional; mode/edge_mode/holds/limits)

    Always returns a short, single-line string.
    """

    if not isinstance(intent, dict):
        intent = {}
    if not isinstance(decision, dict):
        decision = {}

    token = str(intent.get("token") or intent.get("base") or "").upper()
    venue = str(intent.get("venue") or "").upper()
    quote = str(intent.get("quote") or "").upper()
    side  = str(intent.get("action") or intent.get("side") or "BUY").upper()

    amt_req = _safe_float(intent.get("amount_usd"))
    patched = decision.get("patched_intent") or {}
    amt_allowed = _safe_float(patched.get("amount_usd", amt_req))

    ok = bool(decision.get("ok", False))
    status = str(decision.get("status") or ("ok" if ok else "blocked")).upper()
    reason = str(decision.get("reason") or "")

    # Autonomy context (optional)
    mode = None
    edge_mode = None
    if isinstance(autonomy_state, dict):
        mode = autonomy_state.get("mode")
        edge_mode = autonomy_state.get("edge_mode")

    # --- Build prefix describing the trade itself --------------------------
    if token and venue:
        if amt_req is not None:
            prefix = f"{side} {token} ${amt_req:,.2f} on {venue}"
        else:
            prefix = f"{side} {token} on {venue}"
    elif token:
        prefix = f"{side} {token}"
    else:
        prefix = side or "TRADE"

    # --- Outcome text ------------------------------------------------------
    if not ok:
        outcome = "DENIED"
    else:
        if amt_req is not None and amt_allowed is not None:
            if abs(amt_allowed - amt_req) > 1e-6:
                outcome = f"APPROVED (resized → ${amt_allowed:,.2f})"
            else:
                outcome = "APPROVED"
        else:
            outcome = "APPROVED"

    # --- Reason / status fragment -----------------------------------------
    details_parts = []

    if reason:
        details_parts.append(reason)

    # Include autonomy info if available
    auto_fragments = []
    if mode:
        auto_fragments.append(f"autonomy={mode}")
    if edge_mode:
        auto_fragments.append(f"edge={edge_mode}")

    if auto_fragments:
        details_parts.append(", ".join(auto_fragments))

    details = "; ".join(details_parts) if details_parts else ""

    if details:
        return f"{prefix} {outcome}. {details}"
    else:
        return f"{prefix} {outcome}."
