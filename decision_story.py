#!/usr/bin/env python3
"""
decision_story.py

Phase 20C/20D – Decision Stories + Council Influence

Generate short, human-readable explanations for policy decisions.

Always fails safe: if anything is malformed, we fall back to the
plain reason string and never raise.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def _safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def _format_council(decision: Dict[str, Any]) -> str:
    council = decision.get("council")
    if not isinstance(council, dict):
        return ""

    active = [
        name for name, w in council.items()
        if isinstance(w, (int, float)) and w > 0.05
    ]
    if not active:
        return ""

    labels = {
        "soul": "Soul",
        "nova": "Nova",
        "orion": "Orion",
        "ash": "Ash",
        "lumen": "Lumen",
        "vigil": "Vigil",
    }
    pretty = [labels.get(a, a) for a in active]
    return "Influences: " + ", ".join(pretty)


def generate_decision_story(
    intent: Dict[str, Any],
    decision: Dict[str, Any],
    autonomy_state: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Build a concise narrative based on:
      - intent (token/venue/quote/amount_usd/side)
      - decision (ok/status/reason/patched_intent/council)
      - autonomy_state (optional; mode/holds/edge_mode)
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
    reason = str(decision.get("reason") or "")

    # Autonomy context (optional)
    autonomy_label = None
    mode_label = None
    edge_mode = None
    holds = {}
    if isinstance(autonomy_state, dict):
        autonomy_label = autonomy_state.get("autonomy")
        mode_label = autonomy_state.get("mode")
        edge_mode = autonomy_state.get("edge_mode")
        holds = autonomy_state.get("holds") or {}

    # --- Prefix: what trade is this? ---------------------------------------
    if token and venue:
        if amt_req is not None:
            prefix = f"{side} {token} ${amt_req:,.2f} on {venue}"
        else:
            prefix = f"{side} {token} on {venue}"
    elif token:
        prefix = f"{side} {token}"
    else:
        prefix = side or "TRADE"

    # --- Outcome -----------------------------------------------------------
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

    # --- Details: reason, autonomy, council -------------------------------
    parts = []

    if reason:
        parts.append(reason)

    auto_fragments = []
    if autonomy_label:
        auto_fragments.append(f"autonomy={autonomy_label}")
    if mode_label and mode_label != autonomy_label:
        auto_fragments.append(f"mode={mode_label}")
    if edge_mode:
        auto_fragments.append(f"edge={edge_mode}")
    active_holds = [name for name, on in holds.items() if on]
    if active_holds:
        auto_fragments.append("holds=" + ",".join(active_holds))

    council_text = _format_council(decision)
    if council_text:
        parts.append(council_text)

    details = "; ".join(parts) if parts else ""

    if details:
        return f"{prefix} {outcome}. {details}"
    else:
        return f"{prefix} {outcome}."
