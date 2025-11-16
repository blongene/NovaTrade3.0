"""
trade_guard.py

Central gate for trade intents before they hit the Outbox or Edge.

Responsibilities:
  - Normalize a generic trade intent (BUY/SELL, token, venue, quote, amount_usd, price_usd).
  - Apply C-Series venue budget clamp (Unified_Snapshot-based).
  - Run through PolicyEngine for final sizing & risk checks.
  - Return a normalized decision:
        {
          "ok": bool,
          "status": "APPROVED" | "CLIPPED" | "DENIED",
          "reason": str,
          "intent": { ... original-ish ... },
          "patched": { ... final payload ... },
        }

This is the "big red gate" that other modules (nova_trigger, rebuy_driver,
rotation_executor, etc.) should call before enqueueing.
"""

from __future__ import annotations

from typing import Dict, Any, Tuple
import os
import time

from policy_engine import PolicyEngine
from venue_budget import get_budget_for_intent
from utils import warn


def _normalize_base_intent(intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Coerce the minimal fields we need for policy:
      token, action, amount_usd, venue, quote, price_usd?
    """
    out = dict(intent or {})

    token = (out.get("token") or out.get("base") or "").upper()
    if token:
        out["token"] = token

    venue = (out.get("venue") or "").upper()
    if venue:
        out["venue"] = venue

    quote = (out.get("quote") or "").upper()
    if quote:
        out["quote"] = quote

    if "amount_usd" in out:
        try:
            out["amount_usd"] = float(out["amount_usd"])
        except Exception:
            pass

    if "price_usd" in out and out["price_usd"] is not None:
        try:
            out["price_usd"] = float(out["price_usd"])
        except Exception:
            pass

    action = (out.get("action") or out.get("side") or "").upper()
    if not action and out.get("side"):
        action = str(out["side"]).upper()
    if action:
        out["action"] = action  # BUY or SELL

    return out


def guard_trade_intent(intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main entrypoint.

    Input: a generic trade intent dict from *any* source (nova_trigger, rotation, rebuy engine).
           Must at least have "token", "amount_usd", "venue". "quote" and "price_usd" recommended.

    Output: {
        "ok": bool,
        "status": "APPROVED" | "CLIPPED" | "DENIED",
        "reason": str,
        "intent": original_normalized_intent,
        "patched": final_intent_after_policy,
    }
    """
    base = _normalize_base_intent(intent)
    token = (base.get("token") or "").upper()
    venue = (base.get("venue") or "").upper()
    quote = (base.get("quote") or "").upper()
    action = (base.get("action") or "BUY").upper()

    amount_usd = base.get("amount_usd")
    try:
        amount_usd_f = float(amount_usd)
    except Exception:
        return {
            "ok": False,
            "status": "DENIED",
            "reason": f"invalid amount_usd: {amount_usd!r}",
            "intent": base,
            "patched": dict(base),
        }

    if not token:
        return {
            "ok": False,
            "status": "DENIED",
            "reason": "missing token",
            "intent": base,
            "patched": dict(base),
        }
    if not venue:
        return {
            "ok": False,
            "status": "DENIED",
            "reason": "missing venue",
            "intent": base,
            "patched": dict(base),
        }

    # enrich base intent with normalized fields
    base["token"] = token
    base["venue"] = venue
    if quote:
        base["quote"] = quote
    base["amount_usd"] = amount_usd_f
    base["action"] = action

    # ----------------------------------------
    # C-Series: venue budget clamp
    # ----------------------------------------
    try:
        budget_usd, budget_reason = get_budget_for_intent(base)
    except Exception as e:
        warn(f"trade_guard: get_budget_for_intent error: {e}")
        budget_usd, budget_reason = None, f"budget_error:{type(e).__name__}"

    if budget_usd is not None:
        if budget_usd <= 0:
            patched = dict(base)
            patched["amount_usd"] = 0.0
            return {
                "ok": False,
                "status": "DENIED",
                "reason": f"venue_budget_zero ({budget_reason})",
                "intent": base,
                "patched": patched,
            }
        if amount_usd_f > budget_usd:
            base["amount_usd"] = budget_usd

    # ----------------------------------------
    # B-3: PolicyEngine.validate
    # ----------------------------------------
    pe = PolicyEngine()
    ok, reason, patched = pe.validate(base, asset_state=None)
    patched = patched or {}
    if not isinstance(patched, dict):
        patched = {}

    patched_amount_usd = patched.get("amount_usd", base.get("amount_usd"))

    # status determination
    if not ok:
        status = "DENIED"
    else:
        try:
            pa = float(patched_amount_usd)
            ba = float(base["amount_usd"])
            if pa + 1e-9 < ba:
                status = "CLIPPED"
            else:
                status = "APPROVED"
        except Exception:
            status = "APPROVED" if ok else "DENIED"

    return {
        "ok": bool(ok),
        "status": status,
        "reason": reason or "",
        "intent": base,
        "patched": patched,
    }
