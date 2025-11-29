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

# ---- Venue min-notional config ---------------------------------------------

# Conservative defaults; you can override with env vars:
#   MIN_NOTIONAL_<VENUE>_<QUOTE>, e.g. MIN_NOTIONAL_BINANCEUS_USDT=10
MIN_NOTIONAL_DEFAULTS = {
    # BinanceUS rejects notional < 10 USDT – we saw this in live tests.
    "BINANCEUS": {"USDT": 10.0, "USDC": 10.0, "USD": 10.0},
    # Coinbase allows very small trades; leave empty unless you want a floor.
    "COINBASE": {},
    # Kraken is *telemetry-only* by default; we still define a bucket for later.
    "KRAKEN": {},
}

def _get_min_notional_usd(venue: str, quote: str) -> float | None:
    """
    Per-venue, per-quote minimum notional (in quote units).

    Precedence:
      1) Env var MIN_NOTIONAL_<VENUE>_<QUOTE>
      2) MIN_NOTIONAL_DEFAULTS above
    """
    v = (venue or "").upper()
    q = (quote or "").upper()
    if not v or not q:
        return None

    # Env override has highest priority
    env_key = f"MIN_NOTIONAL_{v}_{q}"
    raw = os.getenv(env_key, "").strip()
    if raw:
        try:
            val = float(raw)
            if val > 0:
                return val
        except Exception:
            # Bad env value -> ignore and fall back to defaults
            pass

    return MIN_NOTIONAL_DEFAULTS.get(v, {}).get(q)

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

    if amount_usd_f <= 0:
        return {
            "ok": False,
            "status": "DENIED",
            "reason": "amount_usd <= 0",
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

    # Normalize + enrich
    base["token"] = token
    base["venue"] = venue
    if quote:
        base["quote"] = quote
    base["amount_usd"] = amount_usd_f
    base["action"] = action

    # ----------------------------------------------------------------------
    # Kraken safety: telemetry-only by default
    # ----------------------------------------------------------------------
    if venue == "KRAKEN" and os.getenv("AUTO_ENABLE_KRAKEN", "0").lower() not in {"1", "true", "yes", "on"}:
        patched = dict(base)
        patched["amount_usd"] = 0.0
        return {
            "ok": False,
            "status": "DENIED",
            "reason": "venue_autotrade_disabled",
            "intent": base,
            "patched": patched,
        }

    # ----------------------------------------------------------------------
    # Venue-level minimum notional guard (BinanceUS, etc.)
    # ----------------------------------------------------------------------
    min_notional = _get_min_notional_usd(venue, quote)
    if min_notional is not None and amount_usd_f + 1e-9 < min_notional:
        patched = dict(base)
        patched["amount_usd"] = 0.0
        return {
            "ok": False,
            "status": "DENIED",
            "reason": f"below_venue_min_notional:{min_notional}",
            "intent": base,
            "patched": patched,
        }

    # ----------------------------------------------------------------------
    # C-Series: venue budget clamp (Unified_Snapshot → max per venue)
    # ----------------------------------------------------------------------
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
            # Truncate ask to budget, mark as potential clip later
            base["amount_usd"] = budget_usd

    # ----------------------------------------------------------------------
    # B-3: PolicyEngine.validate (all the Ash / policy.yaml logic)
    # ----------------------------------------------------------------------
    pe = PolicyEngine()
    ok, reason, patched = pe.validate(base, asset_state=None)
    patched = patched or {}
    if not isinstance(patched, dict):
        patched = {}

    # Ensure some core fields exist on the outgoing payload
    patched.setdefault("token", token)
    patched.setdefault("venue", venue)
    if quote:
        patched.setdefault("quote", quote)

    if "amount_usd" in patched:
        try:
            patched["amount_usd"] = float(patched["amount_usd"])
        except Exception:
            pass

    patched_amount_usd = patched.get("amount_usd", base.get("amount_usd"))

    # Final status classification
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
