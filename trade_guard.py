#!/usr/bin/env python3
"""
trade_guard.py

Central gate for trade intents before they hit the Outbox or Edge.

Responsibilities:
  - Normalize a generic trade intent (BUY/SELL, token, venue, quote, amount_usd, price_usd).
  - Apply C-Series venue budget clamp (Unified_Snapshot-based).
  - Enforce venue min-notional and min-volume rules.
  - Run through PolicyEngine for final sizing & risk checks.
  - Return a normalized decision:
        {
          "ok": bool,
          "status": "APPROVED" | "CLIPPED" | "DENIED",
          "reason": str,
          "intent": { ... original-ish ... },
          "patched": { ... final payload ... },
          "decision_id": str,
          "created_at": iso8601,
          "meta": {
              "source": "...",
              "venue": "...",
              "symbol": "...",
              "base": "...",
              "quote": "...",
              "requested_amount_usd": float | null,
              "approved_amount_usd": float | null,
          }
        }

This is the "big red gate" that other modules (nova_trigger, rebuy_driver,
rotation_executor, etc.) should call before enqueueing.
"""

from __future__ import annotations

from typing import Dict, Any, Tuple
import os
import time  # kept for compatibility; safe to remove if truly unused

from policy_engine import PolicyEngine
from venue_budget import get_budget_for_intent
from utils import warn
from policy_decision import PolicyDecision

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

# ---- Venue min-volume config (base-asset quantity) -------------------------
#
# These are *base* quantities per trading pair. They’re intentionally sparse;
# you can override or extend via env vars of the form:
#   MIN_VOLUME_<VENUE>_<BASE>_<QUOTE>
#
# Example:
#   MIN_VOLUME_BINANCEUS_BTC_USDT=1e-05
#
MIN_VOLUME_DEFAULTS = {
    "BINANCEUS": {
        # This matches the error you observed: "min volume 1e-05 not met"
        "BTC_USDT": 1e-05,
    },
    "COINBASE": {
        # Add pairs here as needed.
    },
    "KRAKEN": {
        # Kraken spot minimums vary; keep telemetry-only unless explicitly enabled.
    },
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


def _get_min_volume(venue: str, base: str, quote: str) -> float | None:
    """
    Per-venue, per-(base,quote) minimum *base* quantity.

    Precedence:
      1) Env var MIN_VOLUME_<VENUE>_<BASE>_<QUOTE>
      2) MIN_VOLUME_DEFAULTS above
    """
    v = (venue or "").upper()
    b = (base or "").upper()
    q = (quote or "").upper()
    if not v or not b or not q:
        return None

    env_key = f"MIN_VOLUME_{v}_{b}_{q}"
    raw = os.getenv(env_key, "").strip()
    if raw:
        try:
            val = float(raw)
            if val > 0:
                return val
        except Exception:
            # Ignore bad env and fall back to defaults
            pass

    key = f"{b}_{q}"
    return MIN_VOLUME_DEFAULTS.get(v, {}).get(key)


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
    Main entry.

    Returns a dict:
      {
        "ok": bool,
        "status": "APPROVED" | "CLIPPED" | "DENIED",
        "reason": str,
        "intent": { ... },
        "patched": { ... },
        "decision_id": str,
        "created_at": iso8601,
        "meta": { ... }  # optional extra context
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
        # Invalid notional – build a canonical decision and bail.
        patched = dict(base)
        return _make_policy_decision(
            base=base,
            patched=patched,
            ok=False,
            status="DENIED",
            reason=f"invalid amount_usd: {amount_usd!r}",
        )

    if amount_usd_f <= 0:
        patched = dict(base)
        return _make_policy_decision(
            base=base,
            patched=patched,
            ok=False,
            status="DENIED",
            reason="amount_usd <= 0",
        )

    if not token:
        patched = dict(base)
        return _make_policy_decision(
            base=base,
            patched=patched,
            ok=False,
            status="DENIED",
            reason="missing token",
        )
    if not venue:
        patched = dict(base)
        return _make_policy_decision(
            base=base,
            patched=patched,
            ok=False,
            status="DENIED",
            reason="missing venue",
        )

    base["token"] = token
    base["venue"] = venue
    if quote:
        base["quote"] = quote
    base["amount_usd"] = amount_usd_f
    base["action"] = action

    # Helper to build canonical decisions for all exits from this point on.
    def _safe_float(val):
        try:
            return float(val)
        except Exception:
            return None

    def _make_decision_local(
        ok: bool,
        status: str,
        reason: str,
        intent_dict: Dict[str, Any],
        patched_dict: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Wrap PolicyDecision construction so every outcome has a decision_id/meta.
        """
        symbol = (patched_dict.get("symbol") or intent_dict.get("symbol") or "").upper()
        quote_local = (
            patched_dict.get("quote")
            or intent_dict.get("quote")
            or quote
            or ""
        ).upper()

        decision = PolicyDecision(
            ok=bool(ok),
            status=status,
            reason=reason or "",
            intent=intent_dict,
            patched=patched_dict,
            source=intent_dict.get("source") or "trade_guard",
            venue=venue,
            symbol=symbol,
            base=token,
            quote=quote_local,
            requested_amount_usd=_safe_float(intent_dict.get("amount_usd")),
            approved_amount_usd=_safe_float(patched_dict.get("amount_usd")),
        )
        return decision.to_dict()

    # Rebind outer helper for earlier-return cases already above.
    global _make_policy_decision
    _make_policy_decision = _make_decision_local

    # --- Kraken: telemetry-only by default ---------------------------------
    if venue == "KRAKEN" and os.getenv("AUTO_ENABLE_KRAKEN", "0").lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        patched = dict(base)
        patched["amount_usd"] = 0.0
        return _make_decision_local(
            ok=False,
            status="DENIED",
            reason="venue_autotrade_disabled",
            intent_dict=base,
            patched_dict=patched,
        )

    # --- Venue min-notional guard (e.g., BinanceUS 10 USDT) ----------------
    min_notional = _get_min_notional_usd(venue, quote)
    if min_notional is not None and amount_usd_f + 1e-9 < min_notional:
        patched = dict(base)
        patched["amount_usd"] = 0.0
        return _make_decision_local(
            ok=False,
            status="DENIED",
            reason=f"below_venue_min_notional:{min_notional}",
            intent_dict=base,
            patched_dict=patched,
        )

    # --- Venue budget clamp (Unified_Snapshot) -----------------------------
    try:
        budget_usd, budget_reason = get_budget_for_intent(base)
    except Exception as e:
        warn(f"trade_guard: get_budget_for_intent error: {e}")
        budget_usd, budget_reason = None, f"budget_error:{type(e).__name__}"

    if budget_usd is not None:
        if budget_usd <= 0:
            patched = dict(base)
            patched["amount_usd"] = 0.0
            return _make_decision_local(
                ok=False,
                status="DENIED",
                reason=f"venue_budget_zero ({budget_reason})",
                intent_dict=base,
                patched_dict=patched,
            )
        if amount_usd_f > budget_usd:
            base["amount_usd"] = budget_usd

    # --- PolicyEngine (price, reserves, etc.) ------------------------------
    pe = PolicyEngine()
    ok, reason, patched = pe.validate(base, asset_state=None)
    patched = patched or {}
    if not isinstance(patched, dict):
        patched = {}

    patched.setdefault("token", token)
    patched.setdefault("venue", venue)
    if quote:
        patched.setdefault("quote", quote)

    if "amount_usd" in patched:
        try:
            patched["amount_usd"] = float(patched["amount_usd"])
        except Exception:
            pass

    # --- Min-volume guard (base quantity, after policy) --------------------
    patched_amount_usd = patched.get("amount_usd", base.get("amount_usd"))
    try:
        amt_f = float(patched_amount_usd)
    except Exception:
        amt_f = amount_usd_f

    price = patched.get("price_usd") or base.get("price_usd")
    min_vol = _get_min_volume(venue, token, quote)

    if min_vol is not None and price not in (None, 0, 0.0):
        try:
            price_f = float(price)
        except Exception:
            price_f = 0.0

        if price_f > 0:
            est_qty = amt_f / price_f  # base units
            if est_qty + 1e-12 < float(min_vol):
                # Hard deny: below minimum base quantity for this pair.
                patched["amount_usd"] = 0.0
                return _make_decision_local(
                    ok=False,
                    status="DENIED",
                    reason=f"below_venue_min_volume:{min_vol}",
                    intent_dict=base,
                    patched_dict=patched,
                )

    # --- Final status (APPROVED / CLIPPED / DENIED) ------------------------
    if not ok:
        status = "DENIED"
    else:
        try:
            ba = float(base["amount_usd"])
            pa = float(patched_amount_usd)
            status = "CLIPPED" if pa + 1e-9 < ba else "APPROVED"
        except Exception:
            status = "APPROVED" if ok else "DENIED"

    return _make_decision_local(
        ok=bool(ok),
        status=status,
        reason=reason or "",
        intent_dict=base,
        patched_dict=patched,
    )
