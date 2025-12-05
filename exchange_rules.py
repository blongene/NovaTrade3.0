#!/usr/bin/env python3
"""
exchange_rules.py — NovaTrade 3.0 (Pin 5)

Central place for venue-specific execution rules that we can check
*before* firing commands at the Edge executors.

Goals:
  - Avoid obvious exchange rejections (min notional, unknown pairs).
  - Normalize token/quote for venues where our default choice is invalid.
  - Provide a single, testable surface used by manual_rebuy_policy
    (and later other intent types).

Public API:
  validate_exchange_rules(intent: dict) -> (ok: bool, reason: str, patched_intent: dict)

Expected minimal fields in `intent` (already normalized by caller):
  token: str (UPPER)
  venue: str (UPPER, e.g. 'BINANCEUS', 'COINBASE', 'KRAKEN')
  amount_usd: float
  quote: optional str (UPPER, e.g. 'USDT', 'USD')
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

from utils import info


# ---------------------------------------------------------------------------
# Config: min notional (in USD) per venue / pair
#
# NOTE: These are *our* internal guardrails in USD space, not exact exchange
# limits. We deliberately err a bit on the safe side to avoid pointless
# orders that will be rejected.
# ---------------------------------------------------------------------------

MIN_NOTIONAL_USD: Dict[str, Dict[Any, float]] = {
    # BinanceUS generally requires around 10 USDT min notional on many pairs.
    "BINANCEUS": {
        "default": 10.0,
        ("BTC", "USDT"): 10.0,
        ("BTC", "USD"): 10.0,
    },
    # Coinbase is more permissive but we keep a floor to avoid silly dust buys.
    "COINBASE": {
        "default": 5.0,
        ("BTC", "USD"): 10.0,
        ("BTC", "USDC"): 10.0,
    },
    # Kraken also has pair-specific minimums; we use a simple USD floor here.
    "KRAKEN": {
        "default": 10.0,
        ("BTC", "USD"): 10.0,
        ("OCEAN", "USD"): 10.0,
    },
}

# ---------------------------------------------------------------------------
# Config: pair remaps / overrides
#
# Example: We previously saw Kraken rejecting OCEAN/USDT with
#   "EQuery:Unknown asset pair"
# Kraken lists OCEAN/USD but not OCEAN/USDT, so we remap.
# ---------------------------------------------------------------------------

PAIR_OVERRIDES: Dict[Tuple[str, str, str], Dict[str, Any]] = {
    # (venue, token, quote) -> changes
    ("KRAKEN", "OCEAN", "USDT"): {
        "quote": "USD",           # trade OCEAN/USD instead of OCEAN/USDT
        "symbol_hint": "OCEANUSD"  # optional hint for executors, if they use it
    },
}


def _get_min_notional_usd(venue: str, token: str, quote: str) -> float:
    """
    Look up min notional for (venue, token, quote) with sane fallbacks.
    Returns 0.0 if no rule applies (i.e., no guard from this layer).
    """
    v_cfg = MIN_NOTIONAL_USD.get(venue)
    if not v_cfg:
        return 0.0

    # Most specific: exact (token, quote) tuple
    pair_key = (token, quote)
    if pair_key in v_cfg:
        return float(v_cfg[pair_key])

    # Venue default
    if "default" in v_cfg:
        return float(v_cfg["default"])

    return 0.0


def _apply_pair_overrides(intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply (venue, token, quote) overrides such as:
      KRAKEN OCEAN/USDT -> OCEAN/USD
    Returns a *new* dict; never mutates the input.
    """
    token = (intent.get("token") or "").upper()
    venue = (intent.get("venue") or "").upper()
    quote = (intent.get("quote") or "").upper()

    key = (venue, token, quote)
    overrides = PAIR_OVERRIDES.get(key)
    if not overrides:
        return dict(intent)

    patched = dict(intent)
    if "quote" in overrides:
        patched["quote"] = str(overrides["quote"]).upper()
    if "symbol_hint" in overrides:
        patched["symbol_hint"] = overrides["symbol_hint"]

    info(
        f"exchange_rules: applied override for {venue} {token}/{quote} -> "
        f"{patched.get('quote')} (symbol_hint={patched.get('symbol_hint')})"
    )
    return patched


def validate_exchange_rules(intent: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Main Pin-5 pre-flight validator.

    Returns:
      ok:     bool  — False means "do NOT send this to exchange"
      reason: str   — short machine-readable reason
      patched_intent: dict — possibly modified copy of `intent`
    """
    token = (intent.get("token") or "").upper()
    venue = (intent.get("venue") or "").upper()
    quote = (intent.get("quote") or "").upper()
    amt = intent.get("amount_usd")

    patched = dict(intent)
    patched["token"] = token
    patched["venue"] = venue
    if quote:
        patched["quote"] = quote

    # Basic sanity: without these fields, higher layers will reject anyway,
    # but we keep this defensive.
    try:
        amt_f = float(amt)
    except Exception:
        return False, "exchange_rules_invalid_amount_usd", patched

    if amt_f <= 0:
        return False, "exchange_rules_non_positive_amount", patched

    # First apply pair overrides (e.g., Kraken OCEAN/USDT -> OCEAN/USD).
    patched = _apply_pair_overrides(patched)
    token = patched["token"]
    venue = patched["venue"]
    quote = (patched.get("quote") or quote).upper()
    patched["quote"] = quote

    # Min notional guard.
    min_notional = _get_min_notional_usd(venue, token, quote)
    if min_notional > 0 and amt_f < min_notional:
        reason = (
            f"min_notional_not_met: requested ${amt_f:.2f} < "
            f"${min_notional:.2f} ({venue} {token}/{quote})"
        )
        return False, reason, patched

    # All checks passed from this layer's point of view.
    return True, "exchange_rules_ok", patched
