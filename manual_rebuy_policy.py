#!/usr/bin/env python3
"""
manual_rebuy_policy.py

Wrapper around PolicyEngine for MANUAL_REBUY intents.

Responsibilities:
  - Normalize the incoming intent (token/venue/amount_usd/price_usd).
  - Apply C-Series venue budget clamp (Unified_Snapshot-based) BEFORE policy.
  - Apply Exchange Rule Validator (min notional, known pairs, remaps).
  - Call PolicyEngine.validate(...) for sizing & risk checks.
  - Emit a short Telegram summary.
  - Return a dict: {"ok": bool, "reason": str, "patched_intent": dict}

NOTE
----
This module no longer writes directly to Policy_Log.

Structured logging is handled centrally via policy_logger.log_decision(...)
which is invoked by nova_trigger after this function returns. This avoids
duplicate / misaligned rows and keeps Policy_Log schema consistent.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from policy_engine import PolicyEngine
from venue_budget import get_budget_for_intent
from utils import send_telegram_message_dedup, warn

# ---------------------------------------------------------------------------
# Optional Exchange Rule Validator import (Pin 5)
# If the module is missing or broken, we degrade gracefully and treat it as
# a no-op so boot is never blocked.
# ---------------------------------------------------------------------------

try:  # pragma: no cover - defensive import
    from exchange_rules import validate_exchange_rules as _validate_exchange_rules
except Exception:  # pragma: no cover
    def _validate_exchange_rules(intent: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        """Fallback no-op exchange rule validator."""
        return True, "exchange_rules_disabled", dict(intent)


def _format_telegram_summary(
    intent: Dict[str, Any],
    ok: bool,
    reason: str,
    patched: Dict[str, Any],
) -> str:
    token = str(intent.get("token") or "").upper()
    venue = str(intent.get("venue") or "").upper()
    quote = str(intent.get("quote") or "").upper()

    orig_amt_usd = intent.get("amount_usd")
    patched_amt_usd = patched.get("amount_usd", orig_amt_usd)
    price_usd = intent.get("price_usd")

    lines: list[str] = []
    lines.append("🧭 Manual Rebuy Policy Check")
    lines.append(f"Asset: {token} on {venue}{f' / {quote}' if quote else ''}")

    if orig_amt_usd is not None:
        try:
            orig_f = float(orig_amt_usd)
        except Exception:
            orig_f = None
        try:
            patched_f = float(patched_amt_usd) if patched_amt_usd is not None else None
        except Exception:
            patched_f = None

        if orig_f is not None and patched_f is not None and patched_f != orig_f:
            lines.append(f"Requested: ${orig_f:,.2f} → Allowed: ${patched_f:,.2f}")
        elif orig_f is not None:
            lines.append(f"Requested: ${orig_f:,.2f}")

    if price_usd is not None:
        try:
            lines.append(f"Price: ${float(price_usd):,.4f} (from Unified_Snapshot)")
        except Exception:
            pass

    lines.append(f"Decision: {'✅ APPROVED' if ok else '⛔ DENIED'}")
    if reason:
        lines.append(f"Reason: {reason}")

    return "\n".join(lines)


def _deny_early(
    intent: Dict[str, Any],
    reason: str,
    patched: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Shared early-deny helper: sends Telegram and returns policy result.

    Used by venue-budget layer and exchange-rule layer.

    NOTE: Policy_Log logging is handled upstream in nova_trigger via
    policy_logger.log_decision(...). We intentionally do NOT write to Sheets
    from here to avoid duplicate/misaligned rows.
    """
    patched_intent = patched or dict(intent)
    ok = False

    try:
        msg = _format_telegram_summary(intent, ok, reason, patched_intent)
        token = str(intent.get("token") or "").upper()
        venue = str(intent.get("venue") or "").upper()
        key = f"manual_rebuy_policy:{token}:{venue}"
        send_telegram_message_dedup(msg, key)
    except Exception as e:  # pragma: no cover
        warn(f"manual_rebuy_policy: failed to send Telegram summary (early deny): {e}")

    return {"ok": ok, "reason": reason, "patched_intent": patched_intent}


def evaluate_manual_rebuy(
    intent: Dict[str, Any],
    asset_state: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Main entrypoint used by nova_trigger.

    intent (expected minimal fields):
      token: str
      venue: str
      amount_usd: float
      price_usd: float (set by B-2 where possible)
      quote: str (optional)
      source: 'manual_rebuy' (optional)

    asset_state (optional):
      Optional per-asset state dict passed by nova_trigger. We pass it
      through to PolicyEngine.validate(...) when provided.

    Returns:
      {"ok": bool, "reason": str, "patched_intent": dict}
    """
    # -----------------------------------------------------------------------
    # Normalize core fields
    # -----------------------------------------------------------------------
    token = (intent.get("token") or "").upper()
    venue = (intent.get("venue") or "").upper()
    quote = (intent.get("quote") or "").upper()
    amount_usd = intent.get("amount_usd")
    price_usd = intent.get("price_usd")

    if not token:
        return {"ok": False, "reason": "missing token", "patched_intent": intent}
    if not venue:
        return {"ok": False, "reason": "missing venue", "patched_intent": intent}
    if amount_usd is None:
        return {"ok": False, "reason": "missing amount_usd", "patched_intent": intent}

    try:
        amount_usd_f = float(amount_usd)
    except Exception:
        return {"ok": False, "reason": "invalid amount_usd", "patched_intent": intent}

    if amount_usd_f <= 0:
        return {"ok": False, "reason": "non_positive amount_usd", "patched_intent": intent}

    patched_intent: Dict[str, Any] = dict(intent)
    patched_intent["token"] = token
    patched_intent["venue"] = venue
    if quote:
        patched_intent["quote"] = quote
    patched_intent["amount_usd"] = amount_usd_f
    if price_usd is not None:
        try:
            patched_intent["price_usd"] = float(price_usd)
        except Exception:
            # leave price_usd as-is if it can't be parsed
            pass

    # -----------------------------------------------------------------------
    # C-Series: venue budget clamp (Unified_Snapshot-based)
    # -----------------------------------------------------------------------
    try:
        budget_usd, budget_reason = get_budget_for_intent(patched_intent)
    except Exception as e:  # hard failure should not abort policy
        budget_usd, budget_reason = None, f"budget_error:{type(e).__name__}"

    if budget_usd is not None:
        if budget_usd <= 0:
            # No usable quote after reserve/keepback → DENY
            reason = f"venue_budget_zero ({budget_reason})"
            patched_zero = dict(patched_intent)
            patched_zero["amount_usd"] = 0.0
            return _deny_early(patched_intent, reason, patched_zero)

        # If user requested more than venue budget, clamp down before rules/policy
        if amount_usd_f > budget_usd:
            patched_intent["amount_usd"] = budget_usd

    # -----------------------------------------------------------------------
    # Pin 5: Exchange Rule Validator (min notional, known pairs, remaps)
    # -----------------------------------------------------------------------
    try:
        rules_ok, rules_reason, rules_patched = _validate_exchange_rules(patched_intent)
    except Exception as e:  # pragma: no cover
        warn(f"manual_rebuy_policy: exchange_rules error: {e}")
        rules_ok, rules_reason, rules_patched = True, f"exchange_rules_error:{type(e).__name__}", patched_intent

    if not rules_ok:
        # Hard deny at exchange-rule layer (e.g., below min notional or unknown pair)
        return _deny_early(patched_intent, rules_reason, rules_patched or patched_intent)

    # Carry forward any remaps (e.g., Kraken OCEAN/USDT -> OCEAN/USD)
    if isinstance(rules_patched, dict):
        patched_intent = dict(rules_patched)

    # -----------------------------------------------------------------------
    # B-3: use PolicyEngine wrapper for final sizing & risk checks
    # -----------------------------------------------------------------------
    pe = PolicyEngine()
    ok, reason, patched = pe.validate(patched_intent, asset_state=asset_state)

    # Telegram side-effects (central Policy_Log happens upstream)
    try:
        msg = _format_telegram_summary(patched_intent, ok, reason, patched)
        key = f"manual_rebuy_policy:{token}:{venue}"
        send_telegram_message_dedup(msg, key)
    except Exception as e:  # pragma: no cover
        warn(f"manual_rebuy_policy: failed to send Telegram summary: {e}")

    return {"ok": ok, "reason": reason, "patched_intent": patched}
