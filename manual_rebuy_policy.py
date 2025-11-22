"""
manual_rebuy_policy.py

Wrapper around PolicyEngine for MANUAL_REBUY intents.

Responsibilities:
  - Normalize the incoming intent (token/venue/amount_usd/price_usd).
  - Apply C-Series venue budget clamp (Unified_Snapshot-based) BEFORE policy.
  - Call PolicyEngine.validate(...) for sizing & risk checks.
  - Append a lightweight row into Policy_Log.
  - Send a short Telegram summary.
  - Return a dict: {"ok": bool, "reason": str, "patched_intent": dict}
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from policy_engine import PolicyEngine
from venue_budget import get_budget_for_intent
from utils import sheets_append_rows, send_telegram_message_dedup, warn

SHEET_URL = os.getenv("SHEET_URL", "")
POLICY_LOG_WS = os.getenv("POLICY_LOG_WS", "Policy_Log")


def _append_policy_log_row(intent: Dict[str, Any], ok: bool, reason: str, patched: Dict[str, Any]) -> None:
    """Best-effort append into Policy_Log; never raises."""
    if not SHEET_URL:
        return

    try:
        ts = datetime.now(timezone.utc).isoformat()
        src = str(intent.get("source") or "manual_rebuy")
        token = str(intent.get("token") or "").upper()
        venue = str(intent.get("venue") or "").upper()
        quote = str(intent.get("quote") or "").upper()
        amt_usd = intent.get("amount_usd")
        price_usd = intent.get("price_usd")
        patched_amt_usd = patched.get("amount_usd", amt_usd)

        row = [
            ts,
            src,
            token,
            venue,
            quote,
            amt_usd,
            price_usd,
            patched_amt_usd,
            "OK" if ok else "DENIED",
            reason,
        ]
        sheets_append_rows(SHEET_URL, POLICY_LOG_WS, [row])
    except Exception as e:  # pragma: no cover
        warn(f"manual_rebuy_policy: failed to append Policy_Log row: {e}")


def _format_telegram_summary(intent: Dict[str, Any], ok: bool, reason: str, patched: Dict[str, Any]) -> str:
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
    # Normalize fields
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
            ok = False
            patched = dict(patched_intent)
            patched["amount_usd"] = 0.0
            _append_policy_log_row(patched_intent, ok, reason, patched)
            try:
                msg = _format_telegram_summary(patched_intent, ok, reason, patched)
                key = f"manual_rebuy_policy:{token}:{venue}"
                send_telegram_message_dedup(msg, key)
            except Exception as e:  # pragma: no cover
                warn(f"manual_rebuy_policy: failed to send Telegram summary (budget_zero): {e}")
            return {"ok": ok, "reason": reason, "patched_intent": patched}

        # If user requested more than venue budget, clamp down before PolicyEngine
        if amount_usd_f > budget_usd:
            patched_intent["amount_usd"] = budget_usd

    # -----------------------------------------------------------------------
    # B-3: use PolicyEngine wrapper for final sizing & risk checks
    # -----------------------------------------------------------------------
    pe = PolicyEngine()
    ok, reason, patched = pe.validate(patched_intent, asset_state=asset_state)

    # Logging + Telegram side effects
    _append_policy_log_row(patched_intent, ok, reason, patched)
    try:
        msg = _format_telegram_summary(patched_intent, ok, reason, patched)
        key = f"manual_rebuy_policy:{token}:{venue}"
        send_telegram_message_dedup(msg, key)
    except Exception as e:  # pragma: no cover
        warn(f"manual_rebuy_policy: failed to send Telegram summary: {e}")

    return {"ok": ok, "reason": reason, "patched_intent": patched}
