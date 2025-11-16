"""
manual_rebuy_policy.py

Wrapper around PolicyEngine for MANUAL_REBUY intents.

Responsibilities:
  - Normalize the incoming intent (token/venue/amount_usd/price_usd).
  - Call PolicyEngine.validate(...) for sizing & risk checks.
  - Append a lightweight row into Policy_Log.
  - Send a short Telegram summary.
  - Return (ok, reason, patched) for upstream callers.

Compatible with the B-3 policy_engine.py wrapper.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, Tuple

from policy_engine import PolicyEngine
from utils import sheets_append_rows, send_telegram_message_dedup, warn  # type: ignore

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

    lines = []
    lines.append("🧭 Manual Rebuy Policy Check")
    lines.append(f"Asset: {token} on {venue} {f'/{quote}' if quote else ''}".rstrip())

    if orig_amt_usd is not None:
        if patched_amt_usd is not None and patched_amt_usd != orig_amt_usd:
            lines.append(f"Requested: ${orig_amt_usd:,.2f} → Allowed: ${patched_amt_usd:,.2f}")
        else:
            lines.append(f"Requested: ${orig_amt_usd:,.2f}")
    if price_usd:
        try:
            lines.append(f"Price: ${float(price_usd):,.4f} (from Unified_Snapshot)")
        except Exception:
            pass

    lines.append(f"Decision: {'✅ APPROVED' if ok else '⛔ DENIED'}")
    if reason:
        lines.append(f"Reason: {reason}")

    return "\n".join(lines)


def evaluate_manual_rebuy(intent: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Main entrypoint used by nova_trigger.

    intent (expected minimal fields):
      token: str
      venue: str
      amount_usd: float
      price_usd: float (set by B-2 where possible)
      quote: str (optional)
      source: 'manual_rebuy' (optional)
    """
    # Normalize a couple of fields for PolicyEngine wrapper
    token = (intent.get("token") or "").upper()
    venue = (intent.get("venue") or "").upper()
    quote = (intent.get("quote") or "").upper()
    amount_usd = intent.get("amount_usd")
    price_usd = intent.get("price_usd")

    if not token:
        return False, "missing token", intent
    if amount_usd is None:
        return False, "missing amount_usd", intent
    try:
        amount_usd_f = float(amount_usd)
    except Exception:
        return False, "invalid amount_usd", intent

    patched_intent = dict(intent)
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

    # Use the B-3 PolicyEngine wrapper (legacy API)
    pe = PolicyEngine()
    ok, reason, patched = pe.validate(patched_intent, asset_state=None)

    # Logging + Telegram side effects
    _append_policy_log_row(patched_intent, ok, reason, patched)
    try:
        msg = _format_telegram_summary(patched_intent, ok, reason, patched)
        send_telegram_message_dedup(msg, dedup_ttl_sec=60)
    except Exception as e:  # pragma: no cover
        warn(f"manual_rebuy_policy: failed to send Telegram summary: {e}")

    return ok, reason, patched
