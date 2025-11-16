"""
nova_trigger.py

Manual command router for NovaTrade (Bus side).

Current scope:
  - Parse MANUAL_REBUY commands (via Telegram / NovaTrigger sheet).
  - Normalize into a generic trade intent.
  - Run it through trade_guard.guard_trade_intent(...) which applies:
        • C-Series venue budgets (Unified_Snapshot-based)
        • PolicyEngine (caps, keepback, canary, min-notional, etc.)
  - Optionally enqueue to the Outbox when REBUY_MODE=live (best-effort, fail-safe).
  - Emit a concise Telegram summary for human visibility.

This module is designed to be imported by:
  - nova_trigger_watcher.py (which calls route_manual(msg)),
  - Or used directly from a shell: from nova_trigger import route_manual
"""

from __future__ import annotations

import os
import re
import time
from typing import Any, Dict, Optional

from utils import (
    send_telegram_message_dedup,
    warn,
    info,
)
from trade_guard import guard_trade_intent  # central policy gate


# ---------------------------------------------------------------------------
# Env + constants
# ---------------------------------------------------------------------------

REBUY_MODE = os.getenv("REBUY_MODE", "dryrun").strip().lower()
DEFAULT_AGENT_TARGET = os.getenv("DEFAULT_AGENT_TARGET", "").strip()
MANUAL_AGENT_ID = os.getenv("MANUAL_AGENT_ID", "").strip()
POLICY_ID = os.getenv("POLICY_ID", "main").strip()

# If MANUAL_AGENT_ID not provided, fall back to the first AGENT_ID / DEFAULT_AGENT_TARGET
if not MANUAL_AGENT_ID:
    if DEFAULT_AGENT_TARGET:
        MANUAL_AGENT_ID = DEFAULT_AGENT_TARGET.split(",")[0].strip()
    else:
        MANUAL_AGENT_ID = os.getenv("AGENT_ID", "edge-primary").split(",")[0].strip()

if not MANUAL_AGENT_ID:
    MANUAL_AGENT_ID = "edge-primary"

TELEGRAM_DEDUP_TTL = 60  # seconds for de-duped messages


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

# MANUAL_REBUY BTC 25 VENUE=BINANCEUS QUOTE=USDT
_MANUAL_REBUY_RE = re.compile(
    r"^\s*MANUAL_REBUY\s+([A-Za-z0-9_\-]+)\s+([0-9]*\.?[0-9]+)\s*(.*)$",
    re.IGNORECASE,
)


def _parse_kv_rest(rest: str) -> Dict[str, str]:
    """
    Parse key=value tokens from the tail of the command.

    Example:
      'VENUE=BINANCEUS QUOTE=USDT foo=bar'
      -> {"VENUE": "BINANCEUS", "QUOTE": "USDT", "FOO": "bar"}
    """
    params: Dict[str, str] = {}
    for key, value in re.findall(r"([A-Za-z_]+)=([A-Za-z0-9_\-]+)", rest or ""):
        params[key.upper()] = value
    return params


def parse_manual(msg: str) -> Dict[str, Any]:
    """
    Parse a manual NovaTrigger command string.

    Currently supported:
      MANUAL_REBUY <TOKEN> <AMOUNT_USD> [VENUE=...] [QUOTE=...]

    Returns a dict with at minimum:
      { "type": "MANUAL_REBUY" | "UNKNOWN" | "ERROR", "raw": msg, ... }
    """
    msg = (msg or "").strip()

    m = _MANUAL_REBUY_RE.match(msg)
    if not m:
        # For now, everything non-MANUAL_REBUY is just "UNKNOWN"
        return {
            "type": "UNKNOWN",
            "raw": msg,
        }

    token_raw = m.group(1)
    amt_raw = m.group(2)
    rest = m.group(3) or ""

    try:
        amount_usd = float(amt_raw)
    except Exception:
        return {
            "type": "ERROR",
            "raw": msg,
            "error": f"invalid amount_usd: {amt_raw!r}",
        }

    params = _parse_kv_rest(rest)
    venue = params.get("VENUE")
    quote = params.get("QUOTE")

    parsed: Dict[str, Any] = {
        "type": "MANUAL_REBUY",
        "raw": msg,
        "token": (token_raw or "").upper(),
        "amount_usd": amount_usd,
        "venue": venue.upper() if isinstance(venue, str) else None,
        "quote": quote.upper() if isinstance(quote, str) else None,
        "params": params,
    }
    return parsed


# ---------------------------------------------------------------------------
# Telegram summary
# ---------------------------------------------------------------------------

def _send_orion_summary(
    raw: str,
    parsed: Dict[str, Any],
    status: str,
    ok: bool,
    reason: str,
    orig_amt: Optional[float],
    patched_amt: Optional[float],
    enq_ok: bool,
    enq_mode: str,
    enq_reason: Optional[str] = None,
) -> None:
    """
    Human-facing Telegram line for the Council.
    """
    token = (parsed.get("token") or "").upper()
    venue = (parsed.get("venue") or "") or "?"
    quote = (parsed.get("quote") or "") or ""
    if quote:
        venue_str = f"{venue}/{quote}"
    else:
        venue_str = venue

    lines = []
    lines.append("🔔 Orion voice triggered:")
    lines.append(f"Command: `{raw}`")
    lines.append(f"Asset: {token} @ {venue_str}")

    if orig_amt is not None:
        if patched_amt is not None and abs(patched_amt - orig_amt) > 1e-9:
            lines.append(f"Sizing: {orig_amt:,.2f} → {patched_amt:,.2f} USD")
        else:
            lines.append(f"Sizing: {orig_amt:,.2f} USD")

    lines.append(f"Policy: {status} ({reason or 'no reason provided'})")

    if enq_mode == "live":
        lines.append(f"Enqueued: {'True' if enq_ok else 'False'} mode=live")
        if enq_reason:
            lines.append(f"Enqueue note: {enq_reason}")
    else:
        lines.append(f"Enqueued: False mode={enq_mode or 'dryrun'}")

    text = "\n".join(lines)
    try:
        send_telegram_message_dedup(text, dedup_ttl_sec=TELEGRAM_DEDUP_TTL)
    except Exception as e:  # pragma: no cover
        warn(f"nova_trigger: failed to send Orion summary: {e}")


# ---------------------------------------------------------------------------
# Manual rebuy handling (via trade_guard)
# ---------------------------------------------------------------------------

def _handle_manual_rebuy(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a generic trade intent, pass through trade_guard, and optionally enqueue.
    """
    token = (parsed.get("token") or "").upper()
    venue = (parsed.get("venue") or "")
    quote = (parsed.get("quote") or "")
    amount_usd = parsed.get("amount_usd")

    try:
        amount_usd_f = float(amount_usd)
    except Exception:
        return {
            "ok": False,
            "status": "DENIED",
            "reason": f"invalid amount_usd: {amount_usd!r}",
            "parsed": parsed,
        }

    if not token:
        return {"ok": False, "status": "DENIED", "reason": "missing token", "parsed": parsed}
    if not venue:
        return {"ok": False, "status": "DENIED", "reason": "missing venue", "parsed": parsed}

    now = int(time.time())
    intent_id = f"manual_rebuy:{token}:{now}"

    # Base intent we give to the guard
    base_intent: Dict[str, Any] = {
        "token": token,
        "venue": venue.upper(),
        "quote": quote.upper() if quote else None,
        "amount_usd": amount_usd_f,
        "action": "BUY",
        # metadata for downstream logging/policy
        "intent_id": intent_id,
        "agent_target": DEFAULT_AGENT_TARGET or MANUAL_AGENT_ID,
        "source": "nova_trigger.manual_rebuy",
        "policy_id": POLICY_ID,
    }

    # trade_guard applies C-Series + PolicyEngine
    decision = guard_trade_intent(base_intent)

    ok = bool(decision.get("ok"))
    status = decision.get("status") or ("APPROVED" if ok else "DENIED")
    reason = decision.get("reason") or ""
    patched = decision.get("patched") or {}
    if not isinstance(patched, dict):
        patched = {}

    orig_amt = amount_usd_f
    patched_amt = None
    if isinstance(patched.get("amount_usd"), (int, float)):
        patched_amt = float(patched["amount_usd"])

    # -----------------------------------------------------------------------
    # Optional enqueue (best-effort, fail-safe) when REBUY_MODE=live
    # -----------------------------------------------------------------------
    enq_ok = False
    enq_reason: Optional[str] = None
    enqueue_result: Dict[str, Any] = {"ok": False}
    enq_mode = REBUY_MODE

    if ok and REBUY_MODE == "live":
        try:
            from ops_sign_and_enqueue import attempt as outbox_attempt  # type: ignore

            payload: Dict[str, Any] = {
                "agent_id": MANUAL_AGENT_ID,
                "intent": {
                    "type": "manual_rebuy",
                    "token": token,
                    "venue": patched.get("venue") or base_intent["venue"],
                    "quote": patched.get("quote") or base_intent.get("quote"),
                    "amount_usd": patched_amt if patched_amt is not None else orig_amt,
                    "intent_id": intent_id,
                },
            }

            enqueue_result = outbox_attempt(payload) or {}
            enq_ok = bool(enqueue_result.get("ok"))
            enq_reason = enqueue_result.get("reason") or None
        except Exception as e:  # pragma: no cover
            warn(f"nova_trigger: enqueue attempt failed: {e}")
            enq_ok = False
            enq_reason = f"enqueue_error:{type(e).__name__}"

    # -----------------------------------------------------------------------
    # Telegram summary
    # -----------------------------------------------------------------------
    _send_orion_summary(
        raw=parsed.get("raw") or "",
        parsed=parsed,
        status=status,
        ok=ok,
        reason=reason,
        orig_amt=orig_amt,
        patched_amt=patched_amt,
        enq_ok=enq_ok,
        enq_mode=enq_mode,
        enq_reason=enq_reason,
    )

    return {
        "ok": ok,
        "status": status,
        "reason": reason,
        "intent": base_intent,
        "patched_intent": patched,
        "enqueue": {
            "mode": enq_mode,
            "ok": enq_ok,
            "result": enqueue_result,
            "reason": enq_reason,
        },
    }


# ---------------------------------------------------------------------------
# Public entrypoint used by watcher / shell
# ---------------------------------------------------------------------------

def route_manual(msg: str) -> Dict[str, Any]:
    """
    Entry point for nova_trigger_watcher and shell tests.

    Example:
      from nova_trigger import route_manual
      route_manual("MANUAL_REBUY BTC 25 VENUE=BINANCEUS")
    """
    parsed = parse_manual(msg)

    if parsed.get("type") == "ERROR":
        _send_orion_summary(
            raw=parsed.get("raw") or msg,
            parsed=parsed,
            status="DENIED",
            ok=False,
            reason=str(parsed.get("error") or "parse error"),
            orig_amt=None,
            patched_amt=None,
            enq_ok=False,
            enq_mode=REBUY_MODE,
            enq_reason="parse_error",
        )
        return {"ok": False, "error": parsed.get("error"), "parsed": parsed}

    if parsed.get("type") == "UNKNOWN":
        warn(f"nova_trigger: unknown manual command: {msg!r}")
        return {"ok": False, "error": "unknown_command", "parsed": parsed}

    if parsed.get("type") == "MANUAL_REBUY":
        return _handle_manual_rebuy(parsed)

    warn(f"nova_trigger: unsupported parsed type: {parsed.get('type')}")
    return {"ok": False, "error": "unsupported_type", "parsed": parsed}


# ---------------------------------------------------------------------------
# CLI shim for quick manual testing in Render shell
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        msg = " ".join(sys.argv[1:])
    else:
        msg = os.getenv("NOVA_TRIGGER_MSG", "").strip()

    if not msg:
        print("Usage: python nova_trigger.py 'MANUAL_REBUY BTC 25 VENUE=BINANCEUS'")
        raise SystemExit(1)

    info(f"nova_trigger CLI invoked with: {msg!r}")
    res = route_manual(msg)
    print(res)
