from __future__ import annotations
import os, re, time
from typing import Any, Dict, Optional

from utils import send_telegram_message_dedup, warn, info
from trade_guard import guard_trade_intent
from price_feed import get_price_usd  # NEW

REBUY_MODE = os.getenv("REBUY_MODE", "dryrun").strip().lower()
DEFAULT_AGENT_TARGET = os.getenv("DEFAULT_AGENT_TARGET", "")
MANUAL_AGENT_ID = os.getenv("MANUAL_AGENT_ID", "")
POLICY_ID = os.getenv("POLICY_ID", "main")

if not MANUAL_AGENT_ID:
    MANUAL_AGENT_ID = (DEFAULT_AGENT_TARGET or os.getenv("AGENT_ID", "edge-primary")).split(",")[0]

_MANUAL_REBUY_RE = re.compile(
    r"^\s*MANUAL_REBUY\s+([A-Za-z0-9_\-]+)\s+([0-9]*\.?[0-9]+)\s*(.*)$",
    re.IGNORECASE,
)

def _parse_kv(rest: str) -> Dict[str, str]:
    return {k.upper(): v for k, v in re.findall(r"([A-Za-z_]+)=([A-Za-z0-9_\-]+)", rest or "")}

def parse_manual(msg: str) -> Dict[str, Any]:
    msg = (msg or "").strip()
    m = _MANUAL_REBUY_RE.match(msg)
    if not m:
        return {"type": "UNKNOWN", "raw": msg}

    token_raw = m.group(1)
    amt_raw = m.group(2)
    rest = m.group(3) or ""

    try:
        amt = float(amt_raw)
    except Exception:
        return {"type": "ERROR", "raw": msg, "error": f"invalid amount_usd: {amt_raw}"}

    params = _parse_kv(rest)
    venue = params.get("VENUE")
    quote = params.get("QUOTE")

    return {
        "type": "MANUAL_REBUY",
        "raw": msg,
        "token": token_raw.upper(),
        "amount_usd": amt,
        "venue": venue.upper() if venue else None,
        "quote": quote.upper() if quote else None,
        "params": params,
    }

def _send_summary(raw, parsed, status, ok, reason, orig_amt, patched_amt, enq_ok, mode, enq_reason):
    token = parsed.get("token") or "?"
    venue = parsed.get("venue") or "?"
    quote = parsed.get("quote") or ""

    venue_str = f"{venue}/{quote}" if quote else venue

    lines = [
        "🔔 Orion voice triggered:",
        f"Command: `{raw}`",
        f"Asset: {token} @ {venue_str}",
    ]
    if orig_amt is not None:
        if patched_amt is not None and abs(patched_amt - orig_amt) > 1e-9:
            lines.append(f"Sizing: {orig_amt} → {patched_amt} USD")
        else:
            lines.append(f"Sizing: {orig_amt} USD")

    lines.append(f"Policy: {status} ({reason or 'no reason'})")

    if mode == "live":
        lines.append(f"Enqueued: {enq_ok}")
        if enq_reason:
            lines.append(f"Note: {enq_reason}")
    else:
        lines.append(f"Enqueued: False (mode={mode})")

    try:
        # utils version takes positional args only
        send_telegram_message_dedup("\n".join(lines))
    except Exception as e:
        warn(f"nova_trigger: failed summary send: {e}")

def _handle_manual_rebuy(parsed):
    token = parsed["token"]
    venue = parsed["venue"]
    amount = parsed["amount_usd"]
    quote = parsed.get("quote") or "USDT"

    now = int(time.time())
    intent_id = f"manual_rebuy:{token}:{now}"

    # B-2: get price from Unified_Snapshot (venue-specific with fallback)
    price_usd = get_price_usd(token, quote, venue)

    guard_intent = {
        "token": token,
        "venue": venue,
        "quote": quote,
        "amount_usd": float(amount),
        "price_usd": price_usd,  # NEW
        "action": "BUY",
        "intent_id": intent_id,
        "agent_target": DEFAULT_AGENT_TARGET or MANUAL_AGENT_ID,
        "source": "nova_trigger.manual_rebuy",
        "policy_id": POLICY_ID,
    }

    decision = guard_trade_intent(guard_intent)

    ok = bool(decision.get("ok"))
    status = decision.get("status") or ("APPROVED" if ok else "DENIED")
    reason = decision.get("reason") or ""
    patched = decision.get("patched") or {}

    orig_amt = float(amount)
    patched_amt = patched.get("amount_usd", orig_amt)

    # Enqueue logic (best-effort)
    enq_ok = False
    enq_reason = None

    if ok and REBUY_MODE == "live":
        try:
            from ops_sign_and_enqueue import attempt as outbox_attempt
            payload = {
                "agent_id": MANUAL_AGENT_ID,
                "intent": {
                    "type": "manual_rebuy",
                    "token": token,
                    "venue": patched.get("venue", venue),
                    "quote": patched.get("quote", quote),
                    "amount_usd": patched_amt,
                    "intent_id": intent_id,
                },
            }
            res = outbox_attempt(payload) or {}
            enq_ok = bool(res.get("ok"))
            enq_reason = res.get("reason")
        except Exception as e:
            enq_ok = False
            enq_reason = f"enqueue_error:{e}"

    _send_summary(
        raw=parsed["raw"],
        parsed=parsed,
        status=status,
        ok=ok,
        reason=reason,
        orig_amt=orig_amt,
        patched_amt=patched_amt,
        enq_ok=enq_ok,
        mode=REBUY_MODE,
        enq_reason=enq_reason,
    )

    return {
        "ok": ok,
        "status": status,
        "reason": reason,
        "decision": {       # watcher compatibility
            "status": status,
            "reason": reason,
            "ok": ok,
        },
        "intent": guard_intent,
        "patched_intent": patched,
        "enqueue": {
            "mode": REBUY_MODE,
            "ok": enq_ok,
            "reason": enq_reason,
        },
    }

def route_manual(msg: str) -> Dict[str, Any]:
    parsed = parse_manual(msg)

    if parsed["type"] == "ERROR":
        return {"ok": False, "error": parsed["error"], "decision": {"ok": False}}

    if parsed["type"] == "UNKNOWN":
        warn(f"nova_trigger: unknown manual command: {msg!r}")
        return {"ok": False, "error": "unknown_command", "decision": {"ok": False}}

    if parsed["type"] == "MANUAL_REBUY":
        return _handle_manual_rebuy(parsed)

    warn(f"nova_trigger: unsupported type: {parsed['type']}")
    return {"ok": False, "error": "unsupported_type", "decision": {"ok": False}}

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
