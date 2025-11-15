# nova_trigger.py — parse + route manual commands, Telegram ping / Policy Spine

import os
import json
import time
import hmac
import hashlib
import re
from typing import Optional, Dict, Any

import requests
from policy_engine import PolicyEngine

# ---------------------------------------------------------------------------
# Config / env
# ---------------------------------------------------------------------------

BASE_URL = (
    os.getenv("OPS_BASE_URL")          # optional override just for enqueue calls
    or os.getenv("CLOUD_BASE_URL")
    or "https://novatrade3-0.onrender.com"   # fallback to your service URL
).rstrip("/")

REBUY_MODE = os.getenv("REBUY_MODE", "dryrun").lower()
OUTBOX_SECRET = os.getenv("OUTBOX_SECRET", "")

# Policy Spine metadata
DEFAULT_AGENT_TARGET = os.getenv("DEFAULT_AGENT_TARGET", "edge-primary,edge-nl1")
POLICY_ID = os.getenv("POLICY_ID", "main")

# ---------------------------------------------------------------------------
# Telegram helper (reuses your existing infra)
# ---------------------------------------------------------------------------


def send_telegram(text: str) -> None:
    bot = os.getenv("BOT_TOKEN")
    chat = os.getenv("TELEGRAM_CHAT_ID")
    if not (bot and chat):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{bot}/sendMessage",
            json={"chat_id": chat, "text": text},
            timeout=10,
        )
    except Exception:
        # We deliberately swallow errors here; this is best-effort only.
        pass


# ---------------------------------------------------------------------------
# Outbox enqueue helper
# ---------------------------------------------------------------------------


def _hmac(sig_payload: Dict[str, Any]) -> str:
    """
    Legacy helper: compute HMAC over a JSON payload.
    Kept for backwards-compat, although _enqueue does not currently use it.
    """
    raw = json.dumps(
        sig_payload, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hmac.new(OUTBOX_SECRET.encode("utf-8"), raw, hashlib.sha256).hexdigest()


def _enqueue(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sends raw JSON body to OPS_ENQUEUE_URL (or BASE_URL + /api/ops/enqueue)
    with X-Outbox-Signature: sha256=<hex(hmac(body))> when OUTBOX_SECRET is set.
    """
    url = os.getenv("OPS_ENQUEUE_URL") or (BASE_URL + "/api/ops/enqueue")

    raw = json.dumps(
        payload, separators=(",", ":"), sort_keys=False
    ).encode("utf-8")
    headers = {"Content-Type": "application/json"}

    if OUTBOX_SECRET:
        mac = hmac.new(
            OUTBOX_SECRET.encode("utf-8"), raw, hashlib.sha256
        ).hexdigest()
        headers["X-Outbox-Signature"] = f"sha256={mac}"

    try:
        r = requests.post(url, data=raw, headers=headers, timeout=20)
        return {
            "ok": r.ok,
            "status": r.status_code,
            "text": r.text[:200],
            "url": url,
        }
    except Exception as e:
        return {"ok": False, "status": 0, "text": str(e), "url": url}


# ---------------------------------------------------------------------------
# Manual rebuy parsing + routing
# ---------------------------------------------------------------------------


def parse_manual(msg: str) -> Optional[Dict[str, Any]]:
    """
    Parse a MANUAL_REBUY command from NovaTrigger.

    Examples:
      MANUAL_REBUY BTC 5 VENUE=BINANCEUS
      MANUAL_REBUY ETH 10 VENUE=COINBASE QUOTE=USD
    """
    m = re.match(
        r"^\s*MANUAL_REBUY\s+([A-Za-z0-9]+)\s+(\d+(?:\.\d+)?)\s*(.*)$", msg or ""
    )
    if not m:
        return None

    token = m.group(1).upper()
    amt = float(m.group(2))
    rest = m.group(3) or ""

    # Parse key=value pairs (VENUE=..., QUOTE=..., etc.)
    kv = {
        k.upper(): v.upper()
        for k, v in re.findall(
            r"([A-Za-z_]+)\s*=\s*([A-Za-z0-9\-]+)", rest
        )
    }

    venue = kv.get("VENUE", "BINANCEUS")
    quote = kv.get("QUOTE", "")

    return {
        "source": "manual_rebuy",
        "token": token,
        "action": "BUY",
        "amount_usd": amt,
        "venue": venue,
        "quote": quote,
        "ts": int(time.time()),
    }


def route_manual(msg: str) -> Dict[str, Any]:
    """
    Entry-point used by NovaTrigger.

    - Parses MANUAL_REBUY commands
    - Builds a Policy Spine–aware intent
    - Evaluates via PolicyEngine
    - Optionally enqueues a trade into the Outbox (when REBUY_MODE=live)
    - Sends a brief Telegram notification
    """
    parsed = parse_manual(msg)
    if not parsed:
        return {"ok": False, "reason": "unrecognized manual format"}

    # Build a stable Intent_ID + metadata for the Policy Spine
    now = int(parsed.get("ts") or time.time())
    token = parsed["token"]
    intent_id = f"manual_rebuy:{token}:{now}"

    intent: Dict[str, Any] = {
        **parsed,
        "id": intent_id,
        "agent_target": DEFAULT_AGENT_TARGET,
        "source": "nova_trigger.manual_rebuy",
        "policy_id": POLICY_ID,
    }

    pe = PolicyEngine()
    # If you have on-sheet metrics, pass them as asset_state; for these manual
    # majors we're fine leaving this empty for now.
    asset_state: Dict[str, Any] = {}
    decision = pe.validate(intent, asset_state)

    # Enqueue only if policy says OK AND we're in live mode
    enq: Dict[str, Any] = {"ok": False}
    if decision.get("ok") and REBUY_MODE == "live":
        symbol = decision["symbol"]
        quote_amt = float(decision["amount_usd"])

        payload: Dict[str, Any] = {
            "venue": decision["venue"],
            "symbol": symbol,
            "side": "BUY",
            # ops_enqueue expects amount in the quote currency (USD/USDT/USDC)
            "amount_quote": quote_amt,
            "client_id": f"manual-{decision['token']}-{int(decision['ts'])}",
            "policy_reason": decision.get("reason", "ok"),
            "intent_id": intent_id,
        }
        enq = _enqueue(payload)
        print(
            f"[manual_enq] url={enq.get('url')} "
            f"status={enq.get('status')} ok={enq.get('ok')} "
            f"text={enq.get('text')}"
        )

    # Telegram notice (brief)
    policy_word = "OK" if decision.get("ok") else "DENY"
    reason = decision.get("reason")
    send_telegram(
        f"🔔 Orion voice triggered: {msg}\n"
        f"Policy: {policy_word} ({reason})\n"
        f"Enqueued: {enq.get('ok')} mode={REBUY_MODE}"
    )

    return {"ok": True, "intent": intent, "decision": decision, "enqueue": enq}


# ---------------------------------------------------------------------------
# Shim: trigger_nova_ping, expected by Nova ping infrastructure
# ---------------------------------------------------------------------------


def trigger_nova_ping(trigger_type: str = "NOVA UPDATE") -> Dict[str, Any]:
    presets = {
        "SOS": "🚨 *NovaTrade SOS*\nTesting alert path.",
        "PRESALE ALERT": "🚀 *Presale Alert*\nNew high-score presale detected.",
        "ROTATION COMPLETE": "🔁 *Rotation Complete*\nVault rotation executed.",
        "SYNC NEEDED": "🧩 *Sync Needed*\nPlease review latest responses.",
        "FYI ONLY": "📘 *FYI*\nNon-urgent update.",
        "NOVA UPDATE": "🧠 *Nova Update*\nSystem improvement deployed.",
    }
    text = presets.get(trigger_type.upper(), f"🔔 *{trigger_type}*")
    send_telegram(text)
    return {"ok": True, "type": trigger_type}
