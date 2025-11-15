# nova_trigger.py — parse + route manual commands, Telegram ping
from __future__ import annotations

import os, json, time, re
from typing import Dict, Any

import requests

from policy_engine import PolicyEngine  # still imported for backwards compat
from manual_rebuy_policy import evaluate_manual_rebuy
from ops_sign_and_enqueue import attempt as _ops_attempt  # reuse the canonical signer

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = (
    os.getenv("OPS_BASE_URL") or          # optional new var just for enqueue calls
    os.getenv("CLOUD_BASE_URL") or
    "https://novatrade3-0.onrender.com"   # your service URL
).rstrip("/")

REBUY_MODE        = os.getenv("REBUY_MODE", "dryrun").lower()
OUTBOX_SECRET_RAW = os.getenv("OUTBOX_SECRET", "")
OUTBOX_SECRET     = OUTBOX_SECRET_RAW.encode() if OUTBOX_SECRET_RAW else b""
MANUAL_AGENT_ID   = os.getenv("MANUAL_AGENT_ID", os.getenv("EDGE_AGENT_ID", "edge-primary"))

# ---------------------------------------------------------------------------
# Telegram helper (minimal; reuses existing BOT_TOKEN / TELEGRAM_CHAT_ID)
# ---------------------------------------------------------------------------

def send_telegram(text: str) -> None:
    bot  = os.getenv("BOT_TOKEN")
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
        # soft-fail; Nova heartbeat will still be alive
        return

# ---------------------------------------------------------------------------
# Low-level enqueue helper — wraps ops_sign_and_enqueue.attempt
# ---------------------------------------------------------------------------

def _enqueue(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Wraps the /ops/enqueue signing logic used by the CLI helper.

    payload example:
      {
        "venue": "BINANCEUS",
        "symbol": "BTC/USDT",
        "side": "BUY",
        "amount": "25",          # quote or base per executor config
        "time_in_force": "IOC",
        ...
      }
    """
    if not OUTBOX_SECRET:
        return {"ok": False, "error": "OUTBOX_SECRET missing", "url": f"{BASE_URL}/ops/enqueue"}

    body = {
        "agent_id": MANUAL_AGENT_ID,
        "type": "order.place",
        "payload": payload,
    }

    ok, label, resp = _ops_attempt(BASE_URL, OUTBOX_SECRET, body)
    j = resp or {}

    return {
        "ok": bool(ok and j.get("ok", False)),
        "label": label,
        "json": j,
        "status": j.get("status"),
        "url": f"{BASE_URL}/ops/enqueue",
        "text": json.dumps(j)[:200] if j else "",
    }

# ---------------------------------------------------------------------------
# Manual rebuy parsing + routing
# ---------------------------------------------------------------------------

def parse_manual(msg: str) -> Dict[str, Any] | None:
    """
    EXAMPLES (from NovaTrigger sheet / Orion Voice):

      MANUAL_REBUY BTC 5
      MANUAL_REBUY BTC 5 VENUE=BINANCEUS
      MANUAL_REBUY ETH 10 VENUE=COINBASE QUOTE=USD
    """
    m = re.match(r"^\s*MANUAL_REBUY\s+([A-Za-z0-9]+)\s+(\d+(?:\.\d+)?)\s*(.*)$", msg or "")
    if not m:
        return None

    token = m.group(1).upper()
    amt   = float(m.group(2))
    rest  = m.group(3) or ""

    # parse simple KEY=VALUE pairs (VENUE=..., QUOTE=...)
    kv = {
        k.upper(): v.upper()
        for k, v in re.findall(r"([A-Za-z_]+)\s*=\s*([A-Za-z0-9\-]+)", rest)
    }
    venue = kv.get("VENUE", "BINANCEUS").upper()
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
    Entry point used by nova_trigger_watcher.check_nova_trigger
    when it sees a MANUAL_REBUY command in NovaTrigger!A1.
    """
    intent = parse_manual(msg)
    if not intent:
        return {
            "ok": False,
            "reason": "unrecognized manual format",
            "decision": {},
            "enqueue": {"ok": False},
        }

    # Run through unified policy engine via our manual wrapper
    decision = evaluate_manual_rebuy(intent)
    patched  = decision.get("patched") or {}

    # enqueue only if OK + live
    enq: Dict[str, Any] = {"ok": False}
    if decision.get("ok") and REBUY_MODE == "live":
        symbol = patched.get("symbol") or f"{intent['token']}/{patched.get('quote') or intent.get('quote') or 'USDT'}"
        venue  = patched.get("venue")  or intent.get("venue") or "BINANCEUS"
        amt_usd = patched.get("amount_usd") or intent.get("amount_usd")

        # For now our manual path uses quote-notional = amt_usd; executors on edge side
        # know whether that is base or quote per venue.
        quote_amt = str(amt_usd)

        payload = {
            "venue":  venue,
            "symbol": symbol,
            "side":   "BUY",
            "amount": quote_amt,
            "time_in_force": "IOC",
            "client_id": f"manual-{intent['token']}-{int(intent['ts'])}",
            "policy_reason": decision.get("reason", "ok"),
            "source": "manual_rebuy",
        }
        enq = _enqueue(payload)

    # Telegram notice (brief, but shows policy outcome)
    status = decision.get("status", "UNKNOWN")
    reason = decision.get("reason", "")
    orig   = decision.get("original_amount_usd")
    patched_amt = (decision.get("patched") or {}).get("amount_usd", orig)

    sizing_line = "n/a"
    try:
        if orig is not None and patched_amt is not None:
            o = float(orig)
            p = float(patched_amt)
            if abs(p - o) > 0.01:
                sizing_line = f"{o:.2f} → {p:.2f} USD"
            else:
                sizing_line = f"{o:.2f} USD"
    except Exception:
        pass

    send_telegram(
        f"🔔 Orion voice triggered: {msg}\n"
        f"Policy: {status} ({reason or 'ok'})\n"
        f"Sizing: {sizing_line}\n"
        f"Enqueued: {enq.get('ok')} mode={REBUY_MODE}"
    )

    return {"ok": bool(decision.get("ok")), "intent": intent, "decision": decision, "enqueue": enq}

# ---------------------------------------------------------------------------
# shim: trigger_nova_ping, expected by Nova ping job
# ---------------------------------------------------------------------------

def trigger_nova_ping(trigger_type: str = "NOVA UPDATE") -> Dict[str, Any]:
    presets = {
        "SOS": "🚨 *NovaTrade SOS*\\nTesting alert path.",
        "PRESALE ALERT": "🚀 *Presale Alert*\\nNew high-score presale detected.",
        "ROTATION COMPLETE": "🔁 *Rotation Complete*\\nVault rotation executed.",
        "SYNC NEEDED": "🧩 *Sync Needed*\\nPlease review latest responses.",
        "FYI ONLY": "📘 *FYI*\\nNon-urgent update.",
        "NOVA UPDATE": "🧠 *Nova Update*\\nSystem improvement deployed.",
    }
    text = presets.get(trigger_type.upper(), f"🔔 *{trigger_type}*")
    send_telegram(text)
    return {"ok": True, "type": trigger_type}
