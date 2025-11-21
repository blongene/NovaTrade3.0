# nova_trigger.py — Manual Command Router & Price Feed
# B-2 PATCH: Auto-injects price_usd from Unified_Snapshot to resolve "price unknown" denials.
# COMPATIBILITY: Updated to use the new hardened utils.py.

import os, json, time, re
from typing import Any, Dict, Optional, Tuple, List

# Import from the new hardened utils
from utils import (
    get_ws, 
    get_sheet, 
    warn, 
    info, 
    send_telegram_message_dedup, 
    hmac_enqueue  # Ensure utils.py has hmac_enqueue or we use ops_sign_and_enqueue shim
)
# If utils.py doesn't export hmac_enqueue, we use a local shim or import it
try:
    from utils import hmac_enqueue
except ImportError:
    # Fallback if utils.py hasn't been fully updated with the enqueue helper
    from ops_sign_and_enqueue import attempt as outbox_attempt
    def hmac_enqueue(payload):
        return outbox_attempt({"agent_id": "cloud", "intent": payload, "type": "order.place"})

# We might need the policy engine
try:
    from manual_rebuy_policy import evaluate_manual_rebuy
except ImportError:
    # Fallback stub if file missing
    def evaluate_manual_rebuy(intent, asset_state):
        return {"ok": True, "reason": "policy_missing_allow_all", "patched_intent": intent}

# === Config ===
UNIFIED_SNAPSHOT_WS = os.getenv("UNIFIED_SNAPSHOT_WS", "Unified_Snapshot")
PRICE_CACHE_TTL_SEC = int(os.getenv("PRICE_CACHE_TTL_SEC", "180"))
TELEGRAM_DEDUP_TTL  = int(os.getenv("TELEGRAM_DEDUP_TTL_SEC", "120"))

# Global Cache
_price_cache = { "ts": 0.0, "rows": [] }

# ---------------------------------------------------------------------------
# B-2: Unified_Snapshot price lookup
# ---------------------------------------------------------------------------
def _load_price_snapshot(force: bool = False) -> List[Dict[str, Any]]:
    """Load & cache the Unified_Snapshot sheet for a short TTL."""
    global _price_cache
    now = time.time()
    
    if not force and _price_cache.get("rows") and (now - float(_price_cache.get("ts", 0.0)) < PRICE_CACHE_TTL_SEC):
        return _price_cache["rows"]

    try:
        # Use new utils.get_ws which handles backoff/auth
        ws = get_ws(UNIFIED_SNAPSHOT_WS)
        rows = ws.get_all_records()
        _price_cache["ts"] = now
        _price_cache["rows"] = rows or []
        return _price_cache["rows"]
    except Exception as e:
        warn(f"nova_trigger: Failed to load {UNIFIED_SNAPSHOT_WS}: {e}")
        return []

def _get_price_usd_from_snapshot(token: str) -> Tuple[Optional[float], str]:
    """Look up a USD price for `token`."""
    token_up = (token or "").upper()
    if not token_up: return None, "no_token"
    
    rows = _load_price_snapshot()
    if not rows: return None, "no_snapshot_rows"
    
    # Fuzzy match columns
    price_cols = ["Price_USD", "Price", "Current Price", "USD Price", "value"]
    sym_cols   = ["Token", "Symbol", "Asset"]

    for r in rows:
        # Try to find the symbol
        found_sym = False
        for c in sym_cols:
            if str(r.get(c, "")).upper() == token_up:
                found_sym = True
                break
        
        if found_sym:
            # Try to find the price
            for pc in price_cols:
                val = r.get(pc)
                if val is not None and str(val).strip() != "":
                    try:
                        p = float(str(val).replace(",", "").replace("$", "").strip())
                        if p > 0: return p, "ok"
                    except:
                        continue
    return None, "not_found"

# -----------------------------------------------------------------------
# Routing Logic
# -----------------------------------------------------------------------
def route_manual(raw: str) -> dict:
    """
    Handles manual commands (e.g. 'MANUAL_REBUY BTC 500').
    1. Parses command
    2. Fetches Price (B-2)
    3. Checks Policy
    4. Enqueues to Bus
    """
    parsed = parse_manual(raw)
    if not parsed["ok"]:
        return {"ok": False, "reason": parsed["reason"]}

    intent = {
        "source": "manual_rebuy",
        "token": parsed["token"],
        "action": "BUY",
        "amount_usd": parsed["amount_usd"],
        "venue": parsed["venue"],
        "quote": parsed["quote"],
        "ts": time.time(),
        "raw_msg": raw,
    }
    
    # B-2: Auto Price Fetch
    price_usd, p_reason = _get_price_usd_from_snapshot(intent["token"])
    if price_usd:
        intent["price_usd"] = price_usd
    else:
        warn(f"nova_trigger: Could not find price for {intent['token']} ({p_reason})")

    # Policy Check
    decision = evaluate_manual_rebuy(intent, asset_state={})
    
    policy_ok = bool(decision.get("ok"))
    reason = decision.get("reason")
    patched = decision.get("patched_intent", {})
    
    enq_ok = False
    enq_reason = None
    mode = os.getenv("REBUY_MODE", "dryrun").lower()
    
    if policy_ok and mode == "live":
        try:
            # Construct payload for /ops/enqueue
            payload = {
                "venue": patched.get("venue") or intent["venue"],
                "symbol": patched.get("symbol") or f"{intent['token']}/{intent['quote']}",
                "side": patched.get("side") or "BUY",
                "amount": patched.get("amount") or 0.0,  # Base units
                "amount_quote": patched.get("amount_usd"), # Quote units (helper)
                "source": "manual_rebuy",
                "ts": int(time.time())
            }
            
            # Pass price if we have it
            if intent.get("price_usd"):
                payload["price_usd"] = intent["price_usd"]

            res = hmac_enqueue(payload)
            enq_ok = bool(res.get("ok"))
            enq_reason = res.get("reason")
        except Exception as e:
            warn(f"nova_trigger: Enqueue failed: {e}")
            enq_reason = str(e)

    # Telegram Summary
    _send_summary(raw, intent, decision, enq_ok, enq_reason, mode)
    
    return {
        "ok": policy_ok,
        "decision": decision,
        "enqueue": {"ok": enq_ok, "reason": enq_reason}
    }

def parse_manual(raw: str) -> dict:
    """Simple parser: MANUAL_REBUY [TOKEN] [USD_AMOUNT] (VENUE=...)"""
    # Example: MANUAL_REBUY BTC 500 VENUE=BINANCEUS
    parts = raw.strip().split()
    if len(parts) < 3:
        return {"ok": False, "reason": "formatting_error"}
    
    token = parts[1].upper()
    try:
        amt = float(parts[2])
    except:
        return {"ok": False, "reason": "invalid_amount"}
    
    venue = None
    quote = "USDT" # default
    
    for p in parts[3:]:
        if p.startswith("VENUE="):
            venue = p.split("=")[1].upper()
        if p.startswith("QUOTE="):
            quote = p.split("=")[1].upper()
            
    return {
        "ok": True, 
        "token": token, 
        "amount_usd": amt, 
        "venue": venue, 
        "quote": quote
    }

def _send_summary(raw, intent, decision, enq_ok, enq_reason, mode):
    icon = "✅" if decision.get("ok") else "❌"
    lines = [
        f"{icon} <b>Manual Rebuy</b>",
        f"Cmd: <code>{raw}</code>",
        f"Policy: {decision.get('reason')}",
    ]
    if intent.get("price_usd"):
        lines.append(f"Price: ${intent['price_usd']:,.2f}")
        
    if mode == "live":
        e_icon = "🚀" if enq_ok else "⚠️"
        lines.append(f"Enqueue: {e_icon} {enq_reason or 'OK'}")
    else:
        lines.append(f"Mode: DRYRUN (Not Enqueued)")
        
    text = "\n".join(lines)
    send_telegram_message_dedup(text, key=f"manual:{int(time.time())}", ttl_min=1)
