# nova_trigger.py — Manual command router with B-2 Price Feed (Bus side).
# - Routes MANUAL_REBUY commands to the policy engine for validation.
# - Auto-injects price_usd from Unified_Snapshot to resolve "price unknown" denials.
# - Uses SAFE IMPORTS to prevent crashes if utils.py is mid-update.

import os, json, time, re
from typing import Any, Dict, Optional, Tuple, List

# 1. Safe Import Block
try:
    from utils import (
        get_ws, 
        get_sheet, 
        warn, 
        info, 
        send_telegram_message_dedup, 
    )
except Exception:  # Fallbacks if utils has changed mid-deploy
    def warn(msg: str): print(f"[WARN] {msg}")
    def info(msg: str): print(f"[INFO] {msg}")
    def send_telegram_message_dedup(*args, **kwargs): pass
    def get_sheet():
        from gspread import authorize
        from oauth2client.service_account import ServiceAccountCredentials
        scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            os.getenv("GOOGLE_CREDS_JSON_PATH","sentiment-log-service.json"), scope
        )
        gc = authorize(creds)
        return gc.open_by_url(os.environ["SHEET_URL"])
    def get_ws(tab: str):
        return get_sheet().worksheet(tab)

try:
    from manual_rebuy_policy import evaluate_manual_rebuy
except Exception:
    evaluate_manual_rebuy = None

try:
    from price_feed import get_price_usd as _feed_get_price_usd
except Exception:
    def _feed_get_price_usd(token: str, quote: str, venue: Optional[str]) -> Optional[float]:
        return None

try:
    from ops_sign_and_enqueue import hmac_enqueue
except Exception:
    def hmac_enqueue(payload: dict) -> dict:
        warn("hmac_enqueue not available; returning dry-run result.")
        return {"ok": False, "reason": "hmac_enqueue_not_available"}


UNIFIED_SNAPSHOT_TAB = os.getenv("UNIFIED_SNAPSHOT_WS", "Unified_Snapshot")

# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _float(x: Any, default: float = 0.0) -> float:
    try:
        return float(str(x).replace(",", "").replace("$", "").strip())
    except Exception:
        return default


def _get_ws_safe(tab: str):
    try:
        return get_ws(tab)
    except Exception as e:
        warn(f"nova_trigger: Failed to open sheet/tab {tab}: {e}")
        return None


def _get_price_usd_from_snapshot(token: str) -> Tuple[Optional[float], str]:
    """
    Try to infer a USD price for token from Unified_Snapshot.

    Current schema (by design) does not guarantee a Price_USD column, so we
    fall back to heuristics on Wallet_Monitor-like data if needed.
    """
    ws = _get_ws_safe(UNIFIED_SNAPSHOT_TAB)
    if ws is None:
        return None, "no_snapshot_ws"

    try:
        rows = ws.get_all_records()
    except Exception as e:
        warn(f"nova_trigger: Could not read Unified_Snapshot: {e}")
        return None, "snapshot_read_error"

    token_up = token.upper()
    candidates: List[float] = []

    for row in rows:
        r_token = str(row.get("Token") or "").upper()
        if r_token != token_up:
            continue

        price_cols = [
            "Price_USD", 
            "Price", 
            "Last_Price_USD",
        ]
        for pc in price_cols:
            val = row.get(pc)
            if val is not None and str(val).strip() != "":
                try:
                    p = float(str(val).replace(",", "").replace("$", "").strip())
                    if p > 0:
                        candidates.append(p)
                except Exception:
                    continue

    if not candidates:
        return None, "not_found"

    # Simple heuristic: take median-ish (sort and pick middle)
    candidates.sort()
    mid = len(candidates) // 2
    return candidates[mid], "ok"


def _get_price_usd(token: str, quote: str, venue: Optional[str]) -> Tuple[Optional[float], str]:
    """
    Unified price helper:
    1) Try Unified_Snapshot (if it ever gets a Price_USD column)
    2) If not found, fall back to direct venue price via price_feed.get_price_usd
    """
    # 1) Try snapshot (current behavior)
    snap_price, snap_reason = _get_price_usd_from_snapshot(token)
    if snap_price is not None:
        return snap_price, "snapshot_ok"

    # 2) Fall back to direct venue price feed
    price = _feed_get_price_usd(token, quote or "USDT", venue)
    if price is not None:
        return price, "venue_feed_ok"

    # Still nothing
    return None, f"snapshot:{snap_reason};feed_not_found"

# -----------------------------------------------------------------------
# Core Router
# -----------------------------------------------------------------------
def route_manual(raw: str) -> dict:
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
    
    # B-2: Auto Price Fetch (snapshot → venue feed)
    price_usd, p_reason = _get_price_usd(
        intent["token"],
        intent.get("quote") or "USDT",
        intent.get("venue"),
    )
    if price_usd is not None:
        intent["price_usd"] = price_usd
    else:
        warn(f"nova_trigger: Could not find price for {intent['token']} ({p_reason})")

    # Policy Check
    if evaluate_manual_rebuy is None:
        warn("nova_trigger: evaluate_manual_rebuy not available; treating as rejected.")
        decision = {"ok": False, "reason": "policy_engine_unavailable", "patched_intent": {}}
    else:
        decision = evaluate_manual_rebuy(intent, asset_state={})

    policy_ok = bool(decision.get("ok"))
    decision_reason = decision.get("reason")
    patched = decision.get("patched_intent", {})
    
    enq_ok = False
    enq_reason = None
    mode = os.getenv("REBUY_MODE", "dryrun").lower()
    
    if policy_ok and mode == "live":
        try:
            # Construct payload for enqueue
            payload = {
                "venue": patched.get("venue") or intent["venue"],
                "symbol": patched.get("symbol") or f"{intent['token']}/{intent['quote']}",
                "side": patched.get("side") or "BUY",
                "amount": patched.get("amount") or 0.0,
                "amount_quote": patched.get("amount_usd"),
                "source": "manual_rebuy",
                "ts": int(time.time())
            }
            if intent.get("price_usd"):
                payload["price_usd"] = intent["price_usd"]

            res = hmac_enqueue(payload)
            enq_ok = bool(res.get("ok"))
            enq_reason = res.get("reason")
        except Exception as e:
            warn(f"nova_trigger: Enqueue failed: {e}")
            enq_reason = str(e)

    _send_summary(raw, intent, decision, enq_ok, enq_reason, mode)
    
    return {
        "ok": policy_ok,
        "decision": decision,
        "enqueue": {"ok": enq_ok, "reason": enq_reason}
    }

def parse_manual(raw: str) -> dict:
    # MANUAL_REBUY BTC 500 VENUE=BINANCEUS
    parts = raw.strip().split()
    if len(parts) < 3:
        return {"ok": False, "reason": "formatting_error"}
    
    token = parts[1].upper()
    try:
        amt = float(parts[2])
    except Exception:
        return {"ok": False, "reason": "invalid_amount"}
    
    venue = None
    quote = "USDT"
    
    for p in parts[3:]:
        p_up = p.upper()
        if p_up.startswith("VENUE="):
            venue = p_up.split("=",1)[1]
        elif p_up.startswith("QUOTE="):
            quote = p_up.split("=",1)[1]

    if not venue:
        return {"ok": False, "reason": "missing_venue"}

    return {
        "ok": True,
        "token": token,
        "amount_usd": amt,
        "venue": venue,
        "quote": quote,
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
