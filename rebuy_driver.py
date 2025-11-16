# rebuy_driver.py — C-Series + B-2 price feed via trade_guard

import os, json, hmac, hashlib, time
from datetime import datetime
from typing import Dict, Any

import gspread  # type: ignore

from utils import get_gspread_client, warn  # type: ignore
from policy_engine import PolicyEngine
from trade_guard import guard_trade_intent
from price_feed import get_price_usd  # NEW

SHEET_URL = os.getenv("SHEET_URL")
VAULT_WS_NAME = os.getenv("VAULT_INTELLIGENCE_WS", "Vault Intelligence")
REBUY_MODE = os.getenv("REBUY_MODE", "dryrun").lower()  # 'dryrun' or 'live'

OUTBOX_SECRET = os.getenv("OUTBOX_SECRET", "")
OPS_ENQUEUE_URL = os.getenv("OPS_ENQUEUE_URL", "http://localhost:10000/ops/enqueue")


def _hmac_sign(secret: str, payload: dict) -> str:
    msg = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def _open_sheet() -> gspread.Spreadsheet:
    if not SHEET_URL:
        raise RuntimeError("SHEET_URL not set.")
    gc = get_gspread_client()
    return gc.open_by_url(SHEET_URL)


def run_rebuy_driver():
    """
    Pull candidates from 'Vault Intelligence' where rebuy_ready == TRUE.
    For each, build BUY intent → trade_guard.guard_trade_intent() → if ok:
       - DRYRUN: log to Policy_Log only
       - LIVE  : HMAC-sign and POST /ops/enqueue with patched sizing
    """
    print("🔁 Rebuy Driver: evaluating candidates…")

    if not SHEET_URL:
        print("⚠️ SHEET_URL not set; aborting rebuy_driver.")
        return

    try:
        sh = _open_sheet()
    except Exception as e:
        print(f"❌ rebuy_driver: failed to open sheet: {e}")
        return

    try:
        ws = sh.worksheet(VAULT_WS_NAME)
        rows = ws.get_all_records()
    except Exception as e:
        print(f"⚠️ No '{VAULT_WS_NAME}' sheet or unable to read it: {e}")
        return

    pe = PolicyEngine()
    max_per_coin = float(pe.cfg.get("max_per_coin_usd", 25) or 25)
    venue_order = pe.cfg.get("venue_order", ["BINANCEUS", "COINBASE", "KRAKEN"]) or ["BINANCEUS"]
    prefer_quotes = pe.cfg.get("prefer_quotes", {}) or {}

    enqueued = 0

    for r in rows:
        token = str(r.get("Token", "")).strip().upper()
        if not token:
            continue

        ready_raw = str(r.get("rebuy_ready", "")).strip().upper()
        if ready_raw not in ("TRUE", "YES", "1", "Y"):
            continue

        amt_usd = max_per_coin

        venue = str(venue_order[0]).upper() if venue_order else "BINANCEUS"
        quote = prefer_quotes.get(venue, "USDT")

        now = int(time.time())
        intent_id = f"rebuy:{token}:{now}"

        # B-2: get price for this venue/token/quote
        price_usd = get_price_usd(token, quote, venue)

        guard_intent: Dict[str, Any] = {
            "token": token,
            "venue": venue,
            "quote": quote,
            "amount_usd": amt_usd,
            "price_usd": price_usd,
            "action": "BUY",
            "intent_id": intent_id,
            "agent_target": os.getenv("DEFAULT_AGENT_TARGET", "edge-primary,edge-nl1"),
            "source": "rebuy_driver",
            "policy_id": os.getenv("POLICY_ID", "main"),
        }

        decision = guard_trade_intent(guard_intent)
        ok = bool(decision.get("ok"))
        if not ok:
            reason = decision.get("reason") or ""
            status = decision.get("status") or "DENIED"
            print(f"…policy {status} {token} on {venue}: {reason}")
            continue

        patched = decision.get("patched") or {}
        if not isinstance(patched, dict):
            patched = {}

        final_amt_usd = float(patched.get("amount_usd", amt_usd))
        final_venue = str(patched.get("venue") or venue).upper()
        final_quote = str(patched.get("quote") or quote).upper()

        if REBUY_MODE == "dryrun":
            try:
                log_ws = sh.worksheet("Policy_Log")
                log_ws.append_row(
                    [
                        datetime.utcnow().isoformat(),
                        token,
                        "AUTO_REBUY_DRYRUN",
                        final_amt_usd,
                        "TRUE",
                        "dryrun-ok",
                        json.dumps(patched),
                        final_venue,
                        final_quote,
                        r.get("liquidity_usd", ""),
                        pe.cooldown_min,
                    ],
                    value_input_option="USER_ENTERED",
                )
            except Exception as e:
                warn(f"rebuy_driver: failed to append Policy_Log row: {e}")

            print(f"✅ DRYRUN approved: {token} {final_amt_usd} {final_quote}")
            enqueued += 1
            continue

        if not OUTBOX_SECRET or not OPS_ENQUEUE_URL:
            print("⚠️ OUTBOX_SECRET or OPS_ENQUEUE_URL missing; skipping LIVE enqueue.")
            continue

        payload = {
            "symbol": f"{token}/{final_quote}",
            "venue": final_venue,
            "side": "BUY",
            "amount_usd": final_amt_usd,
            "source": "rebuy_driver",
            "ts": int(time.time()),
            "intent_id": intent_id,
        }
        sig = _hmac_sign(OUTBOX_SECRET, payload)

        import requests  # type: ignore
        try:
            rpost = requests.post(
                OPS_ENQUEUE_URL,
                json={"payload": payload, "sig": sig},
                timeout=15,
            )
            if rpost.ok:
                print(f"✅ ENQUEUED: {payload}")
                enqueued += 1
            else:
                print(f"❌ enqueue failed: {rpost.status_code} {rpost.text}")
        except Exception as e:
            print(f"❌ enqueue error: {e}")

    print(f"Rebuy Driver complete. Approved/Enqueued={enqueued}")
