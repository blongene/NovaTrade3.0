# rebuy_driver.py
import os, json, hmac, hashlib, time
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from policy_engine import PolicyEngine

SHEET_URL = os.getenv("SHEET_URL")
VAULT_WS_NAME = os.getenv("VAULT_INTELLIGENCE_WS", "Vault Intelligence")
REBUY_MODE = os.getenv("REBUY_MODE", "dryrun").lower()  # 'dryrun' or 'live'

OUTBOX_SECRET = os.getenv("OUTBOX_SECRET","")
OPS_ENQUEUE_URL = os.getenv("OPS_ENQUEUE_URL","http://localhost:10000/ops/enqueue")

MIN_QUOTE_RESERVE_USD = float(os.getenv("MIN_QUOTE_RESERVE_USD","10") or 10)

def _open_sheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL)

def _hmac_sign(secret:str, payload:dict)->str:
    msg = json.dumps(payload, separators=(",",":"), sort_keys=True).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()

def _quote_reserve_ok(amount_usd:float)->bool:
    return amount_usd >= MIN_QUOTE_RESERVE_USD

def run_rebuy_driver():
    """
    Pull candidates from 'Vault Intelligence' where rebuy_ready == TRUE.
    For each, build BUY intent → policy_engine.validate() → if ok:
       - DRYRUN: log to sheet only
       - LIVE  : HMAC-sign and POST /ops/enqueue
    """
    print("🔁 Rebuy Driver: evaluating candidates…")
    sh = _open_sheet()
    try:
        ws = sh.worksheet(VAULT_WS_NAME)
        rows = ws.get_all_records()
    except Exception:
        print("⚠️ No Vault Intelligence sheet yet.")
        return

    pe = PolicyEngine()
    enqueued = 0
    for r in rows:
        token = str(r.get("Token","")).strip().upper()
        if not token: continue
        if str(r.get("rebuy_ready","")).strip().upper() != "TRUE":
            continue

        amt_usd = float(pe.cfg.get("max_per_coin_usd", 25) or 25)
        if not _quote_reserve_ok(amt_usd):
            print(f"…skip {token}: below min quote reserve")
            continue

        venue_order = pe.cfg.get("venue_order", ["BINANCEUS","COINBASE","KRAKEN"])
        venue = venue_order[0] if venue_order else "BINANCEUS"
        quote = pe.cfg.get("prefer_quotes",{}).get(venue, "USDT")

        # Stable identity + metadata for Policy Spine
        now = int(time.time())
        intent_id = f"rebuy:{token}:{now}"

        intent = {
            "id": intent_id,
            "token": token,
            "action": "BUY",
            "amount_usd": amt_usd,
            "venue": venue,
            "quote": quote,
            "rebuy_driver": True,
            "agent_target": os.getenv("DEFAULT_AGENT_TARGET", "edge-primary,edge-nl1"),
            "source": "rebuy_driver",
            "policy_id": os.getenv("POLICY_ID", "main"),
        }

        ok, reason, patched = pe.validate(intent, r)
        if not ok:
            print(f"…policy reject {token}: {reason}")
            continue

        if REBUY_MODE == "dryrun":
            try:
                log = sh.worksheet("Policy_Log")
                log.append_row([datetime.utcnow().isoformat(), token, "BUY_DRYRUN", patched.get("amount_usd"), "TRUE", "dryrun-ok", json.dumps(patched), patched["venue"], patched["quote"], r.get("liquidity_usd",""), pe.cooldown_min])
            except Exception:
                pass
            print(f"✅ DRYRUN approved: {token} {patched.get('amount_usd')} {patched.get('quote')}")
            enqueued += 1
        else:
            payload = {
                "symbol": f"{token}/{patched['quote']}",
                "venue": patched["venue"],
                "side": "BUY",
                "amount_usd": patched["amount_usd"],
                "source": "rebuy_driver",
                "ts": int(time.time())
            }
            sig = _hmac_sign(OUTBOX_SECRET, payload)

            import requests
            try:
                rpost = requests.post(OPS_ENQUEUE_URL, json={"payload": payload, "sig": sig}, timeout=15)
                if rpost.ok:
                    print(f"✅ ENQUEUED: {payload}")
                    enqueued += 1
                else:
                    print(f"❌ enqueue failed: {rpost.status_code} {rpost.text}")
            except Exception as e:
                print(f"❌ enqueue error: {e}")

    print(f"Rebuy Driver complete. Approved={enqueued}")
