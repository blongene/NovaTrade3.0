import os, json, time, hmac, hashlib
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.getenv("SHEET_URL")
SNAP_WS = os.getenv("UNIFIED_SNAPSHOT_WS", "Unified_Snapshot")
HARMONIZER_MODE = os.getenv("HARMONIZER_MODE","dryrun").lower()  # 'dryrun' or 'live'
QUOTE_FLOORS = json.loads(os.getenv("QUOTE_FLOORS_JSON",'{}'))   # {"BINANCEUS":{"USDT":50},...}
OPS_ENQUEUE_URL = os.getenv("OPS_ENQUEUE_URL","http://localhost:10000/ops/enqueue")
OUTBOX_SECRET = os.getenv("OUTBOX_SECRET","")

def _open():
    scope=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds=ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds).open_by_url(SHEET_URL)

def _hmac(payload:dict)->str:
    import json
    msg = json.dumps(payload, separators=(",",":"), sort_keys=True).encode("utf-8")
    return hmac.new(OUTBOX_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()

def _post(url, json_payload):
    import requests
    return requests.post(url, json=json_payload, timeout=10)

def run_wallet_harmonizer():
    """
    Ensures each venue maintains minimum quote liquidity (USDT/USDC) per QUOTE_FLOORS_JSON.
    If a venue is below floor, enqueues a SWAP to raise it (dryrun by default).
    Assumes your Edge can interpret a `{action:'SWAP', from:'XYZ', to:'USDT', amount_usd: ...}` intent.
    """
    print("🧮 Wallet Harmonizer scanning …")
    sh = _open()
    try:
        snap = sh.worksheet(SNAP_WS).get_all_records()
    except Exception as e:
        print(f"⚠️ No Unified_Snapshot yet: {e}")
        return

    # Aggregate quote balances per venue
    by_venue = {}
    for r in snap:
        v = str(r.get("Venue","")).upper()
        a = str(r.get("Asset","")).upper()
        tot = float(r.get("Total",0) or 0)
        if v not in by_venue: by_venue[v] = {}
        by_venue[v][a] = by_venue[v].get(a, 0.0) + tot

    # For each venue & required quote, verify floors
    fixes = []
    for venue, floors in QUOTE_FLOORS.items():
        have = by_venue.get(venue, {})
        for quote_sym, min_amt in floors.items():
            current = have.get(quote_sym, 0.0)
            deficit = float(min_amt) - float(current)
            if deficit > 0:
                # Choose a donor asset if any (simple heuristic: largest non-quote balance)
                donors = [(a, bal) for a, bal in have.items() if a not in ("USDT","USDC","USD","EUR")]
                donors.sort(key=lambda x: x[1], reverse=True)
                donor_asset = donors[0][0] if donors else None
                if donor_asset:
                    fixes.append((venue, donor_asset, quote_sym, deficit))

    if not fixes:
        print("✅ All venues above quote floors.")
        return

    for venue, from_asset, to_quote, amount in fixes:
        intent = {
            "action":"SWAP",
            "venue": venue,
            "from": from_asset,
            "to": to_quote,
            "amount_usd": round(float(amount), 2),
            "source":"wallet_harmonizer",
            "ts": int(time.time())
        }

        if HARMONIZER_MODE == "dryrun":
            print(f"…dryrun SWAP {venue}: {from_asset}→{to_quote} ${intent['amount_usd']}")
            continue

        # LIVE: sign + enqueue
        payload = {"intent": intent, "sig": _hmac(intent)}
        try:
            resp = _post(OPS_ENQUEUE_URL, payload)
            if resp.ok:
                print(f"🚚 Enqueued SWAP {venue}: {from_asset}→{to_quote} ${intent['amount_usd']}")
            else:
                print(f"⚠️ Enqueue failed: {resp.status_code} {resp.text}")
        except Exception as e:
            print(f"⚠️ Enqueue error: {e}")
