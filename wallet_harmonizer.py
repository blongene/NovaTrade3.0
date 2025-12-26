# wallet_harmonizer.py — Bus (Phase: liquidity floor keeper)
# Scans Unified_Snapshot and enqueues SWAP intents to restore quote floors per venue.
#
# Modes:
# - HARMONIZER_MODE=dryrun (default): prints actions only
# - HARMONIZER_MODE=live: posts signed intents to OPS_ENQUEUE_URL
#
# Uses Bus utils.get_sheet() + with_sheet_backoff (consistent Sheets posture).

import os
import json
import time
import hmac
import hashlib
from typing import Dict, Any, List, Tuple, Optional

import requests
from utils import with_sheet_backoff, get_sheet

SHEET_URL = os.getenv("SHEET_URL")
SNAP_WS = os.getenv("UNIFIED_SNAPSHOT_WS", "Unified_Snapshot")

HARMONIZER_MODE = (os.getenv("HARMONIZER_MODE", "dryrun") or "dryrun").lower().strip()
QUOTE_FLOORS = json.loads(os.getenv("QUOTE_FLOORS_JSON", "{}") or "{}")

OPS_ENQUEUE_URL = (os.getenv("OPS_ENQUEUE_URL", "http://localhost:10000/ops/enqueue") or "").strip()
OUTBOX_SECRET = os.getenv("OUTBOX_SECRET", "") or os.getenv("OUTBOX_SECRET_FILE", "")

# If OUTBOX_SECRET_FILE is used, try to read it
if OUTBOX_SECRET and OUTBOX_SECRET.endswith(".txt") is False and os.path.exists(OUTBOX_SECRET):
    # If someone mistakenly put a filepath into OUTBOX_SECRET, accept it
    try:
        with open(OUTBOX_SECRET, "r", encoding="utf-8") as f:
            OUTBOX_SECRET = (f.read() or "").strip()
    except Exception:
        pass

if os.getenv("OUTBOX_SECRET_FILE"):
    try:
        with open(os.getenv("OUTBOX_SECRET_FILE"), "r", encoding="utf-8") as f:
            OUTBOX_SECRET = (f.read() or "").strip()
    except Exception:
        pass

HARMONIZER_ALLOW_FIAT_BRIDGE = os.getenv("HARMONIZER_ALLOW_FIAT_BRIDGE", "0").lower() in {"1", "true", "yes", "on"}
FIAT_BRIDGES = json.loads(os.getenv("HARMONIZER_FIAT_BRIDGES_JSON", "{}") or "{}")

QUOTES = {"USDT", "USDC", "USD", "EUR"}

if not OUTBOX_SECRET:
    if HARMONIZER_MODE == "live":
        print("⚠️ wallet_harmonizer: OUTBOX_SECRET missing; forcing dryrun.")
    HARMONIZER_MODE = "dryrun"


def _hmac(payload: dict) -> str:
    msg = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hmac.new(OUTBOX_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def _intent_id(intent: dict) -> str:
    # Stable idempotency key
    core = {
        "action": intent.get("action"),
        "venue": intent.get("venue"),
        "from": intent.get("from"),
        "to": intent.get("to"),
        "amount_usd": intent.get("amount_usd"),
        "source": intent.get("source"),
    }
    msg = json.dumps(core, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(msg).hexdigest()[:24]


def _post_intent(intent: dict) -> Tuple[bool, str]:
    payload = {"intent": intent, "sig": _hmac(intent)}
    headers = {"Content-Type": "application/json"}
    # also include signature in header for compatibility with other Nova endpoints
    try:
        headers["X-NT-Sig"] = _hmac(intent)
    except Exception:
        pass
    try:
        r = requests.post(OPS_ENQUEUE_URL, json=payload, headers=headers, timeout=12)
        if 200 <= r.status_code < 300:
            return True, "ok"
        return False, f"{r.status_code}:{(r.text or '')[:240]}"
    except Exception as e:
        return False, str(e)


@with_sheet_backoff
def run_wallet_harmonizer() -> None:
    print("🧮 Wallet Harmonizer scanning…")

    if not SHEET_URL:
        print("⚠️ wallet_harmonizer: SHEET_URL missing; skipping.")
        return

    if not OPS_ENQUEUE_URL and HARMONIZER_MODE == "live":
        print("⚠️ wallet_harmonizer: OPS_ENQUEUE_URL missing; forcing dryrun.")
        mode = "dryrun"
    else:
        mode = HARMONIZER_MODE

    sheet = get_sheet()
    try:
        snap = sheet.worksheet(SNAP_WS).get_all_records()
    except Exception as e:
        print(f"⚠️ wallet_harmonizer: no Unified_Snapshot yet: {e}; skipping.")
        return

    if not snap:
        print("⚠️ wallet_harmonizer: Unified_Snapshot empty; nothing to harmonize.")
        return

    by_venue: Dict[str, Dict[str, float]] = {}
    for r in snap:
        v = str(r.get("Venue", "") or "").upper().strip()
        a = str(r.get("Asset", "") or "").upper().strip()
        tot = float(r.get("Total", 0) or 0)
        if not v or not a:
            continue
        by_venue.setdefault(v, {})
        by_venue[v][a] = by_venue[v].get(a, 0.0) + tot

    fixes: List[Tuple[str, str, str, float]] = []
    notes: List[str] = []

    for venue, floors in (QUOTE_FLOORS or {}).items():
        v = str(venue).upper().strip()
        have = by_venue.get(v, {})

        if not isinstance(floors, dict):
            continue

        for quote_sym, min_amt in floors.items():
            q = str(quote_sym).upper().strip()
            try:
                floor_amt = float(min_amt)
            except Exception:
                continue

            current = float(have.get(q, 0.0))
            deficit = floor_amt - current
            if deficit <= 0:
                continue

            # Donors: largest non-quote asset
            donors = [(a, bal) for a, bal in have.items() if a not in QUOTES and float(bal) > 0.0]
            donors.sort(key=lambda x: x[1], reverse=True)
            donor_asset = donors[0][0] if donors else None

            if donor_asset:
                fixes.append((v, donor_asset, q, deficit))
                continue

            if HARMONIZER_ALLOW_FIAT_BRIDGE:
                fiats = FIAT_BRIDGES.get(v, []) or []
                fiat_donors = [(a, float(have.get(a, 0.0))) for a in fiats if float(have.get(a, 0.0)) > 0.0]
                fiat_donors.sort(key=lambda x: x[1], reverse=True)
                if fiat_donors:
                    donor_asset = fiat_donors[0][0]
                    fixes.append((v, donor_asset, q, deficit))
                else:
                    notes.append(f"⚠️ {v}: below floor for {q} by {deficit:.2f} but no donors (alts/fiat).")
            else:
                notes.append(f"⚠️ {v}: below floor for {q} by {deficit:.2f} but no donors.")

    if notes:
        for n in notes:
            print(n)

    if not fixes:
        print("✅ Wallet Harmonizer: all venues at/above quote floors (or no donors available).")
        return

    for venue, from_asset, to_quote, amount in fixes:
        intent = {
            "action": "SWAP",
            "venue": venue,
            "from": from_asset,
            "to": to_quote,
            "amount_usd": round(float(amount), 2),
            "source": "wallet_harmonizer",
            "ts": int(time.time()),
        }
        intent["intent_id"] = _intent_id(intent)

        if mode != "live":
            print(f"…dryrun SWAP {venue}: {from_asset}→{to_quote} ${intent['amount_usd']} (intent_id={intent['intent_id']})")
            continue

        ok, msg = _post_intent(intent)
        if ok:
            print(f"🚚 Enqueued SWAP {venue}: {from_asset}→{to_quote} ${intent['amount_usd']} (intent_id={intent['intent_id']})")
        else:
            print(f"⚠️ Enqueue failed {venue}: {from_asset}→{to_quote} ${intent['amount_usd']} err={msg}")


if __name__ == "__main__":
    run_wallet_harmonizer()
