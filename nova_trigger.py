# nova_trigger.py — parse + route manual commands, Telegram ping
import os, json, time, hmac, hashlib, re
from policy_engine import PolicyEngine
import requests

BASE_URL = (
    os.getenv("OPS_BASE_URL") or          # optional new var just for enqueue calls
    os.getenv("CLOUD_BASE_URL") or
    "https://novatrade3-0.onrender.com"   # your service URL
).rstrip("/")
REBUY_MODE    = os.getenv("REBUY_MODE","dryrun").lower()
OUTBOX_SECRET = os.getenv("OUTBOX_SECRET","")

# optional Telegram (uses your existing infra)
def send_telegram(text:str):
    bot = os.getenv("BOT_TOKEN"); chat = os.getenv("TELEGRAM_CHAT_ID")
    if not (bot and chat): return
    try:
        requests.post(f"https://api.telegram.org/bot{bot}/sendMessage",
                      json={"chat_id":chat,"text":text}, timeout=10)
    except Exception: pass

def _hmac(sig_payload:dict) -> str:
    raw = json.dumps(sig_payload, separators=(",",":"), sort_keys=True).encode("utf-8")
    return hmac.new(OUTBOX_SECRET.encode("utf-8"), raw, hashlib.sha256).hexdigest()

def _enqueue(payload: dict) -> dict:
    """
    Sends raw JSON body to OPS_ENQUEUE_URL (or BASE_URL + /api/ops/enqueue)
    with X-Outbox-Signature: sha256=<hex(hmac(body))>
    """
    import requests, json, hmac, hashlib, os

    # ✅ Always prefer the correct API path
    url = (
        os.getenv("OPS_ENQUEUE_URL")
        or (BASE_URL.rstrip("/") + "/api/ops/enqueue")
    )

    raw = json.dumps(payload, separators=(",", ":"), sort_keys=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}

    if OUTBOX_SECRET:
        mac = hmac.new(OUTBOX_SECRET.encode("utf-8"), raw, hashlib.sha256).hexdigest()
        headers["X-Outbox-Signature"] = f"sha256={mac}"

    try:
        r = requests.post(url, data=raw, headers=headers, timeout=20)
        return {
            "ok": r.ok,
            "status": r.status_code,
            "text": r.text[:200],
            "url": url
        }
    except Exception as e:
        return {"ok": False, "status": 0, "text": str(e), "url": url}

def parse_manual(msg:str) -> dict|None:
    """
    EXAMPLES:
      MANUAL_REBUY BTC 5 VENUE=BINANCEUS
      MANUAL_REBUY ETH 10 VENUE=COINBASE QUOTE=USD
    """
    m = re.match(r"^\s*MANUAL_REBUY\s+([A-Za-z0-9]+)\s+(\d+(?:\.\d+)?)\s*(.*)$", msg or "")
    if not m: return None
    token = m.group(1).upper()
    amt   = float(m.group(2))
    rest  = m.group(3) or ""
    kv = dict((k.upper(),v.upper()) for k,v in re.findall(r"([A-Za-z_]+)\s*=\s*([A-Za-z0-9\-]+)", rest))
    venue = kv.get("VENUE","BINANCEUS")
    quote = kv.get("QUOTE","")
    return {"source":"manual_rebuy","token":token,"action":"BUY","amount_usd":amt,"venue":venue,"quote":quote,"ts":int(time.time())}

def route_manual(msg:str) -> dict:
    intent = parse_manual(msg)
    if not intent: return {"ok": False, "reason":"unrecognized manual format"}

    pe = PolicyEngine()
    # if you have on-sheet metrics, pass them here; for manual majors we skip liq anyway
    asset_state = {}
    decision = pe.validate(intent, asset_state)

    # enqueue only if OK + live
    enq = {"ok": False}
    if decision.get("ok") and REBUY_MODE == "live":
        payload = {
            "venue": decision["venue"],
            "symbol": decision["symbol"],     # mapped per venue
            "side":   "BUY",
            "amount_usd": decision["amount_usd"],
            "ts": decision["ts"]
        }
        quote_amt = float(decision["amount_usd"])
        symbol = decision["symbol"]
        payload = {
            "venue":  decision["venue"],
            "symbol": symbol,
            "side":   "BUY",
            # ops_enqueue expects amount in the quote currency (USD/USDT/USDC)
            "amount_quote": quote_amt,
            "client_id": f"manual-{decision['token']}-{int(decision['ts'])}",
            "policy_reason": decision.get("reason","ok"),
        }
        enq = _enqueue(payload)
        print(f"[manual_enq] url={enq.get('url')} status={enq.get('status')} ok={enq.get('ok')} text={enq.get('text')}")
    
    # Telegram notice (brief)
    send_telegram(f"🔔 Orion voice triggered: {msg}\nPolicy: {'OK' if decision.get('ok') else 'DENY'} ({decision.get('reason')})\nEnqueued: {enq.get('ok')} mode={REBUY_MODE}")
    return {"intent": intent, "decision": decision, "enqueue": enq}

# --- shim: trigger_nova_ping, expected by Nova ping ---
def trigger_nova_ping(trigger_type: str = "NOVA UPDATE"):
    presets = {
        "SOS": "🚨 *NovaTrade SOS*\nTesting alert path.",
        "PRESALE ALERT": "🚀 *Presale Alert*\nNew high-score presale detected.",
        "ROTATION COMPLETE": "🔁 *Rotation Complete*\nVault rotation executed.",
        "SYNC NEEDED": "🧩 *Sync Needed*\nPlease review latest responses.",
        "FYI ONLY": "📘 *FYI*\nNon-urgent update.",
        "NOVA UPDATE": "🧠 *Nova Update*\nSystem improvement deployed.",
    }
    text = presets.get(trigger_type.upper(), f"🔔 *{trigger_type}*")
    # reuse the helper already in nova_trigger.py
    send_telegram(text)
    return {"ok": True, "type": trigger_type}
