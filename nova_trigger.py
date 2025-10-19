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

def _enqueue(payload:dict) -> dict:
    body = {"payload": payload, "sig": _hmac(payload)} if OUTBOX_SECRET else {"payload": payload}
    r = requests.post(f"{BASE_URL}/ops/enqueue", json=body, timeout=20)
    ok = r.ok
    return {"ok": ok, "status": r.status_code, "text": r.text[:200]}

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
        enq = _enqueue(payload)
    print(f"[manual_enq] base={BASE_URL} mode={REBUY_MODE} "
    f"status={enq.get('status')} ok={enq.get('ok')} text={enq.get('text')[:160]}")
    
    # Telegram notice (brief)
    send_telegram(f"🔔 Orion voice triggered: {msg}\nPolicy: {'OK' if decision.get('ok') else 'DENY'} ({decision.get('reason')})\nEnqueued: {enq.get('ok')} mode={REBUY_MODE}")
    return {"intent": intent, "decision": decision, "enqueue": enq}
