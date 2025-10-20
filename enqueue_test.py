# enqueue_test.py — prints the REAL error message from /api/ops/enqueue

import json, os, hmac, hashlib, urllib.request, urllib.error

HOST   = os.environ.get("HOST",   "https://novatrade3-0.onrender.com")
SECRET = os.environ.get("SECRET", "3f36e385d5b3c83e66209cdac0d815788e1459b49cc67b6a6159cfa4de34511b8")

# ⚠️ The body *must* be compact JSON (no spaces) if you sign it
payload = {
    "intent": {
        "action": "BUY",
        "venue": "KRAKEN",
        "symbol": "BTC-USDT",
        "amount_usd": 5,
        "source": "smoke",
    }
}
BODY = json.dumps(payload, separators=(",", ":")).encode("utf-8")

# Compute HMAC SHA-256 over the *exact* BODY bytes and send it in the header
sig = hmac.new(SECRET.encode("utf-8"), BODY, hashlib.sha256).hexdigest()
headers = {
    "Content-Type": "application/json",
    "X-Outbox-Signature": f"sha256={sig}",
}

req = urllib.request.Request(f"{HOST}/api/ops/enqueue", data=BODY, headers=headers, method="POST")

print("📡 Sending intent to Bus...")
try:
    with urllib.request.urlopen(req, timeout=20) as resp:
        print("✅ OK", resp.status, resp.read().decode())
except urllib.error.HTTPError as e:
    # 💡 THIS is the important bit — print the server’s 422 details
    body = e.read().decode(errors="replace")
    print(f"❌ HTTP {e.code}")
    print(body)
except Exception as e:
    print("❌", repr(e))
