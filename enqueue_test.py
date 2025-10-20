#!/usr/bin/env python3
"""
enqueue_test.py  –  quick smoke test for NovaTrade Bus /api/ops/enqueue

Usage:
    python enqueue_test.py
"""

import json, hmac, hashlib, urllib.request

# === CONFIG ===
BUS_URL   = "https://novatrade3-0.onrender.com/api/ops/enqueue"   # your Bus endpoint
OUTBOX_SECRET = "3f36e385d5b3c83e66209cdac0d815788e1459b49cc67b6a6159cfa4de34511b8"  # <-- paste your Bus OUTBOX_SECRET here

# Simple intent to verify the path + signature + Policy_Log
intent = {
    "intent": {
        "action": "BUY",
        "venue": "KRAKEN",
        "symbol": "BTC-USDT",
        "amount_usd": 5,
        "source": "smoke-test"
    }
}

# Canonical JSON → bytes
raw = json.dumps(intent, separators=(",", ":"), sort_keys=False).encode()

# HMAC-SHA256 using your secret
sig = hmac.new(OUTBOX_SECRET.encode(), raw, hashlib.sha256).hexdigest()

# HTTP request
req = urllib.request.Request(
    BUS_URL,
    data=raw,
    headers={
        "Content-Type": "application/json",
        "X-Outbox-Signature": f"sha256={sig}"
    },
    method="POST"
)

print("📡 Sending intent to Bus...")
try:
    with urllib.request.urlopen(req, timeout=20) as r:
        print(f"✅ Response {r.status}:")
        print(r.read().decode())
except Exception as e:
    print(f"❌ Error: {e}")
