#!/usr/bin/env python3
"""Safe dry-run injector for BinanceUS via the Bus /ops/enqueue endpoint.

Usage (Windows PowerShell or bash):
  python tools/inject_binanceus_dryrun.py --base-url https://novatrade3-0.onrender.com \
      --secret $env:OUTBOX_SECRET --agent edge-primary --symbol BTCUSDT --side buy --amount 5

Notes:
- This ONLY enqueues a command. Your Edge Agent decides live vs dry via EDGE_MODE / config.
- If REQUIRE_HMAC_OPS=1 on the Bus, you must provide OUTBOX_SECRET.
"""

import argparse, json, hmac, hashlib, sys
from urllib.parse import urljoin
import requests

def sign(secret: str, body_bytes: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", required=True, help="Bus base URL, e.g. https://novatrade3-0.onrender.com")
    ap.add_argument("--secret", default="", help="OUTBOX_SECRET (required if Bus requires ops HMAC)")
    ap.add_argument("--agent", default="edge-primary", help="agent_target (Edge Agent ID)")
    ap.add_argument("--venue", default="BINANCEUS", help="venue (optional; default BINANCEUS)")
    ap.add_argument("--symbol", default="BTCUSDT", help="symbol, e.g. BTCUSDT or BTC-USDT")
    ap.add_argument("--side", default="buy", choices=["buy","sell"], help="buy|sell")
    ap.add_argument("--amount", type=float, required=True, help="amount in quote currency (USD/USDT), per Bus schema")
    ap.add_argument("--source", default="injector", help="source tag")
    ap.add_argument("--id", default="", help="optional idempotency id")
    args = ap.parse_args()

    body = {
        "agent_target": args.agent,
        "venue": args.venue,
        "symbol": args.symbol,
        "side": args.side,
        "amount": float(args.amount),
        "flags": ["quote"],   # treat amount as quote currency
        "source": args.source,
    }
    if args.id:
        body["id"] = args.id

    raw = json.dumps(body, separators=(",",":"), sort_keys=True).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if args.secret:
        headers["X-NT-Sig"] = sign(args.secret, raw)

    url = urljoin(args.base_url.rstrip("/") + "/", "ops/enqueue")
    r = requests.post(url, data=raw, headers=headers, timeout=20)
    print(f"HTTP {r.status_code}")
    try:
        print(json.dumps(r.json(), indent=2))
    except Exception:
        print(r.text)
    return 0 if r.ok else 1

if __name__ == "__main__":
    raise SystemExit(main())
