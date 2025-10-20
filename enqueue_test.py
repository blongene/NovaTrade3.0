#!/usr/bin/env python3
"""
enqueue_test.py — minimal, correct, quiet-by-default enqueue tester.

Usage (examples):
  # env-driven (recommended)
  HOST=https://your-app.onrender.com SECRET=... python enqueue_test.py \
      --side BUY --venue KRAKEN --symbol BTC-USDT --amount 5

  # or pass everything explicitly
  python enqueue_test.py \
      --host https://your-app.onrender.com --secret <hex-hmac-key> \
      --side BUY --venue KRAKEN --symbol BTC-USDT --amount 5 --source smoke --verbose
"""

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from urllib import request, error

DEFAULT_PATH = "/api/ops/enqueue"


def die(msg: str, code: int = 1):
    print(msg, file=sys.stderr)
    sys.exit(code)


def parse_args():
    p = argparse.ArgumentParser(
        description="Post a signed intent to the Bus /api/ops/enqueue.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--host", default=os.environ.get("HOST", "").strip(),
                   help="Base URL of your service (e.g., https://…render.com)")
    p.add_argument("--secret", default=os.environ.get("SECRET", "").strip(),
                   help="HMAC hex key shared with Outbox/Bus (X-Outbox-Signature)")
    p.add_argument("--path", default=DEFAULT_PATH, help="Enqueue endpoint path")
    p.add_argument("--side", required=True, help="BUY or SELL")
    p.add_argument("--venue", required=True, help="Exchange/venue id (e.g., KRAKEN)")
    p.add_argument("--symbol", required=True, help="Market symbol (e.g., BTC-USDT)")
    p.add_argument("--amount", type=float, default=5.0, help="USD notional (amount_usd)")
    p.add_argument("--source", default="smoke", help="Intent source tag")
    p.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout (seconds)")
    p.add_argument("--retries", type=int, default=0, help="Number of retry attempts on network errors")
    p.add_argument("--sleep", type=float, default=0.5, help="Seconds to sleep between retries")
    p.add_argument("--verbose", action="store_true", help="Print full response/error body")
    p.add_argument("--quiet", action="store_true", help="Print the *absolute minimum* output")
    return p.parse_args()


def normalize_args(a):
    side = a.side.upper().strip()
    if side not in {"BUY", "SELL"}:
        die(f"Invalid --side '{a.side}'. Must be BUY or SELL.")
    venue = a.venue.upper().strip()
    symbol = a.symbol.strip()
    if not symbol or "-" not in symbol:
        die(f"Invalid --symbol '{a.symbol}'. Expected like 'BTC-USDT'.")
    if a.amount <= 0:
        die("--amount must be > 0")
    host = (a.host or "").rstrip("/")
    if not host.startswith("http"):
        die("Missing or invalid --host (or HOST env). Example: https://your-app.onrender.com")
    secret = a.secret or ""
    if not secret:
        die("Missing --secret (or SECRET env).")
    return side, venue, symbol, a.amount, a.source, host, a.path, secret


def make_body(side, venue, symbol, amount_usd, source):
    payload = {
        "intent": {
            "side": side,
            "venue": venue,
            "symbol": symbol,
            "amount_usd": amount_usd,
            "source": source,
        }
    }
    # compact JSON is important if the server verifies the exact signed bytes
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return body


def sign(secret_hex: str, body: bytes) -> str:
    try:
        key = bytes.fromhex(secret_hex)
    except ValueError:
        # allow raw bytes as a fallback (not recommended)
        key = secret_hex.encode("utf-8")
    sig = hmac.new(key, body, hashlib.sha256).hexdigest()
    return f"sha256={sig}"


def post(host, path, body, signature, timeout):
    url = f"{host}{path}"
    headers = {
        "Content-Type": "application/json",
        "X-Outbox-Signature": signature,
    }
    req = request.Request(url, data=body, headers=headers, method="POST")
    return request.urlopen(req, timeout=timeout)  # may raise


def main():
    a = parse_args()
    side, venue, symbol, amount, source, host, path, secret = normalize_args(a)
    body = make_body(side, venue, symbol, amount, source)
    signature = sign(secret, body)

    attempt = 0
    while True:
        try:
            if not a.quiet:
                print("📡 Sending intent…", flush=True)
            with post(host, path, body, signature, a.timeout) as resp:
                payload = resp.read().decode() if a.verbose else ""
                if a.quiet:
                    # one-line success suitable for piping/grepping
                    print(f"OK {resp.status}")
                else:
                    print(f"✅ OK {resp.status}")
                    if a.verbose and payload:
                        print(payload)
                return 0
        except error.HTTPError as e:
            err = e.read().decode(errors="replace")
            if a.quiet:
                print(f"HTTP {e.code}")
                if a.verbose and err:
                    print(err)
            else:
                print(f"❌ HTTP {e.code}")
                if a.verbose and err:
                    print(err)
            return 2
        except Exception as e:
            attempt += 1
            if attempt > a.retries:
                if a.quiet:
                    print("ERR network")
                else:
                    print(f"❌ {type(e).__name__}: {e}")
                return 3
            time.sleep(a.sleep)


if __name__ == "__main__":
    sys.exit(main())
