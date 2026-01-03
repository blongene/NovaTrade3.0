#!/usr/bin/env python3
"""
Inject a single COINBASE SELL command into the NovaTrade Bus outbox.

This injector uses BASE sizing (amount_base) for SELL, which is the safest and most deterministic.

Usage:
  python tools/inject_coinbase_sell_base.py --base-url https://<bus> --secret <OUTBOX_SECRET> \
    --agent edge-primary --symbol BTC/USDC --amount-base 5e-05

Notes:
- Sends side=\"sell\"
- Sets flags=[\"base\"] so the Edge executor treats amount_base as authoritative
- DRYRUN by default unless you pass --live
"""
import argparse
import hashlib
import hmac
import json
import os
import time
from urllib import request as urlrequest

def _hmac_sig(secret: str, body_bytes: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()

def _http_post(url: str, body: dict, secret: str | None, timeout: int = 20):
    body_bytes = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if secret:
        headers["X-NT-Sig"] = _hmac_sig(secret, body_bytes)
    req = urlrequest.Request(url, data=body_bytes, headers=headers, method="POST")
    with urlrequest.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return resp.status, raw

def _print_resp(status: int, raw: str):
    print(f\"HTTP {status}\")
    try:
        obj = json.loads(raw)
        print(json.dumps(obj, indent=2, sort_keys=True))
    except Exception:
        print(raw)

def _make_body(agent: str, cmd: dict, source: str, ts: int):
    return {
        "agent": agent,
        "agent_id": agent,
        "agentId": agent,
        "agent_name": agent,
        "agentName": agent,
        "target_agent": agent,
        "agent_target": agent,
        "command": cmd,
        "meta": {"source": source, "ts": ts},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--secret", default=os.getenv("OUTBOX_SECRET"))
    ap.add_argument("--agent", required=True)
    ap.add_argument("--symbol", default="BTC/USDC")
    ap.add_argument("--amount-base", type=float, default=5e-05)
    ap.add_argument("--venue", default="COINBASE")
    ap.add_argument("--client-order-id", default=None)
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--timeout", type=int, default=20)
    args = ap.parse_args()

    base_url = args.base_url.rstrip("/")
    url = f"{base_url}/ops/enqueue"

    now = int(time.time())
    cid = args.client_order_id or f"sell-{args.venue.lower()}-{args.symbol.replace('/','-').lower()}-base-{int(args.amount_base*1e8)}-{now}"

    cmd = {
        "type": "trade",
        "venue": args.venue,
        "symbol": args.symbol,
        "side": "sell",
        "amount_base": float(args.amount_base),
        "flags": ["base"],
        "dry_run": (not args.live),
        "client_order_id": cid,
        "idempotency_key": cid
    }

    body = _make_body(args.agent, cmd, "inject_coinbase_sell_base.py", now)

    status, raw = _http_post(url, body, args.secret, timeout=args.timeout)
    _print_resp(status, raw)

if __name__ == "__main__":
    main()
