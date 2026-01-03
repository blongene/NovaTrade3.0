#!/usr/bin/env python3
"""
Inject a single BinanceUS dryrun command into the NovaTrade Bus outbox.

Why this exists:
- Some Bus deployments key commands by `agent_id` (or `agent`) during enqueue.
- To avoid "queued but never pulled", we send ALL common agent fields:
  agent, agent_id, agentId, agent_name, agentName, target_agent.

Usage:
  python tools/inject_binanceus_dryrun.py --base-url https://<bus> --secret <OUTBOX_SECRET> \
    --agent edge-primary --symbol BTCUSDT --side buy --amount 5

Notes:
- amount is interpreted as QUOTE by default (flags=["quote"]).
- This is DRYRUN by default unless you pass --live.
"""
import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from urllib import request as urlrequest

def _hmac_sig(secret: str, body_bytes: bytes) -> str:
    mac = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()
    return mac

def _http_post(url: str, body: dict, secret: str | None, timeout: int = 20):
    body_bytes = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if secret:
        headers["X-NT-Sig"] = _hmac_sig(secret, body_bytes)
    req = urlrequest.Request(url, data=body_bytes, headers=headers, method="POST")
    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return resp.status, raw
    except Exception as e:
        # Try to print any HTTP body if available
        if hasattr(e, "read"):
            try:
                raw = e.read().decode("utf-8", errors="replace")
                return getattr(e, "code", 0), raw
            except Exception:
                pass
        raise

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", required=True, help="Bus base url, e.g. https://novatrade3-0.onrender.com")
    ap.add_argument("--secret", default=os.getenv("OUTBOX_SECRET"), help="OUTBOX_SECRET for ops HMAC. If omitted, no signature header is sent.")
    ap.add_argument("--agent", required=True, help="Target edge agent id/name (must match the Edge pull agent_id)")
    ap.add_argument("--symbol", required=True, help="Symbol, e.g. BTCUSDT or BTCUSD (BinanceUS)")
    ap.add_argument("--side", required=True, choices=["buy","sell"], help="buy or sell")
    ap.add_argument("--amount", required=True, type=float, help="Quote amount (USD/USDT) when flags include 'quote'")
    ap.add_argument("--venue", default="BINANCEUS", help="Venue string; default BINANCEUS")
    ap.add_argument("--client-order-id", default=None, help="Optional stable client id (idempotency). If omitted, one is generated.")
    ap.add_argument("--live", action="store_true", help="If set, will request live execution (NOT recommended until venue audit passes).")
    ap.add_argument("--timeout", type=int, default=20)
    args = ap.parse_args()

    base = args.base_url.rstrip("/")
    url = f"{base}/ops/enqueue"

    now = int(time.time())
    cid = args.client_order_id or f"dryrun-{args.venue.lower()}-{args.symbol.lower()}-{args.side}-{int(args.amount*100)}-{now}"

    cmd = {
        "type": "trade",
        "venue": args.venue,
        "symbol": args.symbol,
        "side": args.side,
        "amount": args.amount,
        "flags": ["quote"],
        "dry_run": (not args.live),
        # Strongly preferred idempotency key field(s)
        "client_order_id": cid,
        "idempotency_key": cid,
    }

    body = {
        # Send all common variants so the Bus can match whatever it expects.
        "agent": args.agent,
        "agent_id": args.agent,
        "agentId": args.agent,
        "agent_name": args.agent,
        "agentName": args.agent,
        "target_agent": args.agent,
        # The actual command
        "command": cmd,
        # Convenience/debug metadata
        "meta": {
            "source": "inject_binanceus_dryrun.py",
            "ts": now,
        }
    }

    status, raw = _http_post(url, body, args.secret, timeout=args.timeout)
    print(f"HTTP {status}")
    try:
        obj = json.loads(raw)
        print(json.dumps(obj, indent=2, sort_keys=True))
    except Exception:
        print(raw)

if __name__ == "__main__":
    main()
