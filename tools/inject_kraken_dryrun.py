#!/usr/bin/env python3
"""
Inject a single Kraken dryrun trade command into the NovaTrade Bus outbox.
"""
import argparse, hashlib, hmac, json, os, time
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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--secret", default=os.getenv("OUTBOX_SECRET"))
    ap.add_argument("--agent", required=True)
    ap.add_argument("--symbol", default="XBT/USDT")
    ap.add_argument("--side", choices=["buy","sell"], default="buy")
    ap.add_argument("--amount", type=float, default=5.0)
    ap.add_argument("--venue", default="KRAKEN")
    ap.add_argument("--client-order-id", default=None)
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--timeout", type=int, default=20)
    args = ap.parse_args()

    base = args.base_url.rstrip("/")
    url = f"{base}/ops/enqueue"

    now = int(time.time())
    cid = args.client_order_id or f"dryrun-{args.venue.lower()}-{args.symbol.replace('/','-').lower()}-{args.side}-{int(args.amount*100)}-{now}"

    cmd = {
        "type": "trade",
        "venue": args.venue,
        "symbol": args.symbol,
        "side": args.side,
        "amount": args.amount,
        "flags": ["quote"],
        "dry_run": (not args.live),
        "client_order_id": cid,
        "idempotency_key": cid,
    }

    body = {
        "agent": args.agent,
        "agent_id": args.agent,
        "agentId": args.agent,
        "agent_name": args.agent,
        "agentName": args.agent,
        "target_agent": args.agent,
        "command": cmd,
        "meta": {"source": "inject_kraken_dryrun.py", "ts": now},
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
