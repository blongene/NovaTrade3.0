# ops_sign_and_enqueue.py
#
# Shared helper for signing and enqueuing /ops commands.
# Two use cases:
#  1) CLI:
#       python ops_sign_and_enqueue.py --base https://your-app.onrender.com \
#           --secret <OUTBOX_SECRET> --agent edge-primary \
#           --venue BINANCEUS --symbol BTC/USDT --side BUY --amount 25
#
#  2) In-bus:
#       from ops_sign_and_enqueue import attempt
#       res = attempt({"agent_id": "...", "intent": {...}})
#       if res.get("ok"): ...
#
# Env used in bus mode:
#   OUTBOX_BASE_URL or CLOUD_BASE_URL  – base URL of the bus (e.g. https://novatrade3-0.onrender.com)
#   OUTBOX_SECRET                      – shared HMAC secret

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from typing import Any, Dict, Tuple

import requests


def now_ms() -> str:
    return str(int(time.time() * 1000))


def hmac_hex(secret: bytes, data: bytes) -> str:
    return hmac.new(secret, data, hashlib.sha256).hexdigest()


# ---------- core HTTP signer ---------------------------------------------

def _attempt_http(base: str, secret: bytes, body_dict: Dict[str, Any]) -> Tuple[bool, str, Any, int]:
    """
    Low-level HTTP helper. This is what both the CLI and the bus wrapper use.

    Returns: (ok, label, json_or_text, status_code)
    """
    if not base or not secret:
        return False, "missing_config", {
            "error": "missing_base_or_secret",
            "has_base": bool(base),
            "has_secret": bool(secret),
        }, 0

    url = base.rstrip("/") + "/ops/enqueue"
    raw = json.dumps(body_dict, separators=(",", ":"), sort_keys=True).encode()
    ts = now_ms()

    # Try a few common signing styles – server will accept whichever matches.
    trials = [
        ("body", hmac_hex(secret, raw)),
        ("ts+body", hmac_hex(secret, (ts + raw.decode()).encode())),
        ("body+ts", hmac_hex(secret, (raw.decode() + ts).encode())),
        ("ts:body", hmac_hex(secret, (ts + ":" + raw.decode()).encode())),
        ("ts.body", hmac_hex(secret, (ts + "." + raw.decode()).encode())),
        ("ts\\nbody", hmac_hex(secret, (ts + "\n" + raw.decode()).encode())),
    ]

    last_label = "none"
    last_payload: Any = None
    last_status = 0

    for label, sig in trials:
        headers = {
            "Content-Type": "application/json",
            "X-OUTBOX-SIGNATURE": sig,
            "X-OUTBOX-TIMESTAMP": ts,
            "X-OUTBOX-SIGN-FLAVOR": label,
        }
        try:
            r = requests.post(url, data=raw, headers=headers, timeout=10)
            last_label = label
            last_status = r.status_code
            try:
                payload = r.json()
            except ValueError:
                payload = r.text

            last_payload = payload

            if r.ok and isinstance(payload, dict) and payload.get("ok"):
                # success
                return True, label, payload, r.status_code
        except Exception as err:
            last_payload = {"error": str(err)}
            last_label = label
            last_status = 0

    return False, last_label, last_payload, last_status


# ---------- bus-facing wrapper -------------------------------------------

# These envs are read once at import; if you change them, redeploy/restart.
_OUTBOX_BASE = (
    os.getenv("OUTBOX_BASE_URL")
    or os.getenv("CLOUD_BASE_URL")
    or os.getenv("BUS_BASE_URL")
)
_OUTBOX_SECRET = (os.getenv("OUTBOX_SECRET") or "").strip().encode() if os.getenv("OUTBOX_SECRET") else b""


def attempt(body_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bus-friendly API used by nova_trigger.

    Example return:
        {
            "ok": True/False,
            "label": "ts:body",
            "response": {...} or "raw text",
            "status": 200
        }
    """
    ok, label, payload, status = _attempt_http(_OUTBOX_BASE, _OUTBOX_SECRET, body_dict)
    return {
        "ok": ok,
        "label": label,
        "response": payload,
        "status": status,
    }


# ---------- CLI entrypoint -----------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="Base URL (e.g., https://novatrade3-0.onrender.com)")
    ap.add_argument("--secret", required=True, help="OUTBOX_SECRET (hex or plain)")
    ap.add_argument("--agent", required=True, help="agent_id (e.g., edge-primary)")
    ap.add_argument(
        "--venue",
        required=True,
        choices=["COINBASE", "COINBASE_ADVANCED", "BINANCEUS", "MEXC", "KRAKEN"],
    )
    ap.add_argument("--symbol", required=True, help="e.g., BTC/USDT")
    ap.add_argument("--side", required=True, choices=["BUY", "SELL"])
    ap.add_argument("--amount", required=True, type=float, help="Notional in quote")
    ap.add_argument("--tif", default="GTC", help="Time in force (default GTC)")
    args = ap.parse_args()

    secret = args.secret.encode()

    body = {
        "agent_id": args.agent,
        "intent": {
            "type": "manual_rebuy",
            "venue": args.venue,
            "symbol": args.symbol,
            "side": args.side,
            "amount": str(args.amount),
            "time_in_force": args.tif,
        },
    }

    ok, label, payload, status = _attempt_http(args.base, secret, body)
    print(f"Trial label={label} status={status}")
    print(json.dumps(payload, indent=2))

    if not ok:
        print(
            "All signing patterns failed. "
            "Check OUTBOX_SECRET, time sync, and that /ops/enqueue is deployed."
        )
        sys.exit(2)


if __name__ == "__main__":
    main()
