# ops_sign_and_enqueue.py
#
# Dual-use module:
#   1) Bus helper for nova_trigger:
#
#         from ops_sign_and_enqueue import attempt as outbox_attempt
#         result = outbox_attempt(envelope)
#
#      where `envelope` is a dict like:
#
#         {
#             "agent_id": "edge-primary",
#             "intent": {
#                 "type": "order.place",
#                 "venue": "BINANCEUS",
#                 "symbol": "BTC/USDT",
#                 "side": "BUY",
#                 "amount": "25",
#                 ...
#             },
#             "meta": {...},      # optional
#         }
#
#      attempt() reads base URL + secret from env and returns:
#
#         { "ok": bool, "reason": str, "status": int | None }
#
#   2) CLI tester:
#
#         python ops_sign_and_enqueue.py \
#             --base https://novatrade3-0.onrender.com/api \
#             --secret <OUTBOX_SECRET> \
#             --agent edge-primary \
#             --venue BINANCE.US \
#             --symbol BTC/USDT \
#             --side BUY \
#             --amount 25
#
#   NOTE: Requires `requests` in your environment.

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

import requests


# ---------- shared helpers ----------


def now_ms() -> str:
    return str(int(time.time() * 1000))


def hmac_hex(secret: bytes, data: bytes) -> str:
    return hmac.new(secret, data, hashlib.sha256).hexdigest()


def _trial_signatures(secret: bytes, raw_json: bytes, ts: str):
    """Yield (label, headers) for the various signing schemes."""
    body_str = raw_json.decode()
    trials = [
        ("body", hmac_hex(secret, raw_json)),
        ("ts+body", hmac_hex(secret, (ts + body_str).encode())),
        ("body+ts", hmac_hex(secret, (body_str + ts).encode())),
        ("ts:body", hmac_hex(secret, (ts + ":" + body_str).encode())),
        ("ts.body", hmac_hex(secret, (ts + "." + body_str).encode())),
        ("ts\\nbody", hmac_hex(secret, (ts + "\n" + body_str).encode())),
    ]
    for label, sig in trials:
        # IMPORTANT: ops_api_sqlite expects X-Outbox-Signature
        yield label, {
            "Content-Type": "application/json",
            "X-Timestamp": ts,
            "X-Outbox-Signature": sig,
        }


def _attempt_raw(
    url: str,
    secret: bytes,
    body_dict: Dict[str, Any],
    *,
    timeout: float = 15.0,
    verbose: bool = True,
) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]], Optional[int]]:
    """
    Core HTTP logic, shared by CLI and bus.

    Returns (ok, label, json_response, status_code)
    """
    raw = json.dumps(body_dict, separators=(",", ":"), sort_keys=True).encode()
    ts = now_ms()
    last_status: Optional[int] = None

    for label, headers in _trial_signatures(secret, raw, ts):
        try:
            r = requests.post(url, data=raw, headers=headers, timeout=timeout)
            status = r.status_code
            last_status = status
            is_json = r.headers.get("content-type", "").startswith("application/json")
            if verbose:
                if not is_json:
                    snippet = r.text[:200] if r.text else ""
                    print(f"[{label}] HTTP {status} → {snippet}")
            if status == 200 and is_json:
                j = r.json()
                if j.get("ok") is True:
                    if verbose:
                        print(f"[SUCCESS via {label}] id={j.get('id')}, status={status}")
                    return True, label, j, status
                else:
                    if verbose:
                        print(f"[{label}] HTTP 200 but ok!=True → {j}")
            else:
                if verbose and is_json:
                    print(f"[{label}] HTTP {status} → {r.text[:200]}")
        except Exception as err:
            if verbose:
                print(f"[{label}] error: {err}")

    return False, None, None, last_status


# ---------- BUS-SIDE API (for nova_trigger) ----------


def _load_secret_from_env() -> bytes:
    """
    Load OUTBOX secret from either OUTBOX_SECRET_FILE or OUTBOX_SECRET.
    Returns bytes suitable for HMAC.
    """
    path = os.getenv("OUTBOX_SECRET_FILE", "").strip()
    if path:
        try:
            with open(path, "r", encoding="utf-8") as f:
                val = f.read().strip()
                if val:
                    try:
                        return bytes.fromhex(val)
                    except ValueError:
                        return val.encode("utf-8")
        except FileNotFoundError:
            pass  # fall through to env var

    s = os.getenv("OUTBOX_SECRET", "").strip()
    if not s:
        raise RuntimeError("OUTBOX_SECRET / OUTBOX_SECRET_FILE is missing")
    try:
        return bytes.fromhex(s)
    except ValueError:
        return s.encode("utf-8")


def _derive_base_from_env() -> str:
    """
    Prefer OPS_ENQUEUE_BASE; fallback to OPS_ENQUEUE_URL.

    For ops_api_sqlite's /api/ops/enqueue you typically want:
        OPS_ENQUEUE_BASE = https://novatrade3-0.onrender.com/api
    """
    base = os.getenv("OPS_ENQUEUE_BASE", "").strip()
    if base:
        return base.rstrip("/")

    url = os.getenv("OPS_ENQUEUE_URL", "").strip()
    if not url:
        raise RuntimeError("OPS_ENQUEUE_BASE or OPS_ENQUEUE_URL must be set")

    if url.endswith("/ops/enqueue"):
        url = url[: -len("/ops/enqueue")]
    return url.rstrip("/")


def _shape_body_from_envelope(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a high-level `envelope` into the body expected by /ops/enqueue.

    Accepted envelope shapes:

      * Native:
            {"agent_id": ..., "type": ..., "payload": {...}}

      * Intent-based (recommended for nova_trigger):
            {
                "agent_id": "...",
                "intent": {
                    "type": "order.place",
                    "venue": "...",
                    "symbol": "...",
                    "side": "BUY",
                    "amount": "25",
                    ...
                },
                "meta": {...},  # optional, passed through
            }
    """
    if not isinstance(envelope, dict):
        raise RuntimeError("envelope must be a dict")

    agent_id = envelope.get("agent_id") or "edge-primary"

    if "payload" in envelope and "type" in envelope:
        body = {
            "agent_id": agent_id,
            "type": envelope["type"],
            "payload": envelope["payload"],
        }
        if "meta" in envelope:
            body["meta"] = envelope["meta"]
        return body

    intent: Dict[str, Any] = envelope.get("intent") or {}
    cmd_type = intent.get("type") or "order.place"

    body = {
        "agent_id": agent_id,
        "type": cmd_type,
        "payload": intent,
    }
    meta = envelope.get("meta")
    if isinstance(meta, dict):
        body["meta"] = meta
    return body


def attempt(envelope: Dict[str, Any], *, timeout: float = 15.0) -> Dict[str, Any]:
    """
    Bus-facing API used by nova_trigger and future modules.

    Reads base URL and secret from env, shapes the envelope into the
    /ops/enqueue body schema, signs it, and POSTs.

    Returns:

        { "ok": bool, "reason": str, "status": int | None }
    """
    try:
        base = _derive_base_from_env()
    except RuntimeError as e:
        return {"ok": False, "reason": str(e), "status": None}

    url = base.rstrip("/") + "/ops/enqueue"

    try:
        secret = _load_secret_from_env()
    except RuntimeError as e:
        return {"ok": False, "reason": str(e), "status": None}

    try:
        body_dict = _shape_body_from_envelope(envelope)
    except RuntimeError as e:
        return {"ok": False, "reason": str(e), "status": None}

    ok, label, j, status = _attempt_raw(url, secret, body_dict, timeout=timeout, verbose=False)

    if not ok:
        return {
            "ok": False,
            "reason": f"enqueue_failed (label={label}, status={status})",
            "status": status,
        }

    reason = j.get("reason") if isinstance(j, dict) else "ok"
    if not reason:
        reason = "ok"

    return {"ok": True, "reason": reason, "status": status}


# ---------- CLI entrypoint ----------


def cli_attempt(base: str, secret_str: str, body_dict: Dict[str, Any]) -> None:
    """
    Retains the old CLI semantics:
      - prints detailed results
      - exits with code 0/2 based on success
    """
    secret = secret_str.encode()
    url = base.rstrip("/") + "/ops/enqueue"

    ok, label, j, status = _attempt_raw(url, secret, body_dict, timeout=15.0, verbose=True)
    if not ok:
        print(
            "All signing patterns failed. Check OUTBOX_SECRET, time sync, "
            "and that /ops/enqueue is deployed."
        )
        sys.exit(2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="Base URL (e.g., https://novatrade3-0.onrender.com/api)")
    ap.add_argument("--secret", required=True, help="OUTBOX_SECRET")
    ap.add_argument("--agent", required=True, help="agent_id (e.g., edge-cb-1)")
    ap.add_argument(
        "--venue",
        required=True,
        choices=["COINBASE", "COINBASE_ADVANCED", "BINANCE.US", "MEXC", "KRAKEN"],
    )
    ap.add_argument("--symbol", required=True, help="e.g., BTC/USDT")
    ap.add_argument("--side", required=True, choices=["BUY", "SELL"])
    ap.add_argument("--amount", required=True, help="numeric string (quote or base per executor config)")
    ap.add_argument("--tif", default="IOC")
    args = ap.parse_args()

    body = {
        "agent_id": args.agent,
        "type": "order.place",
        "payload": {
            "venue": args.venue,
            "symbol": args.symbol,
            "side": args.side,
            "amount": str(args.amount),
            "time_in_force": args.tif,
        },
    }
    cli_attempt(args.base, args.secret, body)


if __name__ == "__main__":
    main()
