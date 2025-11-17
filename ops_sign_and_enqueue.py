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
#                 "amount": 25,
#             },
#         }
#
#      which will be converted into the canonical Outbox payload:
#
#         {
#             "payload": {
#                 "agent_id": "edge-primary",
#                 "type": "order.place",
#                 "payload": {...},
#                 "meta": {...optional...},
#             }
#         }
#
#   2) CLI helper:
#
#         python ops_sign_and_enqueue.py \
#             --base https://novatrade3-0.onrender.com \
#             --secret $OUTBOX_SECRET \
#             --agent edge-primary \
#             --venue BINANCEUS \
#             --symbol BTCUSDT \
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


# ---------- HMAC helpers ----------


def _load_secret_from_env() -> bytes:
    """
    Load OUTBOX_SECRET from either OUTBOX_SECRET or OUTBOX_SECRET_FILE.
    """
    s = os.getenv("OUTBOX_SECRET") or ""
    path = os.getenv("OUTBOX_SECRET_FILE")
    if not s and path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            s = f.read().strip()
    if not s:
        raise RuntimeError("OUTBOX_SECRET or OUTBOX_SECRET_FILE must be set")
    return s.encode("utf-8")


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

    We try multiple signing formats; the Bus will accept whichever
    matches its HMAC policy (if any). If none match, we fail.
    """
    raw_json = json.dumps(body_dict, separators=(",", ":"), ensure_ascii=False).encode()
    ts = str(int(time.time()))
    last_status: Optional[int] = None
    last_body: Optional[Dict[str, Any]] = None

    for label, headers in _trial_signatures(secret, raw_json, ts):
        try:
            r = requests.post(url, data=raw_json, headers=headers, timeout=timeout)
            last_status = r.status_code
            try:
                last_body = r.json()
            except Exception:
                last_body = {"raw": r.text}
            if verbose:
                print(f"[{label}] status={r.status_code} body={last_body}")
            # if bus replies with ok or 4xx, treat as definitive
            if r.status_code < 500:
                return True, label, last_body, last_status
        except Exception as e:
            if verbose:
                print(f"[{label}] request failed: {e}")
            last_body = {"error": str(e)}

    # all attempts failed
    return False, "all_failed", last_body, last_status


# ---------- Envelope → payload ----------


def _derive_base_from_env() -> str:
    """Resolve the Outbox base URL.

    Priority:
      1) OPS_ENQUEUE_BASE
      2) OPS_BASE_URL
      3) Derive from OPS_ENQUEUE_URL (backwards-compat).

    Normalized for Bus wsgi.py which exposes POST /ops/enqueue (no /api prefix).
    """
    # Highest priority: explicit base override
    base = os.getenv("OPS_ENQUEUE_BASE", "").strip()
    if not base:
        # Next: general Bus base (e.g., https://novatrade3-0.onrender.com)
        base = os.getenv("OPS_BASE_URL", "").strip()
    if base:
        return base.rstrip("/")

    url = os.getenv("OPS_ENQUEUE_URL", "").strip()
    if not url:
        raise RuntimeError("OPS_ENQUEUE_BASE, OPS_BASE_URL or OPS_ENQUEUE_URL must be set")

    # Legacy patterns:
    #   https://host/api/ops/enqueue  -> strip /api/ops/enqueue
    #   https://host/ops/enqueue      -> strip /ops/enqueue
    for suffix in ("/api/ops/enqueue", "/ops/enqueue"):
        if url.endswith(suffix):
            url = url[: -len(suffix)]
            break
    # Also tolerate bare '/api' base
    if url.endswith("/api"):
        url = url[:-4]
    return url.rstrip("/")


def _shape_body_from_envelope(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a high-level `envelope` into the payload expected by /ops/enqueue.

    Accepted envelope shapes:

      * Native:
            {"agent_id": ..., "type": ..., "payload": {...}, "meta": {...}?}

      * Intent-based:
            {
              "agent_id": "edge-primary",
              "intent": {
                  "type": "order.place",
                  "venue": "BINANCEUS",
                  "symbol": "BTC/USDT",
                  "side": "BUY",
                  "amount": 25,
                  ...
              },
              "meta": {...optional...}
            }

    Returns the canonical *payload* dict (not yet wrapped in {"payload": ...}).
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
    if not isinstance(intent, dict):
        raise RuntimeError("envelope.intent must be a dict")

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
    """Bus-facing API used by nova_trigger and future modules.

    Reads base URL and secret from env, shapes the envelope into the
    /ops/enqueue payload schema, signs it, and POSTs.

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
        payload = _shape_body_from_envelope(envelope)
        body_dict = {"payload": payload}
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


def cli_attempt(base: str, secret_str: str, payload: Dict[str, Any]) -> None:
    """
    CLI helper used by __main__.

    Wraps the high-level payload into the /ops/enqueue schema
    and prints verbose diagnostics.
    """
    secret = secret_str.encode()
    url = base.rstrip("/") + "/ops/enqueue"
    body_dict = {"payload": payload}
    ok, label, j, status = _attempt_raw(url, secret, body_dict, timeout=15.0, verbose=True)
    if not ok:
        print(
            "All signing patterns failed. Check OUTBOX_SECRET, time sync, "
            "and that /ops/enqueue is deployed.",
        )
        sys.exit(2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="Base URL (e.g., https://novatrade3-0.onrender.com)")
    ap.add_argument("--secret", required=True, help="Outbox secret (string)")
    ap.add_argument("--agent", default="edge-primary")
    ap.add_argument("--venue", required=True)
    ap.add_argument("--symbol", required=True)
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
