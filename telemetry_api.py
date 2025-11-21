# telemetry_api.py — Bus endpoints for telemetry & heartbeat (HMAC-protected, schema-lenient)
# FULL PRODUCTION DROP-IN: Robust HMAC + All Original Logic Preserved.
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import logging
import re
from typing import Any, Dict, Optional, Tuple

from flask import Blueprint, jsonify, request

# Persistence helpers (existing in your repo)
try:
    import telemetry_store  # write-side
except ImportError:
    telemetry_store = None

try:
    import telemetry_read   # read-side
except ImportError:
    telemetry_read = None

bp = Blueprint("telemetry", __name__, url_prefix="/api")

REQUIRE_HMAC_TELEM = os.getenv("REQUIRE_HMAC_TELEMETRY", "1").lower() in {"1", "true", "yes"}

# --------------------------
# HMAC verification (ROBUST)
# --------------------------

def _sanitize_secret(raw):
    cleaned = ''.join(ch for ch in raw if ch.lower() in '0123456789abcdef').lower()[:64]
    if len(cleaned) != 64:
        logging.warning(f"[HMAC] Secret length mismatch: {len(cleaned)} (expected 64)")
    return cleaned.encode()

def _get_sig_from_headers() -> str:
    """
    Accept any of these headers:
      - X-Nova-Signature
      - X-NT-Sig
      - X-Edge-Signature
      - X-Signature
      - X-Hub-Signature-256: sha256=<digest>
    """
    sig = (
        request.headers.get("X-Nova-Signature")
        or request.headers.get("X-NT-Sig")
        or request.headers.get("X-Edge-Signature")
        or request.headers.get("X-Signature")
        or request.headers.get("X-Hub-Signature-256")
        or ""
    ).strip()
    if sig.lower().startswith("sha256="):
        sig = sig.split("=", 1)[1].strip()
    return sig

def _digests_match(expected_hex: str, provided: str) -> bool:
    """
    Compare server-computed hex digest with provided value in either hex or base64.
    """
    # Provided as hex?
    try:
        int(provided, 16)
        if hmac.compare_digest(expected_hex, provided.lower()):
            return True
    except Exception:
        pass

    # Provided as base64?
    try:
        provided_bytes = base64.b64decode(provided, validate=True)
        provided_hex = provided_bytes.hex()
        if hmac.compare_digest(expected_hex, provided_hex):
            return True
    except Exception:
        pass
        
    return False

def _verify_hmac_robust(body: bytes) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Robust HMAC Verification:
    1. Checks multiple secrets (TELEMETRY, OUTBOX, EDGE).
    2. Checks multiple formats (Raw Bytes AND Canonical Sorted JSON).
    """
    if not REQUIRE_HMAC_TELEM:
        return True, None

    provided = _get_sig_from_headers()
    if not provided:
        return False, {"ok": False, "error": "missing_signature"}

    # Prepare candidates
    secrets = [
        os.getenv("TELEMETRY_SECRET", ""),
        os.getenv("OUTBOX_SECRET", ""),
        os.getenv("EDGE_SECRET", ""),
    ]
    # Clean and deduplicate
    clean_secrets = []
    seen = set()
    for s in secrets:
        if s:
            clean = _sanitize_secret(s)
            if clean not in seen:
                clean_secrets.append(clean)
                seen.add(clean)

    if not clean_secrets:
        logging.warning("[HMAC] No secrets configured for telemetry verification.")
        return False, {"ok": False, "error": "server_misconfiguration"}

    # 1. Check RAW bytes (Fast path - assumes sender signed exact payload)
    raw_bytes = body or b""
    for secret_bytes in clean_secrets:
        expected_raw = hmac.new(secret_bytes, raw_bytes, hashlib.sha256).hexdigest()
        if _digests_match(expected_raw, provided):
            return True, None

    # 2. Check CANONICAL bytes (Robust path - handles serialization drift)
    try:
        data = json.loads(raw_bytes.decode("utf-8") or "{}")
        canonical_bytes = json.dumps(
            data, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        
        for secret_bytes in clean_secrets:
            expected_canon = hmac.new(secret_bytes, canonical_bytes, hashlib.sha256).hexdigest()
            if _digests_match(expected_canon, provided):
                return True, None
    except Exception:
        pass

    return False, {"ok": False, "error": "invalid_signature"}

# --------------------------
# Parsing / normalization
# --------------------------

def _json_body() -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Load raw request body as JSON (strict), returning (dict, error_json).
    """
    try:
        raw = request.get_data(cache=False)  # exact bytes for HMAC
        # HMAC is checked by caller; here we only parse
        body = json.loads(raw.decode("utf-8") or "{}")
        if not isinstance(body, dict):
            return None, {"ok": False, "error": "malformed_json"}
        return body, None
    except Exception:
        return None, {"ok": False, "error": "malformed_json"}

def _normalize_payload(body: Dict[str, Any]) -> Tuple[str, Dict[str, Any], Dict[str, Any], int]:
    """
    Accepts either:
      { "agent": "...", "flat": {...}, "by_venue": {...}, "ts": 123 }
      { "telemetry": { ...same fields... } }
      and also supports "agent_id".
    Coerces numeric strings to floats in venue maps.
    """
    root = body.get("telemetry") if isinstance(body.get("telemetry"), dict) else body

    agent = root.get("agent") or root.get("agent_id") or "edge"
    ts = int(root.get("ts") or time.time())

    flat = root.get("flat") or {}
    by_venue = root.get("by_venue") or {}

    # Coerce venue numeric strings to floats (USD/USDC/etc.)
    for v, m in list(by_venue.items()):
        if not isinstance(m, dict):
            continue
        by_venue[v] = {k: _to_number(val) for k, val in m.items()}

    return agent, flat, by_venue, ts

def _to_number(x: Any) -> Any:
    try:
        # Keep ints as ints where possible for nicer display
        f = float(x)
        i = int(f)
        return i if f == i else f
    except Exception:
        return x

# --------------------------
# Persistence shim (store)
# --------------------------

def _persist(agent: str, flat: Dict[str, Any], by_venue: Dict[str, Any], ts: int) -> None:
    """
    Call through to whichever function your telemetry_store provides.
    Tries common names; no-ops if none found (won't crash).
    """
    if not telemetry_store:
        return

    try_order = [
        "store_last",
        "save_last",
        "set_last",
        "write_last",
        "push_balances",  # some builds use this with same args
    ]
    for fn in try_order:
        if hasattr(telemetry_store, fn):
            try:
                getattr(telemetry_store, fn)(agent=agent, flat=flat, by_venue=by_venue, ts=ts)
                return
            except TypeError:
                # Fallback: positional call if the store signature is positional
                try:
                    getattr(telemetry_store, fn)(agent, flat, by_venue, ts)
                    return
                except Exception:
                    pass
            except Exception:
                pass

    # As a last resort, store via a generic setter if present
    if hasattr(telemetry_store, "store"):
        try:
            telemetry_store.store({"agent": agent, "flat": flat, "by_venue": by_venue, "ts": ts})
            return
        except Exception:
            pass

# --------------------------
# Routes
# --------------------------

def _ok(**kw):
    data = {"ok": True}
    data.update(kw)
    return jsonify(data)

def _err(status: int, msg: str):
    return jsonify({"ok": False, "error": msg}), status

@bp.post("/telemetry/push_balances")
def telemetry_push_balances():
    # Robust HMAC
    ok, err = _verify_hmac_robust(request.get_data(cache=False))
    if not ok:
        return _err(401, err["error"])

    # Parse + normalize
    body, jerr = _json_body()
    if jerr:
        return _err(400, jerr["error"])
    agent, flat, by_venue, ts = _normalize_payload(body)

    # Persist
    _persist(agent, flat, by_venue, ts)
    received = sum(len(m) for m in by_venue.values() if isinstance(m, dict))
    return _ok(received=received)

@bp.post("/telemetry/push")
def telemetry_push():
    # Robust HMAC
    ok, err = _verify_hmac_robust(request.get_data(cache=False))
    if not ok:
        return _err(401, err["error"])

    # Parse + normalize
    body, jerr = _json_body()
    if jerr:
        return _err(400, jerr["error"])
    agent, flat, by_venue, ts = _normalize_payload(body)

    # Persist
    _persist(agent, flat, by_venue, ts)
    received = len(flat) + sum(len(m) for m in by_venue.values() if isinstance(m, dict))
    return _ok(received=received)

@bp.post("/edge/balances")
def edge_balances_alias():
    """
    Alias kept for compatibility.
    - If REQUIRE_HMAC_TELEMETRY=1, normal Robust HMAC rules apply.
    - If disabled, allows `?secret=` in query for quick testing.
    """
    if REQUIRE_HMAC_TELEM:
        ok, err = _verify_hmac_robust(request.get_data(cache=False))
        if not ok:
            return _err(401, err["error"])
    else:
        secret_q = request.args.get("secret", "")
        any_secret = os.getenv("TELEMETRY_SECRET") or os.getenv("OUTBOX_SECRET") or os.getenv("EDGE_SECRET") or ""
        if any_secret and secret_q != any_secret:
            # Fallback to robust HMAC if query param fails
            ok, err = _verify_hmac_robust(request.get_data(cache=False))
            if not ok:
                return _err(401, err["error"])

    body, jerr = _json_body()
    if jerr:
        return _err(400, jerr["error"])
    agent, flat, by_venue, ts = _normalize_payload(body)

    _persist(agent, flat, by_venue, ts)
    received = sum(len(m) for m in by_venue.values() if isinstance(m, dict))
    return _ok(received=received)
