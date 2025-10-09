# ops_enqueue.py — strict enqueue endpoint for NovaTrade 3.0
# Routes:
#   POST /ops/enqueue        -> enqueue a validated command (HMAC required if enabled)
#   GET  /ops/enqueue/schema -> quick schema help (no auth)
#
# Env:
#   OUTBOX_AGENT_ALLOW   comma list of allowed agent IDs (optional)
#   REQUIRE_HMAC_OPS     1/true to require HMAC for /ops/*
#   OUTBOX_LEASE_S       default lease seconds (not used here; DB controls)
#
# Depends:
#   outbox_db.enqueue(agent_id, kind, payload, not_before=0, dedupe_key=None) -> int
#   hmac_auth.require_hmac(request) -> (ok: bool, err: str)

from __future__ import annotations
import os, json
from typing import Any, Dict, Tuple
from flask import Blueprint, request, jsonify

# DB + HMAC helpers (already in your repo)
from outbox_db import enqueue as db_enqueue
from hmac_auth import require_hmac

bp = Blueprint("ops", __name__, url_prefix="/ops")

# ---------- Config ----------
REQUIRE_HMAC = os.getenv("REQUIRE_HMAC_OPS", "1").strip().lower() in {"1", "true", "yes"}
ALLOW = {a.strip() for a in (os.getenv("OUTBOX_AGENT_ALLOW") or "").split(",") if a.strip()}

# ---------- Utilities ----------
def _err(status: int, msg: str, extra: Dict[str, Any] | None = None):
    body = {"ok": False, "error": msg}
    if extra:
        body.update(extra)
    return jsonify(body), status

def _as_json() -> Tuple[Dict[str, Any], bool]:
    """
    Load JSON body safely from application/json or form-encoded 'json'/'payload'.
    Returns (dict, ok).
    """
    ctype = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    if ctype == "application/json":
        try:
            return request.get_json(force=True) or {}, True
        except Exception:
            return {}, False
    # allow simple form submits for quick tests
    if ctype in {"application/x-www-form-urlencoded", "multipart/form-data"}:
        raw = request.form.get("json") or request.form.get("payload") or ""
        try:
            return (json.loads(raw) if raw else {}), True
        except Exception:
            return {}, False
    # last-ditch: body as json
    try:
        return json.loads((request.get_data() or b"{}").decode("utf-8") or "{}"), True
    except Exception:
        return {}, False

def _require_agent(agent_id: str) -> Tuple[bool, str]:
    if not agent_id:
        return False, "missing field: agent"
    if ALLOW and agent_id not in ALLOW:
        return False, f"agent not allowed: {agent_id}"
    return True, ""

def _normalize_payload(p: Dict[str, Any]) -> Tuple[Dict[str, Any], str | None]:
    """
    Validate + normalize an order.place payload.
    Required: venue, symbol, side, (amount or quote_amount), mode (MARKET/market default)
    Returns (normalized_payload, error_or_None)
    """
    norm: Dict[str, Any] = {}

    # Required strings
    for k in ("venue", "symbol", "side"):
        v = (p.get(k) or "").strip()
        if not v:
            return {}, f"missing field: {k}"
        norm[k] = v.upper() if k in ("venue", "side") else v  # symbol keep case like 'BTC/USDT'

    # Mode
    mode = (p.get("mode") or "MARKET").strip().upper()
    if mode not in {"MARKET", "LIMIT"}:
        return {}, "invalid mode (use MARKET or LIMIT)"
    norm["mode"] = mode

    # Amount vs quote_amount (exactly one required)
    has_amount = "amount" in p and p.get("amount") not in (None, "")
    has_quote  = "quote_amount" in p and p.get("quote_amount") not in (None, "")
    if not (has_amount or has_quote):
        return {}, "missing field: amount OR quote_amount"
    if has_amount and has_quote:
        return {}, "provide only one of: amount or quote_amount"

    if has_amount:
        try:
            amt = float(p["amount"])
            if amt <= 0:
                return {}, "amount must be > 0"
            norm["amount"] = amt
        except Exception:
            return {}, "amount must be numeric"
    else:
        try:
            qa = float(p["quote_amount"])
            if qa <= 0:
                return {}, "quote_amount must be > 0"
            norm["quote_amount"] = qa
        except Exception:
            return {}, "quote_amount must be numeric"

    # Optional fields passthrough (client_order_id, price, dedupe_key, not_before)
    if p.get("client_order_id"):
        norm["client_order_id"] = str(p["client_order_id"])[:64]
    if mode == "LIMIT":
        try:
            price = float(p.get("price"))
            if price <= 0:
                return {}, "price must be > 0 for LIMIT orders"
            norm["price"] = price
        except Exception:
            return {}, "invalid price for LIMIT order"
    if p.get("dedupe_key"):
        norm["dedupe_key"] = str(p["dedupe_key"])[:100]
    if p.get("not_before"):
        try:
            nb = int(p["not_before"])
            if nb < 0:
                return {}, "not_before must be epoch seconds"
            norm["not_before"] = nb
        except Exception:
            return {}, "invalid not_before (epoch seconds)"

    return norm, None

# ---------- Routes ----------
@bp.get("/enqueue/schema")
def schema():
    return jsonify({
        "ok": True,
        "kind": "order.place",
        "required": ["agent", "venue", "symbol", "side", "amount | quote_amount"],
        "optional": ["mode=MARKET|LIMIT", "price (LIMIT only)", "client_order_id", "dedupe_key", "not_before"],
        "example": {
            "agent": "edge-cb-1",
            "venue": "COINBASE",
            "symbol": "BTC/USDT",
            "side": "BUY",
            "mode": "MARKET",
            "amount": 10.0
        }
    })

@bp.post("/enqueue")
def enqueue():
    # HMAC (optional but recommended)
    if REQUIRE_HMAC:
        ok, err = require_hmac(request)
        if not ok:
            return _err(401, err)

    # Parse + validate
    body, ok = _as_json()
    if not ok:
        return _err(400, "malformed JSON body")

    agent = (body.get("agent") or body.get("agent_id") or "").strip()
    ok, msg = _require_agent(agent)
    if not ok:
        return _err(403, msg)

    # normalize payload
    payload, perr = _normalize_payload(body)
    if perr:
        return _err(400, perr)

    # enqueue
    kind = "order.place"
    dedupe_key = payload.pop("dedupe_key", None)
    not_before = int(payload.pop("not_before", 0) or 0)
    try:
        cid = db_enqueue(agent_id=agent, kind=kind, payload=payload, not_before=not_before, dedupe_key=dedupe_key)
        if cid == -1:
            return jsonify(ok=True, dedup=True), 200
        return jsonify(ok=True, id=cid, agent=agent), 200
    except Exception as e:
        return _err(500, f"enqueue failed: {e!s}")

