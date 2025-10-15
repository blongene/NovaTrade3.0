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

def _normalize_symbol(sym: str) -> str:
    """
    Accepts 'BTC/USDT' (preferred) or 'BTCUSDT' and normalizes to 'BASE/QUOTE'.
    """
    s = (sym or "").upper().strip()
    if not s:
        return ""
    if "/" in s:
        return s
    # naïve split for majors; safe for our current pairs
    if s.endswith(("USDT", "USDC")) and len(s) > 4:
        return f"{s[:-4]}/{s[-4:]}"
    if len(s) >= 6:
        return f"{s[:3]}/{s[3:]}"
    return s

def _normalize_payload(p: Dict[str, Any]) -> Tuple[Dict[str, Any], str | None]:
    """
    Validate + normalize an order.place payload.

    Required:
      - agent handled outside
      - venue, symbol, side
      - BUY:  amount (quote spend) OR quote_amount  (exactly one > 0)
      - SELL: base_amount (base size)               (required > 0)
      - mode: MARKET|LIMIT (default MARKET)

    Optional:
      - price (LIMIT only), client_order_id, dedupe_key, not_before
    """
    norm: Dict[str, Any] = {}

    # Required strings
    venue = (p.get("venue") or "").strip().upper()
    symbol = _normalize_symbol(p.get("symbol") or "")
    side = (p.get("side") or "").strip().upper()
    if not venue:
        return {}, "missing field: venue"
    if not symbol:
        return {}, "missing field: symbol"
    if side not in {"BUY", "SELL"}:
        return {}, "invalid side (use BUY or SELL)"
    norm["venue"], norm["symbol"], norm["side"] = venue, symbol, side

    # Mode
    mode = (p.get("mode") or "MARKET").strip().upper()
    if mode not in {"MARKET", "LIMIT"}:
        return {}, "invalid mode (use MARKET or LIMIT)"
    norm["mode"] = mode

    # Amount semantics by side
    has_amount = "amount" in p and p.get("amount") not in (None, "")
    has_quote  = "quote_amount" in p and p.get("quote_amount") not in (None, "")
    has_base   = "base_amount" in p and p.get("base_amount") not in (None, "")

    if side == "BUY":
        # BUY requires quote spend; allow amount OR quote_amount (but not both)
        if not (has_amount or has_quote):
            return {}, "missing field: amount OR quote_amount"
        if has_amount and has_quote:
            return {}, "provide only one of: amount or quote_amount"
        try:
            spend = float(p["amount"] if has_amount else p["quote_amount"])
            if spend <= 0:
                return {}, "amount/quote_amount must be > 0"
        except Exception:
            return {}, "amount/quote_amount must be numeric"
        # normalize to 'amount' in payload for downstream simplicity
        norm["amount"] = spend
        # ignore any accidental base_amount on BUY
    else:
        # SELL requires base_amount only
        if not has_base:
            return {}, "missing field: base_amount (>0) for SELL"
        # reject if user also passed amount/quote_amount to avoid ambiguity
        if has_amount or has_quote:
            return {}, "SELL must not include amount/quote_amount (use base_amount)"
        try:
            base_amt = float(p["base_amount"])
            if base_amt <= 0:
                return {}, "base_amount must be > 0 for SELL"
        except Exception:
            return {}, "base_amount must be numeric"
        norm["base_amount"] = base_amt

    # Optional fields passthrough
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
        "required_common": ["agent", "venue", "symbol", "side"],
        "required_buy": ["amount OR quote_amount"],
        "required_sell": ["base_amount"],
        "optional": ["mode=MARKET|LIMIT", "price (LIMIT only)", "client_order_id", "dedupe_key", "not_before"],
        "examples": {
            "BUY_market": {
                "agent": "edge-cb-1",
                "venue": "COINBASE",
                "symbol": "BTC/USDC",
                "side": "BUY",
                "mode": "MARKET",
                "amount": 10.0
            },
            "SELL_market": {
                "agent": "edge-cb-1",
                "venue": "BINANCEUS",
                "symbol": "BTC/USDT",
                "side": "SELL",
                "mode": "MARKET",
                "base_amount": 0.00009
            }
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
