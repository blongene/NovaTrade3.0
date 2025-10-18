# ops_enqueue.py — strict enqueue endpoint for NovaTrade 3.0
from __future__ import annotations
import os, json
from typing import Any, Dict, Tuple
from flask import Blueprint, request, jsonify
from outbox_db import enqueue as db_enqueue
from hmac_auth import require_hmac

bp = Blueprint("ops", __name__, url_prefix="/ops")

REQUIRE_HMAC = os.getenv("REQUIRE_HMAC_OPS", "1").strip().lower() in {"1","true","yes"}
ALLOW = {a.strip() for a in (os.getenv("OUTBOX_AGENT_ALLOW") or "").split(",") if a.strip()}
PAIR_GUARD_MODE = (os.getenv("PAIR_GUARD_MODE") or "rewrite").strip().lower()  # rewrite|warn|off
OUTBOX_SECRET_FILE=/etc/secrets/outbox_secret

def _err(status: int, msg: str, extra: Dict[str, Any] | None = None):
    body = {"ok": False, "error": msg}
    if extra: body.update(extra)
    return jsonify(body), status

def _as_json() -> Tuple[Dict[str, Any], bool]:
    ctype = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    if ctype == "application/json":
        try: return request.get_json(force=True) or {}, True
        except Exception: return {}, False
    if ctype in {"application/x-www-form-urlencoded","multipart/form-data"}:
        raw = request.form.get("json") or request.form.get("payload") or ""
        try: return (json.loads(raw) if raw else {}), True
        except Exception: return {}, False
    try: return json.loads((request.get_data() or b"{}").decode("utf-8") or "{}"), True
    except Exception: return {}, False

def _require_agent(agent_id: str) -> Tuple[bool, str]:
    if not agent_id: return False, "missing field: agent"
    if ALLOW and agent_id not in ALLOW: return False, f"agent not allowed: {agent_id}"
    return True, ""

def _normalize_symbol(sym: str) -> str:
    s = (sym or "").upper().replace(" ", "")
    if not s: return ""
    if "/" in s: base, quote = s.split("/", 1); return f"{base}/{quote}"
    if "-" in s: base, quote = s.split("-", 1); return f"{base}/{quote}"
    # naive fallback: BTCUSDT -> BTC/USDT
    if len(s) >= 6:
        return f"{s[:-4]}/{s[-4:]}"
    return s

def _pair_guard(venue: str, requested_symbol: str) -> Tuple[str, Dict[str, Any]]:
    """Potentially rewrite BINANCEUS …/USDC -> …/USDT (policy), return (resolved, extras)."""
    extras: Dict[str, Any] = {"requested_symbol": requested_symbol}
    resolved = requested_symbol
    if venue == "BINANCEUS":
        if requested_symbol.endswith("/USDC"):
            if PAIR_GUARD_MODE == "rewrite":
                resolved = requested_symbol[:-4] + "USDT"
                extras["pair_guard"] = "rewritten_USDC_to_USDT"
            elif PAIR_GUARD_MODE == "warn":
                extras["pair_guard"] = "warn_USDC_on_BINANCEUS"
            else:
                extras["pair_guard"] = "off"
    extras["resolved_symbol"] = resolved
    return resolved, extras

def _normalize_payload(p: Dict[str, Any]) -> Tuple[Dict[str, Any], str | None]:
    norm: Dict[str, Any] = {}

    venue = (p.get("venue") or "").strip().upper()
    if not venue: return {}, "missing field: venue"
    requested_symbol = _normalize_symbol(p.get("symbol") or "")
    if not requested_symbol: return {}, "missing field: symbol"
    side = (p.get("side") or "").strip().upper()
    if side not in {"BUY","SELL"}: return {}, "invalid side (use BUY or SELL)"

    resolved_symbol, extras = _pair_guard(venue, requested_symbol)

    mode = (p.get("mode") or "MARKET").strip().upper()
    if mode not in {"MARKET","LIMIT"}: return {}, "invalid mode (use MARKET or LIMIT)"

    # BUY vs SELL semantics
    has_amount = "amount" in p and p.get("amount") not in (None, "")
    has_quote  = "quote_amount" in p and p.get("quote_amount") not in (None, "")
    has_base   = "base_amount" in p and p.get("base_amount") not in (None, "")

    if side == "BUY":
        if not (has_amount or has_quote):
            return {}, "missing field: amount OR quote_amount"
        if has_amount and has_quote:
            return {}, "provide only one of: amount or quote_amount"
        try:
            spend = float(p["amount"] if has_amount else p["quote_amount"])
            if spend <= 0: return {}, "amount/quote_amount must be > 0"
        except Exception:
            return {}, "amount/quote_amount must be numeric"
        norm["amount"] = spend
    else:
        if not has_base: return {}, "missing field: base_amount (>0) for SELL"
        if has_amount or has_quote:
            return {}, "SELL must not include amount/quote_amount (use base_amount)"
        try:
            base_amt = float(p["base_amount"])
            if base_amt <= 0: return {}, "base_amount must be > 0 for SELL"
        except Exception:
            return {}, "base_amount must be numeric"
        norm["base_amount"] = base_amt

    # passthroughs
    if p.get("client_order_id"): norm["client_order_id"] = str(p["client_order_id"])[:64]
    if mode == "LIMIT":
        try:
            price = float(p.get("price")); 
            if price <= 0: return {}, "price must be > 0 for LIMIT orders"
            norm["price"] = price
        except Exception: return {}, "invalid price for LIMIT order"
    if p.get("dedupe_key"): norm["dedupe_key"] = str(p["dedupe_key"])[:100]
    if p.get("not_before"):
        try:
            nb = int(p["not_before"]); 
            if nb < 0: return {}, "not_before must be epoch seconds"
            norm["not_before"] = nb
        except Exception: return {}, "invalid not_before (epoch seconds)"

    # finalize
    norm.update({
        "venue": venue,
        "symbol": resolved_symbol,
        "side": side,
        "mode": mode,
        "requested_symbol": extras["requested_symbol"],
        "resolved_symbol":  extras["resolved_symbol"],
    })
    if "pair_guard" in extras: norm["pair_guard"] = extras["pair_guard"]
    return norm, None

@bp.get("/enqueue/schema")
def schema():
    return jsonify({
        "ok": True,
        "kind": "order.place",
        "pair_guard_mode": PAIR_GUARD_MODE,
        "required_common": ["agent","venue","symbol","side"],
        "required_buy": ["amount OR quote_amount"],
        "required_sell": ["base_amount"],
        "notes": ["BINANCEUS: USDT quote preferred; USDC auto-rewritten if PAIR_GUARD_MODE=rewrite"],
    })

@bp.post("/enqueue")
def enqueue():
    if REQUIRE_HMAC:
        ok, err = require_hmac(request)
        if not ok: return _err(401, err)

    body, ok = _as_json()
    if not ok: return _err(400, "malformed JSON body")

    agent = (body.get("agent") or body.get("agent_id") or "").strip()
    ok, msg = _require_agent(agent)
    if not ok: return _err(403, msg)

    payload, perr = _normalize_payload(body)
    if perr: return _err(400, perr)

    kind = "order.place"
    dedupe_key = payload.pop("dedupe_key", None)
    not_before = int(payload.pop("not_before", 0) or 0)
    try:
        cid = db_enqueue(agent_id=agent, kind=kind, payload=payload, not_before=not_before, dedupe_key=dedupe_key)
        if cid == -1: return jsonify(ok=True, dedup=True), 200
        return jsonify(ok=True, id=cid, agent=agent, pair_guard=payload.get("pair_guard")), 200
    except Exception as e:
        return _err(500, f"enqueue failed: {e!s}")
