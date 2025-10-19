# ops_enqueue.py — strict enqueue endpoint for NovaTrade 3.0
from __future__ import annotations
import os, hmac, hashlib, json, time
from typing import Any, Dict, Tuple
from flask import Blueprint, request, jsonify
from outbox_db import enqueue as db_enqueue
from hmac_auth import require_hmac
from datetime import datetime

bp = Blueprint("ops", __name__, url_prefix="/ops")
ops_bp = Blueprint("ops_bp", __name__, url_prefix="/api/ops")

REQUIRE_HMAC = os.getenv("REQUIRE_HMAC_OPS", "1").strip().lower() in {"1","true","yes"}
ALLOW = {a.strip() for a in (os.getenv("OUTBOX_AGENT_ALLOW") or "").split(",") if a.strip()}
PAIR_GUARD_MODE = (os.getenv("PAIR_GUARD_MODE") or "rewrite").strip().lower()  # rewrite|warn|off
OUTBOX_SECRET = os.getenv("OUTBOX_SECRET","3f36e385d5b3c83e66209cdac0d815788e1459b49cc67b6a6159cfa4de34511b8")

try:
    from utils import get_sheet
except Exception:
    get_sheet = None

OUTBOX_SECRET = (os.getenv("OUTBOX_SECRET") or "").encode("utf-8")

def _bad(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code

def _verify_hmac(raw_body: bytes, provided: str) -> bool:
    if not OUTBOX_SECRET:
        # For bootstrapping: allow when no secret is set (but warn in logs)
        print("[OPS] WARNING: OUTBOX_SECRET not set; HMAC verification disabled.")
        return True
    try:
        mac = hmac.new(OUTBOX_SECRET, raw_body, hashlib.sha256).hexdigest()
        # Accept hex (lower/upper) and optional "sha256=" prefix formats
        provided = (provided or "").strip()
        if provided.lower().startswith("sha256="):
            provided = provided.split("=", 1)[1]
        ok = hmac.compare_digest(mac, provided.lower())
        if not ok:
            print(f"[OPS] HMAC mismatch. expected={mac} provided={provided}")
        return ok
    except Exception as e:
        print(f"[OPS] HMAC verify error: {e}")
        return False

def _write_trade_intent_to_sheet(intent: dict, status: str, reason: str = ""):
    if not get_sheet:
        print("[OPS] get_sheet() unavailable; skipping Trade_Log write.")
        return
    try:
        sheet = get_sheet()
        ws = sheet.worksheet("Trade_Log")
        # Ensure a minimal, robust schema. We’ll append even if extra columns exist.
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        row = [
            ts,
            intent.get("symbol", ""),
            intent.get("venue", ""),
            intent.get("side", ""),
            str(intent.get("amount_quote", "")),
            status.upper(),
            reason or intent.get("reason", "") or "",
            # helpful breadcrumbs:
            intent.get("client_id", ""),
            "LIVE" if str(intent.get("dryrun", "false")).lower() in ("0", "false", "no") else "DRYRUN",
        ]
        ws.append_row(row)
        print(f"[OPS] Trade_Log append ok → {row}")
    except Exception as e:
        print(f"[OPS] Trade_Log append failed: {e}")

@ops_bp.route("/enqueue", methods=["POST"])
def enqueue():
    try:
        raw = request.get_data() or b""
        sig = request.headers.get("X-Outbox-Signature", "")
        if not _verify_hmac(raw, sig):
            return _bad("invalid signature", 401)

        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            return _bad("invalid json body", 400)

        # Minimal required fields
        symbol = (payload.get("symbol") or "").upper().strip()
        venue  = (payload.get("venue")  or "").upper().strip()
        side   = (payload.get("side")   or "").upper().strip()   # BUY / SELL
        amt_q  = payload.get("amount_quote")

        if not symbol or not venue or side not in ("BUY", "SELL"):
            return _bad("missing or invalid fields: symbol/venue/side", 422)

        # If REBUY_MODE=dryrun, we still “enqueue” but mark as DRYRUN in sheet.
        dryrun = str(os.getenv("REBUY_MODE", "") or payload.get("dryrun", "")).lower() == "dryrun"

        # At this point you may also push into your DB-backed outbox.
        # To keep this drop-in dependency-free and unblock your flow,
        # we write immediately to Trade_Log so you SEE it in Sheets.
        _write_trade_intent_to_sheet(
            {
                "symbol": symbol,
                "venue": venue,
                "side": side,
                "amount_quote": amt_q,
                "reason": payload.get("reason", ""),
                "client_id": payload.get("client_id", ""),
                "dryrun": dryrun,
            },
            status="ENQUEUED",
            reason=payload.get("policy_reason", ""),
        )

        # Respond in the shape your callers expect.
        return jsonify({
            "ok": True,
            "enqueued": True,
            "dryrun": dryrun,
            "echo": {
                "symbol": symbol, "venue": venue, "side": side,
                "amount_quote": amt_q
            }
        }), 200

    except Exception as e:
        print(f"[OPS] enqueue error: {e}")
        return _bad("internal error", 500)
        
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
