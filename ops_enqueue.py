# ops_enqueue.py — enqueue one row per agent with proper payload
import os, json
from flask import Blueprint, request, jsonify
from hmac_auth import require_hmac
from outbox_db import init as db_init, enqueue as db_enqueue

bp = Blueprint("ops_enqueue", __name__, url_prefix="/ops")

REQUIRE_HMAC_OPS = os.getenv("REQUIRE_HMAC_OPS", "1").strip().lower() in {"1","true","yes"}

# idempotent init
try:
    db_init()
except Exception as e:
    print(f"[OPS] outbox init skipped: {e}")

def _norm_order(body: dict) -> dict:
    """Normalize incoming fields to the edge-friendly payload."""
    venue  = (body.get("venue")  or "").strip().upper()
    symbol = (body.get("symbol") or "").strip().upper()
    side   = (body.get("side")   or "").strip().upper()
    mode   = (body.get("mode")   or "MARKET").strip().upper()

    # support amount or quote_amount
    amt  = body.get("amount")
    qamt = body.get("quote_amount")
    payload = {
        "venue": venue,
        "symbol": symbol,
        "side": side,
        "mode": mode,
    }
    if qamt is not None:
        payload["quote_amount"] = float(qamt)
    elif amt is not None:
        payload["amount"] = float(amt)
    return payload

@bp.post("/enqueue")
def enqueue():
    if REQUIRE_HMAC_OPS:
        ok, err = require_hmac(request)
        if not ok:
            return jsonify(error=err), 401

    body = request.get_json(force=True) or {}
    # agents: prefer 'agent_id', else 'agents' (comma list)
    agents_str = (body.get("agent_id") or body.get("agents") or "").strip()
    agents = [a.strip() for a in agents_str.split(",") if a.strip()]
    if not agents:
        return jsonify(error="agent_id (or agents) required"), 400

    payload = _norm_order(body)
    # basic validation
    for k in ("venue","symbol","side"):
        if not payload.get(k):
            return jsonify(error=f"missing field: {k}"), 400

    # enqueue one command per agent (dedupe key optional)
    not_before = int(body.get("not_before") or 0)
    dedupe_key = (body.get("dedupe_key") or "").strip() or None

    ids = []
    for agent in agents:
        cid = db_enqueue(agent_id=agent, kind="order.place",
                         payload=payload, not_before=not_before,
                         dedupe_key=dedupe_key)
        ids.append({"agent": agent, "id": cid})

    return jsonify(ok=True, items=ids), 200
