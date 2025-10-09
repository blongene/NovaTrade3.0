# api_commands.py — Command Bus for NovaTrade (compatible with your outbox_db)
import os, json, time
from flask import Blueprint, request, jsonify
from hmac_auth import require_hmac
import outbox_db as db

bp = Blueprint("api_commands", __name__, url_prefix="/api/commands")

# Config: require HMAC on pull? (ACK always requires HMAC)
REQUIRE_HMAC_PULL = os.getenv("REQUIRE_HMAC_PULL", "0").strip().lower() in {"1","true","yes"}
# Allow list for pull/ack. Prefer OUTBOX_AGENT_ALLOW; else AGENT_ID list; else allow-all (setup)
_allow_env = (os.getenv("OUTBOX_AGENT_ALLOW") or os.getenv("AGENT_ID") or "").strip()
if _allow_env:
    AGENTS = {a.strip() for a in _allow_env.split(",") if a.strip()}
    ALLOW_ALL = False
else:
    AGENTS = set()
    ALLOW_ALL = True

def _agent_ok(agent_id: str) -> bool:
    return ALLOW_ALL or agent_id in AGENTS

# Ensure schema exists (idempotent)
try:
    db.init()
except Exception as err:
    print(f"[API] outbox db init skipped: {err}")

@bp.post("/pull")
def pull():
    # Optional HMAC
    if REQUIRE_HMAC_PULL:
        ok, err = require_hmac(request)
        if not ok:
            return jsonify(error=err), 401

    body = request.get_json(silent=True) or {}
    agent_id = (body.get("agent_id") or "").strip() or "edge-unknown"
    limit    = int(body.get("max") or 5)
    lease_s  = int(os.getenv("OUTBOX_LEASE_S", "45"))

    try:
        # Return expired leases to pending (optional hygiene)
        try:
            db.reap_expired()
        except Exception as e:
            print(f"[API] reap_expired skipped: {e}")

        rows = db.pull(agent_id=agent_id, limit=limit, lease_s=lease_s) or []
        # Normalize for Edge Agent: id, type, payload, ttl
        items = []
        for r in rows:
            items.append({
                "id": r["id"],
                "type": r.get("kind") or "order.place",
                "payload": r.get("payload") or {},   # outbox_db already dict-ified
                "ttl_s": lease_s
            })
        return jsonify(items), 200
    except Exception as err:
        return jsonify(error=str(err)), 500

@bp.post("/ack")
def ack():
    # HMAC required
    ok, err = require_hmac(request)
    if not ok:
        return jsonify(error=err), 401

    body = request.get_json(force=True)
    agent_id = (body.get("agent_id") or "").strip() or "edge-unknown"

    # Edge sends a single result; outbox_db.ack expects a list
    receipt = {
        "id": int(body.get("id")),
        "ok": (body.get("status") == "ok"),
        "status": body.get("status"),
        "txid": body.get("txid"),
        "message": body.get("message"),
        "result": {
            "fills": body.get("fills") or [],
            "ts": body.get("ts"),
            "hmac": body.get("hmac")
        }
    }
    try:
        db.ack(agent_id=agent_id, receipts=[receipt])
        return jsonify(ok=True), 200
    except Exception as err:
        return jsonify(ok=False, error=str(err)), 500
