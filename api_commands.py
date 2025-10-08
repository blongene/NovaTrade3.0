# api_commands.py — Command Bus blueprint: /api/commands/pull, /ack
import os, json
from flask import Blueprint, request, jsonify

# Storage & HMAC
from outbox_db import init as db_init, pull_pending_for_agent, set_inflight_lease, ack_receipt, enqueue  # adjust if your functions differ
from hmac_auth import require_hmac

bp = Blueprint("api_commands", __name__, url_prefix="/api/commands")

# Config: require HMAC on pull? (ACK always requires HMAC)
REQUIRE_HMAC_PULL = os.getenv("REQUIRE_HMAC_PULL", "0").strip().lower() in {"1","true","yes"}

# Ensure DB tables exist (idempotent)
try:
    db_init()
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
    agent = (body.get("agent_id") or "").strip() or "edge-unknown"
    max_n = int(body.get("max") or 5)

    try:
        # Expire leases inside your DB layer (if supported) or here; keeping it simple:
        cmds = pull_pending_for_agent(agent_id=agent, limit=max_n)
        # Set leases (lease TTL from env or a default)
        ttl_s = int(os.getenv("OUTBOX_LEASE_S", "120"))
        items = []
        for c in cmds:
            # Expect row dict like: {"id":..., "type":..., "payload":..., "hmac":...}
            cid = c["id"]
            set_inflight_lease(cid, ttl_s)
            items.append({
                "id": cid,
                "type": c.get("type") or "order.place",
                "payload": json.loads(c.get("payload") or "{}"),
                "hmac": c.get("hmac"),
                "ttl_s": ttl_s
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
    try:
        rid = ack_receipt(
            cmd_id=body.get("id"),
            agent_id=body.get("agent_id"),
            status=body.get("status"),
            txid=body.get("txid"),
            fills=body.get("fills") or [],
            message=body.get("message"),
            ts=body.get("ts"),
            hmac=body.get("hmac")
        )
        return jsonify(ok=True, receipt_id=rid), 200
    except Exception as err:
        return jsonify(ok=False, error=str(err)), 500
