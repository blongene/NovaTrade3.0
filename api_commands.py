# api_commands.py — Command Bus: /api/commands/pull, /ack  (aligned to outbox_db.py)
import os, time
from flask import Blueprint, request, jsonify
from outbox_db import init as db_init, pull as db_pull, ack as db_ack, enqueue as db_enqueue, reap_expired
from hmac_auth import require_hmac
from db_backbone import record_command_enqueued, record_receipt

bp = Blueprint("api_commands", __name__, url_prefix="/api/commands")

REQUIRE_HMAC_PULL = os.getenv("REQUIRE_HMAC_PULL", "0").strip().lower() in {"1","true","yes"}
# ACK always uses HMAC via require_hmac below

ALLOW = {a.strip() for a in (os.getenv("OUTBOX_AGENT_ALLOW") or os.getenv("AGENT_ID") or "").split(",") if a.strip()}

try:
    db_init()
except Exception as err:
    print(f"[API] outbox db init skipped: {err}")

def _agent_allowed(aid: str) -> bool:
    return True if not ALLOW else (aid in ALLOW)

@bp.post("/pull")
def pull():
    if REQUIRE_HMAC_PULL:
        ok, err = require_hmac(request)
        if not ok:
            return jsonify(error=err), 401

    body = request.get_json(silent=True) or {}
    agent_id = (body.get("agent_id") or "").strip()
    if not agent_id:
        return jsonify(error="missing agent_id"), 400
    if not _agent_allowed(agent_id):
        return jsonify(error="agent not allowed"), 403

    limit = int(body.get("max") or 5)
    lease_s = int(os.getenv("OUTBOX_LEASE_S", "120"))

    try:
        try:
            reap_expired(int(time.time()))
        except Exception:
            pass

        rows = db_pull(agent_id=agent_id, limit=limit, lease_s=lease_s)
        items = []
        for r in rows:
            items.append({
                "id": r["id"],
                "type": r.get("kind") or "order.place",
                "payload": r.get("payload") or {},
            })
        return jsonify(items), 200
    except Exception as err:
        return jsonify(error=str(err)), 500

@bp.post("/ack")
def ack():
    ok, err = require_hmac(request)
    if not ok:
        return jsonify(ok=False, error=err), 401

    body = request.get_json(force=True)
    agent_id = (body.get("agent_id") or "").strip()
    if not agent_id:
        return jsonify(ok=False, error="missing agent_id"), 400
    if not _agent_allowed(agent_id):
        return jsonify(ok=False, error="agent not allowed"), 403

    cmd_id = body.get("id")
    if not cmd_id:
        return jsonify(ok=False, error="missing id"), 400

    try:
        receipt = {
            "id": int(cmd_id),
            "ok": bool(body.get("ok")),
            "status": body.get("status"),
            "txid": body.get("txid"),
            "message": body.get("message"),
            "result": body.get("result") or {},
        }

        # 1) canonical edge → SQLite outbox (current behaviour)
        db_ack(agent_id=agent_id, receipts=[receipt])

        # 2) mirror into Postgres backbone (Phase 19)
        try:
            record_receipt(agent_id, int(cmd_id), receipt, ok=receipt["ok"])
        except Exception:
            # never break ACK on DB backbone failure
            pass

        return jsonify(ok=True), 200

    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500
