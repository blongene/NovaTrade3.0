# ops_enqueue.py — simple ops helper to enqueue test commands (HMAC-protected)
import os, json, time
from flask import Blueprint, request, jsonify
from hmac_auth import require_hmac
from outbox_db import enqueue

bp = Blueprint("ops", __name__, url_prefix="/ops")

@bp.post("/enqueue")
def ops_enqueue():
    ok, err = require_hmac(request)
    if not ok:
        return jsonify(error=err), 401

    data = request.get_json(silent=True) or {}

    agent_id   = data.get("agent_id")   or os.getenv("AGENT_ID", "edge-nl-1")
    kind       = data.get("type")       or "order.place"
    payload    = data.get("payload")    or {}
    not_before = int(data.get("not_before") or 0)
    dedupe_key = data.get("dedupe_key") or None  # optional

    try:
        cmd_id = enqueue(agent_id=agent_id, kind=kind, payload=payload, not_before=not_before, dedupe_key=dedupe_key)
        return jsonify(ok=True, id=cmd_id)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500
