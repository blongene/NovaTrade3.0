# api_commands.py — Command Bus API (cloud)
import os, json, time
from flask import Blueprint, request, jsonify
from outbox_db import init, pull, ack
from hmac_auth import verify  # verify(secret, body_bytes, timestamp, signature, ttl_s=...)

bp = Blueprint("cmdapi", __name__)

OUTBOX_SECRET = os.getenv("OUTBOX_SECRET", "")
# Comma-separated allow-list of agents; defaults to single agent 'orion-local'
AGENTS = {a.strip() for a in (os.getenv("AGENT_ID") or "orion-local").split(",") if a.strip()}

@bp.record_once
def _on_register(_):
    init()

def _require_auth():
    """
    Returns (agent, None) on success, or (None, (json_response, http_code)) on failure.
    Auth = HMAC of raw body + timestamp header + allowed agent id.
    """
    agent = request.headers.get("X-Agent-ID", "").strip()
    ts    = request.headers.get("X-Timestamp", "").strip()
    sig   = request.headers.get("X-Signature", "").strip()
    body  = request.get_data() or b""

    if not agent:
        return None, (jsonify({"error": "missing agent id"}), 400)
    if agent not in AGENTS:
        return None, (jsonify({"error": "forbidden agent"}), 403)
    if not (sig and ts and verify(OUTBOX_SECRET, body, ts, sig, ttl_s=300)):
        return None, (jsonify({"error": "unauthorized"}), 401)
    return agent, None

@bp.route("/api/commands/pull", methods=["POST"])
def api_pull():
    agent, err = _require_auth()
    if err: return err

    try:
        data  = request.get_json(force=True) or {}
        limit = int(data.get("limit", 10))
        cmds  = pull(agent, limit=limit)
        return jsonify({"ok": True, "agent": agent, "commands": cmds})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@bp.route("/api/commands/ack", methods=["POST"])
def api_ack():
    agent, err = _require_auth()
    if err: return err

    try:
        data = request.get_json(force=True) or {}
        receipts = data.get("receipts", [])
        if not isinstance(receipts, list):
            return jsonify({"ok": False, "error": "receipts must be a list"}), 400
        ack(agent, receipts)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
