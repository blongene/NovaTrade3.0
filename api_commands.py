# api_commands.py — Command Bus API (cloud)
import os, json, time
from flask import Blueprint, request, jsonify
from outbox_db import init, pull, ack
from hmac_auth import verify

bp = Blueprint("cmdapi", __name__)
SECRET = os.getenv("OUTBOX_SECRET", "")
AGENTS = set((os.getenv("AGENT_ID") or "orion-local").split(","))  # allow list

@bp.before_app_first_request
def _init():
    init()

def _auth_or_403():
    agent = request.headers.get("X-Agent-ID", "")
    ts    = request.headers.get("X-Timestamp", "")
    sig   = request.headers.get("X-Signature", "")
    if not agent or agent not in AGENTS:
        return False, ("forbidden", 403)
    body = request.get_data() or b""
    if not (sig and ts and verify(SECRET, body, ts, ttl_s=300)):
        return False, ("unauthorized", 401)
    return True, agent

@bp.route("/api/commands/pull", methods=["POST"])
def api_pull():
    ok, res = _auth_or_403()
    if not ok:
        msg, code = res
        return jsonify({"error": msg}), code
    agent = res
    try:
        data = request.get_json(force=True) or {}
        limit = int(data.get("limit", 10))
        cmds = pull(agent, limit=limit)
        return jsonify({"ok": True, "commands": cmds})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@bp.route("/api/commands/ack", methods=["POST"])
def api_ack():
    ok, res = _auth_or_403()
    if not ok:
        msg, code = res
        return jsonify({"error": msg}), code
    agent = res
    try:
        data = request.get_json(force=True) or {}
        receipts = data.get("receipts", [])
        ack(agent, receipts)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
