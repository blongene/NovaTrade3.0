# api_commands.py — Command Bus API (cloud)
import os, json, time
from typing import Tuple, Optional, Dict, Any, List
from flask import Blueprint, request, jsonify

from outbox_db import init, pull, ack
from hmac_auth import verify

bp = Blueprint("cmdapi", __name__)

# Secrets / policy
OUTBOX_SECRET = os.getenv("OUTBOX_SECRET", "")
# Comma-separated allow list of agent IDs; default single "orion-local"
AGENTS = set((os.getenv("AGENT_ID") or "orion-local").split(","))


@bp.record_once
def _on_register(setup_state):
    # Ensure DB is ready once at app start
    init()


def _auth_hmac_or_401() -> Optional[Any]:
    """Reject request if HMAC invalid or missing."""
    ts  = request.headers.get("X-Timestamp", "")
    sig = request.headers.get("X-Signature", "")
    body = request.get_data() or b""
    if not (sig and ts and verify(OUTBOX_SECRET, body, ts, sig, ttl_s=300)):
        return jsonify({"error": "unauthorized"}), 401
    return None  # OK


def _auth_agent_or_403() -> Tuple[bool, Any]:
    """
    Returns (True, agent_id) when authorized, otherwise (False, (resp, code)).
    Agent is taken from header X-Agent-ID primarily; falls back to JSON body agent_id.
    """
    hdr_agent = request.headers.get("X-Agent-ID", "") or ""
    try:
        body_agent = (request.get_json(silent=True) or {}).get("agent_id", "") or ""
    except Exception:
        body_agent = ""

    agent = hdr_agent or body_agent
    if not agent:
        return False, (jsonify({"error": "missing agent id"}), 400)

    if AGENTS and agent not in AGENTS:
        return False, (jsonify({"error": "forbidden agent"}), 403)

    # If both header and body provided, require they match to avoid spoofing
    if hdr_agent and body_agent and hdr_agent != body_agent:
        return False, (jsonify({"error": "agent mismatch"}), 400)

    return True, agent


@bp.route("/api/commands/pull", methods=["POST"])
def api_pull():
    # HMAC auth
    resp = _auth_hmac_or_401()
    if resp:
        return resp

    ok, res = _auth_agent_or_403()
    if not ok:
        return res  # (resp, code)
    agent = res

    try:
        data = request.get_json(force=True) or {}
        # Support both "max" and "limit" keys; default 10
        max_n = int(data.get("max", data.get("limit", 10)))
        max_n = max(1, min(max_n, 50))  # sane bounds
        cmds = pull(agent, limit=max_n)  # returns a list[dict]
        return jsonify({"ok": True, "commands": cmds})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/commands/ack", methods=["POST"])
def api_ack():
    # HMAC auth
    resp = _auth_hmac_or_401()
    if resp:
        return resp

    ok, res = _auth_agent_or_403()
    if not ok:
        return res
    agent = res

    try:
        data = request.get_json(force=True) or {}

        # Accept either batch: {"receipts":[ {...}, {...} ]}
        # or single: { "id": "...", "status": "ok|error|expired|rejected", "result": {...}, "txid": "...", ... }
        receipts = data.get("receipts")
        if receipts is None:
            # Normalize single payload to a receipts list
            rid = data.get("id")
            if not rid:
                return jsonify({"error": "missing id"}), 400
            receipts = [{
                "id": rid,
                "status": data.get("status", "ok"),
                "result": data.get("result", {}),
                "txid": data.get("txid"),
                "fills": data.get("fills", []),
                "message": data.get("message"),
                "ts": data.get("ts", int(time.time())),
                "mode": data.get("mode"),   # dryrun|live (optional)
            }]

        # Delegate to outbox layer
        ack(agent, receipts)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
