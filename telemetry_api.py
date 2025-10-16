# telemetry_api.py — Bus endpoints for telemetry & heartbeat (HMAC-protected)
from __future__ import annotations
import os, json, time
from typing import Dict, Any
from flask import Blueprint, request, jsonify

from hmac_auth import require_hmac  # same helper you already use elsewhere
import telemetry_store              # tiny persistence on the Bus (below)

bp = Blueprint("telemetry", __name__, url_prefix="/api")

REQUIRE_HMAC_TELEM = (os.getenv("REQUIRE_HMAC_TELEMETRY", "1").lower() in {"1","true","yes"})

def _err(status: int, msg: str):
    return jsonify({"ok": False, "error": msg}), status

@bp.post("/telemetry/push")
def push():
    if REQUIRE_HMAC_TELEM:
        ok, err = require_hmac(request)
        if not ok: return _err(401, err)
    try:
        body = request.get_json(force=True)
    except Exception:
        return _err(400, "malformed JSON body")

    agent = (body or {}).get("agent") or ""
    ts = int((body or {}).get("ts") or time.time())
    aggs = (body or {}).get("aggregates") or {}
    if not agent:
        return _err(400, "missing agent")
    telemetry_store.store_push(agent=agent, ts=ts, aggregates=aggs)
    return jsonify({"ok": True})

@bp.post("/heartbeat")
def heartbeat():
    if REQUIRE_HMAC_TELEM:
        ok, err = require_hmac(request)
        if not ok: return _err(401, err)
    try:
        body = request.get_json(force=True)
    except Exception:
        return _err(400, "malformed JSON body")

    agent = (body or {}).get("agent") or ""
    ts = int((body or {}).get("ts") or time.time())
    latency = int((body or {}).get("latency_ms") or 0)
    if not agent:
        return _err(400, "missing agent")
    telemetry_store.store_heartbeat(agent=agent, ts=ts, latency_ms=latency)
    return jsonify({"ok": True})
