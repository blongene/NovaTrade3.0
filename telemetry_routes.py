# telemetry_routes.py — Bus-side telemetry ingestion for NovaTrade 3.0
#
# Handles:
#   - /api/telemetry/heartbeat   (Edge → Bus)
#   - /api/telemetry/push        (Edge → Bus)
#
# Accepts HMAC-signed JSON from telemetry_sync.py on the Edge.
# Stores last-known telemetry in memory (for fast reads),
# and mirrors into Sheets + DB if configured.

from __future__ import annotations

import os
import hmac
import hashlib
import time
from flask import Blueprint, request, jsonify
from typing import Dict, Any

# ---- Imports from utils.py --------------------------------------------------

try:
    from utils import warn, info, get_ws, sheets_append_rows
except Exception:
    def warn(msg): print(f"[WARN] {msg}")
    def info(msg): print(f"[INFO] {msg}")
    def get_ws(name): raise RuntimeError("utils.get_ws unavailable")
    def sheets_append_rows(*a, **k): pass

# -----------------------------------------------------------------------------

bp_telemetry = Blueprint("bp_telemetry", __name__)

TELEMETRY_SECRET = (
    os.getenv("TELEMETRY_SECRET")
    or os.getenv("OUTBOX_SECRET")
    or ""
)

TELEMETRY_LOG_SHEET = os.getenv("TELEMETRY_LOG_WS", "Telemetry_Log")
HEARTBEAT_LOG_SHEET = os.getenv("HEARTBEAT_LOG_WS", "Heartbeat_Log")

# In-memory latest state (the Bus uses this as Unified Snapshot feeder)
_last_balances: Dict[str, Any] = {}
_last_heartbeat: Dict[str, Any] = {}
_last_aggregates: Dict[str, Any] = {}
_last_telemetry_ts: float = 0.0


# -----------------------------------------------------------------------------
# HMAC verification
# -----------------------------------------------------------------------------
def verify_signature(body: Dict[str, Any], signature: str) -> bool:
    if not TELEMETRY_SECRET:
        return True  # If no secret set, accept all (debug mode)
    if not signature:
        return False
    payload = (
        __import__("json")
        .dumps(body, separators=(",", ":"), sort_keys=True)
        .encode("utf-8")
    )
    expected = hmac.new(
        TELEMETRY_SECRET.encode("utf-8"),
        payload,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


# -----------------------------------------------------------------------------
# /api/telemetry/heartbeat  (Edge → Bus)
# -----------------------------------------------------------------------------
@bp_telemetry.route("/api/telemetry/heartbeat", methods=["POST"])
def telemetry_heartbeat():
    global _last_heartbeat

    body = request.get_json(force=True, silent=True) or {}
    sig = request.headers.get("X-Nova-Signature", "")

    if not verify_signature(body, sig):
        warn(f"heartbeat: invalid signature from agent={body.get('agent')}")
        return jsonify({"ok": False, "error": "bad_signature"}), 403

    agent = body.get("agent") or "unknown"
    ts = int(body.get("ts") or time.time())
    latency = float(body.get("latency_ms") or 0.0)

    _last_heartbeat = {
        "agent": agent,
        "ts": ts,
        "latency_ms": latency,
    }

    info(f"heartbeat ok from {agent} latency={latency}ms")

    # Optional: mirror to Sheets
    try:
        ws = get_ws(HEARTBEAT_LOG_SHEET)
        ws.append_row(
            [
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)),
                agent,
                latency,
            ],
            value_input_option="USER_ENTERED",
        )
    except Exception as e:
        warn(f"heartbeat sheet append failed: {e}")

    return jsonify({"ok": True, "age_sec": time.time() - ts})


# -----------------------------------------------------------------------------
# /api/telemetry/push  (Edge → Bus)
# -----------------------------------------------------------------------------
@bp_telemetry.route("/api/telemetry/push", methods=["POST"])
def telemetry_push():
    global _last_balances, _last_aggregates, _last_telemetry_ts

    body = request.get_json(force=True, silent=True) or {}
    sig = request.headers.get("X-Nova-Signature", "")

    if not verify_signature(body, sig):
        warn(f"telemetry: invalid signature from agent={body.get('agent')}")
        return jsonify({"ok": False, "error": "bad_signature"}), 403

    agent = body.get("agent") or "unknown"
    ts = int(body.get("ts") or time.time())
    aggregates = body.get("aggregates") or {}
    last_balances = (aggregates.get("last_balances") or {})

    _last_aggregates = aggregates
    _last_balances = last_balances
    _last_telemetry_ts = ts

    info(
        f"telemetry: balances from {agent} venues="
        f"{list(last_balances.keys())} ts={ts}"
    )

    # Optional mirror to Sheets
    try:
        ws = get_ws(TELEMETRY_LOG_SHEET)
        rows = []
        for venue, assets in last_balances.items():
            for asset, amount in assets.items():
                rows.append(
                    [
                        time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)),
                        agent,
                        venue,
                        asset,
                        amount,
                    ]
                )
        if rows:
            sheets_append_rows(ws, rows)
    except Exception as e:
        warn(f"telemetry sheet append failed: {e}")

    return jsonify({"ok": True})


# -----------------------------------------------------------------------------
# Public accessors (for Unified_Snapshot, dashboards, etc.)
# -----------------------------------------------------------------------------
def get_latest_balances() -> Dict[str, Any]:
    return dict(_last_balances)


def get_latest_heartbeat() -> Dict[str, Any]:
    return dict(_last_heartbeat)


def get_latest_aggregates() -> Dict[str, Any]:
    return dict(_last_aggregates)


def get_telemetry_age_sec() -> float:
    if not _last_telemetry_ts:
        return 9e9  # effectively "infinite"
    return time.time() - _last_telemetry_ts
