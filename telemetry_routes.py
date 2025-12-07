# telemetry_routes.py — Bus-side telemetry ingestion for NovaTrade 3.0
#
# Handles:
#   - /api/heartbeat          (Edge → Bus)
#   - /api/telemetry/push     (Edge → Bus)
#
# Accepts HMAC-signed JSON from telemetry_sync.py on the Edge.
# Stores last-known telemetry in memory (for fast reads),
# and mirrors into Sheets if configured.

from __future__ import annotations

import os
import hmac
import hashlib
import time
from typing import Dict, Any, Optional

from flask import Blueprint, request, jsonify

try:
    from utils import warn, info, get_ws, sheets_append_rows, SHEET_URL
except Exception:  # pragma: no cover
    def warn(msg): print(f"[WARN] {msg}")
    def info(msg): print(f"[INFO] {msg}")
    def get_ws(name): raise RuntimeError("utils.get_ws unavailable")
    def sheets_append_rows(*a, **k): pass
    SHEET_URL = ""

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
    """
    Edge (telemetry_sync.py) signs JSON body with TELEMETRY_SECRET and sends
    it in the 'X-Signature' header.
    """
    if not TELEMETRY_SECRET:
        # If no secret set, accept all — useful in dev, but set TELEMETRY_SECRET in prod.
        return True
    if not signature:
        return False
    import json
    payload = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    expected = hmac.new(
        TELEMETRY_SECRET.encode("utf-8"), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


# -----------------------------------------------------------------------------
# /api/heartbeat  (Edge → Bus)
# -----------------------------------------------------------------------------
@bp_telemetry.route("/api/heartbeat", methods=["POST"])
def telemetry_heartbeat():
    """
    Heartbeat endpoint for Edge Agent.

    Expected JSON:
      {
        "agent": "edge-primary",
        "ts": 1699999999,
        "latency_ms": 123.4
      }
    """
    global _last_heartbeat

    body = request.get_json(force=True, silent=True) or {}
    sig = request.headers.get("X-Signature", "")

    if not verify_signature(body, sig):
        warn(f"heartbeat: invalid signature from agent={body.get('agent')}")
        return jsonify({"ok": False, "error": "bad_signature"}), 403

    agent = body.get("agent") or "unknown"
    ts = int(body.get("ts") or time.time())
    latency_ms = float(body.get("latency_ms") or 0.0)

    _last_heartbeat = {
        "agent": agent,
        "ts": ts,
        "latency_ms": latency_ms,
    }

    info(f"heartbeat ok from {agent} latency={latency_ms}ms")

    # Optional: mirror to Sheets (best effort, non-fatal)
    try:
        if SHEET_URL:
            ws = get_ws(HEARTBEAT_LOG_SHEET)
            ws.append_row(
                [
                    time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)),
                    agent,
                    latency_ms,
                ],
                value_input_option="USER_ENTERED",
            )
    except Exception as e:
        warn(f"heartbeat sheet append failed (non-fatal): {e}")

    return jsonify({"ok": True, "age_sec": max(0.0, time.time() - ts)})


# -----------------------------------------------------------------------------
# /api/telemetry/push  (Edge → Bus)
# -----------------------------------------------------------------------------
@bp_telemetry.route("/api/telemetry/push", methods=["POST"])
def telemetry_push():
    """
    Ingest telemetry snapshots from Edge Agent.

    Expected JSON (schema-lenient, but typically):
      {
        "agent": "edge-primary",
        "ts": 1699999999,
        "aggregates": {
          "last_balances": { "BINANCEUS": { "USDT": 123.45, ... }, ... },
          ... other aggregates ...
        }
      }
    """
    global _last_balances, _last_aggregates, _last_telemetry_ts

    import json as _json

    body = request.get_json(force=True, silent=True) or {}
    sig = request.headers.get("X-Signature", "")

    if not verify_signature(body, sig):
        warn(f"telemetry: invalid signature from agent={body.get('agent')}")
        return jsonify({"ok": False, "error": "bad_signature"}), 403

    agent = body.get("agent") or "unknown"
    ts = int(body.get("ts") or time.time())
    aggregates = body.get("aggregates") or {}
    last_balances = aggregates.get("last_balances") or {}

    if not isinstance(aggregates, dict):
        aggregates = {}
    if not isinstance(last_balances, dict):
        last_balances = {}

    _last_aggregates = aggregates
    _last_balances = last_balances
    _last_telemetry_ts = ts

    info(
        f"telemetry: received aggregates from {agent} "
        f"venues={list(last_balances.keys())} ts={ts}"
    )

    # Optional: mirror balances to Sheets (one row per venue/asset)
    rows = []
    try:
        for venue, assets in last_balances.items():
            if not isinstance(assets, dict):
                continue
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
        if rows and SHEET_URL:
            sheets_append_rows(SHEET_URL, TELEMETRY_LOG_SHEET, rows)
    except Exception as e:
        warn(f"telemetry sheet append failed (non-fatal): {e}")

    return jsonify({"ok": True})


# -----------------------------------------------------------------------------
# Simple in-process getters (used by Unified Snapshot & health)
# -----------------------------------------------------------------------------
def get_latest_balances() -> Dict[str, Any]:
    return dict(_last_balances)

def update_from_push(
    agent: str,
    balances: Dict[str, Any],
    aggregates: Optional[Dict[str, Any]] = None,
    ts: Optional[int] = None,
) -> None:
    """
    Called from wsgi.py when a telemetry push (balances snapshot) arrives.

    It keeps the in-memory cache in sync so that:
      • /api/telemetry/last prefers this cache
      • /api/telemetry/health can report something meaningful
    """
    global _last_balances, _last_aggregates, _last_telemetry_ts

    if not isinstance(balances, dict):
        balances = {}

    if aggregates is None or not isinstance(aggregates, dict):
        aggregates = {}

    # Update balances + timestamp
    _last_balances = balances
    now_ts = int(ts or time.time())
    _last_telemetry_ts = float(now_ts)

    # Merge aggregates and make sure we have a heartbeat-ish record
    agg = dict(_last_aggregates)
    agg.update(aggregates)

    hb = agg.get("last_heartbeat") or {}
    if not isinstance(hb, dict):
        hb = {}
    hb.setdefault("agent", agent)
    hb["ts"] = now_ts
    agg["last_heartbeat"] = hb

    _last_aggregates = agg

    # Optional: also persist into telemetry_store if available
    try:
        import telemetry_store

        telemetry_store.store_push(
            agent=agent,
            ts=now_ts,
            aggregates=_last_aggregates,
        )
    except Exception:
        # Soft-fail only; we don’t want telemetry to break because SQLite is unhappy
        pass

def get_latest_heartbeat() -> Dict[str, Any]:
    return dict(_last_heartbeat)


def get_latest_aggregates() -> Dict[str, Any]:
    return dict(_last_aggregates)


def get_telemetry_age_sec() -> float:
    if not _last_telemetry_ts:
        return 9e9  # effectively "infinite"
    return time.time() - _last_telemetry_ts


# -----------------------------------------------------------------------------
# /api/telemetry/health — in-process summary (no HTTP self-call)
# -----------------------------------------------------------------------------
@bp_telemetry.route("/api/telemetry/health", methods=["GET"])
def telemetry_health():
    """Return a small JSON health summary of Edge telemetry.

    This endpoint is intentionally lightweight and **never** calls back into
    our own HTTP server (to avoid deadlocks on single-worker deployments).

    It uses only the in-process caches maintained by:
      - telemetry_heartbeat()   -> _last_heartbeat
      - telemetry_push()        -> _last_balances, _last_aggregates, _last_telemetry_ts
    """
    try:
        age_sec = float(get_telemetry_age_sec())
        aggregates = get_latest_aggregates() or {}
        heartbeat = get_latest_heartbeat() or {}
        balances = get_latest_balances() or {}

        if not isinstance(aggregates, dict):
            aggregates = {}
        if not isinstance(heartbeat, dict):
            heartbeat = {}
        if not isinstance(balances, dict):
            balances = {}

        venues = sorted(balances.keys())
        aggregates_keys = sorted(aggregates.keys())

        # Prefer the agent recorded in aggregates; fall back to heartbeat.
        agent = str(aggregates.get("agent") or heartbeat.get("agent") or "")

        # Optional soft health flag: consider "ok" if we've seen telemetry
        # within the last TELEMETRY_HEALTH_MAX_AGE_SEC seconds.
        max_age = float(os.getenv("TELEMETRY_HEALTH_MAX_AGE_SEC", "900"))  # 15 minutes default
        ok = bool(age_sec < max_age)

        return jsonify(
            {
                "ok": ok,
                "age_sec": age_sec,
                "venues": venues,
                "aggregates_keys": aggregates_keys,
                "agent": agent,
                "source": "memory",
            }
        )
    except Exception as e:
        # Never let health checks raise; just return a degraded blob.
        try:
            warn(f"telemetry_health error: {e}")
        except Exception:
            pass
        return jsonify(
            {
                "ok": False,
                "error": "exception",
                "age_sec": 9e9,
                "venues": [],
                "aggregates_keys": [],
                "agent": "",
                "source": "memory_error",
            }
        )
