# telemetry_routes.py — Bus-side telemetry ingestion for NovaTrade 3.0
#
# Handles:
#   - /api/heartbeat          (Edge → Bus)
#   - /api/telemetry/push     (Edge → Bus)
#
# Accepts HMAC-signed JSON from telemetry_sync.py on the Edge.
# Stores last-known telemetry in memory (for fast reads),
# mirrors into Sheets if configured,
# and (Phase 19.2) persists heartbeats + pushes into telemetry_store
# so telemetry_read.py can serve DB-backed views.

from __future__ import annotations

import os
import hmac
import hashlib
import time
from typing import Dict, Any

from flask import Blueprint, request, jsonify

# ---- Imports from utils.py --------------------------------------------------

try:
    from utils import warn, info, get_ws, sheets_append_rows, SHEET_URL
except Exception:  # pragma: no cover - defensive fallback
    def warn(msg): print(f"[WARN] {msg}")
    def info(msg): print(f"[INFO] {msg}")
    def get_ws(name): raise RuntimeError("utils.get_ws unavailable")
    def sheets_append_rows(*a, **k): pass
    SHEET_URL = ""

# ---- Optional DB warehouse (telemetry_store) -------------------------------

try:
    import telemetry_store  # provides store_heartbeat, store_push
except Exception:  # pragma: no cover - telemetry DB is optional
    telemetry_store = None  # type: ignore

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
# DB persistence helpers (Phase 19.2)
# -----------------------------------------------------------------------------
def _store_heartbeat(agent: str, ts: int, latency_ms: float) -> None:
    """
    Best-effort write of heartbeat into telemetry_store.

    Uses telemetry_store.store_heartbeat(agent=..., ts=..., latency_ms=...).
    Never raises.
    """
    if telemetry_store is None:
        return
    try:
        # store_heartbeat wants ints
        telemetry_store.store_heartbeat(
            agent=str(agent),
            ts=int(ts),
            latency_ms=int(latency_ms),
        )
    except Exception as e:
        # Never break the HTTP path on DB issues
        try:
            warn(f"telemetry_store.store_heartbeat failed: {e}")
        except Exception:
            pass


def _store_push(agent: str, ts: int, aggregates: Dict[str, Any]) -> None:
    """
    Best-effort write of telemetry push into telemetry_store.

    Uses telemetry_store.store_push(agent=..., ts=..., aggregates=...).
    Never raises.
    """
    if telemetry_store is None:
        return
    try:
        telemetry_store.store_push(
            agent=str(agent),
            ts=int(ts),
            aggregates=aggregates or {},
        )
    except Exception as e:
        try:
            warn(f"telemetry_store.store_push failed: {e}")
        except Exception:
            pass


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

    Edge sends JSON:
        { "agent": "...", "ts": 1234567890, "latency_ms": 42 }
    with header:
        X-Signature: hmac_sha256(secret, raw_json)

    Phase 19.2:
      - Keeps in-memory _last_heartbeat for fast access.
      - Mirrors to Sheets (Heartbeat_Log) if configured.
      - Persists into telemetry_store telemetry_heartbeat table (DB backbone).
    """
    global _last_heartbeat

    # Edge sends raw bytes with Content-Type: application/json
    body = request.get_json(force=True, silent=True) or {}
    sig = request.headers.get("X-Signature", "")

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

    # Phase 19.2: persist into DB warehouse (best-effort)
    _store_heartbeat(agent, ts, latency)

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
    """
    Aggregated telemetry endpoint.

    Edge sends JSON:
        {
          "agent": "...",
          "ts": 1234567890,
          "aggregates": {
              "trades_24h": {...},
              "last_balances": { "COINBASE":{"BTC":0.01,...}, ... },
              "last_heartbeat": {...}
          }
        }
    with header:
        X-Signature: hmac_sha256(secret, raw_json)

    Phase 19.2:
      - Keeps in-memory _last_aggregates/_last_balances for fast access.
      - Mirrors to Sheets (Telemetry_Log) if configured.
      - Persists aggregates into telemetry_store telemetry_push table (DB backbone).
    """
    global _last_balances, _last_aggregates, _last_telemetry_ts

    body = request.get_json(force=True, silent=True) or {}
    sig = request.headers.get("X-Signature", "")

    if not verify_signature(body, sig):
        warn(f"telemetry: invalid signature from agent={body.get('agent')}")
        return jsonify({"ok": False, "error": "bad_signature"}), 403

    agent = body.get("agent") or "unknown"
    ts = int(body.get("ts") or time.time())
    aggregates = body.get("aggregates") or {}
    last_balances = aggregates.get("last_balances") or {}

    _last_aggregates = aggregates
    _last_balances = last_balances
    _last_telemetry_ts = ts

    info(
        f"telemetry: balances from {agent} venues="
        f"{list(last_balances.keys())} ts={ts}"
    )

    # Phase 19.2: persist into DB warehouse (best-effort)
    _store_push(agent, ts, aggregates)

    # Optional mirror to Sheets (one row per venue/asset)
    try:
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
        if rows and SHEET_URL:
            # utils.sheets_append_rows(sheet_url, worksheet_name, rows)
            sheets_append_rows(SHEET_URL, TELEMETRY_LOG_SHEET, rows)
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


# -----------------------------------------------------------------------------
# Simple telemetry health view (Phase 19.2)
# -----------------------------------------------------------------------------
@bp_telemetry.route("/api/telemetry/health", methods=["GET"])
def telemetry_health():
    """
    Lightweight health view for ops dashboards and human checks.

    Returns:
      {
        "ok": true,
        "heartbeat": {...},          # last heartbeat (in-memory)
        "age_sec": 12.3,             # age of last telemetry push
        "venues": ["BINANCEUS",...], # venues in last_balances
        "aggregates_keys": ["trades_24h","last_balances",...]
      }

    DB-backed history is available via telemetry_read.py (/api/telemetry/last_seen),
    while this endpoint is intentionally cheap and derived from in-memory state.
    """
    hb = get_latest_heartbeat()
    balances = get_latest_balances()
    aggregates = get_latest_aggregates()
    age_sec = get_telemetry_age_sec()

    venues = sorted(balances.keys()) if isinstance(balances, dict) else []
    agg_keys = sorted(aggregates.keys()) if isinstance(aggregates, dict) else []

    return jsonify(
        {
            "ok": True,
            "heartbeat": hb,
            "age_sec": age_sec,
            "venues": venues,
            "aggregates_keys": agg_keys,
        }
    )
