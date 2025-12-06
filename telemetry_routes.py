from __future__ import annotations

import os
import hmac
import hashlib
import time
import json
from typing import Dict, Any

from flask import Blueprint, request, jsonify

# ---- Imports from utils.py --------------------------------------------------

try:
    from utils import warn, info, get_ws, sheets_append_rows, SHEET_URL
except Exception:  # pragma: no cover
    def warn(msg): print(f"[WARN] {msg}")
    def info(msg): print(f"[INFO] {msg}")
    def get_ws(name): raise RuntimeError("utils.get_ws unavailable")
    def sheets_append_rows(*a, **k): pass
    SHEET_URL = ""

# ---- Optional DB warehouse (telemetry_store) -------------------------------

try:
    import telemetry_store  # provides store_heartbeat, store_push
except Exception:  # pragma: no cover
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
BUS_TELEMETRY_DB = os.getenv("BUS_TELEMETRY_DB", "bus_telemetry.db")

# In-memory latest state (best-effort cache)
_last_balances: Dict[str, Any] = {}
_last_heartbeat: Dict[str, Any] = {}
_last_aggregates: Dict[str, Any] = {}
_last_telemetry_ts: float = 0.0


# -----------------------------------------------------------------------------
# DB persistence helpers
# -----------------------------------------------------------------------------
def _store_heartbeat(agent: str, ts: int, latency_ms: float) -> None:
    if telemetry_store is None:
        return
    try:
        telemetry_store.store_heartbeat(
            agent=str(agent),
            ts=int(ts),
            latency_ms=int(latency_ms),
        )
    except Exception as e:
        try:
            warn(f"telemetry_store.store_heartbeat failed: {e}")
        except Exception:
            pass


def _store_push(agent: str, ts: int, aggregates: Dict[str, Any]) -> None:
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
    Edge signs JSON body with TELEMETRY_SECRET and sends it in 'X-Signature'.
    """
    if not TELEMETRY_SECRET:
        # If no secret set, accept all — useful in dev, but set TELEMETRY_SECRET in prod.
        return True
    if not signature:
        return False
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
    global _last_heartbeat

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

    # Persist to DB
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

    # Persist to DB
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
# Public accessors (for other modules if needed)
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
# DB-backed telemetry health
# -----------------------------------------------------------------------------
@bp_telemetry.route("/api/telemetry/health", methods=["GET"])
def telemetry_health():
    """
    DB-backed health view.

    Reads from BUS_TELEMETRY_DB (same tables as telemetry_read.py) so that
    health reflects real pushes/heartbeats even if this process was restarted.
    """
    import sqlite3

    try:
        con = sqlite3.connect(BUS_TELEMETRY_DB)
        con.row_factory = sqlite3.Row
        with con:
            hb_rows = con.execute(
                "SELECT agent, MAX(ts) AS ts, MAX(latency_ms) AS latency_ms "
                "FROM telemetry_heartbeat GROUP BY agent"
            ).fetchall()
            push_rows = con.execute(
                "SELECT agent, aggregates_json, MAX(id) AS id "
                "FROM telemetry_push GROUP BY agent"
            ).fetchall()
    except Exception as e:
        warn(f"telemetry_health DB query failed: {e}")
        # Fallback to in-memory snapshot so endpoint still works
        hb = get_latest_heartbeat()
        balances = get_latest_balances()
        aggregates = get_latest_aggregates()
        age_sec = get_telemetry_age_sec()
        venues = sorted(balances.keys()) if isinstance(balances, dict) else []
        agg_keys = sorted(aggregates.keys()) if isinstance(aggregates, dict) else []
        return jsonify(
            {
                "ok": True,
                "heartbeats": [] if not hb else [hb],
                "age_sec": age_sec,
                "venues": venues,
                "aggregates_keys": agg_keys,
                "source": "memory",
            }
        )

    now = time.time()

    heartbeats = []
    last_ts = 0
    venues = set()
    aggregates_keys = set()

    for r in hb_rows:
        heartbeats.append(
            {"agent": r["agent"], "ts": r["ts"], "latency_ms": r["latency_ms"]}
        )
        if r["ts"] and r["ts"] > last_ts:
            last_ts = r["ts"]

    for r in push_rows:
        try:
            agg = json.loads(r["aggregates_json"] or "{}")
        except Exception:
            agg = {}
        if isinstance(agg, dict):
            aggregates_keys.update(agg.keys())
            lb = agg.get("last_balances") or {}
            if isinstance(lb, dict):
                venues.update(lb.keys())
            ts_val = agg.get("ts")
            if isinstance(ts_val, (int, float)) and ts_val > last_ts:
                last_ts = ts_val

    age_sec = (now - last_ts) if last_ts else 9e9

    return jsonify(
        {
            "ok": True,
            "heartbeats": heartbeats,
            "age_sec": age_sec,
            "venues": sorted(venues),
            "aggregates_keys": sorted(aggregates_keys),
            "source": "db",
        }
    )
