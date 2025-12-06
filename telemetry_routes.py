from __future__ import annotations

import os
import hmac
import hashlib
import time
import json
from typing import Dict, Any

import requests
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

# Base URL to talk to our own /api/telemetry/last endpoint (same as telemetry_mirror)
PORT = int(os.getenv("PORT", "10000"))
LAST_URL = os.getenv("TELEMETRY_LAST_URL", f"http://127.0.0.1:{PORT}/api/telemetry/last")

# In-memory latest state (best-effort cache for quick peeks)
_last_balances: Dict[str, Any] = {}
_last_heartbeat: Dict[str, Any] = {}
_last_aggregates: Dict[str, Any] = {}
_last_telemetry_ts: float = 0.0


# -----------------------------------------------------------------------------
# DB persistence helpers (best-effort, no hard failures)
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

    # Persist to DB (best-effort)
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

    # Persist to DB (best-effort)
    _store_push(agent, ts, aggregates)

    # Optional mirror to Sheets (one row per venue/asset)
    try:
        rows = []
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
        warn(f"telemetry sheet append failed: {e}")

    return jsonify({"ok": True})


# -----------------------------------------------------------------------------
# Simple getters (if other modules need quick in-process peeks)
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
# /api/telemetry/health — wrapper around /api/telemetry/last
# -----------------------------------------------------------------------------
@bp_telemetry.route("/api/telemetry/health", methods=["GET"])
def telemetry_health():
    """
    Health view built on top of the same /api/telemetry/last endpoint that
    telemetry_mirror.py uses.

    We *do not* query SQLite directly here; instead we:
      - GET LAST_URL (usually http://127.0.0.1:PORT/api/telemetry/last)
      - Expect: {"ok": true, "data": {...}, "age_sec": 7.3}
      - Summarize venues and top-level keys from the inner telemetry dict.
    """
    try:
        resp = requests.get(LAST_URL, timeout=5)
    except Exception as e:
        warn(f"telemetry_health: error calling {LAST_URL}: {e}")
        return jsonify(
            {
                "ok": False,
                "error": "unreachable",
                "age_sec": 9e9,
                "venues": [],
                "aggregates_keys": [],
                "agent": "",
                "source": "last_url_error",
            }
        )

    if not resp.ok:
        warn(f"telemetry_health: HTTP {resp.status_code} from {LAST_URL}: {resp.text}")
        return jsonify(
            {
                "ok": False,
                "error": f"http_{resp.status_code}",
                "age_sec": 9e9,
                "venues": [],
                "aggregates_keys": [],
                "agent": "",
                "source": "last_url_http",
            }
        )

    try:
        body = resp.json()
    except Exception as e:
        warn(f"telemetry_health: bad JSON from {LAST_URL}: {e}")
        return jsonify(
            {
                "ok": False,
                "error": "bad_json",
                "age_sec": 9e9,
                "venues": [],
                "aggregates_keys": [],
                "agent": "",
                "source": "last_url_bad_json",
            }
        )

    if not isinstance(body, dict):
        return jsonify(
            {
                "ok": False,
                "error": "non_dict_body",
                "age_sec": 9e9,
                "venues": [],
                "aggregates_keys": [],
                "agent": "",
                "source": "last_url_non_dict",
            }
        )

    ok = bool(body.get("ok", True))
    age_sec = float(body.get("age_sec") or 9e9)
    data = body.get("data") or {}
    if not isinstance(data, dict):
        data = {}

    by_venue = data.get("by_venue") or {}
    if not isinstance(by_venue, dict):
        by_venue = {}

    venues = sorted(by_venue.keys())
    aggregates_keys = sorted(data.keys())
    agent = str(data.get("agent") or "")

    return jsonify(
        {
            "ok": ok,
            "age_sec": age_sec,
            "venues": venues,
            "aggregates_keys": aggregates_keys,
            "agent": agent,
            "source": "last_url",
        }
    )
