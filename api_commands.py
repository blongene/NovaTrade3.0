# api_commands.py — Command Bus: /api/commands/pull, /ack
# Phase 19: backed by Postgres via bus_store_pg (with SQLite fallback)

import os
from flask import Blueprint, request, jsonify

from bus_store_pg import get_store
from hmac_auth import require_hmac

bp = Blueprint("api_commands", __name__, url_prefix="/api/commands")

# Toggle HMAC requirement for /pull (ACK always uses HMAC)
REQUIRE_HMAC_PULL = os.getenv("REQUIRE_HMAC_PULL", "0").strip().lower() in {"1", "true", "yes"}

# Allowed agents (edge IDs). If empty, allow all.
ALLOW = {
    a.strip()
    for a in (os.getenv("OUTBOX_AGENT_ALLOW") or os.getenv("AGENT_ID") or "").split(",")
    if a.strip()
}

# Global store (PGStore when DB_URL+psycopg2 are available, else SQLiteStore fallback)
try:
    STORE = get_store()
    print(f"[API] command store backend: {type(STORE).__name__}")
except Exception as err:
    # If this fails, endpoints will 503 until fixed.
    print(f"[API] FATAL: cannot init command store: {err}")
    STORE = None


def _agent_allowed(aid: str) -> bool:
    """Return True if agent is allowed to use the command bus."""
    if not ALLOW:
        return True
    return aid in ALLOW


def _ensure_store():
    if STORE is None:
        return None, ("command store unavailable", 503)
    return STORE, None


@bp.post("/pull")
def pull():
    """
    Edge Agent pulls commands.

    Input JSON:
      { "agent_id": "edge-primary", "max": 5? }

    Output JSON:
      [
        { "id": 123, "type": "order.place", "payload": {...} },
        ...
      ]
    """
    # Optional HMAC for pulls
    if REQUIRE_HMAC_PULL:
        ok, err = require_hmac(request)
        if not ok:
            return jsonify(error=err), 401

    store, err = _ensure_store()
    if err:
        msg, code = err
        return jsonify(error=msg), code

    body = request.get_json(silent=True) or {}
    agent_id = (body.get("agent_id") or "").strip()
    if not agent_id:
        return jsonify(error="missing agent_id"), 400

    if not _agent_allowed(agent_id):
        return jsonify(error="agent not allowed"), 403

    # Default/max number of commands to lease
    limit = int(body.get("max") or 5)
    if limit <= 0:
        limit = 1
    if limit > 50:
        limit = 50

    try:
        # PGStore / SQLiteStore both expose .lease(agent_id, limit)
        rows = store.lease(agent_id=agent_id, limit=limit) or []

        items = []
        for r in rows:
            # For PGStore, r["intent"] is a JSON object; we keep it flexible.
            intent = r.get("intent") or {}

            # Support both "type/payload" style and bare intent dicts.
            cmd_type = intent.get("type") or "order.place"
            payload = intent.get("payload") or intent

            items.append(
                {
                    "id": r.get("id"),
                    "type": cmd_type,
                    "payload": payload,
                }
            )

        return jsonify(items), 200

    except Exception as e:
        return jsonify(error=str(e)), 500


@bp.post("/ack")
def ack():
    """
    Edge Agent acknowledges command execution.

    Expected JSON:
      {
        "agent_id": "edge-primary",
        "id": 123,
        "ok": true,
        "status": "ok|error|held|...",
        "txid": "...",          # optional
        "message": "...",       # optional
        "result": {...}         # optional, venue-native payload
      }
    """
    # ACK always requires HMAC
    ok, err = require_hmac(request)
    if not ok:
        return jsonify(ok=False, error=err), 401

    store, serr = _ensure_store()
    if serr:
        msg, code = serr
        return jsonify(ok=False, error=msg), code

    body = request.get_json(force=True) or {}

    agent_id = (body.get("agent_id") or "").strip()
    if not agent_id:
        return jsonify(ok=False, error="missing agent_id"), 400

    if not _agent_allowed(agent_id):
        return jsonify(ok=False, error="agent not allowed"), 403

    cmd_id = body.get("id")
    if not cmd_id:
        return jsonify(ok=False, error="missing id"), 400

    try:
        cmd_id = int(cmd_id)
    except Exception:
        return jsonify(ok=False, error="id must be int-convertible"), 400

    ok_flag = bool(body.get("ok"))
    status = body.get("status") or ("ok" if ok_flag else "error")
    txid = body.get("txid")
    msg = body.get("message")
    result = body.get("result") or {}

    receipt = {
        "id": cmd_id,
        "ok": ok_flag,
        "status": status,
        "txid": txid,
        "message": msg,
        "result": result,
    }

    try:
        # Persist receipt to DB
        try:
            store.save_receipt(agent_id=agent_id, cmd_id=cmd_id, receipt=receipt, ok=ok_flag)
        except TypeError:
            # Backwards safety: earlier versions had save_receipt(agent_id, cmd_id, receipt)
            store.save_receipt(agent_id, cmd_id, receipt)  # type: ignore[arg-type]

        # Update command status
        if ok_flag:
            store.done(cmd_id)
        else:
            # Best-effort reason for debugging
            reason = msg or status
            try:
                store.fail(cmd_id, reason)
            except TypeError:
                # Older signature fail(cmd_id, reason) vs fail(cmd_id)
                try:
                    store.fail(cmd_id)  # type: ignore[call-arg]
                except Exception:
                    pass

        return jsonify(ok=True), 200

    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500
