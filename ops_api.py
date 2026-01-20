# ops_api.py — Command Bus API (SQLite edition)
# CLEAN • QUIET • WAL • SAFE
#
# Endpoints:
#   POST /api/commands/pull   -> edge pulls NEW commands
#   POST /api/commands/ack    -> edge acks with receipt payload (mirrored into receipts)
#
# Env:
#   OUTBOX_DB_PATH=/data/outbox.db         (SQLite path)
#   OUTBOX_SECRET=<hmac key>               (optional verify on ack; if unset, skip verify)
#   MAX_PULL=50                            (optional; default 50)

from __future__ import annotations
import os, json, hmac, hashlib, sqlite3, time, traceback
import logging
from typing import Any, Dict, List
from flask import Blueprint, request, jsonify
from logging import getLogger

log = logging.getLogger(__name__)

bp = Blueprint("ops_api", __name__)

OUTBOX_DB_PATH = os.getenv("OUTBOX_DB_PATH", "/data/outbox.db")
OUTBOX_SECRET  = os.getenv("OUTBOX_SECRET", "")  # if empty => no signature required
MAX_PULL       = int(os.getenv("MAX_PULL", "50"))

# Council Insight log – local JSONL that policy_logger writes
INSIGHT_LOG_PATH = os.getenv("COUNCIL_INSIGHTS_FILE", "council_insights.jsonl")

def ify(*args, **kwargs):
    """Alias so older code that calls `ify(...)` still works."""
    return jsonify(*args, **kwargs)

# ------------ SQLite helpers ------------

def _open_db() -> sqlite3.Connection:
    con = sqlite3.connect(OUTBOX_DB_PATH, timeout=10, isolation_level=None, check_same_thread=False)
    cur = con.cursor()
    # Harden for concurrent readers/writers
    try:
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA busy_timeout=5000")
    except Exception:
        pass
    return con

def _ensure_schema(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    # Outbox exists in legacy; just ensure newer columns exist
    cur.execute("CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    # add columns if missing
    cur.execute("PRAGMA table_info(outbox)")
    have = {r[1] for r in cur.fetchall()}
    def add(col, ddl):
        if col not in have:
            cur.execute(f"ALTER TABLE outbox ADD COLUMN {col} {ddl}")
            have.add(col)

    for col, ddl in [
        ("status","TEXT"),("venue","TEXT"),("symbol","TEXT"),("side","TEXT"),
        ("amount_usd","REAL"),("amount_base","REAL"),("note","TEXT"),
        ("mode","TEXT"),("agent_id","TEXT"),("cmd_id","INTEGER"),
        ("payload","TEXT"), # optional blob of original cmd
        ("created_ts","TEXT"),("updated_ts","TEXT")
    ]:
        try: add(col, ddl)
        except Exception: pass

    # Receipts table (for bridge)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS receipts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      payload TEXT NOT NULL,              -- full  {cmd_id, command, receipt, meta}
      ts TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

# ---------- HMAC verify (optional) ----------

def _verify_signature(raw: bytes) -> bool:
    if not OUTBOX_SECRET:
        return True
    sig = request.headers.get("X-OUTBOX-SIGN","")
    if not sig:
        return False
    mac = hmac.new(OUTBOX_SECRET.encode("utf-8"), raw, hashlib.sha256).hexdigest()
    try:
        return hmac.compare_digest(mac, sig)
    except Exception:
        return False

# ---------- Serializers ----------

CMD_FIELDS = ("id","venue","symbol","side","amount_usd","amount_base","note","mode","agent_id","cmd_id")

def _row_to_cmd(row: sqlite3.Row) -> Dict[str, Any]:
    d = {}
    for k in CMD_FIELDS:
        try:
            d[k] = row[k]
        except Exception:
            d[k] = None
    # Backward-compat: symbol/pair naming used by some edges
    if d.get("symbol") and "/" in str(d["symbol"]):
        d["pair"] = d["symbol"]
    return d

# ---------- Routes ----------

@bp.route("/commands/pull", methods=["POST"])
def commands_pull():
    try:
        raw = request.get_data()
        try:
            j = json.loads(raw.decode("utf-8"))
        except Exception:
            return jsonify({"ok": False, "error": "invalid_json"}), 400

        agent_id = j.get("agent_id")
        limit = int(j.get("limit", 20))
        if not agent_id:
            return jsonify({"ok": False, "error": "missing_agent_id"}), 400

        rows = _pull_commands(agent_id=agent_id, limit=limit)
        return jsonify({"ok": True, "commands": rows})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/commands/ack", methods=["POST"])
def commands_ack():
    """
    Edge returns execution receipts.
    Body:
      {
        "id": <cmd_id>,
        "status": "done|error|held"  (or ok/error/held — we normalize)
        "ok": true|false (optional),
        "receipt": { ... exchange/broker payload ... },
        "meta": {"agent_id":"edge-primary", "ts": "..."},
        "command": { optional echo of the command fields }
      }
    """
    try:
        raw = request.get_data(cache=False) or b"{}"
        if not _verify_signature(raw):
            return jsonify({"ok": False, "error": "invalid signature"}), 401

        try:
            j = json.loads(raw.decode("utf-8"))
        except Exception:
            return jsonify({"ok": False, "error": "invalid json body"}), 400

        # ---- Parse / normalize ----
        cid = int(j.get("id") or j.get("cmd_id") or 0)
        if not cid:
            return jsonify({"ok": False, "error": "missing id"}), 400

        status_in = str(j.get("status") or "").strip().lower()

        # Normalize common shapes into terminal statuses
        if status_in in ("ok", "done", "success", "completed"):
            status = "DONE"
        elif status_in in ("error", "failed", "fail"):
            status = "ERROR"
        elif status_in in ("held", "hold", "blocked"):
            status = "HELD"
        else:
            # If unknown, treat as ERROR to avoid “silent ok”
            status = "ERROR"

        receipt = j.get("receipt") or {}
        meta = j.get("meta") or {}
        command = j.get("command") or {}

        agent = (meta.get("agent_id") or j.get("agent_id") or j.get("agent") or "?")
        ok_val = j.get("ok", None)
        ok_str = "true" if (ok_val is None or bool(ok_val)) else "false"

        # Mirror payload we store (keeps bridge compatibility)
        payload = json.dumps({
            "cmd_id": cid,
            "status": status.lower(),
            "ok": (True if ok_val is None else bool(ok_val)),
            "agent_id": agent,
            "meta": meta,
            "receipt": receipt,
            "command": command,
        }, separators=(",", ":"), sort_keys=True)

        # ---- DB writes ----
        con = _open_db()
        _ensure_schema(con)
        cur = con.cursor()

        # receipt first (never blocks the state update)
        cur.execute("INSERT INTO receipts (payload) VALUES (?)", (payload,))

        # outbox status update (safe even if row missing)
        try:
            cur.execute(
                "UPDATE outbox SET status=?, updated_ts=datetime('now') WHERE id=?",
                (status, cid),
            )
        except sqlite3.OperationalError:
            # legacy tolerance if schema is old; ensure_schema should prevent this
            pass

        con.commit()

        # ---- One clean, operator-visible line ----
        buslog = logging.getLogger("bus")
        buslog.info("ops_ack: agent=%s cmd=%s status=%s ok=%s", agent, cid, status.lower(), ok_str)

        return jsonify({"ok": True}), 200

    except Exception as e:
        try:
            logging.getLogger("bus").exception("[ops_api] commands_ack failed: %s", e)
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(e)}), 500

# ---------------- Council Insight API ----------------

@bp.route("/insight/<decision_id>", methods=["GET"])
def get_insight(decision_id):
    try:
        if not os.path.exists(INSIGHT_LOG_PATH):
            return jsonify({"ok": False, "error": "not_found"}), 404

        with open(INSIGHT_LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if obj.get("decision_id") == decision_id:
                    return jsonify({"ok": True, "insight": obj})

        return jsonify({"ok": False, "error": "decision_id_not_found"}), 404
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/insight/recent", methods=["GET"])
def recent_insights():
    """
    Return the most recent CouncilInsight rows from council_insights.jsonl.

    Response:
    {
      "ok": true,
      "count": N,
      "insights": [ ...most recent first... ]
    }
    """
    limit = int(request.args.get("limit", 50))

    entries: list[dict] = []

    if os.path.exists(INSIGHT_LOG_PATH):
        try:
            with open(INSIGHT_LOG_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        entries.append(obj)
                    except Exception:
                        # skip malformed lines
                        continue
        except Exception:
            # behave like "no insights yet"
            entries = []

    if not entries:
        return jsonify({"ok": True, "count": 0, "insights": []})

    # newest first by ts (fallback 0)
    entries.sort(key=lambda r: r.get("ts", 0), reverse=True)
    sliced = entries[:limit]

    return jsonify({"ok": True, "count": len(sliced), "insights": sliced})


@bp.route("/insight/<decision_id>/view")
def insight_html(decision_id):
    try:
        with open(INSIGHT_LOG_PATH, "r") as f:
            for line in f:
                entry = json.loads(line.strip())
                if entry.get("decision_id") == decision_id:
                    council = entry.get("council", {})
                    ash_lens = entry.get("ash_lens", "")
                    html = f"""
                    <html>
                    <body style='font-family:sans-serif;padding:20px'>
                      <h2>Decision {decision_id}</h2>
                      <p><b>Story:</b> {entry.get('story')}</p>
                      <p><b>Autonomy:</b> {entry.get('autonomy')}</p>
                      <p><b>Ash's Lens:</b> {ash_lens}</p>

                      <h3>Council Influence</h3>
                      <pre>{json.dumps(council, indent=2)}</pre>

                      <h3>Raw Intent</h3>
                      <pre>{json.dumps(entry.get('raw_intent'), indent=2)}</pre>

                      <h3>Patched Intent</h3>
                      <pre>{json.dumps(entry.get('patched_intent'), indent=2)}</pre>

                      <h3>Flags</h3>
                      <pre>{json.dumps(entry.get('flags'), indent=2)}</pre>
                    </body>
                    </html>
                    """
                    return html
        return "Not found", 404
    except Exception as e:
        return f"Error: {e}", 500
