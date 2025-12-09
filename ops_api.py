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
from typing import Any, Dict, List
from flask import Blueprint, request, jsonify
from logging import getLogger

log = logging.getLogger(__name__)

bp = Blueprint("ops_api", __name__)

OUTBOX_DB_PATH = os.getenv("OUTBOX_DB_PATH", "/data/outbox.db")
OUTBOX_SECRET  = os.getenv("OUTBOX_SECRET", "")  # if empty => no signature required
MAX_PULL       = int(os.getenv("MAX_PULL", "50"))
INSIGHT_LOGFILE = "council_insights.jsonl"
INSIGHT_LOG_PATH = os.path.join(os.path.dirname(__file__), INSIGHT_LOGFILE)

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
    """
    Input:  {"limit": 10}  (optional)
    Output: [{"id":..., "venue":..., "symbol":..., ...}, ...]
    """
    try:
        raw = request.get_data(cache=False) or b"{}"
        try:
            j = .loads(raw.decode("utf-8"))
        except Exception:
            j = {}
        limit = int(j.get("limit") or MAX_PULL)
        limit = max(1, min(limit, MAX_PULL))

        con = _open_db(); con.row_factory = sqlite3.Row
        _ensure_schema(con)
        cur = con.cursor()

        # pick NEW first; if you prefer queued, add additional status here
        cur.execute("""
            SELECT id, venue, symbol, side, amount_usd, amount_base, note, mode, agent_id, cmd_id
            FROM outbox
            WHERE status='NEW'
            ORDER BY id ASC
            LIMIT ?
        """, (limit,))
        rows = cur.fetchall()
        cmds = [_row_to_cmd(r) for r in rows]

        return ify(cmds), 200
    except Exception as e:
        print(f"[ops_api] pull error: {e}")
        traceback.print_exc()
        return ify({"ok": False, "error": str(e)}), 500

@bp.route("/commands/ack", methods=["POST"])
def commands_ack():
    """
    Edge returns execution receipts.
    Body:
      {
        "id": <cmd_id>,
        "status": "ok|error|held",
        "receipt": { ... exchange/broker payload ... },
        "meta": {"agent_id":"edge-primary", "ts": "..."},
        "command": { optional echo of the command fields }
      }
    """
    try:
        raw = request.get_data(cache=False) or b"{}"
        if not _verify_signature(raw):
            return ify({"ok": False, "error": "invalid signature"}), 401

        j = .loads(raw.decode("utf-8"))
        cid     = int(j.get("id") or j.get("cmd_id") or 0)
        status  = str(j.get("status") or "").lower() or "ok"
        receipt = j.get("receipt") or {}
        meta    = j.get("meta") or {}
        command = j.get("command") or {}

        if not cid:
            return ify({"ok": False, "error": "missing id"}), 400

        # mirror for bridge
        payload = .dumps({
            "cmd_id": cid,
            "status": status,
            "receipt": receipt,
            "meta": meta,
            "command": command
        })

        con = _open_db()
        _ensure_schema(con)
        cur = con.cursor()

        # write receipt first (never blocks the outbox state update)
        cur.execute("INSERT INTO receipts (payload) VALUES (?)", [payload])

        # update outbox row if present
        try:
            cur.execute("UPDATE outbox SET status=?, updated_ts=datetime('now') WHERE id=?", [status.upper(), cid])
        except sqlite3.OperationalError:
            # tolerate legacy outbox without status column (should be fixed by ensure_schema)
            pass

        return ify({"ok": True}), 200
    except Exception as e:
        print(f"[ops_api] ack error: {e}")
        traceback.print_exc()
        return ify({"ok": False, "error": str(e)}), 500

# ---------------- Council Insight API ----------------

@bp.route("/insight/<decision_id>", methods=["GET"])
def get_insight(decision_id: str):
    """
    Look up a single CouncilInsight by decision_id.

    Final URL (because of blueprint registration in main.py):
      GET /api/insight/<decision_id>
    """
    path = INSIGHT_LOG_PATH

    if not os.path.exists(path):
        return jsonify({"ok": False, "error": "no_insights"}), 404

    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                except Exception:
                    # skip bad lines
                    continue

                if obj.get("decision_id") == decision_id:
                    return jsonify({"ok": True, "insight": obj})

        return jsonify({"ok": False, "error": "decision_id_not_found"}), 404

    except Exception as e:
        log.exception("council_insights: get_insight failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/insight/recent", methods=["GET"])
def recent_insights():
    """
    Return the most recent CouncilInsight entries from council_insights.jsonl.

    Final URL:
      GET /api/insight/recent?limit=50
    """
    limit = int(request.args.get("limit", 50))
    path = INSIGHT_LOG_PATH

    # If file doesn't exist yet, treat as "no insights yet".
    if not os.path.exists(path):
        return jsonify({"ok": True, "count": 0, "entries": []})

    entries = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except Exception as e:
                    log.warning("council_insights: failed to parse line: %s", e)

        if not entries:
            return jsonify({"ok": True, "count": 0, "entries": []})

        subset = entries[-limit:]
        return jsonify({"ok": True, "count": len(subset), "entries": subset})

    except Exception as e:
        log.exception("council_insights: recent_insights failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/insight/<decision_id>/view")
def insight_html(decision_id: str):
    """
    Simple HTML view for a single decision.
    Final URL:
      GET /api/insight/<decision_id>/view
    """
    path = INSIGHT_LOG_PATH

    if not os.path.exists(path):
        return "No insights yet", 404

    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:
                    continue
                if entry.get("decision_id") == decision_id:
                    council = entry.get("council", {})
                    html = f"""
                    <html>
                      <body style='font-family:sans-serif;padding:20px'>
                        <h2>Decision {decision_id}</h2>
                        <p><b>Story:</b> {entry.get('story')}</p>
                        <p><b>Autonomy:</b> {entry.get('autonomy')}</p>
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
        log.exception("council_insights: insight_html failed: %s", e)
        return f"Error: {e}", 500

@bp.route("/insight/<decision_id>")
def insight_html(decision_id):
    try:
        with open("council_insights.jsonl", "r") as f:
            for line in f:
                entry = json.loads(line.strip())
                if entry.get("decision_id") == decision_id:
                    council = entry.get("council", {})
                    html = f"""
                    <html>
                    <body style='font-family:sans-serif;padding:20px'>
                      <h2>Decision {decision_id}</h2>
                      <p><b>Story:</b> {entry.get('story')}</p>
                      <p><b>Autonomy:</b> {entry.get('autonomy')}</p>
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
