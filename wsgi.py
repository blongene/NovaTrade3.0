# wsgi.py — web entrypoint (binds immediately, starts Nova boot once)
import os, threading
from flask import Flask, jsonify

# -----------------------------
# Base app: try Telegram app; else create a bare Flask app
# -----------------------------
telegram_init_err = None
app = None

try:
    from telegram_webhook import telegram_app as app, set_telegram_webhook
    try:
        set_telegram_webhook()
        print("[WEB] Telegram webhook set.")
    except Exception as err:
        telegram_init_err = f"webhook setup skipped: {err}"
        print(f"[WEB] {telegram_init_err}")
except Exception as err:
    telegram_init_err = f"telegram_webhook import failed: {err}"
    app = Flask(__name__)
    print(f"[WEB] {telegram_init_err}; using bare Flask app")

if app is None:  # absolute fallback safety
    app = Flask(__name__)

# -----------------------------
# Health (ALWAYS 200; never depends on undefined names)
# -----------------------------
def _read_version():
    for p in ("/data/VERSION", "./VERSION"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                v = f.read().strip()
                if v:
                    return v
        except Exception:
            pass
    return None

@app.get("/health")
def health():
    try:
        return jsonify(
            ok=True,
            version=_read_version(),
            db=os.environ.get("OUTBOX_DB_PATH"),
            webhook=({"status": "ok"} if telegram_init_err is None else {"status": "degraded", "reason": telegram_init_err})
        ), 200
    except Exception as err:
        # Never 500 on health
        return jsonify(ok=True, fallback=True, reason=str(err)), 200
        
@app.post("/ops/admin/clear_bad_pending")
def _clear_bad_pending():
    import sqlite3
    from outbox_db import DB_PATH
    con = sqlite3.connect(DB_PATH)
    cur = con.execute(
        "DELETE FROM commands WHERE status='pending' AND instr(agent_id, ',')>0"
    )
    n = cur.rowcount or 0
    con.commit(); con.close()
    return jsonify(deleted=n), 200

# -----------------------------
# Blueprints: Command Bus + Ops (best-effort)
# -----------------------------
try:
    from api_commands import bp as cmdapi_bp  # /api/commands/pull, /api/commands/ack
    app.register_blueprint(cmdapi_bp)
    print("[WEB] Command Bus API registered.")
except Exception as err:
    print(f"[WEB] Command Bus API not available: {err}")

# Optional ops helpers if present
for mod_name, attr_name, what in [
    ("ops_enqueue", "bp", "Ops enqueue"),
    ("ops_venue",   "bp", "Ops venue checker"),
]:
    try:
        mod = __import__(mod_name, fromlist=[attr_name])
        bp = getattr(mod, attr_name)
        app.register_blueprint(bp)
        print(f"[WEB] {what} registered.")
    except Exception as err:
        print(f"[WEB] {what} not available: {err}")
        
from receipts_api import bp as receipts_api_bp
app.register_blueprint(receipts_api_bp)

from telemetry_api import bp as telemetry_bp
app.register_blueprint(telemetry_bp)

# --- TEMP DEBUG (safe read-only, idempotent) ---------------------------------
from flask import request, jsonify
import os, sqlite3
from outbox_db import DB_PATH

def _add_debug_route(rule, endpoint, view_func):
    # only register if endpoint not present
    if endpoint not in app.view_functions:
        app.add_url_rule(rule, endpoint=endpoint, view_func=view_func, methods=["GET"])

def _dbg_dbinfo():
    p = DB_PATH
    exists = os.path.exists(p)
    size = os.path.getsize(p) if exists else 0
    info = {"DB_PATH": p, "exists": exists, "size": size}
    if exists:
        con = sqlite3.connect(p); con.row_factory = sqlite3.Row
        info["tables"] = [r["name"] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")]
        counts = {}
        for s in ("pending","in_flight","done","error"):
            counts[s] = con.execute(
                "SELECT COUNT(*) AS n FROM commands WHERE status=?", (s,)
            ).fetchone()["n"]
        info["counts"] = counts
        con.close()
    return jsonify(info), 200

def _dbg_cmds():
    agent = (request.args.get("agent") or "").strip()
    con = sqlite3.connect(DB_PATH); con.row_factory = sqlite3.Row
    sql = ("SELECT id, created_at, agent_id, kind, status, not_before, "
           "lease_expires_at, payload FROM commands ")
    args = []
    if agent:
        sql += "WHERE agent_id=? "
        args.append(agent)
    sql += "ORDER BY id DESC LIMIT 50"
    rows = [dict(r) for r in con.execute(sql, args).fetchall()]
    con.close()
    return jsonify(rows), 200

def _dbg_receipts():
    con = sqlite3.connect(DB_PATH); con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute(
        "SELECT id, cmd_id, agent_id, ok, status, txid, message, received_at "
        "FROM receipts ORDER BY id DESC LIMIT 50").fetchall()]
    con.close()
    return jsonify(rows), 200

_add_debug_route("/ops/debug/dbinfo",   "debug_dbinfo",   _dbg_dbinfo)
_add_debug_route("/ops/debug/cmds",     "debug_cmds",     _dbg_cmds)
_add_debug_route("/ops/debug/receipts", "debug_receipts", _dbg_receipts)


# -----------------------------
# Start NovaTrade boot sequence once (scheduler, loops, etc.)
# -----------------------------
_BOOT_STARTED = False

def _start_boot_once():
    global _BOOT_STARTED
    if _BOOT_STARTED:
        return
    _BOOT_STARTED = True
    try:
        import main as nova_main  # expects nova_main.boot()
        def _bg():
            try:
                nova_main.boot()
            except Exception as err:
                print(f"[WEB] Nova boot failed: {err}")
        t = threading.Thread(target=_bg, daemon=True)
        t.start()
        print("[WEB] Nova boot thread started.")
    except Exception as err:
        print(f"[WEB] Unable to import main/boot: {err}")

if os.getenv("RUN_BOOT_IN_WSGI", "1").strip().lower() in {"1", "true", "yes"}:
    _start_boot_once()
