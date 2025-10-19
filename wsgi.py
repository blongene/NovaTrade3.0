# wsgi.py — NovaTrade Bus web entrypoint (safe, idempotent blueprint registration)
from __future__ import annotations
import os, threading, sqlite3
from flask import Flask, jsonify, request


# -----------------------------
# Base app: try Telegram app; else bare Flask
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

if app is None:
    app = Flask(__name__)

from ops_api_sqlite import OPS
app.register_blueprint(OPS)
print("[WEB] Command Bus API registered.")

# -----------------------------
# Health (ALWAYS 200)
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
            webhook=({"status": "ok"} if telegram_init_err is None
                     else {"status": "degraded", "reason": telegram_init_err})
        ), 200
    except Exception as err:
        # Never 500 on health
        return jsonify(ok=True, fallback=True, reason=str(err)), 200

# Small admin helper to clear obviously bad pending rows
@app.post("/ops/admin/clear_bad_pending")
def _clear_bad_pending():
    from outbox_db import DB_PATH
    con = sqlite3.connect(DB_PATH)
    cur = con.execute(
        "DELETE FROM commands WHERE status='pending' AND instr(agent_id, ',')>0"
    )
    n = cur.rowcount or 0
    con.commit(); con.close()
    return jsonify(deleted=n), 200

# -----------------------------
# Blueprint registration helpers (idempotent)
# -----------------------------
def _register_bp_once(bp, label: str = ""):
    """
    Register a Flask Blueprint only if its name is not already in app.blueprints.
    """
    try:
        name = getattr(bp, "name", None) or ""
        if name and name in app.blueprints:
            print(f"[WEB] {label or name} already registered: skipped.")
            return
        app.register_blueprint(bp)
        print(f"[WEB] {label or name} registered.")
    except Exception as err:
        print(f"[WEB] {label or 'blueprint'} not available: {err}")

def _import_and_register(mod_name: str, attr_name: str = "bp", label: str = ""):
    try:
        mod = __import__(mod_name, fromlist=[attr_name])
        bp = getattr(mod, attr_name)
        _register_bp_once(bp, label or mod_name)
    except Exception as err:
        print(f"[WEB] {label or mod_name} not available: {err}")


# -----------------------------
# Command Bus + Ops + helpers
# -----------------------------
try:
    from api_commands import bp as cmdapi_bp  # /api/commands/pull, /api/commands/ack
    _register_bp_once(cmdapi_bp, "Command Bus API")
except Exception as err:
    print(f"[WEB] Command Bus API not available: {err}")

# Optional ops helpers (if present)
for mod_name, attr, label in [
    ("ops_api", "bp", "Ops api"),         
    ("ops_venue", "bp", "Ops venue checker"),
]:
    _import_and_register(mod_name, attr, label)

from vault_intelligence import run_vault_intelligence
from rebuy_driver import run_rebuy_driver
from daily_summary import daily_phase5_summary

import schedule, threading, time, os

def _run_scheduled_job(fn):
    try:
        fn()
    except Exception as e:
        print(f"[scheduler] job error in {getattr(fn,'__name__','fn')}: {e}")

schedule.every(int(os.getenv("VAULT_INTELLIGENCE_INTERVAL_MIN","60"))).minutes.do(lambda: _run_scheduled_job(run_vault_intelligence))
schedule.every(int(os.getenv("REBUY_INTERVAL_MIN","180"))).minutes.do(lambda: _run_scheduled_job(run_rebuy_driver))
# Daily summary at 09:00 ET; if your container is UTC, adjust to match 13:00 UTC.
schedule.every().day.at("09:10").do(lambda: _run_scheduled_job(daily_phase5_summary))

def _schedule_loop():
    while True:
        schedule.run_pending()
        time.sleep(1)

t = threading.Thread(target=_schedule_loop, daemon=True)
t.start()

print("✅ Phase‑5 schedulers active (Vault Intelligence, Rebuy Driver, Daily Summary).")

from nova_trigger_watcher import check_nova_trigger
schedule.every(12).hours.do(lambda: _run_scheduled_job(check_nova_trigger))

# -----------------------------
# Receipts / Telemetry endpoints
# Prefer the NEW receipt_bus over legacy receipts_api if both exist.
# -----------------------------
# New Receipts API (with provenance → Sheets)
try:
    from receipt_bus import bp as receipt_bus_bp
    _register_bp_once(receipt_bus_bp, "Receipts API (bus)")
    RECEIPTS_REGISTERED = True
except Exception as _e:
    print(f"[WEB] Receipts API (bus) not available: {_e}")
    RECEIPTS_REGISTERED = False

# Legacy receipts_api (only register if new one isn't present or uses a different name)
try:
    from receipts_api import bp as receipts_api_bp
    # Register only if not already taken by the new one
    if not RECEIPTS_REGISTERED or getattr(receipts_api_bp, "name", "") not in app.blueprints:
        _register_bp_once(receipts_api_bp, "Receipts API (legacy)")
    else:
        print("[WEB] Receipts API (legacy) skipped: name already registered.")
except Exception as _e:
    print(f"[WEB] Receipts API (legacy) not available: {_e}")

# Telemetry write API (heartbeats/push from Edge)
_import_and_register("telemetry_api", "bp", "Telemetry write API")

# Telemetry read-only view (browser quick check), optional
_import_and_register("telemetry_read", "bp", "Telemetry read API")

# -----------------------------
# Debug routes (safe, read-only)
# -----------------------------
from outbox_db import DB_PATH

def _add_debug_route(rule, endpoint, view_func):
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
# Daily scheduler (no-cost, in-process)
# -----------------------------
try:
    from daily_scheduler import run_daily
    def _send_daily_health():
        import health_summary
        try:
            health_summary.main()
            print("[bus] daily health summary sent")
        except Exception as e:
            print("[bus] daily health summary error:", e)

    run_daily(_send_daily_health)
except Exception as err:
    print(f"[WEB] daily scheduler not available: {err}")

from unified_snapshot import run_unified_snapshot
from wallet_harmonizer import run_wallet_harmonizer

schedule.every(30).minutes.do(run_unified_snapshot)
schedule.every(30).minutes.do(run_wallet_harmonizer)
run_unified_snapshot(); run_wallet_harmonizer()

# -----------------------------
# Boot sequence (background)
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

from threading import Thread
import time, receipts_bridge

def _bridge_loop():
    time.sleep(15)
    while True:
        try:
            receipts_bridge.run_once()
        except Exception as e:
            print(f"[bridge-loop] error: {e}")
        time.sleep(300)  # every 5 minutes

Thread(target=_bridge_loop, daemon=True).start()
print("[WEB] receipts_bridge scheduled every 5m.")
