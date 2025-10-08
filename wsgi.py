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

# --- TEMP DEBUG ROUTE --------------------------------------------------------
try:
    @app.get("/ops/debug/pending")
    def _debug_pending():
        import outbox_db as db
        agent = (request.args.get("agent") or "").strip()
        db.init()
        items = db.pull(agent_id=agent or "edge-cb-1", limit=100, lease_s=999999)
        return jsonify(items), 200
except Exception as err:
    print(f"[WEB] debug pending skipped: {err}")

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
