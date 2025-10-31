# wsgi.py — NovaTrade Bus (quiet, resilient, ASGI-ready for Render)
from __future__ import annotations
import os, logging, threading, time
from typing import Optional
from flask import Flask, jsonify, request

# ---------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------
LOG_LEVEL = os.environ.get("NOVA_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bus")

# Silence noisy third-party modules
for name in (
    "werkzeug", "gunicorn.access", "uvicorn.access",
    "schedule", "gspread", "googleapiclient", "urllib3"
):
    logging.getLogger(name).setLevel(logging.ERROR)

# ---------------------------------------------------------------------
# FLASK APP INIT
# ---------------------------------------------------------------------
flask_app = Flask(__name__)

# ---------------------------------------------------------------------
# TELEGRAM (optional, safe auto-skip)
# ---------------------------------------------------------------------
def _maybe_init_telegram(app: Flask) -> Optional[str]:
    if os.environ.get("ENABLE_TELEGRAM", "").lower() not in ("1", "true", "yes"):
        return None
    try:
        from telegram_webhook import telegram_app, set_telegram_webhook  # type: ignore
        app.register_blueprint(telegram_app, url_prefix="/tg")  # type: ignore
        set_telegram_webhook()
        log.info("Telegram webhook registered.")
        return None
    except Exception as e:
        log.warning("Telegram init failed: %s", e)
        return str(e)

telegram_status = _maybe_init_telegram(flask_app)

# ---------------------------------------------------------------------
# CORE ENDPOINTS
# ---------------------------------------------------------------------
@flask_app.get("/")
def index():
    return jsonify(ok=True, service="NovaTrade Bus", status="ready"), 200

@flask_app.get("/healthz")
def healthz():
    info = {"ok": True, "web": "up"}
    info["telegram"] = {
        "status": "ok" if not telegram_status else "degraded",
        "reason": telegram_status or None,
    }
    return jsonify(info), 200

@flask_app.get("/readyz")
def readyz():
    return jsonify(ok=True), 200

# ---------------------------------------------------------------------
# EDGE AGENT API ENDPOINTS
# ---------------------------------------------------------------------
@flask_app.post("/api/telemetry/push")
def telemetry_push():
    """Edge Agent posts wallet snapshots and telemetry here."""
    data = request.get_json(silent=True) or {}
    summary = {
        "source": data.get("agent_id", "edge"),
        "balances": {k: round(v, 4) for k, v in (data.get("balances") or {}).items()},
    }
    log.info("📡 Telemetry push received: %s", summary)
    return jsonify(ok=True, received=len(data or {})), 200

# ---- Compatibility aliases for older agents ----
@flask_app.post("/api/telemetry/push_balances")
@flask_app.post("/bus/push_balances")
@flask_app.post("/api/edge/balances")
def telemetry_push_alias():
    """Allow older edge agents to push via legacy endpoints."""
    return telemetry_push()

@flask_app.post("/api/commands/pull")
def commands_pull():
    """Edge polls here for new trade instructions."""
    return jsonify(ok=True, commands=[]), 200

@flask_app.post("/api/commands/ack")
def commands_ack():
    """Edge acknowledges completed commands."""
    data = request.get_json(silent=True) or {}
    log.info("✅ ACK from edge: %s", data)
    return jsonify(ok=True), 200

@flask_app.post("/api/heartbeat")
def heartbeat():
    """Edge heartbeat."""
    return jsonify(ok=True, service="Bus", alive=True), 200

# ---------------------------------------------------------------------
# ERROR HANDLERS
# ---------------------------------------------------------------------
@flask_app.errorhandler(404)
def not_found(_e): return jsonify(error="not_found"), 404

@flask_app.errorhandler(405)
def method_not_allowed(_e): return jsonify(error="method_not_allowed"), 405

@flask_app.errorhandler(500)
def server_error(e):
    log.warning("Unhandled error: %s", e)
    return jsonify(error="internal_error"), 500

# ---------------------------------------------------------------------
# BACKGROUND RECEIPTS BRIDGE
# ---------------------------------------------------------------------
def _start_receipts_bridge():
    if os.environ.get("DISABLE_RECEIPTS_BRIDGE", "").lower() in ("1", "true", "yes"):
        log.info("receipts_bridge disabled via env.")
        return
    try:
        import receipts_bridge  # type: ignore
    except Exception as e:
        log.debug("receipts_bridge unavailable: %s", e)
        return

    def _loop():
        time.sleep(15)
        while True:
            try:
                receipts_bridge.run_once()  # type: ignore
            except Exception as err:
                log.info("receipts_bridge error: %s", err)
            time.sleep(300)

    threading.Thread(target=_loop, name="receipts-bridge", daemon=True).start()
    log.info("receipts_bridge scheduled every 5m")

_start_receipts_bridge()

# ---------------------------------------------------------------------
# SCHEDULER LOOP (for hourly/daily Nova jobs)
# ---------------------------------------------------------------------
def _start_scheduler():
    try:
        import schedule
    except ImportError:
        log.warning("schedule lib not found; skipping scheduler thread")
        return

    def _loop():
        log.info("⏰ Scheduler thread active.")
        while True:
            schedule.run_pending()
            time.sleep(5)

    threading.Thread(target=_loop, name="scheduler", daemon=True).start()

_start_scheduler()

# ---------------------------------------------------------------------
# ASGI ADAPTER (Uvicorn support)
# ---------------------------------------------------------------------
try:
    from asgiref.wsgi import WsgiToAsgi
    app = WsgiToAsgi(flask_app)
except Exception as e:
    log.warning("ASGI adapter unavailable; falling back to WSGI: %s", e)
    app = flask_app  # type: ignore
