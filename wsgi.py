# wsgi.py — quiet, ASGI-ready NovaTrade Bus for Render
from __future__ import annotations
import os, logging, threading, time
from typing import Optional
from flask import Flask, jsonify, request

# ---------------------------------------------------------------------
# Logging setup (INFO shows lifecycle events; WARNING hides chatter)
# ---------------------------------------------------------------------
LOG_LEVEL = os.environ.get("NOVA_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bus")

# Silence noisy libs
for name in ("werkzeug", "gunicorn.access", "uvicorn.access",
             "schedule", "gspread", "googleapiclient"):
    logging.getLogger(name).setLevel(logging.ERROR)

# ---------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------
flask_app = Flask(__name__)

# ---------------------------------------------------------------------
# Optional Telegram integration
# ---------------------------------------------------------------------
def _maybe_init_telegram(app: Flask) -> Optional[str]:
    if os.environ.get("ENABLE_TELEGRAM", "").lower() not in ("1", "true", "yes"):
        return None
    try:
        from telegram_webhook import telegram_app, set_telegram_webhook  # type: ignore
        app.register_blueprint(telegram_app, url_prefix="/tg")  # type: ignore
        set_telegram_webhook()
        return None
    except Exception as e:
        log.warning("Telegram init failed: %s", e)
        return str(e)

telegram_status = _maybe_init_telegram(flask_app)

# ---------------------------------------------------------------------
# Core endpoints
# ---------------------------------------------------------------------
@flask_app.get("/")
def index():
    return jsonify(ok=True, service="NovaTrade Bus", status="ready"), 200

@flask_app.get("/healthz")
def healthz():
    info = {"ok": True, "web": "up",
            "telegram": {"status": "ok" if not telegram_status else "degraded"}}
    return jsonify(info), 200

@flask_app.get("/readyz")
def readyz():
    return jsonify(ok=True), 200

# ---------------------------------------------------------------------
# Edge Agent API endpoints
# ---------------------------------------------------------------------
@flask_app.post("/api/telemetry/push")
def telemetry_push():
    """Edge Agent posts wallet snapshots and health telemetry here."""
    data = request.get_json(silent=True) or {}
    summary = {
        "source": data.get("agent_id", "edge"),
        "balances": {k: round(v, 4) for k, v in (data.get("balances") or {}).items()},
    }
    log.info("📡 Telemetry push received: %s", summary)
    return jsonify(ok=True, received=len(data or {})), 200


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
    return jsonify(ok=True, service="Bus", alive=True), 200

# ---------------------------------------------------------------------
# Error handlers
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
# Background receipts bridge (optional)
# ---------------------------------------------------------------------
def _start_receipts_bridge():
    if os.environ.get("DISABLE_RECEIPTS_BRIDGE", "").lower() in ("1", "true", "yes"):
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
# Scheduler loop (to run hourly/daily jobs)
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
# ASGI adapter (so Uvicorn can serve Flask cleanly)
# ---------------------------------------------------------------------
try:
    from asgiref.wsgi import WsgiToAsgi
    app = WsgiToAsgi(flask_app)
except Exception as e:
    log.warning("ASGI adapter unavailable; falling back to WSGI: %s", e)
    app = flask_app  # type: ignore
