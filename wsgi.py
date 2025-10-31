# wsgi.py — NovaTrade Bus (quiet, ASGI-ready, no duplicate routes)

from __future__ import annotations
import os, logging, threading, time
from typing import Optional, Dict, Any
from flask import Flask, jsonify, request

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
LOG_LEVEL = os.environ.get("NOVA_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bus")

werk = logging.getLogger("werkzeug")
if LOG_LEVEL not in ("DEBUG",):
    werk.setLevel(logging.WARNING)

# ---------------------------------------------------------------------
# App (Flask WSGI, later adapted to ASGI)
# ---------------------------------------------------------------------
flask_app = Flask(__name__)

# ---------------------------------------------------------------------
# Optional: Telegram webhook integration
#   Set ENABLE_TELEGRAM=true and provide a module exposing:
#     telegram_app : flask.Blueprint
#     set_telegram_webhook() : callable (optional)
# ---------------------------------------------------------------------
def _maybe_init_telegram(app: Flask) -> Optional[str]:
    if os.environ.get("ENABLE_TELEGRAM", "").lower() not in ("1", "true", "yes"):
        return None
    try:
        from telegram_webhook import telegram_app, set_telegram_webhook  # type: ignore
    except Exception as e:
        log.warning("Telegram init failed (import): %s", e)
        return str(e)

    try:
        # Correct API: register_blueprint
        app.register_blueprint(telegram_app, url_prefix="/tg")  # type: ignore
    except Exception as e:
        log.warning("Telegram blueprint mount failed: %s", e)
        return str(e)

    # Try to set webhook, but don't fail if it errors
    try:
        if callable(set_telegram_webhook):
            set_telegram_webhook()
    except Exception as e:
        log.info("Telegram webhook degraded: %s", e)

    log.info("Telegram webhook mounted at /tg")
    return None

telegram_status = _maybe_init_telegram(flask_app)

# ---------------------------------------------------------------------
# Basic service / health
# ---------------------------------------------------------------------
@flask_app.get("/")
def index():
    return jsonify(ok=True, service="NovaTrade Bus", status="ready"), 200

@flask_app.get("/healthz")
def healthz():
    info: Dict[str, Any] = {"ok": True, "web": "up"}
    info["telegram"] = {"status": "ok" if not telegram_status else "degraded", "reason": telegram_status}  # type: ignore
    try:
        for name in ("VERSION", "version.txt", ".version"):
            p = os.path.join(os.getcwd(), name)
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as fh:
                    info["version"] = fh.read().strip()
                break
    except Exception as e:
        log.debug("version read failed: %s", e)
    info["db"] = os.environ.get("OUTBOX_DB_PATH", "unset")
    return jsonify(info), 200

@flask_app.get("/readyz")
def readyz():
    return jsonify(ok=True, service="Bus", ready=True), 200

# ---------------------------------------------------------------------
# EDGE AGENT API ENDPOINTS (final, hardened, single definition each)
# ---------------------------------------------------------------------
def _safe_floats(d: Dict[str, Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for k, v in (d or {}).items():
        try:
            out[k] = round(float(v), 8)
        except Exception:
            out[k] = 0.0
    return out

@flask_app.post("/api/telemetry/push")
def telemetry_push():
    """
    Edge Agent posts wallet snapshots & telemetry here.
    Always safe-casts and never raises.
    """
    data = request.get_json(silent=True) or {}
    agent_id = data.get("agent_id", "edge")
    balances = _safe_floats(data.get("balances") or {})
    # concise, useful line
    log.info("📡 Telemetry from %s: %s", agent_id, balances)
    return jsonify(ok=True, received=len(balances)), 200

# Legacy compatibility aliases
@flask_app.post("/api/telemetry/push_balances")
@flask_app.post("/bus/push_balances")
@flask_app.post("/api/edge/balances")
def telemetry_push_alias():
    return telemetry_push()

@flask_app.post("/api/commands/pull")
def commands_pull():
    """
    Edge polls for commands. Empty until outbox/queue is wired.
    """
    log.debug("Edge poll ok (no commands queued)")
    return jsonify(ok=True, commands=[]), 200

@flask_app.post("/api/commands/ack")
def commands_ack():
    """
    Edge acknowledges command execution.
    JSON: {agent_id, command_id, status, ...}
    """
    data = request.get_json(silent=True) or {}
    agent = data.get("agent_id", "edge")
    cmd_id = data.get("command_id", "?")
    status = data.get("status", "ok")
    log.info("✅ ACK from %s → %s (%s)", agent, cmd_id, status)
    return jsonify(ok=True), 200

@flask_app.post("/api/heartbeat")
def heartbeat():
    return jsonify(ok=True, service="Bus", alive=True), 200

# ---------------------------------------------------------------------
# Quiet JSON error handlers (single definitions)
# ---------------------------------------------------------------------
@flask_app.errorhandler(404)
def _not_found(_e):
    return jsonify(error="not_found"), 404

@flask_app.errorhandler(405)
def _method_not_allowed(_e):
    return jsonify(error="method_not_allowed"), 405

@flask_app.errorhandler(500)
def _server_error(e):
    log.warning("Unhandled error: %s", e)
    return jsonify(error="internal_error"), 500

# ---------------------------------------------------------------------
# Optional: Receipts bridge (background, quiet)
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

# Optional: tiny “I’m alive” scheduler marker (no jobs here)
def _scheduler_banner():
    log.info("⏰ Scheduler thread active.")
threading.Thread(target=_scheduler_banner, daemon=True).start()

# ---------------------------------------------------------------------
# ASGI adapter for Uvicorn
# ---------------------------------------------------------------------
try:
    from asgiref.wsgi import WsgiToAsgi
    app = WsgiToAsgi(flask_app)
except Exception as e:
    log.warning("ASGI adapter unavailable; falling back to WSGI: %s", e)
    app = flask_app  # type: ignore
