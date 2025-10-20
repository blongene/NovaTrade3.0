# wsgi.py — quiet, ASGI-ready web entrypoint for Render
from __future__ import annotations

import os
import logging
import threading
import time
from typing import Optional

from flask import Flask, jsonify, request

# ---------------------------------------------------------------------
# Logging: quiet by default. Only WARNING+ goes to stdout.
# You can raise verbosity by NOVA_LOG_LEVEL=INFO or DEBUG if needed.
# ---------------------------------------------------------------------
LOG_LEVEL = os.environ.get("NOVA_LOG_LEVEL", "WARNING").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.WARNING),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("web")

# Silence Werkzeug’s request logs unless you explicitly set INFO/DEBUG
werkzeug_log = logging.getLogger("werkzeug")
if LOG_LEVEL not in ("INFO", "DEBUG"):
    werkzeug_log.setLevel(logging.ERROR)

# ---------------------------------------------------------------------
# Create base Flask app
# ---------------------------------------------------------------------
flask_app = Flask(__name__)

# ---------------------------------------------------------------------
# Optional: Telegram webhook integration (quiet & safe)
# ---------------------------------------------------------------------
def _maybe_init_telegram(app: Flask) -> Optional[str]:
    """
    Try to import and attach telegram blueprint. Return error string on failure,
    None on success, and None if feature is simply unavailable.
    """
    if os.environ.get("ENABLE_TELEGRAM", "").lower() not in ("1", "true", "yes"):
        return None
    try:
        from telegram_webhook import telegram_app, set_telegram_webhook  # type: ignore
        # mount telegram blueprint/app under /tg if it's a Flask blueprint/app
        try:
            app.register_blueprint(telegram_app, url_prefix="/tg")  # type: ignore
        except Exception:
            # If it’s a Flask app, mount via WSGI midleware style
            from werkzeug.middleware.dispatcher import DispatcherMiddleware
            app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {"/tg": telegram_app})  # type: ignore
        try:
            set_telegram_webhook()
        except Exception as e:
            # webhook is optional; keep quiet unless you asked for INFO/DEBUG
            log.info("Telegram webhook degraded: %s", e)
        return None
    except Exception as e:
        # Feature unavailable or failed; report in /healthz but don’t break boot
        log.debug("Telegram init failed: %s", e)
        return str(e)

telegram_status = _maybe_init_telegram(flask_app)

# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------
@flask_app.get("/")
def index():
    return jsonify(ok=True, service="NovaTrade Bus", status="ready"), 200

@flask_app.get("/healthz")
def healthz():
    # Keep this endpoint FAST and always 200 for Render health checks.
    info = {"ok": True, "web": "up"}
    if telegram_status:
        info["telegram"] = {"status": "degraded", "reason": telegram_status}
    else:
        info["telegram"] = {"status": "ok"}
    # Include version/db path if available (optional, quiet on failure)
    try:
        def _read_version() -> str:
            for name in ("VERSION", "version.txt", ".version"):
                p = os.path.join(os.getcwd(), name)
                if os.path.exists(p):
                    with open(p, "r", encoding="utf-8") as fh:
                        return fh.read().strip()
            return "unknown"
        info["version"] = _read_version()
    except Exception as e:
        log.debug("version read failed: %s", e)
    try:
        info["db"] = os.environ.get("OUTBOX_DB_PATH", "unset")
    except Exception:
        pass
    return jsonify(info), 200

@flask_app.get("/readyz")
def readyz():
    # If you want to block readiness on some dependency, check it here.
    # Keep it quiet and fast; only WARNING+ logs will show by default.
    return jsonify(ok=True), 200

# ---------------------------------------------------------------------
# Minimal JSON error handlers (quiet)
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
# Optional: background receipts bridge (quiet & safe)
# Runs every 5 minutes if module present and not disabled
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
        # small defer so boot doesn’t race with cold starts
        time.sleep(15)
        while True:
            try:
                receipts_bridge.run_once()  # type: ignore
            except Exception as err:
                log.info("receipts_bridge error: %s", err)
            time.sleep(300)  # 5 minutes

    t = threading.Thread(target=_loop, name="receipts-bridge", daemon=True)
    t.start()
    log.info("receipts_bridge scheduled every 5m")

_start_receipts_bridge()

# ---------------------------------------------------------------------
# ASGI adapter: make Flask (WSGI) app runnable under Uvicorn on Render
# Export symbol `app` for `uvicorn wsgi:app …`
# ---------------------------------------------------------------------
try:
    from asgiref.wsgi import WsgiToAsgi  # lightweight and reliable
    app = WsgiToAsgi(flask_app)
except Exception as e:
    # Fallback: expose raw Flask app (still works under gunicorn/WSGI)
    log.warning("ASGI adapter unavailable; falling back to WSGI: %s", e)
    app = flask_app  # type: ignore

# ---------------------------------------------------------------------
# Notes for Render (keep here for reference, not executed):
# - Use this start command (no access log & warning level):
#   uvicorn wsgi:app --host 0.0.0.0 --port $PORT --log-level warning --no-access-log
# - To quiet app further, keep NOVA_LOG_LEVEL at WARNING (default).
# - To disable optional parts:
#     ENABLE_TELEGRAM=false
#     DISABLE_RECEIPTS_BRIDGE=true
# ---------------------------------------------------------------------
