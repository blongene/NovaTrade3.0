# wsgi.py — web entrypoint (binds immediately, starts Nova boot once)

import os, threading, time
from flask import Flask, jsonify

# --- Base app (telegram_webhook if present; else fallback) -------------------
_TELEGRAM_APP_ERROR = None
try:
    from telegram_webhook import telegram_app as app, set_telegram_webhook
    try:
        set_telegram_webhook()
        print("[WEB] Telegram webhook set.")
    except Exception as e:
        print(f"[WEB] webhook setup skipped: {e}")
except Exception as e:
    _TELEGRAM_APP_ERROR = e
    app = Flask(__name__)
    print(f"[WEB] Fallback Flask app created: {_TELEGRAM_APP_ERROR}")


# --- Health endpoint ---------------------------------------------------------
@app.get("/health")
def health_check():
    if _TELEGRAM_APP_ERROR:
        return jsonify(ok=True, fallback=True, reason=str(_TELEGRAM_APP_ERROR)), 200
    return jsonify(ok=True), 200

# --- Register Command Bus API (pull/ack) ------------------------------------
try:
    from api_commands import bp as cmdapi_bp  # /api/commands/pull, /api/commands/ack
    app.register_blueprint(cmdapi_bp)
    print("[WEB] Command Bus API registered.")
except Exception as e:
    print(f"[WEB] Command Bus API not available: {e}")

# --- Start NovaTrade boot sequence once (scheduler, loops, etc.) ------------
_BOOT_STARTED = False
def _start_boot_once():
    global _BOOT_STARTED
    if _BOOT_STARTED:
        return
    _BOOT_STARTED = True
    try:
        import main as nova_main  # contains boot() and receipts blueprint already wired
        def _bg():
            try:
                nova_main.boot()
            except Exception as e:
                print(f"[WEB] Nova boot failed: {e}")
        t = threading.Thread(target=_bg, daemon=True)
        t.start()
        print("[WEB] Nova boot thread started.")
    except Exception as e:
        print(f"[WEB] Unable to import main/boot: {e}")

# Control via env; default ON in Render
if os.getenv("RUN_BOOT_IN_WSGI", "1").strip().lower() in {"1", "true", "yes"}:
    _start_boot_once()
