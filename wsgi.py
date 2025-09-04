# wsgi.py — web entrypoint (binds immediately, no schedulers here)
try:
    from telegram_webhook import telegram_app as app, set_telegram_webhook
    # Configure webhook once at web start (safe no-op if already set)
    try:
        set_telegram_webhook()
    except Exception as e:
        print(f"[WEB] webhook setup skipped: {e}")
except Exception as e:
    # Fallback so Render always sees a bound port
    from flask import Flask, jsonify
    app = Flask(__name__)
    @app.get("/health")
    def _health_fallback():
        return jsonify(ok=True, fallback=True, reason=str(e)), 200

# Lightweight health even when telegram_webhook is present
try:
    from flask import jsonify
    @app.get("/health")
    def _health_ok():
        return jsonify(ok=True), 200
except Exception:
    pass
