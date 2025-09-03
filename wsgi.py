# wsgi.py — production entrypoint for Gunicorn on Render
from main import boot
boot()  # start schedulers/threads/watchdogs etc.

# Try your real Flask app first; fall back to a minimal app that binds.
try:
    from telegram_webhook import telegram_app as app
except Exception as e:
    # Fallback so Render always sees a bound port even if telegram_webhook import fails.
    from flask import Flask, jsonify
    app = Flask(__name__)
    @app.get("/health")
    def _health():
        return jsonify(ok=True, fallback=True, reason=str(e)), 200
