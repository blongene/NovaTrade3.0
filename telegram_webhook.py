# telegram_webhook.py — cleaned for Render boot
import os
import requests
from flask import Flask, request, jsonify

# --- ENV ---
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID")

# Prefer Render’s external URL; fall back to your manual setting
PUBLIC_BASE_URL = (
    os.getenv("PUBLIC_BASE_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or os.getenv("RENDER_WEBHOOK_URL")
)

# --- Flask app (create FIRST) ---
telegram_app = Flask(__name__)

# --- Routes ---
@telegram_app.get("/")
def health():
    return "ok", 200

@telegram_app.post(f"/{BOT_TOKEN or 'token'}")
def telegram_hook():
    try:
        payload = request.get_json(silent=True) or {}
        msg = (payload.get("message") or payload.get("edited_message") or {})
        text = (msg.get("text") or "").strip()

        # Minimal echo/status
        if text == "/status" and BOT_TOKEN and CHAT_ID:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": "✅ NovaTrade online."},
                timeout=10,
            )
        return "ok", 200
    except Exception as e:
        # Don't 500 Telegram; return 200 so it won't keep retrying the same payload
        return f"err: {e}", 200

def set_telegram_webhook():
    if not BOT_TOKEN:
        print("⚠️ BOT_TOKEN missing; skipping webhook setup.")
        return
    if not PUBLIC_BASE_URL:
        print("⚠️ PUBLIC_BASE_URL/RENDER_EXTERNAL_URL missing; skipping webhook setup.")
        return
    url = f"{PUBLIC_BASE_URL.rstrip('/')}/{BOT_TOKEN}"
    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
            params={"url": url},
            timeout=10,
        )
        ok = resp.json().get("ok")
        print(f"📡 Set webhook → {url} | ok={ok}")
    except Exception as e:
        print(f"❌ setWebhook failed: {e}")

# --- Command Bus blueprint (register AFTER app is created) ---
from api_commands import bp as cmdapi_bp
telegram_app.register_blueprint(cmdapi_bp)

# (Optional) If you want belt-and-suspenders DB init, uncomment:
from outbox_db import init as outbox_init
outbox_init()  # DDL is idempotent
