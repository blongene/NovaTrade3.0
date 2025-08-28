# telegram_webhook.py
import os
import requests
from flask import Flask, request
from utils import send_telegram_message_dedup

app = Flask(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")

# Prefer Render’s built-in external URL; fall back to a manual PUBLIC_BASE_URL
PUBLIC_BASE_URL = (
    os.getenv("PUBLIC_BASE_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or os.getenv("RENDER_WEBHOOK_URL")  # if you set it yourself
)

@app.get("/")
def health():
    return "ok", 200

@app.post(f"/{BOT_TOKEN or 'token'}")
def telegram_hook():
    try:
        payload = request.get_json(silent=True) or {}
        # minimal echo/status
        msg = (payload.get("message") or payload.get("edited_message") or {})
        text = (msg.get("text") or "").strip()
        if text == "/status" and BOT_TOKEN and CHAT_ID:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": "✅ NovaTrade online."},
                timeout=10,
            )
        return "ok", 200
    except Exception as e:
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

# expose telegram_app so main.py can import it
telegram_app = app
