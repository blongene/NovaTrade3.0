# telegram_webhook.py — Render-safe Telegram webhook + Command Bus registration
import os
import requests
from flask import Flask, request, jsonify

# ---------- ENV ----------
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID")

PUBLIC_BASE_URL = (
    os.getenv("PUBLIC_BASE_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or os.getenv("RENDER_WEBHOOK_URL")
)

SKIP_WEBHOOK = os.getenv("TELEGRAM_SKIP_WEBHOOK", "0").strip().lower() in {"1","true","yes"}

# ---------- Flask app (create FIRST) ----------
telegram_app = Flask(__name__)
telegram_app.config["PROPAGATE_EXCEPTIONS"] = False

# ---------- Health ----------
@telegram_app.get("/")
def _root_ok():
    # ultra-light probe endpoint
    return "ok", 200

@telegram_app.get("/healthz")
def _healthz():
    return jsonify(ok=True), 200

# ---------- Webhook route ----------
# If BOT_TOKEN is present, Telegram will call /<BOT_TOKEN>. Otherwise, expose a harmless /token path.
WEBHOOK_PATH = f"/{BOT_TOKEN}" if BOT_TOKEN else "/token"

@telegram_app.post(WEBHOOK_PATH)
def telegram_hook():
    """
    Minimal, safe handler:
    - Echoes /status with a confirmation ping to your chat, if CHAT_ID present.
    - Always returns 200 to avoid Telegram retries even on parsing errors.
    """
    try:
        payload = request.get_json(silent=True) or {}
        msg = (payload.get("message") or payload.get("edited_message") or {})
        text = (msg.get("text") or "").strip()

        if text == "/status" and BOT_TOKEN and CHAT_ID:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={"chat_id": CHAT_ID, "text": "✅ NovaTrade online."},
                    timeout=10,
                )
            except Exception as send_err:
                telegram_app.logger.warning(f"[tg] sendMessage failed: {send_err}")
        return "ok", 200
    except Exception as err:
        # Never 500 Telegram payloads—ack to stop retries, log the issue.
        telegram_app.logger.warning(f"[tg] webhook parse skipped: {err}")
        return "ok", 200

# ---------- Optional: set Telegram webhook ----------
def set_telegram_webhook():
    if SKIP_WEBHOOK:
        print("[TG] SKIP_WEBHOOK=1 — not setting Telegram webhook.")
        return False
    if not BOT_TOKEN:
        print("[TG] BOT_TOKEN missing — skipping webhook setup.")
        return False
    if not PUBLIC_BASE_URL:
        print("[TG] PUBLIC_BASE_URL/RENDER_EXTERNAL_URL missing — skipping webhook setup.")
        return False

    url = f"{PUBLIC_BASE_URL.rstrip('/')}{WEBHOOK_PATH}"
    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
            params={"url": url},
            timeout=10,
        )
        ok = False
        try:
            ok = bool(resp.json().get("ok"))
        except Exception:
            pass
        print(f"[TG] setWebhook → {resp.status_code} ok={ok} url={url}")
        return ok
    except Exception as err:
        print(f"[TG] setWebhook failed: {err}")
        return False

# ---------- Command Bus blueprint (best-effort) ----------
try:
    from api_commands import bp as cmdapi_bp  # exposes /api/commands/*
    telegram_app.register_blueprint(cmdapi_bp)
    print("[WEB] Command Bus API registered on telegram_app.")
except Exception as err:
    print(f"[WEB] Command Bus API not available: {err}")

# ---------- Optional: initialize Outbox DB (idempotent) ----------
try:
    from outbox_db import init as outbox_init
    outbox_init()
except Exception as err:
    print(f"[WEB] outbox_init skipped: {err}")
