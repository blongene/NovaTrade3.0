# telegram_webhook.py — Safe Telegram webhook handler for NovaTrade Bus
from __future__ import annotations
import os, logging
from typing import Optional
from flask import Blueprint, request, jsonify

log = logging.getLogger("tg")

# --- Env ---------------------------------------------------------------------
BOT_TOKEN  = os.getenv("BOT_TOKEN")
CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID")
ENABLE_TELEGRAM = os.getenv("ENABLE_TELEGRAM","0").lower() in ("1","true","yes")

# Dedup / summaries
DEDUP_TTL_MIN        = int(os.getenv("TG_DEDUP_TTL_MIN","1"))
SUMMARIES_ENABLED    = os.getenv("TELEGRAM_SUMMARIES_ENABLED","0").lower() in ("1","true","yes")
SUMMARIES_TTL_MIN    = int(os.getenv("TELEGRAM_SUMMARIES_TTL_MIN","720"))

# Timeout + secrets
TIMEOUT_SEC     = int(os.getenv("TG_TIMEOUT_SEC","10"))
WEBHOOK_SECRET  = os.getenv("WEBHOOK_SECRET") or os.getenv("TELEGRAM_WEBHOOK_SECRET")

# Build or read full webhook URL
BASE = os.getenv("TELEGRAM_WEBHOOK_BASE") or os.getenv("BASE_URL")
WEBHOOK_URL = (
    os.getenv("WEBHOOK_URL")
    or (f"{BASE.rstrip('/')}/tg/webhook" if BASE else None)
)

# --- Blueprint ---------------------------------------------------------------
tg_blueprint = Blueprint("tg", __name__)

def _ok(**kw):
    return jsonify(dict(ok=True, **kw)), 200

def _bad(msg: str, code: int = 400):
    return jsonify(dict(ok=False, error=msg)), code

def _send_telegram(text: str, chat_id: Optional[str] = None) -> bool:
    """Send a message; returns True/False. Quiet on failure."""
    token = BOT_TOKEN
    if not token:
        return False
    cid = chat_id or CHAT_ID
    if not cid:
        return False
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": cid, "text": text[:4000], "parse_mode": "HTML"},
            timeout=TIMEOUT_SEC,
        )
        return True
    except Exception as e:
        log.debug("send degraded: %s", e)
        return False

# --- Health ------------------------------------------------------------------
@tg_blueprint.get("/health")
def tg_health():
    status = "ok" if BOT_TOKEN and (CHAT_ID or WEBHOOK_URL) else "degraded"
    return _ok(service="telegram", status=status, webhook=WEBHOOK_URL)

# --- Webhook -----------------------------------------------------------------
@tg_blueprint.post("/webhook")
def tg_webhook():
    """Webhook endpoint mounted at /tg/webhook by the Bus."""
    # Optional shared secret, via query string or header
    if WEBHOOK_SECRET:
        got = request.args.get("secret") or request.headers.get("X-TG-Secret")
        if (got or "") != WEBHOOK_SECRET:
            return _bad("forbidden", 403)

    try:
        data = request.get_json(silent=True) or {}
        msg  = (data.get("message") or data.get("edited_message") or {}) or {}
        text = (msg.get("text") or "").strip()
        chat = (msg.get("chat") or {}).get("id")

        if text.lower() in ("/id", "id"):
            _send_telegram(f"chat_id = <code>{chat}</code>", chat_id=str(chat) if chat else None)
            return _ok(received=True)

        if text.lower() in ("/ping", "ping"):
            _send_telegram("🏓 pong", chat_id=str(chat) if chat else None)

        # extend here if needed
        return _ok(received=bool(data))
    except Exception as e:
        log.info("webhook degraded: %s", e)
        return _ok(received=False, degraded=str(e))

# --- Webhook registration ----------------------------------------------------
def set_telegram_webhook():
    """Best-effort webhook registration with Telegram."""
    token = BOT_TOKEN
    url   = WEBHOOK_URL
    if not token or not url:
        log.info("Telegram webhook skipped (missing BOT_TOKEN or WEBHOOK_URL)")
        return

    # Append secret query if configured
    if WEBHOOK_SECRET and "secret=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}secret={WEBHOOK_SECRET}"

    try:
        import requests
        r = requests.post(
            f"https://api.telegram.org/bot{token}/setWebhook",
            json={"url": url},
            timeout=TIMEOUT_SEC,
        )
        ok = r.ok and r.json().get("ok", False)
        if ok:
            log.info("Telegram webhook set: %s", url)
        else:
            log.warning("setWebhook degraded: %s", r.text)
    except Exception as e:
        log.warning("setWebhook error: %s", e)
