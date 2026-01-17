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
# NOTE: The canonical blueprint name is `tg_blueprint`.
# We also export `bp_telegram` as a backward-compatible alias, because some
# older wsgi.py versions import `bp_telegram`.
tg_blueprint = Blueprint("tg", __name__)
bp_telegram = tg_blueprint

def _ok(**kw):
    return jsonify(dict(ok=True, **kw)), 200

def _bad(msg: str, code: int = 400):
    return jsonify(dict(ok=False, error=msg)), code

def _build_inline_keyboard(buttons: Any) -> Dict[str, Any]:
    """Build Telegram inline keyboard payload.

    Supported inputs:
      - [[(label, data), ...], ...] (rows)
      - [(label, data), ...]       (single row)
      - ["YES", "NO"]             (labels = callback_data)
    """
    if not buttons:
        buttons = ["YES", "NO"]

    # Normalize to rows
    if isinstance(buttons, (tuple, list)) and buttons and not isinstance(buttons[0], (tuple, list)):
        rows = [buttons]
    else:
        rows = buttons if isinstance(buttons, list) else [[buttons]]

    def to_btn(x: Any) -> Dict[str, Any]:
        if isinstance(x, (tuple, list)) and len(x) >= 2:
            label, val = x[0], x[1]
        else:
            label, val = str(x), str(x)
        val_s = str(val)
        # URL buttons
        if val_s.lower().startswith(("http://", "https://", "tg://")):
            return {"text": str(label), "url": val_s}
        return {"text": str(label), "callback_data": val_s[:64]}

    keyboard = []
    for row in rows:
        if not isinstance(row, (list, tuple)):
            row = [row]
        keyboard.append([to_btn(i) for i in row])

    return {"inline_keyboard": keyboard}


def _send_telegram(text: str, chat_id: Optional[str] = None, buttons: Any = None) -> bool:
    """Send a message (optionally with buttons). Returns True/False; quiet on failure."""
    token = BOT_TOKEN
    if not token:
        return False
    cid = chat_id or CHAT_ID
    if not cid:
        return False
    try:
        import requests
        payload: Dict[str, Any] = {"chat_id": cid, "text": text[:4000], "parse_mode": "HTML"}
        if buttons:
            payload["reply_markup"] = _build_inline_keyboard(buttons)
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json=payload,
            timeout=TIMEOUT_SEC,
        )
        return True
    except Exception as e:
        log.debug("send degraded: %s", e)
        return False


def _answer_callback(callback_query_id: str, text: str = "") -> None:
    """Best-effort ack to stop Telegram client spinner."""
    if not BOT_TOKEN or not callback_query_id:
        return
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": callback_query_id, "text": text[:200]},
            timeout=TIMEOUT_SEC,
        )
    except Exception:
        return


def _ops_secret_ok() -> bool:
    secret = os.getenv("OPS_SECRET") or WEBHOOK_SECRET or ""
    if not secret:
        # If no secret configured, allow (local/dev). This is intentionally permissive.
        return True
    got = request.headers.get("X-OPS-SECRET") or request.args.get("secret") or ""
    return got == secret


def _db_write_callback(agent: str, payload: Dict[str, Any]) -> None:
    """Optional persistence of callbacks for audit/approvals."""
    db_url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        return
    try:
        import psycopg2
        conn = psycopg2.connect(db_url)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                create table if not exists telegram_callbacks (
                  id bigserial primary key,
                  created_at timestamptz not null default now(),
                  agent_id text,
                  payload jsonb not null
                );
                """
            )
            cur.execute(
                "insert into telegram_callbacks(agent_id, payload) values (%s, %s::jsonb)",
                (agent, json.dumps(payload)),
            )
            conn.commit()
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        log.debug("callback db write degraded: %s", e)

# --- Health ------------------------------------------------------------------
@tg_blueprint.get("/health")
def tg_health():
    status = "ok" if BOT_TOKEN and (CHAT_ID or WEBHOOK_URL) else "degraded"
    return _ok(service="telegram", status=status, webhook=WEBHOOK_URL)


# --- Prompt sender ------------------------------------------------------------
# This is a *local* control-plane endpoint (called by Bus modules / ops) to
# send an inline-button prompt into Telegram.
@tg_blueprint.post("/prompt")
def tg_prompt():
    # Simple shared-secret auth (prevents random internet abuse)
    ops_secret = os.getenv("OPS_SECRET") or WEBHOOK_SECRET
    if ops_secret:
        got = request.headers.get("X-OPS-SECRET") or request.args.get("secret")
        if (got or "") != ops_secret:
            return _bad("forbidden", 403)

    j = request.get_json(silent=True) or {}
    text = str(j.get("text") or j.get("message") or "").strip()
    if not text:
        return _bad("missing text")

    buttons = j.get("buttons")
    chat_id = j.get("chat_id") or j.get("chat") or CHAT_ID

    ok = _send_telegram(text, chat_id=str(chat_id) if chat_id else None, buttons=buttons)
    return _ok(sent=bool(ok))

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

        # 1) Button callbacks arrive as callback_query
        cb = (data.get("callback_query") or {}) or {}
        if cb:
            cb_data = str(cb.get("data") or "").strip()
            cb_id = cb.get("id")
            from_user = (cb.get("from") or {}) or {}
            user = from_user.get("username") or from_user.get("first_name") or ""
            msg = (cb.get("message") or {}) or {}
            chat = (msg.get("chat") or {}).get("id")

            # Persist best-effort (audit trail)
            _db_write_callback(agent="telegram", payload={"callback_data": cb_data, "user": user, "update": data})

            # Answer callback (best-effort) to remove the loading spinner
            try:
                token = BOT_TOKEN
                if token and cb_id:
                    import requests
                    requests.post(
                        f"https://api.telegram.org/bot{token}/answerCallbackQuery",
                        json={"callback_query_id": cb_id, "text": "Received ✅"},
                        timeout=TIMEOUT_SEC,
                    )
            except Exception:
                pass

            # Small acknowledgement in chat (dedupe handled upstream if desired)
            if cb_data and chat:
                _send_telegram(f"✅ Received: <code>{cb_data}</code>", chat_id=str(chat))

            return _ok(received=True, callback=cb_data)

        # 2) Regular messages
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
