# telegram_webhook.py — Telegram webhook + button prompts (NovaTrade Bus, Phase 28+)
from __future__ import annotations

import os
import json
import time
import logging
from typing import Any, Dict, Optional, Tuple, List

from flask import Blueprint, request, jsonify

log = logging.getLogger("tg")

# ---- env ----
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_URL = os.getenv("DB_URL") or os.getenv("DATABASE_URL")

TG_TIMEOUT_SEC = int(os.getenv("TG_TIMEOUT_SEC", "10"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET") or os.getenv("TELEGRAM_WEBHOOK_SECRET")

# Optional: internal prompt protection (recommended, but fail-open if unset)
OPS_TOKEN = os.getenv("OPS_TOKEN") or os.getenv("NOVA_OPS_TOKEN")

# Build webhook URL (optional helper)
BASE = os.getenv("TELEGRAM_WEBHOOK_BASE") or os.getenv("BASE_URL")
WEBHOOK_URL = os.getenv("WEBHOOK_URL") or (f"{BASE.rstrip('/')}/tg/webhook" if BASE else None)

# ---- blueprint ----
bp_telegram = Blueprint("telegram", __name__)

def _ok(**kw):
    return jsonify(dict(ok=True, **kw)), 200

def _bad(msg: str, code: int = 400, **kw):
    return jsonify(dict(ok=False, error=msg, **kw)), code

def _tg_post(method: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
    if not BOT_TOKEN:
        return False, "missing BOT_TOKEN"
    try:
        import requests
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
            json=payload,
            timeout=TG_TIMEOUT_SEC,
        )
        if not r.ok:
            return False, r.text
        return True, r.text
    except Exception as e:
        return False, repr(e)

def _build_inline_keyboard(rows: List[List[Tuple[str, str]]]) -> Dict[str, Any]:
    # rows = [[("YES","cb"), ("NO","cb")], ...]
    return {
        "inline_keyboard": [
            [{"text": str(label), "callback_data": str(cb)} for (label, cb) in row]
            for row in rows
        ]
    }

def _parse_callback_data(cb: str) -> Dict[str, str]:
    """
    Supports:
      - "nt|<key>|<choice>"
      - "nt:<key>:<choice>"
      - "key=<k>&choice=<c>"
      - plain strings (become choice)
    """
    cb = (cb or "").strip()
    if cb.startswith("nt|"):
        parts = cb.split("|", 2)
        if len(parts) == 3:
            return {"key": parts[1], "choice": parts[2]}
    if cb.startswith("nt:"):
        parts = cb.split(":", 2)
        if len(parts) == 3:
            return {"key": parts[1], "choice": parts[2]}
    if "key=" in cb and "choice=" in cb:
        try:
            # very small parser, no deps
            out = {}
            for kv in cb.split("&"):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    out[k.strip()] = v.strip()
            if "key" in out and "choice" in out:
                return {"key": out["key"], "choice": out["choice"]}
        except Exception:
            pass
    return {"key": "", "choice": cb}

def _db_exec(sql: str, params: Tuple[Any, ...] = ()) -> None:
    if not DB_URL:
        return
    import psycopg2
    conn = psycopg2.connect(DB_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
    finally:
        conn.close()

def _ensure_schema() -> None:
    if not DB_URL:
        return
    _db_exec(
        """
        create table if not exists telegram_decisions (
          id bigserial primary key,
          created_at timestamptz not null default now(),
          key text,
          choice text,
          chat_id text,
          user_id text,
          username text,
          message_id bigint,
          callback_id text,
          raw jsonb
        );
        create index if not exists idx_telegram_decisions_created_at on telegram_decisions(created_at desc);
        create index if not exists idx_telegram_decisions_key on telegram_decisions(key);
        """
    )

def _record_decision(
    key: str,
    choice: str,
    chat_id: Optional[str],
    user_id: Optional[str],
    username: Optional[str],
    message_id: Optional[int],
    callback_id: Optional[str],
    raw: Dict[str, Any],
) -> None:
    if not DB_URL:
        return
    _ensure_schema()
    _db_exec(
        """
        insert into telegram_decisions
          (key, choice, chat_id, user_id, username, message_id, callback_id, raw)
        values (%s,%s,%s,%s,%s,%s,%s,%s);
        """,
        (
            key or "",
            choice or "",
            str(chat_id) if chat_id is not None else None,
            str(user_id) if user_id is not None else None,
            str(username) if username is not None else None,
            int(message_id) if message_id is not None else None,
            str(callback_id) if callback_id is not None else None,
            json.dumps(raw),
        ),
    )

# ---- routes ----

@bp_telegram.get("/tg/health")
def tg_health():
    status = "ok" if BOT_TOKEN else "degraded"
    return _ok(service="telegram", status=status, webhook=WEBHOOK_URL, db=bool(DB_URL))

@bp_telegram.post("/tg/webhook")
def tg_webhook():
    # Optional shared secret
    if WEBHOOK_SECRET:
        got = request.args.get("secret") or request.headers.get("X-TG-Secret")
        if (got or "") != WEBHOOK_SECRET:
            return _bad("forbidden", 403)

    data = request.get_json(silent=True) or {}

    # 1) callback buttons
    cbq = data.get("callback_query")
    if isinstance(cbq, dict) and cbq:
        cb_id = cbq.get("id")
        cb_data = (cbq.get("data") or "").strip()
        parsed = _parse_callback_data(cb_data)

        frm = cbq.get("from") or {}
        msg = cbq.get("message") or {}
        chat = msg.get("chat") or {}

        key = parsed.get("key") or ""
        choice = parsed.get("choice") or ""

        try:
            _record_decision(
                key=key,
                choice=choice,
                chat_id=str(chat.get("id")) if chat.get("id") is not None else None,
                user_id=str(frm.get("id")) if frm.get("id") is not None else None,
                username=frm.get("username") or frm.get("first_name") or None,
                message_id=msg.get("message_id"),
                callback_id=cb_id,
                raw=data,
            )
        except Exception as e:
            log.warning("decision record failed: %r", e)

        # Acknowledge callback to stop Telegram spinner
        _tg_post("answerCallbackQuery", {"callback_query_id": cb_id, "text": f"✅ {choice}"[:200]})

        # Optional: edit message or post a small confirmation
        # Keep minimal spam; do nothing else by default.
        return _ok(received=True, kind="callback", key=key, choice=choice)

    # 2) text messages (optional tiny commands)
    msg = data.get("message") or data.get("edited_message") or {}
    if isinstance(msg, dict) and msg:
        text = (msg.get("text") or "").strip()
        chat_id = (msg.get("chat") or {}).get("id")

        if text.lower() in ("/ping", "ping"):
            _tg_post("sendMessage", {"chat_id": chat_id, "text": "🏓 pong"})
        if text.lower() in ("/id", "id"):
            _tg_post("sendMessage", {"chat_id": chat_id, "text": f"chat_id={chat_id}"})
        return _ok(received=True, kind="message")

    return _ok(received=bool(data), kind="unknown")

@bp_telegram.post("/telegram/prompt")
def telegram_prompt():
    """
    Internal endpoint for Bus->Telegram prompts.

    Body:
      {
        "prompt_id": "unique-key",
        "text": "Question?",
        "mode": "YESNO" | "BUYSELLHOLD" | "CUSTOM",
        "buttons": [["YES","NO","MAYBE"]]  # for CUSTOM
      }
    """
    # Optional auth gate
    if OPS_TOKEN:
        got = request.headers.get("X-OPS-TOKEN") or request.args.get("token")
        if (got or "") != OPS_TOKEN:
            return _bad("forbidden", 403)

    j = request.get_json(silent=True) or {}
    prompt_id = str(j.get("prompt_id") or j.get("key") or f"prompt:{int(time.time())}")
    text = str(j.get("text") or "").strip()
    mode = str(j.get("mode") or "YESNO").upper()

    if not text:
        return _bad("missing text", 400)

    if not CHAT_ID:
        return _bad("missing TELEGRAM_CHAT_ID", 400)

    # Build buttons
    if mode == "YESNO":
        labels = ["YES", "NO", "MAYBE"]
        rows = [[(lab, f"nt|{prompt_id}|{lab}") for lab in labels]]
    elif mode == "BUYSELLHOLD":
        labels = ["BUY", "SELL", "HOLD"]
        rows = [[(lab, f"nt|{prompt_id}|{lab}") for lab in labels]]
    else:
        # CUSTOM
        btns = j.get("buttons") or [["YES", "NO"]]
        rows = []
        for row in btns:
            if not isinstance(row, list):
                row = [row]
            rows.append([(str(lab), f"nt|{prompt_id}|{str(lab)}") for lab in row])

    payload = {
        "chat_id": CHAT_ID,
        "text": text[:4000],
        "reply_markup": _build_inline_keyboard(rows),
    }
    ok, detail = _tg_post("sendMessage", payload)
    return _ok(sent=ok, prompt_id=prompt_id, detail=detail, mode=mode)

def set_telegram_webhook() -> None:
    """Best-effort webhook registration with Telegram."""
    if not BOT_TOKEN or not WEBHOOK_URL:
        log.info("Telegram webhook skipped (missing BOT_TOKEN or WEBHOOK_URL)")
        return

    url = WEBHOOK_URL
    if WEBHOOK_SECRET and "secret=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}secret={WEBHOOK_SECRET}"

    ok, detail = _tg_post("setWebhook", {"url": url})
    if ok:
        log.info("Telegram webhook set: %s", url)
    else:
        log.warning("setWebhook degraded: %s", detail)
