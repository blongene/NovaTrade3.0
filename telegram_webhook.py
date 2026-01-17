#!/usr/bin/env python3
"""
telegram_webhook.py — NovaTrade Bus
Phase 28.x+ Telegram Decision Buttons

- Receives Telegram webhook updates (message + callback_query)
- Can send inline-button prompts
- On button press: answers callback + enqueues a NOTE intent via local /ops/enqueue

Env:
  BOT_TOKEN or TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID (optional; if absent, uses incoming chat_id)
  TELEGRAM_WEBHOOK_SECRET (optional; if set, validates Telegram secret header)
  PORT (Render) default 10000
  DEFAULT_AGENT_ID (optional; default edge-primary)
  TELEGRAM_PROMPT_TTL_MIN (optional; default 180)
"""

from __future__ import annotations

import os
import json
import time
import logging
from typing import Any, Dict, List, Optional

import requests
from flask import Blueprint, request, jsonify

log = logging.getLogger("telegram")

BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or ""
DEFAULT_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") or ""
WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET") or ""  # Telegram "secret token" header
PORT = os.getenv("PORT", "10000")
DEFAULT_AGENT_ID = os.getenv("DEFAULT_AGENT_ID", "edge-primary")
PROMPT_TTL_MIN = int(os.getenv("TELEGRAM_PROMPT_TTL_MIN", "180"))

bp_telegram = Blueprint("telegram_webhook", __name__)

# ----------------------------
# Very small, in-process de-dupe (covers retry storms / duplicate callbacks)
# ----------------------------
_dedup: Dict[str, float] = {}


def _dedup_ok(key: str, ttl_s: int) -> bool:
    now = time.time()
    last = _dedup.get(key, 0.0)
    if now - last < ttl_s:
        return False
    _dedup[key] = now
    return True


def _tg_api(method: str) -> str:
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"


def _tg_post(method: str, payload: Dict[str, Any], timeout: int = 12) -> Dict[str, Any]:
    if not BOT_TOKEN:
        return {"ok": False, "error": "missing BOT_TOKEN"}
    try:
        r = requests.post(_tg_api(method), json=payload, timeout=timeout)
        if r.status_code != 200:
            return {"ok": False, "status": r.status_code, "body": r.text}
        return r.json() if r.text else {"ok": True}
    except Exception as e:
        return {"ok": False, "error": f"telegram_post_failed: {e!r}"}


def _send_message(chat_id: str, text: str, parse_mode: str = "Markdown") -> Dict[str, Any]:
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    return _tg_post("sendMessage", payload)


def _send_prompt(chat_id: str, text: str, buttons: List[List[Dict[str, str]]], parse_mode: str = "Markdown") -> Dict[str, Any]:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "reply_markup": {"inline_keyboard": buttons},
    }
    return _tg_post("sendMessage", payload)


def _answer_callback(callback_query_id: str, text: str = "", show_alert: bool = False) -> Dict[str, Any]:
    payload = {"callback_query_id": callback_query_id, "text": text, "show_alert": bool(show_alert)}
    return _tg_post("answerCallbackQuery", payload)


def _local_ops_enqueue(agent_id: str, intent: Dict[str, Any], idempotency_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Enqueue through the Bus itself so we use the same canonical pipeline.
    Uses localhost to avoid depending on external routing.
    """
    try:
        url = f"http://127.0.0.1:{PORT}/ops/enqueue"
        body: Dict[str, Any] = {"agent_id": agent_id, "intent": intent}
        if idempotency_key:
            body["idempotency_key"] = idempotency_key
        r = requests.post(url, json=body, timeout=8)
        if r.status_code != 200:
            return {"ok": False, "status": r.status_code, "body": r.text}
        return r.json()
    except Exception as e:
        return {"ok": False, "error": f"enqueue_failed: {e!r}"}


def _coerce_chat_id(update: Dict[str, Any]) -> str:
    # Prefer explicit configured chat id; otherwise use the inbound chat.
    if DEFAULT_CHAT_ID:
        return DEFAULT_CHAT_ID
    msg = update.get("message") or update.get("edited_message") or {}
    chat = msg.get("chat") or {}
    cid = chat.get("id")
    return str(cid) if cid is not None else ""


def _verify_webhook_secret() -> bool:
    if not WEBHOOK_SECRET:
        return True
    got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    return bool(got) and got == WEBHOOK_SECRET


# ----------------------------
# Public webhook
# ----------------------------
@bp_telegram.post("/telegram/webhook")
def telegram_webhook():
    if not _verify_webhook_secret():
        return jsonify({"ok": False, "error": "invalid_webhook_secret"}), 401

    update = request.get_json(silent=True) or {}
    # Telegram may retry; make retry a fast no-op if we saw update_id recently
    uid = update.get("update_id")
    if uid is not None:
        if not _dedup_ok(f"upd:{uid}", ttl_s=30):
            return jsonify({"ok": True, "dedup": True})

    # Handle callback buttons
    if "callback_query" in update:
        return jsonify(_handle_callback(update["callback_query"], update))

    # Handle messages (optional command parsing)
    if "message" in update or "edited_message" in update:
        msg = update.get("message") or update.get("edited_message") or {}
        return jsonify(_handle_message(msg, update))

    return jsonify({"ok": True})


def _handle_message(msg: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
    chat_id = str((msg.get("chat") or {}).get("id") or _coerce_chat_id(update) or "")
    text = (msg.get("text") or "").strip()

    # Minimal operator QoL:
    # /ping -> pong
    # /yesno <prompt_id> <text...>  (sends yes/no/maybe buttons)
    # /bsh <prompt_id> <text...>    (sends buy/sell/hold buttons)
    if text.lower().startswith("/ping"):
        if chat_id:
            _send_message(chat_id, "✅ pong")
        return {"ok": True}

    # Only treat as commands if starts with /
    if not text.startswith("/"):
        return {"ok": True}

    parts = text.split(maxsplit=2)
    cmd = parts[0].lower()

    if cmd in ("/yesno", "/yn"):
        prompt_id = parts[1] if len(parts) >= 2 else f"yn:{int(time.time())}"
        prompt_text = parts[2] if len(parts) >= 3 else "Decision?"
        return _send_standard_prompt(chat_id, prompt_id, prompt_text, mode="YESNO")

    if cmd in ("/bsh", "/buy"):
        prompt_id = parts[1] if len(parts) >= 2 else f"bsh:{int(time.time())}"
        prompt_text = parts[2] if len(parts) >= 3 else "Trade action?"
        return _send_standard_prompt(chat_id, prompt_id, prompt_text, mode="BSH")

    if chat_id:
        _send_message(chat_id, "ℹ️ Commands: /ping, /yesno <id> <text>, /bsh <id> <text>")
    return {"ok": True}


def _send_standard_prompt(chat_id: str, prompt_id: str, prompt_text: str, mode: str) -> Dict[str, Any]:
    if not chat_id:
        return {"ok": False, "error": "missing chat_id"}

    mode = (mode or "YESNO").upper()
    if mode == "BSH":
        choices = ["BUY", "SELL", "HOLD"]
    else:
        choices = ["YES", "NO", "MAYBE"]

    # callback_data is compact and deterministic:
    # nt|<prompt_id>|<choice>
    rows: List[List[Dict[str, str]]] = [
        [{"text": c, "callback_data": f"nt|{prompt_id}|{c}"} for c in choices]
    ]

    # Light de-dupe on the prompt itself
    if not _dedup_ok(f"prompt:{prompt_id}:{mode}", ttl_s=60):
        return {"ok": True, "dedup": True}

    r = _send_prompt(chat_id, prompt_text, rows)
    return {"ok": True, "sent": True, "telegram": r, "prompt_id": prompt_id, "mode": mode}


def _handle_callback(cb: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
    cb_id = cb.get("id") or ""
    data = (cb.get("data") or "").strip()

    # Always answer callback quickly to stop the Telegram spinner
    if cb_id:
        _answer_callback(cb_id, text="✅ received")

    # De-dupe callbacks (Telegram retries / double taps)
    if data:
        if not _dedup_ok(f"cb:{data}", ttl_s=10):
            return {"ok": True, "dedup": True}

    from_user = cb.get("from") or {}
    who = from_user.get("username") or from_user.get("first_name") or "operator"
    msg = cb.get("message") or {}
    chat = msg.get("chat") or {}
    chat_id = str(chat.get("id") or _coerce_chat_id(update) or DEFAULT_CHAT_ID or "")

    # Parse our callback_data format
    # nt|<prompt_id>|<choice>
    prompt_id = ""
    choice = ""
    if data.startswith("nt|"):
        parts = data.split("|", 2)
        if len(parts) == 3:
            _, prompt_id, choice = parts

    # If we can’t parse, still record it.
    if not prompt_id:
        prompt_id = f"unknown:{int(time.time())}"
    if not choice:
        choice = data or "UNKNOWN"

    # Emit a small confirmation back to Telegram (optional)
    if chat_id:
        _send_message(chat_id, f"🗳️ *{prompt_id}*: `{choice}` — by *{who}*")

    # Enqueue a NOTE intent to the default agent (or operator’s desired agent later)
    intent = {
        "type": "NOTE",
        "phase": "phase28",
        "mode": "dryrun",
        "payload": {
            "note": "telegram_button",
            "prompt_id": prompt_id,
            "choice": choice,
            "who": who,
            "data": data,
            "chat_id": chat_id,
            "message_id": msg.get("message_id"),
            "ts": int(time.time()),
        },
    }

    idem = f"tg:{prompt_id}:{choice}:{chat_id}:{msg.get('message_id')}"
    enq = _local_ops_enqueue(DEFAULT_AGENT_ID, intent, idempotency_key=idem)
    if not enq.get("ok"):
        log.warning("telegram callback enqueue failed: %s", enq)

    return {"ok": True, "enqueued": enq, "prompt_id": prompt_id, "choice": choice}


# ----------------------------
# Optional internal endpoints (handy for Bus modules calling prompts)
# ----------------------------
@bp_telegram.post("/telegram/send")
def telegram_send():
    """
    Internal helper: POST { "chat_id"?, "text": "...", "parse_mode"?: "Markdown" }
    """
    body = request.get_json(silent=True) or {}
    chat_id = str(body.get("chat_id") or DEFAULT_CHAT_ID or "")
    text = str(body.get("text") or "")
    parse_mode = str(body.get("parse_mode") or "Markdown")
    if not chat_id or not text:
        return jsonify({"ok": False, "error": "missing chat_id or text"}), 400
    r = _send_message(chat_id, text, parse_mode=parse_mode)
    return jsonify({"ok": True, "telegram": r})


@bp_telegram.post("/telegram/prompt")
def telegram_prompt():
    """
    Internal helper:
      POST {
        "chat_id"?,
        "prompt_id": "alpha:123",
        "text": "Approve?",
        "mode": "YESNO"|"BSH" (optional)
      }
    """
    body = request.get_json(silent=True) or {}
    chat_id = str(body.get("chat_id") or DEFAULT_CHAT_ID or "")
    prompt_id = str(body.get("prompt_id") or f"prompt:{int(time.time())}")
    text = str(body.get("text") or "Decision?")
    mode = str(body.get("mode") or "YESNO")
    out = _send_standard_prompt(chat_id, prompt_id, text, mode=mode)
    return jsonify(out)
