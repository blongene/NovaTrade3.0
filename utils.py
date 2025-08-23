# utils.py
import os
import time
import random
import json
import hashlib
import requests
import gspread
from functools import wraps
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

# =============================================================================
# Backoff / Retry Utilities
# =============================================================================

def with_sheet_backoff(fn):
    """Retry wrapper for Google Sheets 429/quota errors."""
    @wraps(fn)
    def _inner(*a, **k):
        delays = [2, 5, 15, 40]  # ~1 minute total
        for d in delays:
            try:
                return fn(*a, **k)
            except Exception as e:
                msg = str(e).lower()
                if "quota" in msg or "429" in msg or "rate limit" in msg:
                    print(f"⏳ Sheets 429/backoff ({d}s) in {fn.__name__}: {e}")
                    time.sleep(d)
                else:
                    raise
        # final attempt (let error bubble if it still fails)
        return fn(*a, **k)
    return _inner

def throttle_retry(max_retries=3, delay=2, jitter=1):
    """Generic retry with jitter for non-Sheets calls (e.g., HTTP)."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"⚠️ Attempt {attempt+1} failed in {func.__name__}: {e}")
                    if attempt < max_retries - 1:
                        sleep_time = delay + random.uniform(0, jitter)
                        time.sleep(sleep_time)
                    else:
                        raise
        return wrapper
    return decorator

# =============================================================================
# GSpread Helpers
# =============================================================================

SHEET_URL = os.getenv("SHEET_URL")

def _gspread_creds():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    return ServiceAccountCredentials.from_json_keyfile_name(
        "sentiment-log-service.json", scope
    )

def get_gspread_client():
    creds = _gspread_creds()
    return gspread.authorize(creds)

@with_sheet_backoff
def _open_sheet():
    return get_gspread_client().open_by_url(SHEET_URL)

# Generic backoff-wrapped actions
@with_sheet_backoff
def _ws_get_all_values(ws):
    return ws.get_all_values()

@with_sheet_backoff
def _ws_get_all_records(ws):
    return ws.get_all_records()

@with_sheet_backoff
def _ws_append_row(ws, row):
    return ws.append_row(row, value_input_option="USER_ENTERED")

@with_sheet_backoff
def _ws_update_cell(ws, r, c, v):
    return ws.update_cell(r, c, v)

@with_sheet_backoff
def _ws_update_acell(ws, a1, v):
    return ws.update_acell(a1, v)

@with_sheet_backoff
def _ws_update(ws, rng, rows):
    return ws.update(rng, rows, value_input_option="USER_ENTERED")

# =============================================================================
# Telegram + Debug
# =============================================================================

def ping_webhook_debug(msg):
    try:
        sh = _open_sheet()
        ws = sh.worksheet("Webhook_Debug")
        _ws_update_acell(ws, "A1", f"{datetime.now().isoformat()} - {msg}")
    except Exception:
        # Silent on purpose to avoid loops
        pass

@throttle_retry(max_retries=3, delay=2, jitter=1)
def send_telegram_message(message, chat_id=None, dedupe_key=None, ttl_minutes=None):
    """
    Sends a Telegram message with built-in de-duplication by content (or a custom key).
    If the same message/key was sent recently (within ttl), it will be skipped.

    ENV:
      TELEGRAM_DEDUP_TTL_MIN  -> default 10 minutes
      BOT_TOKEN / TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    """
    try:
        bot_token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
        if not bot_token or not chat_id:
            raise Exception("Missing BOT_TOKEN/TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")

        # --- de-dupe cache ---
        cache_path = "/tmp/telegram_dedupe.json"
        ttl = int(os.getenv("TELEGRAM_DEDUP_TTL_MIN", "10"))
        if ttl_minutes is not None:
            ttl = int(ttl_minutes)

        key_src = dedupe_key if dedupe_key else str(message)
        msg_key = hashlib.sha1(key_src.encode("utf-8")).hexdigest()

        cache = {}
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r") as f:
                    cache = json.load(f)
            except Exception:
                cache = {}

        now = time.time()
        last = cache.get(msg_key, 0)
        if now - last < ttl * 60:
            print(f"⏭️ Telegram dedupe: skipped resend within {ttl}m for key={msg_key[:8]}")
            return {"skipped": True, "reason": "dedupe", "ttl_minutes": ttl}

        # actually send
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        resp = requests.post(url, json=payload, timeout=10)
        if not resp.ok:
            raise Exception(resp.text)

        # update cache
        cache[msg_key] = now
        try:
            with open(cache_path, "w") as f:
                json.dump(cache, f)
        except Exception:
            pass

        return resp.json()

    except Exception as e:
        ping_webhook_debug(f"❌ Telegram send error: {e}")
        raise

def send_telegram_prompt(token, message, buttons=None, prefix="REBALANCE"):
    bot_token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        print("❌ BOT_TOKEN/TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not found.")
        return

    buttons = buttons or ["YES", "NO"]
    inline = [[{"text": btn, "callback_data": f"{btn}|{token}"}] for btn in buttons]
    payload = {
        "chat_id": chat_id,
        "text": f"🔁 *{prefix} ALERT*\n\n{message}",
        "parse_mode": "Markdown",
        "reply_markup": {"inline_keyboard": inline},
    }
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        r = requests.post(url, json=payload, timeout=10)
        if r.ok:
            print(f"✅ Telegram prompt sent for {token}")
        else:
            print(f"⚠️ Telegram error: {r.text}")
    except Exception as e:
        print(f"❌ Telegram prompt failed: {e}")

# ---- Convenience wrappers (throttled banners) -------------------------------

def notify_system_online():
    """At most once every 6h."""
    return send_telegram_message(
        "📡 NovaTrade System Online\nAll modules are active.\nYou will be notified if input is needed or a token stalls.",
        dedupe_key="system_online_banner",
        ttl_minutes=360
    )

def notify_sync_required():
    """At most once every 30m."""
    return send_telegram_message(
        "🧠 Sync Required\nNew decisions are pending rotation. Please review the planner tab.",
        dedupe_key="sync_required_banner",
        ttl_minutes=30
    )

def notify_sync_needed():
    """At most once every 30m."""
    return send_telegram_message(
        "🧩 NovaTrade Sync Needed\nPlease review the latest responses or re-run the sync loop.",
        dedupe_key="sync_needed_banner",
        ttl_minutes=30
    )

# =============================================================================
# Rotation / Scout Logging Utilities
# =============================================================================

def get_sheet():
    """Kept for backward-compat: returns the Spreadsheet object."""
    return _open_sheet()

def log_scout_decision(token, decision):
    """Log a YES/NO/SKIP to Scout Decisions; auto-confirm in Rotation_Planner on YES/VAULT/ROTATE."""
    token_u = (token or "").strip().upper()
    print(f"📥 Logging decision: {decision} for token {token_u}")
    try:
        sh = _open_sheet()
        ws = sh.worksheet("Scout Decisions")
        planner_ws = sh.worksheet("Rotation_Planner")

        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        _ws_append_row(ws, [now, token_u, (decision or '').upper(), "Telegram"])
        print("✅ Decision logged to Scout Decisions")

        if (decision or "").strip().upper() in {"YES", "VAULT", "ROTATE"}:
            planner_vals = _ws_get_all_values(planner_ws)
            if not planner_vals:
                return
            headers = planner_vals[0]
            try:
                token_idx = headers.index("Token")
                confirm_idx = headers.index("Confirmed")
            except ValueError:
                print("⚠️ Rotation_Planner missing 'Token' or 'Confirmed' headers.")
                return
            for i, row in enumerate(planner_vals[1:], start=2):
                if token_idx < len(row) and row[token_idx].strip().upper() == token_u:
                    _ws_update_cell(planner_ws, i, confirm_idx + 1, "YES")
                    print(f"✅ Auto-confirmed {token_u} in Rotation_Planner")
                    break
    except Exception as e:
        print(f"❌ Failed to log decision for {token_u}: {e}")
        ping_webhook_debug(f"❌ Log Scout Decision error: {e}")

def log_rebuy_decision(token):
    """Append a YES Rebuy row to Scout Decisions using context from Rotation_Log/Sentiment_Radar."""
    try:
        sh = _open_sheet()
        scout_ws = sh.worksheet("Scout Decisions")
        log_ws = sh.worksheet("Rotation_Log")
        radar_ws = sh.worksheet("Sentiment_Radar")

        token_u = (token or "").strip().upper()
        log_data = _ws_get_all_records(log_ws)
        log_row = next((r for r in log_data if (r.get("Token", "") or "").strip().upper() == token_u), {})

        score = log_row.get("Score", "")
        sentiment = log_row.get("Sentiment", "")
        market_cap = log_row.get("Market Cap", "")
        scout_url = log_row.get("Scout URL", "")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        if not sentiment:
            radar = _ws_get_all_records(radar_ws)
            rrow = next((r for r in radar if (r.get("Token", "") or "").strip().upper() == token_u), {})
            sentiment = rrow.get("Mentions", "")

        new_row = [timestamp, token_u, "YES", "Rebuy", score, sentiment, market_cap, scout_url, ""]
        _ws_append_row(scout_ws, new_row)
        print(f"✅ Rebuy for ${token_u} logged to Scout Decisions.")
    except Exception as e:
        print(f"❌ Failed to log rebuy decision for {token}: {e}")

def log_rotation_confirmation(token, decision):
    """Set 'User Response' in Rotation_Planner for a token."""
    try:
        sh = _open_sheet()
        planner_ws = sh.worksheet("Rotation_Planner")
        records = _ws_get_all_records(planner_ws)
        for i, row in enumerate(records, start=2):  # Skip header
            if (row.get("Token", "") or "").strip().upper() == (token or "").strip().upper():
                _ws_update_acell(planner_ws, f"C{i}", (decision or "").upper())  # Column C = 'User Response'
                print(f"✅ Rotation confirmation logged: {token} → {decision}")
                return
        print(f"⚠️ Token not found in Rotation_Planner: {token}")
    except Exception as e:
        print(f"❌ Error in log_rotation_confirmation: {e}")

def log_roi_feedback(token, decision):
    try:
        sh = _open_sheet()
        ws = sh.worksheet("ROI_Review_Log")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        _ws_append_row(ws, [timestamp, (token or "").upper(), (decision or "").upper()])
        print(f"✅ ROI Feedback logged: {token} → {decision}")
    except Exception as e:
        print(f"❌ Failed to log ROI Feedback: {e}")
        ping_webhook_debug(f"❌ ROI Feedback log error: {e}")

def log_vault_review(token, decision):
    try:
        sh = _open_sheet()
        ws = sh.worksheet("Vault_Review_Log")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        _ws_append_row(ws, [timestamp, (token or "").upper(), (decision or "").upper()])
        print(f"✅ Vault Review logged: {token} → {decision}")
    except Exception as e:
        print(f"❌ Failed to log Vault Review: {e}")
        ping_webhook_debug(f"❌ Vault Review log error: {e}")

def log_token_unlock(token, date):
    """Mark a token as Claimed/Resolved in Claim_Tracker and set Arrival Date."""
    try:
        sh = _open_sheet()
        ws = sh.worksheet("Claim_Tracker")
        rows = _ws_get_all_records(ws)
        token_u = (token or "").strip().upper()
        for i, row in enumerate(rows, start=2):  # Start at row 2
            if (row.get("Token", "") or "").strip().upper() == token_u:
                _ws_update_acell(ws, f"H{i}", "Claimed")   # Claimed?
                _ws_update_acell(ws, f"I{i}", "Resolved")  # Status
                _ws_update_acell(ws, f"G{i}", date)        # Arrival Date
                print(f"✅ Unlock logged for {token_u}")
                return
    except Exception as e:
        print(f"❌ Failed to log unlock for {token}: {e}")

def log_unclaimed_alert(token):
    try:
        sh = _open_sheet()
        ws = sh.worksheet("Webhook_Debug")
        _ws_update_acell(ws, "A1", f"{datetime.now().isoformat()} – ⚠️ {token} arrived in wallet but not marked claimed")
    except Exception:
        pass

def log_rebuy_confirmation(token):
    log_rebuy_decision(token)

# =============================================================================
# Utilities
# =============================================================================

def safe_float(value, default=0.0):
    try:
        return float(str(value).strip().replace("%", ""))
    except (ValueError, TypeError, AttributeError):
        return default

# --- compatibility stub to avoid boot crash (watchdog) ---
def detect_stalled_tokens(*args, **kwargs):
    """Return a list of stalled tokens; stubbed to empty to keep watchdog non-blocking."""
    return []

# --- Boot announce gate ------------------------------------------------------
import pathlib

_BOOT_FLAG_FILE = "/tmp/nova_boot.flag"  # reset on container restart

def _write_boot_flag():
    try:
        pathlib.Path(_BOOT_FLAG_FILE).write_text(str(int(time.time())))
    except Exception:
        pass

def _read_boot_flag_ts():
    try:
        return int(pathlib.Path(_BOOT_FLAG_FILE).read_text().strip())
    except Exception:
        return 0

def is_boot_announced(cooldown_min:int = 120) -> bool:
    """
    Returns True if we've already announced this boot (or if the last
    announce was within `cooldown_min` minutes). Uses /tmp flag so no
    Sheets calls needed; also mirrors to Webhook_Debug as FYI if available.
    """
    ts = _read_boot_flag_ts()
    if ts and (time.time() - ts) < cooldown_min * 60:
        return True
    return False

def mark_boot_announced() -> None:
    _write_boot_flag()
    # Best‑effort FYI in the sheet (non‑blocking)
    try:
        sh = get_sheet()
        sh.worksheet("Webhook_Debug").append_row(
            [datetime.now().isoformat(), "Boot notice sent"], value_input_option="RAW"
        )
    except Exception:
        pass
