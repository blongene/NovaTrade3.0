# utils.py  — NovaTrade 3.0 (clean, unified)
# ---------------------------------------------------------------------
# - Safe parsing helpers: str_or_empty, to_float, safe_float (exported)
# - Backoff: with_sheet_backoff (429/quota/5xx retry)
# - Sheets gates: minute token-buckets + concurrency gate
# - Cached open_by_url + per-worksheet TTL caches
# - gspread Worksheet compat: get_records_cached(ttl_s=120)
# - Telegram: raw + de-duped sender, once-per-day, boot notices
# - Sheet helpers: get_ws/get_values_cached/get_records_cached, batch updates
# - Logging helpers for Scout/Rotation/Vault flows (kept for BC)
# ---------------------------------------------------------------------

import os
import time
import json
import random
import pathlib
import hashlib
import threading
from functools import wraps
from datetime import datetime, timezone
from collections import deque

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =============================================================================
# Simple parsing helpers (exported)
# =============================================================================

def str_or_empty(v):
    """Coerce value to trimmed string ('' if None)."""
    return str(v).strip() if v is not None else ""

def to_float(v, default=None):
    """
    Convert a Sheet cell to float.
    Handles %, commas, blanks. Returns default if conversion fails.
    """
    try:
        s = str(v).replace("%", "").replace(",", "").strip()
        return float(s) if s else default
    except Exception:
        return default

def safe_float(value, default=0.0):
    """Legacy helper used by some modules."""
    try:
        return float(str(value).strip().replace("%", ""))
    except (ValueError, TypeError, AttributeError):
        return default

# =============================================================================
# Backoff / Retry Utilities
# =============================================================================

def with_sheet_backoff(fn):
    """Retry wrapper for Google Sheets 429/quota/5xx errors (exponential + jitter)."""
    @wraps(fn)
    def _inner(*a, **k):
        max_tries = 6
        base = 0.35
        for i in range(max_tries):
            try:
                return fn(*a, **k)
            except Exception as e:
                msg = str(e).lower()
                retryable = ("429" in msg or "quota" in msg or "rate limit" in msg
                             or "internal error" in msg or "temporarily" in msg)
                if i == max_tries - 1 or not retryable:
                    raise
                sleep_s = base * (2 ** i) + random.uniform(0, 0.4)
                print(f"⏳ Sheets backoff {sleep_s:.2f}s in {fn.__name__}: {e}")
                time.sleep(sleep_s)
    return _inner

def throttle_retry(max_retries=3, delay=2, jitter=1):
    """Generic retry with jitter for non-Sheets IO (e.g., HTTP)."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"⚠️ Attempt {attempt+1} failed in {func.__name__}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(delay + random.uniform(0, jitter))
                    else:
                        raise
        return wrapper
    return decorator

# =============================================================================
# GSpread Auth + Cached open_by_url
# =============================================================================

SHEET_URL = os.getenv("SHEET_URL")

def _gspread_creds():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds_path = os.getenv("GOOGLE_CREDS_JSON_PATH", "sentiment-log-service.json")
    return ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)

def get_gspread_client():
    creds = _gspread_creds()
    return gspread.authorize(creds)

# Cache the Spreadsheet object ~90s to avoid repeated open_by_url calls
_SHEET_OBJ_CACHE = {"obj": None, "ts": 0.0}
_SHEET_OBJ_TTL_S = int(os.getenv("SHEET_CACHE_TTL_SEC", "90"))

@with_sheet_backoff
def _cached_open_by_url(url: str):
    now = time.time()
    if _SHEET_OBJ_CACHE["obj"] and (now - _SHEET_OBJ_CACHE["ts"] < _SHEET_OBJ_TTL_S):
        return _SHEET_OBJ_CACHE["obj"]
    sh = get_gspread_client().open_by_url(url)
    _SHEET_OBJ_CACHE["obj"] = sh
    _SHEET_OBJ_CACHE["ts"] = now
    return sh

@with_sheet_backoff
def _open_sheet():
    return _cached_open_by_url(SHEET_URL)

# =============================================================================
# Minute token-buckets (global) + Concurrency gate
# =============================================================================

# Aim under 50 read calls/min and 30 write calls/min (tweak via env).
_SHEETS_READ_TIMES  = deque(maxlen=120)
_SHEETS_WRITE_TIMES = deque(maxlen=120)
_READ_MAX_PER_MIN   = int(os.getenv("SHEETS_READ_MAX_PER_MIN", "50"))
_WRITE_MAX_PER_MIN  = int(os.getenv("SHEETS_WRITE_MAX_PER_MIN", "30"))

def _ratelimit(bucket: deque, max_per_min: int):
    now = time.time()
    while bucket and now - bucket[0] > 60:
        bucket.popleft()
    if len(bucket) >= max_per_min:
        wait = 60 - (now - bucket[0]) + 0.05
        if wait > 0:
            time.sleep(wait)
    bucket.append(time.time())

def _sheet_read_gate():  _ratelimit(_SHEETS_READ_TIMES,  _READ_MAX_PER_MIN)
def _sheet_write_gate(): _ratelimit(_SHEETS_WRITE_TIMES, _WRITE_MAX_PER_MIN)

# Single-process concurrency gate for Sheets (serialize by default)
_SHEETS_MAX_CONCURRENCY = int(os.getenv("SHEETS_MAX_CONCURRENCY", "1"))
_SHEETS_GATE = threading.BoundedSemaphore(value=max(1, _SHEETS_MAX_CONCURRENCY))

def with_sheets_gate(fn):
    @wraps(fn)
    def _inner(*a, **k):
        with _SHEETS_GATE:
            return fn(*a, **k)
    return _inner

# =============================================================================
# Low-level WS ops (gated + backoff)
# =============================================================================

@with_sheet_backoff
def _ws_get_all_values(ws, *_, **__):
    _sheet_read_gate()
    return ws.get_all_values()

@with_sheet_backoff
def _ws_get_all_records(ws, *_, **__):
    _sheet_read_gate()
    return ws.get_all_records()

@with_sheet_backoff
def _ws_append_row(ws, row):
    _sheet_write_gate()
    return ws.append_row(row, value_input_option="USER_ENTERED")

@with_sheet_backoff
def _ws_update_cell(ws, r, c, v):
    _sheet_write_gate()
    return ws.update_cell(r, c, v)

@with_sheet_backoff
def _ws_update_acell(ws, a1, v):
    _sheet_write_gate()
    return ws.update_acell(a1, v)

@with_sheet_backoff
def _ws_update(ws, rng, rows):
    _sheet_write_gate()
    return ws.update(rng, rows, value_input_option="USER_ENTERED")

# =============================================================================
# Per-worksheet TTL caches + helpers
# =============================================================================

# In-memory TTL cache: {("values", sheet_name): {"ts": epoch, "data": ...}, ...}
_SHEETS_CACHE = {}
def _cache_get(kind: str, key: str, ttl_s: int):
    now = time.time()
    entry = _SHEETS_CACHE.get((kind, key))
    if entry and (now - entry["ts"]) < ttl_s:
        return entry["data"]
    return None

def _cache_put(kind: str, key: str, data):
    _SHEETS_CACHE[(kind, key)] = {"ts": time.time(), "data": data}

@with_sheets_gate
def get_ws(sheet_name: str):
    """Worksheet object (guarded + backoff)."""
    sh = _open_sheet()
    return sh.worksheet(sheet_name)

def get_values_cached(sheet_name: str, ttl_s: int = 120):
    """Cached get_all_values for a worksheet."""
    cached = _cache_get("values", sheet_name, ttl_s)
    if cached is not None:
        return cached
    ws = get_ws(sheet_name)
    vals = _ws_get_all_values(ws)
    _cache_put("values", sheet_name, vals)
    return vals

def get_records_cached(sheet_name: str, ttl_s: int = 120):
    """Cached get_all_records for a worksheet."""
    cached = _cache_get("records", sheet_name, ttl_s)
    if cached is not None:
        return cached
    ws = get_ws(sheet_name)
    recs = _ws_get_all_records(ws)
    _cache_put("records", sheet_name, recs)
    return recs

# Convenience: batch updates
@with_sheets_gate
@with_sheet_backoff
def ws_batch_update(ws, updates):
    """
    updates = [ {"range": "A2", "values": [[...]]}, {"range": "C5:D5", "values": [[...]]}, ... ]
    """
    if not updates:
        return None
    _sheet_write_gate()
    return ws.batch_update(updates, value_input_option="USER_ENTERED")

def batch_update_cells(sheet_name: str, updates):
    ws = get_ws(sheet_name)
    return ws_batch_update(ws, updates)

# =============================================================================
# gspread Worksheet compat shim: get_records_cached(ttl_s=120)
# =============================================================================

# TTL cache by worksheet *title* (for direct Worksheet calls)
_WS_TITLE_CACHE = {}  # { title: (ts, records) }
_WS_TITLE_LOCK = threading.Lock()

def _get_records_cached_by_title(sheet, title: str, ttl_s: int = 120):
    now = time.time()
    with _WS_TITLE_LOCK:
        hit = _WS_TITLE_CACHE.get(title)
        if hit and (now - hit[0] <= ttl_s):
            return hit[1]
    _sheet_read_gate()
    ws = sheet.worksheet(title)
    recs = ws.get_all_records()
    with _WS_TITLE_LOCK:
        _WS_TITLE_CACHE[title] = (now, recs)
    return recs

def ws_get_all_records_cached(ws, ttl_s: int = 120):
    title = getattr(ws, "title", "") or ""
    return _get_records_cached_by_title(ws.spreadsheet, title, ttl_s)

def install_ws_compat_cache():
    """
    Adds Worksheet.get_records_cached(ttl_s=120) compat even if gspread.models is absent.
    """
    try:
        Worksheet = None
        try:
            from gspread.models import Worksheet as _W
            Worksheet = _W
        except Exception:
            try:
                sh = _open_sheet()
                ws0 = sh.sheet1
                Worksheet = ws0.__class__
            except Exception:
                Worksheet = None

        if Worksheet is not None and not hasattr(Worksheet, "get_records_cached"):
            def _shim(self, ttl_s=120, *args, **kwargs):
                # delegate to our cached reader; extra args are ignored
                return ws_get_all_records_cached(self, ttl_s=ttl_s)
            setattr(Worksheet, "get_records_cached", _shim)
    except Exception as e:
        print(f"[utils] install_ws_compat_cache failed (non-fatal): {e}")

# Call once on import (idempotent)
try:
    install_ws_compat_cache()
except Exception as _e:
    print(f"[utils] Worksheet compat install skipped: {_e}")


# =============================================================================
# Headers + misc helpers (exported)
# =============================================================================

def header_index_map(header_row):
    return {str_or_empty(h): i for i, h in enumerate(header_row, start=1)}

def pick_col(rec: dict, names):
    """Return the first present key from names (case-sensitive list)."""
    for n in names:
        if n in rec:
            return rec[n]
    return None

# =============================================================================
# Telegram + Debug
# =============================================================================

def ping_webhook_debug(msg):
    """Best-effort log to Webhook_Debug!A1; silent if unavailable."""
    try:
        sh = _open_sheet()
        ws = sh.worksheet("Webhook_Debug")
        _ws_update_acell(ws, "A1", f"{datetime.now().isoformat()} - {msg}")
    except Exception:
        # Silent on purpose to avoid loops
        pass

def _tg_creds():
    bot_token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        raise RuntimeError("Missing BOT_TOKEN/TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    return bot_token, chat_id

@throttle_retry(max_retries=3, delay=2, jitter=1)
def send_telegram_message(message, chat_id=None):
    """Raw Telegram send (no de-dupe)."""
    bot_token = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN"))
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        ping_webhook_debug("❌ Telegram creds missing")
        raise RuntimeError("Missing BOT_TOKEN/TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    resp = requests.post(url, json=payload, timeout=10)
    if not resp.ok:
        ping_webhook_debug(f"❌ Telegram send error: {resp.text}")
        raise RuntimeError(resp.text)
    return resp.json()

# File-backed de-dup state in /tmp (persists across function calls in same container)
_TG_DEDUP_DIR = pathlib.Path("/tmp/nova_tg")
_TG_DEDUP_DIR.mkdir(parents=True, exist_ok=True)
_TG_DEDUP_TTL_MIN_DEFAULT = 15

def _hash_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()

def _dedup_file(key: str) -> pathlib.Path:
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_", ":", ".") else "_" for ch in (key or "global"))
    return _TG_DEDUP_DIR / f"{safe}.json"

def _read_json(path: pathlib.Path):
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}

def _write_json(path: pathlib.Path, data: dict):
    try:
        path.write_text(json.dumps(data))
    except Exception:
        pass

def tg_should_send(message: str, key: str = "global", ttl_min: int | None = None) -> bool:
    ttl = int(os.getenv("TG_DEDUP_TTL_MIN", str(_TG_DEDUP_TTL_MIN_DEFAULT)))
    if ttl_min is not None:
        ttl = int(ttl_min)
    f = _dedup_file(key)
    state = _read_json(f)
    last_hash = state.get("hash")
    last_ts = state.get("ts", 0)
    now = time.time()
    msg_hash = _hash_text(message)
    if last_hash == msg_hash and (now - last_ts) < ttl * 60:
        return False
    return True

def tg_mark_sent(message: str, key: str = "global"):
    f = _dedup_file(key)
    _write_json(f, {"hash": _hash_text(message), "ts": time.time()})

def send_telegram_message_dedup(message: str, key: str = "global", ttl_min: int | None = None, chat_id: str | None = None):
    """
    Telegram send with global de-duplication by key.
    - key groups messages; same message+key won't be resent within ttl_min.
    - ttl_min defaults to env TG_DEDUP_TTL_MIN (default 15).
    """
    if tg_should_send(message, key=key, ttl_min=ttl_min):
        resp = send_telegram_message(message, chat_id=chat_id)
        tg_mark_sent(message, key=key)
        return resp
    else:
        print(f"🛑 Telegram de-dupe suppressed for key='{key}'")
        return None

# Once-per-day convenience
def _utc_yyyymmdd():
    return datetime.now(timezone.utc).strftime("%Y%m%d")

def send_once_per_day(key: str, message: str, chat_id: str | None = None):
    per_day_key = f"daily:{key}:{_utc_yyyymmdd()}"
    return send_telegram_message_dedup(message, key=per_day_key, ttl_min=24*60, chat_id=chat_id)

# Boot announce gate (/tmp flag)
_BOOT_FLAG_FILE = "/tmp/nova_boot.flag"

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

def is_boot_announced(cooldown_min: int = 120) -> bool:
    ts = _read_boot_flag_ts()
    return bool(ts and (time.time() - ts) < cooldown_min * 60)

def mark_boot_announced() -> None:
    _write_boot_flag()
    try:
        sh = _open_sheet()
        sh.worksheet("Webhook_Debug").append_row(
            [datetime.now().isoformat(), "Boot notice sent"], value_input_option="RAW"
        )
    except Exception:
        pass

def send_boot_notice_once(message: str = "🟢 NovaTrade system booted and live.", chat_id: str | None = None, cooldown_min: int = 120):
    key = "boot_notice"
    if not is_boot_announced(cooldown_min=cooldown_min):
        resp = send_telegram_message_dedup(message, key=key, ttl_min=cooldown_min, chat_id=chat_id)
        mark_boot_announced()
        return resp
    else:
        print("🔇 Boot notice suppressed (already announced).")
        return None

def send_system_online_once(chat_id: str | None = None):
    return send_boot_notice_once(
        "📡 NovaTrade System Online\nAll modules are active.\nYou will be notified if input is needed or a token stalls.",
        chat_id=chat_id
    )

# === Inline-button prompt sender (for approvals / rebuy etc.) ===============
def send_telegram_prompt(
    token_or_title: str,
    message: str,
    buttons=None,
    prefix: str | None = "REBALANCE",
    dedupe_key: str | None = None,
    ttl_min: int | None = None,
    chat_id: str | None = None,
):
    """
    Send a Telegram message with inline buttons.
    - token_or_title: used in callback_data and in the prompt header
    - message: body text (Markdown)
    - buttons: list[str] or list[list[str]]; defaults to ["YES", "NO"]
    - prefix: header prefix (e.g., "REBALANCE", "CONFIRM", etc.)
    - dedupe_key: optional de-dupe key; if provided, suppress identical prompt within ttl_min
    - ttl_min: override dedupe window in minutes
    """
    bot_token = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN"))
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        print("❌ BOT_TOKEN/TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not found.")
        return None

    # Optional de-dupe
    if dedupe_key:
        # include the header+body so a different message bypasses the window
        composed = f"{prefix or ''}|{token_or_title}|{message}"
        if not tg_should_send(composed, key=dedupe_key, ttl_min=ttl_min):
            print(f"🛑 Telegram de-dupe suppressed for key='{dedupe_key}'")
            return None

    # Normalize buttons: allow ["YES","NO"] or [["YES","NO"],["HOLD"]]
    if not buttons:
        buttons = ["YES", "NO"]
    if all(isinstance(b, str) for b in buttons):
        layout = [[{"text": b, "callback_data": f"{b}|{token_or_title}"}] for b in buttons]
    else:
        # list[list[str]]
        layout = [
            [{"text": b, "callback_data": f"{b}|{token_or_title}"} for b in row]
            for row in buttons
        ]

    header = f"🔁 *{prefix} ALERT*\n\n" if prefix else ""
    payload = {
        "chat_id": chat_id,
        "text": f"{header}{message}",
        "parse_mode": "Markdown",
        "reply_markup": {"inline_keyboard": layout},
    }

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        r = requests.post(url, json=payload, timeout=10)
        if r.ok:
            if dedupe_key:
                tg_mark_sent(f"{prefix or ''}|{token_or_title}|{message}", key=dedupe_key)
            return r.json()
        else:
            print(f"⚠️ Telegram prompt error: {r.text}")
            return None
    except Exception as e:
        print(f"❌ Telegram prompt failed: {e}")
        return None

# =============================================================================
# Rotation / Scout Logging Utilities (kept for backward-compat)
# =============================================================================

def get_sheet():
    """Backward-compat alias: returns Spreadsheet object."""
    return _open_sheet()

def log_scout_decision(token, decision):
    token_u = str_or_empty(token).upper()
    print(f"📥 Logging decision: {decision} for token {token_u}")
    try:
        sh = _open_sheet()
        ws = sh.worksheet("Scout Decisions")
        planner_ws = sh.worksheet("Rotation_Planner")

        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        _ws_append_row(ws, [now, token_u, str_or_empty(decision).upper(), "Telegram"])
        print("✅ Decision logged to Scout Decisions")

        if str_or_empty(decision).upper() in {"YES", "VAULT", "ROTATE"}:
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
                if token_idx < len(row) and str_or_empty(row[token_idx]).upper() == token_u:
                    _ws_update_cell(planner_ws, i, confirm_idx + 1, "YES")
                    print(f"✅ Auto-confirmed {token_u} in Rotation_Planner")
                    break
    except Exception as e:
        print(f"❌ Failed to log decision for {token_u}: {e}")
        ping_webhook_debug(f"❌ Log Scout Decision error: {e}")

def log_rebuy_decision(token):
    try:
        sh = _open_sheet()
        scout_ws = sh.worksheet("Scout Decisions")
        log_ws = sh.worksheet("Rotation_Log")
        radar_ws = sh.worksheet("Sentiment_Radar")

        token_u = str_or_empty(token).upper()
        log_data = _ws_get_all_records(log_ws)
        log_row = next((r for r in log_data if str_or_empty(r.get("Token")).upper() == token_u), {})

        score = log_row.get("Score", "")
        sentiment = log_row.get("Sentiment", "")
        market_cap = log_row.get("Market Cap", "")
        scout_url = log_row.get("Scout URL", "")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        if not sentiment:
            radar = _ws_get_all_records(radar_ws)
            rrow = next((r for r in radar if str_or_empty(r.get("Token")).upper() == token_u), {})
            sentiment = rrow.get("Mentions", "")

        new_row = [timestamp, token_u, "YES", "Rebuy", score, sentiment, market_cap, scout_url, ""]
        _ws_append_row(scout_ws, new_row)
        print(f"✅ Rebuy for ${token_u} logged to Scout Decisions.")
    except Exception as e:
        print(f"❌ Failed to log rebuy decision for {token}: {e}")

def log_rotation_confirmation(token, decision):
    try:
        sh = _open_sheet()
        planner_ws = sh.worksheet("Rotation_Planner")
        records = _ws_get_all_records(planner_ws)
        for i, row in enumerate(records, start=2):  # Skip header
            if str_or_empty(row.get("Token")).upper() == str_or_empty(token).upper():
                _ws_update_acell(planner_ws, f"C{i}", str_or_empty(decision).upper())  # C = 'User Response'
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
        _ws_append_row(ws, [timestamp, str_or_empty(token).upper(), str_or_empty(decision).upper()])
        print(f"✅ ROI Feedback logged: {token} → {decision}")
    except Exception as e:
        print(f"❌ Failed to log ROI Feedback: {e}")
        ping_webhook_debug(f"❌ ROI Feedback log error: {e}")

def log_vault_review(token, decision):
    try:
        sh = _open_sheet()
        ws = sh.worksheet("Vault_Review_Log")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        _ws_append_row(ws, [timestamp, str_or_empty(token).upper(), str_or_empty(decision).upper()])
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
        token_u = str_or_empty(token).upper()
        for i, row in enumerate(rows, start=2):  # Start at row 2
            if str_or_empty(row.get("Token")).upper() == token_u:
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

# --- compatibility stub to avoid boot crash (watchdog) ---
def detect_stalled_tokens(*args, **kwargs):
    """Return a list of stalled tokens; stubbed to empty to keep watchdog non-blocking."""
    return []

# =============================================================================
# Backward-compat exports & aliases (append-only, safe to keep)
# =============================================================================

# --- Sheet client / open helpers ---
# Already defined: get_gspread_client, _open_sheet, get_sheet (alias), get_ws
# Make sure legacy names resolve (some modules import these)
open_sheet = _open_sheet  # legacy alias

# --- Read/write wrappers (legacy names kept for modules that import them) ---
ws_get_all_values = _ws_get_all_values
ws_get_all_records = _ws_get_all_records
ws_append_row = _ws_append_row
ws_update_cell = _ws_update_cell
ws_update_acell = _ws_update_acell
ws_update = _ws_update

# Provide old camelCase variants if any module referenced them
_wsGetAllValues = _ws_get_all_values
_wsGetAllRecords = _ws_get_all_records

# --- Cached helpers (module-friendly) ---
safe_get_all_records = safe_get_all_records if 'safe_get_all_records' in globals() else ws_get_all_records
safe_update = safe_update if 'safe_update' in globals() else ws_update
safe_batch_update = safe_batch_update if 'safe_batch_update' in globals() else ws_batch_update

# --- Headers & parsing ---
# Already defined: header_index_map, pick_col, str_or_empty, to_float, safe_float

# --- Telegram layer (raw + dedupe + prompt) ---
# Already defined: send_telegram_message, send_telegram_message_dedup, send_telegram_prompt
# Also expose daily/boot convenience
system_online_once = send_system_online_once
once_per_day = send_once_per_day

# --- Dedupe helpers (sometimes imported directly) ---
tg_dedupe_should_send = tg_should_send
tg_dedupe_mark_sent = tg_mark_sent

# --- Compat shims for rarely-used legacy helpers (no-ops but safe) ---
def get_records_cached_ws(ws, ttl_s: int = 120):
    """Compat: some modules call get_records_cached(ws, ttl) vs by title."""
    try:
        return ws_get_all_records_cached(ws, ttl_s=ttl_s)
    except Exception:
        return _ws_get_all_records(ws)

def get_values_cached_ws(ws, ttl_s: int = 120):
    """Compat: values cache by Worksheet object."""
    try:
        # We don't maintain a values cache per title; do a gated read.
        return _ws_get_all_values(ws)
    except Exception:
        return []

def get_records_cached_safe(sheet_name: str, ttl_s: int = 120):
    """Compat alias for get_records_cached(sheet_name, ttl)."""
    return get_records_cached(sheet_name, ttl_s=ttl_s)

def get_values_cached_safe(sheet_name: str, ttl_s: int = 120):
    """Compat alias for get_values_cached(sheet_name, ttl)."""
    return get_values_cached(sheet_name, ttl_s=ttl_s)

# Some modules used these placeholders; keep harmless stubs so imports never break.
def with_sheet_budget(*args, **kwargs):
    """Legacy stub: previously enforced budgets; superseded by token buckets."""
    return True

def ws_get_records_cached(ws, ttl_s: int = 120):
    """Alias to the monkey-patched method for explicit calls."""
    return ws_get_all_records_cached(ws, ttl_s=ttl_s)

# --- Explicit __all__ to make exported surface obvious ---
__all__ = [
    # Parsing & headers
    "str_or_empty", "to_float", "safe_float", "header_index_map", "pick_col",
    # Sheets: auth/open
    "get_gspread_client", "get_sheet", "get_ws", "open_sheet",
    # Sheets: cached reads
    "get_records_cached", "get_values_cached",
    "get_records_cached_safe", "get_values_cached_safe",
    "get_records_cached_ws", "get_values_cached_ws",
    "ws_get_all_records_cached",
    # Sheets: raw ops (gated/backoff)
    "ws_get_all_values", "ws_get_all_records", "ws_update", "ws_update_cell",
    "ws_update_acell", "ws_append_row", "ws_batch_update", "batch_update_cells",
    # Safe ops
    "safe_get_all_records", "safe_update", "safe_batch_update",
    # Budget/gates/backoff
    "with_sheet_backoff", "with_sheets_gate",
    # Telegram
    "send_telegram_message", "send_telegram_message_dedup", "send_telegram_prompt",
    "send_once_per_day", "send_boot_notice_once", "send_system_online_once",
    "system_online_once",
    # Dedupe extras
    "tg_should_send", "tg_mark_sent", "tg_dedupe_should_send", "tg_dedupe_mark_sent",
    # Debug
    "ping_webhook_debug",
    # Stubs
    "with_sheet_budget", "ws_get_records_cached",
]
