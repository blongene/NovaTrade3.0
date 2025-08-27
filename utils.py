# utils.py
import os
import time
import json
import random
import pathlib
import hashlib
import requests
import gspread
from functools import wraps
from datetime import datetime, timezone
from oauth2client.service_account import ServiceAccountCredentials

# ===== Sheets global rate limit + cache (paste into utils.py) =====
from collections import deque

# Token-bucket-ish limiter: aim under 50 read calls/min and 30 write calls/min.
_SHEETS_READ_TIMES  = deque(maxlen=60)
_SHEETS_WRITE_TIMES = deque(maxlen=60)
_READ_MAX_PER_MIN   = int(os.getenv("SHEETS_READ_MAX_PER_MIN", "50"))
_WRITE_MAX_PER_MIN  = int(os.getenv("SHEETS_WRITE_MAX_PER_MIN", "30"))

def _ratelimit(bucket: deque, max_per_min: int):
    now = time.time()
    # prune older than 60s
    while bucket and now - bucket[0] > 60:
        bucket.popleft()
    if len(bucket) >= max_per_min:
        # sleep until we drop under the window
        wait = 60 - (now - bucket[0]) + 0.01
        if wait > 0:
            time.sleep(wait)
    bucket.append(time.time())

# Wrap low-level sheet ops
def _sheet_read_gate():  _ratelimit(_SHEETS_READ_TIMES,  _READ_MAX_PER_MIN)
def _sheet_write_gate(): _ratelimit(_SHEETS_WRITE_TIMES, _WRITE_MAX_PER_MIN)

# Cache the Spreadsheet object ~90s to avoid repeated open_by_url calls
_SHEET_CACHE = {"obj": None, "ts": 0.0}
_SHEET_TTL_S = int(os.getenv("SHEET_CACHE_TTL_SEC", "90"))

def _cached_open_by_url(url: str):
    now = time.time()
    if _SHEET_CACHE["obj"] and (now - _SHEET_CACHE["ts"] < _SHEET_TTL_S):
        return _SHEET_CACHE["obj"]
    _sheet_read_gate()
    sh = get_gspread_client().open_by_url(url)
    _SHEET_CACHE["obj"] = sh
    _SHEET_CACHE["ts"] = now
    return sh

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
    return _cached_open_by_url(SHEET_URL)

# Generic backoff-wrapped actions
@with_sheet_backoff
def _ws_get_all_values(ws):
    _sheet_read_gate()
    return ws.get_all_values()

@with_sheet_backoff
def _ws_get_all_records(ws):
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

# === utils.py (add below your existing imports/helpers) ======================
import time
import threading
from collections import deque

# ---- Simple global token bucket for Sheets calls ----------------------------
# default budget ~50 reads/min, ~30 writes/min (tweak via env or setters)
_SHEETS_BUDGET = {
    "reads_per_min": int(os.getenv("SHEETS_READS_PER_MIN", "50")),
    "writes_per_min": int(os.getenv("SHEETS_WRITES_PER_MIN", "30")),
}
_tokens = {
    "read": deque(),   # timestamps of read calls
    "write": deque(),  # timestamps of write calls
}
_tokens_lock = threading.Lock()

def _check_token(kind: str):
    """Rate-limit sheets calls by minute window."""
    now = time.time()
    window = 60.0
    limit = _SHEETS_BUDGET["reads_per_min"] if kind == "read" else _SHEETS_BUDGET["writes_per_min"]
    with _tokens_lock:
        dq = _tokens["read" if kind == "read" else "write"]
        # drop old timestamps
        while dq and now - dq[0] > window:
            dq.popleft()
        if len(dq) >= limit:
            # sleep until first token expires
            sleep_for = window - (now - dq[0]) + 0.05
            time.sleep(max(0.05, sleep_for))
            # after sleep, clean again
            now = time.time()
            while dq and now - dq[0] > window:
                dq.popleft()
        dq.append(now)

def set_sheets_budget(reads_per_min=None, writes_per_min=None):
    if reads_per_min:
        _SHEETS_BUDGET["reads_per_min"] = int(reads_per_min)
    if writes_per_min:
        _SHEETS_BUDGET["writes_per_min"] = int(writes_per_min)

# ---- Read cache keyed by worksheet title -----------------------------------
_SHEET_CACHE = {}  # { title: (ts, records) }
_SHEET_CACHE_LOCK = threading.Lock()

def get_records_cached_by_title(sheet, title: str, ttl_s: int = 120):
    """
    Return list of dicts for given worksheet title with TTL caching.
    """
    now = time.time()
    with _SHEET_CACHE_LOCK:
        hit = _SHEET_CACHE.get(title)
        if hit and (now - hit[0] <= ttl_s):
            return hit[1]

    # cache miss -> live read
    _check_token("read")
    ws = sheet.worksheet(title)
    records = ws.get_all_records()  # one shot read
    with _SHEET_CACHE_LOCK:
        _SHEET_CACHE[title] = (now, records)
    return records

# Convenience (common pattern): direct by Worksheet object
def ws_get_all_records_cached(ws, ttl_s: int = 120):
    """
    Cached version for a gspread Worksheet object.
    """
    title = getattr(ws, "title", None) or ""
    sheet = ws.spreadsheet
    return get_records_cached_by_title(sheet, title, ttl_s)

# ---- Compat shim so ws.get_records_cached(...) won't crash ------------------
# Call this once after auth; or we can lazy-install the first time utils is imported.
def install_ws_compat_cache():
    """
    Monkey-patch gspread.Worksheet to add get_records_cached(ttl_s=120),
    delegating to our cache above. This lets legacy calls keep working.
    """
    try:
        import gspread
        if not hasattr(gspread.models.Worksheet, "get_records_cached"):
            def _shim(self, ttl_s=120):
                return ws_get_all_records_cached(self, ttl_s=ttl_s)
            gspread.models.Worksheet.get_records_cached = _shim
    except Exception as e:
        print(f"[utils] install_ws_compat_cache failed (non-fatal): {e}")

# ---- Safer header helpers ---------------------------------------------------
def header_index_map(header_row):
    return {str_or_empty(h): i for i, h in enumerate(header_row, start=1)}

def pick_col(rec: dict, names):
    """Return the first present key from names (case sensitive list)."""
    for n in names:
        if n in rec:
            return rec[n]
    return None

# ---- Wrap gspread reads/writes with budget ----------------------------------
# If you already have with_sheet_backoff, you can nest _check_token calls inside
# your read/write points (acell/get_all_records/update/batch_update), e.g.:

def safe_get_all_records(ws, ttl_s: int = 0):
    """Optional helper: cache if ttl_s>0, else live read with rate-limit."""
    if ttl_s and ttl_s > 0:
        return ws_get_all_records_cached(ws, ttl_s=ttl_s)
    _check_token("read")
    return ws.get_all_records()

def safe_batch_update(ws, payload, **kwargs):
    _check_token("write")
    return ws.batch_update(payload, **kwargs)

def safe_update(ws, range_a1, values, **kwargs):
    _check_token("write")
    return ws.update(range_a1, values, **kwargs)

# =============================================================================
# Telegram + Debug
# =============================================================================

# Add near the bottom of utils.py (after helpers are defined)
try:
    import gspread.models as _gmodels
    if not hasattr(_gmodels.Worksheet, "get_records_cached"):
        def _ws_get_records_cached(self):
            # just delegate to our gated wrapper
            return _ws_get_all_records(self)
        setattr(_gmodels.Worksheet, "get_records_cached", _ws_get_records_cached)
except Exception:
    # ignore; worst case the old callers will still raise visibly
    pass

def ping_webhook_debug(msg):
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
    """Raw Telegram send (no dedupe)."""
    bot_token = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN"))
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        ping_webhook_debug("❌ Telegram creds missing")
        raise RuntimeError("Missing BOT_TOKEN/TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    resp = requests.post(url, json=payload, timeout=10)
    if not resp.ok:
        ping_webhook_debug(f"❌ Telegram send error: {resp.text}")
        raise RuntimeError(resp.text)
    return resp.json()

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

# -----------------------------------------------------------------------------
# Global Telegram de-dupe (file-based, no Sheets calls)
# -----------------------------------------------------------------------------

_TG_DEDUP_DIR = pathlib.Path("/tmp/nova_tg")
_TG_DEDUP_DIR.mkdir(parents=True, exist_ok=True)
# default dedupe interval (minutes) – configurable via env
_TG_DEDUP_TTL_MIN_DEFAULT = 15

def _hash_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()

def _dedup_file(key: str) -> pathlib.Path:
    # key can be "global", "boot", "daily:summary", etc
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
    """
    Returns True if we should send the message now (i.e., it's not a duplicate
    within the TTL window for the provided key).
    """
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
        # duplicate within window
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

# -----------------------------------------------------------------------------
# Once-per-day / Once-per-boot convenience wrappers
# -----------------------------------------------------------------------------

def _utc_yyyymmdd():
    return datetime.now(timezone.utc).strftime("%Y%m%d")

def send_once_per_day(key: str, message: str, chat_id: str | None = None):
    """
    Ensures message is sent at most once per UTC day for the provided key.
    Uses a /tmp flag file (no Sheets traffic).
    """
    per_day_key = f"daily:{key}:{_utc_yyyymmdd()}"
    # Use dedupe TTL of 24h for safety
    return send_telegram_message_dedup(message, key=per_day_key, ttl_min=24*60, chat_id=chat_id)

# --- Boot announce gate ------------------------------------------------------
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

def is_boot_announced(cooldown_min: int = 120) -> bool:
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
        sh = _open_sheet()
        sh.worksheet("Webhook_Debug").append_row(
            [datetime.now().isoformat(), "Boot notice sent"], value_input_option="RAW"
        )
    except Exception:
        pass

def send_boot_notice_once(message: str = "🟢 NovaTrade system booted and live.", chat_id: str | None = None, cooldown_min: int = 120):
    """
    Sends boot notice once per container boot (or if previous boot notice was
    older than `cooldown_min`). Uses /tmp boot flag + de-dup guard.
    """
    key = "boot_notice"
    if not is_boot_announced(cooldown_min=cooldown_min):
        resp = send_telegram_message_dedup(message, key=key, ttl_min=cooldown_min, chat_id=chat_id)
        mark_boot_announced()
        return resp
    else:
        print("🔇 Boot notice suppressed (already announced).")
        return None

def send_system_online_once(chat_id: str | None = None):
    """Handy alias for your 'System Online' heartbeat, once per boot."""
    return send_boot_notice_once("📡 NovaTrade System Online\nAll modules are active.\nYou will be notified if input is needed or a token stalls.", chat_id=chat_id)

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
    
# === Sheets global gate + TTL caching + batch helpers ========================
import threading

# Single-file global concurrency gate for Sheets (default 1: fully serialized)
_SHEETS_MAX_CONCURRENCY = int(os.getenv("SHEETS_MAX_CONCURRENCY", "1"))
_SHEETS_GATE = threading.BoundedSemaphore(value=max(1, _SHEETS_MAX_CONCURRENCY))

def with_sheets_gate(fn):
    @wraps(fn)
    def _inner(*a, **k):
        with _SHEETS_GATE:
            return fn(*a, **k)
    return _inner

# In‑memory TTL cache: {("values", sheet_name): {"ts": epoch, "data": ...}, ...}
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
@with_sheet_backoff
def _open_ws(sheet_name: str):
    sh = _open_sheet()
    return sh.worksheet(sheet_name)

def get_ws(sheet_name: str):
    """Worksheet object (NOT cached by API call count, but guarded+backed off)."""
    return _open_ws(sheet_name)

def get_values_cached(sheet_name: str, ttl_s: int = 120):
    """
    Cached 'get_all_values' for a worksheet. Multiple modules in the same
    run will reuse the same payload (dramatically reduces reads).
    """
    cached = _cache_get("values", sheet_name, ttl_s)
    if cached is not None:
        return cached
    ws = get_ws(sheet_name)
    vals = _ws_get_all_values(ws)  # already gate+backoff
    _cache_put("values", sheet_name, vals)
    return vals

def get_records_cached(sheet_name: str, ttl_s: int = 120):
    """
    Cached 'get_all_records' for a worksheet.
    """
    cached = _cache_get("records", sheet_name, ttl_s)
    if cached is not None:
        return cached
    ws = get_ws(sheet_name)
    recs = _ws_get_all_records(ws)  # already gate+backoff
    _cache_put("records", sheet_name, recs)
    return recs

# Batch update helpers (coalesce writes)
@with_sheets_gate
@with_sheet_backoff
def ws_batch_update(ws, updates):
    """
    updates = [
      {"range": "A2", "values": [[val1, val2, ...]]},
      {"range": "C5:D5", "values": [[x, y]]},
      ...
    ]
    """
    if not updates:
        return None
    # gspread expects a list of {range, values}
    return ws.batch_update(updates, value_input_option="USER_ENTERED")

def batch_update_cells(sheet_name: str, updates):
    """
    Convenience to get ws + call ws_batch_update.
    """
    ws = get_ws(sheet_name)
    return ws_batch_update(ws, updates)
