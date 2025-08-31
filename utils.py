# utils.py — NT3.0 Phase-1 Polish (quota-proof + quiet + cached values + legacy shims)
import os, time, json, threading, functools, hashlib, hmac
from datetime import datetime, timezone, timedelta

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests

# ========= Env / Config =========
SHEET_URL = os.getenv("SHEET_URL", "")

BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TG_DEDUP_TTL_MIN = int(os.getenv("TG_DEDUP_TTL_MIN", "15"))

# Sheets budgets (global)
READS_PER_MIN  = int(os.getenv("SHEETS_READS_PER_MIN",  "50"))
WRITES_PER_MIN = int(os.getenv("SHEETS_WRITES_PER_MIN", "30"))

# Backoff caps
BACKOFF_BASE_S = float(os.getenv("SHEETS_BACKOFF_BASE_S", "1.25"))
BACKOFF_MAX_S  = float(os.getenv("SHEETS_BACKOFF_MAX_S",  "20"))

# ========= Logging (quiet, single-line) =========
def _ts(): return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
def info(msg):  print(f"[{_ts()}] INFO  {msg}")
def warn(msg):  print(f"[{_ts()}] WARN  {msg}")
def error(msg): print(f"[{_ts()}] ERROR {msg}")

# ========= Token Bucket (global budgets) =========
class TokenBucket:
    def __init__(self, capacity, refill_per_sec):
        self.capacity = capacity
        self.refill_per_sec = refill_per_sec
        self.tokens = capacity
        self.last = time.monotonic()
        self.lock = threading.Lock()
    def take(self, n=1):
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_sec)
            self.last = now
            if self.tokens >= n:
                self.tokens -= n
                return True
            return False

_read_bucket  = TokenBucket(READS_PER_MIN,  READS_PER_MIN  / 60.0)
_write_bucket = TokenBucket(WRITES_PER_MIN, WRITES_PER_MIN / 60.0)

def _wait_for(bucket):
    while not bucket.take(1):
        time.sleep(0.2)

def set_sheets_budget(reads_per_min=None, writes_per_min=None):
    """Optional live tuning at runtime."""
    global _read_bucket, _write_bucket
    if reads_per_min:
        _read_bucket  = TokenBucket(reads_per_min,  reads_per_min  / 60.0)
    if writes_per_min:
        _write_bucket = TokenBucket(writes_per_min, writes_per_min / 60.0)

# ========= Telegram (de-duped) =========
_dedup_cache = {}
_dedup_lock = threading.Lock()
_boot_once_key = "_boot_once_sent"

def _tg_send_raw(text):
    if not BOT_TOKEN or not TELEGRAM_CHAT_ID:
        warn("Telegram disabled (BOT_TOKEN/TELEGRAM_CHAT_ID missing)")
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
        requests.post(url, json=data, timeout=10)
    except Exception as e:
        warn(f"Telegram send failed: {e}")

def send_telegram_message_dedup(message: str, key: str, ttl_min: int = TG_DEDUP_TTL_MIN):
    now = time.time()
    with _dedup_lock:
        last = _dedup_cache.get(key, 0)
        if now - last < ttl_min * 60:
            return
        _dedup_cache[key] = now
    _tg_send_raw(message)

# ========= Telegram prompt (inline keyboard) — legacy compat =========
def _build_inline_keyboard(buttons):
    """
    buttons can be:
      - list of strings: ["YES", "NO"]  -> callback_data same as label
      - list of (label, callback_or_url) tuples
      - list of lists for multi-row keyboards
    """
    def to_btn(b):
        if isinstance(b, (list, tuple)) and len(b) >= 2:
            label, val = b[0], b[1]
        else:
            label, val = str(b), str(b)
        if isinstance(val, str) and val.lower().startswith(("http://", "https://", "tg://")):
            return {"text": str(label), "url": val}
        return {"text": str(label), "callback_data": str(val)}

    if not isinstance(buttons, list):
        buttons = [buttons]
    rows = []
    for row in buttons:
        if isinstance(row, list) and row and isinstance(row[0], (list, tuple, str)):
            rows.append([to_btn(x) for x in row])
        else:
            rows.append([to_btn(row)])
    return {"inline_keyboard": rows}

def send_telegram_prompt(text, buttons=None, key=None, ttl_min: int = TG_DEDUP_TTL_MIN):
    # allow 2-arg legacy form: (text, key)
    if isinstance(buttons, str) and key is None:
        key = buttons
        buttons = None
    if key:
        now = time.time()
        with _dedup_lock:
            last = _dedup_cache.get(key, 0)
            if now - last < ttl_min * 60:
                return
            _dedup_cache[key] = now
    if not BOT_TOKEN or not TELEGRAM_CHAT_ID:
        warn("Telegram disabled; cannot send prompt.")
        return
    if not buttons:
        buttons = ["YES", "NO"]
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
            "reply_markup": _build_inline_keyboard(buttons),
        }
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        warn(f"Telegram prompt send failed: {e}")

# ========= Telegram de-dupe helpers (legacy compat) =========
import hashlib as _hashlib
def _tg_key_from_message(msg: str) -> str:
    h = _hashlib.sha1(str(msg).encode()).hexdigest()[:12]
    return f"tg:{h}"

def tg_should_send(key_or_message: str, key: str = None, ttl_min: int = TG_DEDUP_TTL_MIN, consume: bool = True) -> bool:
    k = key or _tg_key_from_message(key_or_message)
    now = time.time()
    with _dedup_lock:
        last = _dedup_cache.get(k, 0)
        if now - last < ttl_min * 60:
            return False
        if consume:
            _dedup_cache[k] = now
    return True

def tg_mark_sent(key_or_message: str, key: str = None):
    k = key or _tg_key_from_message(key_or_message)
    with _dedup_lock:
        _dedup_cache[k] = time.time()

def send_telegram_message_if_new(message: str, key: str = None, ttl_min: int = TG_DEDUP_TTL_MIN):
    if tg_should_send(message, key=key, ttl_min=ttl_min, consume=True):
        _tg_send_raw(message)

# Optional alias some modules used historically
def send_telegram_inline(text, buttons=None, key=None, ttl_min: int = TG_DEDUP_TTL_MIN):
    return send_telegram_prompt(text, buttons=buttons, key=key, ttl_min=ttl_min)

# Backwards-compat alias used by some modules
def send_telegram_message(message: str, key: str = "default"):
    send_telegram_message_dedup(message, key, ttl_min=TG_DEDUP_TTL_MIN)

def send_once_per_day(key: str, message: str):
    today_key = f"{key}:{datetime.utcnow().strftime('%Y-%m-%d')}"
    send_telegram_message_dedup(message, today_key, ttl_min=24*60)

def send_boot_notice_once(message="🟢 NovaTrade system booted and live.", cooldown_min=120):
    send_telegram_message_dedup(message, key=_boot_once_key, ttl_min=cooldown_min)

def send_system_online_once():
    send_once_per_day("system_online", "✅ NovaTrade online — all modules healthy.")

# ========= Cold boot detection (legacy compat) =========
_BOOT_STARTED_AT = time.time()
COLD_BOOT_WINDOW_SEC = int(float(os.getenv("COLD_BOOT_WINDOW_MIN", "5")) * 60)  # default 5 minutes
def is_cold_boot() -> bool:
    if os.getenv("FORCE_COLD_BOOT", "0") == "1":
        return True
    return (time.time() - _BOOT_STARTED_AT) <= COLD_BOOT_WINDOW_SEC
def mark_warm_boot():
    global _BOOT_STARTED_AT
    _BOOT_STARTED_AT = 0

# ========= Service / Sheets helpers =========
_SCOPE = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
_gs_lock = threading.Lock()
_gs_client = None

def _load_service_account():
    """
    Loads Google creds from either a JSON file path (SVC_JSON) or the default
    'sentiment-log-service.json' in cwd. Also supports SVC_JSON containing raw JSON.
    """
    svc = os.getenv("SVC_JSON") or "sentiment-log-service.json"
    if os.path.isfile(svc):
        return ServiceAccountCredentials.from_json_keyfile_name(svc, _SCOPE)
    try:
        data = json.loads(svc)
        return ServiceAccountCredentials.from_json_keyfile_dict(data, _SCOPE)
    except Exception:
        return ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", _SCOPE)

def get_gspread_client():
    global _gs_client
    with _gs_lock:
        if _gs_client is None:
            creds = _load_service_account()
            _gs_client = gspread.authorize(creds)
        return _gs_client

def get_sheet():
    return get_gspread_client().open_by_url(SHEET_URL)

# ========= Backoff + Budget decorator for Sheets =========
def with_sheet_backoff(fn):
    @functools.wraps(fn)
    def wrapper(*a, **k):
        delay = BACKOFF_BASE_S
        while True:
            try:
                op = k.pop("_sheet_op", None) or fn.__name__
                if "update" in op or "batch" in op or "append" in op:
                    _wait_for(_write_bucket)
                else:
                    _wait_for(_read_bucket)
                return fn(*a, **k)
            except gspread.exceptions.APIError as e:
                msg = str(e).lower()
                if any(s in msg for s in ["rate limit", "quota", "429", "500", "503", "user rate limit"]):
                    warn(f"Sheets backoff ({fn.__name__}): {e}")
                    time.sleep(delay)
                    delay = min(BACKOFF_MAX_S, delay * 1.8)
                    continue
                raise
            except Exception as e:
                if any(x in str(e).lower() for x in ["timed out", "connection reset", "temporarily", "unavailable"]):
                    warn(f"Transient error ({fn.__name__}): {e}; retrying…")
                    time.sleep(delay)
                    delay = min(BACKOFF_MAX_S, delay * 1.8)
                    continue
                raise
    return wrapper

# ========= Cached reads (TTL) =========
_cached_ws = {}
_cached_rows = {}
_values_cache = {}
_cache_lock = threading.Lock()

@with_sheet_backoff
def get_ws(name: str):
    return get_sheet().worksheet(name)

def get_ws_cached(name: str, ttl_s: int = 120):
    key = f"ws::{name}"
    with _cache_lock:
        item = _cached_ws.get(key)
        if item:
            exp, ws = item
            if time.time() < exp:
                return ws
            _cached_ws.pop(key, None)
    ws = get_ws(name)
    with _cache_lock:
        _cached_ws[key] = (time.time()+ttl_s, ws)
    return ws

@with_sheet_backoff
def _ws_get_all_records(ws):
    return ws.get_all_records()

def get_all_records_cached(name: str, ttl_s: int = 120):
    key = f"rows::{name}"
    with _cache_lock:
        item = _cached_rows.get(key)
        if item:
            exp, rows = item
            if time.time() < exp:
                return rows
            _cached_rows.pop(key, None)
    ws = get_ws_cached(name, ttl_s=ttl_s)
    rows = _ws_get_all_records(ws)
    with _cache_lock:
        _cached_rows[key] = (time.time()+ttl_s, rows)
    return rows

# Backwards-compat alias some modules expect
def get_records_cached(sheet_name: str, ttl_s: int = 120):
    return get_all_records_cached(sheet_name, ttl_s)

# ========= Cached range reads (values) =========
@with_sheet_backoff
def _ws_get(ws, range_a1: str):
    return ws.get(range_a1)

def get_values_cached(sheet_name: str, range_a1: str, ttl_s: int = 60):
    key = f"vals::{sheet_name}::{range_a1}"
    with _cache_lock:
        item = _values_cache.get(key)
        if item:
            exp, vals = item
            if time.time() < exp:
                return vals
            _values_cache.pop(key, None)
    ws = get_ws_cached(sheet_name, ttl_s=ttl_s)
    vals = _ws_get(ws, range_a1)
    with _cache_lock:
        _values_cache[key] = (time.time() + ttl_s, vals)
    return vals

def get_value_cached(sheet_name: str, cell_a1: str, ttl_s: int = 60):
    data = get_values_cached(sheet_name, cell_a1, ttl_s=ttl_s)
    if not data:
        return ""
    row = data[0] if isinstance(data, list) else data
    if isinstance(row, list) and row:
        return row[0]
    return row or ""

# ========= Batch/Append/Update writes (single round-trips) =========
@with_sheet_backoff
def ws_batch_update(ws, writes):
    if not writes: return
    ws.batch_update(writes, value_input_option="RAW")

@with_sheet_backoff
def ws_append_row(ws, values):
    ws.append_row(values, value_input_option="RAW")

@with_sheet_backoff
def ws_update(ws, range_a1, values):
    ws.update(range_a1, values)

# ========= A1 helpers / parsing =========
def str_or_empty(v):
    return str(v).strip() if v is not None else ""

def to_float(v):
    s = str_or_empty(v).replace("%", "").replace(",", "")
    if s == "": return None
    try:
        return float(s)
    except Exception:
        return None

# ========= Legacy-safe parsing helpers (compat) =========
def safe_float(v, default=None):
    """
    Accepts numbers/strings like '12.3', '12%', '1,234', '-', 'N/A'.
    Returns float or `default` when not parseable.
    """
    s = str_or_empty(v).strip()
    if s == "" or s in {"-", "—", "N/A", "n/a", "NA", "na", "None", "null"}:
        return default
    s = s.replace("%", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return default

def safe_int(v, default=None):
    f = safe_float(v, default=None)
    if f is None:
        return default
    try:
        return int(float(f))
    except Exception:
        return default

def safe_str(v, default=""):
    s = str_or_empty(v)
    return s if s != "" else default

def cell_address(col_idx, row_idx):
    n = col_idx
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return f"{letters}{row_idx}"

def sanitize_range(a1: str) -> str:
    if "!" not in a1: return a1
    tab, rng = a1.split("!", 1)
    tab = tab.split("!")[-1]
    return f"{tab}!{rng}"

# ========= Legacy compat shims =========
def ping_webhook_debug(message: str):
    try:
        send_telegram_message_dedup(f"🛠️ Debug: {message}", key="webhook_debug")
    except Exception:
        pass
    try:
        ws = get_ws_cached("Webhook_Debug", ttl_s=30)
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        ws_update(ws, "A1", f"{ts}Z — {message}")
    except Exception:
        pass

def ping_webhook(message: str):
    ping_webhook_debug(message)

# --- detect_stalled_tokens (used by nova_watchdog) --------------------------
WATCHDOG_TAB = os.getenv("WATCHDOG_TAB", "Rotation_Log")
WATCHDOG_TOKEN_COL = os.getenv("WATCHDOG_TOKEN_COL", "Token")
WATCHDOG_TIME_HEADERS = [h.strip() for h in os.getenv(
    "WATCHDOG_TIME_HEADERS",
    "Last Updated,Updated At,Timestamp,Last_Alerted,Last Alerted,Last Seen"
).split(",")]
STALLED_THRESHOLD_HOURS = float(os.getenv("WATCHDOG_STALLED_THRESHOLD_HOURS", "12"))

def _parse_dt(val):
    s = str_or_empty(val)
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def detect_stalled_tokens(
    tab: str = WATCHDOG_TAB,
    token_col: str = WATCHDOG_TOKEN_COL,
    time_headers: list[str] = WATCHDOG_TIME_HEADERS,
    threshold_hours: float = STALLED_THRESHOLD_HOURS,
):
    stalled = []
    try:
        ws = get_ws_cached(tab, ttl_s=60)
        rows = ws.get_all_records()
        now = datetime.now(timezone.utc)
        for idx, row in enumerate(rows, start=2):
            token = str_or_empty(row.get(token_col))
            if not token:
                continue
            seen_dt = None
            last_col = None
            for h in time_headers:
                dt = _parse_dt(row.get(h))
                if dt:
                    seen_dt, last_col = dt, h
                    break
            if not seen_dt:
                continue
            age_h = (now - seen_dt).total_seconds() / 3600.0
            if age_h >= threshold_hours:
                stalled.append({
                    "row": idx,
                    "token": token,
                    "age_hours": round(age_h, 2),
                    "last_seen_col": last_col,
                    "last_seen": seen_dt.isoformat().replace("+00:00", "Z"),
                })
    except Exception as e:
        warn(f"detect_stalled_tokens: fallback no-op due to {e}")
        return []
    return stalled

# ========= HMAC helpers (Phase-2 ready) =========
def hmac_hex(secret: str, payload: dict) -> str:
    key = secret.encode()
    msg = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return hmac.new(key, msg, hashlib.sha256).hexdigest()
