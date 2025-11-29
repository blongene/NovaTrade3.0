# utils.py — NovaTrade 3.0 (Phase 5/6 hardened)
# - Bullet-proof gspread auth resolution (env JSON, env file, common secret paths)
# - Token-bucket + exponential backoff to survive 429s / intermittent errors
# - Cached reads (worksheets, rows, values) with simple invalidation helpers
# - Telegram de-duped notifications + prompts (quiet failure)
# - Legacy shims preserved (names/behaviors used by prior modules)
# - Small reliability polish: requests Session with retries for Telegram
# - HMAC signing & Enqueue helpers (FIXED)

from __future__ import annotations
import os, time, json, threading, functools, hashlib, hmac, random, traceback
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Any

import requests
from requests.adapters import HTTPAdapter, Retry

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ========= Env / Config =========
SHEET_URL = os.getenv("SHEET_URL", "")

BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TG_DEDUP_TTL_MIN = int(os.getenv("TG_DEDUP_TTL_MIN", "15"))

# Sheets budgets (global) — throttle harder by default; override in Render ENV
READS_PER_MIN  = int(os.getenv("SHEETS_READS_PER_MIN",  "30"))
WRITES_PER_MIN = int(os.getenv("SHEETS_WRITES_PER_MIN", "18"))

# Backoff caps
BACKOFF_BASE_S = float(os.getenv("SHEETS_BACKOFF_BASE_S", "1.75"))
BACKOFF_MAX_S  = float(os.getenv("SHEETS_BACKOFF_MAX_S",  "24"))
BACKOFF_JIT_S  = float(os.getenv("SHEETS_BACKOFF_JIT_S",  "0.35"))  # small jitter to desync callers

# Cache TTL defaults (tunable via env)
DEFAULT_ROWS_TTL_S   = int(os.getenv("ROWS_TTL_S",   "240"))  # rows cache
DEFAULT_VALUES_TTL_S = int(os.getenv("VALUES_TTL_S", "120"))  # values cache
DEFAULT_WS_TTL_S     = int(os.getenv("WS_TTL_S",     "180"))  # worksheet handle cache

# ========= Logging (quiet, single-line) =========
def _ts(): return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
def info(msg):  print(f"[{_ts()}] INFO  {msg}")
def warn(msg):  print(f"[{_ts()}] WARN  {msg}")
def error(msg): print(f"[{_ts()}] ERROR {msg}")

# ========= Requests Session (Telegram reliability) =========
def _requests_session():
    s = requests.Session()
    retry = Retry(
        total=4, connect=4, read=4, backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET","POST"])
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

_REQ = _requests_session()

# ========= Token Bucket (global budgets) =========
class TokenBucket:
    def __init__(self, capacity, refill_per_sec):
        self.capacity = max(1, int(capacity))
        self.refill_per_sec = float(refill_per_sec)
        self.tokens = float(capacity)
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

# ========= Sheets gate (decorator + context manager) =========
def _take_tokens(bucket, tokens: int):
    tokens = max(1, int(tokens))
    for _ in range(tokens):
        _wait_for(bucket)

def with_sheets_gate(mode: str = "read", tokens: int = 1):
    """Decorator form: pre-consume read/write tokens before running the func."""
    mode_l = (mode or "read").lower()
    bucket = _read_bucket if mode_l == "read" else _write_bucket
    def _decorator(fn):
        @functools.wraps(fn)
        def _wrapper(*args, **kwargs):
            _take_tokens(bucket, tokens)
            return fn(*args, **kwargs)
        return _wrapper
    return _decorator

@contextmanager
def sheets_gate(mode: str = "read", tokens: int = 1):
    """Context-manager form to pre-consume tokens around raw gspread usage."""
    mode_l = (mode or "read").lower()
    bucket = _read_bucket if mode_l == "read" else _write_bucket
    _take_tokens(bucket, tokens)
    try:
        yield
    finally:
        pass

# ========= Telegram (de-duped) =========
_dedup_cache: dict[str, float] = {}
_dedup_lock = threading.Lock()
_boot_once_key = "_boot_once_sent"

def _tg_send_raw(text):
    if not BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
        _REQ.post(url, json=data, timeout=10)
    except Exception as e:
        warn(f"Telegram send failed: {e}")

def send_telegram_message_dedup(message: str, key: str, ttl_min: int = TG_DEDUP_TTL_MIN):
    now = time.time()
    with _dedup_lock:
        last = _dedup_cache.get(key, 0.0)
        if now - last < ttl_min * 60:
            return
        _dedup_cache[key] = now
    _tg_send_raw(message)

# Inline prompt (legacy-compatible)
def _build_inline_keyboard(buttons):
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
    if isinstance(buttons, str) and key is None:
        key = buttons; buttons = None
    if key:
        now = time.time()
        with _dedup_lock:
            last = _dedup_cache.get(key, 0.0)
            if now - last < ttl_min * 60:
                return
            _dedup_cache[key] = now
    if not BOT_TOKEN or not TELEGRAM_CHAT_ID:
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
        _REQ.post(url, json=payload, timeout=10)
    except Exception as e:
        warn(f"Telegram prompt send failed: {e}")

# De-dupe helpers
import hashlib as _hashlib
def _tg_key_from_message(msg: str) -> str:
    h = _hashlib.sha1(str(msg).encode()).hexdigest()[:12]
    return f"tg:{h}"

def tg_should_send(key_or_message: str, key: str = None, ttl_min: int = TG_DEDUP_TTL_MIN, consume: bool = True) -> bool:
    k = key or _tg_key_from_message(key_or_message)
    now = time.time()
    with _dedup_lock:
        last = _dedup_cache.get(k, 0.0)
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

def send_telegram_inline(text, buttons=None, key=None, ttl_min: int = TG_DEDUP_TTL_MIN):
    return send_telegram_prompt(text, buttons=buttons, key=key, ttl_min=ttl_min)

def send_telegram_message(message: str, key: str = "default"):
    send_telegram_message_dedup(message, key, ttl_min=TG_DEDUP_TTL_MIN)

def send_once_per_day(key: str, message: str):
    today_key = f"{key}:{datetime.utcnow().strftime('%Y-%m-%d')}"
    send_telegram_message_dedup(message, today_key, ttl_min=24*60)

def send_boot_notice_once(message="🟢 NovaTrade system booted and live.", cooldown_min=120):
    send_telegram_message_dedup(message, key=_boot_once_key, ttl_min=cooldown_min)

def send_system_online_once():
    send_once_per_day("system_online", "✅ NovaTrade online — all modules healthy.")

# ========= Cold boot detection =========
_BOOT_STARTED_AT = time.time()
COLD_BOOT_WINDOW_SEC = int(float(os.getenv("COLD_BOOT_WINDOW_MIN", "8")) * 60)  # default 8 min
def is_cold_boot() -> bool:
    if os.getenv("FORCE_COLD_BOOT", "0") == "1":
        return True
    return (time.time() - _BOOT_STARTED_AT) <= COLD_BOOT_WINDOW_SEC
def mark_warm_boot():
    global _BOOT_STARTED_AT
    _BOOT_STARTED_AT = 0

# ========= Google credentials resolution (bullet-proof) =========
_SCOPE = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
_gs_lock = threading.Lock()
_gs_client = None

def _resolve_service_account_path() -> str | None:
    # 1) Explicit env path(s)
    for env_name in ("GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CREDS_JSON_PATH"):
        p = (os.getenv(env_name) or "").strip()
        if p and os.path.isfile(p):
            info(f"[WEB] using {env_name}={p}")
            return p

    # 2) Common Render secret-file paths
    for p in (
        "/etc/secrets/service_account.json",
        "/etc/secrets/sentiment-log-service.json",
        "/opt/render/.config/gspread/service_account.json",
        "./service_account.json",
        "./sentiment-log-service.json",
    ):
        if os.path.isfile(p):
            info(f"[WEB] using fallback creds at {p}")
            return p
    return None

def _make_gspread_client():
    # Highest precedence: full JSON blob in env
    js = (
        os.getenv("GSPREAD_SERVICE_ACCOUNT_JSON")
        or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        or os.getenv("SVC_JSON")
    )
    if js:
        try:
            data = json.loads(js)
            return gspread.service_account_from_dict(data)
        except Exception as e:
            warn(f"[WEB] service_account_from_dict failed: {e}")

    # Next: a real filename from env or known secret locations
    path = _resolve_service_account_path()
    if path:
        try:
            return gspread.service_account(filename=path)
        except Exception as e:
            warn(f"[WEB] gspread.service_account(filename={path}) failed: {e}")

    # Final: oauth2client fallback to legacy file name if present
    try:
        svc_path = path or "sentiment-log-service.json"
        if os.path.isfile(svc_path):
            creds = ServiceAccountCredentials.from_json_keyfile_name(svc_path, _SCOPE)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(os.getenv("SVC_JSON", "{}")), _SCOPE)
        return gspread.authorize(creds)
    except Exception as e:
        warn(f"[WEB] oauth2client fallback failed: {e}; trying gspread default lookup.")
        return gspread.service_account()

def get_gspread_client():
    global _gs_client
    with _gs_lock:
        if _gs_client is None:
            _gs_client = _make_gspread_client()
        return _gs_client

def get_sheet():
    if not SHEET_URL:
        raise RuntimeError("SHEET_URL env is not set.")
    return get_gspread_client().open_by_url(SHEET_URL)

# ========= Backoff + Budget decorator for Sheets =========
def with_sheet_backoff(fn):
    @functools.wraps(fn)
    def wrapper(*a, **k):
        delay = BACKOFF_BASE_S + random.random()*BACKOFF_JIT_S
        while True:
            try:
                op = k.pop("_sheet_op", None) or fn.__name__
                if "update" in op or "batch" in op or "append" in op or "write" in op:
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

# ========= Cached handles/rows/values =========
_cached_ws: dict[str, tuple[float, Any]] = {}
_cached_rows: dict[str, tuple[float, Any]] = {}
_values_cache: dict[str, tuple[float, Any]] = {}
_cache_lock = threading.Lock()

def clear_sheet_caches():
    with _cache_lock:
        _cached_ws.clear()
        _cached_rows.clear()
        _values_cache.clear()

def invalidate_tab(tab: str):
    with _cache_lock:
        _cached_ws.pop(f"ws::{tab}", None)
        for k in list(_values_cache.keys()):
            if k.startswith(f"vals::{tab}::"):
                _values_cache.pop(k, None)
        _cached_rows.pop(f"rows::{tab}", None)

@with_sheet_backoff
def get_ws(name: str):
    return get_sheet().worksheet(name)

def get_ws_cached(name: str, ttl_s: int | None = None):
    ttl_s = DEFAULT_WS_TTL_S if ttl_s is None else ttl_s
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

@with_sheet_backoff
def _ws_get_all_values(ws):
    return ws.get_all_values()

def get_all_records_cached(name: str, ttl_s: int | None = None):
    ttl_s = DEFAULT_ROWS_TTL_S if ttl_s is None else ttl_s
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

def get_records_cached(sheet_name: str, ttl_s: int = 120):
    return get_all_records_cached(sheet_name, ttl_s)

@with_sheet_backoff
def _ws_get(ws, range_a1: str):
    return ws.get(range_a1)

def get_values_cached(sheet_name: str, range_a1: str | None = None, ttl_s: int | None = None):
    ttl_s = DEFAULT_VALUES_TTL_S if ttl_s is None else ttl_s
    key = f"vals::{sheet_name}::{range_a1 or '__ALL__'}"
    with _cache_lock:
        item = _values_cache.get(key)
        if item:
            exp, vals = item
            if time.time() < exp:
                return vals
            _values_cache.pop(key, None)

    ws = get_ws_cached(sheet_name, ttl_s=ttl_s)
    vals = _ws_get(ws, range_a1) if range_a1 else _ws_get_all_values(ws)
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

# ---------------------------------------------------------------------------
# Backwards-compat shim: write_rows_to_sheet
# Older code (and some boot diagnostics) still import this from utils.
# Newer code should prefer SheetsGateway, but this keeps everything happy.
# ---------------------------------------------------------------------------

def write_rows_to_sheet(sheet_name, rows, *args, clear=False, **kwargs):
    """
    Backwards-compat helper to write rows into a worksheet.

    Parameters (loosely inferred from older usage):
        sheet_name: str   – tab name in the main sheet
        rows: list[list]  – rows of values to append or write
        clear: bool       – if True, clear the sheet before writing

    Any extra positional/keyword args are ignored so we don't break old callers
    that may have passed additional flags.
    """
    from utils import get_ws, warn  # self-import is fine inside the function

    if not rows:
        return

    try:
        ws = get_ws(sheet_name)
    except Exception as e:
        warn(f"write_rows_to_sheet: failed to open worksheet {sheet_name!r}: {e}")
        return

    try:
        # Normalise single row vs list of rows
        if rows and not isinstance(rows[0], (list, tuple)):
            rows = [rows]

        if clear:
            ws.clear()

        # Basic append; this is what the old helper did in practice for logs.
        ws.append_rows(rows, value_input_option="USER_ENTERED")

    except Exception as e:
        warn(f"write_rows_to_sheet: error writing to {sheet_name!r}: {e}")

@with_sheet_backoff
def ws_batch_update(ws, writes):
    if not writes: return
    ws.batch_update(writes, value_input_option="RAW")

@with_sheet_backoff
def ws_append_row(ws, values):
    ws.append_row(values, value_input_option="RAW")

def sanitize_range(a1: str) -> str:
    if "!" not in a1: return a1
    tab, rng = a1.split("!", 1)
    tab = tab.split("!")[-1]
    return f"{tab}!{rng}"

@with_sheet_backoff
def ws_update(ws, range_a1, values):
    ws.update(sanitize_range(range_a1), values)

def ensure_sheet_headers(tab: str, required_headers: list[str]) -> list[str]:
    try:
        ws = get_ws_cached(tab, ttl_s=30)
        vals = _ws_get_all_values(ws) or []
        header = (vals[0] if vals else [])[:]
        if not header:
            return header
        existing = {h.strip() for h in header}
        changed = False
        for name in required_headers:
            if name not in existing:
                header.append(name)
                changed = True
        if changed:
            ws_update(ws, "A1", [header])
            invalidate_tab(tab)
        return header
    except Exception as e:
        warn(f"ensure_sheet_headers({tab}) skipped: {e}")
        return []

def ws_get_all_records_cached(name: str, ttl_s: int = 120):
    return get_all_records_cached(name, ttl_s)

def ws_get_values_cached(name: str, ttl_s: int = 60):
    return get_values_cached(name, range_a1=None, ttl_s=ttl_s)

def ping_webhook_debug(message: str):
    try:
        send_telegram_message_dedup(f"🛠️ Debug: {message}", key="webhook_debug")
    except Exception:
        pass
    try:
        ws = get_ws_cached("Webhook_Debug", ttl_s=30)
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        ws_update(ws, "A1", [[f"{ts}Z — {message}"]])
    except Exception:
        pass

def ping_webhook(message: str):
    ping_webhook_debug(message)

def safe_get_all_records(sheet_name: str, ttl_s: int = 120):
    return get_all_records_cached(sheet_name, ttl_s=ttl_s)

from functools import wraps
def backoff_guard(tries=5, base=1.8, first_sleep=1.0):
    def _wrap(fn):
        @wraps(fn)
        def _run(*a, **k):
            sleep = first_sleep
            for i in range(tries):
                try:
                    return fn(*a, **k)
                except Exception as e:
                    if i == tries - 1:
                        raise
                    warn(f"{fn.__name__} retry {i+1}/{tries}: {e}")
                    time.sleep(sleep)
                    sleep *= base
        return _run
    return _wrap

@backoff_guard(tries=6, base=1.6, first_sleep=1.0)
def sheets_append_rows(sheet_url: str, worksheet_name: str, rows: list[list]):
    gc = get_gspread_client()
    sh = gc.open_by_url(sheet_url)
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=200, cols=20)
    ws.append_rows(rows, value_input_option="USER_ENTERED")

WATCHDOG_TAB = os.getenv("WATCHDOG_TAB", "Rotation_Log")
WATCHDOG_TOKEN_COL = os.getenv("WATCHDOG_TOKEN_COL", "Token")
WATCHDOG_TIME_HEADERS = [h.strip() for h in os.getenv(
    "WATCHDOG_TIME_HEADERS",
    "Last Updated,Updated At,Timestamp,Last_Alerted,Last Alerted,Last Seen"
).split(",")]
STALLED_THRESHOLD_HOURS = float(os.getenv("WATCHDOG_STALLED_THRESHOLD_HOURS", "12"))

def str_or_empty(v):
    return str(v).strip() if v is not None else ""

def _parse_dt(val):
    s = str_or_empty(val)
    if not s: return None
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
            if not token: continue
            seen_dt = None
            last_col = None
            for h in time_headers:
                dt = _parse_dt(row.get(h))
                if dt:
                    seen_dt, last_col = dt, h
                    break
            if not seen_dt: continue
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

def to_float(v, default=None):
    s = str_or_empty(v).replace("%", "").replace(",", "")
    if s == "": return default
    try:
        return float(s)
    except Exception:
        return default

def safe_float(v, default=None):
    s = str_or_empty(v)
    if s == "" or s in {"-", "—", "N/A", "n/a", "NA", "na", "None", "null"}:
        return default
    s = s.replace("%", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return default

def safe_int(v, default=None):
    f = safe_float(v, default=None)
    if f is None: return default
    try:
        return int(float(f))
    except Exception:
        return default

def safe_str(v, default=""):
    s = str_or_empty(v)
    return s if s != "" else default

def safe_len(x) -> int:
    try:
        return len(x)
    except TypeError:
        return len(str(x))

# ========= HMAC Helpers (FIXED) =========
def hmac_hex(secret: str, payload: dict) -> str:
    """Canonical signer using sort_keys=True"""
    key = secret.encode()
    msg = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return hmac.new(key, msg, hashlib.sha256).hexdigest()

def hmac_enqueue(intent: dict) -> dict:
    """
    Helper to enqueue an intent to the Bus via HTTP (loopback or external).
    Wraps intent -> envelope -> payload, signs it, and posts.
    """
    base_url = os.getenv("CLOUD_BASE_URL") or os.getenv("OPS_BASE_URL") or "http://127.0.0.1:5000"
    url = f"{base_url.rstrip('/')}/ops/enqueue"
    
    secret = os.getenv("OUTBOX_SECRET", "")
    if not secret:
        return {"ok": False, "reason": "no_outbox_secret"}
        
    # Construct Envelope
    agent_id = (os.getenv("AGENT_ID") or "cloud").split(",")[0]
    envelope = {
        "agent_id": agent_id,
        "type": "order.place",
        "payload": intent,
        "ts": int(time.time())
    }
    
    # Wrap for API: {"payload": envelope}
    api_body = {"payload": envelope}
    
    try:
        sig = hmac_hex(secret, api_body)
    except Exception as e:
        return {"ok": False, "reason": f"signing_error: {e}"}
        
    headers = {
        "Content-Type": "application/json",
        "X-Nova-Signature": sig,
        "X-NT-Sig": sig,
        "X-Timestamp": str(int(time.time()))
    }
    
    try:
        resp = _REQ.post(url, json=api_body, headers=headers, timeout=10)
        if resp.ok:
            return resp.json()
        return {"ok": False, "reason": f"http_{resp.status_code}", "body": resp.text}
    except Exception as e:
        return {"ok": False, "reason": f"connection_error: {e}"}
