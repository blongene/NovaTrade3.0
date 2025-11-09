
# council_ledger.py — Hardened drop‑in (Phase 9B/9C)
# Audit summary of prior version:
# - Pros: simple, clear; creates/ensures both tabs; consistent headers; UTC timestamps.
# - Risks:
#   • _ensure_headers() clears the entire worksheet if header order differs — potential data loss.
#   • No retries/backoff → can 429 under Sheets pressure.
#   • No fast tab bootstrap (ensure once at boot).
#   • No basic sanitation/length guard on free‑form fields.
#   • No optional de‑dup guard to prevent spammy repeats.
#
# This version:
# - Avoids destructive clears — patches headers non‑destructively (adds missing columns, preserves order/data).
# - Lightweight retries with exponential backoff (no external utils dependency).
# - Exposes ensure_ledger_tabs() to be called once at boot.
# - Sanitizes strings and trims to safe lengths.
# - Optional dedup for last N rows (off by default; enable via env LEDGER_DEDUP=1).
#
import os
import time
from datetime import datetime
from typing import List
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.getenv("SHEET_URL", "")
VOICE_WS  = os.getenv("COUNCIL_VOICE_WS", "Council_Voice")
RECKON_WS = os.getenv("ASHS_RECKONING_WS", "Ashs_Reckoning")
LEDGER_DEDUP = str(os.getenv("LEDGER_DEDUP", "0")).lower() in ("1","true","yes","on")
LEDGER_DEDUP_WINDOW = int(os.getenv("LEDGER_DEDUP_WINDOW", "25") or 25)

# ---------- Backoff wrappers (minimal, internal) ----------
def _retry(fn, *args, **kwargs):
    delay = 0.8
    for attempt in range(5):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last = e
            time.sleep(delay)
            delay = min(6.0, delay * 1.7)
    raise last  # noqa: F821

def _open_sheet():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    if not SHEET_URL:
        raise ValueError("SHEET_URL not set for council_ledger.py")
    return _retry(client.open_by_url, SHEET_URL)

# ---------- Header helpers (non‑destructive) ----------
def _row1(ws) -> List[str]:
    try:
        return ws.row_values(1)
    except Exception:
        return []

def _ensure_headers(ws, desired: List[str]):
    existing = _row1(ws)
    if not existing:
        _retry(ws.append_row, desired, value_input_option="USER_ENTERED")
        return

    # Add any missing headers at the end; preserve existing order and data.
    missing = [h for h in desired if h not in existing]
    if missing:
        ws.resize(rows=ws.row_count, cols=max(ws.col_count, len(existing)+len(missing)))
        new_header = existing + missing
        _retry(ws.update, "1:1", [new_header], value_input_option="USER_ENTERED")

def _get_ws(sh, name: str, headers: List[str]):
    try:
        ws = sh.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        ws = _retry(sh.add_worksheet, title=name, rows=1000, cols=max(8, len(headers)+2))
    _ensure_headers(ws, headers)
    return ws

# ---------- Sanitation / trimming ----------
def _s(val, max_len=400):
    if val is None:
        return ""
    s = str(val).replace("\r"," ").replace("\n"," ").strip()
    if len(s) > max_len:
        s = s[:max_len]
    return s

# ---------- Optional short-window de‑dup ----------
def _is_dup(ws, row_tuple: tuple) -> bool:
    if not LEDGER_DEDUP:
        return False
    try:
        values = ws.get_all_values()
    except Exception:
        return False
    tail = values[1:][-LEDGER_DEDUP_WINDOW:] if len(values) > 1 else []
    for r in tail:
        if tuple(r[:len(row_tuple)]) == row_tuple:
            return True
    return False

# ---------- Public API ----------
def ensure_ledger_tabs():
    sh = _open_sheet()
    _get_ws(sh, VOICE_WS, ["Timestamp","Actor","Scope","Message","Token","Ref"])
    _get_ws(sh, RECKON_WS, ["Timestamp","Event","OK","Reason","Token","Action","Amount_USD","Venue","Quote","Patched","Ref"])
    print("[CouncilLedger] Tabs ensured.")

def log_council_voice(actor: str, scope: str, message: str, token: str = "", ref: str = ""):
    sh = _open_sheet()
    ws = _get_ws(sh, VOICE_WS, ["Timestamp","Actor","Scope","Message","Token","Ref"])
    row = (
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        _s(actor, 60),
        _s(scope, 60),
        _s(message, 400),
        _s(token, 32),
        _s(ref, 120),
    )
    if not _is_dup(ws, row):
        _retry(ws.append_row, list(row), value_input_option="USER_ENTERED")

def log_ash_reckoning(event: str, ok: bool, reason: str = "", token: str = "", action: str = "",
                      amount_usd: str = "", venue: str = "", quote: str = "", patched: str = "", ref: str = ""):
    sh = _open_sheet()
    ws = _get_ws(sh, RECKON_WS, ["Timestamp","Event","OK","Reason","Token","Action","Amount_USD","Venue","Quote","Patched","Ref"])
    row = (
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        _s(event, 60),
        "TRUE" if ok else "FALSE",
        _s(reason, 200),
        _s(token, 32),
        _s(action, 32),
        _s(amount_usd, 24),
        _s(venue, 24),
        _s(quote, 12),
        _s(patched, 400),
        _s(ref, 120),
    )
    if not _is_dup(ws, row):
        _retry(ws.append_row, list(row), value_input_option="USER_ENTERED")

if __name__ == "__main__":
    ensure_ledger_tabs()
    print("[CouncilLedger] Ready.")
