
# telemetry_digest.py — Phase 9D
# Pulls /api/telemetry/last (local) and writes a heartbeat row to NovaHeartbeat.
# If telemetry age > threshold, posts a Telegram alert (deduped).
import os, time
from datetime import datetime, timezone

HEARTBEAT_WS = os.getenv("HEARTBEAT_WS", "NovaHeartbeat")
HEARTBEAT_ALERT_MIN = int(os.getenv("HEARTBEAT_ALERT_MIN", "90"))  # minutes
SHEET_URL = os.getenv("SHEET_URL", "")
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

try:
    from utils import get_gspread_client, send_telegram_message_dedup, warn, info
except Exception:
    get_gspread_client = None
    send_telegram_message_dedup = None
    def warn(x): print("[telemetry_digest] WARN:", x)
    def info(x): print("[telemetry_digest] INFO:", x)

def _http_get(url, timeout=6):
    try:
        import requests
        r = requests.get(url, timeout=timeout)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return {}

def _send_tg(text, key):
    if send_telegram_message_dedup:
        try:
            send_telegram_message_dedup(text, key=key, ttl_min=120)
            return
        except Exception:
            pass
    if BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            import requests
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
                timeout=8,
            )
        except Exception:
            pass

# --- drop-in patch: heartbeat sheet hygiene + top-insert writing ---

def _ensure_ws(sh, name: str, headers: list[str]):
    """Return a worksheet with a correct header row.
    - Creates the sheet if missing
    - Fixes header row if empty or wrong
    """
    try:
        ws = sh.worksheet(name)
    except Exception:
        ws = sh.add_worksheet(title=name, rows=2000, cols=max(8, len(headers) + 2))

    # Ensure header row is present & correct
    try:
        first = ws.row_values(1)
        if [h.strip() for h in first] != headers:
            # Overwrite row 1 safely
            ws.update('1:1', [headers], value_input_option="USER_ENTERED")
    except Exception:
        # Fallback if read failed for any reason
        ws.update('1:1', [headers], value_input_option="USER_ENTERED")
    return ws


def _trim_tail(ws, key_col: int = 1):
    """One-time housekeeping: remove trailing empty rows after the last non-empty
    in `key_col` (default column A).
    Controlled by env HEARTBEAT_TRIM_TAIL_ON_BOOT in run_telemetry_digest().
    """
    try:
        col = ws.col_values(key_col)  # list already trimmed by Sheets API
        last = len(col)
        # walk back to last non-empty
        while last > 1 and (col[last - 1] or "").strip() == "":
            last -= 1

        total = ws.row_count
        if total > last:
            start = last + 1
            # delete in chunks so we don't exceed batch limits
            while start <= total:
                end = min(start + 499, total)
                ws.delete_rows(start, end)
                total -= (end - start + 1)
    except Exception:
        # non-fatal; leave as-is if trimming fails
        pass


def _insert_or_append(ws, row: list, mode: str = "top"):
    """
    mode:
      - 'top'    : insert at row 2 (newest-first under headers)
      - 'append' : standard append at bottom (can hit ghost tails)
    """
    mode = (mode or "top").lower()
    if mode == "append":
        ws.append_row(row, value_input_option="USER_ENTERED")
    else:
        ws.insert_rows([row], row=2, value_input_option="USER_ENTERED")


def run_telemetry_digest():
    if not SHEET_URL:
        warn("SHEET_URL missing; abort.")
        return

    # 1) Pull telemetry snapshot from local Bus
    port = os.getenv("PORT", "10000")
    url = f"http://127.0.0.1:{port}/api/telemetry/last"
    j = _http_get(url) or {}

    age_sec = j.get("age_sec")
    # accept either {agent_id:"..."} or {agent:"..."}
    agent = j.get("agent_id") or (j.get("agent") if isinstance(j.get("agent"), str) else None) or ""
    # support both flat/by_venue at top-level or nested under "telemetry"
    t = j.get("telemetry") if isinstance(j.get("telemetry"), dict) else {}
    flat = j.get("flat") or t.get("flat") or {}
    by_venue = j.get("by_venue") or t.get("by_venue") or {}

    # 2) Write to NovaHeartbeat (top-insert by default)
    try:
        gc = get_gspread_client()
        sh = gc.open_by_url(SHEET_URL)
        headers = ["Timestamp", "Agent", "Age_sec", "Flat_Tokens", "Venues", "Note"]
        ws = _ensure_ws(sh, HEARTBEAT_WS, headers)

        # Optional one-time tail cleanup
        if os.getenv("HEARTBEAT_TRIM_TAIL_ON_BOOT", "0").lower() in ("1", "true", "yes"):
            _trim_tail(ws)

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = [
            ts,
            agent,
            "" if age_sec is None else int(age_sec),
            len(flat),
            len(by_venue),
            ""
        ]

        mode = os.getenv("HEARTBEAT_APPEND_MODE", "top")  # 'top' | 'append'
        _insert_or_append(ws, row, mode)
        info(f"Heartbeat row written (mode={mode}).")
    except Exception as e:
        warn(f"heartbeat write failed: {e}")

    # 3) Alert if stale
    try:
        if age_sec is not None:
            age_min = age_sec / 60.0
            if age_min > HEARTBEAT_ALERT_MIN:
                _send_tg(f"⚠️ Edge heartbeat stale: {int(age_min)} min (>{HEARTBEAT_ALERT_MIN} min)", key=f"hb:{int(age_min)}")
    except Exception:
        pass

if __name__ == "__main__":
    run_telemetry_digest()
