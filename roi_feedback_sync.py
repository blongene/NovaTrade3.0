# roi_feedback_sync.py
import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import with_sheet_backoff, get_gspread_client, send_telegram_message, ping_webhook_debug

SHEET_URL = os.getenv("SHEET_URL")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def _open_sheet():
    client = get_gspread_client()
    return client.open_by_url(SHEET_URL)

@with_sheet_backoff
def _get_all(ws):
    return ws.get_all_values()

@with_sheet_backoff
def _append(ws, row):
    ws.append_row(row, value_input_option="USER_ENTERED")

@with_sheet_backoff
def _update_cell(ws, r, c, v):
    ws.update_cell(r, c, v)

def _header_index_map(headers):
    return {h.strip(): i for i, h in enumerate(headers)}

def _normalize(s):
    return (s or "").strip().upper()

def _safe_float(x, default=None):
    try:
        return float(str(x).replace("%", "").strip())
    except Exception:
        return default

def run_roi_feedback_sync():
    """
    Syncs 'Would you vote YES again?' feedback from ROI_Review_Log into Rotation_Stats.
    - Writes 'Last Feedback' (YES/NO/SKIP)
    - Writes 'Last Feedback At' (UTC)
    - Optionally updates 'Performance Note' column if present
    Skips rows without valid token or feedback. Backoff protects against 429s.
    """
    try:
        print("🔄 Syncing ROI feedback from ROI_Review_Log → Rotation_Stats ...")
        sh = _open_sheet()

        log_ws = sh.worksheet("ROI_Review_Log")
        stats_ws = sh.worksheet("Rotation_Stats")

        log_vals = _get_all(log_ws)
        stats_vals = _get_all(stats_ws)

        if not log_vals or not stats_vals:
            print("⚠️ Missing sheet values; aborting feedback sync.")
            return

        log_headers = log_vals[0]
        stats_headers = stats_vals[0]
        log_idx = _header_index_map(log_headers)
        stats_idx = _header_index_map(stats_headers)

        # Required columns
        t_col = log_idx.get("Token", 1)
        d_col = log_idx.get("Decision", 2)

        # Stats columns (create if missing)
        if "Last Feedback" not in stats_idx:
            stats_headers.append("Last Feedback")
            stats_idx["Last Feedback"] = len(stats_headers) - 1
        if "Last Feedback At" not in stats_idx:
            stats_headers.append("Last Feedback At")
            stats_idx["Last Feedback At"] = len(stats_headers) - 1
        if stats_headers != stats_vals[0]:
            # write updated header row
            _update_cell(stats_ws, 1, 1, stats_headers[0])  # touch to ensure worksheet is “dirty”
            stats_ws.update("A1", [stats_headers])

        # Build a token → row mapping for Rotation_Stats for fast lookup
        token_col = stats_idx.get("Token")
        if token_col is None:
            print("⛔️ Rotation_Stats requires a 'Token' column. Aborting.")
            return

        stats_map = {}  # TOKEN -> (row_num, row_list)
        for r_i, row in enumerate(stats_vals[1:], start=2):
            tok = _normalize(row[token_col]) if token_col < len(row) else ""
            if tok:
                stats_map[tok] = (r_i, row)

        updates = 0
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for log_row in log_vals[1:]:
            token = _normalize(log_row[t_col] if t_col < len(log_row) else "")
            decision = _normalize(log_row[d_col] if d_col < len(log_row) else "")
            if not token or decision not in {"YES", "NO", "SKIP"}:
                continue

            if token not in stats_map:
                # Not all tokens must be in Rotation_Stats—skip silently
                continue

            # Prepare row to update
            row_num, current = stats_map[token]
            # Ensure row length >= required columns
            need_len = max(stats_idx["Last Feedback"], stats_idx["Last Feedback At"]) + 1
            if len(current) < need_len:
                current += [""] * (need_len - len(current))

            last_fb_val = current[stats_idx["Last Feedback"]]
            last_at_val = current[stats_idx["Last Feedback At"]]

            # Update if different (idempotent)
            if (last_fb_val or "").strip().upper() != decision or not last_at_val:
                _update_cell(stats_ws, row_num, stats_idx["Last Feedback"] + 1, decision)
                _update_cell(stats_ws, row_num, stats_idx["Last Feedback At"] + 1, now)
                updates += 1

        print(f"✅ ROI Feedback sync complete. {updates} row(s) updated.")
        if updates and TELEGRAM_CHAT_ID:
            send_telegram_message(f"🧠 ROI feedback sync complete • {updates} stats row(s) updated.")
    except Exception as e:
        print(f"❌ roi_feedback_sync error: {e}")
        ping_webhook_debug(f"roi_feedback_sync error: {e}")
