import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# utils hooks: backoff + helpers
from utils import with_sheet_backoff, safe_float, get_sheet

# ---------- tiny wrapped readers (429-safe via decorator) ----------
@with_sheet_backoff
def _ws_get_all_records(ws):
    return ws.get_all_records()

@with_sheet_backoff
def _ws_acell(ws, a1):
    return ws.acell(a1).value

@with_sheet_backoff
def _ws_batch_update(ws, rng, two_col_rows):
    """
    Write 2-column rows into range like 'A2:B9'.
    """
    ws.update(rng, two_col_rows, value_input_option="USER_ENTERED")

# ---------- core ----------
def _to_float_or_none(v):
    # robust: handles int/float/str/%/blank
    try:
        return float(str(v).replace("%", "").strip())
    except Exception:
        return None

def run_performance_dashboard():
    # 1) open once
    sh = get_sheet()
    stats_ws = sh.worksheet("Rotation_Stats")
    dash_ws  = sh.worksheet("Performance_Dashboard")

    # 2) read once
    stats = _ws_get_all_records(stats_ws)

    # 3) compute
    total_yes = 0
    roi_values = []
    token_rois = {}

    for row in stats:
        decision = (row.get("Decision") or "").strip().upper()
        token = (row.get("Token") or "").strip()
        if decision == "YES":
            total_yes += 1

        # accept either "Performance" or "Follow-up ROI" if present
        perf_raw = row.get("Performance", "")
        if perf_raw in ("", None):
            perf_raw = row.get("Follow-up ROI", "")

        val = _to_float_or_none(perf_raw)
        if val is not None:
            roi_values.append(val)
            if token:
                token_rois[token] = val

    avg_roi = round(sum(roi_values) / len(roi_values), 2) if roi_values else 0.0
    top_token = max(token_rois, key=token_rois.get, default="N/A")
    bottom_token = min(token_rois, key=token_rois.get, default="N/A")
    unique_rotated = len(set([t for t in token_rois.keys() if t]))

    # 4) heartbeat timestamp (cheap single cell)
    try:
        last_update = _ws_acell(sh.worksheet("NovaHeartbeat"), "A2") or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        last_update = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # 5) single batched write into A2:B9 (8 rows, 2 cols)
    rows = [
        ["Total YES Votes",           total_yes],
        ["Average ROI (YES)",         f"{avg_roi}%"],
        ["Top Performer",             top_token],
        ["Worst Performer",           bottom_token],
        ["Projected Portfolio Value", "$5,000.00"],   # keep placeholder until you wire a calc
        ["% Progress to $250K Goal",  "2.0%"],        # keep placeholder until you wire a calc
        ["Unique Tokens Rotated",     unique_rotated],
        ["Last Updated",              last_update],
    ]
    _ws_batch_update(dash_ws, "A2:B9", rows)
