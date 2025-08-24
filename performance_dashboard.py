import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import with_sheet_backoff

DEBUG = os.getenv("DEBUG", "0") == "1"
def _log(msg: str): 
    if DEBUG: 
        print(msg)

@with_sheet_backoff
def _get_all_records(ws):
    return ws.get_all_records()

@with_sheet_backoff
def _get_value(ws, a1):
    return ws.acell(a1).value

@with_sheet_backoff
def _update_range(ws, a1, rows):
    return ws.update(a1, rows, value_input_option="USER_ENTERED")

def _to_float(value, default=None):
    try:
        s = str(value).strip().replace("%", "")
        return float(s)
    except Exception:
        return default

def run_performance_dashboard():
    """
    Reads Rotation_Stats (low calls), computes summary,
    and writes a compact block to Performance_Dashboard!A2:B9.
    All operations are wrapped with Sheets backoff.
    """
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    stats_ws = sheet.worksheet("Rotation_Stats")
    dash_ws  = sheet.worksheet("Performance_Dashboard")

    # single read of records
    stats = _get_all_records(stats_ws)
    _log(f"stats rows: {len(stats)}")

    # collect ROI from column "Performance" or "Follow-up ROI"/"ROI" fallbacks
    roi_vals = []
    token_rois = {}
    total_yes = 0

    for row in stats:
        decision = (row.get("Decision", "") or "").strip().upper()
        if decision == "YES":
            total_yes += 1

        perf = row.get("Performance", "")
        if perf in (None, ""):
            # fallbacks commonly seen in earlier sheets
            perf = row.get("Follow-up ROI", row.get("ROI", ""))

        roi = _to_float(perf)
        if roi is not None:
            token = (row.get("Token", "") or "").strip()
            roi_vals.append(roi)
            if token:
                token_rois[token] = roi

    avg_roi = round(sum(roi_vals) / len(roi_vals), 2) if roi_vals else 0.0
    top_token = max(token_rois, key=token_rois.get, default="N/A")
    bottom_token = min(token_rois, key=token_rois.get, default="N/A")

    # Low‑quota heartbeat: one cell read only
    try:
        last_update_val = _get_value(sheet.worksheet("NovaHeartbeat"), "A2")
        last_update = last_update_val or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        last_update = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Static placeholders you can wire later
    projected_value = "$5,000.00"
    progress_to_goal = "2.0%"
    unique_tokens = len(set(k for k in token_rois.keys() if k))

    rows = [
        ["Total YES Votes", total_yes],
        ["Average ROI (YES)", f"{avg_roi}%"],
        ["Top Performer", top_token],
        ["Worst Performer", bottom_token],
        ["Projected Portfolio Value", projected_value],
        ["% Progress to $250K Goal", progress_to_goal],
        ["Unique Tokens Rotated", unique_tokens],
        ["Last Updated", last_update],
    ]
    _update_range(dash_ws, "A2:B9", rows)
    _log("performance_dashboard: update complete")
