# performance_dashboard.py
import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from utils import with_sheet_backoff, safe_float

# --- tiny wrappers to keep all Sheets calls 429-safe -------------------------
@with_sheet_backoff
def _open_sheet(url: str):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds).open_by_url(url)

@with_sheet_backoff
def _get_all_records(ws):
    return ws.get_all_records()

@with_sheet_backoff
def _get_acell(ws, a1):
    return ws.acell(a1).value

@with_sheet_backoff
def _update_range(ws, a1, rows):
    # single batch update to minimize quota
    return ws.update(a1, rows, value_input_option="USER_ENTERED")

# --- helpers -----------------------------------------------------------------
def _to_float_or_none(v):
    """
    Parse numbers like '12.3', '12.3%', '', 'N/A' safely.
    Returns float or None.
    """
    s = str(v).strip()
    if not s or s.upper() in {"N/A", "NA", "NONE", "NULL", "—", "-"}:
        return None
    # prefer utils.safe_float but allow None detection
    try:
        s = s.replace("%", "").strip()
        return float(s)
    except Exception:
        # fall back to utils.safe_float defaulting to 0.0, then treat 0.0-from-empty as None
        val = safe_float(v, 0.0)
        return val if str(v).strip() not in {"", "N/A", "NA"} else None

def run_performance_dashboard():
    sheet_url = os.getenv("SHEET_URL")
    if not sheet_url:
        print("⚠️ SHEET_URL not set; skipping Performance Dashboard.")
        return

    sh = _open_sheet(sheet_url)
    stats_ws = sh.worksheet("Rotation_Stats")
    dash_ws = sh.worksheet("Performance_Dashboard")

    # Read Rotation_Stats once
    stats = _get_all_records(stats_ws)

    # 1) Totals
    total_yes = sum(1 for r in stats if str(r.get("Decision", "")).strip().upper() == "YES")

    # 2) Collect numeric performance values
    roi_values = []
    token_rois = {}
    for r in stats:
        token = str(r.get("Token", "")).strip()
        perf_raw = r.get("Performance", "")
        perf = _to_float_or_none(perf_raw)
        if perf is None:
            continue
        roi_values.append(perf)
        if token:
            token_rois[token] = perf

    avg_roi = round(sum(roi_values) / len(roi_values), 2) if roi_values else 0.0
    top_token = max(token_rois, key=token_rois.get, default="N/A")
    bottom_token = min(token_rois, key=token_rois.get, default="N/A")
    unique_tokens = len(token_rois)

    # 3) Low-quota heartbeat fetch (single cell)
    try:
        last_update_val = _get_acell(sh.worksheet("NovaHeartbeat"), "A2")
        last_update = last_update_val if str(last_update_val).strip() else datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        last_update = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # 4) Prepare rows (A2:B9) – keep these placeholders for now; replace later if you wire real calc
    rows = [
        ["Total YES Votes", total_yes],
        ["Average ROI (YES)", f"{avg_roi}%"],
        ["Top Performer", top_token],
        ["Worst Performer", bottom_token],
        ["Projected Portfolio Value", "$5,000.00"],
        ["% Progress to $250K Goal", "2.0%"],
        ["Unique Tokens Rotated", unique_tokens],
        ["Last Updated", last_update],
    ]

    # Single batch write
    _update_range(dash_ws, "A2:B9", rows)
