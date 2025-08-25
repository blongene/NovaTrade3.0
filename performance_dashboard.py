import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import with_sheet_backoff, safe_float

# --- tiny, 429-safe helpers ---------------------------------------------------

@with_sheet_backoff
def _open_sheet(url):
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "sentiment-log-service.json", scope
    )
    return gspread.authorize(creds).open_by_url(url)

@with_sheet_backoff
def _get_records(ws):
    return ws.get_all_records()

@with_sheet_backoff
def _get_acell(ws, a1):
    return ws.acell(a1).value

@with_sheet_backoff
def _update(ws, a1, rows):
    # batch update in a single call (reduces write RPCs)
    ws.update(a1, rows, value_input_option="USER_ENTERED")

# --- main ---------------------------------------------------------------------

def run_performance_dashboard():
    try:
        sheet = _open_sheet(os.getenv("SHEET_URL"))
        stats_ws = sheet.worksheet("Rotation_Stats")
        dash_ws  = sheet.worksheet("Performance_Dashboard")

        stats = _get_records(stats_ws)

        # YES votes
        total_yes = sum(1 for r in stats if (r.get("Decision") or "").strip().upper() == "YES")

        # collect numeric performance values
        roi_values = []
        token_rois = {}
        for r in stats:
            token = (r.get("Token") or "").strip()
            perf_raw = r.get("Performance", "")
            # robust parse: handles "", "12", "12.5", "12%", None, ints, etc.
            perf = safe_float(perf_raw, default=None)
            if perf is None:
                continue
            roi_values.append(perf)
            if token:
                token_rois[token] = perf

        avg_roi = round(sum(roi_values) / len(roi_values), 2) if roi_values else 0.0
        top_token = max(token_rois, key=token_rois.get, default="N/A")
        bottom_token = min(token_rois, key=token_rois.get, default="N/A")
        unique_rotated = len(set(token_rois.keys()))

        # low‑quota heartbeat read (single cell)
        try:
            last_update = _get_acell(sheet.worksheet("NovaHeartbeat"), "A2") or \
                          datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            last_update = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # write all dashboard stats in **one** update call
        rows = [
            ["Total YES Votes",           total_yes],
            ["Average ROI (YES)",         f"{avg_roi}%"],
            ["Top Performer",             top_token],
            ["Worst Performer",           bottom_token],
            ["Projected Portfolio Value", "$5,000.00"],     # keep as placeholder
            ["% Progress to $250K Goal",  "2.0%"],          # keep as placeholder
            ["Unique Tokens Rotated",     unique_rotated],
            ["Last Updated",              last_update],
        ]
        _update(dash_ws, "A2", rows)

        print("✅ Performance Dashboard updated.")
    except Exception as e:
        print(f"❌ Performance dashboard error: {e}")
