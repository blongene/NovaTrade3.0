import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from utils import with_sheet_backoff

def _gclient():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds)

@with_sheet_backoff
def _open_ws(sheet_url: str, title: str):
    sh = _gclient().open_by_url(sheet_url)
    return sh.worksheet(title)

def _to_float(v, default=None):
    try:
        s = str(v).strip().replace("%", "")
        if s == "" or s.lower() in {"n/a", "na", "none"}:
            return default
        return float(s)
    except Exception:
        return default

def run_performance_dashboard():
    sheet_url = os.getenv("SHEET_URL")
    stats_ws = _open_ws(sheet_url, "Rotation_Stats")
    dash_ws  = _open_ws(sheet_url, "Performance_Dashboard")

    # Pull as records once (cheaper than multiple calls)
    @with_sheet_backoff
    def _stats():
        return stats_ws.get_all_records()
    stats = _stats()

    # Totals / aggregates
    total_yes = 0
    roi_values = []
    per_token = {}

    for row in stats:
        # Defensive header access
        decision = (row.get("Decision") or "").strip().upper()
        token    = (row.get("Token") or "").strip()
        perf     = row.get("Performance", "")

        if decision == "YES":
            total_yes += 1

        val = _to_float(perf)
        if val is not None and token:
            roi_values.append(val)
            per_token[token] = val

    avg_roi = round(sum(roi_values) / len(roi_values), 2) if roi_values else 0.0
    top_token = max(per_token, key=per_token.get, default="N/A")
    bottom_token = min(per_token, key=per_token.get, default="N/A")
    unique_rotated = len(per_token)

    # Low‑quota heartbeat grab: only A2
    try:
        @with_sheet_backoff
        def _hb():
            hb_ws = _open_ws(sheet_url, "NovaHeartbeat")
            return hb_ws.acell("A2").value
        last_update = _hb() or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        last_update = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Write in a single batch update (A2:B9 area expected by your layout)
    values = [
        ["Total YES Votes", total_yes],
        ["Average ROI (YES)", f"{avg_roi}%"],
        ["Top Performer", top_token],
        ["Worst Performer", bottom_token],
        ["Projected Portfolio Value", "$5,000.00"],   # placeholder you can wire later
        ["% Progress to $250K Goal", "2.0%"],         # placeholder you can wire later
        ["Unique Tokens Rotated", unique_rotated],
        ["Last Updated", last_update],
    ]

    @with_sheet_backoff
    def _update():
        dash_ws.update("A2:B9", values, value_input_option="USER_ENTERED")
    _update()

    print("✅ Performance Dashboard updated.")
