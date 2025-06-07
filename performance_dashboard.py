import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

def run_performance_dashboard():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    stats = sheet.worksheet("Rotation_Stats").get_all_records()
    dashboard = sheet.worksheet("Performance_Dashboard")

    total_votes = len([r for r in stats if r["Decision"] == "YES"])
    roi_values = []
    token_rois = {}

    for row in stats:
        token = row["Token"]
        perf = row.get("Performance", "")
        try:
            val = float(perf)
            roi_values.append(val)
            token_rois[token] = val
        except:
            continue

    avg_roi = round(sum(roi_values) / len(roi_values), 2) if roi_values else 0.0
    top_token = max(token_rois, key=token_rois.get, default="N/A")
    bottom_token = min(token_rois, key=token_rois.get, default="N/A")

    # Low-quota fallback: only fetch NovaHeartbeat!A2 (not all records)
    try:
        value = sheet.worksheet("NovaHeartbeat").acell("A2").value
        last_update = value if value else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except:
        last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    dashboard.update("A2", [
        ["Total YES Votes", total_votes],
        ["Average ROI (YES)", f"{avg_roi}%"],
        ["Top Performer", top_token],
        ["Worst Performer", bottom_token],
        ["Projected Portfolio Value", "$5,000.00"],
        ["% Progress to $250K Goal", "2.0%"],
        ["Unique Tokens Rotated", len(set(token_rois.keys()))],
        ["Last Updated", last_update]
    ])
