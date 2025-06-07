import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

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

    # Look for the most recent valid timestamp in NovaHeartbeat
    try:
        heartbeat_rows = sheet.worksheet("NovaHeartbeat").get_all_records()
        latest_valid = next((r for r in reversed(heartbeat_rows) if r.get("Timestamp")), None)
        last_update = latest_valid["Timestamp"] if latest_valid else "N/A"
    except Exception as e:
        last_update = "N/A"

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
