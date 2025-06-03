# performance_dashboard.py

import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

def run_performance_dashboard():
    try:
        # Auth to Google Sheets
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        stats_ws = sheet.worksheet("Rotation_Stats")
        trade_ws = sheet.worksheet("Trade_Log")
        dash_ws = sheet.worksheet("Performance_Dashboard")

        # Pull data
        stats_data = stats_ws.get_all_records()
        trade_data = trade_ws.get_all_records()

        # Clean & parse
        roi_values = [float(r.get("Follow-up ROI", 0) or 0) for r in stats_data if r.get("Decision") == "YES"]
        tokens = [r.get("Token", "") for r in stats_data if r.get("Decision") == "YES"]
        unique_rotations = set([r.get("Token", "") for r in trade_data])

        # Metrics
        total_yes = len(roi_values)
        avg_roi = round(sum(roi_values) / total_yes, 2) if roi_values else 0
        best_token = max(stats_data, key=lambda r: float(r.get("Follow-up ROI", 0) or 0)).get("Token", "N/A")
        worst_token = min(stats_data, key=lambda r: float(r.get("Follow-up ROI", 0) or 0)).get("Token", "N/A")

        # Calculate % progress toward $250K from $5K
        growth_factor = 1 + (avg_roi / 100)
        projected_balance = 5000 * growth_factor
        goal_progress = min(round((projected_balance / 250000) * 100, 2), 100)

        # Output to dashboard
        dashboard_rows = [
            ["Metric", "Value"],
            ["Total YES Votes", total_yes],
            ["Average ROI (YES)", f"{avg_roi}%"],
            ["Top Performer", best_token],
            ["Worst Performer", worst_token],
            ["Projected Portfolio Value", f"${projected_balance:,.2f}"],
            ["% Progress to $250K Goal", f"{goal_progress}%"],
            ["Unique Tokens Rotated", len(unique_rotations)],
            ["Last Updated", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")]
        ]

        dash_ws.clear()
        dash_ws.update("A1", dashboard_rows)
        print("✅ Performance dashboard updated.")

    except Exception as e:
        print(f"❌ Dashboard sync failed: {e}")
