# === File: auto_confirm_planner.py ===
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

def get_ws(name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    return client.open_by_url(os.getenv("SHEET_URL")).worksheet(name)

def run_auto_confirm_planner():
    print("📋 Running Auto-Confirm for Rotation_Planner...")
    planner_ws = get_ws("Rotation_Planner")
    scout_ws = get_ws("Scout Decisions")

    planner_rows = planner_ws.get_all_records()
    scout_rows = scout_ws.get_all_records()

    confirmed_count = 0
    for i, row in enumerate(planner_rows, start=2):  # header = row 1
        if not row.get("Confirmed", "").strip():
            token = row.get("Token", "").strip().upper()
            match = next((s for s in scout_rows if s.get("Token", "").strip().upper() == token and s.get("Decision", "").upper() == "YES"), None)
            if match:
                planner_ws.update_acell(f"D{i}", "YES")
                print(f"✅ Auto-confirmed: {token}")
                confirmed_count += 1

    print(f"✅ Auto-Confirm complete. {confirmed_count} token(s) updated.")
