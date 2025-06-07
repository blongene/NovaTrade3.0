import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run_rebalance_scanner():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    rebalance_ws = sheet.worksheet("Rebalance")

    log_data = sheet.worksheet("Rotation_Log").get_all_records()
    rows = []
    for row in log_data:
        token = row["Token"]
        score = row.get("Score", "0")
        try:
            score_val = float(score)
        except:
            score_val = 0.0

        current_allocation = row.get("Allocation", "0%").replace("%", "")
        try:
            alloc_val = float(current_allocation)
        except:
            alloc_val = 0.0

        suggested_action = "Sell" if alloc_val > 20 else "Buy" if alloc_val < 5 else "Hold"
        rows.append([token, f"{alloc_val:.2f}%", "20.00%", suggested_action, f"${round(alloc_val * 10, 2)}"])

    rebalance_ws.clear()
    rebalance_ws.append_row(["Token", "Current %", "Target %", "Suggested Action", "Rebalance Amount (USD)"])
    for r in rows:
        rebalance_ws.append_row(r)
