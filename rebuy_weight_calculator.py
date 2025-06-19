import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run_rebuy_weight_calculator():
    print("🔁 Calculating Rebuy Weights...")

    try:
        # Auth to Google Sheets
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        stats_ws = sheet.worksheet("Rotation_Stats")

        data = stats_ws.get_all_records()
        headers = stats_ws.row_values(1)

        # Column index helpers
        count_idx = headers.index("Rebuy Count")
        avg_roi_idx = headers.index("Avg Rebuy ROI")
        weight_col = headers.index("Rebuy Weight") + 1 if "Rebuy Weight" in headers else len(headers) + 1

        # Add header if missing
        if "Rebuy Weight" not in headers:
            stats_ws.update_cell(1, weight_col, "Rebuy Weight")

        updated = 0
        for i, row in enumerate(data, start=2):
            try:
                count = int(row.get("Rebuy Count", 0))
                avg_roi = float(str(row.get("Avg Rebuy ROI", "0")).replace("%", ""))
                weight = round(count * (avg_roi / 100), 2)
            except:
                weight = 0.0

            stats_ws.update_cell(i, weight_col, weight)
            updated += 1

        print(f"✅ Rebuy Weights updated for {updated} tokens.")

    except Exception as e:
        print(f"❌ Error in run_rebuy_weight_calculator: {e}")

if __name__ == "__main__":
    run_rebuy_weight_calculator()
