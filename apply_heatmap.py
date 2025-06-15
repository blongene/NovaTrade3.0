import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# === SETUP ===
def get_gspread_ws(tab_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    return sheet.worksheet(tab_name)

# === MAIN FUNCTION ===
def run_apply_heatmap():
    print("🎨 Running Follow-up ROI Heatmap...")
    ws = get_gspread_ws("Rotation_Log")
    data = ws.get_all_values()
    headers = data[0]

    roi_col = headers.index("Follow-up ROI") + 1
    sent_col = headers.index("Sentiment") + 1

    requests = []
    for i, row in enumerate(data[1:], start=2):
        roi = row[roi_col - 1].strip().replace("%", "")
        sent = row[sent_col - 1].strip()

        try:
            roi_val = float(roi)
        except:
            continue

        # ROI color scale
        if roi_val >= 100:
            color = {"red": 0.0, "green": 0.7, "blue": 0.0}  # Green
        elif roi_val >= 20:
            color = {"red": 0.9, "green": 0.9, "blue": 0.0}  # Yellow
        elif roi_val <= -30:
            color = {"red": 1.0, "green": 0.4, "blue": 0.4}  # Red
        else:
            color = {"red": 1.0, "green": 1.0, "blue": 1.0}  # White (no highlight)

        requests.append({
            "updateCells": {
                "range": {
                    "sheetId": ws._properties["sheetId"],
                    "startRowIndex": i - 1,
                    "endRowIndex": i,
                    "startColumnIndex": roi_col - 1,
                    "endColumnIndex": roi_col
                },
                "rows": [{
                    "values": [{
                        "userEnteredFormat": {
                            "backgroundColor": color
                        }
                    }]
                }],
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

    if requests:
        service = build("sheets", "v4", credentials=creds)
        body = {"requests": requests}
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=ws.spreadsheet.id,
                body=body
            ).execute()
            print(f"✅ Heatmap applied to {len(requests)} row(s)")
        except HttpError as e:
            print(f"❌ Sheets API error: {e}")
    else:
        print("⚠️ No valid ROI rows to color")
