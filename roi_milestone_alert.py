import gspread
import os
import requests
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

# Setup auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("token_vault.json", scope)
sheet = gspread.authorize(creds).open_by_url(os.getenv("SHEET_URL"))

TELEGRAM_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("CHAT_ID")

def send_milestone_alert(token, milestone, roi):
    message = (
        f"📈 *{milestone} ROI Milestone Hit: {token}*\n"
        f"- ROI: {roi}x\n\n"
        f"Would you make the same decision again? (YES / NO)"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    requests.post(url, data=data)

def scan_roi_tracking():
    ws = sheet.worksheet("ROI_Tracking")
    log_ws = sheet.worksheet("ROI_Review_Log")
    rows = ws.get_all_records()
    now = datetime.utcnow().isoformat()

    for row in rows:
        token = row.get("Token", "").strip()
        if not token:
            continue
        for milestone in ["7d ROI", "14d ROI", "30d ROI"]:
            status_col = f"{milestone} Alerted"
            roi_value = row.get(milestone)
            if row.get(status_col, "").strip().upper() != "YES" and roi_value:
                try:
                    roi = float(roi_value)
                    send_milestone_alert(token, milestone.replace(" ROI", ""), roi)
                    log_ws.append_row([now, token, milestone, roi, "Ping Sent"])
                    cell = ws.find(token)
                    ws.update_cell(cell.row, ws.find(status_col).col, "YES")
                except ValueError:
                    print(f"⚠️ Skipping {token} - non-numeric ROI: {roi_value}")

if __name__ == "__main__":
    print("📈 Checking for ROI milestone follow-ups...")
    scan_roi_tracking()
