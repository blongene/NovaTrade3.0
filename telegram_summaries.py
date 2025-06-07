import os
import gspread
import requests
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

def run_telegram_summaries():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    stats_ws = sheet.worksheet("Rotation_Stats")
    rows = stats_ws.get_all_records()

    roi_data = []
    for row in rows:
        token = row.get("Token", "")
        roi = row.get("Follow-up ROI", "")
        try:
            roi_val = float(roi)
            roi_data.append((token, roi_val))
        except:
            continue

    top = sorted(roi_data, key=lambda x: x[1], reverse=True)[:3]
    summary_lines = [f"{t}: +{r:.2f}x" for t, r in top]
    message = "📈 Top ROI Tokens:\n" + "\n".join(summary_lines)

    TELEGRAM_TOKEN = os.getenv("BOT_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"❌ Telegram summary send failed: {e}")
