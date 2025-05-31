
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os

def trigger_nova_ping(message_type="SOS"):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)

    sheet_url = os.getenv("SHEET_URL")
    if not sheet_url:
        print("❌ SHEET_URL not found in environment variables.")
        return

    sheet = client.open_by_url(sheet_url)
    try:
        nova_ws = sheet.worksheet("NovaTrigger")
    except:
        print("❌ NovaTrigger sheet not found.")
        return

    valid_values = ["SOS", "FYI ONLY", "SYNC NEEDED", "NOVA UPDATE", "PRESALE ALERT", "ROTATION COMPLETE"]
    if message_type not in valid_values:
        print(f"⚠️ Invalid message_type: {message_type}")
        return

    nova_ws.update("A1", message_type)
    print(f"✅ NovaTrigger set to '{message_type}' at {datetime.utcnow()}")

