import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

def get_sheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds).open_by_url("https://docs.google.com/spreadsheets/d/1rE6rbUnCPiL8OgBj6hPWNppOV1uaIl8m41nrv-x1xg/edit")

def log_scout_decision(token, action):
    try:
        sheet = get_sheet()
        ws = sheet.worksheet("Scout Decisions")
        timestamp = datetime.now().isoformat()
        ws.append_row([timestamp, token, action, "Telegram"])
    except Exception as e:
        ping_webhook_debug(f"❌ Decision log error: {e}")

def ping_webhook_debug(msg):
    try:
        sheet = get_sheet()
        sheet.worksheet("Webhook_Debug").update_acell("A1", f"{datetime.now().isoformat()} - {msg}")
    except:
        pass
