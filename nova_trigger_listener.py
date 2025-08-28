import os
import time
import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_message_dedup, with_sheet_backoff, get_ws, str_or_empty

def listen_for_nova_trigger():
    print("🎯 NovaTrigger listener started...")

    # Setup
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    trigger_ws = sheet.worksheet("NovaTrigger")

    # Constants
    bot_token = os.getenv("BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    # Loop
    while True:
        try:
            raw = trigger_ws.acell("A1").value
            value = str(raw).upper().strip()

            if value != "READY":
                message = get_trigger_message(value)
                if message:
                    send_telegram_message_dedup(bot_token, chat_id, message, key="nova_trigger", ttl_min=10))
                    print(f"✅ NovaTrigger sent: {value}")
                    trigger_ws.update_acell("A1", "READY")
                else:
                    print(f"⚠️ Unknown NovaTrigger value: {value}")
        except Exception as e:
            print(f"❌ NovaTrigger error: {e}")
        time.sleep(60)

def get_trigger_message(trigger_type):
    messages = {
        "SOS": "🚨 *NovaTrade SOS*\nThis is a test alert to confirm outbound messaging is working.",
        "FYI ONLY": "📘 *NovaTrade FYI*\nNon-urgent update: system status or data refreshed.",
        "SYNC NEEDED": "🧩 *NovaTrade Sync Needed*\nPlease review the latest responses or re-run the sync loop.",
        "NOVA UPDATE": "🧠 *NovaTrade Intelligence*\nA logic update or system improvement has been deployed.",
        "ROTATION COMPLETE": "🔄 *Rotation Complete*\nToken rotation executed. Review updated vault.",
        "PRESALE ALERT": "🚀 *Presale Alert*\nA new token opportunity has been identified. Review now.",
    }
    return messages.get(trigger_type)

def send_telegram_message_dedup(token, chat_id, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    response = requests.post(url, json=payload)
    return response.ok
