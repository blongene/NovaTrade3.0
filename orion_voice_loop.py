# orion_voice_loop.py
import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests

def check_nova_trigger_and_ping():
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "sentiment-log-service.json", scope
        )
        client = gspread.authorize(creds)

        sheet_url = os.environ["SHEET_URL"]
        sheet = client.open_by_url(sheet_url)
        trigger_tab = sheet.worksheet("NovaTrigger")

        value = trigger_tab.acell("A1").value.strip().upper()

        if value and value != "READY":
            message = build_message_from_trigger(value)
            send_telegram_message(message)

            # Reset trigger after firing
            trigger_tab.update_acell("A1", "READY")

            print(f"🔔 Orion voice triggered: {value}")

    except Exception as e:
        print(f"❌ Error in check_nova_trigger_and_ping: {e}")


def build_message_from_trigger(trigger_value):
    trigger_value = trigger_value.upper()

    if trigger_value == "ROTATION COMPLETE":
        return "📥 *Rotation Execution Confirmed*\n\n✅ New token(s) added to active tracking.\nROI monitoring has begun.\nLoop closed. Orion is watching."
    elif trigger_value == "NOVA UPDATE":
        return "📡 *NovaTrade System Online*\nAll modules are active.\nYou will be notified if input is needed or a token stalls."
    elif trigger_value == "SOS":
        return "🆘 *NovaTrade Alert*\nA system error or webhook failure was detected.\nPlease check the system log immediately."
    elif trigger_value == "SYNC NEEDED":
        return "🧠 *Sync Required*\nNew decisions are pending rotation. Please review the planner tab."
    elif trigger_value == "FYI ONLY":
        return "🔔 *FYI Notification*\nThis is a passive update. No action is required."
    else:
        return f"🔔 *NovaTrade Alert*\nTrigger: `{trigger_value}` received.\nCheck the system dashboard for details."


def send_telegram_message(text):
    token = os.environ["BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }

    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"❌ Telegram message failed: {e}")


def run_orion_voice_loop():
    while True:
        check_nova_trigger_and_ping()
        time.sleep(60)  # Check every 60 seconds
