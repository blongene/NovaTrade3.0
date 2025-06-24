from flask import Flask, request
import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import (
    send_telegram_message,
    log_scout_decision,
    log_rotation_confirmation,
    log_roi_feedback,
    log_vault_review,
    log_rebuy_confirmation,
    ping_webhook_debug
)

telegram_app = Flask(__name__)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# Load Sheet connection
def get_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    return sheet

def parse_button_response(data):
    try:
        callback = data["callback_query"]
        print("📬 Raw callback received:")
        print(json.dumps(callback, indent=2))  # NEW DEBUG LINE

        raw_data = callback["data"].strip()
        user_response, token = map(lambda x: x.strip().upper(), raw_data.split("|", 1))
        message_text = callback["message"]["text"]

        print(f"📨 Telegram reply parsed: {token} → {user_response}")

        if "UNVAULT" in user_response or "VAULT CHECK" in message_text or "VAULT REVIEW" in message_text:
            log_vault_review(token, user_response)
        elif "REBUY" in message_text:
            log_rebuy_confirmation(token, user_response)
        elif "ROI" in message_text:
            log_roi_feedback(token, user_response)
        elif "CONFIRM" in user_response or "ROTATE" in user_response:
            log_rotation_confirmation(token, user_response)
        elif "CLAIMED ACTION" in message_text:
            try:
                clean_response = user_response.replace("📦", "").replace("🔁", "").replace("🔕", "").replace("IT", "").strip().upper()
                log_scout_decision(token, clean_response)
            except Exception as e:
                print(f"❌ Failed to log CLAIMED ACTION response: {e}")
                ping_webhook_debug(f"❌ CLAIMED ACTION error: {e}")
        else:
            log_scout_decision(token, user_response)

        return True
    except Exception as e:
        print(f"❌ Error parsing Telegram button response: {e}")
        ping_webhook_debug(f"❌ Telegram parse error: {e}")
        return False

@telegram_app.route("/", methods=["POST"])
def handle_telegram():
    try:
        data = request.get_json()
        if not data:
            return "No data", 400

        if "callback_query" in data:
            parse_button_response(data)
            return "Callback processed", 200

        if "message" in data:
            msg = data["message"]
            text = msg.get("text", "").strip()
            chat_id = msg["chat"]["id"]

            if text.lower() in ["/start", "hi", "hello"]:
                send_telegram_message("👋 NovaTrade is active and listening.", chat_id=chat_id)

        return "OK", 200
    except Exception as e:
        print(f"❌ Telegram webhook error: {e}")
        ping_webhook_debug(f"❌ Telegram webhook failure: {e}")
        return "Error", 500

def set_telegram_webhook():
    import os
    import requests

    bot_token = os.getenv("BOT_TOKEN")
    webhook_url = os.getenv("RENDER_WEBHOOK_URL")  # Should be in your Render env vars

    if not bot_token or not webhook_url:
        print("❌ BOT_TOKEN or RENDER_WEBHOOK_URL missing.")
        return

    url = f"https://api.telegram.org/bot{bot_token}/setWebhook"
    response = requests.post(url, data={"url": webhook_url})
    if response.ok:
        print("✅ Webhook set successfully.")
    else:
        print(f"❌ Failed to set webhook: {response.text}")
