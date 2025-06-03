from flask import Flask, request
import os, requests
from utils import log_scout_decision, ping_webhook_debug
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

telegram_app = Flask(__name__)
PROMPT_MEMORY = {}

@telegram_app.route('/', methods=['POST'])
def webhook():
    try:
        data = request.get_json()

        # Handle inline button responses
        if 'callback_query' in data:
            callback = data['callback_query']
            payload = callback['data']
            callback_id = callback['id']
            chat_id = callback['message']['chat']['id']

            # Handle REYES/RENO (rotation feedback)
            if payload.startswith("REYES") or payload.startswith("RENO"):
                parts = payload.split("|")
                decision = "YES" if parts[0] == "REYES" else "NO"
                token = parts[1]
                days = parts[2]
                roi = parts[3]
                timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

                # Log to ROI_Review_Log
                scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
                creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
                client = gspread.authorize(creds)
                sheet = client.open_by_url(os.getenv("SHEET_URL"))
                review_ws = sheet.worksheet("ROI_Review_Log")

                review_ws.append_row(
                    [timestamp, token, days, "", roi, decision, ""],
                    value_input_option="USER_ENTERED"
                )

                # Acknowledge re-vote
                bot_token = os.environ["BOT_TOKEN"]
                ack_url = f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery"
                confirm_payload = {
                    "callback_query_id": callback_id,
                    "text": f"📝 Feedback logged: {decision} for {token}.",
                    "show_alert": False
                }
                requests.post(ack_url, json=confirm_payload)
                print(f"✅ Logged re-vote for {token} — {decision}")

            # Handle YES/NO/ROTATE original votes
            elif "|" in payload:
                action, token = payload.split("|")
                log_scout_decision(token, action)

                try:
                    bot_token = os.environ["BOT_TOKEN"]
                    url = f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery"
                    confirm_payload = {
                        "callback_query_id": callback_id,
                        "text": "📝 Response logged. ROI tracking enabled.",
                        "show_alert": False
                    }
                    requests.post(url, json=confirm_payload)
                    print(f"✅ Acknowledged Telegram response for {token}")
                except Exception as e:
                    print(f"⚠️ Failed to send Telegram acknowledgment: {e}")

        # Handle manual messages
        elif 'message' in data:
            message = data['message']
            user_id = str(message['chat']['id'])
            text = message.get('text', '').strip().upper()

            if text.startswith("/ROTATE"):
                _, token, action = text.split()
                PROMPT_MEMORY[user_id] = {"token": token, "action": action}

            elif text in ["YES", "NO", "SKIP"] and user_id in PROMPT_MEMORY:
                token = PROMPT_MEMORY[user_id]["token"]
                log_scout_decision(token, text)

        return 'OK', 200

    except Exception as e:
        ping_webhook_debug(f"❌ Webhook error: {e}")
        return 'FAIL', 500

def set_telegram_webhook():
    token = os.environ.get("BOT_TOKEN")
    url = os.environ.get("WEBHOOK_URL")
    if token and url:
        try:
            r = requests.get(f"https://api.telegram.org/bot{token}/setWebhook?url={url}")
            print(f"✅ Webhook set: {r.text}")
        except Exception as e:
            print(f"❌ Failed to set webhook: {e}")
    else:
        print("❌ BOT_TOKEN or WEBHOOK_URL not found in environment")
