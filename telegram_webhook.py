from flask import Flask, request
import os, requests
from utils import log_scout_decision, ping_webhook_debug

telegram_app = Flask(__name__)
PROMPT_MEMORY = {}

@telegram_app.route('/', methods=['POST'])
def webhook():
    try:
        data = request.get_json()
        if 'callback_query' in data:
            payload = data['callback_query']['data']
            if "|" in payload:
                action, token = payload.split("|")
                log_scout_decision(token, action)
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
