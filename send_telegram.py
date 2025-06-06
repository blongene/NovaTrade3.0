import os
import requests

def send_message(message="📡 Orion Heartbeat\nSystem is live."):
    try:
        token = os.environ["BOT_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHAT_ID"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message}
        response = requests.post(url, json=payload)
        print(f"✅ Telegram ping sent. Code: {response.status_code}")
    except Exception as e:
        print(f"❌ Failed to send Telegram message: {e}")

def send_rotation_alert(token_name, message):
    try:
        token = os.environ["BOT_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHAT_ID"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message + "\n\nWhat's your move?",
            "reply_markup": {
                "inline_keyboard": [[
                    {"text": "🔁 Rotate", "callback_data": f"ROTATE|{token_name}"},
                    {"text": "⏳ Hold", "callback_data": f"HOLD|{token_name}"},
                    {"text": "🪫 Ignore", "callback_data": f"IGNORE|{token_name}"}
                ]]
            }
        }
        response = requests.post(url, json=payload)
        print(f"📨 Rebalance Alert Sent for {token_name} – {response.status_code}")
    except Exception as e:
        print(f"❌ Failed to send rebalance alert for {token_name}: {e}")
