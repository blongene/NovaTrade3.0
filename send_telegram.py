import os
import requests


def send_message(message="📡 Orion Heartbeat\nSystem is live."):
    token = os.environ["BOT_TOKEN"]
    chat_id = os.environ["CHAT_ID"]

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}

    response = requests.post(url, json=payload)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")


def send_rotation_alert(token_name, message):
    token = os.environ["BOT_TOKEN"]
    chat_id = os.environ["CHAT_ID"]

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message + "\n\nWhat's your move?",
        "reply_markup": {
            "inline_keyboard": [[{
                "text": "🔁 Rotate",
                "callback_data": f"ROTATE|{token_name}"
            }, {
                "text": "⏳ Hold",
                "callback_data": f"HOLD|{token_name}"
            }, {
                "text": "🪫 Ignore",
                "callback_data": f"IGNORE|{token_name}"
            }]]
        }
    }

    response = requests.post(url, json=payload)
    print(f"📨 Rotation Alert Sent: {token_name}")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
