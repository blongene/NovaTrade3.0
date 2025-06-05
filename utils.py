import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

def get_sheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds).open_by_url("https://docs.google.com/spreadsheets/d/1rE6rbUnCPiL8OgBj6hPWNppOV1uaII8im41nrv-x1xg/edit")

def log_scout_decision(token, decision):
    print(f"📥 Logging decision: {decision} for token {token}")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        ws = sheet.worksheet("Scout Decisions")
        planner_ws = sheet.worksheet("Rotation_Planner")

        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        existing = ws.get_all_records()
        new_row = [
            now,
            token.upper(),
            decision.upper(),
            "Telegram"
        ]
        ws.append_row(new_row)
        print("✅ Decision logged to Scout Decisions")

        # Auto-confirm logic for YES votes
        if decision.upper() == "YES":
            planner_data = planner_ws.get_all_values()
            headers = planner_data[0]
            token_idx = headers.index("Token")
            confirm_idx = headers.index("Confirmed")

            for i, row in enumerate(planner_data[1:], start=2):
                if row[token_idx].strip().upper() == token.upper():
                    planner_ws.update_cell(i, confirm_idx + 1, "YES")
                    print(f"✅ Auto-confirmed token {token} in Rotation_Planner")
                    break

    except Exception as e:
        print(f"❌ Failed to log decision for {token}: {e}")
        ping_webhook_debug(f"❌ Log Scout Decision error: {e}")

def ping_webhook_debug(msg):
    try:
        sheet = get_sheet()
        sheet.worksheet("Webhook_Debug").update_acell("A1", f"{datetime.now().isoformat()} - {msg}")
    except:
        pass

def log_rotation_alert(token, milestone):
    try:
        sheet = get_sheet()
        ws = sheet.worksheet("Rotation_Log")
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([now, f"MILESTONE {milestone}", token, "AUTO"])
        print(f"📌 Rotation_Log updated for {token} @ {milestone}d")
    except Exception as e:
        ping_webhook_debug(f"❌ Failed to log milestone for {token}: {e}")

def send_telegram_message(message):
    try:
        bot_token = os.getenv("BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not bot_token or not chat_id:
            raise Exception("Missing BOT_TOKEN or TELEGRAM_CHAT_ID")
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, json=payload)
        if not response.ok:
            raise Exception(response.text)
        return response.json()
    except Exception as e:
        ping_webhook_debug(f"❌ Telegram send error: {e}")

import os
import requests

def send_telegram_prompt(token, message, buttons=["YES", "NO"], prefix="REBALANCE"):
    bot_token = os.environ.get("BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        print("❌ BOT_TOKEN or TELEGRAM_CHAT_ID not found.")
        return

    button_data = [
        [{"text": btn, "callback_data": f"{btn}|{token}"}] for btn in buttons
    ]
    payload = {
        "chat_id": chat_id,
        "text": f"🔁 *{prefix} ALERT*\n\n{message}",
        "parse_mode": "Markdown",
        "reply_markup": {"inline_keyboard": button_data}
    }

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        r = requests.post(url, json=payload)
        if r.ok:
            print(f"✅ Telegram prompt sent for {token}")
        else:
            print(f"⚠️ Telegram error: {r.text}")
    except Exception as e:
        print(f"❌ Telegram prompt failed: {e}")
