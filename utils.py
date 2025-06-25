import os
import gspread
import requests
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import time
import random
from functools import wraps

def throttle_retry(max_retries=3, delay=2, jitter=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"⚠️ Attempt {attempt+1} failed: {e}")
                    if attempt < max_retries - 1:
                        sleep_time = delay + random.uniform(0, jitter)
                        time.sleep(sleep_time)
                    else:
                        raise e
        return wrapper
    return decorator

def get_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds).open_by_url(os.getenv("SHEET_URL"))

def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds)

def log_scout_decision(token, decision):
    print(f"📥 Logging decision: {decision} for token {token}")
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        ws = sheet.worksheet("Scout Decisions")
        planner_ws = sheet.worksheet("Rotation_Planner")

        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        new_row = [now, token.upper(), decision.upper(), "Telegram"]
        ws.append_row(new_row)
        print("✅ Decision logged to Scout Decisions")

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

def log_rebuy_decision(token):
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        scout_ws = sheet.worksheet("Scout Decisions")
        log_ws = sheet.worksheet("Rotation_Log")
        radar_ws = sheet.worksheet("Sentiment_Radar")

        token = token.strip().upper()
        log_data = log_ws.get_all_records()
        log_row = next((row for row in log_data if row.get("Token", "").strip().upper() == token), {})

        score = log_row.get("Score", "")
        sentiment = log_row.get("Sentiment", "")
        market_cap = log_row.get("Market Cap", "")
        scout_url = log_row.get("Scout URL", "")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if not sentiment:
            radar = next((r for r in radar_ws.get_all_records() if r.get("Token", "").strip().upper() == token), {})
            sentiment = radar.get("Mentions", "")

        new_row = [
            timestamp, token, "YES", "Rebuy", score, sentiment, market_cap, scout_url, ""
        ]
        scout_ws.append_row(new_row)
        print(f"✅ Rebuy for ${token} logged to Scout Decisions.")
    except Exception as e:
        print(f"❌ Failed to log rebuy decision for {token}: {e}")

def ping_webhook_debug(msg):
    try:
        sheet = get_sheet()
        sheet.worksheet("Webhook_Debug").update_acell("A1", f"{datetime.now().isoformat()} - {msg}")
    except:
        pass

def send_telegram_message(message, chat_id=None):
    try:
        bot_token = os.getenv("BOT_TOKEN")
        if not chat_id:
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

def send_telegram_prompt(token, message, buttons=["YES", "NO"], prefix="REBALANCE"):
    bot_token = os.environ.get("BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        print("❌ BOT_TOKEN or TELEGRAM_CHAT_ID not found.")
        return

    button_data = [[{"text": btn, "callback_data": f"{btn}|{token}"}] for btn in buttons]
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

def log_rotation_confirmation(token, decision):
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        planner_ws = sheet.worksheet("Rotation_Planner")

        records = planner_ws.get_all_records()
        for i, row in enumerate(records, start=2):  # Skip header
            if row.get("Token", "").strip().upper() == token.strip().upper():
                planner_ws.update_acell(f"C{i}", decision.upper())  # Column C = 'User Response'
                print(f"✅ Rotation confirmation logged: {token} → {decision}")
                return
        print(f"⚠️ Token not found in Rotation_Planner: {token}")
    except Exception as e:
        print(f"❌ Error in log_rotation_confirmation: {e}")

def log_roi_feedback(token, decision):
    try:
        sheet = get_sheet()
        ws = sheet.worksheet("ROI_Review_Log")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_row = [timestamp, token.upper(), decision.upper()]
        ws.append_row(new_row)
        print(f"✅ ROI Feedback logged: {token} → {decision}")
    except Exception as e:
        print(f"❌ Failed to log ROI Feedback: {e}")
        ping_webhook_debug(f"❌ ROI Feedback log error: {e}")

def log_vault_review(token, decision):
    try:
        sheet = get_sheet()
        ws = sheet.worksheet("Vault_Review_Log")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_row = [timestamp, token.upper(), decision.upper()]
        ws.append_row(new_row)
        print(f"✅ Vault Review logged: {token} → {decision}")
    except Exception as e:
        print(f"❌ Failed to log Vault Review: {e}")
        ping_webhook_debug(f"❌ Vault Review log error: {e}")

def log_token_unlock(token, date):
    try:
        sheet = get_sheet()
        ws = sheet.worksheet("Claim_Tracker")
        rows = ws.get_all_records()
        for i, row in enumerate(rows, start=2):  # Start at row 2
            if row.get("Token", "").strip().upper() == token.strip().upper():
                ws.update_acell(f"H{i}", "Claimed")  # Claimed? column
                ws.update_acell(f"I{i}", "Resolved")  # Status column
                ws.update_acell(f"G{i}", date)  # Arrival Date
                print(f"✅ Unlock logged for {token}")
                return
    except Exception as e:
        print(f"❌ Failed to log unlock for {token}: {e}")

def log_unclaimed_alert(token):
    try:
        sheet = get_sheet()
        ws = sheet.worksheet("Webhook_Debug")
        ws.update_acell("A1", f"{datetime.now().isoformat()} – ⚠️ {token} arrived in wallet but not marked claimed")
    except Exception:
        pass

def log_rebuy_confirmation(token):
    log_rebuy_decision(token)

