from flask import Flask, request
import os, requests, re
from utils import log_scout_decision, ping_webhook_debug, log_rebuy_decision
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from rebalance_scanner import run_rebalance_scanner
from portfolio_weight_adjuster import run_portfolio_weight_adjuster

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
            msg_text = callback['message']['text']

            # ✅ Phase 14B: Handle Rebuy Confirmations
            if "re-enter" in msg_text.lower() or "rebuy signal" in msg_text.lower():
                token_match = re.search(r"\$(\w+)", msg_text)
                if token_match:
                    token = token_match.group(1)
                    log_rebuy_decision(token)

                    ack_url = f"https://api.telegram.org/bot{os.environ['BOT_TOKEN']}/answerCallbackQuery"
                    confirm_payload = {
                        "callback_query_id": callback_id,
                        "text": f"✅ Rebuy for ${token} logged.",
                        "show_alert": False
                    }
                    requests.post(ack_url, json=confirm_payload)
                    print(f"✅ Rebuy decision logged for {token}")
                    return 'OK', 200

            # Handle REYES/RENO (rotation feedback)
            if payload.startswith("REYES") or payload.startswith("RENO"):
                parts = payload.split("|")
                if len(parts) == 4:
                    decision = "YES" if parts[0] == "REYES" else "NO"
                    token = parts[1].strip().upper()
                    days = int(parts[2])
                    roi = parts[3].strip()
                    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

                    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
                    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
                    client = gspread.authorize(creds)
                    sheet = client.open_by_url(os.getenv("SHEET_URL"))

                    review_ws = sheet.worksheet("ROI_Review_Log")
                    for i, row in enumerate(review_ws.get_all_records(), start=2):
                        if row.get("Token", "").strip().upper() == token and int(row.get("Days Held", 0)) == days:
                            review_ws.update_cell(i, 9, decision)
                            break

                    stats_ws = sheet.worksheet("Rotation_Stats")
                    for i, row in enumerate(stats_ws.get_all_records(), start=2):
                        if row.get("Token", "").strip().upper() == token and int(row.get("Days Held", 0)) == days:
                            stats_ws.update_cell(i, 10, decision)
                            break

                    ack_url = f"https://api.telegram.org/bot{os.environ['BOT_TOKEN']}/answerCallbackQuery"
                    confirm_payload = {
                        "callback_query_id": callback_id,
                        "text": f"📝 Feedback recorded: {decision} for {token} @ {days}d.",
                        "show_alert": False
                    }
                    requests.post(ack_url, json=confirm_payload)
                    print(f"✅ Logged re-vote for {token} — {decision}")
                    return 'OK', 200

            # Standard scout YES/NO buttons
            elif "|" in payload:
                action, token = payload.split("|")
                log_scout_decision(token, action)

                try:
                    ack_url = f"https://api.telegram.org/bot{os.environ['BOT_TOKEN']}/answerCallbackQuery"
                    confirm_payload = {
                        "callback_query_id": callback_id,
                        "text": "📝 Response logged. ROI tracking enabled.",
                        "show_alert": False
                    }
                    requests.post(ack_url, json=confirm_payload)
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

            elif text == "/REBALANCE":
                run_rebalance_scanner()
                print("✅ Manual rebalance scan triggered via Telegram")

            elif text == "/REWEIGHT":
                run_portfolio_weight_adjuster()
                print("✅ Portfolio weights adjusted via Telegram")

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
