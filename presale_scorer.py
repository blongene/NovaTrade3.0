import os
import json
import requests
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import ping_webhook_debug

# === CONFIGURATION ===
SHEET_NAME = "Presale_Stream"
ALERT_THRESHOLD = 85
TELEGRAM_TOKEN = os.environ.get("BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
SHEET_URL = os.environ.get("SHEET_URL")

# === AUTH ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_url(SHEET_URL)
worksheet = sheet.worksheet(SHEET_NAME)

HYPE_KEYWORDS = ["utility", "ai", "real", "staking", "tokenomics", "launchpad", "audit", "deflationary", "tool", "platform"]

def score_token(row):
    sentiment_raw = row[3].lower() if row[3] else ""
    market_cap = row[4].lower() if row[4] else "unknown"
    launch_date = row[5]
    description = row[6].lower() if row[6] else ""
    token = row[0]

    if "skyrocket" in sentiment_raw or "🚀" in sentiment_raw:
        s_pts = 40
    elif "high" in sentiment_raw or "🔥" in sentiment_raw:
        s_pts = 30
    elif "moderate" in sentiment_raw or "👍" in sentiment_raw:
        s_pts = 20
    elif "low" in sentiment_raw or "😐" in sentiment_raw:
        s_pts = 10
    else:
        s_pts = 0

    if "micro" in market_cap:
        m_pts = 20
    elif "nano" in market_cap:
        m_pts = 15
    elif "mid" in market_cap:
        m_pts = 10
    else:
        m_pts = 5

    try:
        days_to_launch = (datetime.strptime(launch_date, "%Y-%m-%d") - datetime.utcnow()).days
        f_pts = 20 if days_to_launch <= 3 else max(0, 15 - days_to_launch)
    except:
        f_pts = 10

    match_count = sum(1 for kw in HYPE_KEYWORDS if kw in description)
    k_pts = min(match_count * 4, 20)

    total_score = s_pts + m_pts + f_pts + k_pts
    return total_score

def already_sent(token):
    try:
        existing = sheet.worksheet("Scout Decisions").col_values(2)
        return token.upper() in [t.upper() for t in existing]
    except Exception as e:
        print(f"⚠️ Could not check Scout Decisions: {e}")
        return False

def mark_sent(row_num):
    worksheet.update_cell(row_num + 1, 8, "SENT")  # Column H = Status

def send_presale_alert(token, score, description):
    text = f"""💡 *New Presale Scouted!*

Token: *${token}*
Score: *{score}/100*

_{description}_

🔥 Action?
"""
    keyboard = {
        "inline_keyboard": [[
            {"text": "✅ YES", "callback_data": f"YES|{token}"},
            {"text": "❌ NO", "callback_data": f"NO|{token}"},
            {"text": "🤔 SKIP", "callback_data": f"SKIP|{token}"}
        ]]
    }
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "reply_markup": json.dumps(keyboard)
    }
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        res = requests.post(url, json=payload)
        print(f"📬 Telegram response: {res.status_code}, {res.text}")
    except Exception as e:
        ping_webhook_debug(f"❌ Telegram send error: {e}")
        print(f"❌ Telegram send failed: {e}")

def run_presale_scorer():
    print("📊 Checking Presale_Stream for PENDING tokens...")
    try:
        data = worksheet.get_all_values()
        if not data or len(data) < 2:
            print("⛔️ No data or only headers found in Presale_Stream")
            return
        headers = data[0]
        rows = data[1:]
        print(f"📋 Found {len(rows)} presale rows")

        for i, row in enumerate(rows):
            if len(row) < 7:
                print(f"⛔️ Row {i+2} skipped: too short")
                continue

            token = row[0].strip().upper()
            status = row[7].strip().upper() if len(row) > 7 else ""

            print(f"🔎 Evaluating {token} — Status: {status}")

            if status != "PENDING":
                print(f"⏭️ Skipping {token}: not PENDING")
                continue

            if already_sent(token):
                print(f"🟡 Already seen in Scout Decisions: {token}")
                mark_sent(i)
                continue

            try:
                score = score_token(row)
                print(f"📈 {token} scored {score}/100")

                if score >= ALERT_THRESHOLD:
                    print(f"🚀 {token} passed threshold — sending alert...")
                    description = row[6] if len(row) > 6 else "No description"
                    send_presale_alert(token, int(score), description)
                    mark_sent(i)
                else:
                    print(f"❌ {token} below threshold — not alerting")
            except Exception as e:
                print(f"❌ ERROR scoring {token}: {e}")
    except Exception as fatal:
        print(f"💥 FATAL ERROR in presale_scorer: {fatal}")

# Call on startup
if __name__ == "__main__":
    print("🔍 Starting presale scoring loop...")
    run_presale_scorer()
