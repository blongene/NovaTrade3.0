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

# === KEYWORDS TO SCORE ===
HYPE_KEYWORDS = ["utility", "ai", "real", "staking", "tokenomics", "launchpad", "audit", "deflationary", "tool", "platform"]

def score_token(row):
    sentiment_raw = row[3].lower() if row[3] else ""
    market_cap = row[4].lower() if row[4] else "unknown"
    launch_date = row[5]
    description = row[6].lower() if row[6] else ""
    token = row[0]

    # Sentiment Score (0–40 pts)
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

    # Market Cap Score (0–20 pts)
    if "micro" in market_cap:
        m_pts = 20
    elif "nano" in market_cap:
        m_pts = 15
    elif "mid" in market_cap:
        m_pts = 10
    else:
        m_pts = 5

    # Freshness Score (0–20 pts)
    try:
        days_to_launch = (datetime.strptime(launch_date, "%Y-%m-%d") - datetime.utcnow()).days
        f_pts = 20 if days_to_launch <= 3 else max(0, 15 - days_to_launch)
    except:
        f_pts = 10

    # Keyword Hype Score (0–20 pts)
    match_count = sum(1 for kw in HYPE_KEYWORDS if kw in description)
    k_pts = min(match_count * 4, 20)

    total_score = s_pts + m_pts + f_pts + k_pts
    return total_score

def already_sent(token):
    existing = sheet.worksheet("Scout Decisions").col_values(2)
    return token.upper() in [t.upper() for t in existing]

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
        requests.post(url, json=payload)
        print(f"📡 Sent alert for {token} ({score})")
    except Exception as e:
        ping_webhook_debug(f"❌ Telegram send error: {e}")
        print(f"❌ Telegram send failed: {e}")

def run_presale_scorer():
    print("📊 Checking Presale_Stream...")
    data = worksheet.get_all_values()
    headers = data[0]
    rows = data[1:]
    print(f"📋 Found {len(rows)} presale rows to evaluate")

    for i, row in enumerate(rows):
        if len(row) < 7:
            print(f"⛔️ Row {i+2} skipped: not enough columns")
            continue

        status = row[7].strip().upper() if len(row) > 7 else ""
        token = row[0].strip().upper()

        print(f"🔎 Evaluating {token} (Status: {status})")

        if status != "PENDING":
            print(f"➡️ Skipping {token} — not pending")
            continue
        if already_sent(token):
            print(f"🟡 Skipping {token} — already logged in Scout Decisions")
            mark_sent(i)
            continue

        score = score_token(row)
        print(f"📈 {token} scored {score}/100")

        if score >= ALERT_THRESHOLD:
            description = row[6] if len(row) > 6 else "No description"
            print(f"🚀 ALERT: {token} passed threshold — sending ping...")
            send_presale_alert(token, int(score), description)
            mark_sent(i)
        else:
            print(f"❌ BELOW THRESHOLD: {token} only scored {score}")

# Only runs when executed directly (not on import)
if __name__ == "__main__":
    print("🔍 Starting presale scoring loop...")
    run_presale_scorer()
