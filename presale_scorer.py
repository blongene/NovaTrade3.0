# presale_scorer.py
import os, json, requests, time
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import ping_webhook_debug, with_sheet_backoff

# === CONFIGURATION ===
SHEET_NAME = "Presale_Stream"
ALERT_THRESHOLD = 85
TELEGRAM_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SHEET_URL = os.getenv("SHEET_URL")

HYPE_KEYWORDS = [
    "utility","ai","real","staking","tokenomics","launchpad",
    "audit","deflationary","tool","platform"
]

# === AUTH HELPERS ===
def _gs_client():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "sentiment-log-service.json", scope
    )
    return gspread.authorize(creds)

@with_sheet_backoff
def _open_ws(url, tab):
    client = _gs_client()
    sh = client.open_by_url(url)
    return sh.worksheet(tab)

# === SCORING LOGIC ===
def score_token(row):
    sentiment_raw = row[3].lower() if len(row) > 3 and row[3] else ""
    market_cap = row[4].lower() if len(row) > 4 and row[4] else "unknown"
    launch_date = row[5] if len(row) > 5 else ""
    description = row[6].lower() if len(row) > 6 and row[6] else ""
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

    if "micro" in market_cap: m_pts = 20
    elif "nano" in market_cap: m_pts = 15
    elif "mid" in market_cap: m_pts = 10
    else: m_pts = 5

    try:
        days_to_launch = (datetime.strptime(launch_date, "%Y-%m-%d") - datetime.utcnow()).days
        f_pts = 20 if days_to_launch <= 3 else max(0, 15 - days_to_launch)
    except:
        f_pts = 10

    match_count = sum(1 for kw in HYPE_KEYWORDS if kw in description)
    k_pts = min(match_count * 4, 20)

    return s_pts + m_pts + f_pts + k_pts

# === HELPERS ===
def already_sent(client, token):
    try:
        ws = client.open_by_url(SHEET_URL).worksheet("Scout Decisions")
        existing = ws.col_values(2)
        return token.upper() in [t.upper() for t in existing]
    except Exception as e:
        print(f"⚠️ Could not access Scout Decisions: {e}")
        return False

def mark_sent(ws, row_num):
    try:
        ws.update_cell(row_num + 1, 8, "SENT")
    except Exception as e:
        print(f"⚠️ Failed to mark SENT: {e}")

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
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, json=payload, timeout=10)
        print(f"📬 Telegram response: {r.status_code}, {r.text}")
    except Exception as e:
        print(f"❌ Telegram send error: {e}")
        ping_webhook_debug(f"❌ Telegram send error: {e}")

# === MAIN ENGINE ===
def run_presale_scorer():
    print("💥 run_presale_scorer() BOOTED")
    try:
        ws = _open_ws(SHEET_URL, SHEET_NAME)
    except Exception as e:
        print(f"🚫 No worksheet loaded — {e}")
        return

    try:
        data = ws.get_all_values()
        print(f"📦 Raw worksheet data length: {len(data)}")
        if not data or len(data) < 2:
            print("⛔️ No presale rows found")
            return
        headers, rows = data[0], data[1:]
        print(f"📋 Found {len(rows)} presale rows")

        client = _gs_client()

        for i, row in enumerate(rows):
            if len(row) < 7:
                print(f"⛔️ Row {i+2} skipped: too short")
                continue

            token = row[0].strip().upper()
            status = row[8].strip().upper() if len(row) > 8 else ""

            print(f"🔎 Evaluating {token} — Status: {status}")

            if status != "PENDING":
                print(f"⏭️ Skipping {token}: not PENDING")
                continue

            if already_sent(client, token):
                print(f"🟡 Already seen in Scout Decisions: {token}")
                mark_sent(ws, i)
                continue

            try:
                from memory_score_booster import get_memory_boost
                score = score_token(row)
                boost = get_memory_boost(token)
                score += boost
                print(f"📈 Final score for {token} after memory = {score}/100")

                if score >= ALERT_THRESHOLD:
                    print(f"🚀 {token} passed threshold — sending alert...")
                    description = row[6] if len(row) > 6 else "No description"
                    send_presale_alert(token, int(score), description)
                    mark_sent(ws, i)
                else:
                    print(f"❌ {token} below threshold — not alerting")
            except Exception as e:
                print(f"❌ ERROR scoring {token}: {e}")
    except Exception as fatal:
        print(f"💥 FATAL ERROR in presale_scorer: {fatal}")
