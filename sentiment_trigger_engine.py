import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_prompt

# === CONFIG ===
SENTIMENT_THRESHOLD = 10  # minimum recent mentions to trigger
SCORE_THRESHOLD = 2       # minimum memory score to be considered
SHEET_URL = os.getenv("SHEET_URL")

# === SHEET SETUP ===
def get_ws(sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(SHEET_URL)
    return sheet.worksheet(sheet_name)

# === MAIN ===
def run_sentiment_trigger_engine():
    print("🧠 Running Sentiment-Triggered Rebuy Engine...")

    try:
        radar_ws = get_ws("Sentiment_Summary")
        memory_ws = get_ws("Rotation_Stats")
        planner_ws = get_ws("Rotation_Planner")

        summary = radar_ws.get_all_records()[-30:]  # recent entries only
        stats = memory_ws.get_all_records()
        planner = planner_ws.get_all_records()

        for row in summary:
            token = row.get("Token", "").strip().upper()
            total_mentions = int(row.get("Total", 0))
            signal_score = float(row.get("Signal Score", 0))

            if total_mentions < SENTIMENT_THRESHOLD:
                continue

            match = next((r for r in stats if r.get("Token", "").strip().upper() == token), {})
            score = int(match.get("Memory Score", 0))
            tag = match.get("Memory Tag", "")

            if score < SCORE_THRESHOLD or tag == "":
                continue

            already_in_planner = any(p.get("Token", "").strip().upper() == token and p.get("Confirmed", "") == "YES" for p in planner)
            if already_in_planner:
                continue

            # Trigger alert
            message = (
                f"$$ {token} previously scored as {tag} with memory score {score},\n"
                f"but has just spiked to {total_mentions} mentions with signal {signal_score}.\n\n"
                f"Would you like to *rebuy* this token based on renewed interest?"
            )
            send_telegram_prompt(token, message, prefix="REBUY SUGGESTION")
            print(f"📢 Rebuy suggestion sent for {token}")

    except Exception as e:
        print(f"❌ Error in run_sentiment_trigger_engine: {e}")
