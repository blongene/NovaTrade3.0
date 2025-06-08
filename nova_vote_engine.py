# nova_vote_engine.py

import os
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Bot

# === Config ===
MIN_SCORE = 85
MIN_SENTIMENT = 0.4
LOOKBACK_DAYS = 3
MAX_AUTOVOTES_PER_RUN = 3

# === Setup ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
gclient = gspread.authorize(creds)
sheet = gclient.open_by_url(os.getenv("SHEET_URL"))
scout_ws = sheet.worksheet("Scout Decisions")
data = scout_ws.get_all_records()

bot = Bot(token=os.getenv("PINGBOT_TOKEN"))
chat_id = os.getenv("TELEGRAM_CHAT_ID")

# === Logic ===
autovotes = 0
now = datetime.utcnow()
cutoff = now - timedelta(days=LOOKBACK_DAYS)

for i, row in enumerate(data):
    if autovotes >= MAX_AUTOVOTES_PER_RUN:
        break

    if row.get("Decision") or not row.get("Timestamp"):
        continue

    try:
        timestamp = datetime.strptime(row["Timestamp"], "%Y-%m-%d %H:%M:%S")
    except:
        continue

    if timestamp < cutoff:
        continue

    try:
        score = float(row.get("Score", 0))
        sentiment = float(row.get("Sentiment", 0))
    except:
        continue

    if score >= MIN_SCORE and sentiment >= MIN_SENTIMENT:
        token = row["Token"]
        symbol = token.upper()
        scout_url = row.get("Scout URL", "")

        scout_ws.update_cell(i + 2, data[0].keys().index("Decision") + 1, "YES")
        scout_ws.update_cell(i + 2, data[0].keys().index("Source") + 1, "NovaVote")
        scout_ws.update_cell(i + 2, data[0].keys().index("Symbol") + 1, symbol)

        msg = f"**🧠 NovaVote Trigger**\nNova auto-voted YES on **${symbol}**\nReason: High Score ({score}), Sentiment ({sentiment})\n✅ Logged to Scout Decisions"
        if scout_url:
            msg += f"\n🔍 [Scout Link]({scout_url})"

        bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
        autovotes += 1

print(f"✅ NovaVote complete. {autovotes} tokens auto-voted.")
