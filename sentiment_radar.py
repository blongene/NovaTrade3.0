import os
import gspread
import re
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
# from telegram_notifier import send_alert  # Disabled to prevent module error

# Optional toggle
ENABLE_YOUTUBE = True

def run_sentiment_radar():
    print("📡 Running Sentiment Radar...")

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    radar_ws = sheet.worksheet("Sentiment_Radar")
    targets_ws = sheet.worksheet("Sentiment_Targets")

    targets = targets_ws.get_all_records()
    token_list = sorted(targets, key=lambda x: x.get("Priority", 0), reverse=True)[:3]  # Top 3 tokens

    sentiment_entries = []

    # YouTube setup
    if ENABLE_YOUTUBE:
        try:
            youtube = build("youtube", "v3", developerKey=os.getenv("YOUTUBE_API_KEY"))
        except Exception as e:
            print(f"⚠️ YouTube client failed: {e}")
            youtube = None

    for target in token_list:
        token = target.get("Token", "").strip()
        if not token:
            continue

        # Search YouTube
        if ENABLE_YOUTUBE and youtube:
            try:
                request = youtube.search().list(
                    q=token,
                    part="snippet",
                    maxResults=3,
                    type="video"
                )
                response = request.execute()
                mentions = len(response.get("items", []))
                sentiment_entries.append([datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), token, "YouTube", mentions])
                print(f"📺 YouTube: {token} → {mentions} mentions")
            except Exception as e:
                if "quotaExceeded" in str(e):
                    print(f"⚠️ YouTube quota exceeded for token '{token}'")
                else:
                    print(f"⚠️ YouTube error for '{token}': {e}")
                continue

        # You can expand this to Reddit/Twitter scraping as needed

    if sentiment_entries:
        radar_ws.append_rows(sentiment_entries, value_input_option="USER_ENTERED")
        print(f"✅ Sentiment Radar logged {len(sentiment_entries)} mentions.")
    else:
        print("⚠️ No sentiment entries written.")
