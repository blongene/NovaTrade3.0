import os
import gspread
import requests
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

# ===== CONFIG TOGGLES =====
ENABLE_REDDIT = False  # Currently inactive, placeholder for future
ENABLE_YOUTUBE = True
ENABLE_TWITTER = True  # Toggle Twitter scraping

# ===== SETUP GOOGLE SHEETS CLIENT =====
def get_worksheet(name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    return sheet.worksheet(name)

# ===== TWITTER SCRAPER =====
def fetch_twitter_mentions(token):
    try:
        bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
        if not bearer_token:
            print("⚠️ Twitter token missing. Skipping Twitter scan.")
            return 0

        headers = {"Authorization": f"Bearer {bearer_token}"}
        query = f"{token} -is:retweet lang:en"
        url = f"https://api.twitter.com/2/tweets/search/recent?query={query}&max_results=10"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"⚠️ Twitter API error: {response.status_code} - {response.text}")
            return 0

        data = response.json()
        return len(data.get("data", []))

    except Exception as e:
        print(f"⚠️ Twitter error for '{token}': {e}")
        return 0

# ===== YOUTUBE SCRAPER =====
def fetch_youtube_mentions(token):
    try:
        youtube = build("youtube", "v3", developerKey=os.getenv("YOUTUBE_API_KEY"))
        request = youtube.search().list(
            q=token,
            part="snippet",
            maxResults=3,
            type="video"
        )
        response = request.execute()
        return len(response.get("items", []))
    except Exception as e:
        print(f"⚠️ YouTube error for '{token}': {e}")
        return 0

# ===== MAIN RADAR ENGINE =====
def run_sentiment_radar():
    print("📡 Running Sentiment Radar...")
    radar_ws = get_worksheet("Sentiment_Radar")
    targets_ws = get_worksheet("Sentiment_Targets")
    targets = targets_ws.get_all_records()
    token_list = sorted(targets, key=lambda x: x.get("Priority", 0), reverse=True)[:3]

    sentiment_entries = []

    for row in token_list:
        token = row.get("Token", "").strip()
        if not token:
            continue

        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # YouTube
        if ENABLE_YOUTUBE:
            yt_mentions = fetch_youtube_mentions(token)
            print(f"📺 YouTube: {token} → {yt_mentions} mentions")
            sentiment_entries.append([timestamp, token, "YouTube", yt_mentions])

        # Twitter
        if ENABLE_TWITTER:
            tw_mentions = fetch_twitter_mentions(token)
            print(f"🐦 Twitter: {token} → {tw_mentions} mentions")
            sentiment_entries.append([timestamp, token, "Twitter", tw_mentions])

        # Reddit stub (future support)
        if ENABLE_REDDIT:
            sentiment_entries.append([timestamp, token, "Reddit", 0])

    if sentiment_entries:
        radar_ws.append_rows(sentiment_entries, value_input_option="USER_ENTERED")
        print(f"✅ Sentiment Radar logged {len(sentiment_entries)} mentions.")
    else:
        print("⚠️ No sentiment entries written.")
