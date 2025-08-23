import os
import gspread
import requests
import time
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

# ===== CONFIG TOGGLES =====
ENABLE_REDDIT = False  # Placeholder for future support
ENABLE_TWITTER = True
YOUTUBE_ENABLED = os.getenv("YOUTUBE_ENABLED", "false").lower() in ("true", "1", "yes")
YT_COOLDOWN_MIN = int(os.getenv("YT_COOLDOWN_MIN", "120"))  # cool down after any YT error (minutes)
_last_yt_fail_ts = 0  # epoch seconds

# ===== SHEET SETUP =====
def get_worksheet(name: str):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    return sheet.worksheet(name)

# ===== TWITTER SCRAPER =====
def fetch_twitter_mentions(token: str) -> int:
    try:
        bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
        if not bearer_token:
            print("⚠️ Twitter token missing. Skipping Twitter scan.")
            return 0

        headers = {"Authorization": f"Bearer {bearer_token}"}
        # simple, safe query
        query = f"{token} -is:retweet lang:en"
        url = f"https://api.twitter.com/2/tweets/search/recent?query={query}&max_results=10"
        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code == 429:
            print("⚠️ Twitter API error: 429 - Too Many Requests")
            return 0
        if response.status_code != 200:
            print(f"⚠️ Twitter API error: {response.status_code} - {response.text}")
            return 0

        data = response.json()
        return len(data.get("data", []))
    except Exception as e:
        print(f"⚠️ Twitter error for '{token}': {e}")
        return 0

# ===== YOUTUBE SCRAPER =====
def yt_allowed_now() -> bool:
    if not YOUTUBE_ENABLED:
        return False
    return (time.time() - _last_yt_fail_ts) > (YT_COOLDOWN_MIN * 60)

def fetch_youtube_mentions(token: str) -> int:
    global _last_yt_fail_ts
    if not yt_allowed_now():
        if YOUTUBE_ENABLED:
            print("📺 YouTube on cooldown or disabled.")
        return 0
    try:
        api_key = os.getenv("YOUTUBE_API_KEY")
        if not api_key:
            print("⚠️ YOUTUBE_API_KEY missing; skipping YT scan.")
            return 0
        youtube = build("youtube", "v3", developerKey=api_key)
        request = youtube.search().list(q=token, part="snippet", maxResults=3, type="video")
        response = request.execute()
        return len(response.get("items", []))
    except Exception as e:
        print(f"📺 YouTube error for '{token}', cooling down {YT_COOLDOWN_MIN}m: {e}")
        _last_yt_fail_ts = time.time()
        return 0

# ===== MAIN ENGINE =====
def run_sentiment_radar():
    print("📡 Running Sentiment Radar...")
    radar_ws = get_worksheet("Sentiment_Radar")
    targets_ws = get_worksheet("Sentiment_Targets")

    targets = targets_ws.get_all_records()
    # pick top 3 by 'Priority' (if missing, treat as 0)
    top_tokens = sorted(targets, key=lambda x: x.get("Priority", 0) or 0, reverse=True)[:3]

    sentiment_entries = []
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for row in top_tokens:
        token = (row.get("Token") or "").strip()
        if not token:
            continue

        # YouTube (optional + cooldown)
        if YOUTUBE_ENABLED:
            yt_mentions = fetch_youtube_mentions(token)
            print(f"📺 YouTube: {token} → {yt_mentions} mentions")
            sentiment_entries.append([timestamp, token, "YouTube", yt_mentions])
        else:
            print("📺 YouTube disabled (set YOUTUBE_ENABLED=true to enable).")

        # Twitter
        if ENABLE_TWITTER:
            tw_mentions = fetch_twitter_mentions(token)
            print(f"🐦 Twitter: {token} → {tw_mentions} mentions")
            sentiment_entries.append([timestamp, token, "Twitter", tw_mentions])

        # Reddit (stub)
        if ENABLE_REDDIT:
            sentiment_entries.append([timestamp, token, "Reddit", 0])

    if sentiment_entries:
        radar_ws.append_rows(sentiment_entries, value_input_option="USER_ENTERED")
        print(f"✅ Sentiment Radar logged {len(sentiment_entries)} mentions.")
    else:
        print("⚠️ No sentiment entries written.")
