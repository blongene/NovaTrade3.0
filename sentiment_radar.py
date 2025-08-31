import os, time, requests, gspread
from datetime import datetime, timezone
from oauth2client.service_account import ServiceAccountCredentials
from utils import with_sheet_backoff, send_telegram_message

ENABLE_TWITTER = True
YOUTUBE_ENABLED = os.getenv("YOUTUBE_ENABLED", "false").lower() == "true"
_YT_COOLDOWN_FILE = "/tmp/nova_yt_quota.block"

def _utc_ymd(): return datetime.now(timezone.utc).strftime("%Y-%m-%d")
def _yt_cooldown_active(): return os.path.exists(_YT_COOLDOWN_FILE) and open(_YT_COOLDOWN_FILE).read().strip() == _utc_ymd()
def _arm_yt_cooldown(): open(_YT_COOLDOWN_FILE,"w").write(_utc_ymd())

def fetch_twitter_mentions(token: str) -> int:
    if not ENABLE_TWITTER: return 0
    bearer = os.getenv("TWITTER_BEARER_TOKEN")
    if not bearer: return 0
    try:
        headers = {"Authorization": f"Bearer {bearer}"}
        url = f"https://api.twitter.com/2/tweets/search/recent?query={token} -is:retweet lang:en&max_results=10"
        r = requests.get(url, headers=headers, timeout=12)
        if r.status_code == 429:
            print("⚠️ Twitter 429; entering cooldown.")
            return 0
        if r.status_code != 200: return 0
        return len(r.json().get("data", []))
    except Exception: return 0

def fetch_youtube_mentions(token: str) -> int:
    if not YOUTUBE_ENABLED or _yt_cooldown_active(): return 0
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key: return 0
    try:
        url = f"https://www.googleapis.com/youtube/v3/search?key={api_key}&part=snippet&type=video&maxResults=3&q={token}"
        r = requests.get(url, timeout=12)
        if r.status_code != 200: return 0
        data = r.json()
        if "error" in data and data["error"].get("errors", [{}])[0].get("reason") == "quotaExceeded":
            print("⚠️ YouTube quota exceeded — silencing for rest of day.")
            _arm_yt_cooldown()
            return 0
        return len(data.get("items", []))
    except Exception: return 0

def run_sentiment_radar():
    print("📡 Running Sentiment Radar...")
    tokens = ["MIND","FOMO","etc"]  # stub
    for t in tokens:
        tw = fetch_twitter_mentions(t)
        yt = fetch_youtube_mentions(t)
        print(f"{t}: Twitter={tw}, YouTube={yt}")
