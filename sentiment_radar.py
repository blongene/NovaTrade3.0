# sentiment_radar.py
import os, requests, gspread
from datetime import datetime, timezone
from oauth2client.service_account import ServiceAccountCredentials
from utils import with_sheet_backoff

ENABLE_REDDIT = False
ENABLE_TWITTER = True
YOUTUBE_ENABLED = os.getenv("YOUTUBE_ENABLED", "false").lower() == "true"

_YT_COOLDOWN_FILE = "/tmp/nova_yt_quota.block"
_TW_COOLDOWN_UNTIL = 0.0

def _utc_ymd():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _yt_cooldown_active():
    try:
        if not os.path.exists(_YT_COOLDOWN_FILE): return False
        return open(_YT_COOLDOWN_FILE).read().strip() == _utc_ymd()
    except: return False

def _arm_yt_cooldown():
    try:
        open(_YT_COOLDOWN_FILE, "w").write(_utc_ymd())
    except: pass

def _gclient():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds)

@with_sheet_backoff
def _open_ws(sheet_url, title):
    return _gclient().open_by_url(sheet_url).worksheet(title)

def fetch_twitter_mentions(token: str) -> int:
    global _TW_COOLDOWN_UNTIL
    if not ENABLE_TWITTER or datetime.now().timestamp() < _TW_COOLDOWN_UNTIL:
        return 0
    try:
        bearer = os.getenv("TWITTER_BEARER_TOKEN")
        if not bearer: return 0
        headers = {"Authorization": f"Bearer {bearer}"}
        q = f"{token} -is:retweet lang:en"
        url = f"https://api.twitter.com/2/tweets/search/recent?query={q}&max_results=10"
        r = requests.get(url, headers=headers, timeout=12)
        if r.status_code == 429:
            print("⚠️ Twitter 429; cooling down 15min.")
            _TW_COOLDOWN_UNTIL = datetime.now().timestamp() + 900
            return 0
        if r.status_code != 200: return 0
        return len(r.json().get("data", []))
    except: return 0

def fetch_youtube_mentions(token: str) -> int:
    if not YOUTUBE_ENABLED or _yt_cooldown_active(): return 0
    try:
        api_key = os.getenv("YOUTUBE_API_KEY")
        if not api_key: return 0
        url = f"https://www.googleapis.com/youtube/v3/search?key={api_key}&part=snippet&type=video&maxResults=3&q={requests.utils.quote(token)}"
        r = requests.get(url, timeout=12)
        if r.status_code == 403 and "quota" in r.text.lower():
            _arm_yt_cooldown()
            print("⛔️ YouTube quota exceeded — silenced until next UTC day.")
            return 0
        if r.status_code != 200: return 0
        return len(r.json().get("items", []))
    except: return 0

def run_sentiment_radar():
    print("📡 Running Sentiment Radar...")
    sheet_url = os.getenv("SHEET_URL")
    targets_ws = _open_ws(sheet_url, "Sentiment_Targets")
    radar_ws = _open_ws(sheet_url, "Sentiment_Radar")
    targets = targets_ws.get_all_records()
    top = sorted(targets, key=lambda x: int(x.get("Priority", 0) or 0), reverse=True)[:3]

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for row in top:
        token = (row.get("Token") or "").strip()
        if not token: continue
        if YOUTUBE_ENABLED and not _yt_cooldown_active():
            rows.append([now, token, "YouTube", fetch_youtube_mentions(token)])
        if ENABLE_TWITTER:
            rows.append([now, token, "Twitter", fetch_twitter_mentions(token)])
        if ENABLE_REDDIT:
            rows.append([now, token, "Reddit", 0])

    if rows:
        @with_sheet_backoff
        def _append(): radar_ws.append_rows(rows, value_input_option="USER_ENTERED")
        _append()
        print(f"✅ Sentiment Radar logged {len(rows)} entries.")
    else:
        print("⚠️ No sentiment entries (all cooled down/disabled).")

if __name__ == "__main__":
    run_sentiment_radar()
