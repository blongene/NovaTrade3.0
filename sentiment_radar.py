# sentiment_radar.py
import os, time, requests, gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import with_sheet_backoff

# === CONFIG ===
SHEET_URL = os.getenv("SHEET_URL")
YOUTUBE_ENABLED = os.getenv("YOUTUBE_ENABLED", "false").lower() == "true"
TW_COOLDOWN_MIN = int(os.getenv("TW_COOLDOWN_MIN", "30"))
TW_MAX_RESULTS  = int(os.getenv("TW_MAX_RESULTS", "10"))
TW_BEARER       = os.getenv("TWITTER_BEARER_TOKEN")
YT_KEY          = os.getenv("YOUTUBE_API_KEY")

_last_tw_fail = 0.0

# === GSpread helpers ===
def _gs_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds)

@with_sheet_backoff
def _open_ws(url, tab):
    sh = _gs_client().open_by_url(url)
    return sh.worksheet(tab)

@with_sheet_backoff
def _append_rows(ws, rows):
    ws.append_rows(rows, value_input_option="USER_ENTERED")

@with_sheet_backoff
def _get_records(ws):
    return ws.get_all_records()

# === Providers ===
def fetch_twitter_mentions(token: str) -> int:
    global _last_tw_fail
    if not TW_BEARER:
        print("⚠️ Twitter bearer missing; skipping.")
        return 0
    if time.time() - _last_tw_fail < TW_COOLDOWN_MIN * 60:
        print("🐦 Twitter on cooldown.")
        return 0
    try:
        headers = {"Authorization": f"Bearer {TW_BEARER}"}
        query = f"{token} -is:retweet lang:en"
        url = f"https://api.twitter.com/2/tweets/search/recent?query={query}&max_results={TW_MAX_RESULTS}"
        r = requests.get(url, headers=headers, timeout=12)
        if r.status_code == 429:
            print("⚠️ Twitter 429; entering cooldown.")
            _last_tw_fail = time.time()
            return 0
        if r.status_code != 200:
            print(f"⚠️ Twitter error {r.status_code}: {r.text[:120]}")
            return 0
        data = r.json()
        return len(data.get("data", []))
    except Exception as e:
        print(f"⚠️ Twitter exception: {e}")
        _last_tw_fail = time.time()
        return 0

def fetch_youtube_mentions(token: str) -> int:
    if not YOUTUBE_ENABLED:
        # hard-off by default to avoid quota noise
        return 0
    try:
        from googleapiclient.discovery import build
        yt = build("youtube", "v3", developerKey=YT_KEY)
        req = yt.search().list(q=token, part="snippet", maxResults=3, type="video")
        res = req.execute()
        return len(res.get("items", []))
    except Exception as e:
        print(f"⚠️ YouTube error for '{token}': {e}")
        return 0

# === Main ===
def run_sentiment_radar():
    print("📡 Running Sentiment Radar...")
    radar_ws = _open_ws(SHEET_URL, "Sentiment_Radar")
    targets_ws = _open_ws(SHEET_URL, "Sentiment_Targets")

    targets = _get_records(targets_ws)
    # take top 3 by 'Priority' if present
    try:
        top = sorted(targets, key=lambda x: x.get("Priority", 0), reverse=True)[:3]
    except Exception:
        top = targets[:3]

    entries = []
    for row in top:
        token = (row.get("Token") or "").strip()
        if not token:
            continue
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        yt = fetch_youtube_mentions(token)
        if yt:
            print(f"📺 YouTube: {token} → {yt}")
            entries.append([ts, token, "YouTube", yt])

        tw = fetch_twitter_mentions(token)
        print(f"🐦 Twitter: {token} → {tw}")
        entries.append([ts, token, "Twitter", tw])

    if entries:
        _append_rows(radar_ws, entries)
        print(f"✅ Sentiment Radar logged {len(entries)} entries.")
    else:
        print("⚠️ No sentiment entries written.")
