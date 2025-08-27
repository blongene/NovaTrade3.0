# sentiment_radar.py
import os
import time
import requests
import gspread
from datetime import datetime, timezone
from oauth2client.service_account import ServiceAccountCredentials

from utils import with_sheet_backoff

# ===== Toggles =====
ENABLE_REDDIT = False   # placeholder
ENABLE_TWITTER = True
YOUTUBE_ENABLED = os.getenv("YOUTUBE_ENABLED", "false").lower() == "true"

# Local daily cooldown file for YouTube (resets each UTC midnight)
_YT_COOLDOWN_FILE = "/tmp/nova_yt_quota.block"

# Soft in-process cooldown for Twitter 429s
_TW_COOLDOWN_UNTIL = 0.0  # epoch seconds


def _utc_ymd():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _yt_cooldown_active() -> bool:
    try:
        if not os.path.exists(_YT_COOLDOWN_FILE):
            return False
        stamp = open(_YT_COOLDOWN_FILE, "r").read().strip()
        return stamp == _utc_ymd()
    except Exception:
        return False


def _arm_yt_cooldown():
    try:
        with open(_YT_COOLDOWN_FILE, "w") as f:
            f.write(_utc_ymd())
    except Exception:
        pass


def _gclient():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds)


@with_sheet_backoff
def _open_ws(sheet_url: str, title: str):
    sh = _gclient().open_by_url(sheet_url)
    return sh.worksheet(title)


def _safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


def fetch_twitter_mentions(token: str) -> int:
    global _TW_COOLDOWN_UNTIL
    try:
        if not ENABLE_TWITTER:
            return 0
        now = time.time()
        if now < _TW_COOLDOWN_UNTIL:
            # still cooling down
            return 0

        bearer = os.getenv("TWITTER_BEARER_TOKEN")
        if not bearer:
            return 0
        headers = {"Authorization": f"Bearer {bearer}"}
        q = f"{token} -is:retweet lang:en"
        url = f"https://api.twitter.com/2/tweets/search/recent?query={q}&max_results=10"
        r = requests.get(url, headers=headers, timeout=12)
        if r.status_code == 429:
            # enter a 10-minute cooldown to avoid noisy logs
            _TW_COOLDOWN_UNTIL = now + 10 * 60
            print("⚠️ Twitter 429; entering 10-minute cooldown.")
            return 0
        if r.status_code != 200:
            return 0
        data = r.json()
        return len(data.get("data", []))
    except Exception:
        return 0


def fetch_youtube_mentions(token: str) -> int:
    # Hard silence for the rest of the UTC day after a quotaExceeded once
    if not YOUTUBE_ENABLED or _yt_cooldown_active():
        return 0
    try:
        api_key = os.getenv("YOUTUBE_API_KEY")
        if not api_key:
            return 0
        url = (
            "https://www.googleapis.com/youtube/v3/search"
            f"?key={api_key}&part=snippet&type=video&maxResults=3&q={requests.utils.quote(token)}"
        )
        r = requests.get(url, timeout=12)
        if r.status_code == 403 and "quota" in (r.text or "").lower():
            _arm_yt_cooldown()
            print("⛔️ YouTube quota exceeded — silenced until next UTC day.")
            return 0
        if r.status_code != 200:
            return 0
        data = r.json()
        return _safe_int(len(data.get("items", [])))
    except Exception:
        return 0


def run_sentiment_radar():
    print("📡 Running Sentiment Radar...")
    sheet_url = os.getenv("SHEET_URL")
    targets_ws = _open_ws(sheet_url, "Sentiment_Targets")
    radar_ws = _open_ws(sheet_url, "Sentiment_Radar")

    targets = targets_ws.get_all_records()
    # Pick top 3 by Priority
    top = sorted(targets, key=lambda x: _safe_int(x.get("Priority", 0)), reverse=True)[:3]

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for row in top:
        token = (row.get("Token") or "").strip()
        if not token:
            continue

        # YouTube (silenced if quota hit earlier today)
        if YOUTUBE_ENABLED and not _yt_cooldown_active():
            yt = fetch_youtube_mentions(token)
            rows.append([now, token, "YouTube", yt])

        # Twitter (soft cooldown on 429)
        if ENABLE_TWITTER:
            tw = fetch_twitter_mentions(token)
            rows.append([now, token, "Twitter", tw])

        # Reddit stub (disabled)
        if ENABLE_REDDIT:
            rows.append([now, token, "Reddit", 0])

    if rows:
        @with_sheet_backoff
        def _append():
            radar_ws.append_rows(rows, value_input_option="USER_ENTERED")
        _append()
        print(f"✅ Sentiment Radar logged {len(rows)} entries.")
    else:
        print("⚠️ No sentiment entries written (sources disabled/cooldowns).")


if __name__ == "__main__":
    run_sentiment_radar()
