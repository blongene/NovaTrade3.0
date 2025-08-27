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
    # Hard silence for the rest of the UTC
