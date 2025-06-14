import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from collections import defaultdict

# === CONFIG ===
MENTION_THRESHOLD = 30
SIGNAL_SCORE_HYPE = 0.4
SIGNAL_SCORE_NEGATIVE = -0.2

# === SETUP ===
def get_ws(sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    return sheet.worksheet(sheet_name)

# === MAIN FUNCTION ===
def run_sentiment_summary():
    print("📊 Generating Sentiment Summary...")
    radar_ws = get_ws("Sentiment_Radar")
    summary_ws = get_ws("Sentiment_Summary")

    radar_rows = radar_ws.get_all_records()
    grouped = defaultdict(lambda: {"Twitter": 0, "YouTube": 0, "Reddit": 0, "Total": 0})

    for row in radar_rows[-100:]:  # last 100 rows only
        token = row.get("Token", "").strip()
        source = row.get("Source", "").strip()
        count = int(row.get("Mentions", 0))

        if not token or not source:
            continue

        grouped[token][source] += count
        grouped[token]["Total"] += count

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    summary_rows = []

    for token, counts in grouped.items():
        twitter_avg = round(counts["Twitter"] / max(1, counts["Total"]), 2)
        reddit_avg = round(counts["Reddit"] / max(1, counts["Total"]), 2)
        yt_count = counts["YouTube"]
        total = counts["Total"]
        
        # Weighted signal: YT gets more weight
        signal_score = round((0.4 * twitter_avg) + (0.2 * reddit_avg) + (0.4 * yt_count / max(1, total)), 2)

        alert = ""
        if total >= MENTION_THRESHOLD:
            if signal_score >= SIGNAL_SCORE_HYPE:
                alert = "🔥 HIGH HYPE"
            elif signal_score <= SIGNAL_SCORE_NEGATIVE:
                alert = "⚠️ NEGATIVE"

        summary_rows.append([
            timestamp,
            token,
            total,
            twitter_avg,
            reddit_avg,
            yt_count,
            signal_score,
            alert
        ])

    summary_ws.append_rows(summary_rows, value_input_option="USER_ENTERED")
    print(f"✅ Summary written for {len(summary_rows)} token(s)")
