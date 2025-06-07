import os
import gspread
import requests
import time
from oauth2client.service_account import ServiceAccountCredentials

def run_sentiment_radar():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    radar_ws = sheet.worksheet("Sentiment_Radar")
    targets_ws = sheet.worksheet("Sentiment_Targets")
    targets = targets_ws.get_all_records()
    time.sleep(2)

    mentions = []
    for t in targets:
        token = t["Token"]
        aliases = [t.get(f"Alias {i}", "") for i in range(1, 4)]
        for term in [token] + aliases:
            if not term:
                continue
            # Simulate Reddit pull (replace with real)
            mentions.append([token, term, "Sample Reddit Text", "Reddit"])
            time.sleep(0.5)

            # YouTube logic
            yt_key = os.getenv("YOUTUBE_API_KEY")
            if yt_key:
                try:
                    url = f"https://www.googleapis.com/youtube/v3/search?q={term}&key={yt_key}&part=snippet&maxResults=1"
                    res = requests.get(url)
                    if res.status_code == 200:
                        yt_data = res.json()
                        for item in yt_data.get("items", []):
                            mentions.append([token, term, item["snippet"]["title"], "YouTube"])
                    else:
                        print(f"⚠️ YouTube error: {res.status_code} - {res.text}")
                except Exception as e:
                    print(f"⚠️ YouTube fetch failed for {term}: {e}")
            time.sleep(1)

    radar_ws.clear()
    radar_ws.append_row(["Date Detected", "Token", "Mentioned Term", "Sentiment Text", "Source"])
    for m in mentions:
        radar_ws.append_row([time.strftime("%Y-%m-%d %H:%M:%S")] + m)

    print(f"✅ Sentiment Radar logged {len(mentions)} mentions.")
