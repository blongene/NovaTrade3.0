import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import requests
import datetime
from googleapiclient.discovery import build

def run_sentiment_radar():
    try:
        # Auth to Google Sheets
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        radar_tab = sheet.worksheet("Sentiment_Radar")
        targets_tab = sheet.worksheet("Sentiment_Targets")

        # Build alias map from Sentiment_Targets
        tokens = targets_tab.get_all_records()
        alias_map = {}
        for row in tokens:
            token = row["Token"]
            aliases = [v.strip() for k, v in row.items() if k.startswith("Alias") and v]
            alias_map[token] = aliases

        logs = []
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # ---- REDDIT ----
        for token, aliases in alias_map.items():
            search_terms = [token] + aliases
            for term in search_terms:
                response = requests.get(
                    f"https://www.reddit.com/search.json?q={term}&limit=5",
                    headers={'User-agent': 'NovaRadar/1.0'}
                )
                if response.status_code == 200:
                    posts = response.json().get("data", {}).get("children", [])
                    for post in posts:
                        title = post["data"].get("title", "")
                        logs.append([now, token, term, title[:120], "Reddit"])

        # ---- YOUTUBE ----
        YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
        if YOUTUBE_API_KEY:
            youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
            for token, aliases in alias_map.items():
                search_terms = [token] + aliases
                for term in search_terms:
                    request = youtube.search().list(
                        q=term,
                        part="snippet",
                        maxResults=5,
                        type="video"
                    )
                    response = request.execute()
                    for item in response.get("items", []):
                        title = item["snippet"]["title"]
                        logs.append([now, token, term, title[:120], "YouTube"])
        else:
            print("⚠️ YOUTUBE_API_KEY not set. Skipping YouTube scan.")

        # ---- Append results ----
        if logs:
            radar_tab.append_rows(logs, value_input_option="USER_ENTERED")
            print(f"✅ Sentiment Radar logged {len(logs)} mentions.")
        else:
            print("✅ Sentiment Radar found no mentions.")

    except Exception as e:
        print(f"❌ Sentiment Radar failed: {e}")
