# sentiment_radar.py

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import requests
import datetime
import re

def run_sentiment_radar():
    try:
        print("📡 Running Sentiment Radar...")
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        radar_tab = sheet.worksheet("Sentiment_Radar")
        targets_tab = sheet.worksheet("Sentiment_Targets")

        # Build alias map
        tokens = targets_tab.get_all_records()
        alias_map = {}
        for row in tokens:
            token = row["Token"]
            aliases = [v.strip() for k, v in row.items() if k.startswith("Alias") and v]
            alias_map[token] = aliases

        logs = []
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # 🔴 REDDIT SCAN
        for token, aliases in alias_map.items():
            search_terms = [token] + aliases
            for term in search_terms:
                url = f"https://www.reddit.com/search.json?q={term}&limit=5"
                headers = {'User-agent': 'NovaRadar/1.0'}
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    posts = response.json().get("data", {}).get("children", [])
                    for post in posts:
                        text = post["data"].get("title", "")[:80]
                        logs.append([now, token, term, text[:80], "Reddit"])

        # 📺 YOUTUBE SCAN (Unofficial public comment API)
        for token, aliases in alias_map.items():
            search_query = f"{token} crypto"
            yt_api = f"https://yt.lemnoslife.com/videos?part=snippet&q={search_query}&maxResults=3&type=video"
            yt_response = requests.get(yt_api)
            if yt_response.status_code == 200:
                videos = yt_response.json().get("items", [])
                for video in videos:
                    video_id = video["id"]["videoId"]
                    comments_url = f"https://yt.lemnoslife.com/comments?part=snippet&videoId={video_id}"
                    comment_response = requests.get(comments_url)
                    if comment_response.status_code == 200:
                        comments = comment_response.json().get("items", [])
                        for comment in comments:
                            text = comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                            for alias in aliases + [token]:
                                if re.search(rf"\b{alias}\b", text, re.IGNORECASE):
                                    logs.append([now, token, alias, text[:80], "YouTube"])
                                    break

        # Future: Telegram logic

        # ✅ Append results
        if logs:
            radar_tab.append_rows(logs, value_input_option="RAW")
            print(f"✅ Sentiment Radar logged {len(logs)} mentions.")
        else:
            print("✅ Sentiment Radar found no mentions.")

    except Exception as e:
        print(f"❌ Sentiment Radar failed: {e}")
