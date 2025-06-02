import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import requests
import datetime

def run_sentiment_radar():
    try:
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

        # Check each token/alias across mock sources (placeholder for future Reddit/YouTube API calls)
        for token, aliases in alias_map.items():
            search_terms = [token] + aliases
            for term in search_terms:
                # Example Reddit search (mocked as real API not used here)
                response = requests.get(f"https://www.reddit.com/search.json?q={term}&limit=5", headers={'User-agent': 'NovaRadar/1.0'})
                if response.status_code == 200:
                    posts = response.json().get("data", {}).get("children", [])
                    for post in posts:
                        title = post["data"].get("title", "")
                        timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                        logs.append([timestamp, token, term, title, "Reddit"])

        # Append results to Sentiment_Radar
        if logs:
            radar_tab.append_rows(logs, value_input_option="RAW")
            print(f"✅ Sentiment Radar logged {len(logs)} mentions.")
        else:
            print("✅ Sentiment Radar found no mentions.")
    
    except Exception as e:
        print(f"❌ Sentiment Radar failed: {e}")
