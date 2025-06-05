import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
from textblob import TextBlob
import os
import requests
import datetime

def run_sentiment_radar():
    try:
        # === AUTH ===
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        radar_ws = sheet.worksheet("Sentiment_Radar")
        targets_ws = sheet.worksheet("Sentiment_Targets")
        summary_ws = sheet.worksheet("Sentiment_Summary")

        YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
        TWITTER_BEARER = os.getenv("TWITTER_BEARER")

        # === DATA ===
        tokens = targets_ws.get_all_records()
        alias_map = {}
        for row in tokens:
            token = row["Token"]
            aliases = [v.strip() for k, v in row.items() if k.startswith("Alias") and v]
            alias_map[token] = aliases

        logs = []
        summary_logs = []
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for token, aliases in alias_map.items():
            search_terms = [token] + aliases
            reddit_scores, reddit_count = 0, 0
            twitter_scores, twitter_count = 0, 0
            youtube_count = 0

            # === REDDIT ===
            for term in search_terms:
                try:
                    resp = requests.get(f"https://www.reddit.com/search.json?q={term}&limit=5", headers={'User-agent': 'NovaRadar/1.0'})
                    posts = resp.json().get("data", {}).get("children", [])
                    for post in posts:
                        text = post["data"].get("title", "")
                        polarity = TextBlob(text).sentiment.polarity
                        reddit_scores += polarity
                        reddit_count += 1
                        logs.append([now, token, term, text[:120], "Reddit"])
                except:
                    pass

            # === TWITTER ===
            if TWITTER_BEARER:
                for term in search_terms:
                    try:
                        resp = requests.get(
                            "https://api.twitter.com/2/tweets/search/recent",
                            headers={"Authorization": f"Bearer {TWITTER_BEARER}"},
                            params={"query": term, "tweet.fields": "lang,text", "max_results": 10}
                        )
                        tweets = resp.json().get("data", [])
                        for tweet in tweets:
                            text = tweet["text"]
                            if tweet.get("lang") == "en":
                                polarity = TextBlob(text).sentiment.polarity
                                twitter_scores += polarity
                                twitter_count += 1
                                logs.append([now, token, term, text[:120], "Twitter"])
                    except Exception as te:
                        print(f"⚠️ Twitter error on {term}: {te}")
            else:
                print("⚠️ TWITTER_BEARER not set.")

            # === YOUTUBE ===
            if YOUTUBE_API_KEY:
                try:
                    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
                    for term in search_terms:
                        resp = youtube.search().list(q=term, part="snippet", maxResults=5, type="video").execute()
                        for item in resp.get("items", []):
                            text = item["snippet"]["title"]
                            logs.append([now, token, term, text[:120], "YouTube"])
                            youtube_count += 1
                except:
                    print("⚠️ YouTube error.")
            else:
                print("⚠️ YOUTUBE_API_KEY not set.")

            # === SCORE ===
            reddit_avg = round(reddit_scores / max(reddit_count, 1), 2)
            twitter_avg = round(twitter_scores / max(twitter_count, 1), 2)
            total_mentions = reddit_count + twitter_count + youtube_count
            signal_score = round((reddit_avg + twitter_avg) / 2, 2)
            alert = ""
            if signal_score > 0.5 and total_mentions >= 5:
                alert = "🔥 Hype Detected"
            elif signal_score < -0.2:
                alert = "⚠️ Negative"

            summary_logs.append([
                now, token, total_mentions, twitter_avg, reddit_avg, youtube_count, signal_score, alert
            ])

        # === WRITE ===
        if logs:
            radar_ws.append_rows(logs, value_input_option="USER_ENTERED")
            print(f"✅ Sentiment Radar logged {len(logs)} mentions.")
        if summary_logs:
            summary_ws.append_rows(summary_logs, value_input_option="USER_ENTERED")
            print(f"📊 Sentiment Summary updated for {len(summary_logs)} tokens.")

    except Exception as e:
        print(f"❌ Sentiment Radar failed: {e}")
