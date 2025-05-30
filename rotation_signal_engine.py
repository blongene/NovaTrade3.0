import gspread
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import requests

SHEET_URL = os.environ.get("SHEET_URL")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

def run_rotation_signals():
    print("🧠 Running Rotation Signal Engine...")

    try:
        # Authenticate
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(SHEET_URL)
        stats_ws = sheet.worksheet("Rotation_Stats")

        # Load data
        rows = stats_ws.get_all_records()
        print(f"📊 Retrieved {len(rows)} rows from Rotation_Stats")

        for row in rows:
            # Skip empty rows
            if not row.get("Token") or not row.get("Status") or not row.get("Decision"):
                continue

            token = row["Token"]
            status = row["Status"]
            decision = row["Decision"]

            try:
                # Skip non-YES entries or already rotated
                if decision != "YES" or status != "Active":
                    continue

                sentiment = row.get("Sentiment", "").strip().lower()
                roi_str = row.get("Follow-up ROI", "N/A")
                days = row.get("Days Held", "")

                # Skip if ROI is not available
                if roi_str in ["", "N/A"]:
                    continue

                try:
                    roi = float(roi_str)
                except ValueError:
                    print(f"⚠️ Invalid ROI format for {token}: '{roi_str}'")
                    continue

                # Trigger exit suggestion based on logic
                if roi < 1.0 and "weak" in sentiment:
                    send_rotation_alert(token, days, roi, sentiment)

            except Exception as e:
                print(f"❌ Error processing row {row}: {e}")

    except Exception as e:
        print(f"❌ Rotation Engine Failure: {e}")

def send_rotation_alert(token, days, roi, sentiment):
    msg = f"""🔁 *Rotation Suggestion: {token.upper()}*
- Days Held: {days}
- ROI: {roi}x
- Sentiment: {sentiment}

Would you like to rotate out of this token?"""

    payload = {
        "chat_id": CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown"
    }

    try:
        response = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=payload)
        if response.ok:
            print(f"✅ Alert sent for {token}")
        else:
            print(f"⚠️ Telegram error for {token}: {response.text}")
    except Exception as e:
        print(f"❌ Failed to send Telegram alert for {token}: {e}")
