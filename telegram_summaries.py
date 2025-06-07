import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Bot
from datetime import datetime

def run_telegram_summaries():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        bot = Bot(token=os.getenv("BOT_TOKEN"))
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        # Load Performance Metrics
        stats = sheet.worksheet("Rotation_Stats").get_all_records()
        staking = sheet.worksheet("Rotation_Log").get_all_records()

        # Determine Top ROI Performer
        roi_dict = {}
        for row in stats:
            token = row.get("Token", "").strip()
            perf = row.get("Performance", "")
            try:
                roi = float(perf)
                roi_dict[token] = roi
            except:
                continue

        top_roi_token = max(roi_dict, key=roi_dict.get) if roi_dict else "N/A"
        top_roi_value = roi_dict.get(top_roi_token, 0)

        # Determine Top Yielder
        yield_dict = {}
        for row in staking:
            token = row.get("Token", "").strip()
            yield_val = row.get("Staking Yield", "")
            try:
                apr = float(str(yield_val).replace("%", "").strip())
                yield_dict[token] = apr
            except:
                continue

        top_yield_token = max(yield_dict, key=yield_dict.get) if yield_dict else "N/A"
        top_yield_value = yield_dict.get(top_yield_token, 0)

        # Build summary message
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        message = f"📊 *NovaTrade Daily Summary*\n\n"
        message += f"🗓 *{now}*\n"
        message += f"🏆 *Top ROI Token*: `{top_roi_token}` (+{top_roi_value}%)\n"
        message += f"💰 *Top Yielder*: `{top_yield_token}` ({top_yield_value}%)\n"
        message += f"\n🧠 _System is live and healthy._"

        bot.send_message(chat_id=chat_id, text=message, parse_mode="Markdown")
        print("✅ NovaHeartbeat log: [Telegram Summary] Sent summary with 0 votes")
    except Exception as e:
        print(f"⚠️ Telegram send error: {e}")
