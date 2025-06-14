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

        stats = sheet.worksheet("Rotation_Stats").get_all_records()
        staking = sheet.worksheet("Rotation_Log").get_all_records()

        # --- Top ROI Token ---
        roi_dict = {}
        for row in stats:
            token = row.get("Token", "").strip()
            perf = str(row.get("Performance", "")).replace("%", "")
            try:
                roi = float(perf)
                roi_dict[token] = roi
            except:
                continue
        top_roi_token = max(roi_dict, key=roi_dict.get) if roi_dict else "N/A"
        top_roi_value = roi_dict.get(top_roi_token, "N/A")

        # --- Top Yielding Token ---
        yield_dict = {}
        for row in staking:
            token = row.get("Token", "").strip()
            yield_val = str(row.get("Staking Yield (%)", "")).replace("%", "")
            try:
                apr = float(yield_val)
                yield_dict[token] = apr
            except:
                continue
        top_yield_token = max(yield_dict, key=yield_dict.get) if yield_dict else "N/A"
        top_yield_value = yield_dict.get(top_yield_token, "N/A")

        # --- Compose Message ---
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        message = f"📊 <b>NovaTrade Daily Summary</b>\n"
        message += f"🗓 <b>{now}</b>\n\n"
        message += f"🏆 <b>Top ROI Token:</b> <code>{top_roi_token}</code> (+{top_roi_value}%)\n"
        message += f"💰 <b>Top Yielder:</b> <code>{top_yield_token}</code> ({top_yield_value}%)\n"
        message += f"\n🧠 System is live and healthy."

        bot.send_message(chat_id=chat_id, text=message, parse_mode="HTML")
        print("✅ Telegram Summary sent.")

        # --- Log to NovaHeartbeat (optional) ---
        try:
            heartbeat = sheet.worksheet("NovaHeartbeat")
            heartbeat.append_row([str(datetime.utcnow()), "telegram_summaries", f"Sent summary: ROI {top_roi_token}, Yield {top_yield_token}"])
        except:
            print("⚠️ Could not log to NovaHeartbeat.")

    except Exception as e:
        print(f"❌ Telegram summary error: {e}")
