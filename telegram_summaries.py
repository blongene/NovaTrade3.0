import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Bot
from dotenv import load_dotenv
from nova_heartbeat import log_heartbeat

def run_telegram_summaries():
    print("📢 Running Telegram Summary Layer...")

    # Auth
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    stats_ws = sheet.worksheet("Rotation_Stats")
    stats = stats_ws.get_all_records()

    yield_ws = sheet.worksheet("Rotation_Log")
    yields = yield_ws.get_all_records()

    # Filter YES votes and numeric ROI
    roi_entries = []
    for row in stats:
        if row.get("Decision") == "YES":
            try:
                val = float(row.get("Performance", ""))
                roi_entries.append((row["Token"], val))
            except:
                continue

    top_roi = sorted(roi_entries, key=lambda x: x[1], reverse=True)[:3]

    # Staking Yield top performers
    yield_entries = []
    for row in yields:
        try:
            val = float(row.get("Staking Yield", ""))
            yield_entries.append((row["Token"], val))
        except:
            continue

    top_yield = sorted(yield_entries, key=lambda x: x[1], reverse=True)[:3]

    # Summary Stats
    avg_roi = round(sum(r[1] for r in roi_entries) / len(roi_entries), 2) if roi_entries else 0.0
    total_yes = len(roi_entries)

    # Build message
    message = f"📊 *NovaTrade Snapshot*\n\n"
    message += f"✅ *YES Votes:* {total_yes}\n"
    message += f"📈 *Avg ROI:* {avg_roi}%\n\n"

    message += "🏆 *Top ROI Tokens:*\n"
    for token, val in top_roi:
        message += f"• {token}: {val}%\n"

    message += "\n💰 *Top Yielding Tokens:*\n"
    for token, val in top_yield:
        message += f"• {token}: {val}%\n"

    log_heartbeat("Telegram Summary", f"Sent summary with {total_yes} votes")

    # Telegram
    try:
        bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        bot.send_message(chat_id=chat_id, text=message, parse_mode="Markdown")
        print("✅ Telegram Summary sent.")
    except Exception as e:
        print(f"⚠️ Telegram send error: {e}")
