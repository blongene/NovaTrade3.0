import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from utils import send_telegram_message_dedup

send_telegram_message_dedup("🧠 Sync Required\nNew decisions are pending rotation. Please review the planner tab.",
                            key="sync_required", ttl_min=30)
def check_nova_trigger():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    trigger_ws = sheet.worksheet("NovaTrigger")
    raw = trigger_ws.acell("A1").value.strip().upper()

    if not raw or raw == "READY":
        return

    alert_map = {
        "SOS": "🚨 *NovaTrade SOS*\nThis is a test alert to confirm outbound messaging is working.",
        "FYI ONLY": "📘 *NovaTrade FYI*\nNon-urgent update: system status or data refreshed.",
        "SYNC NEEDED": "🧩 *NovaTrade Sync Needed*\nPlease review the latest responses or re-run the sync loop.",
        "NOVA UPDATE": "🧠 *NovaTrade Intelligence*\nA logic update or system improvement has been deployed.",
    }

    msg = alert_map.get(raw)
    if msg:
        send_telegram_message(msg)
        print(f"✅ NovaTrigger sent: {raw}")
    else:
        print(f"⚠️ Unknown NovaTrigger value: {raw}")

    trigger_ws.update_acell("A1", "READY")
