import time, threading
from utils import get_sheet, ping_webhook_debug
from send_telegram import send_message

def run_watchdog():
    def loop():
        print("🔍 Starting Watchdog...")
        while True:
            try:
                # Step 1: Load Google Sheet
                sheet = get_sheet()
                scout_tab = sheet.worksheet("Scout Decisions")

                # Step 2: Get all rows
                data = scout_tab.get_all_records()

                # Step 3: Find rows with missing YES/NO decision
                missing = [row for row in data if row.get("Decision", "").strip() == ""]

                # Step 4: Send alert if any missing
                if missing:
                    msg = f"🐕 NovaWatchdog Alert:\n{len(missing)} presale(s) have no decision logged.\nWould you like to review?"
                    send_message(msg)

            except Exception as e:
                ping_webhook_debug(f"❌ Watchdog Error: {e}")

            time.sleep(300)  # Check every 5 minutes

    threading.Thread(target=loop, daemon=True).start()
