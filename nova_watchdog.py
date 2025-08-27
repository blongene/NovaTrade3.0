import time, threading
from utils import get_sheet, ping_webhook_debug
from utils import send_telegram_message_dedup

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
                    send_telegram_message_dedup(msg, key="nova_trigger", ttl_min=10)

            except Exception as e:
                ping_webhook_debug(f"❌ Watchdog Error: {e}")

            time.sleep(300)  # Check every 5 minutes

import threading, time
from utils import get_sheet, detect_stalled_tokens

def start_watchdog():
    def loop():
        print("🔁 Starting watchdog loop...")
        while True:
            try:
                sheet = get_sheet()
                detect_stalled_tokens(sheet)
            except Exception as e:
                print(f"❌ Watchdog loop error: {e}")
            time.sleep(180)
    threading.Thread(target=loop, daemon=True).start()
