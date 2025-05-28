import time, threading
from utils import get_sheet, ping_webhook_debug

def start_watchdog():
    def loop():
        print("🔁 Starting Watchdog...")
        while True:
            try:
                sheet = get_sheet()
                # Placeholder for token scans
            except Exception as e:
                ping_webhook_debug(f"🛑 Watchdog Error: {e}")
            time.sleep(180)
    threading.Thread(target=loop, daemon=True).start()
