from telegram_webhook import telegram_app
from nova_watchdog import run_watchdog
from rotation_signal_engine import scan_rotation_candidates

import threading
import time

def start_rotation_signaler():
    while True:
        print("🔁 Checking for stalled rotation candidates...")
        scan_rotation_candidates()
        time.sleep(3600)  # Runs every 60 seconds

if __name__ == "__main__":
    print("📡 Orion Cloud Boot Sequence Initiated")
    
    print("✅ Webhook armed. Launching modules...")
    threading.Thread(target=run_watchdog).start()
    threading.Thread(target=start_rotation_signaler).start()

    print("🧠 NovaTrade system is live.")
    telegram_app.run(host="0.0.0.0", port=10000)
