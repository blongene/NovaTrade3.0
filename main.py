
from telegram_webhook import telegram_app
from nova_watchdog import run_watchdog
from rotation_signal_engine import scan_rotation_candidates
from roi_milestone_alert import check_roi_milestones
from flask import Flask
import threading
import time

print("📡 Orion Cloud Boot Sequence Initiated")

# Initialize Flask app for webhook
app = telegram_app

# Start the Telegram webhook listener
def start_webhook():
    app.run(host="0.0.0.0", port=10000)

# Start the Rotation Alert system
def start_rotation_signaler():
    print("🔁 Checking for stalled rotation candidates...")
    while True:
        scan_rotation_candidates()
        time.sleep(3600)

# Start ROI Milestone Tracker
def start_roi_tracker():
    print("📈 Checking ROI Milestone Alerts...")
    while True:
        check_roi_milestones()
        time.sleep(86400)

# Launch threads
if __name__ == "__main__":
    print("✅ Webhook armed. Launching modules...")

    threading.Thread(target=start_webhook).start()
    threading.Thread(target=run_watchdog).start()
    threading.Thread(target=start_rotation_signaler).start()
    threading.Thread(target=start_roi_tracker).start()

    print("🧠 NovaTrade system is live.")
