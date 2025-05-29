from nova_watchdog import run_watchdog
from flask import Flask
from telegram_webhook import telegram_app
from roi_milestone_alert import check_roi_milestones
from rotation_signal_engine import scan_rotation_candidates
import threading
import time
import os

print("📡 Orion Cloud Boot Sequence Initiated")

# Launch Flask Telegram webhook app
def run_flask():
    print("✅ Webhook armed. Launching modules...")
    telegram_app.run(host='0.0.0.0', port=10000)

# Rotation decay + signal scanner
def start_rotation_signaler():
    while True:
        print("🔁 Checking for stalled rotation candidates...")
        scan_rotation_candidates()
        time.sleep(3600)  # run hourly

# ROI milestone alert loop
def start_roi_alert_loop():
    while True:
        print("📈 Checking for ROI milestone follow-ups...")
        check_roi_milestones()
        time.sleep(3600)  # run hourly

# Start Telegram webhook Flask app
flask_thread = threading.Thread(target=run_flask)
flask_thread.start()

# Start watchdog scanner
watchdog_thread = threading.Thread(target=run_watchdog)
watchdog_thread.start()

# Start stalled rotation signal checker
rotation_thread = threading.Thread(target=start_rotation_signaler)
rotation_thread.start()

# Start ROI milestone alert checker
roi_thread = threading.Thread(target=start_roi_alert_loop)
roi_thread.start()

print("🧠 NovaTrade system is live.")
