from flask import Flask
import threading
import time

from telegram_webhook import telegram_app
from presale_scorer import run_presale_scorer
from nova_watchdog import run_watchdog
from rotation_signal_engine import scan_rotation_candidates
from roi_milestone_alert import scan_roi_tracking
from token_vault_sync import sync_token_vault

app = telegram_app

# 🚀 Boot sequence
print("📡 Orion Cloud Boot Sequence Initiated")
print("✅ Webhook armed. Launching modules...")

# Run all main modules
run_watchdog()
scan_rotation_candidates()
scan_roi_tracking()
sync_token_vault()

# Start repeating presale check
def presale_loop(interval_minutes=60):
    def loop():
        while True:
            print(f"⏰ Running presale scan every {interval_minutes} min")
            try:
                run_presale_scorer()
            except Exception as e:
                print(f"⚠️ Presale scorer error: {e}")
            time.sleep(interval_minutes * 60)
    t = threading.Thread(target=loop)
    t.daemon = True
    t.start()

presale_loop(interval_minutes=60)

# Boot log
print("🧠 NovaTrade system is live.")

# Flask launch
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
