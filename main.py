import os
from telegram_webhook import telegram_app, set_telegram_webhook
from nova_watchdog import start_watchdog

if __name__ == "__main__":
    print("📡 Orion Cloud Boot Sequence Initiated")
    set_telegram_webhook()
    start_watchdog()
    telegram_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
