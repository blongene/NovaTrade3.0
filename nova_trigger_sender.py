import os
import requests

def trigger_nova_ping(trigger_type="SOS"):
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    bot_token = os.getenv("BOT_TOKEN")

    if not chat_id or not bot_token:
        print("⚠️ Missing TELEGRAM_CHAT_ID or BOT_TOKEN in environment.")
        return

    messages = {
        "SOS": "🚨 *NovaTrade SOS*\nThis is a test alert to confirm outbound messaging is working.",
        "PRESALE ALERT": "📈 *NovaTrade Presale Alert*\nA new high-score presale has been detected.",
        "ROTATION COMPLETE": "🔁 *NovaTrade Rotation Complete*\nA portfolio rebalancing event has been executed.",
        "SYNC NEEDED": "🧩 *NovaTrade Sync Needed*\nPlease review the latest responses or re-run the sync loop.",
        "FYI ONLY": "📘 *NovaTrade FYI*\nNon-urgent update: system status or data refreshed.",
        "NOVA UPDATE": "🧠 *NovaTrade Intelligence*\nA logic update or system improvement has been deployed."
    }

    msg = messages.get(trigger_type.upper())
    if not msg:
        print(f"⚠️ Unrecognized trigger type: {trigger_type}")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": msg,
        "parse_mode": "Markdown"
    }

    try:
        r = requests.post(url, json=payload)
        print(f"✅ NovaTrigger sent: {trigger_type} → {r.json()}")
    except Exception as e:
        print(f"❌ Failed to send NovaTrigger alert: {e}")
