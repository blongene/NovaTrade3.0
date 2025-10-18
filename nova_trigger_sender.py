# nova_trigger_sender.py — simple Telegram sender (optional)
import os, requests

def trigger_nova_ping(trigger_type="NOVA UPDATE"):
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    bot_token = os.getenv("BOT_TOKEN")
    if not (chat_id and bot_token):
        print("⚠️ Missing TELEGRAM_CHAT_ID/BOT_TOKEN")
        return
    presets = {
        "SOS": "🚨 *NovaTrade SOS*\nTesting alert path.",
        "PRESALE ALERT": "🚀 *Presale Alert*\nNew high-score presale detected.",
        "ROTATION COMPLETE": "🔁 *Rotation Complete*\nVault rotation executed.",
        "SYNC NEEDED": "🧩 *Sync Needed*\nPlease review latest responses.",
        "FYI ONLY": "📘 *FYI*\nNon-urgent update.",
        "NOVA UPDATE": "🧠 *Nova Update*\nSystem improvement deployed.",
    }
    text = presets.get(trigger_type.upper(), f"🔔 *{trigger_type}*")
    try:
        r = requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                          json={"chat_id":chat_id,"text":text,"parse_mode":"Markdown"}, timeout=15)
        print(f"✅ sent: {trigger_type} ({r.status_code})")
    except Exception as e:
        print(f"❌ telegram send failed: {e}")
