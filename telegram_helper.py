# telegram_helper.py
import os, requests

def send_telegram(text: str):
  if os.getenv("ENABLE_TELEGRAM","").lower() not in ("1","true","yes"):
    return
  token = os.getenv("TELEGRAM_BOT_TOKEN")
  chat_id = os.getenv("TELEGRAM_CHAT_ID")
  if not token or not chat_id:
    return
  try:
    requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                  json={"chat_id": chat_id, "text": text[:4000], "parse_mode": "HTML"},
                  timeout=10)
  except Exception:
    pass
