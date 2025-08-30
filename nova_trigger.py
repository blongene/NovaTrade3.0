# nova_trigger.py — Telegram-only notifier (no Sheets I/O)
import os, time, random
from utils import send_telegram_message_dedup

JIT_MIN = float(os.getenv("NOVA_PING_JITTER_MIN_S", "0.2"))
JIT_MAX = float(os.getenv("NOVA_PING_JITTER_MAX_S", "0.8"))

def trigger_nova_ping(title: str = "NOVA UPDATE", body: str = ""):
    print("▶ Nova ping …")
    time.sleep(random.uniform(JIT_MIN, JIT_MAX))
    # One call to Telegram with global de-dup (15–20m by env TG_DEDUP_TTL_MIN)
    msg = f"🔔 *{title}*\n{body}".strip()
    send_telegram_message_dedup(msg, key="nova_ping")
