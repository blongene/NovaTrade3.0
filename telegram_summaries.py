# telegram_summaries.py — NT3.0 Phase-1 Polish
# Lightweight periodic summary → Telegram (de-duped). Zero per-cell writes.

import os
from datetime import datetime
from utils import (
    get_records_cached, str_or_empty, to_float,
    send_telegram_message_dedup,
)

MAX_LINES = int(os.getenv("TG_SUMMARY_MAX_LINES", "10"))
DEDUP_TTL_MIN = int(os.getenv("TG_SUMMARY_TTL_MIN", "90"))  # quiet window

def _top_yes(stats, n=5):
    yes = []
    for r in stats:
        if str_or_empty(r.get("Decision")).upper() == "YES":
            p = to_float(r.get("Performance"))
            if p is not None:
                yes.append((str_or_empty(r.get("Token")).upper(), p))
    yes.sort(key=lambda x: x[1], reverse=True)
    return yes[:n]

def run_telegram_summary():
    stats = get_records_cached("Rotation_Stats", ttl_s=300) or []
    if not stats:
        return

    top = _top_yes(stats, n=5)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    lines = [f"📣 *NovaTrade Summary* — {now}", ""]
    if top:
        lines.append("*Top YES by Performance*")
        for t, p in top:
            lines.append(f"• {t}: `{p:.2f}`")
    else:
        lines.append("_No YES positions with numeric performance yet._")

    msg = "\n".join(lines[:MAX_LINES])
    send_telegram_message_dedup(msg, key="tg_summary", ttl_min=DEDUP_TTL_MIN)
