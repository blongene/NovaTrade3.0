# telegram_rebalance_handler.py — Phase-6 Safe
import os
from datetime import datetime
from utils import get_ws_cached, get_all_records_cached, ws_update, ws_append_row, warn
from telegram_webhook import _send_telegram as send_telegram

PLANNER_TAB = "Rotation_Planner"

def handle_rebalance_vote(token: str, action: str):
    """
    Called when a user clicks an inline button (ROTATE/HOLD/IGNORE).
    Logs the vote to the Rotation_Planner sheet.
    """
    try:
        print(f"📩 Telegram Rebalance Vote: {token} → {action}")
        rows = get_all_records_cached(PLANNER_TAB, ttl_s=60)
        ws   = get_ws_cached(PLANNER_TAB, ttl_s=60)
        now  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        match = next((i+2 for i,r in enumerate(rows) if str(r.get("Token","")).upper()==token.upper()), None)
        if match:
            ws_update(ws, f"B{match}", [[now]])
            ws_update(ws, f"C{match}", [[action.upper()]])
        else:
            ws_append_row(ws, [token, now, action.upper(), "Telegram Vote", "", "", "", "", "NO"])
        send_telegram(f"✅ Rebalance vote logged: <b>{token}</b> → <b>{action}</b>")
    except Exception as e:
        warn(f"rebalance_vote error: {e}")
        send_telegram(f"❌ Rebalance vote error: {e}")
