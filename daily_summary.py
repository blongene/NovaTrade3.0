# daily_summary.py — Phase-5 Telegram digest (09:00 ET)
import os, time, json, requests
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

BOT_TOKEN = os.getenv("BOT_TOKEN","")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID","")
SHEET_URL = os.getenv("SHEET_URL","")
VAULT_WS_NAME = os.getenv("VAULT_INTELLIGENCE_WS", "Vault Intelligence")
POLICY_LOG_WS = os.getenv("POLICY_LOG_WS", "Policy_Log")

def _open_sheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL)

def _send(msg: str):
    if not (BOT_TOKEN and TELEGRAM_CHAT_ID): 
        print("Telegram not configured.")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=15)

def daily_phase5_summary():
    sh = _open_sheet()
    try:
        vi = sh.worksheet(VAULT_WS_NAME).get_all_records()
    except Exception:
        vi = []
    ready = sum(1 for r in vi if str(r.get("rebuy_ready","")).upper()=="TRUE")
    total = len(vi)

    since = datetime.utcnow() - timedelta(hours=24)
    appr = 0; den = 0; reasons = {}
    try:
        pl = sh.worksheet(POLICY_LOG_WS).get_all_records()
    except Exception:
        pl = []
    for r in pl:
        ts = str(r.get("Timestamp","")).replace("Z","")
        ok = str(r.get("OK","")).upper()
        reason = (r.get("Reason","") or "ok").strip()
        try:
            t = datetime.fromisoformat(ts)
        except Exception:
            continue
        if t < since: 
            continue
        if ok in ("TRUE","YES"):
            appr += 1
        else:
            den += 1
            reasons[reason] = reasons.get(reason,0) + 1

    top_denials = sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:3]
    reason_str = ", ".join([f"{k} ({v})" for k,v in top_denials]) if top_denials else "—"
    msg = (
        f"🧠 <b>Phase‑5 Daily</b>\n"
        f"Vault Intelligence: {ready}/{total} rebuy‑ready\n"
        f"Policy: {appr} approved / {den} denied (24h)\n"
        f"Top denials: {reason_str}\n"
        f"Mode: <code>{os.getenv('REBUY_MODE','dryrun')}</code>"
    )
    _send(msg)
    print("Daily Phase‑5 summary sent.")
