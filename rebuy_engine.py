# rebuy_engine.py

import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import ping_webhook_debug, send_telegram_message

def run_undersized_rebuy():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        log_ws = sheet.worksheet("Rotation_Log")
        vault_ws = sheet.worksheet("Token_Vault")
        memory_ws = sheet.worksheet("Rotation_Memory")
        stats_ws = sheet.worksheet("Rotation_Stats")

        vault = {r["Token"].strip().upper(): float(r.get("Allocation", 0)) for r in vault_ws.get_all_records() if r.get("Token")}
        memory = {r["Token"].strip().upper(): float(str(r.get("Win Rate", "")).replace("%", "") or 0) for r in memory_ws.get_all_records() if r.get("Token")}
        stats = {}
        for row in stats_ws.get_all_records():
            token = row.get("Token", "").strip().upper()
            if not token:
                continue
            if token not in stats:
                stats[token] = []
            perf = row.get("Performance")
            if perf and str(perf).replace('.', '', 1).replace('-', '', 1).isdigit():
                stats[token].append(float(perf))

        usdt_available = float(os.getenv("USDT_AVAILABLE", "0"))
        if usdt_available < 20:
            print("⚠️ Not enough USDT for rebuy. Skipping...")
            return

        candidates = []
        rows = log_ws.get_all_records()
        for i, row in enumerate(rows, start=2):
            token = row.get("Token", "").strip().upper()
            status = row.get("Status", "").strip().lower()
            allocation = float(row.get("Allocation", 0))

            if status != "active" or allocation >= 90:
                continue

            vault_alloc = vault.get(token, 100)
            drift = vault_alloc - allocation
            win = memory.get(token, 0)
            avg_roi = round(sum(stats[token]) / len(stats[token]), 2) if token in stats and stats[token] else 0

            if drift >= 5 and win >= 60:
                candidates.append((token, drift, win, avg_roi))

        for token, drift, win, avg_roi in sorted(candidates, key=lambda x: -x[1]):
            msg = f"\ud83d\udd01 Rebuy candidate detected: ${token}\n"
            msg += f"\u2013 Win Rate: {win}%\n"
            msg += f"\u2013 Avg ROI: {avg_roi}x\n"
            msg += f"\u2013 Undersized by -{round(drift,1)}%\n"
            msg += f"\nRebuy $25 into ${token}? ✅ or \u274c"
            send_telegram_message(msg)

        print(f"✅ {len(candidates)} rebuy candidates suggested.")

    except Exception as e:
        print(f"❌ Rebuy engine error: {e}")
        ping_webhook_debug(f"❌ Rebuy engine error: {e}")
