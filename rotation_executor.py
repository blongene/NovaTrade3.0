from utils import get_gspread_client, with_sheet_backoff
import os
from datetime import datetime

# rotation_executor.py — drop-in patch
# Robustly syncs Confirmed=YES tokens from Rotation_Planner → Rotation_Log
# Uses header names (Token, Confirmed, etc.) instead of hardcoded column indexes.

import gspread
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

def _open_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet_url = os.getenv("SHEET_URL")
    if not sheet_url:
        raise ValueError("SHEET_URL not set.")
    return client.open_by_url(sheet_url)

def sync_confirmed_to_rotation_log():
    SHEET_URL = os.getenv("SHEET_URL")
    if not SHEET_URL:
        raise ValueError("SHEET_URL not set.")
    try:
        gc = get_gspread_client()
        sh = gc.open_by_url(SHEET_URL)
        planner_ws = sh.worksheet("Rotation_Planner")
        log_ws = sh.worksheet("Rotation_Log")

        planner = planner_ws.get_all_records()
        log_records = log_ws.get_all_records()
        log_tokens = {str(r.get("Token","")).strip().upper() for r in log_records if r.get("Token")}

        def _append(ws, row):
            return ws.append_row(row, value_input_option="USER_ENTERED")

        to_append = []
        for row in planner:
            token = str(row.get("Token","")).strip().upper()
            confirmed = str(row.get("Confirmed","")).strip().upper()
            if not token or confirmed != "YES" or token in log_tokens:
                continue
            ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            source = row.get("Source") or row.get("Rotation Source") or "Planner"
            score = row.get("Score","")
            sentiment = row.get("Sentiment","")
            mcap = row.get("Market Cap") or row.get("MarketCap","")
            scout = row.get("Scout URL") or row.get("URL","")
            alloc = row.get("Allocation (%)","") or "TBD"
            to_append.append([ts, token, "Active", score, sentiment, mcap, scout, alloc])
            log_tokens.add(token)

        for r in to_append:
            _append(log_ws, r)
            print(f"✅ Synced to Rotation_Log: {r[1]}")
        if not to_append:
            print("ℹ️ No new Confirmed=YES tokens to sync.")
    except Exception as e:
        print(f"❌ sync_confirmed_to_rotation_log error: {e}")


        planner = planner_ws.get_all_records()
        # Build existing token set from Rotation_Log using header name "Token"
        log_records = log_ws.get_all_records()
        log_tokens = {str(r.get("Token","")).strip().upper() for r in log_records if r.get("Token")}

        added = 0
        for row in planner:
            token = str(row.get("Token", "")).strip().upper()
            confirmed = str(row.get("Confirmed", "")).strip().upper()

            if not token or confirmed != "YES" or token in log_tokens:
                continue

            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            source = row.get("Source", "") or row.get("Rotation Source", "") or "Planner"
            score = row.get("Score", "")
            sentiment = row.get("Sentiment", "")
            market_cap = row.get("Market Cap", "") or row.get("MarketCap", "")
            scout_url = row.get("Scout URL", "") or row.get("URL","")
            allocation = row.get("Allocation (%)", "") or "TBD"

            log_ws.append_row(
                [timestamp, token, "Active", score, sentiment, market_cap, scout_url, allocation],
                value_input_option="USER_ENTERED"
            )
            added += 1
            log_tokens.add(token)
            print(f"✅ Synced to Rotation_Log: {token}")

        if added == 0:
            print("ℹ️ No new Confirmed=YES tokens to sync.")
    except Exception as e:
        print(f"❌ sync_confirmed_to_rotation_log error: {e}")
