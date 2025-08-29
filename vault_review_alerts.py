# vault_review_alerts.py
import os, time, random
from datetime import datetime, timezone
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from utils import (
    with_sheet_backoff,
    with_sheets_gate,
    get_records_cached,
    get_ws,
    ws_batch_update,
    send_telegram_message_dedup,   # optional
)

SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
SHEET_URL = os.getenv("SHEET_URL")
STATS_TAB = "Rotation_Stats"

@with_sheet_backoff
def _client():
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", SCOPE)
    return gspread.authorize(creds)

@with_sheets_gate
def run_vault_review_alerts():
    try:
        print("📬 Vault Review Alerts…")
        # small jitter to avoid boot pileup
        time.sleep(random.uniform(0.10, 0.80))

        client = _client()
        sh = client.open_by_url(SHEET_URL)
        stats_ws = sh.worksheet(STATS_TAB)

        # ---- READ ONCE (CACHED) --------------------------------------------
        # Expect columns like: Token, Last Reviewed, Vault Tag, (maybe) Review Due, etc.
        rows = get_records_cached(STATS_TAB, ttl_s=180)  # one cached fetch

        # Compute all decisions in-memory
        updates = []     # for ws_batch_update
        alerts = []      # optional telegram body lines

        # Find headers for writing back if needed
        header = stats_ws.row_values(1)
        def _col_ix(name):
            try:
                return header.index(name) + 1
            except ValueError:
                return None

        last_reviewed_col = _col_ix("Last Reviewed")  # adjust if your column is different
        # If you plan to write another column, add lookup here:
        # review_flag_col = _col_ix("Needs Review")

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        for i, rec in enumerate(rows, start=2):
            token = (rec.get("Token") or "").strip().upper()
            if not token:
                continue

            # Example rule: alert when Vault Tag is "⚠️ Never Vaulted"
            # and Last Reviewed is blank. Tweak to your rules.
            vault_tag = (rec.get("Vault Tag") or "").strip()
            last_rev  = (rec.get("Last Reviewed") or "").strip()

            should_alert = (vault_tag == "⚠️ Never Vaulted") and not last_rev
            if not should_alert:
                continue

            # Queue a write for Last Reviewed (batch later)
            if last_reviewed_col:
                a1 = f"{chr(64+last_reviewed_col)}{i}" if last_reviewed_col <= 26 else None
                if a1 is None:
                    # quick column→A1 converter for columns beyond Z
                    def _a1_col(n):
                        s = ""
                        while n:
                            n, rem = divmod(n - 1, 26)
                            s = chr(65 + rem) + s
                        return s
                    a1 = f"{_a1_col(last_reviewed_col)}{i}"
                updates.append({"range": a1, "values": [[now]]})

            alerts.append(f"• {token}: needs first vault review")

        # ---- WRITE ONCE (BATCH) -------------------------------------------
        if updates:
            ws_batch_update(stats_ws, updates)
            print(f"✅ Vault Review flags updated: {len(updates)} cell(s).")

        # ---- OPTIONAL TELEGRAM SUMMARY ------------------------------------
        if alerts:
            body = "🧰 <b>Vault Review Alerts</b>\n" + "\n".join(alerts)
            # de-duped for 30 min
            send_telegram_message_dedup(body, key="vault_review_alerts", ttl_min=30)

        print("✅ Vault review alerts pass complete.")

    except Exception as e:
        # Let with_sheet_backoff retry most quota cases; if it still bubbles up,
        # fail soft so the main loop continues.
        print(f"❌ Error in run_vault_review_alerts: {e}")
