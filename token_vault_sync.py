# token_vault_sync.py

import gspread
import pandas as pd
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import os

def sync_token_vault():
    print("📦 Syncing Token Vault...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        vault_ws = sheet.worksheet("Token_Vault")
        scout_ws = sheet.worksheet("Scout Decisions")

        vault_df = pd.DataFrame(vault_ws.get_all_records())
        scout_df = pd.DataFrame(scout_ws.get_all_records())

        # Ensure fallback columns exist
        for col in ["Decision", "Last Reviewed", "Source", "Score", "Sentiment", "Market Cap"]:
            if col not in vault_df.columns:
                vault_df[col] = ""

        scout_df["Timestamp"] = pd.to_datetime(scout_df["Timestamp"], errors="coerce")
        scout_latest = scout_df.sort_values("Timestamp").drop_duplicates("Token", keep="last")

        # Sync matching scout info into vault
        for idx, row in vault_df.iterrows():
            token = row.get("Token", "").strip()
            if not token:
                continue

            match = scout_latest[scout_latest["Token"].str.strip() == token]
            if not match.empty:
                latest = match.iloc[0]
                if not row["Decision"]:
                    vault_df.at[idx, "Decision"] = latest.get("Decision", "")
                if not row["Last Reviewed"]:
                    vault_df.at[idx, "Last Reviewed"] = latest["Timestamp"].strftime("%Y-%m-%dT%H:%M:%S")
                if not row["Source"]:
                    vault_df.at[idx, "Source"] = latest.get("Source", "")
                if not row["Score"]:
                    vault_df.at[idx, "Score"] = latest.get("Score", "")
                if not row["Sentiment"]:
                    vault_df.at[idx, "Sentiment"] = latest.get("Sentiment", "")
                if not row["Market Cap"]:
                    vault_df.at[idx, "Market Cap"] = latest.get("Market Cap", "")

        # Push back to sheet
        vault_ws.clear()
        vault_ws.update([vault_df.columns.tolist()] + vault_df.fillna("").astype(str).values.tolist())

        print("✅ Token Vault synced with latest Scout Decisions.")

    except Exception as e:
        print(f"❌ Vault sync error: {e}")
