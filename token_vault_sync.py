import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import os
import pandas as pd

# Authenticate with Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("token_vault.json", scope)
client = gspread.authorize(creds)

sheet = client.open_by_url(os.getenv("SHEET_URL"))

vault_ws = sheet.worksheet("Token_Vault")
scout_ws = sheet.worksheet("Scout Decisions")

# Load data into DataFrames
vault_df = pd.DataFrame(vault_ws.get_all_records())
scout_df = pd.DataFrame(scout_ws.get_all_records())

# Add fallback columns if missing
for col in ["Decision", "Last Reviewed", "Source"]:
    if col not in vault_df.columns:
        vault_df[col] = ""

# Clean timestamps and get latest decisions
scout_df["Timestamp"] = pd.to_datetime(scout_df["Timestamp"], errors="coerce")
scout_latest = scout_df.sort_values("Timestamp").drop_duplicates("Token", keep="last")

# Apply sync logic
for idx, row in vault_df.iterrows():
    token = row["Token"]
    match = scout_latest[scout_latest["Token"] == token]
    if not match.empty:
        vault_df.at[idx, "Decision"] = match["Decision"].values[0]
        vault_df.at[idx, "Last Reviewed"] = match["Timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").values[0]
        vault_df.at[idx, "Source"] = match["Source"].values[0]

# Push updated rows back to sheet
vault_ws.clear()
vault_ws.update([vault_df.columns.values.tolist()] + vault_df.values.tolist())

print("✅ Token Vault synced with latest Scout Decisions.")
