# rotation_memory_scoring.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run_memory_scoring():
    print("🧠 Calculating weighted Memory Scores...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        stats_ws = sheet.worksheet("Rotation_Stats")

        rows = stats_ws.get_all_records()
        headers = stats_ws.row_values(1)

        tag_col = headers.index("Memory Tag") + 1
        vote_col = headers.index("Re-Vote") + 1 if "Re-Vote" in headers else None
        score_col = headers.index("Memory Score") + 1 if "Memory Score" in headers else len(headers) + 1

        if "Memory Score" not in headers:
            stats_ws.update_cell(1, score_col, "Memory Score")

        for i, row in enumerate(rows, start=2):
            tag = str(row.get("Memory Tag", "")).strip()
            vote = str(row.get("Re-Vote", "")).strip().upper()
            score = 0

            # Score from memory tag
            if "Big Win" in tag:
                score += 3
            elif "Small Win" in tag:
                score += 2
            elif "Break-Even" in tag:
                score += 1
            elif "Loss" in tag:
                score -= 1
            elif "Big Loss" in tag:
                score -= 2

            # Score from user re-vote
            if vote == "YES":
                score += 1
            elif vote == "NO":
                score -= 2

            stats_ws.update_cell(i, score_col, score)
            print(f"🔢 Token Row {i}: Memory Score = {score}")

        print("✅ Memory Scoring complete.")

    except Exception as e:
        print(f"❌ Error in run_memory_scoring: {e}")
