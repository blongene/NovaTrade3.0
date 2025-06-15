import os
import gspread
from collections import defaultdict
from oauth2client.service_account import ServiceAccountCredentials

def run_rotation_memory():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        stats_ws = sheet.worksheet("Rotation_Stats")
        memory_ws = sheet.worksheet("Rotation_Memory")

        rows = stats_ws.get_all_records()
        memory = defaultdict(lambda: {"wins": 0, "losses": 0})

        for row in rows:
            token = row.get("Token", "").strip().upper()
            if row.get("Decision", "").strip().upper() != "YES":
                continue
            try:
                perf = float(row.get("Performance", ""))
                if perf >= 1.0:
                    memory[token]["wins"] += 1
                else:
                    memory[token]["losses"] += 1
            except:
                continue

        # Wipe rows 2+ but keep headers
        memory_ws.batch_clear(["A2:E1000"])

        new_rows = []
        for token, stats in memory.items():
            wins = stats["wins"]
            losses = stats["losses"]
            total = wins + losses
            win_rate = f"{round((wins / total) * 100, 2)}%" if total > 0 else "0%"
            new_rows.append([token, wins, losses, win_rate, ""])

        if new_rows:
            memory_ws.append_rows(new_rows, value_input_option="USER_ENTERED")

        print("✅ Rotation_Memory updated with win/loss stats.")

    except Exception as e:
        print(f"❌ Error in run_rotation_memory: {e}")
