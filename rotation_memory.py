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

        memory_ws.clear()
        memory_ws.append_row(["Token", "Wins", "Losses", "Win Rate", "Memory Weight"])

        for token, stats in memory.items():
            wins = stats["wins"]
            losses = stats["losses"]
            total = wins + losses
            win_rate = f"{round((wins / total) * 100, 2)}%" if total > 0 else "0%"
            memory_ws.append_row([token, wins, losses, win_rate, ""])  # Leave Memory Weight blank for now

        print("✅ Rotation_Memory updated with win/loss stats.")

    except Exception as e:
        print(f"❌ Error in run_rotation_memory: {e}")
