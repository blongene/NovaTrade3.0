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
        rows = stats_ws.get_all_records()

        memory = defaultdict(lambda: {"wins": 0, "losses": 0})

        for row in rows:
            token = row.get("Token", "").upper()
            perf = str(row.get("Performance", "")).strip()

            try:
                val = float(perf)
                if val >= 1.0:
                    memory[token]["wins"] += 1
                else:
                    memory[token]["losses"] += 1
            except ValueError:
                continue

        memory_tab = sheet.worksheet("Rotation_Memory")
        memory_tab.clear()
        memory_tab.append_row(["Token", "Wins", "Losses", "Win Rate"])

        for token, result in memory.items():
            wins = result["wins"]
            losses = result["losses"]
            total = wins + losses
            win_rate = round(wins / total * 100, 2) if total > 0 else 0
            memory_tab.append_row([token, wins, losses, f"{win_rate}%"])

        print("✅ Rotation_Memory tab updated.")

    except Exception as e:
        print(f"❌ run_rotation_memory error: {e}")
