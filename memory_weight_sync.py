# === memory_weight_sync.py (patched to ensure write to Rotation_Memory) ===

def run_memory_weight_sync():
    print("🔁 Syncing Memory Weights...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        memory_ws = sheet.worksheet("Rotation_Memory")
        headers = memory_ws.row_values(1)
        data = memory_ws.get_all_records()

        weight_col = headers.index("Memory Weight") + 1 if "Memory Weight" in headers else len(headers) + 1
        if "Memory Weight" not in headers:
            memory_ws.update_cell(1, weight_col, "Memory Weight")

        for i, row in enumerate(data, start=2):
            wins = int(row.get("Wins", 0))
            losses = int(row.get("Losses", 0))
            total = wins + losses
            weight = round(wins / total, 2) if total > 0 else ""
            memory_ws.update_cell(i, weight_col, weight)
            print(f"🧠 {row.get('Token')} → Memory Weight = {weight}")

        print("✅ Memory Weight sync complete.")

    except Exception as e:
        print(f"❌ Error in run_memory_weight_sync: {e}")
