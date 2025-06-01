def run_milestone_alerts():
    import gspread
    import os
    from oauth2client.service_account import ServiceAccountCredentials

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        rotation_log = sheet.worksheet("Rotation_Log")
        rows = rotation_log.get_all_records()

        for i, row in enumerate(rows):
            try:
                token = row.get("Token", "UNKNOWN")
                days_held = int(row.get("Days Held", 0))

                # Example: alert only at 7, 14, 30 days
                if days_held in [7, 14, 30]:
                    print(f"🚀 {token} has reached {days_held} days.")
                    # You could trigger a ping here

            except Exception as e:
                print(f"❌ Milestone Alert Engine failed for row {i + 2}: {e}")

    except Exception as e:
        print(f"❌ milestone_alerts.py failed to initialize: {e}")
