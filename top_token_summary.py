import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_message, ping_webhook_debug

MILESTONES = [10, 20, 50, 100, 200, 500]


def run_top_token_summary():
    print("📈 Running Top Token ROI Summary...")

    try:
        # Setup auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        stats_ws = sheet.worksheet("Rotation_Stats")
        rows = stats_ws.get_all_records()
        headers = stats_ws.row_values(1)

        milestone_col = headers.index("Last Alerted") + 1 if "Last Alerted" in headers else len(headers) + 1
        if "Last Alerted" not in headers:
            stats_ws.update_cell(1, milestone_col, "Last Alerted")

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        alerts_sent = 0

        for i, row in enumerate(rows, start=2):
            token = row.get("Token", "").strip().upper()
            roi_str = row.get("Follow-up ROI", "")
            alerted = row.get("Last Alerted", "")

            try:
                roi = float(roi_str)
            except:
                continue

            # Check if we already alerted this exact milestone
            hit_milestones = [m for m in MILESTONES if roi >= m]
            if not hit_milestones:
                continue

            latest = max(hit_milestones)
            if alerted and str(latest) in alerted:
                continue

            # Format and send Telegram
            msg = f"📈 ${token} just hit *{roi:.1f}% ROI* — milestone passed: {latest}%\n"
            if roi >= 100:
                msg += "\n🟢 _Huge Win!_"
"
            elif roi >= 20:
                msg += "\n💡 _Solid breakout_"
"
            else:
                msg += "\n📊 _Early growth_"
"

            send_telegram_message(msg)
            stats_ws.update_cell(i, milestone_col, f"{latest} at {now}")
            alerts_sent += 1
            print(f"✅ Milestone alert sent for {token} → {roi:.1f}%")

        print(f"✅ Top Token Summary complete. {alerts_sent} alert(s) sent.")

    except Exception as e:
        print(f"❌ Error in run_top_token_summary: {e}")
        ping_webhook_debug(f"❌ ROI Summary alert error: {e}")
