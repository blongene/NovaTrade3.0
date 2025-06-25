import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

def run_portfolio_weight_adjuster():
    print("🧠 Adjusting Portfolio Target Weights...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        memory_ws = sheet.worksheet("Rotation_Memory")
        stats_ws = sheet.worksheet("Rotation_Stats")
        log_ws = sheet.worksheet("Rotation_Log")
        targets_ws = sheet.worksheet("Portfolio_Targets")

        memory_data = {r["Token"].strip().upper(): float(str(r["Win Rate"]).replace("%", "").strip() or 0)
                       for r in memory_ws.get_all_records() if r.get("Token")}
        stats_data = stats_ws.get_all_records()
        log_data = {r["Token"].strip().upper(): {
                        "Staking Yield": r.get("Staking Yield", "").replace("%", ""),
                        "Days Held": r.get("Days Held", "0")
                    } for r in log_ws.get_all_records() if r.get("Token")}

        updated_rows = []

        rows = targets_ws.get_all_records()
        for i, row in enumerate(rows, start=2):
            token = str(row.get("Token", "")).strip().upper()
            if not token:
                continue

            win_rate = memory_data.get(token, 0)
            roi_entries = [float(r.get("Performance", 0)) for r in stats_data if r.get("Token", "").strip().upper() == token and str(r.get("Performance", "")).replace('.', '', 1).isdigit()]
            avg_roi = sum(roi_entries) / len(roi_entries) if roi_entries else 0
            staking = float(log_data.get(token, {}).get("Staking Yield", "0") or 0)
            days_held = int(log_data.get(token, {}).get("Days Held", "0") or 0)

            # Weighting logic
            weighted_score = (
                (win_rate * 0.4) +
                (avg_roi * 0.3) +
                (staking * 0.2) +
                (days_held * 0.1)
            )

            try:
                target_cell = f"G{i}"  # Assuming 'Suggested Target %' is column G
                targets_ws.update_acell(target_cell, round(weighted_score, 2))
                updated_rows.append(f"{token} → {round(weighted_score, 2)}%")
            except Exception as update_err:
                print(f"⚠️ Update failed for {token}: {update_err}")

        print(f"✅ Suggested weights updated for {len(updated_rows)} tokens")
        for u in updated_rows:
            print(f"   – {u}")

    except Exception as e:
        print(f"❌ Portfolio Weight Adjuster error: {e}")
