# 🚀 Milestone Alert Logic
print("🚀 Checking for milestone ROI alerts...")
milestone_days = [3, 7, 14, 30]
for i, row in enumerate(rows[1:], start=2):
    try:
        token = str(row.get("Token", "")).strip()
        decision = str(row.get("Decision", "")).strip().upper()
        days_held = int(row.get("Days Held", "0") or 0)

        if decision != "YES" or not token or days_held not in milestone_days:
            continue

        # Check if already alerted
        memory_key = f"{token}_milestone"
        if PROMPT_MEMORY.get(memory_key) == days_held:
            continue  # already alerted for this milestone

        # Send milestone alert
        message = f"📍 Milestone Alert: *{token}*\n– Days Held: {days_held}d\n– This token has now reached a {days_held}d milestone.\nWould you like to review or consider rotation?"

        keyboard = {
            "inline_keyboard": [[
                {"text": "🔁 Review", "callback_data": f"YES|{token}"},
                {"text": "❌ Skip", "callback_data": f"SKIP|{token}"}
            ]]
        }

        resp = requests.post(
            f"https://api.telegram.org/bot{os.getenv('BOT_TOKEN')}/sendMessage",
            json={
                "chat_id": os.getenv("CHAT_ID"),
                "text": message,
                "parse_mode": "Markdown",
                "reply_markup": keyboard
            }
        )

        print(f"📬 Milestone alert sent for {token} @ {days_held}d: {resp.text}")
        PROMPT_MEMORY[memory_key] = days_held

    except Exception as e:
        print(f"❌ Milestone Alert Engine failed for row {i}: {e}")
