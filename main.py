from telegram_webhook import telegram_app
from nova_watchdog import run_watchdog
from rotation_signal_engine import scan_rotation_candidates
from roi_milestone_alert import scan_roi_tracking  # ✅ fixed here
from token_vault_sync import sync_token_vault
from presale_scorer import run_presale_scorer

# Call once at boot or wrap in scheduler
run_presale_scorer()

print("📡 Orion Cloud Boot Sequence Initiated")

if __name__ == "__main__":
    # Webhook is already set via Render startup
    print("✅ Webhook armed. Launching modules...")

    print("🔍 Starting Watchdog...")
    run_watchdog()

    print("🔁 Checking for stalled rotation candidates...")
    scan_rotation_candidates()

    print("📈 Checking for ROI milestone follow-ups...")
    scan_roi_tracking()  # ✅ fixed here

    print("📦 Syncing Token Vault with Scout Decisions...")
    sync_token_vault()

    print("🧠 NovaTrade system is live.")
    telegram_app.run(host="0.0.0.0", port=10000)
