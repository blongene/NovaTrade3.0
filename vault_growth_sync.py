# vault_growth_sync.py — NovaTrade 3.0 (Phase-1 Polish)
# Read-only scan of Token_Vault; zero per-cell writes.

from time import sleep
from utils import (
    get_ws,
    get_records_cached,
    with_sheet_backoff,
)

SHEET_VAULT = "Token_Vault"

@with_sheet_backoff
def run_vault_growth_sync():
    print("📦 Syncing Vault ROI + Memory Stats …")
    try:
        rows = get_records_cached(SHEET_VAULT, ttl_s=300) or []
        if not rows:
            print("ℹ️ Token_Vault empty; nothing to sync.")
            return

        # No writes yet — just confirming read access is stable.
        print(f"✅ Vault Growth sync complete. {len(rows)} row(s) scanned, 0 updated.")

    except Exception as e:
        msg = str(e).lower()
        if "429" in msg or "quota" in msg:
            print("⏳ Sheets 429 in vault_growth_sync; backing off softly.")
            sleep(1.2)
            return
        if "503" in msg or "unavailable" in msg:
            print("🌩️ Sheets 503 in vault_growth_sync; skipping this pass.")
            return
        print(f"❌ vault_growth_sync error: {e}")
