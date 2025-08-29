# vault_growth_sync.py
import os
from utils import (
    with_sheet_backoff,
    get_ws,
    get_records_cached,   # cache-first list[dict] by sheet name
)
from time import sleep

SHEET_VAULT = "Token_Vault"

@with_sheet_backoff
def _get_vault_rows():
    # Single cached read (default gate + backoff happen inside)
    return get_records_cached(SHEET_VAULT, ttl_s=300)  # 5-minute cache

def run_vault_growth_sync():
    print("📦 Syncing Vault ROI + Memory Stats...")
    try:
        # Touch the worksheet once so gspread auth happens (also gated/backed off)
        _ = get_ws(SHEET_VAULT)

        rows = _get_vault_rows() or []
        if not rows:
            print("ℹ️ No rows in Token_Vault; nothing to sync.")
            return

        # NOTE: This lightweight version only verifies we can read without tripping
        # quota on boot storms. If/when we want to compute & write per-row fields,
        # we’ll add a single batch_update here. For now: read-only = zero write bursts.
        print(f"✅ Vault Growth sync complete. {len(rows)} row(s) scanned, 0 updated.")

    except Exception as e:
        msg = str(e)
        # Graceful degrade on quota/service bounces
        if "429" in msg or "quota" in msg.lower():
            print("⏳ Sheets 429 in vault_growth_sync; backing off softly and skipping this pass.")
            # tiny jitter so concurrent threads desynchronize a bit
            sleep(1.2)
            return
        if "503" in msg or "unavailable" in msg.lower():
            print("🌩️ Sheets 503 (service unavailable) in vault_growth_sync; skipping this pass.")
            return
        print(f"❌ vault_growth_sync error: {e}")
