# vault_to_stats_sync.py — NovaTrade 3.0 (Phase-1 Polish)
# Sync "Vault Tag" from Token_Vault → Rotation_Stats
# - Cache reads
# - Batch writes
# - Safe parsing via utils

from utils import (
    get_ws,
    get_records_cached,
    ws_batch_update,
    str_or_empty,
    with_sheet_backoff,
)

@with_sheet_backoff
def run_vault_to_stats_sync():
    print("📊 Syncing Vault Tags → Rotation_Stats …")

    try:
        # Cached reads
        vault_rows = get_records_cached("Token_Vault", ttl_s=180)
        stats_rows = get_records_cached("Rotation_Stats", ttl_s=180)
        if not vault_rows or not stats_rows:
            print("⚠️ One or both sheets are empty.")
            return

        vault_dict = {
            str_or_empty(r.get("Token")).upper(): str_or_empty(r.get("Vault Tag"))
            for r in vault_rows if r.get("Token")
        }

        ws = get_ws("Rotation_Stats")
        header = ws.row_values(1)
        vault_col = header.index("Vault Tag") + 1 if "Vault Tag" in header else None

        updates = []
        if vault_col is None:
            # Append new header
            vault_col = len(header) + 1
            updates.append({"range": f"{chr(64+vault_col)}1", "values": [["Vault Tag"]]})

        for i, row in enumerate(stats_rows, start=2):
            token = str_or_empty(row.get("Token")).upper()
            if not token:
                continue
            tag = vault_dict.get(token, "")
            current = str_or_empty(row.get("Vault Tag"))
            if tag and tag != current:
                col_letter = chr(64 + vault_col)
                updates.append({"range": f"{col_letter}{i}", "values": [[tag]]})
                print(f"✅ {token} → {tag}")

        if updates:
            ws_batch_update(ws, updates)
            print(f"🔁 Vault Tag sync complete. {len(updates)} row(s) updated.")
        else:
            print("✅ Vault Tag sync complete. No changes needed.")

    except Exception as e:
        print(f"❌ vault_to_stats_sync error: {e}")
