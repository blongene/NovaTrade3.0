# token_vault_sync.py

import pandas as pd
from datetime import datetime, timezone
from utils import (
    get_ws,
    safe_get_all_records,
    ws_batch_update,
    with_sheet_backoff,
    str_or_empty,
)

SHEET_VAULT = "Token_Vault"
SHEET_SCOUT = "Scout Decisions"

def _utc_ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

def _col_letter(idx_1b: int) -> str:
    n = idx_1b
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def sync_token_vault():
    print("📦 Syncing Token Vault...")
    try:
        vault_ws = get_ws(SHEET_VAULT)
        scout_ws = get_ws(SHEET_SCOUT)

        # Read once, cached
        vault_rows = safe_get_all_records(vault_ws, ttl_s=120)
        scout_rows = safe_get_all_records(scout_ws, ttl_s=120)

        # Ensure Vault headers
        headers = vault_ws.row_values(1) or []
        hidx = {h: i + 1 for i, h in enumerate(headers)}
        needed = ["Decision", "Last Reviewed", "Source", "Score", "Sentiment", "Market Cap"]
        header_changed = False
        for col in needed:
            if col not in hidx:
                headers.append(col)
                hidx[col] = len(headers)
                header_changed = True
        if header_changed:
            vault_ws.update("A1", [headers])

        # Build DataFrames
        vdf = pd.DataFrame(vault_rows)
        sdf = pd.DataFrame(scout_rows)

        # Defensive: ensure required columns exist
        for c in ["Token"] + needed:
            if c not in vdf.columns:
                vdf[c] = ""
        for c in ["Timestamp", "Token", "Decision", "Source", "Score", "Sentiment", "Market Cap"]:
            if c not in sdf.columns:
                sdf[c] = ""

        # Latest scout decision per token
        if "Timestamp" in sdf.columns:
            sdf["Timestamp"] = pd.to_datetime(sdf["Timestamp"], errors="coerce")
            sdf = sdf.sort_values("Timestamp").drop_duplicates("Token", keep="last")

        # Map token -> latest scout row (as dict)
        def _tok(x): return str_or_empty(x).upper()
        latest = { _tok(r.get("Token")): r for r in sdf.to_dict(orient="records") }

        # Prepare batched updates (only for changed cells)
        updates = []
        for i, row in enumerate(vdf.to_dict(orient="records"), start=2):
            t = _tok(row.get("Token"))
            if not t:
                continue
            srow = latest.get(t)
            if not srow:
                continue

            # Decide what to fill if empty
            pairs = {
                "Decision": str_or_empty(srow.get("Decision")),
                "Source": str_or_empty(srow.get("Source")),
                "Score": str_or_empty(srow.get("Score")),
                "Sentiment": str_or_empty(srow.get("Sentiment")),
                "Market Cap": str_or_empty(srow.get("Market Cap")),
            }
            # Last Reviewed from Timestamp
            ts = srow.get("Timestamp")
            ts_str = ""
            if isinstance(ts, pd.Timestamp) and pd.notnull(ts):
                ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S")
            pairs["Last Reviewed"] = ts_str

            for col, new_val in pairs.items():
                cur = str_or_empty(row.get(col))
                if not cur and new_val:
                    a1 = f"{SHEET_VAULT}!{_col_letter(hidx[col])}{i}"
                    updates.append({"range": a1, "values": [[new_val]]})

        if updates:
            ws_batch_update(vault_ws, updates)
            print(f"✅ Token Vault synced with latest Scout Decisions. {len(updates)} cell(s) updated (batched).")
        else:
            print("ℹ️ Token Vault already up to date.")

    except Exception as e:
        print(f"❌ Vault sync error: {e}")
