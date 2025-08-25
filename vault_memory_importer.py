# vault_memory_importer.py (replace run_vault_memory_importer with this)
import os
from utils import get_ws, get_records_cached, _ws_update, with_sheet_backoff
from vault_memory_evaluator import evaluate_vault_memory

@with_sheet_backoff
def run_vault_memory_importer():
    try:
        result = evaluate_vault_memory()
        if not result or not isinstance(result, dict):
            print("ℹ️ Vault memory importer: evaluator returned no data; skipping.")
            return

        # Expecting result like: {"MIND": 1.0, "ABC": 0.6, ...}
        stats_ws = get_ws("Rotation_Stats")
        stats = get_records_cached("Rotation_Stats", ttl_s=120)

        # Find target column index for "Memory Vault Score"
        header_vals = [list(stats_ws.row_values(1))]
        headers = header_vals[0] if header_vals and header_vals[0] else []
        try:
            col_idx = headers.index("Memory Vault Score") + 1
        except ValueError:
            print("⚠️ 'Memory Vault Score' column not found in Rotation_Stats.")
            return

        # Build a single batch update for all rows we find
        updates = []
        for i, row in enumerate(stats, start=2):
            token = (row.get("Token") or "").strip().upper()
            if not token:
                continue
            score = result.get(token)
            if score is None:
                continue
            # range like f"{col_letter}{i}" – use A1 with column index via R1C1
            updates.append({"range": f"R{i}C{col_idx}", "values": [[score]]})

        if updates:
            # Convert RnCm → A1 for batch_update: gspread accepts A1, so map them
            # Simple mapper using ws.cell to get A1 notation: but that’s extra calls.
            # Instead, get column letter quickly:
            def _col_letter(c):
                s = ""
                while c:
                    c, r = divmod(c-1, 26)
                    s = chr(65+r) + s
                return s
            for u in updates:
                # u["range"] is like R10C7; convert to A1 ("G10")
                rc = u["range"]
                r = int(rc.split("R")[1].split("C")[0])
                c = int(rc.split("C")[1])
                u["range"] = f"{_col_letter(c)}{r}"
            stats_ws.batch_update(updates, value_input_option="USER_ENTERED")
            print(f"✅ Vault Memory imported for {len(updates)} row(s).")
        else:
            print("ℹ️ Vault Memory importer: nothing to update.")
    except Exception as e:
        print(f"❌ Error in run_vault_memory_importer: {e}")
