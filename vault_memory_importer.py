# vault_memory_importer.py
from typing import List, Dict
from utils import get_ws, get_records_cached, with_sheet_backoff

def _col_letter(c: int) -> str:
    s = ""
    while c:
        c, r = divmod(c - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def run_vault_memory_importer():
    """
    Import per-token memory vault scores (computed elsewhere) into Rotation_Stats!<Memory Vault Score>.
    Expects evaluate_vault_memory() to yield a dict like {"MIND": 1.0, "ABC": 0.6, ...}.
    """
    # Late import to avoid circulars
    from vault_memory_evaluator import evaluate_vault_memory

    try:
        result = evaluate_vault_memory()
        if not isinstance(result, dict) or not result:
            print("ℹ️ Vault memory importer: no evaluator data; skipping.")
            return

        stats_ws = get_ws("Rotation_Stats")
        stats_rows: List[Dict[str, str]] = get_records_cached("Rotation_Stats", ttl_s=120)
        if stats_rows is None:
            stats_rows = []

        # header (single call)
        headers = stats_ws.row_values(1) or []
        try:
            col_idx = [h.strip().lower() for h in headers].index("memory vault score") + 1
        except ValueError:
            print("⚠️ 'Memory Vault Score' column not found in Rotation_Stats; skipping.")
            return

        # Build batch updates, only for rows we have a score for
        batch_body = []
        for i, row in enumerate(stats_rows, start=2):
            token = (row.get("Token") or "").strip().upper()
            if not token:
                continue
            score = result.get(token)
            if score is None:
                continue
            batch_body.append({"range": f"{_col_letter(col_idx)}{i}", "values": [[score]]})

        if batch_body:
            stats_ws.batch_update(batch_body, value_input_option="USER_ENTERED")
            print(f"✅ Vault Memory imported for {len(batch_body)} row(s).")
        else:
            print("ℹ️ Vault memory importer: nothing to update.")
    except Exception as e:
        print(f"❌ Error in run_vault_memory_importer: {e}")
