# vault_memory_importer.py — Phase-1 Polish
# Syncs Vault_Memory_Eval scores → Rotation_Stats.Vault Memory (single batch write)

from utils import (
    get_ws, get_records_cached, ws_batch_update,
    str_or_empty, to_float, with_sheet_backoff
)

EVAL_TAB   = "Vault_Memory_Eval"
TARGET_TAB = "Rotation_Stats"
TARGET_COL_NAME = "Vault Memory"

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def run_vault_memory_importer():
    print("📥 Vault memory importer …")

    eval_rows   = get_records_cached(EVAL_TAB, ttl_s=300) or []
    target_rows = get_records_cached(TARGET_TAB, ttl_s=300) or []

    if not eval_rows:
        print("ℹ️ Vault memory importer: no evaluator data; skipping.")
        return
    if not target_rows:
        print("ℹ️ Rotation_Stats is empty; skipping.")
        return

    # Build token -> score map (string score ok, we store as-is)
    scores = {}
    for r in eval_rows:
        token = str_or_empty(r.get("Token")).upper()
        if not token:
            continue
        s = str_or_empty(r.get("Score"))
        if s:
            scores[token] = s

    ws = get_ws(TARGET_TAB)
    header = ws.row_values(1)

    # Ensure column
    if TARGET_COL_NAME in header:
        col_ix = header.index(TARGET_COL_NAME) + 1
        header_write = False
    else:
        col_ix = len(header) + 1
        header_write = True

    writes = []
    if header_write:
        writes.append({"range": f"{_col_letter(col_ix)}1", "values": [[TARGET_COL_NAME]]})

    # Stage cell updates
    for i, r in enumerate(target_rows, start=2):
        token = str_or_empty(r.get("Token")).upper()
        if not token:
            continue
        new_val = scores.get(token, "")
        cur_val = str_or_empty(r.get(TARGET_COL_NAME))
        if new_val and new_val != cur_val:
            writes.append({"range": f"{_col_letter(col_ix)}{i}", "values": [[new_val]]})

    if writes:
        ws_batch_update(ws, writes)
        print(f"✅ Vault memory importer: {len(writes)} cell(s) updated.")
    else:
        print("✅ Vault memory importer: 0 changes.")
