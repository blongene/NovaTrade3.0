# memory_score_sync.py — NT3.0 Phase-1 Polish (batch write)
from utils import (
    get_ws, get_records_cached, ws_batch_update,
    str_or_empty, with_sheet_backoff
)

TAB = "Rotation_Stats"
COL_TOTAL = "Total Memory Score"

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def run_memory_score_sync():
    print("🧠 Calculating Total Memory Score...")
    rows = get_records_cached(TAB, ttl_s=300) or []
    if not rows:
        print("ℹ️ Rotation_Stats empty; skipping.")
        return

    ws = get_ws(TAB)
    header = ws.row_values(1)

    # Required/optional columns
    ms_col = header.index("Memory Score") + 1 if "Memory Score" in header else None
    rw_col = header.index("Rebuy Weight") + 1 if "Rebuy Weight" in header else None
    vm_col = header.index("Memory Vault Score") + 1 if "Memory Vault Score" in header else None

    if COL_TOTAL in header:
        tot_col = header.index(COL_TOTAL) + 1
        add_header = False
    else:
        tot_col = len(header) + 1
        add_header = True

    def _to_float(s, default=0.0):
        try:
            return float(str(s).strip())
        except Exception:
            return default

    writes = []
    if add_header:
        writes.append({"range": f"{_col_letter(tot_col)}1", "values": [[COL_TOTAL]]})

    for i, r in enumerate(rows, start=2):
        m = _to_float(r.get("Memory Score")) if ms_col else 0.0
        w = _to_float(r.get("Rebuy Weight")) if rw_col else 0.0
        v = _to_float(r.get("Memory Vault Score")) if vm_col else 0.0
        total = round(m + w + v, 2)

        cur = str_or_empty(r.get(COL_TOTAL))
        new = f"{total:.2f}"
        if new != cur:
            writes.append({"range": f"{_col_letter(tot_col)}{i}", "values": [[new]]})

    if writes:
        ws_batch_update(ws, writes)
        print(f"✅ Total Memory Score sync complete. {len(writes)} cell(s) updated.")
    else:
        print("✅ Total Memory Score sync complete. 0 changes.")
