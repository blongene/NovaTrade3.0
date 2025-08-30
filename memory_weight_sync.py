# memory_weight_sync.py — NT3.0 Phase-1 Polish (batch write)
from utils import (
    get_ws, get_records_cached, ws_batch_update,
    str_or_empty, with_sheet_backoff
)

SRC_TAB = "Rotation_Memory"     # where Wins/Losses live
DST_TAB = "Rotation_Stats"      # where Memory Weight column lives
DST_COL = "Memory Weight"

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def run_memory_weight_sync():
    print("🔁 Syncing Memory Weights...")
    mem_rows  = get_records_cached(SRC_TAB, ttl_s=300) or []
    stats_rows = get_records_cached(DST_TAB, ttl_s=300) or []
    if not mem_rows or not stats_rows:
        print("ℹ️ Missing source or destination rows; skipping.")
        return

    # Map token → weight
    def _to_int(x, default=0):
        try:
            return int(float(str(x).strip()))
        except Exception:
            return default

    weights = {}
    for r in mem_rows:
        t = str_or_empty(r.get("Token")).upper()
        if not t:
            continue
        wins   = _to_int(r.get("Wins"))
        losses = _to_int(r.get("Losses"))
        total  = wins + losses
        weight = round(wins / total, 2) if total > 0 else ""
        weights[t] = weight

    ws = get_ws(DST_TAB)
    header = ws.row_values(1)

    if DST_COL in header:
        col_ix = header.index(DST_COL) + 1
        add_header = False
    else:
        col_ix = len(header) + 1
        add_header = True

    writes = []
    if add_header:
        writes.append({"range": f"{_col_letter(col_ix)}1", "values": [[DST_COL]]})

    for i, r in enumerate(stats_rows, start=2):
        t = str_or_empty(r.get("Token")).upper()
        if not t:
            continue
        new = weights.get(t, "")
        cur = str_or_empty(r.get(DST_COL))
        if new != cur:
            writes.append({"range": f"{_col_letter(col_ix)}{i}", "values": [[new]]})

    if writes:
        ws_batch_update(ws, writes)
        print(f"✅ Memory Weight sync complete. {len(writes)} cell(s) updated.")
    else:
        print("✅ Memory Weight sync complete. 0 changes.")
