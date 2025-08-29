# rotation_memory.py — NT3.0 Phase-1 Polish
# Compute simple memory tags and write them in one batch to Rotation_Stats.

from utils import (
    get_ws, get_records_cached, ws_batch_update,
    str_or_empty, to_float, with_sheet_backoff
)

TAB = "Rotation_Stats"

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def run_rotation_memory():
    print("🧠 Rotation Memory Sync …")

    rows = get_records_cached(TAB, ttl_s=240) or []
    if not rows:
        print("ℹ️ Rotation_Stats empty; skipping.")
        return

    ws = get_ws(TAB)
    header = ws.row_values(1)
    try:
        token_col = header.index("Token") + 1
    except ValueError:
        print("⚠️ Missing 'Token' header.")
        return

    # Ensure Memory Tag column
    mem_col = header.index("Memory Tag") + 1 if "Memory Tag" in header else len(header) + 1
    writes = []
    if "Memory Tag" not in header:
        writes.append({"range": f"{_col_letter(mem_col)}1", "values": [["Memory Tag"]]})

    for i, r in enumerate(rows, start=2):
        token = str_or_empty(r.get("Token")).upper()
        if not token:
            continue
        decision = str_or_empty(r.get("Decision")).upper()
        perf = to_float(r.get("Performance"))

        tag = ""
        if decision == "YES":
            if perf is None:
                tag = "yes:unknown"
            elif perf >= 20:
                tag = "yes:strong"
            elif perf >= 0:
                tag = "yes:ok"
            else:
                tag = "yes:weak"
        elif decision == "NO":
            tag = "no"
        else:
            tag = "tbd"

        cur = str_or_empty(r.get("Memory Tag"))
        if tag != cur:
            writes.append({
                "range": f"{_col_letter(mem_col)}{i}",
                "values": [[tag]],
            })

    if writes:
        ws_batch_update(ws, writes)
        print(f"✅ Rotation memory sync: {len(writes)} row(s) updated.")
    else:
        print("✅ Rotation memory sync: 0 changes.")
