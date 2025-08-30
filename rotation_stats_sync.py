# rotation_stats_sync.py — NT3.0 Phase-1 Polish (cache-first + single batch)
# Mirrors ROI/decision fields from Rotation_Log → Rotation_Stats without hammering Sheets.

import os
from datetime import datetime
from utils import (
    get_ws, get_records_cached, ws_batch_update,
    str_or_empty, to_float, with_sheet_backoff
)

SRC_TAB = "Rotation_Log"
DST_TAB = "Rotation_Stats"

# Tunables
TTL_READ_S      = int(os.getenv("ROTSTATS_TTL_READ_SEC", "300"))   # cache reads 5m
MAX_UPDATES     = int(os.getenv("ROTSTATS_MAX_UPDATES", "400"))    # cap per run
CREATE_MISSING  = os.getenv("ROTSTATS_CREATE_MISSING", "true").lower() == "true"

NEEDED_HEADERS = ["Token","Initial ROI","Follow-up ROI","Decision","Days Held","Status","Memory Tag","Performance"]

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65+r)+s
    return s

def _normalize_roi(v):
    x = to_float(v, default=None)
    return None if x is None else round(x, 4)

@with_sheet_backoff
def run_rotation_stats_sync():
    print("📊 Syncing Rotation_Stats...")

    src_rows = get_records_cached(SRC_TAB, ttl_s=TTL_READ_S) or []
    if not src_rows:
        print("ℹ️ Rotation_Log empty; skipping.")
        return

    # Build SRC map by token
    src_map = {}
    for r in src_rows:
        t = str_or_empty(r.get("Token")).upper()
        if not t:
            continue
        src_map[t] = {
            "Initial ROI": _normalize_roi(r.get("Initial ROI")),
            "Follow-up ROI": _normalize_roi(r.get("Follow-up ROI")),
            "Decision": str_or_empty(r.get("Decision")).upper(),
            "Days Held": to_float(r.get("Days Held"), 0) or 0,
            "Status": str_or_empty(r.get("Status")),
            # Optional fields present in SRC that we mirror if you track them there:
            "Memory Tag": str_or_empty(r.get("Memory Tag")),
            "Performance": _normalize_roi(r.get("Performance")),
        }

    # Open DST once, ensure headers, build column index
    ws = get_ws(DST_TAB)
    header = ws.row_values(1) or []
    writes = []

    # Ensure headers exist (append any missing at the end)
    col_index = {h: i+1 for i, h in enumerate(header)}
    changed_header = False
    for h in NEEDED_HEADERS:
        if h not in col_index:
            header.append(h)
            col_index[h] = len(header)
            changed_header = True
    if changed_header:
        writes.append({"range": f"A1:{_col_letter(len(header))}1", "values": [header]})

    # Read current DST body cheaply via cached records
    dst_rows = get_records_cached(DST_TAB, ttl_s=TTL_READ_S) or []

    # Map existing rows by Token for update; track which tokens are missing
    dst_token_to_rowidx = {}  # 2-based row index
    for i, r in enumerate(dst_rows, start=2):
        t = str_or_empty(r.get("Token")).upper()
        if t and t not in dst_token_to_rowidx:
            dst_token_to_rowidx[t] = i

    # Assemble updates for existing rows
    updates = 0
    for token, src in src_map.items():
        row_idx = dst_token_to_rowidx.get(token)
        if not row_idx:
            continue
        row_writes = []
        for key in NEEDED_HEADERS:
            if key == "Token":
                continue
            col = col_index.get(key)
            if not col:
                continue
            val = src.get(key)
            # write normalized strings; keep blank if None
            if isinstance(val, float):
                val = f"{val}"
            row_writes.append((col, val if val is not None else ""))

        if row_writes:
            # Coalesce adjacent columns into minimal ranges
            row_writes.sort(key=lambda x: x[0])
            # pack into contiguous A1 ranges
            start = end = None
            block = []
            for col, val in row_writes:
                if start is None:
                    start = end = col
                    block = [val]
                elif col == end + 1:
                    end = col
                    block.append(val)
                else:
                    writes.append({"range": f"{_col_letter(start)}{row_idx}:{_col_letter(end)}{row_idx}",
                                   "values": [block]})
                    start = end = col
                    block = [val]
            if block:
                writes.append({"range": f"{_col_letter(start)}{row_idx}:{_col_letter(end)}{row_idx}",
                               "values": [block]})
            updates += 1
            if updates >= MAX_UPDATES:
                break

    # Optionally append new rows for tokens not present in DST
    if CREATE_MISSING:
        new_rows = []
        for token, src in src_map.items():
            if token in dst_token_to_rowidx:
                continue
            row = [""] * len(header)
            row[col_index["Token"]-1] = token
            for key in NEEDED_HEADERS:
                if key == "Token":
                    continue
                if key in col_index:
                    j = col_index[key]-1
                    v = src.get(key)
                    row[j] = f"{v}" if isinstance(v, float) else (v or "")
            new_rows.append(row)
            if len(new_rows) >= MAX_UPDATES:
                break
        if new_rows:
            writes.append({"range": f"A{len(dst_rows)+2}", "values": new_rows})

    if writes:
        ws_batch_update(ws, writes)
        print(f"✅ Rotation_Stats synced. Rows touched: {updates}, new rows: {len(writes[-1]['values']) if writes and 'values' in writes[-1] and isinstance(writes[-1]['values'], list) else 0}.")
    else:
        print("✅ Rotation_Stats already up to date.")
