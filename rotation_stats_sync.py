# rotation_stats_sync.py — Phase-6 Safe
import os
from utils import (
    get_ws_cached, get_records_cached, ws_batch_update,
    str_or_empty, to_float, invalidate_tab, warn
)
from datetime import datetime

SRC_TAB, DST_TAB = "Rotation_Log", "Rotation_Stats"
TTL_READ_S = int(os.getenv("ROTSTATS_TTL_READ_SEC", "300"))
MAX_UPDATES = int(os.getenv("ROTSTATS_MAX_UPDATES", "400"))
CREATE_MISSING = os.getenv("ROTSTATS_CREATE_MISSING", "true").lower() == "true"
NEEDED_HEADERS = ["Token","Initial ROI","Follow-up ROI","Decision","Days Held","Status","Memory Tag","Performance"]

def _col_letter(n):
    s=""
    while n: n,r=divmod(n-1,26); s=chr(65+r)+s
    return s

def _normalize_roi(v):
    x = to_float(v, None)
    return None if x is None else round(x,4)

def run_rotation_stats_sync():
    try:
        print("📊 Syncing Rotation_Stats…")
        src_rows = get_records_cached(SRC_TAB, ttl_s=TTL_READ_S) or []
        if not src_rows: return print("ℹ️ Rotation_Log empty; skipping.")

        src_map = {}
        for r in src_rows:
            t = str_or_empty(r.get("Token")).upper()
            if not t: continue
            src_map[t] = {
                "Initial ROI": _normalize_roi(r.get("Initial ROI")),
                "Follow-up ROI": _normalize_roi(r.get("Follow-up ROI")),
                "Decision": str_or_empty(r.get("Decision")).upper(),
                "Days Held": to_float(r.get("Days Held"),0) or 0,
                "Status": str_or_empty(r.get("Status")),
                "Memory Tag": str_or_empty(r.get("Memory Tag")),
                "Performance": _normalize_roi(r.get("Performance")),
            }

        ws = get_ws_cached(DST_TAB, ttl_s=60)
        header = ws.row_values(1) or []
        col_index = {h:i+1 for i,h in enumerate(header)}
        changed=False
        for h in NEEDED_HEADERS:
            if h not in col_index:
                header.append(h); col_index[h]=len(header); changed=True
        writes=[]
        if changed:
            writes.append({"range": f"A1:{_col_letter(len(header))}1", "values": [header]})

        dst_rows = get_records_cached(DST_TAB, ttl_s=TTL_READ_S) or []
        dst_idx = {str_or_empty(r.get("Token")).upper(): i for i,r in enumerate(dst_rows, start=2)}

        for token, src in src_map.items():
            row_idx = dst_idx.get(token)
            if not row_idx: continue
            row_writes=[]
            for key in NEEDED_HEADERS:
                if key=="Token": continue
                col = col_index.get(key)
                if not col: continue
                v = src.get(key)
                if isinstance(v,float): v=f"{v}"
                row_writes.append((col, v or ""))
            row_writes.sort(key=lambda x:x[0])
            if row_writes:
                start,end=row_writes[0][0],row_writes[-1][0]
                vals=[v for _,v in row_writes]
                writes.append({"range": f"{_col_letter(start)}{row_idx}:{_col_letter(end)}{row_idx}","values":[vals]})
            if len(writes)>=MAX_UPDATES: break

        if CREATE_MISSING:
            new=[]
            for token,src in src_map.items():
                if token in dst_idx: continue
                row=[""]*len(header); row[col_index["Token"]-1]=token
                for k in NEEDED_HEADERS:
                    if k=="Token": continue
                    if k in col_index:
                        j=col_index[k]-1; v=src.get(k)
                        row[j]=f"{v}" if isinstance(v,float) else (v or "")
                new.append(row)
                if len(new)>=MAX_UPDATES: break
            if new:
                writes.append({"range": f"A{len(dst_rows)+2}","values":new})

        if writes:
            ws_batch_update(ws,writes)
            invalidate_tab(DST_TAB)
            print(f"✅ Rotation_Stats synced ({len(writes)} writes).")
        else:
            print("✅ Rotation_Stats already up to date.")
    except Exception as e:
        warn(f"rotation_stats_sync: {e}")
