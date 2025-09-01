# top_token_summary.py — quota-safe + utils-only
import time
from utils import (
    info, warn,
    get_values_cached, ws_update, get_ws_cached,
    safe_len, str_or_empty, to_float, with_sheets_gate
)

TAB_STATS   = "Rotation_Stats"
TAB_SUMMARY = "Top_Token_Summary"
TTL_S       = 300  # cache for reads

@with_sheets_gate("read", tokens=2)
def _read_stats():
    vals = get_values_cached(TAB_STATS, ttl_s=TTL_S) or []
    if not vals: return [], {}
    header = vals[0]; rows = vals[1:]
    h = {h:i for i,h in enumerate(header)}
    return rows, h

def _col(h, name): return h.get(name)

def run_top_token_summary():
    info("▶ Top token summary")
    try:
        rows, h = _read_stats()
        if not rows:
            warn("Top summary: Rotation_Stats empty; skipping.")
            return

        need = ["Token", "Follow-up ROI", "Memory Vault Score"]
        missing = [c for c in need if c not in h]
        if missing:
            warn(f"Top summary: missing headers: {', '.join(missing)}; skipping.")
            return

        i_tok = _col(h, "Token")
        i_roi = _col(h, "Follow-up ROI") if "Follow-up ROI" in h else _col(h, "Follow-up ROI (%)")
        i_mem = _col(h, "Memory Vault Score")

        scored = []
        for r in rows:
            t = str_or_empty(r[i_tok] if i_tok is not None and i_tok < safe_len(r) else "").upper()
            if not t: continue
            roi = to_float(r[i_roi] if i_roi is not None and i_roi < safe_len(r) else "", default=0.0) or 0.0
            mem = to_float(r[i_mem] if i_mem is not None and i_mem < safe_len(r) else "", default=0.0) or 0.0
            scored.append((t, roi, mem))

        # rank by memory then ROI
        scored.sort(key=lambda x: (-x[2], -x[1]))
        top = scored[:10]

        # Ensure summary sheet exists and write simple list to A1:C
        ws = get_ws_cached(TAB_SUMMARY, ttl_s=60)
        out = [["Token","ROI %","Memory Score"]] + [[t, f"{roi:.2f}", f"{mem:.2f}"] for t,roi,mem in top]
        ws_update(ws, "A1", out)
        info("✅ Top token summary updated.")
    except Exception as e:
        msg = str(e).lower()
        if "429" in msg or "quota" in msg:
            warn("Top token summary: 429; will retry on next schedule.")
            time.sleep(1.0)
            return
        warn(f"Top token summary error: {e}")
