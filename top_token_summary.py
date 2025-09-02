# top_token_summary.py — NT3.0: quota-safe + header-robust + no-noise
import os, time, random
from datetime import datetime

from utils import (
    get_values_cached, ws_update, with_sheet_backoff,
    warn, info, str_or_empty, to_float, sheets_gate
)

SRC_TAB   = os.getenv("TOP_TOKEN_SRC_TAB", "Rotation_Stats")
DEST_TAB  = os.getenv("TOP_TOKEN_DEST_TAB", "Top_Token_Summary")
READ_TTL  = int(os.getenv("TOP_TOKEN_READ_TTL_SEC", "300"))    # cache reads 5m
MAX_ROWS  = int(os.getenv("TOP_TOKEN_MAX_ROWS", "5000"))
TOP_K     = int(os.getenv("TOP_TOKEN_TOP_K", "25"))

JIT_MIN_S = float(os.getenv("TOP_TOKEN_JITTER_MIN_S", "0.2"))
JIT_MAX_S = float(os.getenv("TOP_TOKEN_JITTER_MAX_S", "0.8"))

NEEDED_COLS = [
    "Token",                # required
    "Memory Tag",           # optional, used for sorting / display
    "Follow-up ROI",        # try both variants
    "Follow-up ROI (%)",
    "Memory Vault Score",   # optional
    "Days Held",            # optional
]

def _col_index(header, name):
    try:
        return header.index(name)
    except ValueError:
        return None

def _pick_roi(row, i_roi1, i_roi2):
    v1 = to_float(row[i_roi1]) if i_roi1 is not None and i_roi1 < len(row) else None
    v2 = to_float(row[i_roi2]) if i_roi2 is not None and i_roi2 < len(row) else None
    return v1 if v1 is not None else (v2 if v2 is not None else None)

@with_sheet_backoff
def _write_table(ws, a1, rows_2d):
    ws_update(ws, a1, rows_2d)

@with_sheet_backoff
def _open_ws(title):
    # Use utils’ cached open to stay under quota
    from utils import get_ws_cached
    return get_ws_cached(title, ttl_s=60)

def run_top_token_summary():
    """Builds a compact Top_Token_Summary sheet from Rotation_Stats with *one* read and *one* write."""
    try:
        time.sleep(random.uniform(JIT_MIN_S, JIT_MAX_S))
        # One read for the whole table (cached)
        values = get_values_cached(SRC_TAB, ttl_s=READ_TTL) or []
        if not values:
            warn(f"Top_Token_Summary: {SRC_TAB} empty; skipping.")
            return

        header = values[0]
        rows   = values[1:MAX_ROWS+1]

        i_token  = _col_index(header, "Token")
        i_memtag = _col_index(header, "Memory Tag")
        i_roi1   = _col_index(header, "Follow-up ROI")
        i_roi2   = _col_index(header, "Follow-up ROI (%)")
        i_mv     = _col_index(header, "Memory Vault Score")
        i_days   = _col_index(header, "Days Held")

        if i_token is None:
            warn("Top_Token_Summary: required column 'Token' missing; skipping.")
            return

        # Build scored view (len-safe)
        items = []
        for r in rows:
            token = str_or_empty(r[i_token] if i_token < len(r) else "").upper()
            if not token:
                continue
            roi   = _pick_roi(r, i_roi1, i_roi2)
            mv    = to_float(r[i_mv] if (i_mv is not None and i_mv < len(r)) else "")
            tag   = str_or_empty(r[i_memtag] if (i_memtag is not None and i_memtag < len(r)) else "")
            days  = to_float(r[i_days] if (i_days is not None and i_days < len(r)) else "")
            # score preference: higher ROI, then higher memory score
            score = ((roi or 0.0), (mv or 0.0))
            items.append((score, token, roi, mv, tag, days))

        # Sort by ROI desc, then MV desc
        items.sort(key=lambda x: (x[0][0], x[0][1]), reverse=True)
        top = items[:TOP_K]

        out_header = ["Token", "Follow-up ROI (%)", "Memory Vault Score", "Memory Tag", "Days Held", "Updated At"]
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + "Z"
        out_rows = [out_header]
        for _, token, roi, mv, tag, days in top:
            out_rows.append([
                token,
                f"{(roi or 0.0):.2f}",
                f"{(mv or 0.0):.2f}",
                tag or "",
                f"{(days or 0):.0f}",
                now
            ])

        # Single write (guarded by sheets gate = pre-consume write token)
        with sheets_gate("write", tokens=1):
            ws = _open_ws(DEST_TAB)
            _write_table(ws, "A1", out_rows)

        info(f"Top_Token_Summary: wrote {len(out_rows)-1} rows.")
    except Exception as e:
        warn(f"Top token summary error: {e}")
