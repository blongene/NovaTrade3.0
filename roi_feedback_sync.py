# roi_feedback_sync.py — NT3.0 Render (429-safe, batch-only)
# Syncs latest per-token ROI feedback from ROI_Review_Log → Rotation_Stats.
# - ONE cached read of ROI_Review_Log
# - ONE cached read of Rotation_Stats
# - ONE batched write of changed cells (creates dest column if missing)
# - Safe parsing + jitter + global backoff/gates from utils.py

import os, time, random
from utils import (
    get_values_cached, get_ws, ws_batch_update, with_sheet_backoff,
    str_or_empty
)

# ---------- Config (override in Render ENV as needed) -------------------------
SRC_TAB             = os.getenv("ROI_FB_SRC_TAB", "ROI_Review_Log")
DST_TAB             = os.getenv("ROT_STATS_TAB",   "Rotation_Stats")

SRC_TOKEN_COL       = os.getenv("ROI_FB_TOKEN_COL",     "Token")
SRC_DECISION_COL    = os.getenv("ROI_FB_DECISION_COL",  "Decision")
SRC_TIMESTAMP_COL   = os.getenv("ROI_FB_TIMESTAMP_COL", "Timestamp")  # optional

DST_TOKEN_COL       = os.getenv("ROT_STATS_TOKEN_COL",  "Token")
DST_FEEDBACK_COL    = os.getenv("ROT_STATS_FEEDBACK_COL", "User Response")  # will be created if missing

# Behavior
TTL_SRC_S           = int(os.getenv("ROI_FB_TTL_SRC_SEC",   "300"))
TTL_DST_S           = int(os.getenv("ROI_FB_TTL_DST_SEC",   "300"))
JIT_MIN_S           = float(os.getenv("ROI_FB_JITTER_MIN_S","0.35"))
JIT_MAX_S           = float(os.getenv("ROI_FB_JITTER_MAX_S","1.10"))
MAX_WRITES          = int(os.getenv("ROI_FB_MAX_WRITES",    "800"))

# Optional normalization of decision strings
# e.g., "Yes", "YES!", "y", "approve" -> "YES"
NORM_ENABLE         = os.getenv("ROI_FB_NORMALIZE", "true").lower() == "true"

YES_TOKENS          = [s.strip().upper() for s in os.getenv("ROI_FB_YES_TOKENS", "YES,Y,APPROVE,BUY").split(",")]
NO_TOKENS           = [s.strip().upper() for s in os.getenv("ROI_FB_NO_TOKENS",  "NO,N,DENY,REJECT,SELL").split(",")]
HOLD_TOKENS         = [s.strip().upper() for s in os.getenv("ROI_FB_HOLD_TOKENS","HOLD,WAIT,LATER").split(",")]

# -----------------------------------------------------------------------------

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def _hmap(header):
    return {str_or_empty(h): i for i, h in enumerate(header)}

def _norm_decision(s: str) -> str:
    if not NORM_ENABLE:
        return s.strip()
    t = str_or_empty(s).upper().strip()
    if any(tk and tk in t for tk in YES_TOKENS):
        return "YES"
    if any(tk and tk in t for tk in NO_TOKENS):
        return "NO"
    if any(tk and tk in t for tk in HOLD_TOKENS):
        return "HOLD"
    # fallback: keep concise original (trimmed) if not matched
    return t or ""

@with_sheet_backoff
def run_roi_feedback_sync():
    print("🔄 Syncing ROI feedback from ROI_Review_Log → Rotation_Stats ...")
    time.sleep(random.uniform(JIT_MIN_S, JIT_MAX_S))

    # -------- Source read (ONE cached call)
    src_vals = get_values_cached(SRC_TAB, ttl_s=TTL_SRC_S) or []
    if not src_vals or not src_vals[0]:
        print(f"ℹ️ {SRC_TAB} empty; nothing to sync.")
        return
    sh = _hmap(src_vals[0])
    si_tok = sh.get(SRC_TOKEN_COL)
    si_dec = sh.get(SRC_DECISION_COL)
    si_ts  = sh.get(SRC_TIMESTAMP_COL) if SRC_TIMESTAMP_COL in sh else None

    miss = [n for n,i in [(SRC_TOKEN_COL,si_tok),(SRC_DECISION_COL,si_dec)] if i is None]
    if miss:
        print(f"⚠️ {SRC_TAB} missing columns: {', '.join(miss)}; skipping.")
        return

    # Build latest decision per token (use timestamp if available, else last row wins)
    latest = {}   # token -> (ts_index, decision)
    for r_idx, row in enumerate(src_vals[1:], start=2):
        token = str_or_empty(row[si_tok] if si_tok < len(row) else "").upper()
        if not token:
            continue
        decision_raw = row[si_dec] if (si_dec < len(row)) else ""
        decision = _norm_decision(decision_raw)
        if not decision:
            continue

        # "timestamp index" chooses recency: either sheet timestamp string or row index
        if si_ts is not None and si_ts < len(row):
            ts_key = str_or_empty(row[si_ts])  # keep lexicographically comparable text
        else:
            ts_key = f"ROW{r_idx:09d}"

        prev = latest.get(token)
        if prev is None or ts_key >= prev[0]:
            latest[token] = (ts_key, decision)

    if not latest:
        print("ℹ️ No decisions found to sync.")
        return

    # -------- Destination read (ONE cached call)
    dst_vals = get_values_cached(DST_TAB, ttl_s=TTL_DST_S) or []
    if not dst_vals or not dst_vals[0]:
        print(f"ℹ️ {DST_TAB} empty; skipping.")
        return
    dh = _hmap(dst_vals[0])
    di_tok = dh.get(DST_TOKEN_COL)

    # Ensure destination feedback column exists (header write if needed)
    header_writes = []
    if DST_FEEDBACK_COL in dh:
        di_fb = dh[DST_FEEDBACK_COL]
    else:
        dst_vals[0].append(DST_FEEDBACK_COL)
        di_fb = len(dst_vals[0]) - 1
        header_writes.append({"range": f"{_col_letter(di_fb+1)}1", "values": [[DST_FEEDBACK_COL]]})

    # -------- Compute row diffs (avoid per-row writes)
    writes, touched = [], 0
    for r_idx, row in enumerate(dst_vals[1:], start=2):
        token = str_or_empty(row[di_tok] if (di_tok is not None and di_tok < len(row)) else "").upper()
        if not token:
            continue
        pair = latest.get(token)
        if not pair:
            continue
        new_val = pair[1]  # normalized decision
        cur_val = str_or_empty(row[di_fb] if di_fb < len(row) else "")
        if new_val == cur_val:
            continue

        writes.append({"range": f"{_col_letter(di_fb+1)}{r_idx}", "values": [[new_val]]})
        touched += 1
        if touched >= MAX_WRITES:
            break

    if not header_writes and not writes:
        print("✅ ROI feedback sync: no changes needed.")
        return

    # -------- Single batched write
    ws = get_ws(DST_TAB)
    payload = []
    if header_writes: payload.extend(header_writes)
    if writes:        payload.extend(writes)
    ws_batch_update(ws, payload)

    print(f"✅ ROI feedback sync: wrote {touched} cell(s){' + header' if header_writes else ''}.")
