# rebuy_roi_tracker.py — NT3.0 Render (429-safe, batch-only)
# Aggregates per-token "rebuy" performance from Rotation_Log and writes
# Rebuy Count / Win% / Avg ROI% into Rotation_Stats in a single batch.

import os, time, random
from utils import (
    get_values_cached, get_ws, ws_batch_update, with_sheet_backoff,
    str_or_empty, to_float,
)

# ---------------- Env config (override in Render) ----------------
LOG_TAB          = os.getenv("REBUY_LOG_TAB", "Rotation_Log")
STATS_TAB        = os.getenv("ROT_STATS_TAB", "Rotation_Stats")

# Source columns (Rotation_Log)
LOG_TOKEN_COL    = os.getenv("REBUY_LOG_TOKEN_COL", "Token")
LOG_ROI_COLS     = [s.strip() for s in os.getenv("REBUY_LOG_ROI_COLS", "ROI %,ROI").split(",")]
LOG_DECISION_COL = os.getenv("REBUY_LOG_DECISION_COL", "Decision")
LOG_STATUS_COL   = os.getenv("REBUY_LOG_STATUS_COL", "Status")

# Which rows count as a "rebuy" record?
# We accept any row whose 'Decision' contains one of these tokens (case-insensitive),
# or whose 'Status' contains one of these tokens.
REBUY_DECISION_TOKENS = [s.strip().upper() for s in os.getenv("REBUY_DECISION_TOKENS", "REBUY,BUYBACK").split(",")]
REBUY_STATUS_TOKENS   = [s.strip().upper() for s in os.getenv("REBUY_STATUS_TOKENS", "REBUY").split(",")]

# Destination columns (Rotation_Stats) — created if missing
STATS_TOKEN_COL  = os.getenv("ROT_STATS_TOKEN_COL", "Token")
STATS_REBUY_CT   = os.getenv("ROT_STATS_REBUY_CT_COL", "Rebuy Count")
STATS_REBUY_WIN  = os.getenv("ROT_STATS_REBUY_WIN_COL", "Rebuy Win%")
STATS_REBUY_AVG  = os.getenv("ROT_STATS_REBUY_AVG_COL", "Rebuy Avg ROI%")

# Behavior
TTL_LOG_S        = int(os.getenv("REBUY_TTL_LOG_SEC",  "300"))
TTL_STATS_S      = int(os.getenv("REBUY_TTL_STATS_SEC","300"))
JIT_MIN_S        = float(os.getenv("REBUY_JITTER_MIN_S","0.35"))
JIT_MAX_S        = float(os.getenv("REBUY_JITTER_MAX_S","1.1"))
MAX_WRITES       = int(os.getenv("REBUY_MAX_WRITES",   "800"))

# -----------------------------------------------------------------

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def _hmap(header):
    return {str_or_empty(h): i for i, h in enumerate(header)}

def _pick_first_index(header, names):
    for n in names:
        i = header.get(n)
        if i is not None:
            return i
    return None

def _is_rebuy(dec_str: str, status_str: str) -> bool:
    d = str_or_empty(dec_str).upper()
    s = str_or_empty(status_str).upper()
    if any(tok and tok in d for tok in REBUY_DECISION_TOKENS):
        return True
    if any(tok and tok in s for tok in REBUY_STATUS_TOKENS):
        return True
    return False

@with_sheet_backoff
def run_rebuy_roi_tracker():
    print("🔁 Syncing Rebuy ROI → Rotation_Stats...")
    time.sleep(random.uniform(JIT_MIN_S, JIT_MAX_S))

    # -------- ONE cached read of Rotation_Log
    log_vals = get_values_cached(LOG_TAB, ttl_s=TTL_LOG_S) or []
    if not log_vals or not log_vals[0]:
        print(f"ℹ️ {LOG_TAB} empty; nothing to aggregate.")
        return
    lh = _hmap(log_vals[0])
    li_token   = lh.get(LOG_TOKEN_COL)
    li_dec     = lh.get(LOG_DECISION_COL)
    li_status  = lh.get(LOG_STATUS_COL)
    li_roi     = _pick_first_index(lh, LOG_ROI_COLS)

    miss = [n for n,i in [(LOG_TOKEN_COL,li_token),(LOG_DECISION_COL,li_dec),(LOG_STATUS_COL,li_status)] if i is None]
    if li_roi is None:
        miss.append(f"one of {LOG_ROI_COLS}")
    if miss:
        print(f"⚠️ {LOG_TAB} missing columns: {', '.join(miss)}; skipping.")
        return

    # Aggregate per token
    agg = {}  # token -> dict(sum, count, wins)
    for row in log_vals[1:]:
        token = str_or_empty(row[li_token] if li_token < len(row) else "").upper()
        if not token:
            continue
        dec = row[li_dec] if (li_dec is not None and li_dec < len(row)) else ""
        st  = row[li_status] if (li_status is not None and li_status < len(row)) else ""
        if not _is_rebuy(dec, st):
            continue
        roi = to_float(row[li_roi] if li_roi < len(row) else "", default=None)
        if roi is None:
            continue
        d = agg.setdefault(token, {"sum":0.0, "cnt":0, "win":0})
        d["sum"] += roi
        d["cnt"] += 1
        if roi > 0:
            d["win"] += 1

    if not agg:
        print("ℹ️ No rebuy rows to aggregate.")
        return

    # -------- ONE cached read of Rotation_Stats
    stats_vals = get_values_cached(STATS_TAB, ttl_s=TTL_STATS_S) or []
    if not stats_vals or not stats_vals[0]:
        print(f"ℹ️ {STATS_TAB} empty; skipping.")
        return
    sh = _hmap(stats_vals[0])
    si_token = sh.get(STATS_TOKEN_COL)

    # Ensure destination columns exist (header-only write later)
    header_writes = []
    def ensure_col(name):
        nonlocal stats_vals, sh, header_writes
        if name in sh:
            return sh[name]
        stats_vals[0].append(name)
        idx = len(stats_vals[0]) - 1
        sh[name] = idx
        header_writes.append({"range": f"{_col_letter(idx+1)}1", "values": [[name]]})
        return idx

    si_ct  = ensure_col(STATS_REBUY_CT)
    si_win = ensure_col(STATS_REBUY_WIN)
    si_avg = ensure_col(STATS_REBUY_AVG)

    # Compute row diffs (no per-row .update_cell)
    writes, touched = [], 0
    for r_idx, row in enumerate(stats_vals[1:], start=2):
        token = str_or_empty(row[si_token] if (si_token is not None and si_token < len(row)) else "").upper()
        if not token or token not in agg:
            continue
        cnt = agg[token]["cnt"]
        if cnt == 0:
            continue
        avg = agg[token]["sum"] / cnt
        winp = 100.0 * agg[token]["win"] / cnt

        # Existing values (to avoid needless writes)
        cur_ct  = str_or_empty(row[si_ct]  if si_ct  < len(row) else "")
        cur_win = str_or_empty(row[si_win] if si_win < len(row) else "")
        cur_avg = str_or_empty(row[si_avg] if si_avg < len(row) else "")

        new_ct  = str(cnt)
        new_win = f"{winp:.1f}"
        new_avg = f"{avg:.2f}"

        def _need(a, b): return (a or "") != (b or "")

        if _need(cur_ct, new_ct):
            writes.append({"range": f"{_col_letter(si_ct+1)}{r_idx}",  "values": [[new_ct]]})
            touched += 1
        if _need(cur_win, new_win):
            writes.append({"range": f"{_col_letter(si_win+1)}{r_idx}", "values": [[new_win]]})
            touched += 1
        if _need(cur_avg, new_avg):
            writes.append({"range": f"{_col_letter(si_avg+1)}{r_idx}", "values": [[new_avg]]})
            touched += 1

        if touched >= MAX_WRITES:
            break

    if not header_writes and not writes:
        print("✅ Rebuy ROI tracker: no changes needed.")
        return

    # Single batched write
    ws = get_ws(STATS_TAB)
    payload = []
    if header_writes: payload.extend(header_writes)
    if writes:        payload.extend(writes)
    ws_batch_update(ws, payload)
    print(f"✅ Rebuy ROI tracker: wrote {touched} cell(s){' + header' if header_writes else ''}.")
