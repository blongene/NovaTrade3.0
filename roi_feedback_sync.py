# roi_feedback_sync.py — NT3.0 Render Phase-1 diet
# Syncs ROI_Review_Log decisions into Rotation_Stats with 1 cached read + 1 batch write.

import os, time, random
from utils import (
    get_values_cached, get_ws, ws_batch_update, with_sheet_backoff, str_or_empty
)

LOG_TAB   = os.getenv("ROI_LOG_TAB", "ROI_Review_Log")
STATS_TAB = os.getenv("ROT_STATS_TAB", "Rotation_Stats")

TTL_LOG_S    = int(os.getenv("ROI_SYNC_TTL_LOG_SEC", "300"))   # cache 5m
TTL_STATS_S  = int(os.getenv("ROI_SYNC_TTL_STATS_SEC", "300")) # cache 5m
MAX_WRITES   = int(os.getenv("ROI_SYNC_MAX_WRITES", "400"))
JIT_MIN_S    = float(os.getenv("ROI_SYNC_JITTER_MIN_S", "0.4"))
JIT_MAX_S    = float(os.getenv("ROI_SYNC_JITTER_MAX_S", "1.4"))

YES_SET = {"YES","Y","TRUE","ON","REBUY","ROTATE","VAULT"}
NO_SET  = {"NO","N","FALSE","OFF","HOLD","SKIP"}

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

def _find_col(header, *names):
    for name in names:
        if name in header:
            return header.index(name)
    return None

@with_sheet_backoff
def run_roi_feedback_sync():
    print("📅 Syncing ROI feedback responses…")
    time.sleep(random.uniform(JIT_MIN_S, JIT_MAX_S))  # de-sync from neighbors

    # ---------- Read ROI_Review_Log (values-only, cached) ----------
    log_vals = get_values_cached(LOG_TAB, ttl_s=TTL_LOG_S) or []
    if not log_vals or not log_vals[0]:
        print(f"ℹ️ {LOG_TAB} empty; nothing to sync.")
        return

    log_header = log_vals[0]
    tcol = _find_col(log_header, "Token")
    dcol = _find_col(log_header, "Decision", "Response")
    if tcol is None or dcol is None:
        miss = []
        if tcol is None: miss.append("Token")
        if dcol is None: miss.append("Decision/Response")
        print(f"⚠️ {LOG_TAB} missing columns: {', '.join(miss)}; skipping.")
        return

    # Build latest decision per token (last write wins)
    decisions = {}
    for row in log_vals[1:]:
        token = str_or_empty(row[tcol] if tcol < len(row) else "").upper()
        if not token:
            continue
        dec = str_or_empty(row[dcol] if dcol < len(row) else "").upper()
        if dec:
            decisions[token] = dec

    if not decisions:
        print("ℹ️ No decisions found in ROI_Review_Log; skipping.")
        return

    # ---------- Read Rotation_Stats (values-only, cached) ----------
    stats_vals = get_values_cached(STATS_TAB, ttl_s=TTL_STATS_S) or []
    if not stats_vals or not stats_vals[0]:
        print(f"ℹ️ {STATS_TAB} empty; skipping.")
        return

    stats_header = stats_vals[0]
    s_tok = _find_col(stats_header, "Token")
    s_usr = _find_col(stats_header, "User Response", "Response")
    s_conf = _find_col(stats_header, "Confirmed")
    if s_tok is None or s_usr is None or s_conf is None:
        miss = []
        if s_tok is None: miss.append("Token")
        if s_usr is None: miss.append("User Response/Response")
        if s_conf is None: miss.append("Confirmed")
        print(f"⚠️ {STATS_TAB} missing columns: {', '.join(miss)}; skipping.")
        return

    # ---------- Compute writes ----------
    writes = []
    touched = 0

    for r_idx, row in enumerate(stats_vals[1:], start=2):
        token = str_or_empty(row[s_tok] if s_tok < len(row) else "").upper()
        if not token:
            continue
        new_dec = decisions.get(token)
        if not new_dec:
            continue

        cur_resp = str_or_empty(row[s_usr] if s_usr < len(row) else "").upper()
        cur_conf = str_or_empty(row[s_conf] if s_conf < len(row) else "").upper()

        # Normalize YES/NO-ish
        if new_dec in YES_SET:
            target_resp = "YES"
            target_conf = "YES"
        elif new_dec in NO_SET:
            target_resp = "NO"
            target_conf = "" if cur_conf == "" else ""   # keep unconfirmed on NO
        else:
            # freeform text → set response, leave Confirmed alone
            target_resp = new_dec
            target_conf = cur_conf

        # Only write if changes needed
        row_writes = []
        if target_resp != cur_resp:
            row_writes.append({"range": f"{_col_letter(s_usr+1)}{r_idx}", "values": [[target_resp]]})
        if target_conf != cur_conf:
            row_writes.append({"range": f"{_col_letter(s_conf+1)}{r_idx}", "values": [[target_conf]]})

        if row_writes:
            writes.extend(row_writes)
            touched += 1
            if touched >= MAX_WRITES:
                break

    if not writes:
        print("✅ ROI feedback sync: no changes needed.")
        return

    # ---------- One batched write ----------
    ws = get_ws(STATS_TAB)  # open only if we actually write
    ws_batch_update(ws, writes)
    print(f"✅ ROI feedback sync: updated {touched} row(s).")
