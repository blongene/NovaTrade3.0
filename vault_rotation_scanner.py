# vault_rotation_scanner.py — NT3.0 Render Phase-1 polish
# Scans vault positions once (cached) and marks rotation candidates with one batch write.

import os, time, random
from utils import (
    get_values_cached, get_ws, ws_batch_update, with_sheet_backoff,
    str_or_empty, to_float
)

SRC_TAB       = os.getenv("VAULT_SRC_TAB", "Vaults")            # same as tracker by default
TTL_READ_S    = int(os.getenv("VAULT_SCAN_TTL_SEC", "300"))
JIT_MIN_S     = float(os.getenv("VAULT_SCAN_JITTER_MIN_S", "0.3"))
JIT_MAX_S     = float(os.getenv("VAULT_SCAN_JITTER_MAX_S", "1.2"))
MAX_UPDATES   = int(os.getenv("VAULT_SCAN_MAX_UPDATES", "300"))

# Columns (override via env to match your sheet)
COL_TOKEN     = os.getenv("VAULT_COL_TOKEN", "Token")
COL_ROI       = os.getenv("VAULT_COL_ROI",   "ROI %")
COL_STATUS    = os.getenv("VAULT_COL_STATUS","Status")
COL_CAND      = os.getenv("VAULT_COL_CAND",  "Rotation Candidate?")
COL_REASON    = os.getenv("VAULT_COL_REASON","Rotation Reason")
COL_LAST      = os.getenv("VAULT_COL_LAST",  "Last Evaluated")

# Simple policy (tune by env):
ROI_MIN_PCT   = float(os.getenv("VAULT_ROT_MIN_ROI_PCT", "-8"))   # e.g. rotate if ROI < -8%
ALLOW_STATUSES= {s.strip().upper() for s in os.getenv("VAULT_ROT_ALLOW_STATUSES", "ACTIVE,STAKED").split(",")}
BLOCK_STATUSES= {s.strip().upper() for s in os.getenv("VAULT_ROT_BLOCK_STATUSES", "LOCKED,CLAIMING").split(",")}

def _col_letter(n:int)->str:
    s=""
    while n:
        n,r=divmod(n-1,26)
        s=chr(65+r)+s
    return s

@with_sheet_backoff
def run_vault_rotation_scanner():
    print("🔁 Scanning Vault for Rotation Candidates…")
    time.sleep(random.uniform(JIT_MIN_S, JIT_MAX_S))

    vals = get_values_cached(SRC_TAB, ttl_s=TTL_READ_S) or []
    if not vals:
        print(f"ℹ️ {SRC_TAB} empty; skipping.")
        return

    header = vals[0]
    h = {h:i for i,h in enumerate(header)}
    need = [COL_TOKEN, COL_ROI]
    miss = [c for c in need if c not in h]
    if miss:
        print(f"⚠️ {SRC_TAB} missing columns: {', '.join(miss)}; skipping.")
        return

    tok_i, roi_i = h[COL_TOKEN], h[COL_ROI]
    status_i = h.get(COL_STATUS)
    cand_i   = h.get(COL_CAND)
    reason_i = h.get(COL_REASON)
    last_i   = h.get(COL_LAST)

    # If destination columns missing, we’ll add headers at the end (one call)
    header_writes = []
    def ensure_col(name):
        nonlocal header_writes, header, h
        if name in h: return h[name]
        j = len(header)  # append new column
        header_writes.append({"range": f"{_col_letter(j+1)}1", "values": [[name]]})
        h[name] = j
        header.append(name)
        return j

    if cand_i is None:   cand_i   = ensure_col(COL_CAND)
    if reason_i is None: reason_i = ensure_col(COL_REASON)
    if last_i is None:   last_i   = ensure_col(COL_LAST)

    writes = []
    touched = 0
    now = time.strftime("%Y-%m-%d %H:%M:%S")

    for r_idx, row in enumerate(vals[1:], start=2):
        token = str_or_empty(row[tok_i] if tok_i < len(row) else "").upper()
        if not token:
            continue
        roi = to_float(row[roi_i] if roi_i < len(row) else "", default=None)
        status = str_or_empty(row[status_i] if (status_i is not None and status_i < len(row)) else "").upper()

        # Skip blocked statuses
        if status and status in BLOCK_STATUSES:
            continue
        # Require allowed statuses if provided
        if ALLOW_STATUSES and status and status not in ALLOW_STATUSES:
            continue
        if roi is None:
            continue

        candidate = roi < ROI_MIN_PCT
        already = str_or_empty(row[cand_i] if cand_i < len(row) else "").upper() == "YES"
        if candidate and not already:
            writes.append({"range": f"{_col_letter(cand_i+1)}{r_idx}", "values": [["YES"]]})
            reason = f"ROI {roi:.2f}% < {ROI_MIN_PCT:.2f}%"
            writes.append({"range": f"{_col_letter(reason_i+1)}{r_idx}", "values": [[reason]]})
            writes.append({"range": f"{_col_letter(last_i+1)}{r_idx}", "values": [[now]]})
            touched += 1
        elif not candidate and already:
            # clear candidate flag if it recovered
            writes.append({"range": f"{_col_letter(cand_i+1)}{r_idx}", "values": [[""]]})
            writes.append({"range": f"{_col_letter(reason_i+1)}{r_idx}", "values": [[""]]})
            writes.append({"range": f"{_col_letter(last_i+1)}{r_idx}", "values": [[now]]})
            touched += 1

        if touched >= MAX_UPDATES:
            break

    if not header_writes and not writes:
        print("✅ Vault rotation scanner: no changes needed.")
        return

    ws = get_ws(SRC_TAB)
    payload = []
    if header_writes: payload.extend(header_writes)
    if writes:        payload.extend(writes)
    ws_batch_update(ws, payload)
    print(f"✅ Vault rotation scanner: updated {touched} row(s){' + headers' if header_writes else ''}.")
