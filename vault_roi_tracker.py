# vault_roi_tracker.py — NT3.0 Render Phase-1 polish
# Reads a vault positions tab once (cached), computes/records a daily snapshot
# in Vault_ROI_Tracker with ONE batch write. Tiny jitter + backoff.

import os, time, random
from utils import (
    get_values_cached, get_ws, ws_batch_update, with_sheet_backoff,
    str_or_empty, to_float
)

SRC_TAB   = os.getenv("VAULT_SRC_TAB", "Vaults")             # positions source
SNAP_TAB  = os.getenv("VAULT_SNAPSHOT_TAB", "Vault_ROI_Tracker")

TTL_READ_S   = int(os.getenv("VAULT_ROI_TTL_SEC", "300"))    # cache 5m
JIT_MIN_S    = float(os.getenv("VAULT_ROI_JITTER_MIN_S", "0.4"))
JIT_MAX_S    = float(os.getenv("VAULT_ROI_JITTER_MAX_S", "1.6"))
MAX_WRITES   = int(os.getenv("VAULT_ROI_MAX_WRITES", "800"))

# Expected columns on SRC_TAB (case-sensitive; override via env if yours differ)
COL_TOKEN    = os.getenv("VAULT_COL_TOKEN", "Token")
COL_ROI      = os.getenv("VAULT_COL_ROI",   "ROI %")
COL_USD      = os.getenv("VAULT_COL_USD",   "Value (USD)")
COL_STATUS   = os.getenv("VAULT_COL_STATUS","Status")  # optional

@with_sheet_backoff
def run_vault_roi_tracker():
    print("📈 Running Vault ROI Tracker…")
    time.sleep(random.uniform(JIT_MIN_S, JIT_MAX_S))

    vals = get_values_cached(SRC_TAB, ttl_s=TTL_READ_S) or []
    if not vals:
        print(f"ℹ️ {SRC_TAB} empty; skipping snapshot.")
        return

    header = vals[0]
    h = {h:i for i,h in enumerate(header)}
    need = [COL_TOKEN, COL_ROI, COL_USD]
    miss = [c for c in need if c not in h]
    if miss:
        print(f"⚠️ {SRC_TAB} missing columns: {', '.join(miss)}; skipping.")
        return

    tok_i, roi_i, usd_i = h[COL_TOKEN], h[COL_ROI], h[COL_USD]
    status_i = h.get(COL_STATUS)

    # Build snapshot rows in memory (no per-row writes)
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for r in vals[1:]:
        token = str_or_empty(r[tok_i] if tok_i < len(r) else "").upper()
        if not token:
            continue
        roi = to_float(r[roi_i] if roi_i < len(r) else "", default=None)
        usd = to_float(r[usd_i] if usd_i < len(r) else "", default=None)
        status = str_or_empty(r[status_i]) if status_i is not None and status_i < len(r) else ""
        if roi is None and usd is None:
            continue
        rows.append([ts, token, "" if roi is None else roi, "" if usd is None else usd, status])

    if not rows:
        print("✅ Vault ROI Tracker: nothing to write.")
        return

    # One batched append (single API call)
    ws = get_ws(SNAP_TAB)
    # Append by giving a starting A1 at the next empty row via batch_update
    payload = [{"range": f"A{ws.row_count+1}", "values": rows[:MAX_WRITES]}]
    ws_batch_update(ws, payload)
    print(f"✅ Vault ROI Tracker: wrote {min(len(rows), MAX_WRITES)} snapshot row(s).")
