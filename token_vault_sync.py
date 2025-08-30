# token_vault_sync.py — NT3.0 Render (429-safe)
# Reads the vault tab ONCE (cached), ensures a few standard columns exist,
# and normalizes a couple of lightweight fields in ONE batch update.

import os, time, random
from utils import (
    get_values_cached, get_ws, ws_batch_update, with_sheet_backoff,
    str_or_empty,
)

SRC_TAB     = os.getenv("VAULT_SRC_TAB", "Vaults")
TTL_S       = int(os.getenv("VAULT_SYNC_TTL_SEC", "300"))
JIT_MIN_S   = float(os.getenv("VAULT_SYNC_JIT_MIN_S", "0.25"))
JIT_MAX_S   = float(os.getenv("VAULT_SYNC_JIT_MAX_S", "1.0"))
MAX_WRITES  = int(os.getenv("VAULT_SYNC_MAX_WRITES", "400"))

# Columns (override by ENV to fit your sheet)
COL_TOKEN   = os.getenv("VAULT_COL_TOKEN",   "Token")
COL_STATUS  = os.getenv("VAULT_COL_STATUS",  "Status")
COL_TAGS    = os.getenv("VAULT_COL_TAGS",    "Tags")
COL_LAST    = os.getenv("VAULT_COL_LAST",    "Last Synced")

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def _hmap(header):
    return {str_or_empty(h): i for i, h in enumerate(header)}

def _normalize_tags(s: str) -> str:
    if not s: return ""
    parts = []
    for c in s.replace(",", " ").split():
        t = c.strip()
        if t: parts.append(t.upper())
    return ", ".join(sorted(set(parts)))

@with_sheet_backoff
def sync_token_vault():
    print("📦 Syncing Token Vault...")
    time.sleep(random.uniform(JIT_MIN_S, JIT_MAX_S))

    vals = get_values_cached(SRC_TAB, ttl_s=TTL_S) or []
    if not vals:
        print(f"ℹ️ {SRC_TAB} empty; skipping.")
        return

    header = vals[0]
    h = _hmap(header)
    ti, si, gi, li = h.get(COL_TOKEN), h.get(COL_STATUS), h.get(COL_TAGS), h.get(COL_LAST)

    # Ensure optional columns exist
    header_writes = []
    def ensure_col(name):
        nonlocal header_writes, header, h
        if name in h: return h[name]
        header.append(name)
        idx = len(header) - 1
        h[name] = idx
        header_writes.append({"range": f"{_col_letter(idx+1)}1", "values": [[name]]})
        return idx

    if gi is None: gi = ensure_col(COL_TAGS)
    if li is None: li = ensure_col(COL_LAST)

    # Build normalization updates (no per-row writes)
    writes, touched = [], 0
    now = time.strftime("%Y-%m-%d %H:%M:%S")

    for r_idx, row in enumerate(vals[1:], start=2):
        token  = str_or_empty(row[ti] if ti is not None and ti < len(row) else "")
        status = str_or_empty(row[si] if si is not None and si < len(row) else "")
        tags   = str_or_empty(row[gi] if gi is not None and gi < len(row) else "")

        if not token:
            continue

        # Normalize tags (UPPER, dedupe, sorted)
        norm_tags = _normalize_tags(tags)
        if norm_tags != tags:
            writes.append({"range": f"{_col_letter(gi+1)}{r_idx}", "values": [[norm_tags]]})
            touched += 1
        # Stamp Last Synced
        writes.append({"range": f"{_col_letter(li+1)}{r_idx}", "values": [[now]]})
        touched += 1

        if touched >= MAX_WRITES:
            break

    if not header_writes and not writes:
        print("✅ Vault sync: no changes needed.")
        return

    ws = get_ws(SRC_TAB)
    payload = []
    if header_writes: payload.extend(header_writes)
    if writes:        payload.extend(writes)
    ws_batch_update(ws, payload)
    print(f"✅ Vault sync: {touched} cell(s) updated{' + header' if header_writes else ''}.")
