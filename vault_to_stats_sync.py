# vault_to_stats_sync.py — NT3.0 Render Phase-1 polish
# Syncs “tags” from the Vaults tab → Rotation_Stats with:
# - ONE cached read of source vault tab
# - ONE cached read of Rotation_Stats
# - ONE batched write for changes (and header creation if missing)
# - Tiny jitter and backoff

import os, time, random
from utils import (
    get_values_cached, get_ws, ws_batch_update, with_sheet_backoff,
    str_or_empty
)

# ---- Config (override in Render ENV) -----------------------------------------
SRC_TAB        = os.getenv("VAULT_SRC_TAB", "Vaults")            # vault source tab
STATS_TAB      = os.getenv("ROT_STATS_TAB", "Rotation_Stats")    # destination

# Source columns on vault tab
SRC_TOKEN_COL  = os.getenv("VAULT_SRC_TOKEN_COL", "Token")
SRC_TAGS_COL   = os.getenv("VAULT_SRC_TAGS_COL",  "Tags")        # comma/space separated

# Destination columns on Rotation_Stats
DST_TOKEN_COL  = os.getenv("ROT_STATS_TOKEN_COL", "Token")
DST_TAGS_COL   = os.getenv("ROT_STATS_TAGS_COL",  "Memory Tag")  # will be created if missing

# Behavior
TTL_SRC_S      = int(os.getenv("VAULT2STATS_TTL_SRC_SEC",   "300"))
TTL_DST_S      = int(os.getenv("VAULT2STATS_TTL_DST_SEC",   "300"))
MAX_WRITES     = int(os.getenv("VAULT2STATS_MAX_WRITES",    "500"))
MIN_CHANGE_LEN = int(os.getenv("VAULT2STATS_MIN_CHANGE_LEN","0")) # threshold for write noise
JIT_MIN_S      = float(os.getenv("VAULT2STATS_JITTER_MIN_S","0.3"))
JIT_MAX_S      = float(os.getenv("VAULT2STATS_JITTER_MAX_S","1.2"))

# Optional: normalize tags (upper, strip, dedupe, sort)
NORM_TAGS      = os.getenv("VAULT2STATS_NORMALIZE", "true").lower() == "true"

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def _idx(header, name):
    try:
        return header.index(name)
    except ValueError:
        return None

def _normalize_tags(s: str) -> str:
    if not s:
        return ""
    # split on comma or whitespace, strip empties
    parts = []
    for chunk in s.replace(",", " ").split():
        t = chunk.strip()
        if t:
            parts.append(t.upper())
    # dedupe, sort
    uniq = sorted(set(parts))
    return ", ".join(uniq)

@with_sheet_backoff
def run_vault_to_stats_sync():
    print("📊 Syncing Vault Tags → Rotation_Stats …")
    time.sleep(random.uniform(JIT_MIN_S, JIT_MAX_S))

    # -------- Source (Vaults) read: ONE cached call
    src_vals = get_values_cached(SRC_TAB, ttl_s=TTL_SRC_S) or []
    if not src_vals or not src_vals[0]:
        print(f"ℹ️ {SRC_TAB} empty; nothing to sync.")
        return
    src_header = src_vals[0]
    s_tok = _idx(src_header, SRC_TOKEN_COL)
    s_tag = _idx(src_header, SRC_TAGS_COL)
    miss = [n for n,(i) in {SRC_TOKEN_COL:s_tok, SRC_TAGS_COL:s_tag}.items() if i is None]
    if miss:
        print(f"⚠️ {SRC_TAB} missing columns: {', '.join(miss)}; skipping.")
        return

    # Build token → tags map from source
    tags_map = {}
    for row in src_vals[1:]:
        token = str_or_empty(row[s_tok] if s_tok < len(row) else "").upper()
        if not token:
            continue
        raw = str_or_empty(row[s_tag] if s_tag < len(row) else "")
        val = _normalize_tags(raw) if NORM_TAGS else raw.strip()
        if val:
            tags_map[token] = val

    if not tags_map:
        print("ℹ️ No tags found on source; nothing to sync.")
        return

    # -------- Destination (Rotation_Stats) read: ONE cached call
    dst_vals = get_values_cached(STATS_TAB, ttl_s=TTL_DST_S) or []
    if not dst_vals or not dst_vals[0]:
        print(f"ℹ️ {STATS_TAB} empty; skipping.")
        return
    dst_header = dst_vals[0]
    d_tok = _idx(dst_header, DST_TOKEN_COL)
    d_tag = _idx(dst_header, DST_TAGS_COL)

    # Ensure destination tag column exists; create header if missing
    header_writes = []
    if d_tag is None:
        dst_header.append(DST_TAGS_COL)
        d_tag = len(dst_header) - 1
        header_writes.append({"range": f"{_col_letter(d_tag+1)}1", "values": [[DST_TAGS_COL]]})

    # -------- Compute row deltas (no per-row writes)
    writes, touched = [], 0
    for r_idx, row in enumerate(dst_vals[1:], start=2):
        token = str_or_empty(row[d_tok] if (d_tok is not None and d_tok < len(row)) else "").upper()
        if not token:
            continue
        new_val = tags_map.get(token, "")
        cur_val = str_or_empty(row[d_tag] if d_tag < len(row) else "")

        if new_val == cur_val:
            continue
        if len(new_val) < MIN_CHANGE_LEN and new_val == "":
            continue  # ignore tiny clears if configured

        writes.append({
            "range": f"{_col_letter(d_tag+1)}{r_idx}",
            "values": [[new_val]]
        })
        touched += 1
        if touched >= MAX_WRITES:
            break

    if not header_writes and not writes:
        print("✅ Vault→Stats tags sync: no changes needed.")
        return

    # -------- One batched write
    ws = get_ws(STATS_TAB)
    payload = []
    if header_writes: payload.extend(header_writes)
    if writes:        payload.extend(writes)
    ws_batch_update(ws, payload)
    print(f"✅ Vault→Stats tags sync: wrote {touched} cell(s){' + header' if header_writes else ''}.")
