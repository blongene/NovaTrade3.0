# vault_intelligence.py — Phase 5 (merged)
# - Preserves legacy Memory Tag sync into Rotation_Stats using utils (backoff + batch)
# - Adds Phase 5 Vault Intelligence sheet generation (metrics + readiness)
#
# Env:
#   SHEET_URL (handled by utils.get_ws_cached)
#   VAULT_INTELLIGENCE_WS (optional, default "Vault Intelligence")
#
# Tabs referenced:
#   Token_Vault (legacy): columns Token, Decision
#   Rotation_Stats (legacy): ensures/updates "Memory Tag" column
#   Rotation_Log (new): expects Token, Follow-up ROI, Days Held, Allocation (%), Last Checked/Timestamp, Liquidity_USD, Memory_Score (best-effort)
#   Claim_Tracker (new, optional): Token, Unlock Date
#
# Output:
#   Vault Intelligence sheet with columns:
#     Timestamp, Token, roi_1d, roi_7d, roi_30d, unlock_days, liquidity_usd, memory_score, rebuy_ready, last_action_ts
#
import os
from datetime import datetime
from utils import (
    get_ws_cached, ws_update, ws_batch_update,
    str_or_empty, with_sheet_backoff
)

VAULT_WS_NAME = os.getenv("VAULT_INTELLIGENCE_WS", "Vault Intelligence")

def _col_letter(idx1: int) -> str:
    n = idx1; s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def _get_ws(title: str):
    return get_ws_cached(title, ttl_s=30)

@with_sheet_backoff
def _get_all_records(ws):
    return ws.get_all_records()

@with_sheet_backoff
def _get_all_values(ws):
    return ws.get_all_values()

@with_sheet_backoff
def _clear_ws(ws):
    return ws.clear()

@with_sheet_backoff
def _append_row(ws, row):
    return ws.append_row(row)

@with_sheet_backoff
def _append_rows(ws, rows):
    return ws.append_rows(rows)

def _safe_float(x):
    try:
        return float(str(x).replace("%","").strip())
    except Exception:
        return None

def _ensure_header(ws, header):
    vals = _get_all_values(ws)
    if not vals:
        _append_row(ws, header)
        return header
    if vals and vals[0] != header:
        # replace header if different
        ws_update(ws, "A1", [header])
    return header

def _sync_memory_tags():
    """Legacy behavior: tag Rotation_Stats.Memory Tag using Token_Vault.Decision."""
    try:
        vault_ws = _get_ws("Token_Vault")
        stats_ws = _get_ws("Rotation_Stats")
    except Exception as e:
        print(f"⚠️ Memory tag sync skipped (missing sheets): {e}")
        return

    # Read once
    vault_rows = _get_all_records(vault_ws)
    stats_vals = _get_all_values(stats_ws)
    if not stats_vals:
        print("⚠️ Rotation_Stats is empty; nothing to tag.")
        return

    header = stats_vals[0]; rows = stats_vals[1:]
    def _hidx(name, default=None):
        try: return header.index(name) + 1
        except ValueError: return default

    token_col   = _hidx("Token")
    memory_col  = _hidx("Memory Tag")
    if memory_col is None:
        header.append("Memory Tag")
        ws_update(stats_ws, "A1", [header])
        memory_col = len(header)

    # Build quick dict from vault: token -> tag
    vault_map = {}
    for r in vault_rows:
        t = str_or_empty(r.get("Token")).strip().upper()
        if not t: continue
        decision = str_or_empty(r.get("Decision")).strip().upper()
        if decision == "VAULT":
            vault_map[t] = "✅ Vaulted"
        elif decision in ("IGNORE", "ROTATE", ""):
            vault_map.setdefault(t, "⚠️ Never Vaulted")

    updates = []
    for i, row in enumerate(rows, start=2):
        t = str_or_empty(row[(token_col - 1) if token_col else 0]).strip().upper()
        if not t: continue
        tag = vault_map.get(t, "")
        if not tag: continue
        a1 = f"{_col_letter(memory_col)}{i}"
        updates.append({"range": a1, "values": [[tag]]})
        print(f"📦 {t} tagged as: {tag}")
    if updates:
        ws_batch_update(stats_ws, updates)
    print("✅ Memory Tag sync complete.")

def _build_vault_intelligence():
    """Phase 5: generate Vault Intelligence metrics sheet."""
    # Inputs
    try:
        rotation_log = _get_all_records(_get_ws("Rotation_Log"))
    except Exception:
        rotation_log = []

    try:
        claim_records = _get_all_records(_get_ws("Claim_Tracker"))
        claim_tracker = {r.get("Token","").strip().upper(): r for r in claim_records}
    except Exception:
        claim_tracker = {}

    now = datetime.utcnow()
    state_rows = []; seen = set()
    for row in rotation_log:
        token = str_or_empty(row.get("Token")).strip().upper()
        if not token or token in seen: continue
        seen.add(token)

        roi_followup = _safe_float(row.get("Follow-up ROI"))
        days_held    = _safe_float(row.get("Days Held"))
        alloc_pct    = _safe_float(row.get("Allocation (%)"))
        last_checked = str_or_empty(row.get("Last Checked")) or str_or_empty(row.get("Timestamp"))

        ct = claim_tracker.get(token, {})
        unlock_str = str_or_empty(ct.get("Unlock Date"))
        unlock_days = ""
        if unlock_str:
            try:
                d = datetime.strptime(unlock_str, "%Y-%m-%d")
                unlock_days = (now - d).days
            except Exception:
                unlock_days = ""

        liquidity_usd = _safe_float(row.get("Liquidity_USD")) or ""
        memory_score  = _safe_float(row.get("Memory_Score")) or ""

        rebuy_ready = False
        if roi_followup is not None and roi_followup <= -5:
            rebuy_ready = True

        last_action_ts = ""
        if last_checked:
            try:
                last_action_ts = int(datetime.fromisoformat(last_checked.replace("Z","")).timestamp())
            except Exception:
                last_action_ts = ""

        state_rows.append([
            now.strftime("%Y-%m-%d %H:%M:%S"),
            token,
            "", "", "",              # roi_1d, roi_7d, roi_30d (placeholders for future adapters)
            unlock_days,
            liquidity_usd,
            memory_score,
            "TRUE" if rebuy_ready else "FALSE",
            last_action_ts
        ])

    # Output
    try:
        vi_ws = _get_ws(VAULT_WS_NAME)
        _clear_ws(vi_ws)
    except Exception:
        # Create if missing
        # We can't create via utils here, so rely on get_ws_cached to have created previously.
        vi_ws = _get_ws(VAULT_WS_NAME)

    headers = ["Timestamp","Token","roi_1d","roi_7d","roi_30d","unlock_days","liquidity_usd","memory_score","rebuy_ready","last_action_ts"]
    _ensure_header(vi_ws, headers)
    if state_rows:
        _append_rows(vi_ws, state_rows)
    print(f"✅ Vault Intelligence updated: {len(state_rows)} assets")

def run_vault_intelligence():
    print("🧠 Running Vault Intelligence (merged legacy + Phase 5)…")
    _sync_memory_tags()
    _build_vault_intelligence()
    print("🏁 Vault Intelligence (merged) complete.")
