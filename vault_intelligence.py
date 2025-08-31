# vault_intelligence.py — utils-first, quota-safe writes
import os
from utils import (
    get_ws_cached, ws_update, ws_batch_update,
    str_or_empty, with_sheet_backoff
)

def _col_letter(idx1: int) -> str:
    # 1-based column index -> letters
    n = idx1
    s = ""
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

def run_vault_intelligence():
    print("📦 Running Vault Intelligence Sync...")
    try:
        vault_ws = _get_ws("Token_Vault")
        stats_ws = _get_ws("Rotation_Stats")

        # Read once
        vault_rows = _get_all_records(vault_ws)
        stats_vals = _get_all_values(stats_ws)
        if not stats_vals:
            print("⚠️ Rotation_Stats is empty; nothing to tag.")
            return

        header = stats_vals[0]
        rows   = stats_vals[1:]

        def _hidx(name, default=None):
            try:
                return header.index(name) + 1  # 1-based
            except ValueError:
                return default

        token_col   = _hidx("Token")
        memory_col  = _hidx("Memory Tag")
        _ = _hidx("Follow-up ROI") or _hidx("Follow-up ROI (%)")  # kept for future logic

        # Create missing Memory Tag header once if absent
        if memory_col is None:
            header.append("Memory Tag")
            ws_update(stats_ws, "A1", [header])
            memory_col = len(header)

        # Build quick dict from vault: token -> tag (✅ Vaulted / ⚠️ Never Vaulted)
        vault_map = {}
        for r in vault_rows:
            t = str_or_empty(r.get("Token")).strip().upper()
            if not t:
                continue
            decision = str_or_empty(r.get("Decision")).strip().upper()
            if decision == "VAULT":
                vault_map[t] = "✅ Vaulted"
            elif decision in ("IGNORE", "ROTATE", ""):
                vault_map.setdefault(t, "⚠️ Never Vaulted")

        # Prepare batch updates on the worksheet (ranges are local to the sheet)
        updates = []
        for i, row in enumerate(rows, start=2):
            t = str_or_empty(row[(token_col - 1) if token_col else 0]).strip().upper()
            if not t:
                continue
            tag = vault_map.get(t, "")
            if not tag:
                continue
            a1 = f"{_col_letter(memory_col)}{i}"
            updates.append({"range": a1, "values": [[tag]]})
            print(f"📦 {t} tagged as: {tag}")

        if updates:
            ws_batch_update(stats_ws, updates)

        print("✅ Vault intelligence sync complete.")
    except Exception as e:
        print(f"❌ Vault sync error: {e}")
