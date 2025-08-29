# vault_intelligence.py
import os
from datetime import datetime
import gspread
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials

from utils import (
    with_sheet_backoff,
    str_or_empty,
    to_float,
)

SHEET_URL = os.getenv("SHEET_URL")
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

def _col_letter(idx1: int) -> str:
    # 1-based column index -> letters
    n = idx1
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def _open_sheet():
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", SCOPE)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL)

@with_sheet_backoff
def _get_ws(title: str):
    sh = _open_sheet()
    return sh.worksheet(title)

def run_vault_intelligence():
    print("📦 Running Vault Intelligence Sync...")
    try:
        vault_ws = _get_ws("Token_Vault")
        stats_ws = _get_ws("Rotation_Stats")

        # Read once
        vault_rows = vault_ws.get_all_records()
        stats_vals = stats_ws.get_all_values()
        if not stats_vals:
            print("⚠️ Rotation_Stats is empty; nothing to tag.")
            return
        header = stats_vals[0]
        rows   = stats_vals[1:]
        # Locate columns (Rotation_Stats)
        def _hidx(name, default=None):
            try:
                return header.index(name) + 1  # 1-based
            except ValueError:
                return default

        token_col   = _hidx("Token")
        memory_col  = _hidx("Memory Tag")
        roi_col     = _hidx("Follow-up ROI") or _hidx("Follow-up ROI (%)")

        # Create missing Memory Tag header once if absent
        updates = []
        if memory_col is None:
            header.append("Memory Tag")
            stats_ws.update("A1", [header])
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
                # keep as unknown unless we want to mark “Never Vaulted”
                vault_map.setdefault(t, "⚠️ Never Vaulted")

        # Prepare batch updates (A1 ranges WITHOUT duplicating the sheet name)
        for i, row in enumerate(rows, start=2):
            t = str_or_empty(row[token_col - 1] if token_col else "").strip().upper()
            if not t:
                continue

            tag = vault_map.get(t, "")
            if not tag:
                continue

            a1 = f"{_col_letter(memory_col)}{i}"
            updates.append({"range": a1, "values": [[tag]]})
            print(f"📦 {t} tagged as: {tag}")

        if updates:
            stats_ws.batch_update(updates, value_input_option="USER_ENTERED")

        print("✅ Vault intelligence sync complete.")

    except APIError as e:
        # Soft-fail on quota; main loop will call us again later
        if "429" in str(e):
            print("❌ Vault sync error: APIError 429 (quota) — skipping this cycle.")
            return
        raise
    except Exception as e:
        print(f"❌ Vault sync error: {e}")
