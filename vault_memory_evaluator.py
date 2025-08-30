
# vault_memory_evaluator.py — NT3.0 Phase-1 Polish (idempotent + cache-first + batch write)
# Produces a lean staging table: Vault_Memory_Eval(Token, Score, Tag, Notes)

import os
from datetime import datetime
from utils import (
    get_ws, get_records_cached, get_values_cached, ws_batch_update,
    str_or_empty, to_float, with_sheet_backoff
)

SRC_TAB  = "Token_Vault"
EVAL_TAB = "Vault_Memory_Eval"

TTL_READ_S = int(os.getenv("VAULT_EVAL_TTL_READ_SEC", "300"))  # cache Token_Vault reads for 5m

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _ensure_eval_ws():
    """
    Idempotently ensure the Vault_Memory_Eval sheet exists.
    Handles concurrent runs where another worker may create it first.
    """
    try:
        return get_ws(EVAL_TAB)
    except Exception as e:
        # Create tab via the parent spreadsheet, then fetch again.
        # Use the SRC_TAB's spreadsheet handle (always exists).
        try:
            src_ws = get_ws(SRC_TAB)
            sh = src_ws.spreadsheet
            # Guard against races: try to add, but swallow "already exists"
            try:
                sh.add_worksheet(title=EVAL_TAB, rows=2000, cols=8)
            except Exception as ae:
                msg = str(ae).lower()
                if "already exists" not in msg and "exists" not in msg:
                    raise
            # Return the ensured worksheet
            return get_ws(EVAL_TAB)
        except Exception as ie:
            raise ie

def _score_row(r: dict) -> tuple[float, str]:
    """
    Conservative, robust score from Token_Vault row.
    Returns (score, notes_str)
    """
    tag   = str_or_empty(r.get("Vault Tag") or r.get("Tag"))
    roi   = to_float(r.get("ROI"))
    days  = to_float(r.get("Days Locked") or r.get("Days"), default=None)
    apy   = to_float(r.get("APY") or r.get("APR"))  # optional
    # Simple weighted blend; clamp contributions to avoid blowups
    score = 0.0
    if roi is not None:
        score += max(-100.0, min(roi, 300.0)) * 0.10
    if days is not None:
        score += max(0.0, min(days, 365.0)) * 0.02
    if apy is not None:
        score += max(0.0, min(apy, 500.0)) * 0.01  # small tilt for yield
    if tag:
        score += 1.0  # tiny bias for tagged items

    notes = []
    if roi is not None:  notes.append(f"roi={roi:.2f}")
    if days is not None: notes.append(f"days={int(days)}")
    if apy is not None:  notes.append(f"apy={apy:.2f}")
    if tag:              notes.append(f"tag={tag}")
    return round(score, 2), ", ".join(notes)

@with_sheet_backoff
def evaluate_vault_memory():
    print("🧠 Evaluating Vault Memory Scores...")

    # Cache-first read of source data
    src_rows = get_records_cached(SRC_TAB, ttl_s=TTL_READ_S) or []
    if not src_rows:
        print("ℹ️ Token_Vault empty; ensuring eval header and exiting.")
        ws = _ensure_eval_ws()
        _ensure_eval_header(ws)
        return

    # Build output rows: Token, Score, Tag, Notes
    out = []
    for r in src_rows:
        token = str_or_empty(r.get("Token") or r.get("Asset") or r.get("Coin")).upper()
        if not token:
            continue
        tag = str_or_empty(r.get("Vault Tag") or r.get("Tag"))
        score, notes = _score_row(r)
        out.append([token, f"{score:.2f}", tag, notes])

    ws = _ensure_eval_ws()

    # Read current header/body to decide if we need to add header, and whether to clear stale rows
    current_vals = []
    try:
        current_vals = get_values_cached(EVAL_TAB, ttl_s=60) or []
    except Exception:
        current_vals = []

    current_header = current_vals[0] if current_vals else []
    need_header = (not current_header) or current_header[:4] != ["Token", "Score", "Tag", "Notes"]

    writes = []
    if need_header:
        writes.append({"range": "A1:D1", "values": [["Token", "Score", "Tag", "Notes"]]})

    # Optional safe clear of stale rows if our new body is shorter than existing
    prev_body_len = max(0, len(current_vals) - 1)
    new_body_len  = len(out)
    if prev_body_len > new_body_len:
        # Clear only the excess area to keep API calls minimal
        start_row = new_body_len + 2  # body starts at row 2
        end_row   = prev_body_len + 1
        try:
            # gspread Worksheet has batch_clear; okay if it’s missing (older lib)
            ws.batch_clear([f"A{start_row}:D{end_row}"])
        except Exception:
            pass  # non-fatal; leftover rows won’t break importer if it uses exact match by token

    # Write the new body (single write)
    if out:
        writes.append({"range": "A2", "values": out})

    if writes:
        ws_batch_update(ws, writes)

    print(f"✅ Vault memory evaluator: {len(out)} token(s) scored; sheet up to date at {datetime.utcnow().isoformat(timespec='seconds')}Z.")

def _ensure_eval_header(ws):
    try:
        ws.batch_update(
            [{"range": "A1:D1", "values": [["Token", "Score", "Tag", "Notes"]]}],
            value_input_option="RAW"
        )
    except Exception:
        pass
