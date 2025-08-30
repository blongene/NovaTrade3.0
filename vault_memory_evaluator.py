
# vault_memory_evaluator.py — Phase-1 Polish
# Produces a lean staging table: Vault_Memory_Eval(Token, Score, Tag, Notes)

from utils import (
    get_ws, get_values_cached, get_records_cached, ws_batch_update,
    str_or_empty, to_float, with_sheet_backoff
)

EVAL_TAB = "Vault_Memory_Eval"
SRC_TAB  = "Token_Vault"

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def evaluate_vault_memory():
    print("🧠 Evaluating Vault Memory Scores...")
    rows = get_records_cached(SRC_TAB, ttl_s=300) or []
    if not rows:
        print("ℹ️ Token_Vault empty; nothing to score.")
        _ensure_eval_header()
        return

    # Build output rows: Token, Score, Tag, Notes (robust to missing columns)
    out = []
    for r in rows:
        token = str_or_empty(r.get("Token")).upper()
        if not token:
            continue
        tag   = str_or_empty(r.get("Vault Tag"))
        roi   = to_float(r.get("ROI"))      # optional
        days  = to_float(r.get("Days Locked"))  # optional

        score = 0.0
        # simple, safe signals (tweak later without quota harm)
        if roi is not None:
            score += min(max(roi, -100.0), 300.0) * 0.10
        if days is not None:
            score += min(max(days, 0.0), 365.0) * 0.02
        if tag:
            score += 1.0  # tiny bias for tagged items

        notes = []
        if roi is not None:  notes.append(f"roi={roi:.2f}")
        if days is not None: notes.append(f"days={int(days)}")
        if tag:              notes.append(f"tag={tag}")
        out.append([token, f"{score:.2f}", tag, ", ".join(notes)])

    # Create/ensure header, then write entire table in a single batch
    ws = get_ws(EVAL_TAB) if _sheet_exists(EVAL_TAB) else _create_eval_tab()
    header = get_values_cached(EVAL_TAB, ttl_s=120)
    needs_header = not header or not header[0] or header[0][0] != "Token"

    writes = []
    if needs_header:
        writes.append({"range": "A1:D1", "values": [["Token", "Score", "Tag", "Notes"]]})

    # clear previous body (A2:D) by overwriting with new body only (no explicit clear to avoid extra write)
    if out:
        writes.append({"range": "A2", "values": out})
    else:
        # if no rows, keep header only; nothing else to do
        pass

    if writes:
        ws_batch_update(ws, writes)
    print(f"✅ Vault memory evaluator: {len(out)} token(s) scored.")

def _sheet_exists(title: str) -> bool:
    try:
        get_ws(title)
        return True
    except Exception:
        return False

def _create_eval_tab():
    # create once with header
    ws = get_ws(SRC_TAB).spreadsheet.add_worksheet(title=EVAL_TAB, rows=2000, cols=8)
    ws.batch_update([{"range": "A1:D1", "values": [["Token", "Score", "Tag", "Notes"]]}], value_input_option="RAW")
    return get_ws(EVAL_TAB)

def _ensure_eval_header():
    try:
        ws = get_ws(EVAL_TAB) if _sheet_exists(EVAL_TAB) else _create_eval_tab()
        vals = get_values_cached(EVAL_TAB, ttl_s=60)
        if not vals or not vals[0] or vals[0][0] != "Token":
            ws_batch_update(ws, [{"range": "A1:D1", "values": [["Token", "Score", "Tag", "Notes"]]}])
    except Exception:
        pass
