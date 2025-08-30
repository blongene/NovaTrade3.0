# rotation_memory_scoring.py — NT3.0 Phase-1 Polish
from utils import (
    get_ws, get_records_cached, ws_batch_update,
    str_or_empty, with_sheet_backoff
)

TAB = "Rotation_Stats"
COL_NAME = "Memory Score"

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def run_memory_scoring():
    print("🧠 Calculating weighted Memory Scores...")
    rows = get_records_cached(TAB, ttl_s=300) or []
    if not rows:
        print("ℹ️ Rotation_Stats empty; skipping.")
        return

    ws = get_ws(TAB)
    header = ws.row_values(1)

    if COL_NAME in header:
        score_col = header.index(COL_NAME) + 1
        add_header = False
    else:
        score_col = len(header) + 1
        add_header = True

    tag_present  = "Memory Tag" in header
    vote_present = "Re-Vote" in header

    writes = []
    if add_header:
        writes.append({"range": f"{_col_letter(score_col)}1", "values": [[COL_NAME]]})

    for i, r in enumerate(rows, start=2):
        tag  = str_or_empty(r.get("Memory Tag") if tag_present else "")
        vote = str_or_empty(r.get("Re-Vote") if vote_present else "").upper()

        score = 0
        if "Big Win" in tag:       score += 3
        elif "Small Win" in tag:   score += 2
        elif "Break-Even" in tag:  score += 1
        elif "Loss" in tag:        score -= 1
        elif "Big Loss" in tag:    score -= 2

        if vote == "YES": score += 1
        elif vote == "NO": score -= 2

        cur = str_or_empty(r.get(COL_NAME))
        new = str(score)
        if new != cur:
            writes.append({"range": f"{_col_letter(score_col)}{i}", "values": [[new]]})

    if writes:
        ws_batch_update(ws, writes)
        print(f"✅ Memory Scoring complete. {len(writes)} cell(s) updated.")
    else:
        print("✅ Memory Scoring complete. 0 changes.")
