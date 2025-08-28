# rotation_stats_sync.py

import re
from utils import (
    get_ws,
    safe_get_all_records,
    ws_batch_update,
    with_sheet_backoff,
    safe_float,        # parses "12.3%" -> 12.3, "" -> 0.0
)

STATS_SHEET = "Rotation_Stats"
LOG_SHEET = "Rotation_Log"

def _col_letter(idx_1b: int) -> str:
    n = idx_1b
    out = ""
    while n:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out

def _is_number_str(s: str) -> bool:
    return bool(re.match(r"^-?\d+(\.\d+)?$", (s or "").strip()))

@with_sheet_backoff
def run_rotation_stats_sync():
    print("📊 Syncing Rotation_Stats...")
    try:
        stats_ws = get_ws(STATS_SHEET)
        log_ws = get_ws(LOG_SHEET)

        # Read all once (cached + rate-limited)
        stats_rows = safe_get_all_records(stats_ws, ttl_s=180)  # list[dict]
        log_rows = safe_get_all_records(log_ws, ttl_s=180)      # list[dict]
        headers = stats_ws.row_values(1) or []
        hidx = {h: i + 1 for i, h in enumerate(headers)}  # 1-based indices

        # Ensure required headers exist
        changed_header = False
        if "Memory Tag" not in hidx:
            headers.append("Memory Tag")
            hidx["Memory Tag"] = len(headers)
            changed_header = True
        if "Performance" not in hidx:
            headers.append("Performance")
            hidx["Performance"] = len(headers)
            changed_header = True

        if changed_header:
            # single atomic header write
            stats_ws.update("A1", [headers])

        memory_col = hidx["Memory Tag"]
        perf_col = hidx["Performance"]

        # Build a quick lookup for Rotation_Log by token
        def _tok(s): return (s or "").strip().upper()
        log_by_token = {}
        for r in log_rows:
            t = _tok(r.get("Token"))
            if t:
                log_by_token[t] = r

        updates = []  # for ws_batch_update
        for i, row in enumerate(stats_rows, start=2):  # data starts at row 2
            token = _tok(row.get("Token"))
            if not token:
                continue

            # --- ROI source preference: Rotation_Log -> fallback own column
            roi_src = "Rotation_Log"
            roi_val = ""
            log_match = log_by_token.get(token)
            if log_match:
                roi_val = str(log_match.get("Follow-up ROI", "")).replace("%", "").strip()
            if not _is_number_str(roi_val):
                roi_val = str(row.get("Follow-up ROI", "")).replace("%", "").strip()
                roi_src = "Rotation_Stats"

            if not _is_number_str(roi_val):
                # nothing to do for this token
                continue

            roi = float(roi_val)

            # --- Memory Tag classification
            if roi >= 200:
                tag = "🟢 Big Win"
            elif 25 <= roi < 200:
                tag = "✅ Small Win"
            elif -24 <= roi <= 24:
                tag = "⚪ Break-Even"
            elif -70 <= roi < -25:
                tag = "🔻 Loss"
            elif roi <= -71:
                tag = "🔴 Big Loss"
            else:
                tag = ""

            a1_tag = f"{STATS_SHEET}!{_col_letter(memory_col)}{i}"
            updates.append({"range": a1_tag, "values": [[tag]]})
            print(f"🧠 {token} tagged as {tag} (ROI={roi} from {roi_src})")

            # --- Performance = followup / initial (if initial present)
            initial_roi_str = str(row.get("Initial ROI", "")).replace("%", "").strip()
            if _is_number_str(initial_roi_str):
                initial = float(initial_roi_str)
                if initial != 0:
                    perf = round(roi / initial, 2)
                    a1_perf = f"{STATS_SHEET}!{_col_letter(perf_col)}{i}"
                    updates.append({"range": a1_perf, "values": [[perf]]})
                    print(f"📈 {token} performance = {perf}")

        if updates:
            ws_batch_update(stats_ws, updates)
            print(f"✅ Rotation_Stats sync complete: {len(updates)} cell(s) updated (batched).")
        else:
            print("ℹ️ Rotation_Stats: nothing to update.")

    except Exception as e:
        print(f"❌ Error in run_rotation_stats_sync: {e}")
