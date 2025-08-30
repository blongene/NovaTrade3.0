# rotation_memory.py
import os
from utils import with_sheet_backoff, get_gspread_client, ping_webhook_debug, str_or_empty, to_float

SHEET_URL = os.getenv("SHEET_URL")

def _open_sheet():
    return get_gspread_client().open_by_url(SHEET_URL)

@with_sheet_backoff
def _get_all(ws):
    return ws.get_all_values()

@with_sheet_backoff
def _update_range(ws, rng, rows):
    ws.update(rng, rows, value_input_option="USER_ENTERED")

def _header_index_map(headers):
    return {str_or_empty(h): i for i, h in enumerate(headers)}

def _normalize(s):
    return str_or_empty(s).upper()

def run_rotation_memory():
    try:
        print("🧠 Running Rotation Memory Sync...")
        sh = _open_sheet()
        log_ws, stats_ws = sh.worksheet("Rotation_Log"), sh.worksheet("Rotation_Stats")
        log_vals, stats_vals = _get_all(log_ws), _get_all(stats_ws)
        if not log_vals or not stats_vals:
            print("⚠️ Missing data for memory sync.")
            return

        lh, sh_idx = _header_index_map(log_vals[0]), _header_index_map(stats_vals[0])
        tok_c = lh.get("Token")
        roi_c = lh.get("ROI %") if lh.get("ROI %") is not None else lh.get("ROI")
        if tok_c is None or roi_c is None:
            print("⛔️ Rotation_Log must include 'Token' and 'ROI %' (or 'ROI').")
            return

        # Aggregate per token
        agg = {}
        for row in log_vals[1:]:
            token = _normalize(row[tok_c] if tok_c < len(row) else "")
            roi = to_float(row[roi_c] if roi_c < len(row) else "")
            if not token or roi is None:
                continue
            st = agg.setdefault(token, {"wins": 0, "total": 0, "sum": 0.0})
            st["total"] += 1
            st["sum"] += roi
            if roi > 0:
                st["wins"] += 1

        # Ensure output columns exist
        for name in ["Memory Win%", "Memory Avg ROI%", "Memory Weight"]:
            if name not in sh_idx:
                stats_vals[0].append(name)
                sh_idx[name] = len(stats_vals[0]) - 1

        out_rows = [stats_vals[0]]
        for row in stats_vals[1:]:
            need_len = max(sh_idx["Memory Win%"], sh_idx["Memory Avg ROI%"], sh_idx["Memory Weight"]) + 1
            if len(row) < need_len:
                row = row + [""] * (need_len - len(row))
            token = _normalize(row[sh_idx.get("Token")] if sh_idx.get("Token") is not None else "")
            if token and token in agg:
                wins, total, avg = agg[token]["wins"], agg[token]["total"], agg[token]["sum"]/agg[token]["total"]
                winp = 100.0 * wins / total
                avg_b = max(-50.0, min(200.0, avg))
                base = (winp / 100.0) * 0.7 + ((avg_b + 50.0) / 250.0) * 0.3
                weight = max(0.0, min(1.0, base))
                row[sh_idx["Memory Win%"]] = f"{winp:.1f}"
                row[sh_idx["Memory Avg ROI%"]] = f"{avg:.2f}"
                row[sh_idx["Memory Weight"]] = f"{weight:.3f}"
            out_rows.append(row)

        _update_range(stats_ws, "A1", out_rows)
        print("✅ Memory Weight sync complete.")

    except Exception as e:
        print(f"❌ Error in run_rotation_memory: {e}")
        ping_webhook_debug(f"rotation_memory error: {e}")

if __name__ == "__main__":
    run_rotation_memory()
