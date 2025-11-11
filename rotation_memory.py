from utils import ws_batch_update, get_ws_cached

# rotation_memory.py — patched to accept 'Follow-up ROI' as ROI source
import os
from utils import with_sheet_backoff, get_gspread_client, ping_webhook_debug

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
    idx = {}
    for i, h in enumerate(headers):
        idx[str(h).strip()] = i
    return idx

def _normalize(s): return str(s or "").strip().upper()

def _safe_float(x):
    try:
        s = str(x).replace("%","").replace(",","").strip()
        if s == "" or s.upper() == "N/A":
            return None
        return float(s)
    except Exception:
        return None

def run_rotation_memory():
    try:
        sh = _open_sheet()
        log_ws = sh.worksheet("Rotation_Log")
        stats_ws = sh.worksheet("Rotation_Stats")
        log_vals = _get_all(log_ws)
        stats_vals = _get_all(stats_ws)

        if not log_vals or not stats_vals:
            print("⚠️ Missing data for memory sync.")
            return

        lh = _header_index_map(log_vals[0])
        sh_idx = _header_index_map(stats_vals[0])

        tok_c = lh.get("Token")

        # 🔧 Accept Follow-up ROI as primary, then fallback to 'ROI %' or 'ROI'
        roi_c = None
        for candidate in ("Follow-up ROI", "ROI %", "ROI"):
            if candidate in lh:
                roi_c = lh.get(candidate)
                break

        if tok_c is None or roi_c is None:
            print("⛔️ Rotation_Log must include 'Token' and 'Follow-up ROI' (or 'ROI %' / 'ROI').")
            return

        agg = {}
        for row in log_vals[1:]:
            t = _normalize(row[tok_c] if tok_c < len(row) else "")
            r = _safe_float(row[roi_c] if roi_c < len(row) else "")
            if not t or r is None:
                continue
            a = agg.setdefault(t, {"wins": 0, "total": 0, "sum": 0.0})
            a["total"] += 1
            a["sum"] += r
            if r > 0:
                a["wins"] += 1

        # Ensure columns exist
        for name in ["Memory Win%", "Memory Avg ROI%", "Memory Weight"]:
            if name not in sh_idx:
                stats_vals[0].append(name)
                sh_idx[name] = len(stats_vals[0]) - 1

        out_rows = [stats_vals[0]]
        for row in stats_vals[1:]:
            need_len = max(sh_idx.values()) + 1
            if len(row) < need_len:
                row = row + [""] * (need_len - len(row))

            token = _normalize(row[sh_idx.get("Token")] if "Token" in sh_idx else "")
            if token and token in agg and agg[token]["total"] > 0:
                wins, total, sumv = agg[token]["wins"], agg[token]["total"], agg[token]["sum"]
                avg = sumv / total
                winp = 100.0 * wins / total
                avg_b = max(-50.0, min(200.0, avg))
                base = (winp/100.0)*0.7 + (avg_b/200.0)*0.3
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
