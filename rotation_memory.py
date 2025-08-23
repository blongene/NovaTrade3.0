# rotation_memory.py
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
    return {h.strip(): i for i, h in enumerate(headers)}

def _safe_float(x):
    try:
        return float(str(x).replace("%", "").strip())
    except Exception:
        return None

def _normalize(s):
    return (s or "").strip().upper()

def run_rotation_memory():
    """
    Learns per-token win rate & average ROI from Rotation_Log, writes to Rotation_Stats:
      - Memory Win%
      - Memory Avg ROI%
      - Memory Weight (0..1)
    Memory Weight is a bounded transform favoring consistent winners.
    """
    try:
        print("🧠 Running Rotation Memory Sync...")
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
        roi_c = lh.get("ROI %") or lh.get("ROI")
        if tok_c is None or roi_c is None:
            print("⛔️ Rotation_Log must include 'Token' and 'ROI %' (or 'ROI').")
            return

        # Aggregate stats per token
        agg = {}  # token -> {"wins":int,"total":int,"avg":float}
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

        # Ensure columns exist in Rotation_Stats
        for name in ["Memory Win%", "Memory Avg ROI%", "Memory Weight"]:
            if name not in sh_idx:
                stats_vals[0].append(name)
                sh_idx[name] = len(stats_vals[0]) - 1

        # Prepare updates
        out_rows = [stats_vals[0]]
        for r_i, row in enumerate(stats_vals[1:], start=2):
            # Extend row to new columns if needed
            need_len = max(sh_idx["Memory Win%"], sh_idx["Memory Avg ROI%"], sh_idx["Memory Weight"]) + 1
            if len(row) < need_len:
                row = row + [""] * (need_len - len(row))

            token_col = sh_idx.get("Token")
            token = _normalize(row[token_col] if token_col is not None and token_col < len(row) else "")
            if token and token in agg and agg[token]["total"] > 0:
                wins = agg[token]["wins"]
                total = agg[token]["total"]
                avg = agg[token]["sum"] / total if total else 0.0
                winp = 100.0 * wins / total

                # Memory Weight: sigmoid-like but lightweight
                # Bound avg ROI to [-50, +200] for stability, then scale
                avg_b = max(-50.0, min(200.0, avg))
                base = (winp / 100.0) * 0.7 + (avg_b / 200.0) * 0.3
                weight = max(0.0, min(1.0, base))

                row[sh_idx["Memory Win%"]] = f"{winp:.1f}"
                row[sh_idx["Memory Avg ROI%"]] = f"{avg:.2f}"
                row[sh_idx["Memory Weight"]] = f"{weight:.3f}"
            out_rows.append(row)

        # Write back (entire sheet header + data to keep simple & consistent)
        _update_range(stats_ws, "A1", out_rows)
        print("✅ Memory Weight sync complete.")
    except Exception as e:
        print(f"❌ Error in run_rotation_memory: {e}")
        ping_webhook_debug(f"rotation_memory error: {e}")
