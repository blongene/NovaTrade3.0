# telegram_summaries.py
import os
from datetime import datetime
from utils import with_sheet_backoff, get_gspread_client, send_telegram_message, ping_webhook_debug

SHEET_URL = os.getenv("SHEET_URL")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def _open_sheet():
    return get_gspread_client().open_by_url(SHEET_URL)

@with_sheet_backoff
def _get_all(ws):
    return ws.get_all_values()

def _header_index_map(headers):
    return {h.strip(): i for i, h in enumerate(headers)}

def _top(n, rows, key_fn):
    scored = []
    for r in rows:
        try:
            scored.append((key_fn(r), r))
        except Exception:
            continue
    scored.sort(key=lambda x: (x[0] is None, -x[0] if x[0] is not None else 0))
    return [r for _, r in scored[:n]]

def _safe_float(x):
    try:
        return float(str(x).replace("%", "").strip())
    except Exception:
        return None

def run_telegram_summary():
    """
    Sends a concise daily snapshot to Telegram:
    - Top 3 tokens by ROI (Rotation_Log)
    - Count of Pending rotations (Rotation_Planner)
    - Last feedback YES/NO counts (Rotation_Stats)
    """
    try:
        if not TELEGRAM_CHAT_ID:
            print("⚠️ TELEGRAM_CHAT_ID not set; skipping summary.")
            return

        sh = _open_sheet()

        # Rotation_Log → top ROI
        log_ws = sh.worksheet("Rotation_Log")
        log_vals = _get_all(log_ws)
        if not log_vals:
            print("⚠️ No Rotation_Log data.")
            return
        log_h = _header_index_map(log_vals[0])
        rows = log_vals[1:]

        roi_col = log_h.get("ROI %") or log_h.get("ROI")
        tok_col = log_h.get("Token")
        top_txt = []
        if roi_col is not None and tok_col is not None:
            top3 = _top(3, rows, key_fn=lambda r: _safe_float(r[roi_col]))
            for r in top3:
                t = r[tok_col]
                v = _safe_float(r[roi_col])
                top_txt.append(f"{t}: {v:.2f}%" if v is not None else f"{t}: n/a")

        # Rotation_Planner → pending
        plan_ws = sh.worksheet("Rotation_Planner")
        plan_vals = _get_all(plan_ws)
        p_h = _header_index_map(plan_vals[0]) if plan_vals else {}
        confirmed_col = p_h.get("Confirmed")
        status_col = p_h.get("Trade Status")  # optional

        pending = 0
        if plan_vals and confirmed_col is not None:
            for r in plan_vals[1:]:
                resp = (r[confirmed_col] if confirmed_col < len(r) else "").strip().upper()
                if resp == "YES":
                    # If Trade Status exists, only count non-executed
                    if status_col is not None and status_col < len(r):
                        if (r[status_col] or "").strip().upper() != "EXECUTED":
                            pending += 1
                    else:
                        pending += 1

        # Rotation_Stats → last feedback counts
        stats_ws = sh.worksheet("Rotation_Stats")
        stats_vals = _get_all(stats_ws)
        fb_yes = fb_no = fb_skip = 0
        if stats_vals:
            s_h = _header_index_map(stats_vals[0])
            lf = s_h.get("Last Feedback")
            if lf is not None:
                for r in stats_vals[1:]:
                    v = (r[lf] if lf < len(r) else "").strip().upper()
                    if v == "YES": fb_yes += 1
                    elif v == "NO": fb_no += 1
                    elif v == "SKIP": fb_skip += 1

        dt = datetime.utcnow().strftime("%Y-%m-%d")
        lines = [f"🧾 <b>Daily Summary — {dt} (UTC)</b>"]
        if top_txt:
            lines.append("🏆 <b>Top ROI (Rotation_Log)</b>")
            for line in top_txt:
                lines.append(f"• {line}")
        lines.append(f"⏳ <b>Pending Rotations:</b> {pending}")
        lines.append(f"🗳 <b>Feedback</b> — YES:{fb_yes}  NO:{fb_no}  SKIP:{fb_skip}")

        send_telegram_message("\n".join(lines))
        print("✅ Telegram summary sent.")
    except Exception as e:
        print(f"❌ telegram summary error: {e}")
        ping_webhook_debug(f"telegram_summaries error: {e}")
