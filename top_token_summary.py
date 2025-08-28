# top_token_summary.py

from datetime import datetime, timezone
from utils import (
    get_ws,
    safe_get_all_records,
    ws_batch_update,
    with_sheet_backoff,
    send_telegram_message_dedup,
    safe_float,
)

SHEET = "Rotation_Stats"
ALERT_KEY = "top_token_summary_daily"

def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def _col_letter(idx_1b: int) -> str:
    n = idx_1b
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def run_top_token_summary():
    print("📈 Running Top Token ROI Summary...")
    try:
        ws = get_ws(SHEET)

        # Read once (rate-limited + optional cache)
        rows = safe_get_all_records(ws, ttl_s=180)
        headers = ws.row_values(1) or []
        hidx = {h: i + 1 for i, h in enumerate(headers)}

        # Ensure "Last Alerted" header exists
        header_changed = False
        if "Last Alerted" not in hidx:
            headers.append("Last Alerted")
            hidx["Last Alerted"] = len(headers)
            ws.update("A1", [headers])  # atomic header write
            header_changed = True

        # Find top winners by Follow-up ROI
        def _tok(x): return (x or "").strip().upper()
        def _num(x): return safe_float(x, default=0.0)

        scored = []
        for r in rows:
            t = _tok(r.get("Token"))
            if not t:
                continue
            roi = _num(r.get("Follow-up ROI"))
            scored.append((t, roi))

        if not scored:
            print("ℹ️ No tokens to score.")
            return

        # Sort desc by ROI, take top 3 (non-zero)
        top = [(t, roi) for t, roi in sorted(scored, key=lambda x: x[1], reverse=True) if roi != 0][:3]

        # Build Telegram message (de-duped for the day)
        if top:
            lines = [f"🏆 <b>Top Follow-up ROI</b> ({_utc_now()} UTC)"]
            for i, (t, roi) in enumerate(top, 1):
                lines.append(f"{i}. <b>{t}</b> — {roi:.2f}%")
            msg = "\n".join(lines)
            # de-dupe key ensures once per ~15 mins (env override) or pass ttl_min=1440 for daily
            send_telegram_message_dedup(msg, key=ALERT_KEY)

        # Stamp "Last Alerted" for the top tokens
        updates = []
        last_alert_col = hidx["Last Alerted"]
        now = _utc_now()
        token_to_row = {}
        for i, r in enumerate(rows, start=2):
            token_to_row[_tok(r.get("Token"))] = i

        for t, _roi in top:
            row_i = token_to_row.get(t)
            if row_i:
                a1 = f"{SHEET}!{_col_letter(last_alert_col)}{row_i}"
                updates.append({"range": a1, "values": [[now]]})

        if updates:
            ws_batch_update(ws, updates)

        sent_n = len(top)
        print(f"✅ Top Token Summary complete. {sent_n} alert(s) sent.")

    except Exception as e:
        print(f"❌ Error in run_top_token_summary: {e}")
