# top_token_summary.py — NT3.0 quota-safe + compat
from utils import (
    get_values_cached,          # cached get_all_values() or ranged reads
    send_telegram_message_if_new,
    str_or_empty, to_float, info, warn,
)

SRC_TAB = "Rotation_Stats"
TTL_S   = 180  # cache to avoid 429s

def run_top_token_summary():
    info("▶ Top token summary")
    vals = get_values_cached(SRC_TAB, ttl_s=TTL_S) or []
    if not vals:
        warn(f"{SRC_TAB} empty; skipping.")
        return

    header = vals[0]
    rows   = vals[1:]

    def hidx(name, default=None):
        try:
            return header.index(name)
        except ValueError:
            return default

    tok_i = hidx("Token")
    roi_i = hidx("Follow-up ROI") if hidx("Follow-up ROI") is not None else hidx("Follow-up ROI (%)")

    if tok_i is None or roi_i is None:
        warn(f"{SRC_TAB} missing columns: Token and/or Follow-up ROI; skipping.")
        return

    leaders = []
    for r in rows:
        t = str_or_empty(r[tok_i] if tok_i < len(r) else "").upper()
        if not t:
            continue
        roi = to_float(r[roi_i] if roi_i < len(r) else "", default=None)
        if roi is None:
            continue
        leaders.append((t, roi))

    leaders.sort(key=lambda x: x[1], reverse=True)
    top = leaders[:5]

    if not top:
        warn("No tokens with ROI to summarize.")
        return

    msg = "📈 *Top Follow-up ROI*\n" + "\n".join([f"• {t}: {roi:.2f}%" for t, roi in top])
    # de-duped by message text hash
    send_telegram_message_if_new(msg)
