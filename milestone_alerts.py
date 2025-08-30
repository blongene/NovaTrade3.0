# milestone_alerts.py — NT3.0 Render (429-safe)
# Reads Rotation_Log ONCE (cached), computes day/ROI milestones, sends
# Telegram alerts with global de-dupe. No Sheet writes here.

import os, time, random
from datetime import datetime
from utils import (
    get_values_cached, with_sheet_backoff,
    str_or_empty, to_float,
    send_telegram_message_dedup,
)

TAB           = os.getenv("MILES_TAB", "Rotation_Log")
TTL_S         = int(os.getenv("MILES_TTL_SEC", "300"))
JIT_MIN_S     = float(os.getenv("MILES_JIT_MIN_S", "0.2"))
JIT_MAX_S     = float(os.getenv("MILES_JIT_MAX_S", "0.9"))
# Columns (override if your headers differ)
COL_TOKEN     = os.getenv("MILES_COL_TOKEN", "Token")
COL_DAYS      = os.getenv("MILES_COL_DAYS",  "Days Held")
COL_ROI       = os.getenv("MILES_COL_ROI",   "ROI %")

# Milestones (comma-sep). Days: 1,3,7,14,30; ROI: +/-10,25,50,100
DAY_MILES     = [int(x) for x in os.getenv("MILES_DAY_LIST", "1,3,7,14,30").split(",") if x.strip()]
ROI_MILES_POS = [float(x) for x in os.getenv("MILES_ROI_POS", "10,25,50,100").split(",") if x.strip()]
ROI_MILES_NEG = [float(x) for x in os.getenv("MILES_ROI_NEG", "10,20,30,50").split(",") if x.strip()]
# De-dupe window
DEDUP_MIN     = int(os.getenv("MILES_DEDUP_MIN", "360"))  # 6h

def _hmap(header):
    return {str_or_empty(h): i for i, h in enumerate(header)}

def _hit_day_milestone(days: int) -> int|None:
    # closest milestone exactly matched
    return days if days in DAY_MILES else None

def _hit_roi_milestone(roi: float) -> str|None:
    if roi is None: return None
    for thr in sorted(ROI_MILES_POS):
        if roi >= thr: last = thr
        else: break
    else:
        last = ROI_MILES_POS[-1] if ROI_MILES_POS else None
    pos = last if ("last" in locals() and roi >= last) else None

    for thr in sorted(ROI_MILES_NEG):
        if roi <= -thr: lastn = thr
        else: break
    else:
        lastn = ROI_MILES_NEG[-1] if ROI_MILES_NEG else None
    neg = lastn if ("lastn" in locals() and roi <= -lastn) else None

    if pos and (not neg or roi >= 0):
        return f"ROI +{pos:.0f}%"
    if neg and (not pos or roi < 0):
        return f"ROI -{neg:.0f}%"
    return None

@with_sheet_backoff
def run_milestone_alerts():
    print("🚀 Running Milestone Alerts...")
    time.sleep(random.uniform(JIT_MIN_S, JIT_MAX_S))

    vals = get_values_cached(TAB, ttl_s=TTL_S) or []
    if not vals:
        print("ℹ️ Rotation_Log empty; no alerts.")
        return

    h = _hmap(vals[0])
    ti, di, ri = h.get(COL_TOKEN), h.get(COL_DAYS), h.get(COL_ROI)
    miss = [n for n, i in [(COL_TOKEN, ti), (COL_DAYS, di), (COL_ROI, ri)] if i is None]
    if miss:
        print(f"⚠️ Missing columns on {TAB}: {', '.join(miss)}; skipping.")
        return

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    sent = 0
    for row in vals[1:]:
        token = str_or_empty(row[ti] if ti < len(row) else "").upper()
        if not token:
            continue
        days = 0
        try:
            days = int(str_or_empty(row[di] if di < len(row) else "").split(".")[0] or "0")
        except:  # noqa: E722
            pass
        roi = to_float(row[ri] if ri is not None and ri < len(row) else "", default=None)

        d_hit = _hit_day_milestone(days)
        r_hit = _hit_roi_milestone(roi)

        # Compose at most one alert per token per run, ROI prioritized
        if r_hit:
            msg = f"🎯 *Milestone* — `{token}` hit **{r_hit}**  \n📅 {now}"
            if send_telegram_message_dedup(msg, key=f"mile:roi:{token}:{r_hit}", ttl_min=DEDUP_MIN):
                sent += 1
            continue
        if d_hit is not None:
            msg = f"📆 *Hold Milestone* — `{token}` reached **{d_hit}d**  \n📅 {now}"
            if send_telegram_message_dedup(msg, key=f"mile:day:{token}:{d_hit}", ttl_min=DEDUP_MIN):
                sent += 1

    print(f"✅ Milestone Alerts: {sent} message(s) considered (de-duped).")
