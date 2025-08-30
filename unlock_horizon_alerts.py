# unlock_horizon_alerts.py — NT3.0 Phase-1 Polish
# Alert for tokens that unlock within N days and are not claimed yet.
# - Single cached read from Claim_Tracker
# - De-duped Telegram alerts (per token)
# - Optional single batch write to "Alerted At"

import os
from datetime import datetime, date, timedelta

from utils import (
    get_ws, get_records_cached, ws_batch_update,
    str_or_empty, send_telegram_prompt, tg_should_send, tg_mark_sent,
    with_sheet_backoff,
)

TAB = "Claim_Tracker"

WINDOW_DAYS   = int(os.getenv("UNLOCK_ALERT_WINDOW_DAYS", "7"))
MAX_PROMPTS   = int(os.getenv("UNLOCK_ALERT_MAX", "6"))
DEDUP_TTL_MIN = int(os.getenv("UNLOCK_ALERT_TTL_MIN", "240"))  # 4h quiet window

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _parse_ymd(s: str):
    s = str_or_empty(s)
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None

def _boolish(v) -> bool:
    s = str_or_empty(v).lower()
    return s in {"true", "yes", "y", "1", "claimed", "✅", "done"}

@with_sheet_backoff
def run_unlock_horizon_alerts():
    print("🔔 Running Unlock Horizon Alerts...")

    rows = get_records_cached(TAB, ttl_s=300) or []
    if not rows:
        print("ℹ️ Claim_Tracker empty; skipping.")
        return

    # Fuzzy headers
    # (Adjust/add synonyms if your sheet differs)
    H_TOKEN     = ("Token", "Asset", "Coin")
    H_UNLOCK    = ("Unlock Date", "Unlock_Date", "Unlock At", "Unlock")
    H_CLAIMED   = ("Claimed?", "Claimed")
    H_STATUS    = ("Status",)
    H_ALERTED   = ("Alerted At", "Unlock Alerted At")

    # build name->present map from first row
    # (we use dict access later via r.get(name))
    def pick_name(sample_row: dict, candidates: tuple[str, ...]) -> str | None:
        for name in candidates:
            if name in sample_row:
                return name
        return None

    # Choose header names off the first row’s keys
    keys = rows[0].keys() if rows else []
    sample = {k: None for k in keys}

    name_token   = pick_name(sample, H_TOKEN)
    name_unlock  = pick_name(sample, H_UNLOCK)
    name_claimed = pick_name(sample, H_CLAIMED)
    name_status  = pick_name(sample, H_STATUS)
    name_alerted = pick_name(sample, H_ALERTED)  # may be None

    if not name_token or not name_unlock:
        print("⚠️ Missing required columns (Token/Unlock Date).")
        return

    today = date.today()
    horizon_end = today + timedelta(days=WINDOW_DAYS)

    candidates = []
    for r in rows:
        token   = str_or_empty(r.get(name_token)).upper()
        if not token:
            continue
        unlock  = _parse_ymd(r.get(name_unlock))
        claimed = _boolish(r.get(name_claimed)) if name_claimed else False
        status  = str_or_empty(r.get(name_status))

        if claimed:
            continue
        if not unlock:
            continue
        if unlock < today:  # already unlocked → other flows handle
            continue
        if unlock > horizon_end:
            continue

        # only alert if sheet isn't already telling us it's claimed/resolved
        candidates.append({
            "token": token,
            "unlock": unlock,
            "row": r,  # keep whole row for future fields if needed
        })

    if not candidates:
        print("✅ No unlocks within window.")
        return

    # Prepare a single ws + header row for optional writeback
    ws = get_ws(TAB)
    header = ws.row_values(1)
    if name_alerted and name_alerted in header:
        alerted_col = header.index(name_alerted) + 1
        add_header = False
    else:
        # if column missing, we’ll add it once (to the right)
        alerted_col = len(header) + 1
        add_header = True
        name_alerted = name_alerted or "Alerted At"

    # Send alerts (de-duped), capped
    candidates.sort(key=lambda c: c["unlock"])  # soonest first
    sent = 0
    writes = []
    if add_header:
        writes.append({"range": f"{_col_letter(alerted_col)}1", "values": [[name_alerted]]})

    # Build a quick row index map: token -> row index (2-based)
    # Using cached records, we can reconstruct position reliably
    token_to_ridx = {}
    for idx, r in enumerate(rows, start=2):
        t = str_or_empty(r.get(name_token)).upper()
        if t and t not in token_to_ridx:
            token_to_ridx[t] = idx

    for c in candidates:
        if sent >= MAX_PROMPTS:
            break
        token  = c["token"]
        unlock = c["unlock"]

        key = f"unlock_alert:{token}"
        if not tg_should_send(f"UNLOCK|{token}", key=key, ttl_min=DEDUP_TTL_MIN):
            continue

        msg = (
            f"*Upcoming Unlock Window*\n\n"
            f"*{token}*\n"
            f"• Unlock date: `{unlock.isoformat()}`\n"
            f"• Window: next {WINDOW_DAYS} day(s)\n\n"
            f"Tap *YES* to mark as handled/snooze after you’ve scheduled the claim."
        )
        # Use inline YES/NO for consistency with other flows
        send_telegram_prompt(
            token_or_title=token,
            message=msg,
            buttons=["YES", "NO"],
            prefix="UNLOCK",
            dedupe_key=key,
            ttl_min=DEDUP_TTL_MIN,
        )
        tg_mark_sent(f"UNLOCK|{token}", key=key)
        sent += 1

        # Optional writeback: Alerted At timestamp
        ridx = token_to_ridx.get(token)
        if ridx:
            writes.append({
                "range": f"{_col_letter(alerted_col)}{ridx}",
                "values": [[datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")]],
            })

    if writes:
        ws_batch_update(ws, writes)

    print(f"✅ Unlock horizon alerts: {sent} prompt(s) sent.")
