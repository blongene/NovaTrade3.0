# rotation_feedback_engine.py — NT3.0 Phase-1 Polish
# Reads Rotation_Log once, computes which rows need re-vote prompts,
# sends de-duped Telegram prompts, optionally stamps a single "Prompted At" column.
# All writes batched; boot quiet-window aware.

import os
from datetime import datetime, timedelta
from utils import (
    get_ws, get_records_cached, ws_batch_update,
    str_or_empty, send_telegram_prompt, tg_should_send, tg_mark_sent,
    with_sheet_backoff, is_cold_boot,
)

TAB = "Rotation_Log"
PROMPT_COL_NAME = os.getenv("ROT_FEEDBACK_PROMPT_COL", "Prompted At")
TTL_READ_S      = int(os.getenv("ROT_FEEDBACK_TTL_READ_SEC", "300"))
DEDUP_TTL_MIN   = int(os.getenv("ROT_FEEDBACK_DEDUP_MIN", "240"))
MAX_PROMPTS     = int(os.getenv("ROT_FEEDBACK_MAX_PROMPTS", "8"))
MIN_DAYS_HELD   = float(os.getenv("ROT_FEEDBACK_MIN_DAYS", "2"))     # only prompt after N days held
RECHECK_NEG_ROI = float(os.getenv("ROT_FEEDBACK_NEG_ROI", "-10"))    # prompt if Follow-up ROI <= this

def _col_letter(n: int) -> str:
    s = ""; 
    while n: n, r = divmod(n-1, 26); s = chr(65+r)+s
    return s

@with_sheet_backoff
def run_rotation_feedback_engine():
    if is_cold_boot():
        print("⏸ rotation_feedback_engine skipped (cold boot quiet window).")
        return

    print("▶ Rotation feedback engine …")

    rows = get_records_cached(TAB, ttl_s=TTL_READ_S) or []
    if not rows:
        print("ℹ️ Rotation_Log empty; skipping.")
        return

    ws = get_ws(TAB)
    header = ws.row_values(1)

    # required/optional columns
    def _ix(name):  return header.index(name)+1 if name in header else None
    c_token     = _ix("Token")
    c_init_roi  = _ix("Initial ROI")
    c_fup_roi   = _ix("Follow-up ROI")
    c_decision  = _ix("Decision")
    c_days      = _ix("Days Held")
    c_status    = _ix("Status")
    c_userresp  = _ix("User Response")
    c_confirm   = _ix("Confirmed")
    c_prompted  = _ix(PROMPT_COL_NAME)

    if not c_token:
        print("⚠️ Missing 'Token' column in Rotation_Log.")
        return

    writes = []
    add_header = False
    if not c_prompted:
        c_prompted = len(header) + 1
        add_header = True
        writes.append({"range": f"{_col_letter(c_prompted)}1", "values": [[PROMPT_COL_NAME]]})

    # build revote candidates
    candidates = []
    for r in rows:
        token = str_or_empty(r.get("Token")).upper()
        if not token:
            continue
        decision = str_or_empty(r.get("Decision")).upper()
        if decision != "YES":
            continue

        days = float(str(r.get("Days Held") or "0").replace(",","") or 0)
        if days < MIN_DAYS_HELD:
            continue

        follow = r.get("Follow-up ROI")
        try:
            f_roi = float(str(follow).replace("%","").replace(",","")) if follow not in (None, "") else None
        except Exception:
            f_roi = None

        userresp = str_or_empty(r.get("User Response")).upper()
        confirmed = str_or_empty(r.get("Confirmed")).upper()

        # if already has a user response/confirmed, skip
        if userresp in {"YES","NO"} or confirmed in {"YES","NO"}:
            continue

        # prompt if ROI is sufficiently negative or stale
        should_prompt = (f_roi is not None and f_roi <= RECHECK_NEG_ROI) or (f_roi is None and days >= (MIN_DAYS_HELD+1))
        if should_prompt:
            candidates.append((token, f_roi, days))

    # send prompts (de-duped), then stamp Prompted At for those rows
    # build a token → row index map
    token_to_idx = {}
    for idx, r in enumerate(rows, start=2):
        t = str_or_empty(r.get("Token")).upper()
        if t and t not in token_to_idx:
            token_to_idx[t] = idx

    sent = 0
    for token, f_roi, days in sorted(candidates, key=lambda x: (x[1] if x[1] is not None else 1e9)):
        if sent >= MAX_PROMPTS:
            break
        key = f"rot_feedback:{token}"
        if not tg_should_send(f"ROTFEED|{token}", key=key, ttl_min=DEDUP_TTL_MIN):
            continue

        roi_str = "unknown" if f_roi is None else f"{f_roi:.2f}%"
        msg = (f"*Re-Vote Needed*\n\n"
               f"*{token}*\n"
               f"• Days held: `{days:.1f}`\n"
               f"• Follow-up ROI: `{roi_str}`\n\n"
               "Rotate out or keep holding?")
        send_telegram_prompt(
            token_or_title=token,
            message=msg,
            buttons=[["YES","NO"],["HOLD"]],
            prefix="RE-VOTE",
            dedupe_key=key,
            ttl_min=DEDUP_TTL_MIN,
        )
        tg_mark_sent(f"ROTFEED|{token}", key=key)
        sent += 1

        idx = token_to_idx.get(token)
        if idx:
            writes.append({"range": f"{_col_letter(c_prompted)}{idx}",
                           "values": [[datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")]]})

    if writes:
        ws_batch_update(ws, writes)

    print(f"✅ rotation_feedback_engine: {sent} prompt(s) sent.")
