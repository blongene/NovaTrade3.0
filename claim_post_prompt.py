# claim_post_prompt.py — NovaTrade 3.0 (Phase-1 Polish)
# - Single cached read from Claim_Tracker
# - Batched write (Prompted At) when available
# - Telegram inline prompt with global de-dup (no spam)
# - Backward compat: run_claim_decision_prompt() is the entrypoint

import os
from datetime import datetime, date

from utils import (
    get_ws, get_values_cached, ws_batch_update, str_or_empty,
    send_telegram_prompt, tg_should_send, tg_mark_sent,
)

# ===== helpers =====
def _a1(col_idx: int, row_idx: int) -> str:
    n = col_idx
    letters = ""
    while n:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return f"{letters}{row_idx}"

def _first_present(header: list[str], *names):
    for n in names:
        if n in header:
            return n
    return None

def _boolish(v) -> bool:
    s = str_or_empty(v).lower()
    return s in {"true", "yes", "y", "1", "claimed", "✅", "done"}

def _parse_date_yyyy_mm_dd(s: str):
    s = str_or_empty(s)
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

# ===== main =====
def run_claim_decision_prompt():
    """
    Sends a CLAIM prompt (with inline YES/NO) for tokens that are:
      - Claimable == TRUE  AND  Claimed? != true
      OR
      - Status == "Claim Now" AND Claimed? != true
      AND
      - Unlock Date not in the future (if present)
    De-duped per token using a persistent key; optional Prompted At write-back.
    """
    # soft controls via env
    max_prompts = int(os.getenv("CLAIM_PROMPT_MAX", "5"))
    ttl_min     = int(os.getenv("CLAIM_PROMPT_TTL_MIN", "60"))  # default 60 min quiet window

    vals = get_values_cached("Claim_Tracker", ttl_s=120)
    if not vals:
        return

    hdr = vals[0]
    rows = vals[1:]

    # Fuzzy headers
    h_token      = _first_present(hdr, "Token", "Asset", "Coin")
    h_claimable  = _first_present(hdr, "Claimable", "Is Claimable", "Ready")
    h_claimed    = _first_present(hdr, "Claimed?", "Claimed")
    h_status     = _first_present(hdr, "Status")
    h_unlock     = _first_present(hdr, "Unlock Date", "Unlock_Date", "Unlock At")
    h_arrival    = _first_present(hdr, "Arrival Date", "Arrival_Date", "Arrived At")
    h_prompted   = _first_present(hdr, "Prompted At", "PromptedAt", "Prompted")

    # Build header -> 1-based col index
    idx = {name: i + 1 for i, name in enumerate(hdr)}

    candidates = []
    today = date.today()

    for rnum, row in enumerate(rows, start=2):
        def _get(name):
            if not name:
                return ""
            ci = idx.get(name, 0) - 1
            return row[ci] if 0 <= ci < len(row) else ""

        token   = str_or_empty(_get(h_token)).upper()
        if not token:
            continue

        claimable = str_or_empty(_get(h_claimable)).upper() == "TRUE" if h_claimable else False
        claimed   = _boolish(_get(h_claimed)) if h_claimed else False
        status    = str_or_empty(_get(h_status))
        unlock_dt = _parse_date_yyyy_mm_dd(_get(h_unlock)) if h_unlock else None
        arrival   = str_or_empty(_get(h_arrival))

        # Skip future unlocks if date present
        if unlock_dt and unlock_dt > today:
            continue

        # Candidate rule
        if claimed:
            continue
        if not (claimable or status == "Claim Now"):
            continue

        days_since_unlock = ""
        if unlock_dt:
            days_since_unlock = str((today - unlock_dt).days)

        candidates.append({
            "rnum": rnum,
            "token": token,
            "arrival": arrival,
            "unlock_dt": unlock_dt,
            "days_since_unlock": days_since_unlock,
        })

    if not candidates:
        return

    # sort oldest-first to prioritize long-waiting claims
    candidates.sort(key=lambda c: (9999 if c["days_since_unlock"] == "" else int(c["days_since_unlock"])), reverse=True)

    # send prompts (cap by max_prompts)
    sent = 0
    writes = []
    for c in candidates:
        if sent >= max_prompts:
            break

        token = c["token"]
        key   = f"claim_prompt:{token}"

        # Global de-dup: skip if recently sent
        if not tg_should_send(f"CLAIM|{token}", key=key, ttl_min=ttl_min):
            continue

        msg_lines = []
        msg_lines.append(f"*{token}* appears ready to claim.")
        if c["unlock_dt"]:
            msg_lines.append(f"• Unlock date: `{c['unlock_dt'].isoformat()}`")
        if c["days_since_unlock"] != "":
            msg_lines.append(f"• Days since unlock: *{c['days_since_unlock']}*")
        if c["arrival"]:
            msg_lines.append(f"• Arrival seen: `{c['arrival']}`")
        msg_lines.append("")
        msg_lines.append("Tap *YES* after you complete the on-chain claim. Tap *NO* to snooze for now.")

        # Inline prompt (YES/NO callbacks, prefix namespaces to avoid handler confusion)
        send_telegram_prompt(
            token_or_title=token,
            message="\n".join(msg_lines),
            buttons=["YES", "NO"],
            prefix="CLAIM",
            dedupe_key=key,
            ttl_min=ttl_min,
        )
        tg_mark_sent(f"CLAIM|{token}", key=key)
        sent += 1

        # Optional: write Prompted At if the column exists (batched)
        if h_prompted:
            col = idx[h_prompted]
            writes.append({
                "range": f"Claim_Tracker!{_a1(col, c['rnum'])}",
                "values": [[datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")]],
            })

    # Single round-trip write
    if writes:
        ws = get_ws("Claim_Tracker")
        ws_batch_update(ws, writes)

if __name__ == "__main__":
    run_claim_decision_prompt()
