# rebuy_engine.py — NT3.0 Phase-1 Polish
# Finds undersized positions vs Target %, sends capped de-duped prompts. No sheet writes here.

import os
from utils import (
    get_records_cached, str_or_empty, to_float,
    send_telegram_prompt, tg_should_send, tg_mark_sent
)

MAX_PROMPTS = int(os.getenv("REBUY_PROMPT_MAX", "5"))
TTL_MIN     = int(os.getenv("REBUY_PROMPT_TTL_MIN", "120"))  # quiet window
THRESHOLD_PCT = float(os.getenv("REBUY_UNDERSIZED_THRESH_PCT", "0.5"))  # e.g. current < 50% of target

def _pct(v):
    f = to_float(v)
    return f if f is not None else None

def _load_targets():
    # Expect Portfolio_Targets with Token / Target % / Current %
    targets = get_records_cached("Portfolio_Targets", ttl_s=300) or []
    out = {}
    for r in targets:
        t = str_or_empty(r.get("Token")).upper()
        tgt = _pct(r.get("Target %"))
        cur = _pct(r.get("Current %"))  # if not present, this will be None
        if t and tgt is not None and cur is not None:
            out[t] = (tgt, cur)
    return out

def _load_stats():
    stats = get_records_cached("Rotation_Stats", ttl_s=300) or []
    return {str_or_empty(r.get("Token")).upper(): str_or_empty(r.get("Decision")).upper() for r in stats if r.get("Token")}

def run_undersized_rebuy():
    print("🔁 Undersized rebuy engine …")
    targets = _load_targets()
    decisions = _load_stats()

    if not targets:
        print("ℹ️ No targets with current% available.")
        return

    candidates = []
    for token, (tgt, cur) in targets.items():
        if tgt <= 0:
            continue
        ratio = (cur / tgt) if tgt else 1.0
        if ratio < THRESHOLD_PCT and decisions.get(token) == "YES":
            deficit = tgt - cur
            candidates.append((token, tgt, cur, deficit, ratio))

    # sort largest deficit first
    candidates.sort(key=lambda x: x[3], reverse=True)

    sent = 0
    for token, tgt, cur, deficit, ratio in candidates:
        if sent >= MAX_PROMPTS:
            break
        key = f"rebuy_prompt:{token}"
        if not tg_should_send(f"REBUY|{token}", key=key, ttl_min=TTL_MIN):
            continue

        msg = (
            f"*Undersized Position Detected*\n\n"
            f"*{token}*\n"
            f"• Target: `{tgt:.2f}%`\n"
            f"• Current: `{cur:.2f}%`\n"
            f"• Deficit: `-{deficit:.2f}%`  (at {ratio*100:.1f}% of target)\n\n"
            f"Proceed with a top-up?"
        )
        send_telegram_prompt(
            token_or_title=token,
            message=msg,
            buttons=["YES", "NO"],
            prefix="REBUY",
            dedupe_key=key,
            ttl_min=TTL_MIN,
        )
        tg_mark_sent(f"REBUY|{token}", key=key)
        sent += 1

    print(f"✅ Undersized rebuy engine: {sent} prompt(s) sent.")
