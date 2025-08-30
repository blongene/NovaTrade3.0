# presale_scorer.py — NT3.0 Phase-1 Polish
# Cache-first presale scan with batched write & de-duped Telegram prompts.

import os
from datetime import datetime
from utils import (
    get_ws, get_records_cached, ws_batch_update,
    str_or_empty, to_float, send_telegram_prompt, tg_should_send, tg_mark_sent,
    with_sheet_backoff, is_cold_boot
)

TAB = "Presale_Stream"

MAX_PROMPTS   = int(os.getenv("PRESALE_PROMPT_MAX", "5"))
TTL_MIN       = int(os.getenv("PRESALE_PROMPT_TTL_MIN", "180"))  # 3h quiet window
TTL_READ_S    = int(os.getenv("PRESALE_TTL_READ_SEC", "300"))    # cache sheet reads 5m
SCORE_MIN     = float(os.getenv("PRESALE_MIN_SCORE", "2.0"))     # threshold to ping

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _score(row: dict) -> float:
    """Conservative, robust scoring; safe if some fields are missing."""
    # Common columns (use what exists, coerce safely)
    hype     = to_float(row.get("Mentions"), 0)            # social mentions / radar
    tg       = to_float(row.get("TG Followers"), 0)        # telegram
    ct       = to_float(row.get("CT Followers"), 0)        # twitter/x
    kols     = to_float(row.get("KOLs"), 0)                # influencers
    liq      = to_float(row.get("Liquidity"), 0)           # planned/initial
    fdv      = to_float(row.get("FDV"), 0)                 # fully diluted val
    audits   = str_or_empty(row.get("Audit")).lower()      # text: certik/xyz/none
    chain    = str_or_empty(row.get("Chain")).lower()

    s = 0.0
    s += min(hype, 5000) * 0.0004
    s += min(tg,   50000) * 0.00002
    s += min(ct,   100000) * 0.00001
    s += min(kols,   200) * 0.01
    s += min(liq,  200000) * 0.000005
    # penalize very high FDV (worse entry risk)
    if fdv and fdv > 0:
        s -= min(fdv / 1_000_000, 30) * 0.03
    # audit bonus
    if "certik" in audits or "solid" in audits or "hacken" in audits:
        s += 0.5
    # chain nuance (cheap L2s slightly preferred)
    if "base" in chain or "bsc" in chain or "arb" in chain:
        s += 0.2
    return round(s, 2)

def _pick_header(header: list[str], *candidates):
    for c in candidates:
        if c in header:
            return c
    return None

@with_sheet_backoff
def run_presale_scorer():
    if is_cold_boot():
        # avoid redeploy storms hammering the sheet at boot
        print("⏸ presale_scorer skipped (cold boot quiet window).")
        return

    print("🎯 Presale scorer …")
    rows = get_records_cached(TAB, ttl_s=TTL_READ_S) or []
    if not rows:
        print("ℹ️ Presale_Stream empty; skipping.")
        return

    # We’ll optionally write a single timestamp column once per alerted row
    ws = get_ws(TAB)
    header = ws.row_values(1)

    h_token   = _pick_header(header, "Token", "Asset", "Project", "Ticker")
    h_alerted = _pick_header(header, "Scored At", "Alerted At", "Presale Alerted At")
    if not h_token:
        print("⚠️ Missing 'Token/Asset/Project/Ticker' header.")
        return

    # map display row index (2-based) for writeback
    token_to_ridx = {}
    for idx, r in enumerate(rows, start=2):
        t = str_or_empty(r.get(h_token)).upper()
        if t and t not in token_to_ridx:
            token_to_ridx[t] = idx

    # ensure header for writeback
    writes = []
    if not h_alerted:
        h_alerted = "Scored At"
        col_ix = len(header) + 1
        writes.append({"range": f"{_col_letter(col_ix)}1", "values": [[h_alerted]]})
    else:
        col_ix = header.index(h_alerted) + 1

    # score & select
    candidates = []
    for r in rows:
        token = str_or_empty(r.get(h_token)).upper()
        if not token:
            continue
        s = _score(r)
        if s >= SCORE_MIN:
            candidates.append((token, s, r))

    # sort by score desc; cap prompts
    candidates.sort(key=lambda x: x[1], reverse=True)
    sent = 0
    for token, s, r in candidates:
        if sent >= MAX_PROMPTS:
            break
        key = f"presale:{token}"
        if not tg_should_send(f"PRESALE|{token}", key=key, ttl_min=TTL_MIN):
            continue

        liq  = str_or_empty(r.get("Liquidity"))
        fdv  = str_or_empty(r.get("FDV"))
        when = str_or_empty(r.get("TGE") or r.get("Launch Date") or r.get("Date"))

        lines = [
            f"*Presale Candidate*  — score `{s:.2f}`",
            f"*{token}*",
        ]
        if when: lines.append(f"• TGE/Launch: `{when}`")
        if liq:  lines.append(f"• Liquidity: `{liq}`")
        if fdv:  lines.append(f"• FDV: `{fdv}`")
        lines.append("")
        lines.append("Track this presale?")
        msg = "\n".join(lines)

        send_telegram_prompt(
            token_or_title=token,
            message=msg,
            buttons=["YES", "NO"],
            prefix="PRESALE",
            dedupe_key=key,
            ttl_min=TTL_MIN,
        )
        tg_mark_sent(f"PRESALE|{token}", key=key)
        sent += 1

        # optional single writeback: timestamp when we alerted/scored
        ridx = token_to_ridx.get(token)
        if ridx:
            writes.append({
                "range": f"{_col_letter(col_ix)}{ridx}",
                "values": [[datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")]],
            })

    if writes:
        ws_batch_update(ws, writes)

    print(f"✅ presale_scorer: {sent} prompt(s) sent.")
