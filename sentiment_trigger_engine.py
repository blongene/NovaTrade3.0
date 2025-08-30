# sentiment_trigger_engine.py — NT3.0 quota-calm
# Scans a sentiment sheet for triggerable tokens, marks them as triggered,
# and (optionally) pings Telegram — all with one cached read + one batch write.

import os, time, random
from utils import (
    get_values_cached, get_ws, ws_batch_update, with_sheet_backoff,
    str_or_empty, send_telegram_message_dedup
)

# ---------- Config (override via ENV) ----------
TAB              = os.getenv("SENTIMENT_TAB", "Sentiment_Radar")
TTL_READ_S       = int(os.getenv("SENTIMENT_TTL_READ_SEC", "300"))   # 5m cache
JITTER_MIN_S     = float(os.getenv("SENTIMENT_JITTER_MIN_S", "0.4"))
JITTER_MAX_S     = float(os.getenv("SENTIMENT_JITTER_MAX_S", "1.6"))
MAX_UPDATES      = int(os.getenv("SENTIMENT_MAX_UPDATES", "200"))

# Column names (case-sensitive to match your sheet headers)
COL_TOKEN        = os.getenv("SENTIMENT_COL_TOKEN", "Token")
COL_MENTIONS     = os.getenv("SENTIMENT_COL_MENTIONS", "Mentions")          # numeric
COL_SIGNAL       = os.getenv("SENTIMENT_COL_SIGNAL", "Signal")              # e.g., BUY/REB
COL_LASTTRIG     = os.getenv("SENTIMENT_COL_LAST_TRIGGERED", "Last Trigger")
COL_TRIG_FLAG    = os.getenv("SENTIMENT_COL_TRIGGERED_FLAG", "Triggered?")  # YES/blank

# Simple trigger policy (tweak)
MIN_MENTIONS     = float(os.getenv("SENTIMENT_MIN_MENTIONS", "3"))
VALID_SIGNALS    = { s.strip().upper() for s in os.getenv("SENTIMENT_VALID_SIGNALS", "BUY,REB,REBUY").split(",") }

# Telegram
ENABLE_TG        = os.getenv("SENTIMENT_TG_ENABLED", "true").lower() == "true"
TG_KEY_PREFIX    = os.getenv("SENTIMENT_TG_KEY_PREFIX", "sentiment_rebuy")  # dedupe bucket

# ---------- Helpers ----------
def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

def _to_float(v, default=None):
    try:
        s = str(v).replace("%","").replace(",","").strip()
        return float(s) if s else default
    except Exception:
        return default

# ---------- Main ----------
@with_sheet_backoff
def run_sentiment_trigger_engine():
    print("🧠 Running Sentiment-Triggered Rebuy Engine...")
    time.sleep(random.uniform(JITTER_MIN_S, JITTER_MAX_S))  # de-sync from neighbors

    vals = get_values_cached(TAB, ttl_s=TTL_READ_S) or []
    if not vals:
        print(f"ℹ️ {TAB} empty; skipping.")
        return

    header = vals[0]
    hidx   = {h: i+1 for i, h in enumerate(header)}

    missing = [c for c in (COL_TOKEN, COL_MENTIONS, COL_SIGNAL, COL_TRIG_FLAG, COL_LASTTRIG) if c not in hidx]
    if missing:
        print(f"⚠️ Missing columns in {TAB}: {', '.join(missing)}; skipping.")
        return

    tok_c     = hidx[COL_TOKEN]-1
    ment_c    = hidx[COL_MENTIONS]-1
    sig_c     = hidx[COL_SIGNAL]-1
    trig_c    = hidx[COL_TRIG_FLAG]-1
    last_c    = hidx[COL_LASTTRIG]-1

    writes = []
    touched = 0
    now_iso = time.strftime("%Y-%m-%d %H:%M:%S")

    # Build updates & Telegram messages in one pass
    for r_idx, row in enumerate(vals[1:], start=2):
        token = str_or_empty(row[tok_c] if tok_c < len(row) else "").upper()
        if not token:
            continue

        mentions = _to_float(row[ment_c] if ment_c < len(row) else None, default=0.0)
        signal   = str_or_empty(row[sig_c] if sig_c < len(row) else "").upper()
        already  = str_or_empty(row[trig_c] if trig_c < len(row) else "").upper() == "YES"

        if already:
            continue
        if mentions is None or mentions < MIN_MENTIONS:
            continue
        if signal not in VALID_SIGNALS:
            continue

        # Mark as triggered + set timestamp
        writes.append({"range": f"{_col_letter(trig_c+1)}{r_idx}", "values": [["YES"]]})
        writes.append({"range": f"{_col_letter(last_c+1)}{r_idx}", "values": [[now_iso]]})
        touched += 1

        # Telegram (de-duped per token+signal)
        if ENABLE_TG:
            msg = f"🟢 Sentiment Trigger\nToken: *{token}*\nSignal: *{signal}*  | Mentions: *{int(mentions)}*\nAction: Consider REBUY."
            send_telegram_message_dedup(msg, key=f"{TG_KEY_PREFIX}:{token}:{signal}", ttl_min=60)

        if touched >= MAX_UPDATES:
            break

    if not writes:
        print("✅ Sentiment engine: no new triggers.")
        return

    ws = get_ws(TAB)  # open only when we actually write
    ws_batch_update(ws, writes)
    print(f"✅ Sentiment engine: marked {touched} new trigger(s).")
