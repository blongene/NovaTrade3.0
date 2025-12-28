# sentiment_log_advisory.py — Phase 22A (advisory only)
# Writes sparse breadcrumbs + notable sentiment observations to Sentiment_Log.

import os, time, math
from typing import Any, Dict, List, Tuple

from utils import (
    get_records_cached,
    ensure_sheet_headers,
    get_ws_cached,
    ws_append_row,
    to_float,
    warn,
)

TAB = os.getenv("SENTIMENT_LOG_TAB", "Sentiment_Log")
ENABLED = os.getenv("SENTIMENT_LOG_ADVISORY_ENABLED", "1").lower() in {"1","true","yes","on"}
MAX_ROWS = int(os.getenv("SENTIMENT_LOG_MAX_ROWS", "20"))
MIN_MENTIONS = float(os.getenv("SENTIMENT_LOG_MIN_MENTIONS", "1"))
TTL_SEC = int(os.getenv("SENTIMENT_LOG_TTL_SEC", "21600"))  # 6h per token

def _now_utc() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def _headers() -> List[str]:
    return [
        "Timestamp",
        "Token",
        "Mentions",
        "Signal",
        "Source",
        "Window",
        "Advisory",
        "Phase",
        "Notes",
    ]

def _is_nan(x: Any) -> bool:
    try:
        return math.isnan(float(x))
    except Exception:
        return False

def _recent_written(existing: List[Dict[str, Any]]) -> Dict[str, float]:
    out={}
    for r in existing[-400:]:
        tok=str(r.get("Token","")).strip().upper()
        ts=str(r.get("Timestamp","")).strip()
        if not tok or not ts:
            continue
        try:
            t=time.mktime(time.strptime(ts,"%Y-%m-%d %H:%M:%S"))
        except Exception:
            continue
        if tok not in out or t>out[tok]:
            out[tok]=t
    return out

def run_sentiment_log_advisory() -> None:
    if not ENABLED:
        return

    ensure_sheet_headers(TAB, _headers())

    # Prefer Sentiment_Summary as it’s already aggregated and cheap to read
    rows = get_records_cached("Sentiment_Summary", ttl_s=180) or []
    existing = get_records_cached(TAB, ttl_s=180) or []
    recent = _recent_written(existing)
    now_epoch=time.time()

    notable: List[Tuple[str,float,float,str]] = []

    for r in rows[-500:]:
        tok = str(r.get("Token","") or r.get("Symbol","") or r.get("Ticker","")).strip().upper()
        if not tok:
            continue
        mentions = to_float(r.get("Mentions")) or 0.0
        signal = r.get("Signal")
        sigf = to_float(signal) if signal is not None and not _is_nan(signal) else None

        if mentions >= MIN_MENTIONS or (sigf is not None and abs(sigf) > 0):
            notable.append((tok, mentions, sigf if sigf is not None else float("nan"), str(r.get("Source","summary"))))

    if not notable:
        _breadcrumb("No non-zero sentiment observations (likely rate-limited / quiet period).")
        return

    # Sort by mentions desc then abs(signal) desc
    def key(t):
        tok, m, s, src = t
        sval = 0 if _is_nan(s) else abs(s)
        return (m, sval)
    notable.sort(key=key, reverse=True)

    ws=get_ws_cached(TAB, ttl_s=60)
    written=0
    for tok, mentions, sig, src in notable:
        if written>=MAX_ROWS:
            break
        last=recent.get(tok)
        if last and (now_epoch-last)<TTL_SEC:
            continue

        notes=[]
        if mentions < MIN_MENTIONS:
            notes.append("low_mentions")
        if _is_nan(sig):
            notes.append("signal_nan")

        row=[
            _now_utc(),
            tok,
            mentions,
            "" if _is_nan(sig) else sig,
            src,
            "summary_tail",
            "TRUE",
            "22A",
            ",".join(notes) if notes else "ok",
        ]
        try:
            ws_append_row(ws,row)
            recent[tok]=now_epoch
            written+=1
        except Exception as e:
            warn(f"Sentiment_Log append failed: {e}")
            break

    if written==0:
        _breadcrumb("Notable sentiment existed but dedupe TTL suppressed writes (normal).")
    else:
        _breadcrumb(f"Wrote {written} sentiment advisory row(s).", advisory=False)

def _breadcrumb(msg: str, advisory: bool = True) -> None:
    try:
        ws=get_ws_cached(TAB, ttl_s=60)
        row=[
            _now_utc(),
            "",
            "",
            "",
            "system",
            "breadcrumb",
            "TRUE" if advisory else "FALSE",
            "22A",
            msg,
        ]
        ws_append_row(ws,row)
    except Exception as e:
        warn(f"Sentiment_Log breadcrumb failed: {e}")
