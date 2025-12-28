# scout_decisions_advisory.py — Phase 22A (advisory only)
# Emits SHADOW scout decisions to "Scout Decisions" without triggering planner imports.

import os, time
from typing import Any, Dict, List, Set

from utils import (
    get_records_cached,
    ensure_sheet_headers,
    get_ws_cached,
    ws_append_row,
    warn,
)

TAB = os.getenv("SCOUT_DECISIONS_TAB", "Scout Decisions")
ENABLED = os.getenv("SCOUT_DECISIONS_ADVISORY_ENABLED", "1").lower() in {"1","true","yes","on"}
MAX_ROWS = int(os.getenv("SCOUT_DECISIONS_MAX_ROWS", "25"))
TTL_SEC  = int(os.getenv("SCOUT_DECISIONS_TTL_SEC", "21600"))  # 6h dedupe

def _now_utc() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def _headers() -> List[str]:
    return [
        "Timestamp",
        "Token",
        "Decision",
        "Source",
        "Score",
        "Sentiment",
        "Market Cap",
        "Scout URL",
        "Decision State",
        "Advisory",
        "Phase",
        "Notes",
    ]

def _existing_tokens(rows: List[Dict[str, Any]]) -> Set[str]:
    out=set()
    for r in rows[-500:]:
        tok=str(r.get("Token","")).strip().upper()
        if tok:
            out.add(tok)
    return out

def run_scout_decisions_advisory() -> None:
    if not ENABLED:
        return

    ensure_sheet_headers(TAB, _headers())

    # Candidate sources: Rotation_Planner (inbox) and Snorter_Stream (new listings)
    planner = get_records_cached("Rotation_Planner", ttl_s=120) or []
    snorter = get_records_cached("Snorter_Stream", ttl_s=120) or []

    existing = get_records_cached(TAB, ttl_s=120) or []
    have = _existing_tokens(existing)

    # Build candidates in priority order (snorter first, then planner)
    candidates: List[Dict[str, Any]] = []

    for r in snorter:
        tok = str(r.get("Token","") or r.get("Symbol","") or r.get("Ticker","")).strip().upper()
        if not tok or tok in have:
            continue
        candidates.append({
            "Token": tok,
            "Source": str(r.get("Source","SNORTER") or "SNORTER"),
            "Score": r.get("Score",""),
            "Sentiment": r.get("Sentiment",""),
            "Market Cap": r.get("Market Cap","") or r.get("Mcap",""),
            "Scout URL": r.get("URL","") or r.get("Scout URL",""),
            "Notes": "shadow ingest from Snorter_Stream",
        })

    for r in planner:
        tok = str(r.get("Token","") or r.get("Symbol","") or r.get("Ticker","")).strip().upper()
        if not tok or tok in have:
            continue
        src = str(r.get("Source","") or r.get("Signal","") or "PLANNER").strip()
        candidates.append({
            "Token": tok,
            "Source": src or "PLANNER",
            "Score": r.get("Score",""),
            "Sentiment": r.get("Sentiment",""),
            "Market Cap": r.get("Market Cap",""),
            "Scout URL": r.get("Scout URL","") or r.get("Contract",""),
            "Notes": "shadow from Rotation_Planner",
        })

    if not candidates:
        _breadcrumb("No new tokens found for scout shadow decisions.")
        return

    ws = get_ws_cached(TAB, ttl_s=60)
    written = 0

    for c in candidates:
        if written >= MAX_ROWS:
            break
        tok = c["Token"]
        # IMPORTANT: Decision must NOT be in {"YES","VAULT","ROTATE"} or it could be imported into planner.
        decision = "SHADOW"
        row = [
            _now_utc(),
            tok,
            decision,
            c.get("Source",""),
            c.get("Score",""),
            c.get("Sentiment",""),
            c.get("Market Cap",""),
            c.get("Scout URL",""),
            "shadow",
            "TRUE",
            "22A",
            c.get("Notes",""),
        ]
        try:
            ws_append_row(ws, row)
            have.add(tok)
            written += 1
        except Exception as e:
            warn(f"Scout Decisions append failed: {e}")
            break

    if written == 0:
        _breadcrumb("Candidates existed but no rows written (unexpected).")
    else:
        _breadcrumb(f"Wrote {written} scout shadow decision row(s).", advisory=False)

def _breadcrumb(msg: str, advisory: bool = True) -> None:
    try:
        ws = get_ws_cached(TAB, ttl_s=60)
        row = [
            _now_utc(),
            "",
            "BREADCRUMB",
            "system",
            "",
            "",
            "",
            "",
            "breadcrumb",
            "TRUE" if advisory else "FALSE",
            "22A",
            msg,
        ]
        ws_append_row(ws, row)
    except Exception as e:
        warn(f"Scout Decisions breadcrumb failed: {e}")
