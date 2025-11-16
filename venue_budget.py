"""
venue_budget.py  — C-Series v1

Computes per-venue USD budgets for quote assets based on Unified_Snapshot.

Sheet schema (Unified_Snapshot):
    Timestamp, Venue, Asset, Free, Locked, Total, IsQuote, QuoteSymbol, Equity_USD

We reduce the spendable budget using:
    POLICY_KEEPBACK_USD
    POLICY_MIN_QUOTE_RESERVE_USD

API:
    get_budget_for_intent(intent: dict) -> (budget_usd: float | None, reason: str)
"""

from __future__ import annotations

import os
import json
import time
from typing import Dict, Any, Tuple, Optional

try:
    import gspread  # type: ignore
    from google.oauth2.service_account import Credentials  # type: ignore
except Exception:  # pragma: no cover
    gspread = None
    Credentials = None


SHEET_URL = os.getenv("SHEET_URL", "").strip()
UNIFIED_SNAPSHOT_WS = os.getenv("UNIFIED_SNAPSHOT_WS", "Unified_Snapshot")

POLICY_KEEPBACK_USD = float(os.getenv("POLICY_KEEPBACK_USD", "5") or 0)
POLICY_MIN_QUOTE_RESERVE_USD = float(os.getenv("POLICY_MIN_QUOTE_RESERVE_USD", "0") or 0)

VENUE_BUDGET_CACHE_TTL_SEC = int(os.getenv("VENUE_BUDGET_CACHE_TTL_SEC", "60"))

_budget_cache: Dict[str, Any] = {
    "ts": 0.0,
    "rows": [],
    "by_venue_quote": {},  # key: f"{VENUE}:{QUOTE}" -> total_equity_usd
}


def _read_service_json_from_env() -> str:
    """
    Load service account JSON from one of:
      GOOGLE_CREDS_JSON_PATH, GOOGLE_APPLICATION_CREDENTIALS, SVC_JSON.
    Returns raw JSON string or "".
    """
    for key in ("GOOGLE_CREDS_JSON_PATH", "GOOGLE_APPLICATION_CREDENTIALS"):
        p = os.getenv(key, "").strip()
        if p and os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return f.read()
            except Exception:
                pass

    svc = os.getenv("SVC_JSON", "").strip()
    if svc:
        # if it's a path, read it; else assume JSON
        if os.path.exists(svc):
            try:
                with open(svc, "r", encoding="utf-8") as f:
                    return f.read()
            except Exception:
                pass
        return svc

    return ""


def _load_snapshot_rows(force: bool = False) -> list[dict]:
    """Load Unified_Snapshot rows with a small TTL cache."""
    now = time.time()
    if (
        not force
        and _budget_cache.get("rows")
        and now - float(_budget_cache.get("ts", 0.0)) < VENUE_BUDGET_CACHE_TTL_SEC
    ):
        return _budget_cache["rows"]

    if not SHEET_URL or not gspread or not Credentials:
        _budget_cache["rows"] = []
        _budget_cache["ts"] = now
        return []

    raw = _read_service_json_from_env()
    if not raw:
        _budget_cache["rows"] = []
        _budget_cache["ts"] = now
        return []

    try:
        data = json.loads(raw)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(data, scopes=scopes)
        client = gspread.authorize(creds)
        sh = client.open_by_url(SHEET_URL)
        ws = sh.worksheet(UNIFIED_SNAPSHOT_WS)
        rows = ws.get_all_records()
    except Exception:
        rows = []

    _budget_cache["rows"] = rows or []
    _budget_cache["ts"] = now
    return _budget_cache["rows"]


def _build_venue_quote_map(force: bool = False) -> Dict[str, float]:
    """
    Build map: "VENUE:QUOTE" -> total_equity_usd (sum of Equity_USD for that quote).
    Only counts rows where IsQuote is truthy.
    """
    rows = _load_snapshot_rows(force=force)
    m: Dict[str, float] = {}

    for r in rows:
        venue = str(r.get("Venue") or "").upper()
        if not venue:
            continue

        is_quote = str(r.get("IsQuote") or "").strip().lower()
        # accept various truthy markers
        if is_quote not in ("1", "true", "yes", "y", "t"):
            continue

        quote = str(r.get("QuoteSymbol") or r.get("Asset") or "").upper()
        if not quote:
            continue

        try:
            eq_usd = float(r.get("Equity_USD") or 0)
        except Exception:
            eq_usd = 0.0
        if eq_usd <= 0:
            continue

        key = f"{venue}:{quote}"
        m[key] = m.get(key, 0.0) + eq_usd

    _budget_cache["by_venue_quote"] = m
    return m


def _get_total_equity_usd(venue: str, quote: str) -> Optional[float]:
    """Return total Equity_USD for VENUE:QUOTE from cached map."""
    if not venue or not quote:
        return None

    v = venue.upper()
    q = quote.upper()
    key = f"{v}:{q}"

    m = _budget_cache.get("by_venue_quote") or {}
    if key not in m:
        m = _build_venue_quote_map(force=False)
    return m.get(key)


def get_budget_for_intent(intent: Dict[str, Any]) -> Tuple[Optional[float], str]:
    """
    Compute a safe spendable USD budget for this intent's venue+quote.

    Returns (budget_usd, reason):
      budget_usd: None => budget unknown (do nothing)
                  0.0  => venue has no usable quote after reserves
                  >0   => max allowed spend in USD for this venue+quote

    Reason is for logging / debugging.
    """
    venue = str(intent.get("venue") or "").upper()
    quote = str(intent.get("quote") or "").upper()

    if not venue:
        return None, "missing_venue"
    if not quote:
        return None, "missing_quote"

    total_equity = _get_total_equity_usd(venue, quote)
    if total_equity is None:
        return None, "no_snapshot_data"

    # Apply min reserve + keepback; never let it go below zero
    usable = float(total_equity) - POLICY_MIN_QUOTE_RESERVE_USD - POLICY_KEEPBACK_USD
    if usable <= 0:
        return 0.0, "no_usable_after_reserve"

    return usable, "ok"
