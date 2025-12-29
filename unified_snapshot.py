#!/usr/bin/env python3
"""
unified_snapshot.py — NovaTrade 3.0

Build Unified_Snapshot from Wallet_Monitor robustly.

Fixes/enhancements:
- Fixes sample-row logging bug (was always picking row 0).
- Tolerates the “shifted columns” situation (Locked == 'QUOTE') gracefully.
- Works with these Wallet_Monitor schemas:

  Legacy:
    Timestamp | Venue | Asset | Free | Locked | Quote | Snapshot

  Telemetry v2 (7-col):
    Timestamp | Agent | Venue | Asset | Amount | Class | Snapshot

  Telemetry v2 (8-col / canonical in your sheet):
    Timestamp | Agent | Venue | Asset | Free | Locked | Class | Snapshot

- Always emits 9 rows: (COINBASE,BINANCEUS,KRAKEN) × (USD,USDC,USDT)
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple

from utils import with_sheet_backoff
from utils import get_ws_cached, info, warn
from utils import get_all_records_cached_dbaware

UNIFIED_SNAPSHOT_SRC_WS = os.getenv("UNIFIED_SNAPSHOT_SRC_WS", "Wallet_Monitor")
UNIFIED_SNAPSHOT_WS = os.getenv("UNIFIED_SNAPSHOT_WS", "Unified_Snapshot")

VENUES: List[str] = ["COINBASE", "BINANCEUS", "KRAKEN"]
STABLES: List[str] = ["USD", "USDC", "USDT"]

HEADER: List[str] = [
    "Timestamp",
    "Venue",
    "Asset",
    "Free",
    "Locked",
    "Total",
    "IsQuote",
    "QuoteSymbol",
    "Equity_USD",
]


@with_sheet_backoff
def _open_ws(name: str):
    return get_ws_cached(name)


@with_sheet_backoff
def _replace_rows(ws, header: List[str], rows: List[List[Any]]) -> None:
    ws.clear()
    ws.append_rows([header] + rows, value_input_option="RAW")


def _safe_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        s = str(v).strip()
        if not s:
            return 0.0
        s = s.replace(",", "")
        return float(s)
    except Exception:
        return 0.0


def _parse_ts(ts_raw: Any, idx_fallback: int) -> float:
    if not ts_raw:
        return float(idx_fallback)

    ts_str = str(ts_raw).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(ts_str.replace("Z", ""), fmt).timestamp()
        except Exception:
            pass
    return float(idx_fallback)


def _coerce_record(row: Dict[str, Any]) -> Tuple[float, float]:
    """
    Extract (free, locked) from a Wallet_Monitor row across schema variants.

    Handles:
    - Free/Locked numeric
    - Amount numeric (7-col telemetry schema)
    - Corrupted-shift case where Locked == 'QUOTE' (string), and Free holds the amount.
    """
    free = _safe_float(row.get("Free"))
    locked_raw = row.get("Locked")
    locked = _safe_float(locked_raw)

    amount = _safe_float(row.get("Amount"))

    # If both Free/Locked are zero but Amount is present, use Amount as Free.
    if free == 0.0 and locked == 0.0 and amount != 0.0:
        return amount, 0.0

    # If Locked is non-numeric (e.g., 'QUOTE'), treat locked as 0 and trust Free.
    if isinstance(locked_raw, str) and locked_raw.strip().upper() in ("QUOTE", "BASE"):
        return free, 0.0

    return free, locked


def _latest_wallet_records(rows: Iterable[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    latest: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for idx, row in enumerate(rows):
        venue = (row.get("Venue") or "").strip().upper()
        asset = (row.get("Asset") or "").strip().upper()
        if not venue or not asset:
            continue
        if venue not in VENUES or asset not in STABLES:
            continue

        ts_raw = row.get("Timestamp") or ""
        ts_val = _parse_ts(ts_raw, idx)

        free, locked = _coerce_record(row)

        key = (venue, asset)
        prev = latest.get(key)
        if prev is not None and prev["ts"] >= ts_val:
            continue

        latest[key] = {
            "ts": ts_val,
            "Timestamp": str(ts_raw) if ts_raw else "",
            "Venue": venue,
            "Asset": asset,
            "Free": free,
            "Locked": locked,
        }

    return latest


def _build_unified_rows(latest: Dict[Tuple[str, str], Dict[str, Any]]) -> List[List[Any]]:
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    out: List[List[Any]] = []

    for venue in VENUES:
        for asset in STABLES:
            rec = latest.get((venue, asset))
            if rec:
                ts = rec.get("Timestamp") or now_str
                free = float(rec.get("Free") or 0.0)
                locked = float(rec.get("Locked") or 0.0)
            else:
                ts = now_str
                free = 0.0
                locked = 0.0

            total = free + locked
            out.append([ts, venue, asset, free, locked, total, True, asset, total])

    return out


def run_unified_snapshot() -> None:
    info("📸 unified_snapshot: building Unified_Snapshot from Wallet_Monitor…")

    try:
        src_ws = _open_ws(UNIFIED_SNAPSHOT_SRC_WS)
    except Exception as e:
        warn(f"unified_snapshot: failed to open source worksheet '{UNIFIED_SNAPSHOT_SRC_WS}': {e}")
        return

    # Phase 22B: Prefer DB-backed sheet_mirror for Wallet_Monitor when available.
    # If DB has no mirror for this tab (or DB unavailable), we fall back to the sheet read below.
    rows = []
    try:
        rows = get_all_records_cached_dbaware(
            UNIFIED_SNAPSHOT_SRC_WS,
            ttl_s=120,
            logical_stream=f"sheet_mirror:{UNIFIED_SNAPSHOT_SRC_WS}",
        ) or []
    except Exception as e:
        warn(f"unified_snapshot: DB-aware read failed (will fall back to Sheets): {e}")
        rows = []

    if not rows:
        try:
            rows = src_ws.get_all_records()
        except Exception as e:
            warn(f"unified_snapshot: get_all_records() failed for '{UNIFIED_SNAPSHOT_SRC_WS}': {e}")
            return

    info(f"unified_snapshot: Wallet_Monitor get_all_records -> {len(rows)} rows")
    if rows:
        sample = rows[-1]  # fixed
        info(f"unified_snapshot: sample row: {sample}")

    latest = _latest_wallet_records(rows)
    info(f"unified_snapshot: latest per (venue, asset) -> {len(latest)} keys")

    out_rows = _build_unified_rows(latest)

    try:
        out_ws = _open_ws(UNIFIED_SNAPSHOT_WS)
    except Exception as e:
        warn(f"unified_snapshot: failed to open Unified_Snapshot worksheet '{UNIFIED_SNAPSHOT_WS}': {e}")
        return

    try:
        _replace_rows(out_ws, HEADER, out_rows)
        info(f"✅ unified_snapshot: wrote {len(out_rows)} rows to Unified_Snapshot")
    except Exception as e:
        warn(f"unified_snapshot: failed writing rows to Unified_Snapshot: {e}")


if __name__ == "__main__":
    run_unified_snapshot()
