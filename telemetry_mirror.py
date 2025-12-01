#!/usr/bin/env python3
"""
telemetry_mirror.py — NovaTrade 3.0

Purpose:
    Read the latest Edge → Bus telemetry snapshot (_last_tel from wsgi)
    and append a compact balance snapshot into Wallet_Monitor.

    This keeps Wallet_Monitor fresh using the same telemetry source the
    rest of the system sees, without putting Sheets in the hot path.

Expected telemetry shape (as used elsewhere in NovaTrade 3.0):

    _last_tel = {
        "agent": "edge-primary,edge-nl1",
        "by_venue": {
            "COINBASE":  {"USDC": 136.38, "USDT": 0.0,    ...},
            "BINANCEUS": {"USD": 9.77,   "USDC": 0.0,
                          "USDT": 159.25, ...},
            "KRAKEN":    {"USDC": 0.00005, "USDT": 155.78, ...},
        },
        "flat": {
            "BTC": 0.015,
            "ETH": 0.42,
            ...
        },
        "ts": 1700000000,
    }
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, List

import json
import os
import time

import requests
from utils import with_sheet_backoff
from utils import (
    sheets_append_rows,
    get_ws_cached,
    warn,
    info,
)  # type: ignore

# Base URL to talk to our own Bus. PORT is present in Render.
_PORT = os.getenv("PORT", "10000")
_BASE = os.getenv("TELEMETRY_LAST_URL_BASE", f"http://localhost:{_PORT}")
LAST_URL = os.getenv("TELEMETRY_LAST_URL", f"{_BASE}/api/telemetry/last")

SHEET_URL = os.getenv("SHEET_URL", "")
WALLET_MONITOR_WS = os.getenv("WALLET_MONITOR_WS", "Wallet_Monitor")
WALLET_MONITOR_MAX_ROWS = int(os.getenv("WALLET_MONITOR_MAX_ROWS", "500"))

# Ignore dust balances below this threshold
TELEMETRY_MIRROR_MIN_BALANCE = float(
    os.getenv("TELEMETRY_MIRROR_MIN_BALANCE", "0.0000001")
)

# Simple stables list so we can tag Quote column
STABLES = {"USD", "USDT", "USDC"}

# Compaction: keep at most this many data rows (excluding header).
# 0 or negative disables compaction.
WALLET_MONITOR_MAX_ROWS = int(os.getenv("WALLET_MONITOR_MAX_ROWS", "1000"))


def _http_get_last() -> Dict[str, Any]:
    """
    Call the local /api/telemetry/last endpoint exposed by wsgi.py and
    return the inner telemetry dict (same shape as _last_tel).

    The endpoint usually returns:
        {"ok": true, "data": {...}, "age_sec": 7.3}
    """
    try:
        resp = requests.get(LAST_URL, timeout=5)
    except Exception as e:
        warn(f"telemetry_mirror: error calling {LAST_URL}: {e}")
        return {}

    if resp.status_code != 200:
        warn(f"telemetry_mirror: {LAST_URL} -> HTTP {resp.status_code}")
        return {}

    try:
        body = resp.json()
    except Exception as e:
        warn(f"telemetry_mirror: invalid JSON from {LAST_URL}: {e}")
        return {}

    if not body.get("ok"):
        warn(f"telemetry_mirror: endpoint returned ok=false: {body}")
        return {}

    data = body.get("data") or {}
    if not isinstance(data, dict):
        return {}
    return data


def _summarize_by_venue(by_venue: Dict[str, Any]) -> str:
    """
    Build a compact human-readable snapshot of balances for logs.
    Example:
        'COINBASE:USDC=136.38; BINANCEUS:USD=9.77,USDT=159.25; ...'
    """
    parts: List[str] = []
    for venue, assets in sorted(by_venue.items()):
        if not isinstance(assets, dict):
            continue
        frag_parts: List[str] = []
        for asset, qty in sorted(assets.items()):
            try:
                qf = float(qty or 0.0)
            except Exception:
                continue
            if qf <= TELEMETRY_MIRROR_MIN_BALANCE:
                continue
            frag_parts.append(f"{asset}={qf:g}")
        if frag_parts:
            parts.append(f"{venue}:" + ",".join(frag_parts))
    return "; ".join(parts)

@with_sheet_backoff("Wallet Monitor Compactor")
def _open_wallet_monitor_ws(
    sheet_url: str = SHEET_URL,
    ws_name: str = WALLET_MONITOR_WS,
) -> Worksheet:
    """
    Open the Wallet_Monitor worksheet with backoff + cache.

    get_ws_cached expects the worksheet *name* first, and the sheet URL
    as a keyword argument, so we must not pass them positionally reversed.
    """
    return get_ws_cached(ws_name, sheet_url=sheet_url)


@with_sheet_backoff
def _delete_wallet_monitor_rows(ws, start_row: int, end_row: int):
    """Delete a block of rows with backoff / retry."""
    # gspread delete_dimension expects 1-based indices, inclusive
    ws.delete_rows(start_row, end_row)


def _compact_wallet_monitor_if_needed() -> None:
    """
    Bounded-history compaction for Wallet_Monitor.

    Strategy:
      - If WALLET_MONITOR_MAX_ROWS <= 0: no-op.
      - Otherwise:
          * Look at column A (Timestamp) to find how many rows are actually used.
          * If used_rows > max_rows, delete the oldest surplus rows, keeping
            the header row intact.
    """
    from utils import info, warn  # already imported at top in this module in your tree

    if WALLET_MONITOR_MAX_ROWS <= 0:
        return

    sheet_url = os.getenv("SHEET_URL", "").strip()
    ws_name = os.getenv("WALLET_MONITOR_WS", "Wallet_Monitor")

    if not sheet_url:
        warn("telemetry_mirror: compaction skipped; SHEET_URL is empty.")
        return

    # 1) Open the worksheet with the same backoff used everywhere else
    try:
        ws = _open_wallet_monitor_ws()
    except Exception as e:
        warn(f"telemetry_mirror: compaction skipped; cannot open Wallet_Monitor: {e!r}")
        return

    # 2) Read column A (timestamps) to determine how many real rows exist
    try:
        col_a = ws.col_values(1)  # includes header; may include trailing blanks
    except Exception as e:
        warn(
            f"telemetry_mirror: compaction failed reading col A on {ws_name}: "
            f"{type(e).__name__}: {e}"
        )
        return

    if not col_a:
        # Completely empty sheet, nothing to do.
        return

    # Trim trailing empty cells so we don’t treat them as data rows
    while col_a and not str(col_a[-1]).strip():
        col_a.pop()

    used_rows = len(col_a)          # includes header row
    if used_rows <= 1:
        # Only header present.
        return

    # We allow WALLET_MONITOR_MAX_ROWS data rows *plus* the header
    allowed_total_rows = 1 + WALLET_MONITOR_MAX_ROWS
    if used_rows <= allowed_total_rows:
        # Under the cap, nothing to compact.
        return

    surplus = used_rows - allowed_total_rows
    # Delete the oldest data rows, keeping row 1 as header:
    #   delete rows 2 .. (1 + surplus)
    start_row = 2
    end_row = 1 + surplus

    try:
        _delete_wallet_monitor_rows(ws, start_row, end_row)
        info(
            f"telemetry_mirror: compacted {ws_name}; "
            f"deleted {surplus} old rows (rows {start_row}-{end_row}), "
            f"rows_before={used_rows}, rows_after={used_rows - surplus}"
        )
    except Exception as e:
        warn(
            f"telemetry_mirror: compaction failed deleting rows {start_row}-{end_row} "
            f"on {ws_name}: {type(e).__name__}: {e}"
        )
        return
        
def mirror_telemetry_once() -> None:
    """
    Pull the latest telemetry snapshot and mirror balances into Wallet_Monitor,
    then compact the tab if it exceeds WALLET_MONITOR_MAX_ROWS.
    """
    if not SHEET_URL:
        warn("telemetry_mirror: SHEET_URL missing; abort.")
        return

    data = _http_get_last()
    if not data:
        return

    by_venue = data.get("by_venue") or {}
    if not isinstance(by_venue, dict):
        warn("telemetry_mirror: telemetry has no by_venue; nothing to mirror.")
        return

    ts = data.get("ts") or time.time()
    # Accept either seconds or ms; if ts is large, assume ms
    if isinstance(ts, (int, float)) and ts > 10_000_000_000:
        ts = ts / 1000.0
    dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
    now_str = dt.strftime("%Y-%m-%d %H:%M:%S")

    info(
        f"telemetry_mirror: using snapshot agent={data.get('agent')} "
        f"age={time.time() - float(ts):.0f}s venues={len(by_venue)} "
        f"raw={_summarize_by_venue(by_venue)}"
    )

    rows: List[List[Any]] = []

    for venue, assets in by_venue.items():
        if not isinstance(assets, dict):
            continue
        venue_u = str(venue).upper()
        for asset, qty in assets.items():
            try:
                qf = float(qty or 0.0)
            except Exception:
                continue
            if qf <= TELEMETRY_MIRROR_MIN_BALANCE:
                continue
            asset_u = str(asset).upper()
            quote = asset_u if asset_u in STABLES else ""
            # Wallet_Monitor columns:
            #   Timestamp, Venue, Asset, Free, Locked, Quote
            rows.append([now_str, venue_u, asset_u, qf, 0.0, quote])

    if not rows:
        info("telemetry_mirror: no non-zero balances to mirror; nothing to do.")
        return

    # Append new rows
    sheets_append_rows(SHEET_URL, WALLET_MONITOR_WS, rows)
    info(
        f"telemetry_mirror: mirrored {len(rows)} balances into {WALLET_MONITOR_WS}."
    )

    # Compact historical rows if needed
    _compact_wallet_monitor_if_needed()


def main() -> None:
    mirror_telemetry_once()


if __name__ == "__main__":
    main()
