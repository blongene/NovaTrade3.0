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
from typing import Any, Dict, Iterable, List, Tuple

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
PORT = int(os.getenv("PORT", "10000"))
LAST_URL = os.getenv("TELEMETRY_LAST_URL", f"http://127.0.0.1:{PORT}/api/telemetry/last")

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
    if not resp.ok:
        warn(f"telemetry_mirror: HTTP {resp.status_code} from {LAST_URL}: {resp.text}")
        return {}
    try:
        body = resp.json()
    except Exception as e:
        warn(f"telemetry_mirror: bad JSON from /api/telemetry/last: {e}")
        return {}
    if not isinstance(body, dict):
        warn("telemetry_mirror: non-dict body from /api/telemetry/last")
        return {}
    if not body.get("ok"):
        warn(f"telemetry_mirror: /api/telemetry/last !ok: {body}")
        return {}
    data = body.get("data") or {}
    if not isinstance(data, dict):
        warn("telemetry_mirror: /api/telemetry/last data is not a dict")
        return {}
    return data


def _flatten_balances(by_venue: Dict[str, Dict[str, float]]) -> List[Tuple[str, str, float]]:
    """
    Convert nested by_venue dict to a flat list of (venue, asset, qty) tuples,
    filtering out dust based on TELEMETRY_MIRROR_MIN_BALANCE.
    """
    rows: List[Tuple[str, str, float]] = []
    for venue, balances in by_venue.items():
        if not isinstance(balances, dict):
            continue
        for asset, qty in balances.items():
            try:
                qf = float(qty or 0.0)
            except Exception:
                continue
            if qf <= TELEMETRY_MIRROR_MIN_BALANCE:
                continue
            rows.append((str(venue).upper(), str(asset).upper(), qf))
    return rows


def _classify_asset(asset: str) -> str:
    """
    Tag whether this asset is a stable (used for Quote, etc.).
    """
    a = (asset or "").upper()
    if a in STABLES:
        return "QUOTE"
    return "BASE"


def _format_compact_fragment(by_venue: Dict[str, Dict[str, float]]) -> str:
    """
    Build a compact human-readable fragment like:

        BINANCEUS:USDT=159.25,USD=9.77; COINBASE:USDC=136.38

    This is used in the Wallet_Monitor sheet's "Snapshot" column.
    """
    parts: List[str] = []
    for venue, balances in by_venue.items():
        if not isinstance(balances, dict):
            continue
        frag_parts: List[str] = []
        for asset, qty in balances.items():
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


@with_sheet_backoff
def _open_wallet_monitor_ws(
    ws_name: str = WALLET_MONITOR_WS,
) -> "Worksheet":
    """
    Open the Wallet_Monitor worksheet with backoff + cache.

    get_ws_cached(name, ttl_s=None) uses SHEET_URL from utils internally,
    so we only pass the worksheet name here.
    """
    return get_ws_cached(ws_name)


@with_sheet_backoff
def _delete_wallet_monitor_rows(ws, start_row: int, end_row: int):
    """Delete a block of rows with backoff / retry."""
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

    try:
        ws = _open_wallet_monitor_ws()
    except Exception as e:
        warn(f"telemetry_mirror: failed to open Wallet_Monitor for compaction: {e}")
        return

    ws_name = getattr(ws, "title", WALLET_MONITOR_WS)

    try:
        col_a = ws.col_values(1)  # 1-based column index for Timestamp
    except Exception as e:
        warn(f"telemetry_mirror: failed to read Wallet_Monitor col A: {e}")
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
            f"kept latest {WALLET_MONITOR_MAX_ROWS} data rows."
        )
    except Exception as e:
        warn(f"telemetry_mirror: failed to compact {ws_name}: {e}")


def _write_wallet_monitor_row(data: Dict[str, Any]) -> None:
    """
    Append a single compact snapshot row into Wallet_Monitor.

    Expected row:
        [Timestamp, Agent, Venue, Asset, Qty, Class, Snapshot]

    Where Snapshot is the compact multi-venue fragment.

    This function assumes data is already validated and flattened.
    """
    if not SHEET_URL:
        warn("telemetry_mirror: SHEET_URL not set; cannot mirror telemetry.")
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
        f"age={time.time() - float(ts):.0f}s venues={len(by_venue)}"
    )

    rows = _flatten_balances(by_venue)
    if not rows:
        info("telemetry_mirror: no non-dust balances to mirror.")
        return

    snapshot_frag = _format_compact_fragment(by_venue)

    out_rows: List[List[Any]] = []
    agent = data.get("agent") or ""
    for venue, asset, qty in rows:
        klass = _classify_asset(asset)
        out_rows.append(
            [
                now_str,
                agent,
                venue,
                asset,
                qty,
                klass,
                snapshot_frag,
            ]
        )

    ws = _open_wallet_monitor_ws()
    sheets_append_rows(ws, out_rows)
    info(f"telemetry_mirror: appended {len(out_rows)} Wallet_Monitor rows.")


def run_telemetry_mirror() -> None:
    """
    Public entrypoint: mirror latest telemetry snapshot into Wallet_Monitor
    and compact the sheet if necessary.
    """
    data = _http_get_last()
    if not data:
        return

    try:
        _write_wallet_monitor_row(data)
    finally:
        # Compaction should not block writes; best-effort.
        _compact_wallet_monitor_if_needed()


if __name__ == "__main__":
    run_telemetry_mirror()
