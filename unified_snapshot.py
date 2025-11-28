"""
NovaTrade Unified Snapshot Builder
----------------------------------
Builds the Unified_Snapshot tab from the latest telemetry (_last_tel).

This version automatically includes all venues present in the telemetry payload
(COINBASE, BINANCEUS, KRAKEN, etc.) without hard-coding them.
"""

import os
import time
import math
import traceback
from datetime import datetime
from utils import get_gspread_client, write_rows_to_sheet, backoff
from telemetry_mirror import get_last_telemetry
from price_feed import get_price_usd

# === Config ===
SHEET_URL = os.getenv("SHEET_URL")
TAB_NAME = "Unified_Snapshot"
QUOTE_TOKENS = {"USDC", "USDT", "USD"}
EQUITY_DECIMALS = 2

# === Helpers ===

def _fmt_ts(ts=None):
    if not ts:
        ts = time.time()
    return datetime.utcfromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")

def _normalize_asset_name(asset: str) -> str:
    return (asset or "").upper().strip()

def _is_quote(asset: str) -> bool:
    return _normalize_asset_name(asset) in QUOTE_TOKENS

def _safe_float(val, default=0.0):
    try:
        if val is None or (isinstance(val, str) and not val.strip()):
            return default
        return float(val)
    except Exception:
        return default


# === Core snapshot builder ===

@backoff(max_tries=3, delay=3)
def build_unified_snapshot():
    """
    Pulls latest telemetry from telemetry_mirror (_last_tel)
    and writes a clean Unified_Snapshot tab for all venues.
    """

    telemetry = get_last_telemetry()
    if not telemetry:
        print("[UnifiedSnapshot] No telemetry data available; skipping.")
        return

    ts = telemetry.get("ts") or time.time()
    by_venue = telemetry.get("by_venue") or {}
    if not by_venue:
        print("[UnifiedSnapshot] Telemetry missing by_venue; skipping.")
        return

    rows = []
    tstamp = _fmt_ts(ts)
    quote_symbol = "USDT"  # default accounting quote
    print(f"[UnifiedSnapshot] Building snapshot at {tstamp} for venues: {list(by_venue.keys())}")

    for venue, asset_map in by_venue.items():
        v_up = (venue or "").upper()
        if not asset_map:
            continue

        for asset, balance in asset_map.items():
            a_up = _normalize_asset_name(asset)
            free = _safe_float(balance)
            locked = 0.0
            total = free + locked
            if math.isclose(total, 0.0, abs_tol=1e-9):
                continue

            is_quote = _is_quote(a_up)
            eq_usd = None
            if is_quote:
                eq_usd = total
            else:
                # Try to look up USD price if available
                try:
                    px = get_price_usd(a_up, quote_symbol, v_up)
                    if px:
                        eq_usd = round(total * px, EQUITY_DECIMALS)
                except Exception:
                    eq_usd = None

            row = [
                tstamp,
                v_up,
                a_up,
                round(free, 8),
                round(locked, 8),
                round(total, 8),
                "TRUE" if is_quote else "FALSE",
                quote_symbol,
                eq_usd,
            ]
            rows.append(row)

    if not rows:
        print("[UnifiedSnapshot] No balances to write; skipping.")
        return

    header = [
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

    try:
        gc = get_gspread_client()
        write_rows_to_sheet(
            gc,
            SHEET_URL,
            TAB_NAME,
            [header] + rows,
            clear_first=True,
        )
        print(f"[UnifiedSnapshot] Wrote {len(rows)} rows across {len(by_venue)} venues.")
    except Exception as e:
        print(f"[UnifiedSnapshot] Sheet write failed: {e}")
        traceback.print_exc()
        return


# === Entry point ===
if __name__ == "__main__":
    build_unified_snapshot()
