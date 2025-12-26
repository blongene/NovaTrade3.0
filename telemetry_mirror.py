#!/usr/bin/env python3
"""
telemetry_mirror.py — NovaTrade 3.0

Mirrors latest Edge→Bus telemetry snapshot into Wallet_Monitor in Sheets.

CRITICAL FIX (Dec 2025):
- Wallet_Monitor header in your sheet is currently 8 columns:
    Timestamp | Agent | Venue | Asset | Free | Locked | Class | Snapshot
  but the old telemetry_mirror wrote only 7 columns:
    Timestamp | Agent | Venue | Asset | Amount | Class | Snapshot
  causing column shift:
    Free=Amount, Locked="QUOTE", Class=<compact fragment>, Snapshot=""
- This drop-in version detects the header and writes the correct format.
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import os
import time
import requests

from utils import with_sheet_backoff
from utils import get_ws_cached, warn, info
from db_backbone import record_telemetry

# Base URL to talk to our own Bus. PORT is present in Render.
PORT = int(os.getenv("PORT", "10000"))
LAST_URL = os.getenv("TELEMETRY_LAST_URL", f"http://127.0.0.1:{PORT}/api/telemetry/last")

SHEET_URL = os.getenv("SHEET_URL", "")
WALLET_MONITOR_WS = os.getenv("WALLET_MONITOR_WS", "Wallet_Monitor")

# Ignore dust balances below this threshold
TELEMETRY_MIRROR_MIN_BALANCE = float(os.getenv("TELEMETRY_MIRROR_MIN_BALANCE", "0.0000001"))

# Telemetry sanity defaults (tunable via env)
TELEMETRY_MAX_AGE_SEC = float(os.getenv("TELEMETRY_MAX_AGE_SEC", "900"))  # 15 minutes

TELEMETRY_REQUIRED_VENUES = {
    v.strip().upper()
    for v in os.getenv("TELEMETRY_REQUIRED_VENUES", "COINBASE,BINANCEUS,KRAKEN").split(",")
    if v.strip()
}

TELEMETRY_MIN_TOTAL_STABLE = float(os.getenv("TELEMETRY_MIN_TOTAL_STABLE", "0.0"))

# Simple stables list so we can tag Quote column
STABLES = {"USD", "USDT", "USDC"}

# Compaction: keep at most this many data rows (excluding header).
WALLET_MONITOR_MAX_ROWS = int(os.getenv("WALLET_MONITOR_MAX_ROWS", "1000"))


# ---------------- Telemetry fetch ----------------
def _http_get_last() -> Dict[str, Any]:
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
    if not isinstance(body, dict) or not body.get("ok"):
        warn(f"telemetry_mirror: /api/telemetry/last !ok: {body}")
        return {}
    data = body.get("data") or {}
    if not isinstance(data, dict):
        warn("telemetry_mirror: /api/telemetry/last data is not a dict")
        return {}
    return data


# ---------------- Sheet helpers ----------------
@with_sheet_backoff
def _open_wallet_monitor_ws():
    return get_ws_cached(WALLET_MONITOR_WS)


@with_sheet_backoff
def _append_rows(ws, rows: List[List[Any]]) -> None:
    ws.append_rows(rows, value_input_option="RAW")


@with_sheet_backoff
def _delete_rows(ws, start_row: int, end_row: int) -> None:
    ws.delete_rows(start_row, end_row)


@with_sheet_backoff
def _clear_and_seed_header(ws, header: List[str]) -> None:
    ws.clear()
    ws.append_rows([header], value_input_option="RAW")


def _read_header(ws) -> List[str]:
    try:
        h = ws.row_values(1) or []
        return [str(x).strip() for x in h]
    except Exception:
        return []


def _ensure_wallet_monitor_header(ws) -> List[str]:
    """
    Ensure Wallet_Monitor has a usable header.
    Prefer the canonical 8-column header to match your sheet (and avoid shifts).
    """
    canonical_8 = ["Timestamp", "Agent", "Venue", "Asset", "Free", "Locked", "Class", "Snapshot"]
    canonical_7 = ["Timestamp", "Agent", "Venue", "Asset", "Amount", "Class", "Snapshot"]

    header = _read_header(ws)
    if not header or all(not str(x).strip() for x in header):
        info("telemetry_mirror: Wallet_Monitor header missing; seeding canonical 8-column header.")
        _clear_and_seed_header(ws, canonical_8)
        return canonical_8

    # Normalize for matching
    norm = [h.strip() for h in header if h is not None]
    if norm == canonical_8:
        return canonical_8
    if norm == canonical_7:
        return canonical_7

    # If it contains these required columns, we’ll respect it,
    # but we will choose how to write by checking which fields exist.
    return norm


# ---------------- Data transforms ----------------
def _flatten_balances(by_venue: Dict[str, Dict[str, float]]) -> List[Tuple[str, str, float]]:
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
    a = (asset or "").upper()
    return "QUOTE" if a in STABLES else "BASE"


def _format_compact_fragment(by_venue: Dict[str, Dict[str, float]]) -> str:
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
            frag_parts.append(f"{str(asset).upper()}={qf:g}")
        if frag_parts:
            parts.append(f"{str(venue).upper()}:" + ",".join(frag_parts))
    return "; ".join(parts)


# ---------------- Safety gating ----------------
def _telemetry_ok_for_sheet(data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        warn("telemetry_mirror: telemetry data is not a dict; skipping Wallet_Monitor write.")
        return False

    by_venue = data.get("by_venue") or {}
    if not isinstance(by_venue, dict) or not by_venue:
        warn("telemetry_mirror: telemetry missing by_venue; skipping Wallet_Monitor write.")
        return False

    ts = data.get("ts") or time.time()
    try:
        ts_f = float(ts)
    except Exception:
        ts_f = time.time()
    if ts_f > 10_000_000_000:
        ts_f /= 1000.0

    age = time.time() - ts_f
    if age > TELEMETRY_MAX_AGE_SEC:
        warn(f"telemetry_mirror: snapshot too old ({age:.0f}s > {TELEMETRY_MAX_AGE_SEC:.0f}s); skip.")
        return False

    venues_present = {str(v).upper() for v in by_venue.keys()}
    missing = sorted(TELEMETRY_REQUIRED_VENUES - venues_present)
    if missing:
        warn(f"telemetry_mirror: missing required venues in snapshot: {missing}; skip Wallet_Monitor write.")
        return False

    total_stables = 0.0
    for v, balances in by_venue.items():
        if not isinstance(balances, dict):
            continue
        for asset, qty in balances.items():
            try:
                qf = float(qty or 0.0)
            except Exception:
                continue
            if qf < 0:
                warn(f"telemetry_mirror: negative balance {qf} for {v}/{asset}; skip Wallet_Monitor write.")
                return False
            if str(asset).upper() in STABLES:
                total_stables += qf

    if total_stables < TELEMETRY_MIN_TOTAL_STABLE:
        warn(f"telemetry_mirror: total stables {total_stables:g} below min {TELEMETRY_MIN_TOTAL_STABLE:g}; skip.")
        return False

    return True


# ---------------- Compaction ----------------
def _compact_wallet_monitor_if_needed() -> None:
    if WALLET_MONITOR_MAX_ROWS <= 0:
        return
    try:
        ws = _open_wallet_monitor_ws()
    except Exception as e:
        warn(f"telemetry_mirror: failed to open Wallet_Monitor for compaction: {e}")
        return

    try:
        col_a = ws.col_values(1)
    except Exception as e:
        warn(f"telemetry_mirror: failed to read Wallet_Monitor col A: {e}")
        return

    if not col_a:
        return

    while col_a and not str(col_a[-1]).strip():
        col_a.pop()

    used_rows = len(col_a)  # includes header
    if used_rows <= 1:
        return

    allowed_total = 1 + WALLET_MONITOR_MAX_ROWS
    if used_rows <= allowed_total:
        return

    surplus = used_rows - allowed_total
    start_row = 2
    end_row = 1 + surplus

    try:
        _delete_rows(ws, start_row, end_row)
        info(
            f"telemetry_mirror: compacted {WALLET_MONITOR_WS}; "
            f"deleted {surplus} old rows (rows {start_row}-{end_row}), kept latest {WALLET_MONITOR_MAX_ROWS}."
        )
    except Exception as e:
        warn(f"telemetry_mirror: failed compaction: {e}")


# ---------------- Write logic (FIXED) ----------------
def _write_wallet_monitor_rows(data: Dict[str, Any]) -> None:
    if not SHEET_URL:
        warn("telemetry_mirror: SHEET_URL not set; cannot mirror telemetry.")
        return

    by_venue = data.get("by_venue") or {}
    if not isinstance(by_venue, dict):
        warn("telemetry_mirror: telemetry has no by_venue; nothing to mirror.")
        return

    ts = data.get("ts") or time.time()
    if isinstance(ts, (int, float)) and ts > 10_000_000_000:
        ts = ts / 1000.0
    dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
    now_str = dt.strftime("%Y-%m-%d %H:%M:%S")

    agent = data.get("agent") or ""
    age_s = time.time() - float(ts)
    info(f"telemetry_mirror: using snapshot agent={agent} age={age_s:.0f}s venues={len(by_venue)}")

    rows = _flatten_balances(by_venue)
    if not rows:
        info("telemetry_mirror: no non-dust balances to mirror.")
        return

    snapshot_frag = _format_compact_fragment(by_venue)

    ws = _open_wallet_monitor_ws()
    header = _ensure_wallet_monitor_header(ws)

    # Determine schema write mode:
    # - If header contains Free/Locked -> write 8-column format
    # - Else if header contains Amount -> write 7-column format
    header_set = {h.strip() for h in header}

    use_8 = ("Free" in header_set and "Locked" in header_set and "Snapshot" in header_set)
    use_7 = ("Amount" in header_set and "Snapshot" in header_set)

    out_rows: List[List[Any]] = []
    for venue, asset, qty in rows:
        klass = _classify_asset(asset)
        if use_8:
            # Timestamp | Agent | Venue | Asset | Free | Locked | Class | Snapshot
            out_rows.append([now_str, agent, venue, asset, qty, 0.0, klass, snapshot_frag])
        elif use_7:
            # Timestamp | Agent | Venue | Asset | Amount | Class | Snapshot
            out_rows.append([now_str, agent, venue, asset, qty, klass, snapshot_frag])
        else:
            # Unknown header. Fail-safe: write the 8-column canonical if header is close-ish,
            # otherwise fall back to 7 to avoid breaking append_rows.
            # We choose by expected length if header has >= 8.
            if len(header) >= 8:
                out_rows.append([now_str, agent, venue, asset, qty, 0.0, klass, snapshot_frag])
            else:
                out_rows.append([now_str, agent, venue, asset, qty, klass, snapshot_frag])

    _append_rows(ws, out_rows)
    info(f"telemetry_mirror: appended {len(out_rows)} Wallet_Monitor rows.")


# ---------------- Entrypoint ----------------
def run_telemetry_mirror() -> None:
    data = _http_get_last()
    if not data:
        return

    # Best-effort DB backbone recording
    try:
        agent = ""
        if isinstance(data, dict):
            agent = data.get("agent") or ""
        if not agent:
            agent = "bus-telemetry"
        record_telemetry(agent, data, kind="snapshot")
    except Exception:
        pass

    if not _telemetry_ok_for_sheet(data):
        return

    try:
        _write_wallet_monitor_rows(data)
    finally:
        _compact_wallet_monitor_if_needed()


if __name__ == "__main__":
    run_telemetry_mirror()
