#!/usr/bin/env python3
# telemetry_digest.py — Phase 9D/18 bridge
#
# Pulls /api/telemetry/last (local) and:
#   1. Writes a heartbeat row to NovaHeartbeat.
#   2. Sends a tiny Telegram digest of per-venue stable balances.
#
import os
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

HEARTBEAT_WS = os.getenv("HEARTBEAT_WS", "NovaHeartbeat")
HEARTBEAT_ALERT_MIN = int(os.getenv("HEARTBEAT_ALERT_MIN", "90"))  # minutes
SHEET_URL = os.getenv("SHEET_URL", "")
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Optional one-shot housekeeping (can safely run on every boot)
HEARTBEAT_TRIM_TAIL_ON_BOOT = os.getenv("HEARTBEAT_TRIM_TAIL_ON_BOOT", "1") in (
    "1",
    "true",
    "True",
)

STABLES = {"USD", "USDT", "USDC"}

try:
    from utils import (
        get_gspread_client,
        send_telegram_message_dedup,
        warn,
        info,
    )
except Exception:
    # Very defensive fallbacks for tooling environments
    def warn(msg: str) -> None:  # type: ignore
        print("[WARN]", msg)

    def info(msg: str) -> None:  # type: ignore
        print("[INFO]", msg)

    def get_gspread_client():  # type: ignore
        raise RuntimeError("get_gspread_client unavailable")

    def send_telegram_message_dedup(  # type: ignore
        message: str, key: str, ttl_min: int = 15
    ) -> None:
        if not BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return
        print("[TG]", key, message)


def _http_get(url: str) -> Dict[str, Any]:
    import requests

    try:
        resp = requests.get(url, timeout=5)
    except Exception as e:
        warn(f"telemetry_digest: GET {url} failed: {e}")
        return {}

    if resp.status_code != 200:
        warn(f"telemetry_digest: {url} -> HTTP {resp.status_code}")
        return {}

    try:
        return resp.json()
    except Exception as e:
        warn(f"telemetry_digest: invalid JSON from {url}: {e}")
        return {}


def _open_or_create_worksheet(sh, name: str, headers) -> Any:
    """Return a worksheet with a correct header row."""
    try:
        ws = sh.worksheet(name)
    except Exception:
        ws = sh.add_worksheet(
            title=name, rows=2000, cols=max(8, len(headers) + 2)
        )

    try:
        first = ws.row_values(1)
        if [h.strip() for h in first] != headers:
            ws.update("1:1", [headers], value_input_option="USER_ENTERED")
    except Exception:
        ws.update("1:1", [headers], value_input_option="USER_ENTERED")
    return ws


def _trim_tail(ws, key_col: int = 1) -> None:
    """
    Remove trailing empty rows after the last non-empty in key_col (default A).

    This is designed to be safe and idempotent: if there is no trailing
    empty region, it becomes a fast no-op.
    """
    try:
        # All values in the key column (e.g., "Timestamp")
        col_vals = ws.col_values(key_col)
    except Exception as e:
        warn(f"telemetry_digest: failed to read column {key_col}: {e!r}")
        return

    if not col_vals:
        # Entire column empty; nothing to trim.
        return

    # Find index of last non-empty cell (1-based)
    last = len(col_vals)
    while last > 0 and not str(col_vals[last - 1]).strip():
        last -= 1

    if last == 0:
        # No non-empty rows; nothing to do.
        return

    total = ws.row_count

    # No gap after last data row => nothing to delete.
    if total <= last:
        return

    start = last + 1
    end = total

    if start > end:
        return

    try:
        ws.delete_rows(start, end)
        warn(
            f"telemetry_digest: trimmed tail rows {start}..{end} "
            f"(row_count={total}, last_data_row={last})"
        )
    except Exception as e:
        warn(f"telemetry_digest: trim tail failed: {e!r}")


def _compute_stable_digest(
    by_venue: Dict[str, Dict[str, float]]
) -> Tuple[str, Dict[str, float]]:
    """
    Return:
        digest_str, per_venue_totals

    per_venue_totals[VENUE] = sum of USD+USDT+USDC balances for that venue.
    """
    totals: Dict[str, float] = {}
    for venue, assets in by_venue.items():
        if not isinstance(assets, dict):
            continue
        acc = 0.0
        for sym, qty in assets.items():
            try:
                qf = float(qty or 0.0)
            except Exception:
                continue
            if sym.upper() not in STABLES:
                continue
            acc += qf
        if acc > 0:
            totals[str(venue).upper()] = acc

    if not totals:
        return "no stable balances reported", totals

    parts = [f"{v}={amt:.2f}" for v, amt in sorted(totals.items())]
    return ", ".join(parts) + " (USD+USDT+USDC)", totals


def _send_tg(msg: str, key: str) -> None:
    if not BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        send_telegram_message_dedup(msg, key=key)
    except Exception as e:
        warn(f"telemetry_digest: telegram send failed: {e}")


def run_telemetry_digest() -> None:
    if not SHEET_URL:
        warn("SHEET_URL missing; abort.")
        return

    # 1) Pull telemetry snapshot from local Bus
    port = os.getenv("PORT", "10000")
    url = f"http://127.0.0.1:{port}/api/telemetry/last"
    j = _http_get(url) or {}

    age_sec = j.get("age_sec")
    data = j.get("data") or {}
    if not isinstance(data, dict):
        warn("telemetry_digest: /api/telemetry/last returned no data")
        return

    agent = (
        data.get("agent_id")
        or data.get("agent")
        or j.get("agent_id")
        or j.get("agent")
        or ""
    )

    by_venue = data.get("by_venue") or {}
    if not isinstance(by_venue, dict):
        by_venue = {}

    # Compute human-readable stable digest
    digest_str, per_venue = _compute_stable_digest(by_venue)

    # 2) Append heartbeat row
    try:
        gc = get_gspread_client()
        sh = gc.open_by_url(SHEET_URL)
        headers = [
            "Timestamp",
            "Agent",
            "Age_sec",
            "Age_min",
            "Digest",
            "PerVenue_JSON",
        ]
        ws = _open_or_create_worksheet(sh, HEARTBEAT_WS, headers)

        if HEARTBEAT_TRIM_TAIL_ON_BOOT:
            _trim_tail(ws, key_col=1)

        now = datetime.now(timezone.utc)
        ts_str = now.strftime("%Y-%m-%d %H:%M:%S")

        age_min = None
        if isinstance(age_sec, (int, float)):
            age_min = age_sec / 60.0

        row = [
            ts_str,
            str(agent or ""),
            age_sec if age_sec is not None else "",
            f"{age_min:.1f}" if age_min is not None else "",
            digest_str,
            str(per_venue),
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
        info(f"telemetry_digest: wrote heartbeat row for agent={agent}")
    except Exception as e:
        warn(f"heartbeat write failed: {e}")

    # 3) Alert if stale
    try:
        if isinstance(age_sec, (int, float)):
            age_min = age_sec / 60.0
            if age_min > HEARTBEAT_ALERT_MIN:
                _send_tg(
                    f"⚠️ Edge heartbeat stale: {int(age_min)} min "
                    f"(>{HEARTBEAT_ALERT_MIN} min)",
                    key=f"hb:{int(age_min)}",
                )
    except Exception:
        pass

    # 4) Send a small daily telemetry digest to Telegram
    try:
        if per_venue:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            _send_tg(
                f"📊 Telemetry digest {today}: {digest_str}",
                key=f"tel_digest:{today}",
            )
    except Exception as e:
        warn(f"telemetry_digest: digest telegram failed: {e}")


if __name__ == "__main__":
    run_telemetry_digest()


# ---- scheduler compatibility ----

def run_daily_telemetry_digest():
    """Compatibility alias expected by some schedulers."""
    return run_telemetry_digest()
