# stalled_asset_detector.py — NT3.0 Telemetry-driven stalled asset detector
#
# Responsibilities:
# - Read latest telemetry snapshot from wsgi._last_tel (by_venue / flat / ts).
# - Compare balances vs Rotation_Log to find:
#     • assets with balances but no Rotation_Log history (orphans),
#     • assets whose last Rotation_Log touch is older than N days (stalled),
#     • tiny residual balances with old history (dust).
# - Return a structured list via detect_stalled_tokens().
# - Expose run_stalled_asset_detector() for the scheduler: logs + Telegram + Policy_Log.
#
# Design goals:
# - Best-effort: never raise; degrade to no-op on errors or stale telemetry.
# - Uses your existing utils helpers: caching, logging, Telegram de-dupe.

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from utils import (
    get_all_records_cached,
    send_telegram_message_dedup,
    warn,
    info,
    str_or_empty,
)

# Optional import: Bus global telemetry (_last_tel)
try:
    import wsgi as bus_wsgi  # type: ignore
except Exception:  # pragma: no cover
    bus_wsgi = None  # type: ignore


# ----- Env toggles & thresholds ------------------------------------------------

ENABLED = os.getenv("STALLED_DETECTOR_ENABLED", "1").lower() in ("1", "true", "yes")

ROTATION_LOG_TAB = os.getenv("ROTATION_LOG_TAB", "Rotation_Log")

# How old a Rotation_Log touch (in days) before we call an asset "stalled".
STALLED_MIN_AGE_DAYS = float(os.getenv("STALLED_MIN_AGE_DAYS", "3.0"))

# Minimum balance to consider for "stalled" (ignore microscopic amounts).
STALLED_MIN_QTY = float(os.getenv("STALLED_MIN_QTY", "0.0"))

# Max balance to treat as "dust" (tiny residuals).
STALLED_DUST_MAX_QTY = float(os.getenv("STALLED_DUST_MAX_QTY", "0.0001"))

# Maximum acceptable age of telemetry snapshot, in seconds.
STALLED_TELEMETRY_MAX_AGE_SEC = int(os.getenv("STALLED_TELEMETRY_MAX_AGE_SEC", "900"))  # 15m

# Telegram de-dupe for the job-level alert (minutes).
STALLED_DETECTOR_TTL_MIN = int(os.getenv("STALLED_DETECTOR_TTL_MIN", "60"))

# Semantic groups (can be extended / overridden later)
MAJORS = {"BTC", "ETH"}
STABLES = {"USD", "USDC", "USDT"}


# ----- Internal helpers --------------------------------------------------------


def _parse_ts_fuzzy(raw: Any) -> Optional[datetime]:
    """Best-effort parse for timestamps coming from Rotation_Log."""
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None

    # Try strict ISO first
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        pass

    # Very common Nova format: "YYYY-MM-DD HH:MM:SS"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            # Treat as UTC by default
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue

    return None


def _get_telemetry_snapshot() -> Dict[str, Any]:
    """Read latest telemetry snapshot from wsgi._last_tel.

    Expected shape (as per mysupersdiscussion33 telemetry patches):

        {
          "agent": "edge-primary",
          "by_venue": { "COINBASE": {"USD": 5000, "USDC": 2500}, ... },
          "flat": { "BTC": 1.2, "ETH": 4.0, ... },
          "ts": 1730...
        }
    """
    if bus_wsgi is None:
        warn("stalled_asset_detector: bus_wsgi not importable; telemetry unavailable.")
        return {"by_venue": {}, "flat": {}, "ts": None, "age_sec": None}

    tel = getattr(bus_wsgi, "_last_tel", None) or {}
    by_venue = tel.get("by_venue") or {}
    flat = tel.get("flat") or {}
    ts = tel.get("ts")

    age_sec: Optional[float] = None
    if ts is not None:
        try:
            now = time.time()
            age_sec = max(0.0, now - float(ts))
        except Exception:
            age_sec = None

    return {
        "by_venue": by_venue,
        "flat": flat,
        "ts": ts,
        "age_sec": age_sec,
    }


def _load_last_rotation_activity() -> Dict[str, datetime]:
    """Return last-known Rotation_Log touch per token.

    Key: TOKEN (uppercased)
    Value: datetime (UTC) of last Rotation_Log row for that token.
    """
    last: Dict[str, datetime] = {}
    try:
        rows = get_all_records_cached(ROTATION_LOG_TAB, ttl_s=600)
    except Exception as e:
        warn(f"stalled_asset_detector: could not read {ROTATION_LOG_TAB}: {e}")
        return last

    for row in rows:
        token_raw = row.get("Token") or row.get("TOKEN") or row.get("Ticker") or row.get("Asset")
        token = str_or_empty(token_raw).upper()
        if not token:
            continue

        ts_raw = (
            row.get("Timestamp")
            or row.get("TS")
            or row.get("Opened At")
            or row.get("Opened_At")
            or row.get("Time")
        )
        dt = _parse_ts_fuzzy(ts_raw)
        if not dt:
            continue

        prev = last.get(token)
        if prev is None or dt > prev:
            last[token] = dt

    return last


# ----- Core: detection logic ---------------------------------------------------


def detect_stalled_tokens() -> List[Dict[str, Any]]:
    """Return a list of stalled/orphan/dust asset descriptors.

    Shape of each entry:
        {
          "kind": "stalled" | "stalled_major" | "orphan" | "dust",
          "symbol": "ABC",
          "qty": 123.45,
          "days_since_rotation": 4.2 or None,
          "venues": ["COINBASE", "BINANCEUS"],
        }

    This is designed to be imported via utils.detect_stalled_tokens()
    and used by telegram_summaries for counts.
    """
    if not ENABLED:
        info("stalled_asset_detector: disabled via STALLED_DETECTOR_ENABLED=0 (no-op).")
        return []

    tel = _get_telemetry_snapshot()
    flat = tel.get("flat") or {}
    by_venue = tel.get("by_venue") or {}
    age_sec = tel.get("age_sec")

    # If telemetry is missing or stale, bail out quietly.
    if age_sec is None:
        info("stalled_asset_detector: telemetry has no ts/age; skipping detection.")
        return []
    if age_sec > STALLED_TELEMETRY_MAX_AGE_SEC:
        info(
            f"stalled_asset_detector: telemetry age {int(age_sec)}s "
            f"> STALLED_TELEMETRY_MAX_AGE_SEC={STALLED_TELEMETRY_MAX_AGE_SEC}; skipping."
        )
        return []

    last_rot = _load_last_rotation_activity()
    now = datetime.now(timezone.utc)

    anomalies: List[Dict[str, Any]] = []

    for sym_raw, raw_qty in flat.items():
        symbol = str_or_empty(sym_raw).upper()
        if not symbol:
            continue
        try:
            qty = float(raw_qty or 0)
        except Exception:
            continue
        if qty <= 0:
            continue

        # Find venues where this symbol is present with >0 balance.
        venues: List[str] = []
        for venue, assets in (by_venue or {}).items():
            try:
                a_val = float((assets or {}).get(symbol, 0) or 0)
            except Exception:
                a_val = 0.0
            if a_val > 0:
                venues.append(str(venue).upper())

        last_dt = last_rot.get(symbol)
        days: Optional[float] = None
        if last_dt:
            days = (now - last_dt).total_seconds() / 86400.0

        # Case 1: asset appears in telemetry but never in Rotation_Log ⇒ orphan
        if last_dt is None:
            anomalies.append(
                {
                    "kind": "orphan",
                    "symbol": symbol,
                    "qty": qty,
                    "days_since_rotation": None,
                    "venues": venues,
                }
            )
            continue

        # Case 2: stalled assets — last Rotation_Log touch older than threshold
        if days is not None and days >= STALLED_MIN_AGE_DAYS and qty > STALLED_MIN_QTY:
            kind = "stalled_major" if symbol in MAJORS else "stalled"
            anomalies.append(
                {
                    "kind": kind,
                    "symbol": symbol,
                    "qty": qty,
                    "days_since_rotation": round(days, 2),
                    "venues": venues,
                }
            )
            continue

        # Case 3: dust — tiny residual balances with "old enough" history
        if (
            symbol not in STABLES
            and qty <= STALLED_DUST_MAX_QTY
            and days is not None
            and days >= 1.0
        ):
            anomalies.append(
                {
                    "kind": "dust",
                    "symbol": symbol,
                    "qty": qty,
                    "days_since_rotation": round(days, 2),
                    "venues": venues,
                }
            )

    return anomalies


# ----- Job entrypoint ---------------------------------------------------------


def run_stalled_asset_detector() -> None:
    """Scheduled job entrypoint (called via main._safe_call).

    - Runs detection
    - Logs a compact Telegram summary (de-duped)
    - Best-effort writes to Policy_Log via policy_logger, if present
    """
    if not ENABLED:
        info("run_stalled_asset_detector: disabled (STALLED_DETECTOR_ENABLED=0).")
        return

    anomalies = detect_stalled_tokens()
    if not anomalies:
        info("run_stalled_asset_detector: no stalled/orphan/dust assets detected.")
        return

    # Summarize counts by kind
    counts: Dict[str, int] = {}
    for a in anomalies:
        k = a.get("kind", "unknown")
        counts[k] = counts.get(k, 0) + 1

    tel = _get_telemetry_snapshot()
    age_sec = tel.get("age_sec")
    if age_sec is None:
        age_str = "unknown"
    elif age_sec < 300:
        age_str = f"{int(age_sec)}s"
    else:
        age_str = f"{int(age_sec // 60)}m"

    # Top examples sorted by days_since_rotation (desc), then qty (desc)
    def _sort_key(a: Dict[str, Any]):
        d = a.get("days_since_rotation") or 0.0
        q = a.get("qty") or 0.0
        return (float(d), float(q))

    examples: List[str] = []
    for a in sorted(anomalies, key=_sort_key, reverse=True)[:5]:
        sym = a.get("symbol", "?")
        qty = a.get("qty", 0)
        ds = a.get("days_since_rotation")
        ds_str = "?" if ds is None else f"{ds:.1f}d"
        venues = a.get("venues") or []
        v_str = ",".join(venues) if venues else "—"
        kind = a.get("kind", "stalled")
        examples.append(f"{sym} {qty} ({kind}, {ds_str}, {v_str})")

    count_str = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    lines = [
        "🧊 *Stalled Asset Detector*",
        f"Telemetry age: {age_str}",
        f"Counts: {count_str}",
    ]
    if examples:
        lines.append("Examples:")
        for line in examples:
            lines.append(f"• {line}")

    text = "\n".join(lines)

    # De-duped Telegram alert
    send_telegram_message_dedup(
        key="stalled_assets",
        text=text,
        ttl_min=STALLED_DETECTOR_TTL_MIN,
    )

    # Best-effort Policy_Log entry per anomaly (if policy_logger is available)
    try:
        from policy_logger import log_policy_event  # type: ignore
    except Exception:
        log_policy_event = None  # type: ignore

    if log_policy_event:
        for a in anomalies:
            try:
                msg = (
                    f"{a.get('kind')} {a.get('symbol')} "
                    f"qty={a.get('qty')} venues={','.join(a.get('venues') or [])}"
                )
                log_policy_event(
                    source="stalled_asset_detector",
                    level="WARN",
                    message=msg,
                    context=a,
                )
            except Exception as e:
                warn(f"stalled_asset_detector: policy_log failed: {e}")
