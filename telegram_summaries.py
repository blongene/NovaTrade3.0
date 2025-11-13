# telegram_summaries.py — NT3.0 Daily Summary (Phase 9B, hardened)
# Backward compatible: keeps run_telegram_summaries(force=False), env toggles, and tab names.
# Adds param-compat wrappers for utils.tg_should_send / send_telegram_message_dedup.

import os
from datetime import datetime, timezone
import time
from utils import (
    send_telegram_message_dedup as _send_dedup_raw,
    tg_should_send as _tg_should_send_raw,
    get_all_records_cached,
    get_value_cached,
    detect_stalled_tokens,
    warn, info, str_or_empty
)

# Env toggles
ENABLED = (os.getenv("TELEGRAM_SUMMARIES_ENABLED", "1").lower() in ("1", "true", "yes"))
DEDUP_TTL_MIN = int(os.getenv("TELEGRAM_SUMMARIES_TTL_MIN", "1440"))  # 24h
SUMMARY_KEY_BASE = os.getenv("TELEGRAM_SUMMARY_KEY_BASE", "telegram_summary_daily")

# Optional sheet/tab names (best-effort; code never crashes if missing)
ROTATION_LOG_TAB     = os.getenv("ROTATION_LOG_TAB", "Rotation_Log")
ROTATION_STATS_TAB   = os.getenv("ROTATION_STATS_TAB", "Rotation_Stats")
ROTATION_MEMORY_TAB  = os.getenv("ROTATION_MEMORY_WS", "Rotation_Memory")
VAULT_INTEL_TAB      = os.getenv("VAULT_INTELLIGENCE_WS", "Vault_Intelligence")
PERF_DASH_TAB        = os.getenv("PERF_DASHBOARD_WS", "Performance_Dashboard")
HEARTBEAT_TAB        = os.getenv("HEARTBEAT_WS", "NovaHeartbeat")
SUMMARY_LOG_TAB      = os.getenv("SUMMARY_LOG_WS", "Summary_Log")  # optional audit

# ---- Param-compat wrappers (ttl_min vs ttl_minutes) ----
def _tg_should_send(kind: str, key: str, ttl_min: int, consume: bool = True) -> bool:
    try:
        return _tg_should_send_raw(kind, key=key, ttl_min=ttl_min, consume=consume)
    except TypeError:
        # older utils version
        return _tg_should_send_raw(kind, key=key, ttl_minutes=ttl_min, consume=consume)

def _send_dedup(msg: str, key: str, ttl_min: int):
    try:
        return _send_dedup_raw(msg, key=key, ttl_min=ttl_min)
    except TypeError:
        # older utils version
        return _send_dedup_raw(msg, key=key, ttl_minutes=ttl_min)

# ---- Helpers ----
def _utc_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _safe_float(x):
    try:
        s = str(x).replace("%","").replace(",","").strip()
        if s == "" or s.upper() == "N/A":
            return None
        return float(s)
    except Exception:
        return None

def _fmt_pct(v, places=1):
    return "—" if v is None else f"{v:.{places}f}%"

def _mean(xs):
    vals = [v for v in xs if v is not None]
    return (sum(vals) / len(vals)) if vals else None
    
def _get_telemetry_snapshot():
    """
    Best-effort read of the latest Edge telemetry snapshot from the Bus.

    Returns dict with:
      - flat: {symbol -> qty}
      - by_venue: {VENUE -> {asset -> qty}}
      - age_sec: seconds since snapshot (or None)
    Never raises; falls back to empty structures.
    """
    try:
        # Import lazily to avoid circulars at import time; wsgi is the Bus module.
        import wsgi as bus_wsgi  # type: ignore
        snap = getattr(bus_wsgi, "_last_tel", {}) or {}
    except Exception:
        snap = {}

    flat = snap.get("flat") or {}
    by_venue = snap.get("by_venue") or {}
    ts = snap.get("ts") or 0

    age = None
    try:
        if ts:
            age = int(time.time()) - int(ts)
    except Exception:
        age = None

    return {"flat": flat, "by_venue": by_venue, "age_sec": age}

def _try_get(ws_name, ttl_s=120):
    try:
        return get_all_records_cached(ws_name, ttl_s=ttl_s)
    except Exception as e:
        warn(f"telegram_summaries: {ws_name} read failed: {e}")
        return []

def _latest_nonempty(rows):
    # choose last record that has at least one non-empty value
    for r in reversed(rows or []):
        if any(str(v).strip() for v in r.values()):
            return r
    return rows[-1] if rows else {}

def _best_effort_counts():
    counts = {}

    # Telemetry snapshot (Edge balances → Bus)
    try:
        tel = _get_telemetry_snapshot()
        counts["tel_by_venue"] = tel.get("by_venue") or {}
        counts["tel_flat"] = tel.get("flat") or {}
        counts["tel_age_sec"] = tel.get("age_sec")
    except Exception as e:
        warn(f"telegram_summaries: telemetry snapshot read failed: {e}")
        counts["tel_by_venue"] = {}
        counts["tel_flat"] = {}
        counts["tel_age_sec"] = None

    # Rotation_Log size
    try:
        rows = get_all_records_cached(ROTATION_LOG_TAB, ttl_s=60)
        counts["rotation_rows"] = len(rows)
    except Exception as e:
        warn(f"telegram_summaries: Rotation_Log read failed: {e}")
        counts["rotation_rows"] = "—"

    # stalled tokens (watchdog)
    try:
        stalled = detect_stalled_tokens()
        counts["stalled"] = len(stalled)
    except Exception as e:
        warn(f"telegram_summaries: stalled detector failed: {e}")
        counts["stalled"] = "—"

    # Portfolio summary (prefer Performance_Dashboard, fallback to Rotation_Stats averages)
    dash = _try_get(PERF_DASH_TAB, ttl_s=180)
    rstats = _try_get(ROTATION_STATS_TAB, ttl_s=120)

    p1 = p7 = p30 = None
    if dash:
        latest = _latest_nonempty(dash)
        p1  = _safe_float(latest.get("Portfolio_1d") or latest.get("Portfolio_1D") or latest.get("1D"))
        p7  = _safe_float(latest.get("Portfolio_7d") or latest.get("Portfolio_7D") or latest.get("7D"))
        p30 = _safe_float(latest.get("Portfolio_30d") or latest.get("Portfolio_30D") or latest.get("30D"))

    if p1 is None and rstats:
        p1 = _mean([_safe_float(r.get("Follow-up ROI")) for r in rstats])
    if p7 is None and rstats:
        p7 = _mean([_safe_float(r.get("ROI_7d") or r.get("ROI 7d") or r.get("ROI7d")) for r in rstats])
    if p30 is None and rstats:
        p30 = _mean([_safe_float(r.get("ROI_30d") or r.get("ROI 30d") or r.get("ROI30d")) for r in rstats])

    counts["p1"] = p1; counts["p7"] = p7; counts["p30"] = p30

    # Top Rotations by Weighted_Score
    mem = _try_get(ROTATION_MEMORY_TAB, ttl_s=180)
    try:
        pairs = []
        for r in mem:
            t = str(r.get("Token","")).strip().upper()
            s = _safe_float(r.get("Weighted_Score"))
            if t and s is not None:
                pairs.append((t, s))
        pairs.sort(key=lambda x: x[1], reverse=True)
        counts["top_rot"] = pairs[:3]
    except Exception as e:
        warn(f"telegram_summaries: Rotation_Memory parse failed: {e}")
        counts["top_rot"] = []

    # Top Vaults by 7D ROI (fallback: Follow-up ROI)
    vint = _try_get(VAULT_INTEL_TAB, ttl_s=180)
    try:
        vpairs = []
        for r in vint:
            t = str(r.get("Token") or r.get("Asset") or "").strip().upper()
            s = _safe_float(r.get("roi_7d") or r.get("ROI_7d") or r.get("ROI 7d") or r.get("Follow-up ROI"))
            if t and s is not None:
                vpairs.append((t, s))
        vpairs.sort(key=lambda x: x[1], reverse=True)
        counts["top_vaults"] = vpairs[:3]
    except Exception as e:
        warn(f"telegram_summaries: Vault_Intelligence parse failed: {e}")
        counts["top_vaults"] = []

    # Optional heartbeat cell (e.g., A2 = "Edge 2m" or timestamp)
    try:
        hb_val = str_or_empty(get_value_cached(HEARTBEAT_TAB, "A2", ttl_s=60)) or "—"
        counts["heartbeat"] = hb_val
    except Exception:
        counts["heartbeat"] = "—"

    return counts

def _format_message(counts):
    today = _utc_date()

    p1  = counts.get("p1")
    p7  = counts.get("p7")
    p30 = counts.get("p30")

    rot = counts.get("top_rot") or []
    vts = counts.get("top_vaults") or []

    rot_str = " | ".join([f"{t} {s:.1f}" for (t, s) in rot]) if rot else "—"
    vt_str  = " | ".join([f"{t} {_fmt_pct(r)}" for (t, r) in vts]) if vts else "—"

    lines = [
        f"🧭 *NovaScore Daily* — {today} (UTC)",
        f"• Portfolio: {_fmt_pct(p1)} (7D {_fmt_pct(p7)}, 30D {_fmt_pct(p30)})",
        f"• Rotation_Log rows: {counts.get('rotation_rows', '—')}",
        f"• Top Rotations (Weighted): {rot_str}",
        f"• Top Vaults (7D): {vt_str}",
        f"• Stalled tokens (≥ threshold): {counts.get('stalled', '—')}",
    ]

    # Telemetry section (per-venue balances)
    tel_by = counts.get("tel_by_venue") or {}
    tel_age = counts.get("tel_age_sec")
    if tel_by or tel_age is not None:
        # Age string
        if tel_age is None:
            age_str = "age: unknown"
        elif tel_age < 300:
            age_str = f"age: {int(tel_age)}s"
        else:
            age_str = f"age: {int(tel_age // 60)}m"

        # Prefer to show stable balances per venue
        venue_lines = []
        preferred = ["COINBASE", "BINANCEUS", "KRAKEN"]
        def _venue_key(v):
            v_up = v.upper()
            return (preferred.index(v_up) if v_up in preferred else len(preferred), v_up)

        for venue, assets in sorted(tel_by.items(), key=lambda kv: _venue_key(kv[0])):
            assets = assets or {}
            pieces = []
            for asset in ("USD", "USDC", "USDT"):
                try:
                    val = float(assets.get(asset, 0) or 0)
                except Exception:
                    val = 0.0
                if val > 0:
                    pieces.append(f"{asset} {val:,.0f}")
            if not pieces and assets:
                pieces.append(f"{len(assets)} assets")
            if pieces:
                venue_lines.append(f"{venue}: " + ", ".join(pieces))

        if venue_lines:
            lines.append(f"• Telemetry balances ({age_str}):")
            # Join on one line to keep message compact
            lines.append("    " + " | ".join(venue_lines))
        else:
            lines.append(f"• Telemetry balances ({age_str}): none")

    hb = counts.get("heartbeat", "—")
    if hb != "—":
        lines.append(f"• Heartbeat: {hb}")

    lines.append("—")
    lines.append("This is an automated status ping. Set TELEGRAM_SUMMARIES_ENABLED=0 to disable.")
    return "\n".join(lines)

def _write_summary_log(kind: str, text: str):
    # best-effort audit log
    try:
        from utils import get_gspread_client
        SHEET_URL = os.getenv("SHEET_URL", "")
        if not SHEET_URL:
            return
        gc = get_gspread_client()
        sh = gc.open_by_url(SHEET_URL)
        try:
            ws = sh.worksheet(SUMMARY_LOG_TAB)
        except Exception:
            ws = sh.add_worksheet(title=SUMMARY_LOG_TAB, rows=1000, cols=8)
            ws.append_row(["Timestamp","Kind","Message"], value_input_option="USER_ENTERED")
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([ts, kind, text], value_input_option="USER_ENTERED")
    except Exception as e:
        warn(f"telegram_summaries: could not write Summary_Log: {e}")

def run_telegram_summaries(force: bool = False):
    """
    Called by main.py. Safe no-op if disabled.
    De-duped to 1x/day by default. Use force=True to bypass de-dupe.
    """
    if not ENABLED:
        info("telegram_summaries: disabled via TELEGRAM_SUMMARIES_ENABLED=0 (no-op).")
        return

    key = f"{SUMMARY_KEY_BASE}:{_utc_date()}"
    if not force and not _tg_should_send("daily_summary", key=key, ttl_min=DEDUP_TTL_MIN, consume=True):
        # Already sent today; stay quiet
        return

    try:
        counts = _best_effort_counts()
        msg = _format_message(counts)
        # Use a fixed dedup key so accidental double-calls won't spam
        _send_dedup(msg, key="telegram_summary_daily", ttl_min=DEDUP_TTL_MIN)
        _write_summary_log("daily", msg)
        info("telegram_summaries: summary sent.")
    except Exception as e:
        # Never crash boot due to summary issues
        warn(f"telegram_summaries: failed to send summary: {e}")
