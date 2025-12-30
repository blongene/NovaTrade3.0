# sheet_mirror_parity_validator.py
"""
Phase 22B — Module 9: Sheet Mirror Parity Validator

Validates that DB mirror reconstruction (sheet_mirror_events) matches Google Sheets reads.
Sheets remain primary; DB reads are advisory until parity is proven.

Config:
- DB_READ_JSON:
    enabled: 1/0
    parity_enabled: 1/0
    notify: 1/0
    max_rows: cap for DB reads
    tabs: optional list override
- Env fallback:
    DB_READ_PARITY_TABS=Rotation_Log,Trade_Log
    DB_READ_PARITY_MAX_COMPARE=200
"""

from __future__ import annotations

import os
import json
import time
import logging
import hashlib
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

DEFAULT_TABS = [
    "Rotation_Log",
    "Rotation_Stats",
    "Trade_Log",
    "Wallet_Monitor",
    "Unified_Snapshot",
]

def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def _load_db_read_json() -> dict:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}

_CFG = _load_db_read_json()

def _cfg_get(key: str, default=None):
    v = _CFG.get(key, default)
    return default if v is None else v

def _get_tabs() -> List[str]:
    tabs = _cfg_get("tabs")
    if isinstance(tabs, list) and tabs:
        return [str(x).strip() for x in tabs if str(x).strip()]
    env = (os.getenv("DB_READ_PARITY_TABS") or "").strip()
    if env:
        return [t.strip() for t in env.split(",") if t.strip()]
    return list(DEFAULT_TABS)

def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (row or {}).items():
        kk = str(k).strip()
        if not kk:
            continue
        if isinstance(v, float):
            out[kk] = round(v, 10)
        else:
            out[kk] = v
    return out


def _ts_str(row: Dict[str, Any]) -> str:
    return str(row.get("Timestamp") or row.get("timestamp") or "").strip()

def _key_agent_venue_asset(row: Dict[str, Any]) -> Tuple[str, str, str]:
    agent = str(row.get("Agent") or row.get("agent") or row.get("  Agent") or "").strip()
    venue = str(row.get("Venue") or row.get("venue") or "").strip().upper()
    asset = str(row.get("Asset") or row.get("asset") or "").strip().upper()
    return (agent, venue, asset)

def _latest_per_key(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Latest row per (Agent, Venue, Asset) based on Timestamp string."""
    best: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    best_ts: Dict[Tuple[str, str, str], str] = {}
    for r in rows or []:
        rr = _normalize_row(r)
        k = _key_agent_venue_asset(rr)
        ts = _ts_str(rr)
        if ts >= best_ts.get(k, ""):
            best_ts[k] = ts
            best[k] = rr
    return [best[k] for k in sorted(best.keys())]

def _project_wallet_monitor(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Wallet_Monitor parity should be stable across DB/SHEETS ordering & formatting.
    Snapshot string formatting can differ (float formatting), so we exclude it from hashing.
    """
    r = _normalize_row(row)
    out = {
        "Timestamp": _ts_str(r),
        "Agent": str(r.get("Agent") or r.get("  Agent") or "").strip(),
        "Venue": str(r.get("Venue") or "").strip().upper(),
        "Asset": str(r.get("Asset") or "").strip().upper(),
        "Free": r.get("Free"),
        "Locked": r.get("Locked"),
        "Class": str(r.get("Class") or "").strip().upper(),
    }
    return out

def _project_unified_snapshot(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Unified_Snapshot rows are derived; compare on the same stable subset.
    """
    r = _normalize_row(row)
    out = {
        "Timestamp": _ts_str(r),
        "Agent": str(r.get("Agent") or r.get("  Agent") or "").strip(),
        "Venue": str(r.get("Venue") or "").strip().upper(),
        "Asset": str(r.get("Asset") or "").strip().upper(),
        "Free": r.get("Free"),
        "Locked": r.get("Locked"),
        "Class": str(r.get("Class") or "").strip().upper(),
    }
    return out

def _row_hash(row: Dict[str, Any]) -> str:
    s = json.dumps(_normalize_row(row), sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _should_notify() -> bool:
    return bool(_cfg_get("notify", _env_bool("DB_READ_NOTIFY", "0")))

def _parity_enabled() -> bool:
    return bool(_cfg_get("parity_enabled", _env_bool("DB_READ_PARITY_ENABLED", "1")))

def _max_compare() -> int:
    try:
        return int(os.getenv("DB_READ_PARITY_MAX_COMPARE") or "200")
    except Exception:
        return 200

def _compare(tab: str, sheets_rows: List[Dict[str, Any]], db_rows: List[Dict[str, Any]], max_compare: int) -> Dict[str, Any]:
    # Make parity meaningful for reconstructed streams:
    # - Wallet_Monitor / Unified_Snapshot: compare latest-per-(Agent,Venue,Asset), order independent
    if tab.strip() in ("Wallet_Monitor", "Unified_Snapshot"):
        s_latest = _latest_per_key(sheets_rows)
        d_latest = _latest_per_key(db_rows)
        if tab.strip() == "Wallet_Monitor":
            s_proj = [_project_wallet_monitor(r) for r in s_latest][:max_compare]
            d_proj = [_project_wallet_monitor(r) for r in d_latest][:max_compare]
        else:
            s_proj = [_project_unified_snapshot(r) for r in s_latest][:max_compare]
            d_proj = [_project_unified_snapshot(r) for r in d_latest][:max_compare]
        s_hashes = [_row_hash(r) for r in s_proj]
        d_hashes = [_row_hash(r) for r in d_proj]
        s_set, d_set = set(s_hashes), set(d_hashes)
        overlap = len(s_set & d_set)
        only_s = len(s_set - d_set)
        only_d = len(d_set - s_set)
        s_keys = set().union(*[set(r.keys()) for r in s_proj]) if s_proj else set()
        d_keys = set().union(*[set(r.keys()) for r in d_proj]) if d_proj else set()
        return {
            "tab": tab,
            "sheets_n": len(sheets_rows),
            "db_n": len(db_rows),
            "overlap": overlap,
            "only_sheets": only_s,
            "only_db": only_d,
            "missing_in_db_cols": sorted(list(s_keys - d_keys))[:50],
            "extra_in_db_cols": sorted(list(d_keys - s_keys))[:50],
        }

    # Default behavior: simple hash compare on first N rows
    s = sheets_rows[:max_compare]
    d = db_rows[:max_compare]
    s_hashes = [_row_hash(r) for r in s]
    d_hashes = [_row_hash(r) for r in d]
    s_set, d_set = set(s_hashes), set(d_hashes)

    overlap = len(s_set & d_set)
    only_s = len(s_set - d_set)
    only_d = len(d_set - s_set)

    def key_union(rows):
        u = set()
        for r in rows:
            for k in (r or {}).keys():
                kk = str(k).strip()
                if kk:
                    u.add(kk)
        return u

    s_keys = key_union(s)
    d_keys = key_union(d)

    return {
        "tab": tab,
        "sheets_n": len(sheets_rows),
        "db_n": len(db_rows),
        "overlap": overlap,
        "only_sheets": only_s,
        "only_db": only_d,
        "missing_in_db_cols": sorted(list(s_keys - d_keys))[:50],
        "extra_in_db_cols": sorted(list(d_keys - s_keys))[:50],
    }

def run_sheet_mirror_parity_validator() -> Dict[str, Any]:
    if not _parity_enabled():
        logger.info("sheet_mirror_parity_validator: parity disabled; skipping.")
        return {"ok": True, "skipped": True}

    try:
        import utils  # type: ignore
        from db_read_adapter import get_records_prefer_db  # type: ignore
    except Exception as e:
        logger.warning("sheet_mirror_parity_validator: imports failed; skipping: %s", e)
        return {"ok": False, "skipped": True, "reason": "imports_failed"}

    tabs = _get_tabs()
    max_compare = _max_compare()

    results: List[Dict[str, Any]] = []
    drift: List[Dict[str, Any]] = []

    for tab in tabs:
        try:
            sheets_rows = utils.get_all_records_cached(tab, ttl_s=120)
        except Exception as e:
            logger.warning("sheet_mirror_parity_validator: sheets read failed tab=%s err=%s", tab, e)
            continue

        try:
            db_rows = get_records_prefer_db(
                sheet_tab=tab,
                logical_stream=f"sheet_mirror:{tab}",
                ttl_s=120,
                sheets_fallback_fn=utils.get_all_records_cached,
            )
        except Exception as e:
            logger.warning("sheet_mirror_parity_validator: db read failed tab=%s err=%s", tab, e)
            db_rows = []

        r = _compare(tab, sheets_rows, db_rows, max_compare=max_compare)
        results.append(r)

        count_gap = abs(int(r["sheets_n"]) - int(r["db_n"]))
        col_drift = bool(r["missing_in_db_cols"] or r["extra_in_db_cols"])
        overlap_min = max(1, int(min(len(sheets_rows), len(db_rows), max_compare) * 0.8))
        overlap_ok = r["overlap"] >= overlap_min

        if count_gap > 5 or (not overlap_ok) or col_drift:
            drift.append(r)

        logger.info(
            "sheet_mirror_parity: tab=%s sheets=%s db=%s overlap=%s only_s=%s only_d=%s",
            tab, r["sheets_n"], r["db_n"], r["overlap"], r["only_sheets"], r["only_db"]
        )

    if drift:
        for r in drift[:10]:
            logger.warning(
                "sheet_mirror_parity_drift: tab=%s sheets=%s db=%s overlap=%s only_s=%s only_d=%s missing_cols=%s extra_cols=%s",
                r["tab"], r["sheets_n"], r["db_n"], r["overlap"], r["only_sheets"], r["only_db"],
                ",".join(r["missing_in_db_cols"][:10]) if r["missing_in_db_cols"] else "",
                ",".join(r["extra_in_db_cols"][:10]) if r["extra_in_db_cols"] else "",
            )

        if _should_notify():
            try:
                msg_lines = ["⚠️ DB parity drift detected (sheet_mirror):"]
                for r in drift[:8]:
                    msg_lines.append(f"• {r['tab']}: sheets={r['sheets_n']} db={r['db_n']} overlap={r['overlap']}")
                msg = "\n".join(msg_lines)
                try:
                    from telegram_notify import send_telegram_message  # type: ignore
                    send_telegram_message(msg)
                except Exception:
                    if hasattr(utils, "send_telegram_message"):
                        utils.send_telegram_message(msg)  # type: ignore
            except Exception as e:
                logger.warning("sheet_mirror_parity_validator: notify failed: %s", e)

    return {
        "ok": True,
        "tabs": len(tabs),
        "checked": len(results),
        "drift": len(drift),
        "max_compare": max_compare,
        "ts": int(time.time()),
    }
