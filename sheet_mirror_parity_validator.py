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

One-shot capstone verification:
- If run as a script/module (python -m sheet_mirror_parity_validator), prints a concise summary
  and performs a DB-only reconstruction sanity test (no Sheets fallback) for at least one tab.
"""

from __future__ import annotations

import os
import json
import time
import logging
import hashlib
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_TABS = [
    "Rotation_Log",
    "Rotation_Stats",
    "Trade_Log",
    "Wallet_Monitor",
    "Unified_Snapshot",
]

# Prefer tabs we know are commonly present in mirror events in your environment
_CAPSTONE_PREFERRED_TABS = [
    "Wallet_Monitor",
    "Trade_Log",
    "Unified_Snapshot",
    "Rotation_Stats",
    "Rotation_Log",
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


def _compare(
    tab: str,
    sheets_rows: List[Dict[str, Any]],
    db_rows: List[Dict[str, Any]],
    max_compare: int
) -> Dict[str, Any]:
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


def _choose_capstone_tab(tabs: List[str]) -> Optional[str]:
    # Prefer known tabs, but only if present in the validator's tabs list.
    tabset = set(tabs)
    for t in _CAPSTONE_PREFERRED_TABS:
        if t in tabset:
            return t
    return tabs[0] if tabs else None


def _capstone_db_only_reconstruct(
    get_records_prefer_db,
    tab: str
) -> Tuple[int, Optional[Dict[str, Any]], str]:
    """
    DB-only reconstruction test: forces sheets_fallback_fn to return [] to prove DB is working.
    Returns: (row_count, sample_row, reason)
    """
    try:
        rows = get_records_prefer_db(
            sheet_tab=tab,
            logical_stream=f"sheet_mirror:{tab}",
            ttl_s=5,
            sheets_fallback_fn=lambda sheet_tab, ttl_s=120: [],
        )
        if not rows:
            return 0, None, "db_returned_0_rows"
        sample = rows[0] if isinstance(rows[0], dict) else {"_sample": str(rows[0])}
        return len(rows), sample, "ok"
    except Exception as e:
        return 0, None, f"exception:{e}"


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


def _print_capstone_summary(result: Dict[str, Any], capstone: Dict[str, Any]) -> None:
    # Minimal stdout reporting for shell usage
    print("=== Phase 22B Capstone Verification ===")
    print(f"parity_ok: {bool(result.get('ok'))} skipped: {bool(result.get('skipped', False))}")
    print(f"tabs_configured: {result.get('tabs')} checked: {result.get('checked')} drift: {result.get('drift')}")
    print("--- DB-only reconstruction test ---")
    print(
        f"tab={capstone.get('tab')} rows={capstone.get('rows')} reason={capstone.get('reason')}"
    )
    if capstone.get("sample"):
        sample = capstone["sample"]
        try:
            s = json.dumps(sample, default=str)[:400]
        except Exception:
            s = str(sample)[:400]
        print(f"sample: {s}")
    print("======================================")


if __name__ == "__main__":
    # When run manually, do:
    #  1) parity validator (normal)
    #  2) DB-only reconstruction test (no Sheets fallback)
    # Print to stdout and exit non-zero if DB-only reconstruction is empty.
    try:
        res = run_sheet_mirror_parity_validator()
    except Exception as e:
        print(f"❌ validator crashed: {e}")
        raise

    capstone_info: Dict[str, Any] = {"tab": None, "rows": 0, "sample": None, "reason": "not_run"}
    try:
        from db_read_adapter import get_records_prefer_db  # type: ignore
        tabs = _get_tabs()
        tab = _choose_capstone_tab(tabs) or "Wallet_Monitor"
        n, sample, reason = _capstone_db_only_reconstruct(get_records_prefer_db, tab)
        capstone_info = {"tab": tab, "rows": n, "sample": sample, "reason": reason}
    except Exception as e:
        capstone_info = {"tab": None, "rows": 0, "sample": None, "reason": f"imports_failed:{e}"}

    _print_capstone_summary(res, capstone_info)

    if capstone_info.get("rows", 0) <= 0:
        # Make failure unambiguous in shell
        raise SystemExit(2)
