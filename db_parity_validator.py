# db_parity_validator.py
"""
Phase 22B — DB Parity Validator (DB_READ_JSON edition)

Purpose
-------
Lightweight safety check before adopting DB-first reads widely.

What it checks
--------------
1) DB health (URL set, driver present, can query)
2) Table existence + freshness (age of newest row):
   - commands / receipts / nova_telemetry (if present)
3) Soft parity signals (when DB sheet mirror exists):
   - mirror window counts for Trade_Log and Wallet_Monitor

Config (single env var)
-----------------------
DB_READ_JSON may include:
{
  "parity": {
    "enabled": 1,
    "notify": 1,
    "log_policy": 1,
    "window_h": 24,
    "max_rows": 5000
  }
}

Back-compat (optional)
----------------------
If DB_READ_JSON.parity is missing, legacy env vars are honored:
DB_PARITY_ENABLED, DB_PARITY_NOTIFY, DB_PARITY_LOG_POLICY, DB_PARITY_WINDOW_H, DB_PARITY_MAX_ROWS

This validator is designed to be scheduled safely. It never blocks and never raises.
"""

from __future__ import annotations

import os
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from db_read_adapter import db_health, _choose_table, _max_created_at, _pg, DB_READ_STALE_SEC

# Optional: Sheets + Telegram are best-effort only
try:
    from utils import sheets_append_rows, _tg_send_raw, send_telegram_message_dedup  # type: ignore
except Exception:
    sheets_append_rows = None
    _tg_send_raw = None
    send_telegram_message_dedup = None


def _env_bool(name: str, default: str = "1") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _safe_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _load_cfg() -> dict:
    raw = os.getenv("DB_READ_JSON", "") or ""
    if not raw.strip():
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


_CFG = _load_cfg()
_PAR = _CFG.get("parity") if isinstance(_CFG.get("parity"), dict) else None

DB_PARITY_ENABLED = bool(_PAR.get("enabled")) if _PAR is not None else _env_bool("DB_PARITY_ENABLED", "1")
DB_PARITY_NOTIFY  = bool(_PAR.get("notify", True)) if _PAR is not None else _env_bool("DB_PARITY_NOTIFY", "1")
DB_PARITY_LOG_POLICY = bool(_PAR.get("log_policy", True)) if _PAR is not None else _env_bool("DB_PARITY_LOG_POLICY", "1")
DB_PARITY_WINDOW_H = _safe_int(_PAR.get("window_h", 24), 24) if _PAR is not None else _safe_int(os.getenv("DB_PARITY_WINDOW_H", "24"), 24)
DB_PARITY_MAX_ROWS = _safe_int(_PAR.get("max_rows", 5000), 5000) if _PAR is not None else _safe_int(os.getenv("DB_PARITY_MAX_ROWS", "5000"), 5000)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _age_str(age_s: Optional[float]) -> str:
    if age_s is None:
        return "n/a"
    if age_s < 60:
        return f"{int(age_s)}s"
    if age_s < 3600:
        return f"{int(age_s//60)}m"
    return f"{age_s/3600:.1f}h"


def _count_since(table: str, since_dt: datetime) -> int:
    rows = _pg.query(
        f"select count(*) as n from {table} where created_at >= %s",
        (since_dt,),
    )
    try:
        return int(rows[0]["n"]) if rows else 0
    except Exception:
        return 0


def _mirror_count_for_tab(tab: str, since_dt: datetime) -> Optional[int]:
    if not _choose_table("sheet_mirror"):
        return None
    rows = _pg.query(
        "select count(*) as n from sheet_mirror_events where tab=%s and created_at >= %s",
        (tab, since_dt),
    )
    try:
        return int(rows[0]["n"]) if rows else 0
    except Exception:
        return None


def run_db_parity_validator() -> dict:
    """
    Returns a dict report. Safe: never raises.
    """
    if not DB_PARITY_ENABLED:
        return {"ok": True, "skipped": True, "reason": "DB parity disabled"}

    now = _utcnow()
    since = now - timedelta(hours=DB_PARITY_WINDOW_H)
    health = db_health()

    report: Dict[str, Any] = {
        "ts": now.isoformat(),
        "db": health,
        "streams": {},
        "ok": True,
    }

    stale_cut = DB_READ_STALE_SEC  # keep aligned with adapter default / cfg

    def add_stream(name: str, table: Optional[str], mirror_tab: Optional[str] = None):
        if not table:
            report["streams"][name] = {"table": None, "warn": "missing"}
            report["ok"] = False
            return

        ts = _max_created_at(table)
        age_s = (time.time() - ts) if ts else None
        n = _count_since(table, since) if ts else 0
        item: Dict[str, Any] = {"table": table, "age": age_s, "n_window": n}

        if age_s is not None and age_s > stale_cut:
            item["warn"] = "stale"
            report["ok"] = False

        if mirror_tab:
            m = _mirror_count_for_tab(mirror_tab, since)
            item["mirror_window"] = m
        report["streams"][name] = item

    add_stream("commands", _choose_table("commands"))
    add_stream("receipts", _choose_table("receipts"), mirror_tab="Trade_Log")
    add_stream("telemetry", _choose_table("telemetry"), mirror_tab="Wallet_Monitor")

    # Soft parity: if mirror exists and is dramatically smaller than DB in window, warn.
    rec = report["streams"].get("receipts") or {}
    if rec.get("mirror_window") is not None and rec.get("n_window"):
        try:
            if rec["mirror_window"] < int(0.4 * rec["n_window"]):
                rec["warn"] = (rec.get("warn") or "") + "|mirror_low"
                report["ok"] = False
        except Exception:
            pass

    _emit(report)
    return report


def _emit(report: dict) -> None:
    try:
        streams = report.get("streams") or {}
        cmd = streams.get("commands") or {}
        rec = streams.get("receipts") or {}
        tel = streams.get("telemetry") or {}

        ok = bool(report.get("ok"))

        msg = (
            f"🧪 DB Parity (22B)\n"
            f"commands: {cmd.get('table') or '—'} age={_age_str(cmd.get('age'))} win={cmd.get('n_window','?')}"
            f"{' ⚠️' if cmd.get('warn') else ''}\n"
            f"receipts: {rec.get('table') or '—'} age={_age_str(rec.get('age'))} win={rec.get('n_window','?')} "
            f"mirror={rec.get('mirror_window') if rec.get('mirror_window') is not None else '—'}"
            f"{' ⚠️' if rec.get('warn') else ''}\n"
            f"telemetry: {tel.get('table') or '—'} age={_age_str(tel.get('age'))} win={tel.get('n_window','?')} "
            f"mirror={tel.get('mirror_window') if tel.get('mirror_window') is not None else '—'}\n"
            f"result: {'✅ OK' if ok else '⚠️ ATTENTION'}"
        )

        if DB_PARITY_NOTIFY:
            try:
                if send_telegram_message_dedup:
                    send_telegram_message_dedup(msg, key="db_parity", ttl_min=30)
                elif _tg_send_raw:
                    _tg_send_raw(msg)
            except Exception:
                pass

        if DB_PARITY_LOG_POLICY and sheets_append_rows:
            try:
                sheet_url = os.getenv("SHEET_URL", "")
                if sheet_url:
                    sheets_append_rows(sheet_url, "Policy_Log", [[
                        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "DB_PARITY",
                        "OK" if ok else "WARN",
                        msg.replace("\n", " | "),
                    ]])
            except Exception:
                pass
    except Exception:
        return
