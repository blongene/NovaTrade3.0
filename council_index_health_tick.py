# council_index_health_tick.py
"""Council Index Health Tick (Bus-driven)

Purpose
-------
Replaces legacy Apps Script 'indexHealth' checks with a Bus/DB-driven health tick.

What it does (safe, read-mostly)
--------------------------------
- Verifies key Sheet tabs exist and have expected headers.
- Emits a single Council_Insight row if issues are detected (or if force=True).
- Never blocks trading; never enqueues commands.

Config (optional)
-----------------
DB_READ_JSON:
  {
    "council_rollups": {
      "index_health": {
        "enabled": 1,
        "tab": "Council_Insight",
        "check_tabs": ["Why_Nothing_Happened","Decision_Analytics","Council_Insight"],
        "dedupe_ttl_sec": 21600
      }
    }
  }
"""

from __future__ import annotations

import os, json, time, hashlib, logging
from datetime import datetime, timezone
from typing import Any, Dict, List

log = logging.getLogger("council_index_health_tick")

def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).strip().lower() in {"1","true","yes","y","on"}

def _load_db_read_json() -> dict:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        o = json.loads(raw)
        return o if isinstance(o, dict) else {}
    except Exception:
        return {}

def _cfg() -> dict:
    cfg = _load_db_read_json()
    roll = cfg.get("council_rollups") or {}
    if isinstance(roll, dict):
        sub = roll.get("index_health") or {}
        return sub if isinstance(sub, dict) else {}
    return {}

def _tabname() -> str:
    return str(_cfg().get("tab") or "Council_Insight").strip() or "Council_Insight"

def _check_tabs() -> List[str]:
    v = _cfg().get("check_tabs")
    if isinstance(v, list) and v:
        return [str(x) for x in v]
    return ["Why_Nothing_Happened","Decision_Analytics","Council_Insight"]

def _dedupe_ttl() -> int:
    try:
        return int(_cfg().get("dedupe_ttl_sec") or 21600)
    except Exception:
        return 21600

def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def _get_ws(tab: str):
    # Prefer your cached helper
    try:
        from utils import get_ws_cached  # type: ignore
        return get_ws_cached(tab, ttl_s=30)
    except Exception:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials

        sheet_url = os.getenv("SHEET_URL")
        if not sheet_url:
            raise RuntimeError("SHEET_URL not set")

        svc = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not svc:
            raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set")

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(svc, scope)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(sheet_url)
        try:
            return sh.worksheet(tab)
        except Exception:
            return sh.add_worksheet(title=tab, rows=4000, cols=50)

def _append_dict(tab: str, row: Dict[str, Any]) -> Dict[str, Any]:
    # Use the same header-driven append pattern as wnh_logger.append_row_dict
    ws = _get_ws(tab)
    try:
        header = ws.row_values(1)
    except Exception:
        vals = ws.get_all_values()
        header = vals[0] if vals else []

    if not header:
        # Create minimal Council_Insight header if missing
        header = [
            "Timestamp","decision_id","Autonomy","OK","Reason","Story","Ash's Lens",
            "Soul","Nova","Orion","Ash","Lumen","Vigil",
            "Raw Intent","Patched","Flags","Exec Timestamp","Exec Status","Exec Cmd_ID",
            "Exec Notional_USD","Exec Quote","Outcome Tag","Mark Price_USD","PnL_USD_Current","PnL_Tag_Current"
        ]
        ws.append_row(header, value_input_option="USER_ENTERED")

    out = [row.get(h, "") for h in header]
    try:
        ws.append_row(out, value_input_option="USER_ENTERED")
    except Exception:
        ws.append_row(out)

    # best-effort DB mirror event
    try:
        from db_mirror import mirror_append  # type: ignore
        mirror_append(tab, [out])
    except Exception:
        pass

    return {"ok": True}

def _dedup_send(key: str, ttl_sec: int) -> bool:
    # Use telegram_summaries-style dedupe if present, otherwise no-op.
    try:
        from telegram_summaries import _seen_recently  # type: ignore
        return _seen_recently(key, ttl_sec=ttl_sec)
    except Exception:
        return False

def run_council_index_health_tick(force: bool = False) -> Dict[str, Any]:
    cfg = _cfg()
    enabled = _truthy(cfg.get("enabled", 1))
    if not enabled and not force:
        return {"ok": False, "skipped": True, "reason": "disabled"}

    issues = []
    for tab in _check_tabs():
        try:
            ws = _get_ws(tab)
            header = ws.row_values(1)
            if not header:
                issues.append(f"{tab}:missing_header")
            else:
                if tab == "Why_Nothing_Happened":
                    needed = {"Timestamp","Token","Stage","Outcome","Primary_Reason"}
                    if not needed.issubset(set(header)):
                        issues.append(f"{tab}:header_mismatch")
                if tab == "Decision_Analytics":
                    needed = {"Timestamp","decision_id","Autonomy"}
                    if not needed.issubset(set(header)):
                        issues.append(f"{tab}:header_mismatch")
                if tab == "Council_Insight":
                    needed = {"Timestamp","decision_id","Autonomy","Reason","Story"}
                    if not needed.issubset(set(header)):
                        issues.append(f"{tab}:header_mismatch")
        except Exception as e:
            issues.append(f"{tab}:{e.__class__.__name__}")

    decision_id = f"index_health_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    dedupe_key = f"council_index_health:{'|'.join(issues) if issues else 'ok'}"
    if not force and _dedup_send(dedupe_key, ttl_sec=_dedupe_ttl()):
        return {"ok": True, "rows": 0, "deduped": True, "issues": issues}

    # Only write a row if issues or force
    if issues or force:
        row = {
            "Timestamp": _utc_now(),
            "decision_id": decision_id,
            "Autonomy": "council_index_health",
            "OK": "TRUE" if not issues else "FALSE",
            "Reason": "INDEX_HEALTH",
            "Story": ("Council index health OK" if not issues else ("Issues: " + ", ".join(issues))),
            "Ash's Lens": "clean" if not issues else "attention",
            "Outcome Tag": "INDEX_OK" if not issues else "INDEX_WARN",
            "Flags": json.dumps(["index_health"], ensure_ascii=False),
            "Raw Intent": json.dumps({"check_tabs": _check_tabs(), "issues": issues}, ensure_ascii=False),
        }
        from event_store import put_council_event
        put_council_event(decision_id, row, tab=_tabname())
        return {"ok": True, "rows": 1, "issues": issues, "tab": _tabname(), "decision_id": decision_id}

    return {"ok": True, "rows": 0, "issues": issues}
