# council_outcomes_pnl_rollup.py
"""Council Outcomes + PnL Rollup (DB-driven, safe)

Minimal first cut:
- Reads recent receipts (and matching commands) from Postgres
- Emits Council_Insight rows that carry Exec Status / Cmd_ID / Notional / Quote
- No PnL until fills/marks are available (we keep fields blank, not wrong)

Config (DB_READ_JSON)
  {
    "council_rollups": {
      "outcomes_pnl": {
        "enabled": 1,
        "tab": "Council_Insight",
        "lookback_hours": 72,
        "limit": 200
      }
    }
  }
"""

from __future__ import annotations

import os, json, hashlib, logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

log = logging.getLogger("council_outcomes_pnl_rollup")

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
        sub = roll.get("outcomes_pnl") or {}
        return sub if isinstance(sub, dict) else {}
    return {}

def _tabname() -> str:
    return str(_cfg().get("tab") or "Council_Insight").strip() or "Council_Insight"

def _lookback_hours() -> int:
    try:
        return int(_cfg().get("lookback_hours") or 72)
    except Exception:
        return 72

def _limit() -> int:
    try:
        return max(1, min(int(_cfg().get("limit") or 200), 2000))
    except Exception:
        return 200

def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def _db_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("DB_URL")
    if not url:
        raise RuntimeError("DATABASE_URL/DB_URL not set")
    return url

def _conn():
    c = psycopg2.connect(_db_url())
    c.autocommit = True
    return c

def _get_ws(tab: str):
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
            return sh.add_worksheet(title=tab, rows=4000, cols=60)

def _append_dict(tab: str, row: Dict[str, Any]) -> None:
    ws = _get_ws(tab)
    try:
        header = ws.row_values(1)
    except Exception:
        vals = ws.get_all_values()
        header = vals[0] if vals else []
    if not header:
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

    try:
        from db_mirror import mirror_append  # type: ignore
        mirror_append(tab, [out])
    except Exception:
        pass

def _seen_row_hash(cur, tab: str, row_hash: str) -> bool:
    # If sheet_mirror_events exists, use it as dedupe DB-side
    try:
        cur.execute("select 1 from sheet_mirror_events where tab=%s and row_hash=%s limit 1", (tab, row_hash))
        return cur.fetchone() is not None
    except Exception:
        return False

def _note_row_hash(cur, tab: str, row_hash: str, payload: dict) -> None:
    try:
        cur.execute(
            "insert into sheet_mirror_events(tab,row_hash,payload,created_at) values(%s,%s,%s,now())",
            (tab, row_hash, json.dumps(payload))
        )
    except Exception:
        pass

def run_council_outcomes_pnl_rollup(force: bool = False) -> Dict[str, Any]:
    cfg = _cfg()
    enabled = _truthy(cfg.get("enabled", 1))
    if not enabled and not force:
        return {"ok": False, "skipped": True, "reason": "disabled"}

    tab = _tabname()
    lookback = datetime.now(timezone.utc) - timedelta(hours=_lookback_hours())
    limit = _limit()

    rows_written = 0
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            select r.created_at, r.agent_id, r.cmd_id, r.ok, r.receipt, c.intent
            from receipts r
            left join commands c on c.id = r.cmd_id
            where r.created_at >= %s
            order by r.created_at desc
            limit %s
            """,
            (lookback, limit)
        )
        data = cur.fetchall()

        for created_at, agent_id, cmd_id, ok, receipt, intent in data:
            receipt = receipt or {}
            intent = intent or {}

            # Dedupe on cmd_id + receipt status
            status = str((receipt or {}).get("status") or ("ok" if ok else "fail")).upper()
            decision_id = f"receipt_{cmd_id}"
            row_hash = _sha(f"{decision_id}|{status}|{agent_id}")

            if not force and _seen_row_hash(cur, tab, row_hash):
                continue

            venue = (receipt or {}).get("venue") or (intent or {}).get("venue") or ""
            symbol = (receipt or {}).get("symbol") or (intent or {}).get("symbol") or ""
            side = (receipt or {}).get("side") or (intent or {}).get("side") or ""
            amount_usd = (receipt or {}).get("amount_usd")
            if amount_usd is None:
                amount_usd = (intent or {}).get("amount") or (intent or {}).get("amount_usd") or 0

            quote = ""
            try:
                if isinstance(symbol, str) and "/" in symbol:
                    quote = symbol.split("/", 1)[1]
            except Exception:
                quote = ""

            story = f"Exec receipt cmd={cmd_id} {venue} {side} {symbol} status={status}"
            row = {
                "Timestamp": created_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "decision_id": decision_id,
                "Autonomy": "council_outcomes_pnl",
                "OK": "TRUE" if ok else "FALSE",
                "Reason": "RECEIPT_ROLLUP",
                "Story": story,
                "Ash's Lens": "clean" if ok else "attention",
                "Raw Intent": json.dumps({"intent": intent}, ensure_ascii=False, default=str),
                "Patched": json.dumps({"receipt": receipt}, ensure_ascii=False, default=str),
                "Flags": json.dumps((intent or {}).get("flags") or [], ensure_ascii=False),
                "Exec Timestamp": created_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "Exec Status": status,
                "Exec Cmd_ID": cmd_id,
                "Exec Notional_USD": float(amount_usd) if amount_usd not in ("", None) else "",
                "Exec Quote": quote,
                "Outcome Tag": status,
                # leave mark/pnl blank for now
                "Mark Price_USD": "",
                "PnL_USD_Current": "",
                "PnL_Tag_Current": "",
            }

            from event_store import put_council_event
            
            put_council_event(
                decision_id=decision_id,
                payload=row,
                tab=tab,  # "Council_Insight"
            )
            rows_written += 1

    return {"ok": True, "rows": rows_written, "decision_id": decision_id, "tab": tab, "lookback_hours": _lookback_hours()}
