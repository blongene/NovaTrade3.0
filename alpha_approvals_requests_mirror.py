# alpha_approvals_requests_mirror.py
"""
HOTFIX — Step 4: Do NOT wipe human decisions.

Previous Step 4 wrote Alpha_Approvals in "replace mode" (clear + rewrite), which can erase the operator's
decision before alpha_approvals_sync reads it.

New behavior (safe):
- If the tab is empty, write header.
- Otherwise, preserve existing rows (including decisions/notes).
- Append only NEW proposal_id rows for today's UTC proposals that are not already present.
- Never clears the sheet by default.

You can force a clear (rare) by setting:
- ALPHA_APPROVALS_CLEAR_MODE=1  (env) OR
- DB_READ_JSON.phase25.alpha.mirror.approvals_clear=1
"""

from __future__ import annotations

import os, json
from typing import Any, Dict, List, Optional

try:
    from utils import info, warn, error, get_sheet
except Exception:  # pragma: no cover
    def info(msg: str): print(msg, flush=True)
    def warn(msg: str): print(f"WARNING: {msg}", flush=True)
    def error(msg: str): print(f"ERROR: {msg}", flush=True)
    def get_sheet(): raise RuntimeError("get_sheet unavailable")

try:
    import psycopg2  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None


def _truthy(v: Any) -> bool:
    if v is None: return False
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return v != 0
    return str(v).strip().lower() in {"1","true","yes","y","on"}


def _load_db_read_json() -> Dict[str, Any]:
    raw=(os.getenv("DB_READ_JSON") or "").strip()
    if not raw: return {}
    try:
        obj=json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _cfg_alpha() -> Dict[str, Any]:
    cfg=_load_db_read_json()
    phase25=cfg.get("phase25") or {}
    if not isinstance(phase25, dict): return {}
    alpha=phase25.get("alpha") or {}
    return alpha if isinstance(alpha, dict) else {}


def _sources() -> Dict[str, Any]:
    alpha=_cfg_alpha()
    src=alpha.get("sources") or {}
    return src if isinstance(src, dict) else {}


def _mirror_cfg() -> Dict[str, Any]:
    alpha=_cfg_alpha()
    m=alpha.get("mirror") or {}
    return m if isinstance(m, dict) else {}


def _approvals_clear_mode() -> bool:
    m=_mirror_cfg()
    if "approvals_clear" in m:
        return _truthy(m.get("approvals_clear"))
    return _truthy(os.getenv("ALPHA_APPROVALS_CLEAR_MODE", "0"))


def _approvals_tab() -> str:
    src=_sources()
    tab=src.get("approvals_tab") or src.get("approvalsTab")
    if isinstance(tab, str) and tab.strip():
        return tab.strip()
    env_tab=os.getenv("ALPHA_APPROVALS_SHEET_TAB","") or ""
    return env_tab.strip() if env_tab.strip() else "Alpha_Approvals"


def _get_db_url() -> Optional[str]:
    return os.getenv("DB_URL") or os.getenv("DATABASE_URL")


def _connect():
    db_url=_get_db_url()
    if not db_url:
        warn("alpha_approvals_requests_mirror: DB_URL not set; skipping.")
        return None
    if not psycopg2:
        warn("alpha_approvals_requests_mirror: psycopg2 not available; skipping.")
        return None
    try:
        return psycopg2.connect(db_url, connect_timeout=10)
    except Exception as e:
        warn(f"alpha_approvals_requests_mirror: DB connect failed; skipping. err={e}")
        return None


def _rows_for_today(cur, limit:int=200) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
          proposal_id::text,
          ts AT TIME ZONE 'UTC' AS ts_utc,
          token,
          COALESCE(venue,'') AS venue,
          COALESCE(symbol,'') AS symbol,
          action,
          COALESCE(notional_usd::text,'') AS notional_usd,
          COALESCE(confidence::text,'') AS confidence,
          COALESCE(rationale,'') AS rationale,
          COALESCE(gates::text,'{}') AS gates_json,
          proposal_hash
        FROM alpha_proposals
        WHERE (ts AT TIME ZONE 'UTC')::date = (NOW() AT TIME ZONE 'UTC')::date
        ORDER BY ts DESC
        LIMIT %s;
        """,
        (limit,),
    )
    out=[]
    for r in cur.fetchall() or []:
        out.append({
            "proposal_id": r[0] or "",
            "ts_utc": str(r[1]),
            "token": (r[2] or "").upper(),
            "venue": r[3] or "",
            "symbol": r[4] or "",
            "action": r[5] or "",
            "notional_usd": r[6] or "",
            "confidence": r[7] or "",
            "rationale": r[8] or "",
            "gates_json": r[9] or "{}",
            "proposal_hash": r[10] or "",
        })
    return out


def run_alpha_approvals_requests_mirror() -> None:
    if not (_truthy(os.getenv("PREVIEW_ENABLED")) and _truthy(os.getenv("ALPHA_PREVIEW_PROPOSALS_ENABLED"))):
        info("alpha_approvals_requests_mirror: disabled (PREVIEW_ENABLED and/or ALPHA_PREVIEW_PROPOSALS_ENABLED not set).")
        return
    if not _truthy(os.getenv("ALPHA_APPROVALS_REQUESTS_MIRROR_ENABLED", "1")):
        info("alpha_approvals_requests_mirror: mirror disabled by env; skipping.")
        return

    conn=_connect()
    if conn is None:
        return

    tab=_approvals_tab()
    limit=int(os.getenv("ALPHA_APPROVALS_SHEET_LIMIT","200"))
    default_actor=os.getenv("ALPHA_APPROVER_ACTOR","human")

    header=[
        "ts","proposal_id","proposal_hash","token","decision","actor","note",
        "venue","symbol","action","notional_usd","confidence","primary_blocker","rationale"
    ]

    try:
        cur=conn.cursor()
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name='alpha_proposals' LIMIT 1;")
        if cur.fetchone() is None:
            warn("alpha_approvals_requests_mirror: missing table alpha_proposals; nothing to mirror.")
            return

        rows=_rows_for_today(cur, limit=limit)

        sheet=get_sheet()
        try:
            ws=sheet.worksheet(tab)
        except Exception:
            warn(f"alpha_approvals_requests_mirror: sheet tab '{tab}' missing; creating.")
            ws=sheet.add_worksheet(title=tab, rows="500", cols="20")

        existing=ws.get_all_values() or []
        if _approvals_clear_mode():
            warn(f"alpha_approvals_requests_mirror: CLEAR MODE enabled; clearing '{tab}'.")
            try: ws.clear()
            except Exception: pass
            existing=[]

        if not existing:
            ws.append_row(header)
            existing=[header]

        existing_ids=set()
        for r in existing[1:]:
            if len(r) > 1 and (r[1] or "").strip():
                existing_ids.add(r[1].strip())

        new_values=[]
        for r in rows:
            pid=r.get("proposal_id") or ""
            if not pid or pid in existing_ids:
                continue
            primary_blocker=""
            try:
                gates=json.loads(r.get("gates_json") or "{}")
                if isinstance(gates, dict):
                    primary_blocker=str(gates.get("primary_blocker") or "")
            except Exception:
                primary_blocker=""

            new_values.append([
                r.get("ts_utc") or "",
                pid,
                r.get("proposal_hash") or "",
                r.get("token") or "",
                "",
                default_actor,
                "",
                r.get("venue") or "",
                r.get("symbol") or "",
                r.get("action") or "",
                str(r.get("notional_usd") or ""),
                str(r.get("confidence") or ""),
                primary_blocker,
                r.get("rationale") or "",
            ])

        if not new_values:
            info(f"alpha_approvals_requests_mirror: no new approval rows to append (tab='{tab}').")
            return

        ws.append_rows(new_values, value_input_option="RAW")
        info(f"alpha_approvals_requests_mirror: appended {len(new_values)} new approval-candidate rows to sheet tab '{tab}'.")

    except Exception as e:
        error(f"alpha_approvals_requests_mirror failed: {e}")
    finally:
        try: conn.close()
        except Exception: pass


if __name__ == "__main__":  # pragma: no cover
    run_alpha_approvals_requests_mirror()
