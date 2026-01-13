# alpha_proposals_mirror.py
"""
Phase 26A — Mirror alpha_proposals → Google Sheet (Alpha_Proposals)

This is *presentation only*. It does NOT trigger trades or commands.

Why Step 3 exists
- Render Bus env var slots are constrained.
- We avoid adding new env vars by reading routing/toggles from DB_READ_JSON when present.
- We also avoid "one pointer fits all" confusion: proposals mirror should have its own tab
  independent of any ideas intake surfaces.

Enable (preview-only):
- PREVIEW_ENABLED=1
- ALPHA_PREVIEW_PROPOSALS_ENABLED=1

Optional toggles (no new env vars required):
- DB_READ_JSON.phase25.alpha.mirror.enabled (default true)
- DB_READ_JSON.phase25.alpha.mirror.silence_row (default true)
- DB_READ_JSON.phase25.alpha.sources.proposals_tab (default "Alpha_Proposals")

Legacy / fallback env vars (still supported):
- ALPHA_SHEETS_MIRROR_ENABLED (default 1)
- ALPHA_SHEET_TAB (fallback tab if DB_READ_JSON does not specify proposals_tab)
- ALPHA_SHEET_LIMIT (default 200)

DB:
- Reads from alpha_proposals table
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Optional, List, Any, Dict

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


# ----------------------------
# helpers
# ----------------------------

def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _load_db_read_json() -> Dict[str, Any]:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _cfg_alpha() -> Dict[str, Any]:
    cfg = _load_db_read_json()
    phase25 = cfg.get("phase25") or {}
    if not isinstance(phase25, dict):
        return {}
    alpha = phase25.get("alpha") or {}
    return alpha if isinstance(alpha, dict) else {}


def _sources() -> Dict[str, Any]:
    alpha = _cfg_alpha()
    src = alpha.get("sources") or {}
    return src if isinstance(src, dict) else {}


def _mirror_cfg() -> Dict[str, Any]:
    alpha = _cfg_alpha()
    m = alpha.get("mirror") or {}
    return m if isinstance(m, dict) else {}


def _mirror_enabled() -> bool:
    # Prefer DB_READ_JSON toggle (no env slot) when present
    m = _mirror_cfg()
    if "enabled" in m:
        return _truthy(m.get("enabled"))
    # Legacy env fallback
    return _truthy(os.getenv("ALPHA_SHEETS_MIRROR_ENABLED", "1"))


def _silence_row_enabled() -> bool:
    m = _mirror_cfg()
    if "silence_row" in m:
        return _truthy(m.get("silence_row"))
    # Default ON for Phase 26A trust-building
    return True


def _target_tab() -> str:
    # Prefer per-surface routing from DB_READ_JSON (no env slot)
    src = _sources()
    tab = src.get("proposals_tab") or src.get("proposalsTab") or None
    if isinstance(tab, str) and tab.strip():
        return tab.strip()

    # Legacy fallback: if operator is still using ALPHA_SHEET_TAB, honor it
    env_tab = os.getenv("ALPHA_SHEET_TAB", "") or ""
    if env_tab.strip():
        return env_tab.strip()

    # Default
    return "Alpha_Proposals"


def _get_db_url() -> Optional[str]:
    return os.getenv("DB_URL") or os.getenv("DATABASE_URL")


def _connect():
    db_url = _get_db_url()
    if not db_url:
        warn("alpha_proposals_mirror: DB_URL not set; skipping.")
        return None
    if not psycopg2:
        warn("alpha_proposals_mirror: psycopg2 not available; skipping.")
        return None
    try:
        return psycopg2.connect(db_url, connect_timeout=10)
    except Exception as e:
        warn(f"alpha_proposals_mirror: DB connect failed; skipping. err={e}")
        return None


def _rows_for_today(cur, limit: int = 200) -> List[Dict[str, Any]]:
    # UTC day window (using DB clock)
    cur.execute(
        """
        SELECT
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
    out: List[Dict[str, Any]] = []
    for r in cur.fetchall() or []:
        out.append(
            {
                "ts_utc": str(r[0]),
                "token": r[1],
                "venue": r[2],
                "symbol": r[3],
                "action": r[4],
                "notional_usd": r[5],
                "confidence": r[6],
                "rationale": r[7],
                "gates_json": r[8],
                "proposal_hash": r[9],
            }
        )
    return out


def run_alpha_proposals_mirror() -> None:
    if not (_truthy(os.getenv("PREVIEW_ENABLED")) and _truthy(os.getenv("ALPHA_PREVIEW_PROPOSALS_ENABLED"))):
        info("alpha_proposals_mirror: disabled (PREVIEW_ENABLED and/or ALPHA_PREVIEW_PROPOSALS_ENABLED not set).")
        return

    if not _mirror_enabled():
        info("alpha_proposals_mirror: disabled (mirror.enabled false).")
        return

    conn = _connect()
    if conn is None:
        return

    tab = _target_tab()
    limit = int(os.getenv("ALPHA_SHEET_LIMIT", "200"))

    # Simple, stable schema for Alpha_Proposals.v1 (daily snapshot)
    header = [
        "ts",
        "token",
        "symbol",
        "venue",
        "action",
        "notional_usd",
        "confidence",
        "primary_blocker",
        "rationale",
        "proposal_hash",
    ]

    try:
        cur = conn.cursor()

        # Check table exists quickly
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'alpha_proposals' LIMIT 1;")
        if cur.fetchone() is None:
            warn("alpha_proposals_mirror: missing table alpha_proposals; nothing to mirror.")
            return

        rows = _rows_for_today(cur, limit=limit)

        sheet = get_sheet()
        try:
            ws = sheet.worksheet(tab)
        except Exception:
            warn(f"alpha_proposals_mirror: sheet tab '{tab}' missing; creating.")
            ws = sheet.add_worksheet(title=tab, rows="500", cols="20")

        # Replace-mode mirror (clears tab and writes today's snapshot)
        try:
            ws.clear()
        except Exception:
            pass

        ws.append_row(header)

        if not rows:
            if _silence_row_enabled():
                now = datetime.now(timezone.utc)
                utc_day = now.date().isoformat()
                ws.append_row(
                    [
                        now.isoformat(),
                        "",
                        "",
                        "",
                        "SILENCE_INTENTIONAL",
                        "",
                        "",
                        "NO_PROPOSALS",
                        "No Alpha proposals met gates today (UTC).",
                        f"SILENCE|{utc_day}",
                    ]
                )
                info(f"alpha_proposals_mirror: wrote SILENCE_INTENTIONAL row to '{tab}' (UTC day {utc_day}).")
            else:
                info("alpha_proposals_mirror: no rows to mirror (UTC day); silence_row disabled.")
            return

        values: List[List[str]] = []
        for r in rows:
            primary_blocker = ""
            try:
                gates = json.loads(r.get("gates_json") or "{}")
                if isinstance(gates, dict):
                    primary_blocker = str(gates.get("primary_blocker") or "")
            except Exception:
                primary_blocker = ""

            values.append(
                [
                    r.get("ts_utc") or "",
                    r.get("token") or "",
                    r.get("symbol") or "",
                    r.get("venue") or "",
                    r.get("action") or "",
                    str(r.get("notional_usd") or ""),
                    str(r.get("confidence") or ""),
                    primary_blocker,
                    r.get("rationale") or "",
                    r.get("proposal_hash") or "",
                ]
            )

        # batch append for quota safety
        ws.append_rows(values, value_input_option="RAW")
        info(f"alpha_proposals_mirror: mirrored {len(values)} rows to sheet tab '{tab}'.")

    except Exception as e:
        error(f"alpha_proposals_mirror failed: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":  # pragma: no cover
    run_alpha_proposals_mirror()
