# alpha_proposals_mirror.py
"""
Phase 26A — Mirror alpha_proposals → Google Sheet (Alpha_Ideas)

This is *presentation only*. It does NOT trigger trades or commands.

Enable:
- PREVIEW_ENABLED=1
- ALPHA_PREVIEW_PROPOSALS_ENABLED=1
- ALPHA_SHEETS_MIRROR_ENABLED=1   (recommended)

Sheet:
- Uses SHEET_URL (same as the rest of Bus)
- Default tab: Alpha_Ideas
- Override with ALPHA_SHEET_TAB

DB:
- Reads from alpha_proposals table
"""

from __future__ import annotations

import os
import json
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


def _truthy(v: Optional[str]) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes", "y", "on")


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
          proposal_id::text,
          token,
          COALESCE(venue,'') AS venue,
          COALESCE(symbol,'') AS symbol,
          action,
          COALESCE(notional_usd::text,'') AS notional_usd,
          COALESCE(confidence::text,'') AS confidence,
          COALESCE(rationale,'') AS rationale,
          COALESCE(gates::text,'{}') AS gates_json,
          COALESCE(payload::text,'{}') AS payload_json,
          proposal_hash
        FROM alpha_proposals
        WHERE (ts AT TIME ZONE 'UTC')::date = (NOW() AT TIME ZONE 'UTC')::date
        ORDER BY ts DESC
        LIMIT %s;
        """,
        (limit,),
    )
    out = []
    for r in cur.fetchall() or []:
        out.append(
            {
                "ts_utc": str(r[0]),
                "proposal_id": r[1],
                "token": r[2],
                "venue": r[3],
                "symbol": r[4],
                "action": r[5],
                "notional_usd": r[6],
                "confidence": r[7],
                "rationale": r[8],
                "gates_json": r[9],
                "payload_json": r[10],
                "proposal_hash": r[11],
            }
        )
    return out


def run_alpha_proposals_mirror() -> None:
    if not (_truthy(os.getenv("PREVIEW_ENABLED")) and _truthy(os.getenv("ALPHA_PREVIEW_PROPOSALS_ENABLED"))):
        info("alpha_proposals_mirror: disabled (PREVIEW_ENABLED and/or ALPHA_PREVIEW_PROPOSALS_ENABLED not set).")
        return

    if not _truthy(os.getenv("ALPHA_SHEETS_MIRROR_ENABLED", "1")):
        info("alpha_proposals_mirror: ALPHA_SHEETS_MIRROR_ENABLED=0; skipping.")
        return

    conn = _connect()
    if conn is None:
        return

    tab = os.getenv("ALPHA_SHEET_TAB", "Alpha_Ideas")
    limit = int(os.getenv("ALPHA_SHEET_LIMIT", "200"))

    try:
        cur = conn.cursor()

        # Check table exists quickly
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'alpha_proposals' LIMIT 1;"
        )
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

        header = [
            "TimestampUTC",
            "Proposal_ID",
            "Token",
            "Venue",
            "Symbol",
            "Action",
            "Notional_USD",
            "Confidence",
            "Rationale",
            "Gates_JSON",
            "Payload_JSON",
            "Proposal_Hash",
        ]

        # Rebuild the tab for today's view (simple, robust)
        ws.clear()
        ws.append_row(header)

        if not rows:
            ws.append_row([str(os.getenv("AGENT_ID") or "edge-primary"), "NO_ROWS_TODAY", "", "", "", "", "", "", "No proposals yet for UTC day.", "{}", "{}", ""])
            info("alpha_proposals_mirror: no rows to mirror (UTC day).")
            return

        values = []
        for r in rows:
            values.append(
                [
                    r["ts_utc"],
                    r["proposal_id"],
                    r["token"],
                    r["venue"],
                    r["symbol"],
                    r["action"],
                    r["notional_usd"],
                    r["confidence"],
                    r["rationale"],
                    r["gates_json"],
                    r["payload_json"],
                    r["proposal_hash"],
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
