#!/usr/bin/env python3
"""
alpha_command_previews_mirror.py - Phase 26A Step 5

Purpose:
- Generate "what WOULD be enqueued" previews (NO enqueue) from:
    alpha_proposals  +  latest alpha_approvals decision per proposal_id
- Mirrors to Google Sheet tab Alpha_CommandPreviews

Safety:
- This module never writes to the bus outbox (commands table).
- Sheet is safe to clear+rewrite each run (operator should not edit previews tab).

Enable:
- PREVIEW_ENABLED=1
- ALPHA_PREVIEW_PROPOSALS_ENABLED=1
- ALPHA_COMMAND_PREVIEWS_MIRROR_ENABLED=1 (default)

Config (preferred via DB_READ_JSON):
DB_READ_JSON.phase25.alpha.sources.command_previews_tab = "Alpha_CommandPreviews"
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

try:
    import psycopg2  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None

try:
    from utils import info, warn, error, get_sheet
except Exception:  # pragma: no cover
    def info(msg: str): print(msg, flush=True)
    def warn(msg: str): print(f"WARNING: {msg}", flush=True)
    def error(msg: str): print(f"ERROR: {msg}", flush=True)
    def get_sheet(): raise RuntimeError("get_sheet unavailable")


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_db_read_json() -> Dict[str, Any]:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _alpha_cfg() -> Dict[str, Any]:
    cfg = _load_db_read_json()
    p25 = cfg.get("phase25") or {}
    if not isinstance(p25, dict):
        return {}
    alpha = p25.get("alpha") or {}
    return alpha if isinstance(alpha, dict) else {}


def _sources_cfg() -> Dict[str, Any]:
    alpha = _alpha_cfg()
    src = alpha.get("sources") or {}
    return src if isinstance(src, dict) else {}


def _tab_name() -> str:
    src = _sources_cfg()
    tab = src.get("command_previews_tab") or src.get("commandPreviewsTab")
    if isinstance(tab, str) and tab.strip():
        return tab.strip()
    env_tab = os.getenv("ALPHA_COMMAND_PREVIEWS_SHEET_TAB", "").strip()
    return env_tab or "Alpha_CommandPreviews"


def _db_url() -> Optional[str]:
    return os.getenv("DB_URL") or os.getenv("DATABASE_URL")


def _connect():
    url = _db_url()
    if not url:
        warn("alpha_command_previews_mirror: DB_URL not set; skipping.")
        return None
    if not psycopg2:
        warn("alpha_command_previews_mirror: psycopg2 not available; skipping.")
        return None
    try:
        return psycopg2.connect(url, connect_timeout=10)
    except Exception as e:
        warn(f"alpha_command_previews_mirror: DB connect failed; err={e}")
        return None


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), sort_keys=True, default=str)
    except Exception:
        return "{}"


def _build_intent(action: str, venue: str, symbol: str, notional_usd: float, meta: Dict[str, Any]) -> Dict[str, Any]:
    action_u = (action or "").upper()
    venue_u = (venue or "").upper()

    # WOULD_WATCH => note preview
    if action_u == "WOULD_WATCH":
        return {
            "type": "note",
            "venue": venue_u,
            "symbol": symbol,
            "payload": {
                "dry_run": True,
                "mode": "dryrun",
                "venue": venue_u,
                "symbol": symbol,
                "note": "Alpha26A preview: WOULD_WATCH (no trade).",
                "meta": meta,
            },
        }

    # WOULD_TRADE => order.place preview (still dryrun)
    amt = float(notional_usd or 0)
    if amt <= 0:
        amt = 25.0  # safe default in previews only
    return {
        "type": "order.place",
        "venue": venue_u,
        "symbol": symbol,
        "payload": {
            "dry_run": True,
            "mode": "dryrun",
            "venue": venue_u,
            "symbol": symbol,
            "side": "BUY",
            "amount_usd": amt,
            "note": "Alpha26A preview: WOULD_TRADE (dryrun).",
            "meta": meta,
        },
    }


def run_alpha_command_previews_mirror(limit: int = 200) -> None:
    if not (_truthy(os.getenv("PREVIEW_ENABLED")) and _truthy(os.getenv("ALPHA_PREVIEW_PROPOSALS_ENABLED"))):
        info("alpha_command_previews_mirror: disabled (set PREVIEW_ENABLED=1 and ALPHA_PREVIEW_PROPOSALS_ENABLED=1).")
        return
    if not _truthy(os.getenv("ALPHA_COMMAND_PREVIEWS_MIRROR_ENABLED", "1")):
        info("alpha_command_previews_mirror: mirror disabled by env; skipping.")
        return

    conn = _connect()
    if conn is None:
        return

    tab = _tab_name()

    header = [
        "ts",
        "proposal_id",
        "proposal_hash",
        "decision",
        "actor",
        "note",
        "token",
        "venue",
        "symbol",
        "action",
        "notional_usd",
        "confidence",
        "primary_blocker",
        "intent_type",
        "intent_json",
    ]

    try:
        cur = conn.cursor()

        cur.execute(
            """
            WITH latest AS (
              SELECT DISTINCT ON (proposal_id)
                proposal_id,
                ts AS approval_ts,
                decision,
                actor,
                note
              FROM alpha_approvals
              ORDER BY proposal_id, ts DESC
            )
            SELECT
              p.ts,
              p.proposal_id::text,
              p.proposal_hash,
              l.decision,
              COALESCE(l.actor,'') AS actor,
              COALESCE(l.note,'') AS note,
              p.token,
              p.venue,
              p.symbol,
              p.action,
              p.notional_usd,
              p.confidence,
              COALESCE((p.gates->>'primary_blocker'),'') AS primary_blocker,
              COALESCE(p.payload,'{}'::jsonb) AS payload
            FROM alpha_proposals p
            JOIN latest l ON l.proposal_id = p.proposal_id
            WHERE l.decision = 'APPROVE'
            ORDER BY p.ts DESC
            LIMIT %s
            """,
            (int(limit),),
        )

        rows = cur.fetchall() or []
        values: List[List[Any]] = [header]

        for r in rows:
            ts = str(r[0])
            proposal_id = r[1] or ""
            proposal_hash = r[2] or ""
            decision = r[3] or ""
            actor = r[4] or ""
            note = r[5] or ""
            token = (r[6] or "").upper()
            venue = (r[7] or "").upper()
            symbol = r[8] or ""
            action = (r[9] or "").upper()
            notional = float(r[10] or 0)
            conf = float(r[11] or 0)
            primary_blocker = r[12] or ""
            payload = r[13] if isinstance(r[13], dict) else {}

            meta = {
                "phase": "26A",
                "preview": True,
                "proposal_id": proposal_id,
                "proposal_hash": proposal_hash,
                "token": token,
                "action": action,
                "confidence": conf,
                "approval_note": note,
                "payload": payload,
            }

            intent = _build_intent(action=action, venue=venue, symbol=symbol, notional_usd=notional, meta=meta)
            intent_type = intent.get("type") or ""

            values.append(
                [
                    ts,
                    proposal_id,
                    proposal_hash,
                    decision,
                    actor,
                    note,
                    token,
                    venue,
                    symbol,
                    action,
                    notional,
                    conf,
                    primary_blocker,
                    intent_type,
                    _safe_json(intent),
                ]
            )

        sheet = get_sheet()
        try:
            ws = sheet.worksheet(tab)
        except Exception:
            warn(f"alpha_command_previews_mirror: sheet tab '{tab}' missing; creating.")
            ws = sheet.add_worksheet(title=tab, rows="500", cols="20")

        ws.clear()
        ws.update("A1", values)

        info(f"alpha_command_previews_mirror: mirrored {max(len(values)-1,0)} rows to sheet tab '{tab}'.")
    except Exception as e:
        error(f"alpha_command_previews_mirror failed: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":  # pragma: no cover
    run_alpha_command_previews_mirror()
