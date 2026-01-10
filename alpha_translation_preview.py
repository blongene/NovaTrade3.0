#!/usr/bin/env python3
# alpha_translation_preview.py — Phase 26C (Translation Preview)
# Reads APPROVED alpha proposals and produces append-only "command-like" preview artifacts.
# IMPORTANT: This does NOT enqueue commands and DOES NOT trade.

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from utils import get_ws_cached, write_rows_to_sheet, warn, info
from db_backbone import _get_conn  # project-standard PG connector

TAB_TRANSLATIONS = os.getenv("ALPHA_TRANSLATIONS_SHEET_TAB", "Alpha_Translations")
TAB_APPROVALS = os.getenv("ALPHA_APPROVALS_SHEET_TAB", "Alpha_Approvals")

PREVIEW_ENABLED = os.getenv("PREVIEW_ENABLED", "0").strip().lower() in ("1", "true", "yes")
TRANSLATION_ENABLED = os.getenv("ALPHA_TRANSLATION_PREVIEW_ENABLED", "1").strip().lower() in ("1", "true", "yes")

DEFAULT_TRADE_NOTIONAL_USD = float(os.getenv("DEFAULT_TRADE_NOTIONAL_USD", "25"))
DEFAULT_AGENT_ID = os.getenv("AGENT_ID", "edge-primary")

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), sort_keys=True)
    except Exception:
        try:
            return json.dumps(str(obj))
        except Exception:
            return "{}"

def _command_preview_from(proposal: Dict[str, Any], approval: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert an APPROVED alpha proposal into a command-like preview.
    This is intentionally non-executable.
    """
    action = (proposal.get("action") or "").upper().strip()
    venue = proposal.get("venue") or ""
    symbol = proposal.get("symbol") or ""
    token = proposal.get("token") or ""
    confidence = float(proposal.get("confidence") or 0)
    notional = float(proposal.get("notional_usd") or 0) or DEFAULT_TRADE_NOTIONAL_USD

    # Map proposal action to a preview command type
    if action == "WOULD_TRADE":
        cmd_type = "TRADE_INTENT_PREVIEW"
        side = "BUY"
        cmd = {
            "type": cmd_type,
            "venue": venue,
            "symbol": symbol,
            "side": side,
            "notional_usd": notional,
        }
    elif action == "WOULD_WATCH":
        cmd_type = "WATCH_INTENT_PREVIEW"
        cmd = {
            "type": cmd_type,
            "venue": venue,
            "symbol": symbol,
            "token": token,
        }
    else:
        cmd_type = "NOOP_PREVIEW"
        cmd = {"type": cmd_type, "reason": f"action={action or 'UNKNOWN'}"}

    preview = {
        "schema": "Alpha_Translation.v1",
        "ts": _utc_now_iso(),
        "mode": "translation_preview_only",
        "execution_allowed": 0,
        "blocked_by": ["translation_preview_only"],
        "source": {
            "proposal_id": proposal.get("proposal_id"),
            "proposal_hash": proposal.get("proposal_hash"),
            "proposal_ts": proposal.get("ts"),
            "token": token,
        },
        "approval": {
            "decision": approval.get("decision"),
            "actor": approval.get("actor"),
            "note": approval.get("note"),
            "ts": approval.get("ts"),
        },
        "command": cmd,
        "confidence": confidence,
        "rationale": proposal.get("rationale") or "",
        "gates": proposal.get("gates") or {},
    }
    return preview

def _fetch_approved_latest_proposals(cur) -> List[Dict[str, Any]]:
    """
    Fetch latest proposal per (token, venue, symbol, action) in last 72h that is currently APPROVED.
    We join on proposal_id (explicit) via alpha_approvals_latest_v.
    """
    cur.execute(
        """
        SELECT
          p.proposal_id, p.ts, p.agent_id, p.token, p.venue, p.symbol,
          p.action, p.notional_usd, p.confidence, p.rationale,
          p.gates, p.payload, p.proposal_hash,
          a.decision AS approval_decision, a.actor AS approval_actor, a.note AS approval_note, a.ts AS approval_ts
        FROM alpha_proposals p
        JOIN alpha_approvals_latest_v a
          ON a.proposal_id = p.proposal_id
        WHERE a.decision = 'APPROVE'
          AND p.ts >= (now() - interval '72 hours')
        ORDER BY p.ts DESC
        """
    )
    rows = cur.fetchall() or []
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "proposal_id": str(r[0]),
            "ts": r[1].isoformat() if r[1] else None,
            "agent_id": r[2],
            "token": r[3],
            "venue": r[4],
            "symbol": r[5],
            "action": r[6],
            "notional_usd": float(r[7] or 0),
            "confidence": float(r[8] or 0),
            "rationale": r[9],
            "gates": r[10] if isinstance(r[10], dict) else (r[10] or {}),
            "payload": r[11] if isinstance(r[11], dict) else (r[11] or {}),
            "proposal_hash": r[12] or "",
            "approval": {
                "decision": r[13],
                "actor": r[14],
                "note": r[15],
                "ts": r[16].isoformat() if r[16] else None,
            }
        })
    return out

def _insert_translation(cur, proposal: Dict[str, Any]) -> int:
    approval = proposal.get("approval") or {}
    preview = _command_preview_from(proposal, approval)

    # Stable idempotency hash
    hash_input = {
        "proposal_id": proposal.get("proposal_id"),
        "proposal_hash": proposal.get("proposal_hash"),
        "approval_decision": approval.get("decision"),
        "approval_actor": approval.get("actor"),
        "action": proposal.get("action"),
        "venue": proposal.get("venue"),
        "symbol": proposal.get("symbol"),
        # include day to allow new translation on new day even if same proposal_id changes in future
        "utc_day": (proposal.get("ts") or "")[:10],
        "command_type": preview.get("command", {}).get("type"),
    }
    row_hash = _sha256(_safe_json(hash_input))

    cur.execute(
        """
        INSERT INTO alpha_translations (
          proposal_id, proposal_hash,
          approval_decision, approval_actor, approval_note,
          agent_id, token, venue, symbol, action,
          notional_usd, confidence, rationale,
          gates, payload, command_preview,
          row_hash
        )
        VALUES (
          %(proposal_id)s, %(proposal_hash)s,
          %(approval_decision)s, %(approval_actor)s, %(approval_note)s,
          %(agent_id)s, %(token)s, %(venue)s, %(symbol)s, %(action)s,
          %(notional_usd)s, %(confidence)s, %(rationale)s,
          %(gates)s::jsonb, %(payload)s::jsonb, %(command_preview)s::jsonb,
          %(row_hash)s
        )
        ON CONFLICT (row_hash) DO NOTHING
        """,
        {
            "proposal_id": proposal.get("proposal_id"),
            "proposal_hash": proposal.get("proposal_hash") or "",
            "approval_decision": approval.get("decision") or "",
            "approval_actor": approval.get("actor") or "",
            "approval_note": approval.get("note") or "",
            "agent_id": proposal.get("agent_id") or DEFAULT_AGENT_ID,
            "token": proposal.get("token") or "",
            "venue": proposal.get("venue") or "",
            "symbol": proposal.get("symbol") or "",
            "action": proposal.get("action") or "",
            "notional_usd": float(proposal.get("notional_usd") or 0),
            "confidence": float(proposal.get("confidence") or 0),
            "rationale": proposal.get("rationale") or "",
            "gates": json.dumps(proposal.get("gates") or {}),
            "payload": json.dumps(proposal.get("payload") or {}),
            "command_preview": json.dumps(preview),
            "row_hash": row_hash,
        }
    )
    return cur.rowcount or 0

def _mirror_latest_translations(conn) -> int:
    """
    Mirror latest translations to Alpha_Translations sheet.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          ts, proposal_id, approval_decision, approval_actor, approval_note,
          token, venue, symbol, action, confidence, notional_usd,
          command_preview, row_hash
        FROM alpha_translations
        ORDER BY ts DESC
        LIMIT 200
        """
    )
    rows = cur.fetchall() or []

    header = [
        "ts","proposal_id","approval_decision","approval_actor","approval_note",
        "token","venue","symbol","action","confidence","notional_usd",
        "command_preview_json","row_hash"
    ]

    values = [header]
    for r in rows:
        values.append([
            str(r[0]), str(r[1]), str(r[2]), str(r[3]), str(r[4]),
            str(r[5]), str(r[6]), str(r[7]), str(r[8]),
            float(r[9] or 0), float(r[10] or 0),
            _safe_json(r[11]), str(r[12]),
        ])

    # write_rows_to_sheet expects rows as list[list]
    write_rows_to_sheet(TAB_TRANSLATIONS, values, clear=True)
    return len(rows)

def run_alpha_translation_preview() -> Tuple[int, int, str]:
    """
    Main entrypoint for scheduling.

    Returns:
      (processed, inserted_new, status)
    """
    if not (PREVIEW_ENABLED and TRANSLATION_ENABLED):
        msg = "skipped (set PREVIEW_ENABLED=1 and ALPHA_TRANSLATION_PREVIEW_ENABLED=1)"
        info(f"alpha_translation_preview: {msg}")
        return 0, 0, msg

    conn = _get_conn()
    if conn is None:
        warn("alpha_translation_preview: no DB connection (DB_URL/psycopg2 missing?)")
        return 0, 0, "no_db"

    inserted = 0
    processed = 0
    try:
        cur = conn.cursor()
        proposals = _fetch_approved_latest_proposals(cur)
        processed = len(proposals)

        for p in proposals:
            inserted += _insert_translation(cur, p)

        conn.commit()

        mirrored = _mirror_latest_translations(conn)

        info(f"alpha_translation_preview: processed={processed} inserted_new={inserted} mirrored={mirrored} (tab='{TAB_TRANSLATIONS}')")
        return processed, inserted, "ok"
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        warn(f"alpha_translation_preview failed: {e}")
        return processed, inserted, f"error:{e}"

if __name__ == "__main__":
    run_alpha_translation_preview()
