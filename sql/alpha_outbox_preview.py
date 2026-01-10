#!/usr/bin/env python3
"""
alpha_outbox_preview.py — Phase 26D-preview (safe enqueue)

Enqueues DRYRUN commands into the Bus outbox from APPROVED alpha translations.

Safety:
- Requires PREVIEW_ENABLED=1
- Requires ALPHA_EXECUTION_PREVIEW_ENABLED=1 (default on)
- Always sets mode="dryrun" and dry_run=true

Idempotency:
- Uses translation row_hash as stable idempotency_key
- Records alpha_command_previews(row_hash -> outbox_cmd_id) to prevent re-enqueue

Sheets:
- Optionally mirrors to Alpha_CommandPreviews tab (create tab first to avoid warnings)
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple

from bus_store_pg import get_store, _intent_hash
from db_backbone import _get_conn
from utils import info, warn, write_rows_to_sheet

TAB_PREVIEWS = os.getenv("ALPHA_COMMAND_PREVIEWS_SHEET_TAB", "Alpha_CommandPreviews")

PREVIEW_ENABLED = os.getenv("PREVIEW_ENABLED", "0").strip().lower() in ("1", "true", "yes")
EXEC_PREVIEW_ENABLED = os.getenv("ALPHA_EXECUTION_PREVIEW_ENABLED", "1").strip().lower() in ("1", "true", "yes")

AGENT_ID = os.getenv("AGENT_ID", "edge-primary")


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), sort_keys=True, default=str)
    except Exception:
        return "{}"


def _fetch_latest_approved_translations(cur, limit: int = 50) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
          t.translation_id, t.ts, t.proposal_id, t.proposal_hash,
          t.approval_decision, t.approval_actor, t.approval_note,
          t.agent_id, t.token, t.venue, t.symbol, t.action,
          t.notional_usd, t.confidence, t.rationale,
          t.gates, t.payload, t.command_preview,
          t.row_hash
        FROM alpha_translations_latest_v t
        WHERE t.approval_decision = 'APPROVE'
        ORDER BY t.ts DESC
        LIMIT %s
        """,
        (int(limit),),
    )
    rows = cur.fetchall() or []
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "translation_id": str(r[0]),
                "ts": r[1].isoformat() if r[1] else None,
                "proposal_id": str(r[2]),
                "proposal_hash": r[3] or "",
                "approval_decision": r[4] or "",
                "approval_actor": r[5] or "",
                "approval_note": r[6] or "",
                "agent_id": r[7] or AGENT_ID,
                "token": r[8] or "",
                "venue": r[9] or "",
                "symbol": r[10] or "",
                "action": (r[11] or "").upper(),
                "notional_usd": float(r[12] or 0),
                "confidence": float(r[13] or 0),
                "rationale": r[14] or "",
                "gates": r[15] if isinstance(r[15], dict) else (r[15] or {}),
                "payload": r[16] if isinstance(r[16], dict) else (r[16] or {}),
                "command_preview": r[17] if isinstance(r[17], dict) else (r[17] or {}),
                "row_hash": r[18] or "",
            }
        )
    return out


def _already_enqueued(cur, row_hash: str) -> bool:
    cur.execute("SELECT 1 FROM alpha_command_previews WHERE row_hash=%s LIMIT 1", (row_hash,))
    return cur.fetchone() is not None


def _build_outbox_intent(t: Dict[str, Any]) -> Dict[str, Any]:
    action = (t.get("action") or "").upper()
    venue = (t.get("venue") or "").upper()
    symbol = t.get("symbol") or ""
    token = t.get("token") or ""
    notional = float(t.get("notional_usd") or 0)
    confidence = float(t.get("confidence") or 0)

    # WOULD_WATCH becomes a harmless dryrun “note-shaped” command (still ACKs cleanly).
    side = "BUY" if action == "WOULD_TRADE" else "HOLD"

    payload = {
        "venue": venue,
        "symbol": symbol,
        "side": side,
        "amount_usd": notional if action == "WOULD_TRADE" else 0,
        "dry_run": True,
        "mode": "dryrun",
        "idempotency_key": f"alpha26d_preview:{t.get('row_hash')}",
        "note": f"Alpha26D-preview from translation {t.get('translation_id')} (proposal {t.get('proposal_id')})",
        "meta": {
            "phase": "26D-preview",
            "translation_id": t.get("translation_id"),
            "proposal_id": t.get("proposal_id"),
            "proposal_hash": t.get("proposal_hash"),
            "token": token,
            "action": action,
            "confidence": confidence,
            "gates": t.get("gates") or {},
            "rationale": t.get("rationale") or "",
        },
    }

    # Edge understands "type":"order.place" with payload.
    return {"type": "order.place", "payload": payload}


def _record_preview(cur, t: Dict[str, Any], outbox_cmd_id: int, intent: Dict[str, Any]) -> int:
    ih = _intent_hash(intent)
    cur.execute(
        """
        INSERT INTO alpha_command_previews(
          translation_id, proposal_id,
          token, venue, symbol, action,
          row_hash, outbox_cmd_id, intent_hash,
          intent, note
        )
        VALUES(
          %(translation_id)s, %(proposal_id)s,
          %(token)s, %(venue)s, %(symbol)s, %(action)s,
          %(row_hash)s, %(outbox_cmd_id)s, %(intent_hash)s,
          %(intent)s::jsonb, %(note)s
        )
        ON CONFLICT (row_hash) DO NOTHING
        """,
        {
            "translation_id": t.get("translation_id"),
            "proposal_id": t.get("proposal_id"),
            "token": t.get("token") or "",
            "venue": t.get("venue") or "",
            "symbol": t.get("symbol") or "",
            "action": t.get("action") or "",
            "row_hash": t.get("row_hash") or "",
            "outbox_cmd_id": int(outbox_cmd_id),
            "intent_hash": ih,
            "intent": json.dumps(intent, separators=(",", ":"), sort_keys=True),
            "note": f"enqueued dryrun preview cmd_id={outbox_cmd_id}",
        },
    )
    return cur.rowcount or 0


def _mirror_sheet(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          ts, translation_id, proposal_id,
          token, venue, symbol, action,
          outbox_cmd_id, intent_hash, note,
          intent, row_hash
        FROM alpha_command_previews
        ORDER BY ts DESC
        LIMIT 200
        """
    )
    rows = cur.fetchall() or []

    header = [
        "ts","translation_id","proposal_id",
        "token","venue","symbol","action",
        "outbox_cmd_id","intent_hash","note",
        "intent_json","row_hash"
    ]
    values = [header]
    for r in rows:
        values.append(
            [
                str(r[0]), str(r[1]), str(r[2]),
                str(r[3]), str(r[4]), str(r[5]), str(r[6]),
                int(r[7]), str(r[8]), str(r[9]),
                _safe_json(r[10]), str(r[11]),
            ]
        )

    write_rows_to_sheet(TAB_PREVIEWS, values, clear=True)
    return len(rows)


def run_alpha_outbox_preview(limit: int = 50) -> Tuple[int, int, str]:
    if not (PREVIEW_ENABLED and EXEC_PREVIEW_ENABLED):
        msg = "skipped (set PREVIEW_ENABLED=1 and ALPHA_EXECUTION_PREVIEW_ENABLED=1)"
        info(f"alpha_outbox_preview: {msg}")
        return 0, 0, msg

    conn = _get_conn()
    if conn is None:
        warn("alpha_outbox_preview: no DB connection (DB_URL/psycopg2 missing?)")
        return 0, 0, "no_db"

    store = get_store()

    processed = 0
    enq = 0
    try:
        cur = conn.cursor()
        translations = _fetch_latest_approved_translations(cur, limit=limit)
        processed = len(translations)

        for t in translations:
            rh = t.get("row_hash") or ""
            if not rh:
                continue
            if _already_enqueued(cur, rh):
                continue

            intent = _build_outbox_intent(t)
            res = store.enqueue(agent_id=AGENT_ID, intent=intent, dedup_ttl_seconds=3600)
            cmd_id = int(res.get("id") or 0)
            if cmd_id <= 0:
                continue

            enq += _record_preview(cur, t, cmd_id, intent)

        try:
            conn.commit()
        except Exception:
            pass

        try:
            mirrored = _mirror_sheet(conn)
        except Exception as e:
            mirrored = 0
            warn(f"alpha_outbox_preview: sheet mirror failed: {e}")

        info(f"alpha_outbox_preview: processed={processed} enqueued_new={enq} mirrored={mirrored} (tab='{TAB_PREVIEWS}')")
        return processed, enq, "ok"

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        warn(f"alpha_outbox_preview failed: {e}")
        return processed, enq, f"error:{e}"


if __name__ == "__main__":
    run_alpha_outbox_preview()
