import json
import os
from typing import Any, Dict, Optional, Tuple

import psycopg2
import psycopg2.extras

from alpha_command_outbox import enqueue_command

def _db_url() -> str:
    u = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not u:
        raise RuntimeError("DB_URL (or DATABASE_URL) is required")
    return u

def _phase26_cfg() -> Dict[str, Any]:
    raw = os.getenv("PHASE26_DELTA_JSON", "") or "{}"
    try:
        return json.loads(raw)
    except Exception:
        return {}

def _enabled() -> bool:
    cfg = _phase26_cfg().get("phase26", {}) or {}
    if int(cfg.get("enabled", 0)) != 1:
        return False
    mode = str(cfg.get("mode", "")).strip().lower()
    if mode in ("dryrun_exec", "26e", "exec", "execution"):
        return True
    alpha = cfg.get("alpha", {}) or {}
    return int(alpha.get("execution_enabled", 0)) == 1 and int(alpha.get("allow_dryrun", 1)) == 1

def _conn():
    return psycopg2.connect(_db_url(), sslmode="require")

def _latest_translation(cur, proposal_id: str) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT ts, translation_id, proposal_id, venue, symbol, action, payload
        FROM alpha_translations
        WHERE proposal_id=%s
        ORDER BY ts DESC
        LIMIT 1
        """,
        (proposal_id,),
    )
    r = cur.fetchone()
    if not r:
        return None
    return {
        "ts": r[0].isoformat() if r[0] else None,
        "translation_id": str(r[1]),
        "proposal_id": str(r[2]),
        "venue": r[3] or "",
        "symbol": r[4] or "",
        "action": r[5] or "",
        "payload": r[6] if isinstance(r[6], dict) else (r[6] or {}),
    }

def _already_enqueued(cur, translation_id: str) -> bool:
    cur.execute(
        "SELECT 1 FROM alpha_dryrun_orderplace_outbox WHERE translation_id=%s LIMIT 1;",
        (translation_id,),
    )
    return cur.fetchone() is not None

def _record_outbox(cur, *, translation: Dict[str, Any], cmd_id: int, intent_hash: str, intent: Dict[str, Any], note: str) -> None:
    cur.execute(
        """
        INSERT INTO alpha_dryrun_orderplace_outbox
          (translation_id, proposal_id, token, venue, symbol, side, cmd_id, intent_hash, intent, note)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (translation_id) DO NOTHING
        """,
        (
            translation["translation_id"],
            translation["proposal_id"],
            intent.get("payload", {}).get("token", "") or "",
            intent.get("venue", "") or translation.get("venue", "") or "",
            intent.get("symbol", "") or translation.get("symbol", "") or "",
            intent.get("payload", {}).get("side", "") or "",
            int(cmd_id),
            intent_hash,
            psycopg2.extras.Json(intent),
            note or "",
        ),
    )

def enqueue_from_approvals(limit: int = 25) -> Tuple[int, int]:
    """
    Process up to `limit` recent APPROVE rows in alpha_approvals.
    For each: enqueue a DRYRUN intent to canonical `commands` outbox.
    Returns (processed, enqueued_new).
    """
    if not _enabled():
        return (0, 0)

    cfg = _phase26_cfg().get("phase26", {}) or {}
    alpha = cfg.get("alpha", {}) or {}
    require_human = int(alpha.get("require_human_approval", 1)) == 1

    conn = _conn()
    processed = 0
    enq = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts, proposal_id, decision, actor, note
                FROM alpha_approvals
                WHERE UPPER(decision)='APPROVE'
                ORDER BY ts DESC
                LIMIT %s
                """,
                (limit,),
            )
            approvals = cur.fetchall()

            for (ts, proposal_id, decision, actor, note) in approvals:
                processed += 1
                if require_human and (actor or "").lower() not in ("human", "brett"):
                    continue

                tr = _latest_translation(cur, str(proposal_id))
                if not tr:
                    continue
                if _already_enqueued(cur, tr["translation_id"]):
                    continue

                meta = tr.get("payload", {}) if isinstance(tr.get("payload", {}), dict) else {}
                token = meta.get("token", "") or ""
                venue = tr.get("venue", "") or meta.get("venue", "")
                symbol = tr.get("symbol", "") or meta.get("symbol", "")

                intent = {
                    "type": "note",
                    "venue": venue,
                    "symbol": symbol,
                    "payload": {
                        "dry_run": True,
                        "mode": "dryrun",
                        "token": token,
                        "venue": venue,
                        "symbol": symbol,
                        "action": tr.get("action", ""),
                        "translation_id": tr["translation_id"],
                        "proposal_id": tr["proposal_id"],
                        "phase": "26E-dryrun-exec",
                    },
                }

                idem = f"alpha26e:{tr['translation_id']}"
                cmd_id = enqueue_command(
                    command_type="ALPHA26E_DRYRUN",
                    payload=intent,
                    idempotency_key=idem,
                    note=f"Alpha26E dryrun from approval proposal_id={tr['proposal_id']}",
                    source="alpha26e",
                    status="queued",
                )

                _record_outbox(
                    cur,
                    translation=tr,
                    cmd_id=cmd_id,
                    intent_hash=idem,
                    intent=intent,
                    note=str(note or ""),
                )
                enq += 1

        conn.commit()
        return (processed, enq)
    finally:
        conn.close()
