#!/usr/bin/env python3
"""Phase 26E — enqueue dry-run BUY/SELL order.place intents.

Reads Alpha translation previews and, when approved, enqueues a dry_run
order.place intent into the Postgres Command Bus.

Safety / intent semantics
- Always sets payload.dry_run = True
- BUY uses payload.amount_usd (quote sizing)
- SELL uses payload.amount_base (fixed base sizing)

Config
- ALPHA26E_AGENT_ID (default: edge-primary)
- ALPHA26E_BUY_USD (default: 10)
- ALPHA26E_SELL_BASE_DEFAULT (default: 1)
- ALPHA26E_SELL_BASE_MAP (optional JSON, e.g. {"BTC":0.00005,"ETH":0.001})
- ALPHA26E_SIDE_DEFAULT (default: BUY)
- ALPHA26E_LIMIT (default: 10)

Dryrun order.place intent envelope (matches Edge expectations):
{
  "type": "order.place",
  "venue": "COINBASE",
  "symbol": "XYZ/USDC",
  "payload": {
     "venue": "COINBASE",
     "symbol": "XYZ/USDC",
     "side": "BUY"|"SELL",
     "amount_usd": 10,
     "amount_base": 0,
     "dry_run": true,
     "idempotency_key": "...",
     "meta": {...}
  }
}
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

from bus_store_pg import get_store
from utils import log as _log


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def _env_json(name: str) -> Optional[dict]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        v = json.loads(raw)
        return v if isinstance(v, dict) else None
    except Exception:
        return None


@dataclass
class TranslationRow:
    ts: datetime
    translation_id: str
    proposal_id: str
    token: str
    venue: str
    symbol: str
    action: str
    confidence: float
    approval_decision: str
    approval_actor: str
    approval_note: str
    gates: Dict[str, Any]
    rationale: str
    proposal_hash: str


def _stable_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def _idempotency_key(prefix: str, row_hash: str) -> str:
    return f"{prefix}:{row_hash}"


def _sell_amount_base(token: str) -> float:
    token_u = (token or "").upper().strip()
    m = _env_json("ALPHA26E_SELL_BASE_MAP") or {}
    if token_u in m:
        try:
            return float(m[token_u])
        except Exception:
            pass
    # sensible defaults for big coins, otherwise 1.0 base
    if token_u in {"BTC", "XBT"}:
        return 0.00005
    if token_u == "ETH":
        return 0.001
    return _env_float("ALPHA26E_SELL_BASE_DEFAULT", 1.0)


def _choose_side(action: str) -> str:
    # If upstream ever emits WOULD_BUY / WOULD_SELL we respect it.
    a = (action or "").upper().strip()
    if "SELL" in a:
        return "SELL"
    if "BUY" in a:
        return "BUY"
    return (os.getenv("ALPHA26E_SIDE_DEFAULT", "BUY").strip().upper() or "BUY")


def _connect_db():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("DB_URL is required")
    return psycopg2.connect(db_url)


def _fetch_candidate_translations(limit: int) -> List[TranslationRow]:
    # We rely on the Phase 26C view (alpha_translations_latest_v) for the latest translation per proposal.
    # It should include the approval fields via joins, but we defensively compute what we need.
    sql = """
    WITH latest AS (
      SELECT
        t.translation_id,
        t.ts,
        t.proposal_id,
        t.token,
        t.venue,
        t.symbol,
        t.action,
        t.confidence,
        COALESCE(t.gates, '{}'::jsonb) AS gates,
        COALESCE(t.rationale, '') AS rationale,
        COALESCE(t.proposal_hash, '') AS proposal_hash,
        COALESCE(t.approval_decision, '') AS approval_decision,
        COALESCE(t.approval_actor, '') AS approval_actor,
        COALESCE(t.approval_note, '') AS approval_note
      FROM alpha_translations_latest_v t
    )
    SELECT *
    FROM latest
    WHERE approval_decision ILIKE 'APPROVE'
    ORDER BY ts DESC
    LIMIT %s;
    """
    rows: List[TranslationRow] = []
    with _connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            for (
                translation_id,
                ts,
                proposal_id,
                token,
                venue,
                symbol,
                action,
                confidence,
                gates,
                rationale,
                proposal_hash,
                approval_decision,
                approval_actor,
                approval_note,
            ) in cur.fetchall():
                # psycopg2 may give tz-aware timestamps; normalize
                if isinstance(ts, datetime) and ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                rows.append(
                    TranslationRow(
                        ts=ts,
                        translation_id=str(translation_id),
                        proposal_id=str(proposal_id),
                        token=str(token),
                        venue=str(venue),
                        symbol=str(symbol),
                        action=str(action),
                        confidence=float(confidence or 0),
                        approval_decision=str(approval_decision),
                        approval_actor=str(approval_actor),
                        approval_note=str(approval_note),
                        gates=dict(gates or {}),
                        rationale=str(rationale),
                        proposal_hash=str(proposal_hash),
                    )
                )
    return rows


def _already_enqueued(row_hash: str) -> bool:
    # If we have already mirrored/enqueued this row_hash, don't enqueue again.
    sql = "SELECT 1 FROM alpha_command_previews WHERE row_hash = %s LIMIT 1;"
    with _connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (row_hash,))
            return cur.fetchone() is not None


def _build_intent(row: TranslationRow, *, side: str, buy_usd: float, sell_base: float, id_prefix: str) -> Dict[str, Any]:
    venue_u = (row.venue or "").upper().strip()
    symbol = row.symbol.strip()
    row_hash = hashlib.sha256(
        f"{row.translation_id}|{venue_u}|{symbol}|{side}|{row.proposal_id}".encode("utf-8")
    ).hexdigest()

    payload: Dict[str, Any] = {
        "venue": venue_u,
        "symbol": symbol,
        "side": side,
        "dry_run": True,
        "idempotency_key": _idempotency_key(id_prefix, row_hash),
        # Sizing:
        "amount_usd": float(buy_usd) if side == "BUY" else 0,
        "amount_base": float(sell_base) if side == "SELL" else 0,
        "meta": {
            "phase": "26E-dryrun-order.place",
            "translation_id": row.translation_id,
            "proposal_id": row.proposal_id,
            "proposal_hash": row.proposal_hash,
            "token": row.token,
            "action": row.action,
            "confidence": row.confidence,
            "gates": row.gates,
            "rationale": row.rationale,
            "approval": {
                "decision": row.approval_decision,
                "actor": row.approval_actor,
                "note": row.approval_note,
                "ts": row.ts.isoformat(),
            },
        },
    }

    intent: Dict[str, Any] = {
        "type": "order.place",
        "venue": venue_u,
        "symbol": symbol,
        "payload": payload,
    }
    return {"row_hash": row_hash, "intent": intent}


def run_alpha_outbox_orderplace_dryrun() -> Tuple[int, str]:
    agent_id = os.getenv("ALPHA26E_AGENT_ID", "edge-primary").strip() or "edge-primary"
    limit = _env_int("ALPHA26E_LIMIT", 10)
    buy_usd = _env_float("ALPHA26E_BUY_USD", 10.0)
    id_prefix = os.getenv("ALPHA26E_IDEM_PREFIX", "alpha26e_dryrun").strip() or "alpha26e_dryrun"

    processed = 0
    enqueued_new = 0

    store = get_store()

    candidates = _fetch_candidate_translations(limit)
    for row in candidates:
        processed += 1

        side = _choose_side(row.action)
        sell_base = _sell_amount_base(row.token)

        built = _build_intent(row, side=side, buy_usd=buy_usd, sell_base=sell_base, id_prefix=id_prefix)
        row_hash = built["row_hash"]
        intent = built["intent"]

        # De-dupe:
        try:
            if _already_enqueued(row_hash):
                continue
        except Exception:
            # If alpha_command_previews isn't there yet, don't block enqueue.
            pass

        # Enqueue to Postgres command bus.
        cmd_id = store.enqueue(agent_id=agent_id, intent=intent)
        enqueued_new += 1

        _log(
            "INFO",
            f"alpha_outbox_orderplace_dryrun: enqueued cmd_id={cmd_id} agent_id={agent_id} {row.token} {row.venue} {row.symbol} side={side} dry_run=1",
        )

    return enqueued_new, "ok"


if __name__ == "__main__":
    t0 = time.time()
    try:
        n, msg = run_alpha_outbox_orderplace_dryrun()
        _log("INFO", f"alpha_outbox_orderplace_dryrun: enqueued_new={n} ({msg}) elapsed={time.time()-t0:.2f}s")
    except Exception as e:
        _log("ERROR", f"alpha_outbox_orderplace_dryrun failed: {e}")
        raise
