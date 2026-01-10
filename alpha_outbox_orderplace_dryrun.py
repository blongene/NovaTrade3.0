#!/usr/bin/env python3
"""
alpha_outbox_orderplace_dryrun.py — Phase 26E

Enqueue APPROVED alpha translations as dryrun order.place commands (BUY/SELL).

Key behaviors:
- Only enqueues when Gate A passes AND blockers are empty.
- Only for actions: WOULD_TRADE, WOULD_BUY, WOULD_SELL
- BUY uses amount_usd (quote sizing).
- SELL uses fixed amount_base (base sizing).
- Always dry_run=true + mode="dryrun"
- Adds venue/symbol at intent root (Edge expectation).
- Idempotent: one enqueue per translation_id (records to alpha_dryrun_orderplace_outbox).

Config (env):
- PREVIEW_ENABLED=1                (required)
- ALPHA_EXECUTION_PREVIEW_ENABLED=1 (required)
- ALPHA26E_BUY_USD_DEFAULT=10
- ALPHA26E_SELL_BASE_DEFAULT=1
- ALPHA26E_SELL_BASE_MAP='{"BTC":0.00005,"ETH":0.001}'   (optional)
- ALPHA26E_DEDUP_TTL_SECONDS=3600

Also supports ALPHA_CONFIG_JSON / ALPHA_CONFIG_PATH via alpha_config.py.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple

from bus_store_pg import get_store, _intent_hash
from db_backbone import _get_conn
from utils import info, warn, error

from alpha_config import get_alpha_config, cfg_get

AGENT_ID = os.getenv("AGENT_ID", "edge-primary")

PREVIEW_ENABLED = os.getenv("PREVIEW_ENABLED", "0").strip().lower() in ("1", "true", "yes")
EXEC_PREVIEW_ENABLED = os.getenv("ALPHA_EXECUTION_PREVIEW_ENABLED", "1").strip().lower() in ("1", "true", "yes")
DEDUP_TTL = int(os.getenv("ALPHA26E_DEDUP_TTL_SECONDS", "3600") or "3600")

DEFAULT_BUY_USD = float(os.getenv("ALPHA26E_BUY_USD_DEFAULT", "10") or "10")
DEFAULT_SELL_BASE = float(os.getenv("ALPHA26E_SELL_BASE_DEFAULT", "1") or "1")
SELL_BASE_MAP_RAW = os.getenv("ALPHA26E_SELL_BASE_MAP", "").strip()

def _load_sell_base_map() -> Dict[str, float]:
    if not SELL_BASE_MAP_RAW:
        return {}
    try:
        obj = json.loads(SELL_BASE_MAP_RAW)
        if not isinstance(obj, dict):
            return {}
        out: Dict[str, float] = {}
        for k, v in obj.items():
            try:
                out[str(k).upper()] = float(v)
            except Exception:
                continue
        return out
    except Exception:
        return {}

SELL_BASE_MAP = _load_sell_base_map()


def _fetch_latest_approved_translations(cur, limit: int = 50) -> List[Dict[str, Any]]:
    # Keep SQL conservative to avoid view drift; do JSON filtering in Python.
    cur.execute(
        """
        SELECT
          t.translation_id, t.ts, t.proposal_id,
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
        gates = r[14] if isinstance(r[14], dict) else (r[14] or {})
        payload = r[15] if isinstance(r[15], dict) else (r[15] or {})
        cmd_prev = r[16] if isinstance(r[16], dict) else (r[16] or {})
        out.append(
            {
                "translation_id": str(r[0]),
                "ts": r[1].isoformat() if r[1] else None,
                "proposal_id": str(r[2]),
                "approval_actor": r[4] or "",
                "approval_note": r[5] or "",
                "agent_id": r[6] or AGENT_ID,
                "token": (r[7] or "").upper(),
                "venue": (r[8] or "").upper(),
                "symbol": r[9] or "",
                "action": (r[10] or "").upper(),
                "notional_usd": float(r[11] or 0),
                "confidence": float(r[12] or 0),
                "rationale": r[13] or "",
                "gates": gates,
                "payload": payload,
                "command_preview": cmd_prev,
                "row_hash": r[17] or "",
            }
        )
    return out


def _gate_a_ok(gates: Dict[str, Any]) -> bool:
    # Accept both "A":1 and "A":true styles
    v = gates.get("A", 0)
    if isinstance(v, bool):
        return bool(v)
    try:
        return int(v) == 1
    except Exception:
        return False


def _blockers_empty(gates: Dict[str, Any]) -> bool:
    blockers = gates.get("blockers", [])
    if blockers is None:
        return True
    if isinstance(blockers, list):
        return len(blockers) == 0
    # sometimes stored as stringified list
    if isinstance(blockers, str):
        s = blockers.strip()
        if not s:
            return True
        # naive: treat any non-empty as blocked
        return False
    return False


def _already_enqueued(cur, translation_id: str) -> bool:
    cur.execute("SELECT 1 FROM alpha_dryrun_orderplace_outbox WHERE translation_id=%s LIMIT 1", (translation_id,))
    return cur.fetchone() is not None


def _build_intent(t: Dict[str, Any], buy_usd_default: float, sell_base_default: float) -> Dict[str, Any]:
    token = t.get("token") or ""
    venue = t.get("venue") or ""
    symbol = t.get("symbol") or ""
    action = t.get("action") or ""
    notional = float(t.get("notional_usd") or 0)
    confidence = float(t.get("confidence") or 0)

    # Map action -> side
    side = "SELL" if action == "WOULD_SELL" else "BUY"

    payload: Dict[str, Any] = {
        "venue": venue,
        "symbol": symbol,
        "side": side,
        "dry_run": True,
        "mode": "dryrun",
        "idempotency_key": f"alpha26e_dryrun:{t.get('translation_id')}",
        "note": f"Phase26E dryrun order.place ({side}) from translation {t.get('translation_id')}",
        "meta": {
            "phase": "26E",
            "translation_id": t.get("translation_id"),
            "proposal_id": t.get("proposal_id"),
            "token": token,
            "action": action,
            "confidence": confidence,
            "gates": t.get("gates") or {},
            "rationale": t.get("rationale") or "",
            "approval": {
                "actor": t.get("approval_actor") or "",
                "note": t.get("approval_note") or "",
            },
        },
    }

    if side == "BUY":
        amt = notional if notional > 0 else buy_usd_default
        payload["amount_usd"] = float(amt)
    else:
        base_amt = SELL_BASE_MAP.get(token.upper(), sell_base_default)
        payload["amount_base"] = float(base_amt)

    # IMPORTANT: venue/symbol at intent root for Edge
    return {
        "type": "order.place",
        "venue": venue,
        "symbol": symbol,
        "payload": payload,
    }


def _record(cur, t: Dict[str, Any], cmd_id: int, intent: Dict[str, Any]) -> int:
    ih = _intent_hash(intent)
    side = str(intent.get("payload", {}).get("side", ""))
    cur.execute(
        """
        INSERT INTO alpha_dryrun_orderplace_outbox(
          translation_id, proposal_id,
          token, venue, symbol, side,
          cmd_id, intent_hash, intent, note
        )
        VALUES(
          %(translation_id)s, %(proposal_id)s,
          %(token)s, %(venue)s, %(symbol)s, %(side)s,
          %(cmd_id)s, %(intent_hash)s, %(intent)s::jsonb, %(note)s
        )
        ON CONFLICT (translation_id) DO NOTHING
        """,
        {
            "translation_id": t.get("translation_id"),
            "proposal_id": t.get("proposal_id"),
            "token": t.get("token") or "",
            "venue": t.get("venue") or "",
            "symbol": t.get("symbol") or "",
            "side": side,
            "cmd_id": int(cmd_id),
            "intent_hash": ih,
            "intent": json.dumps(intent, separators=(",", ":"), sort_keys=True),
            "note": f"enqueued dryrun order.place cmd_id={cmd_id}",
        },
    )
    return cur.rowcount or 0


def run(limit: int = 50) -> Tuple[int, int, str]:
    if not (PREVIEW_ENABLED and EXEC_PREVIEW_ENABLED):
        msg = "skipped (set PREVIEW_ENABLED=1 and ALPHA_EXECUTION_PREVIEW_ENABLED=1)"
        info(f"alpha_outbox_orderplace_dryrun: {msg}")
        return 0, 0, msg

    conn = _get_conn()
    if conn is None:
        warn("alpha_outbox_orderplace_dryrun: no DB connection")
        return 0, 0, "no_db"

    cfg = get_alpha_config()
    buy_usd_default = float(cfg_get(cfg, "phase26.e.buy_usd_default", DEFAULT_BUY_USD))
    sell_base_default = float(cfg_get(cfg, "phase26.e.sell_base_default", DEFAULT_SELL_BASE))

    store = get_store()

    processed = 0
    enq = 0
    try:
        cur = conn.cursor()
        translations = _fetch_latest_approved_translations(cur, limit=limit)
        processed = len(translations)

        for t in translations:
            # eligibility
            gates = t.get("gates") or {}
            if not _gate_a_ok(gates):
                continue
            if not _blockers_empty(gates):
                continue

            action = (t.get("action") or "").upper()
            if action not in ("WOULD_TRADE", "WOULD_BUY", "WOULD_SELL"):
                continue

            if _already_enqueued(cur, t.get("translation_id") or ""):
                continue

            intent = _build_intent(t, buy_usd_default, sell_base_default)
            res = store.enqueue(agent_id=AGENT_ID, intent=intent, dedup_ttl_seconds=DEDUP_TTL)
            cmd_id = int(res.get("id") or 0)
            if cmd_id <= 0:
                continue

            enq += _record(cur, t, cmd_id, intent)

        conn.commit()
        info(f"alpha_outbox_orderplace_dryrun: processed={processed} enqueued_new={enq}")
        return processed, enq, "ok"

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        error(f"alpha_outbox_orderplace_dryrun failed: {e}")
        return processed, enq, f"error:{e}"


if __name__ == "__main__":
    run()
