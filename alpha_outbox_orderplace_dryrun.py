#!/usr/bin/env python3
"""
alpha_outbox_orderplace_dryrun.py — Phase 26E (BUS)

Enqueue APPROVED alpha translations as dryrun order.place commands (BUY/SELL),
while keeping the system close to full operation.

Key behaviors:
- Requires APPROVE.
- Only enqueues for actions: WOULD_BUY, WOULD_SELL
  (WOULD_WATCH should remain note/noop; not an order.place)
- Dryrun-only: dry_run=true, mode="dryrun"
- BUY uses amount_usd (quote sizing) and must be > 0
- SELL uses amount_base (base sizing) and must be > 0
- Intent root MUST include venue + symbol (Edge expectation)
- Idempotent: one enqueue per translation_id (alpha_dryrun_orderplace_outbox)

Config sources:
- ALPHA_CONFIG_JSON / ALPHA_CONFIG_PATH via alpha_config.get_alpha_config()
- Supported keys (matching your posted JSON):
    phase26.dryrun.enabled (bool)
    phase26.dryrun.allow_order_place (bool)
    phase26.dryrun.sell_base_amount (float)
    phase26.dryrun.buy_max_usd (float)
    gates.allow_immature_dryrun (bool)
    kill_switches.global (bool)
    kill_switches.edge_hold (bool)
    venues.coinbase.allow_dryrun (bool)   (optional; venue-specific gate)

Env overrides (optional):
- PREVIEW_ENABLED=1
- ALPHA_EXECUTION_PREVIEW_ENABLED=1
- ALPHA26E_IDEM_PREFIX=alpha26e_test_123   (changes idempotency key prefix)
- ALPHA26E_DEDUP_TTL_SECONDS=3600
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
IDEM_PREFIX = (os.getenv("ALPHA26E_IDEM_PREFIX", "") or "").strip()

DEFAULT_BUY_MAX_USD = float(os.getenv("ALPHA26E_BUY_MAX_USD_DEFAULT", "10") or "10")
DEFAULT_SELL_BASE_AMOUNT = float(os.getenv("ALPHA26E_SELL_BASE_AMOUNT_DEFAULT", "0.00005") or "0.00005")


def _fetch_latest_approved_translations(cur, limit: int = 50) -> List[Dict[str, Any]]:
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


def _already_enqueued(cur, translation_id: str) -> bool:
    cur.execute(
        "SELECT 1 FROM alpha_dryrun_orderplace_outbox WHERE translation_id=%s LIMIT 1",
        (translation_id,),
    )
    return cur.fetchone() is not None


def _boolish(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return int(v) != 0
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "y", "on")
    return False


def _allow_immature(gates: Dict[str, Any], allow_immature_dryrun: bool) -> bool:
    """
    If allow_immature_dryrun=true, then allow Gate A failure only when the blocker is IMMATURE.
    """
    if not allow_immature_dryrun:
        return False
    blockers = gates.get("blockers", [])
    primary = (gates.get("primary_blocker") or "").upper()

    if isinstance(blockers, str):
        # if stored as a string, be conservative: require it explicitly contains IMMATURE
        s = blockers.upper()
        return "IMMATURE" in s and ("[" not in s or "IMMATURE" == primary or primary == "")
    if isinstance(blockers, list):
        if len(blockers) == 0:
            return False
        upper = [str(b).upper() for b in blockers]
        # allow if ALL blockers are IMMATURE
        return all(b == "IMMATURE" for b in upper)
    # unknown type -> do not allow
    return False


def _gate_a_ok(gates: Dict[str, Any]) -> bool:
    v = gates.get("A", 0)
    if isinstance(v, bool):
        return bool(v)
    try:
        return int(v) == 1
    except Exception:
        return False


def _allowed_by_killswitch(cfg: Dict[str, Any]) -> bool:
    if _boolish(cfg_get(cfg, "kill_switches.global", False)):
        return False
    if _boolish(cfg_get(cfg, "kill_switches.edge_hold", False)):
        return False
    return True


def _venue_allows_dryrun(cfg: Dict[str, Any], venue: str) -> bool:
    # Optional; default allow.
    key = f"venues.{venue.lower()}.allow_dryrun"
    v = cfg_get(cfg, key, None)
    if v is None:
        return True
    return _boolish(v)


def _build_intent(
    *,
    t: Dict[str, Any],
    buy_max_usd: float,
    sell_base_amount: float,
) -> Dict[str, Any]:
    token = (t.get("token") or "").upper()
    venue = (t.get("venue") or "").upper()
    symbol = t.get("symbol") or ""
    action = (t.get("action") or "").upper()

    if action == "WOULD_SELL":
        side = "SELL"
    else:
        side = "BUY"

    # Stable, operator-visible idempotency key
    idem_key = f"{IDEM_PREFIX}:{t.get('translation_id')}" if IDEM_PREFIX else f"alpha26e_dryrun:{t.get('translation_id')}"

    payload: Dict[str, Any] = {
        "venue": venue,
        "symbol": symbol,
        "side": side,
        "dry_run": True,
        "mode": "dryrun",
        "idempotency_key": idem_key,
        "meta": {
            "phase": "26E",
            "translation_id": t.get("translation_id"),
            "proposal_id": t.get("proposal_id"),
            "token": token,
            "action": action,
            "confidence": float(t.get("confidence") or 0),
            "gates": t.get("gates") or {},
            "rationale": t.get("rationale") or "",
            "approval": {
                "actor": t.get("approval_actor") or "",
                "note": t.get("approval_note") or "",
            },
        },
        "note": f"Phase26E dryrun order.place ({side}) from translation {t.get('translation_id')}",
    }

    if side == "BUY":
        notional = float(t.get("notional_usd") or 0)
        amt_usd = notional if notional > 0 else float(buy_max_usd)
        # must be > 0 or Edge will reject
        if amt_usd <= 0:
            amt_usd = float(DEFAULT_BUY_MAX_USD)
        payload["amount_usd"] = float(amt_usd)

    else:
        base_amt = float(sell_base_amount)
        if base_amt <= 0:
            base_amt = float(DEFAULT_SELL_BASE_AMOUNT)
        payload["amount_base"] = float(base_amt)

    return {
        "type": "order.place",
        "venue": venue,     # REQUIRED at root
        "symbol": symbol,   # REQUIRED at root
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

    # Master dryrun toggles (your JSON)
    dryrun_enabled = _boolish(cfg_get(cfg, "phase26.dryrun.enabled", True))
    allow_order_place = _boolish(cfg_get(cfg, "phase26.dryrun.allow_order_place", False))
    allow_immature_dryrun = _boolish(cfg_get(cfg, "gates.allow_immature_dryrun", False))

    buy_max_usd = float(cfg_get(cfg, "phase26.dryrun.buy_max_usd", DEFAULT_BUY_MAX_USD))
    sell_base_amount = float(cfg_get(cfg, "phase26.dryrun.sell_base_amount", DEFAULT_SELL_BASE_AMOUNT))

    if not dryrun_enabled:
        info("alpha_outbox_orderplace_dryrun: skipped (phase26.dryrun.enabled=false)")
        return 0, 0, "disabled"
    if not allow_order_place:
        info("alpha_outbox_orderplace_dryrun: skipped (phase26.dryrun.allow_order_place=false)")
        return 0, 0, "not_allowed"
    if not _allowed_by_killswitch(cfg):
        warn("alpha_outbox_orderplace_dryrun: blocked by kill_switches")
        return 0, 0, "killed"

    store = get_store()
    processed = 0
    enq = 0

    try:
        cur = conn.cursor()
        translations = _fetch_latest_approved_translations(cur, limit=limit)
        processed = len(translations)

        for t in translations:
            action = (t.get("action") or "").upper()
            if action not in ("WOULD_BUY", "WOULD_SELL"):
                # IMPORTANT: do not create orders for WOULD_WATCH
                continue

            venue = (t.get("venue") or "").upper()
            if not _venue_allows_dryrun(cfg, venue):
                continue

            gates = t.get("gates") or {}
            gate_a = _gate_a_ok(gates)
            blockers = gates.get("blockers", [])

            # eligibility:
            # - normal: Gate A ok AND no blockers
            # - override: allow immature dryrun (Gate A may fail) only when blocker(s) are IMMATURE
            if gate_a:
                # Gate A passed: require no blockers at all
                if isinstance(blockers, list) and len(blockers) > 0:
                    continue
                if isinstance(blockers, str) and blockers.strip():
                    continue
            else:
                # Gate A failed
                if not _allow_immature(gates, allow_immature_dryrun):
                    continue

            translation_id = t.get("translation_id") or ""
            if not translation_id:
                continue
            if _already_enqueued(cur, translation_id):
                continue

            intent = _build_intent(t=t, buy_max_usd=buy_max_usd, sell_base_amount=sell_base_amount)

            # enqueue
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
