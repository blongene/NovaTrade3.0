#!/usr/bin/env python3
"""
Phase 26E — Allow dryrun order.place BUY/SELL

This module:
- selects most-recent approved translation(s) from alpha_translations
- constructs an intent of type "order.place" with payload sizing:
    BUY  -> amount_usd = buy_max_usd
    SELL -> amount_base = sell_base_amount
- inserts into commands with a computed NOT-NULL intent_hash
- records into alpha_dryrun_orderplace_outbox (unique per translation_id)

Env:
  DB_URL (required)
  ALPHA_CONFIG_JSON (optional, JSON)
  ALPHA26E_IDEM_PREFIX (optional; default: alpha26e_preview)
  ALPHA26E_TEST_SIDE (optional; BUY or SELL; default BUY)
  ALPHA26E_MAX_ROWS (optional; default 1)
"""

import os
import json
import time
import hashlib
import logging
from typing import Any, Dict, Optional, Tuple

import psycopg2
import psycopg2.extras


# ----------------------------
# logging
# ----------------------------
LOG = logging.getLogger("alpha_outbox_orderplace_dryrun")
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s  %(message)s"))
LOG.addHandler(_handler)
LOG.setLevel(logging.INFO)


# ----------------------------
# config helpers
# ----------------------------
def _load_alpha_config() -> Dict[str, Any]:
    raw = os.getenv("ALPHA_CONFIG_JSON", "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception as e:
        LOG.warning("ALPHA_CONFIG_JSON is not valid JSON; ignoring. err=%s", e)
        return {}


def _get_cfg(cfg: Dict[str, Any], path: str, default: Any) -> Any:
    cur: Any = cfg
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


# ----------------------------
# hashing
# ----------------------------
def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _build_intent_and_hash(
    *,
    translation_id: str,
    proposal_id: str,
    token: str,
    venue: str,
    symbol: str,
    side: str,
    cfg: Dict[str, Any],
    idem_prefix: str,
) -> Tuple[Dict[str, Any], str, str]:
    """
    Returns: (intent_json, intent_hash, idempotency_key)
    """
    side_u = (side or "").upper().strip()
    if side_u not in ("BUY", "SELL"):
        side_u = "BUY"

    buy_max_usd = float(_get_cfg(cfg, "phase26.dryrun.buy_max_usd", 10) or 0)
    sell_base_amount = float(_get_cfg(cfg, "phase26.dryrun.sell_base_amount", 0.00005) or 0)

    payload: Dict[str, Any] = {
        "dry_run": True,
        "venue": venue,
        "symbol": symbol,
        "token": token,
        "side": side_u,
        "meta": {
            "phase": "26E",
            "translation_id": translation_id,
            "proposal_id": proposal_id,
        },
    }

    # Sizing rules per your spec
    if side_u == "BUY":
        # must be > 0 or edge will reject
        amount_usd = max(buy_max_usd, 0.0)
        payload["amount_usd"] = amount_usd
    else:
        # SELL uses fixed base amount
        amount_base = max(sell_base_amount, 0.0)
        payload["amount_base"] = amount_base

    # idempotency_key should be stable for same translation+prefix+side
    stable_key_src = f"{idem_prefix}|{translation_id}|{side_u}|{venue}|{symbol}"
    stable_key = _sha256_hex(stable_key_src)
    idempotency_key = f"{idem_prefix}:{stable_key}"
    payload["idempotency_key"] = idempotency_key

    intent = {
        "type": "order.place",
        "venue": venue,
        "symbol": symbol,
        "payload": payload,
    }

    # intent_hash must be NOT NULL in commands.
    # Use a deterministic hash of canonical fields.
    # (Do NOT include timestamps.)
    intent_hash_src = json.dumps(
        {
            "type": intent["type"],
            "venue": venue,
            "symbol": symbol,
            "side": side_u,
            "amount_usd": payload.get("amount_usd"),
            "amount_base": payload.get("amount_base"),
            "idempotency_key": idempotency_key,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    intent_hash = _sha256_hex(intent_hash_src)

    return intent, intent_hash, idempotency_key


# ----------------------------
# db helpers
# ----------------------------
def _db_conn():
    db_url = os.getenv("DB_URL", "").strip()
    if not db_url:
        raise RuntimeError("DB_URL env var is required")
    return psycopg2.connect(db_url)


def run() -> None:
    cfg = _load_alpha_config()

    enabled = bool(_get_cfg(cfg, "phase26.dryrun.enabled", True))
    allow_order_place = bool(_get_cfg(cfg, "phase26.dryrun.allow_order_place", True))
    kill_global = bool(_get_cfg(cfg, "kill_switches.global", False))
    kill_edge = bool(_get_cfg(cfg, "kill_switches.edge_hold", False))
    allow_immature = bool(_get_cfg(cfg, "gates.allow_immature_dryrun", True))

    if not enabled:
        LOG.info("alpha_outbox_orderplace_dryrun: disabled by config (phase26.dryrun.enabled=false)")
        return
    if not allow_order_place:
        LOG.info("alpha_outbox_orderplace_dryrun: allow_order_place=false (no enqueue)")
        return
    if kill_global or kill_edge:
        LOG.warning(
            "alpha_outbox_orderplace_dryrun: kill switch active global=%s edge_hold=%s (no enqueue)",
            kill_global,
            kill_edge,
        )
        return

    idem_prefix = os.getenv("ALPHA26E_IDEM_PREFIX", "alpha26e_preview").strip() or "alpha26e_preview"
    side = (os.getenv("ALPHA26E_TEST_SIDE", "BUY") or "BUY").upper().strip()
    max_rows = int(os.getenv("ALPHA26E_MAX_ROWS", "1") or "1")

    processed = 0
    enqueued_new = 0

    with _db_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Pull latest approved translations that aren't already in alpha_dryrun_orderplace_outbox
        # Note: we intentionally do not require "mature" if allow_immature_dryrun=true.
        # If you later want a strict gate, add WHERE clauses based on payload/gates.
        cur.execute(
            """
            SELECT
              t.translation_id::text,
              t.proposal_id::text,
              COALESCE(NULLIF(t.token,''), '') AS token,
              COALESCE(NULLIF(t.venue,''), '') AS venue,
              COALESCE(NULLIF(t.symbol,''), '') AS symbol,
              COALESCE(NULLIF(t.agent_id,''), 'edge-primary') AS agent_id,
              COALESCE(NULLIF(t.action,''), '') AS action,
              t.ts
            FROM alpha_translations t
            WHERE COALESCE(NULLIF(t.approval_decision,''),'') = 'APPROVE'
              AND NOT EXISTS (
                SELECT 1 FROM alpha_dryrun_orderplace_outbox o
                WHERE o.translation_id = t.translation_id
              )
            ORDER BY t.ts DESC
            LIMIT %s;
            """,
            (max_rows,),
        )
        rows = cur.fetchall() or []

        for r in rows:
            processed += 1

            translation_id = r["translation_id"]
            proposal_id = r["proposal_id"]
            token = (r["token"] or "").strip()
            venue = (r["venue"] or "").strip()
            symbol = (r["symbol"] or "").strip()
            agent_id = (r["agent_id"] or "edge-primary").strip()

            # sanity checks
            if not venue or not symbol:
                LOG.warning("skip translation_id=%s missing venue/symbol venue='%s' symbol='%s'", translation_id, venue, symbol)
                continue

            # If you ever want to block WOULD_WATCH translations unless allow_immature, do it here.
            # For now: honor allow_immature_dryrun; enqueue even from watch as a realistic dryrun test.
            if not allow_immature and (r.get("action") == "WOULD_WATCH"):
                LOG.info("skip translation_id=%s action=WOULD_WATCH and allow_immature_dryrun=false", translation_id)
                continue

            intent, intent_hash, idempotency_key = _build_intent_and_hash(
                translation_id=translation_id,
                proposal_id=proposal_id,
                token=token,
                venue=venue,
                symbol=symbol,
                side=side,
                cfg=cfg,
                idem_prefix=idem_prefix,
            )

            # Ensure BUY has positive amount_usd; SELL has positive amount_base
            payload = intent.get("payload", {})
            if intent["payload"].get("side") == "BUY" and float(payload.get("amount_usd") or 0) <= 0:
                LOG.warning("skip translation_id=%s BUY amount_usd<=0", translation_id)
                continue
            if intent["payload"].get("side") == "SELL" and float(payload.get("amount_base") or 0) <= 0:
                LOG.warning("skip translation_id=%s SELL amount_base<=0", translation_id)
                continue

            # Insert command (must include intent_hash)
            cur.execute(
                """
                INSERT INTO commands (agent_id, intent, intent_hash, status, created_at)
                VALUES (%s, %s::jsonb, %s, 'queued', now())
                RETURNING id;
                """,
                (agent_id, json.dumps(intent), intent_hash),
            )
            cmd_id = cur.fetchone()["id"]

            # Record outbox row (unique per translation_id)
            note = f"Alpha26E dryrun {intent['payload'].get('side')} queued cmd_id={cmd_id} idem={idempotency_key}"
            cur.execute(
                """
                INSERT INTO alpha_dryrun_orderplace_outbox
                  (translation_id, proposal_id, token, venue, symbol, side, cmd_id, intent_hash, intent, note)
                VALUES
                  (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (translation_id) DO NOTHING;
                """,
                (
                    translation_id,
                    proposal_id,
                    token,
                    venue,
                    symbol,
                    intent["payload"].get("side", ""),
                    cmd_id,
                    intent_hash,
                    json.dumps(intent),
                    note,
                ),
            )

            enqueued_new += 1

        conn.commit()

    LOG.info("alpha_outbox_orderplace_dryrun: processed=%s enqueued_new=%s", processed, enqueued_new)


if __name__ == "__main__":
    run()
