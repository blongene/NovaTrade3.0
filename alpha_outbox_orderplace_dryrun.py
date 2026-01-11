#!/usr/bin/env python3
"""
Phase 26E — Dryrun order.place outbox (BUY / SELL)

• BUY  -> quote-sized using buy_max_usd
• SELL -> base-sized using sell_base_amount
• Fully dryrun-safe
• No utils.log dependency
"""

import os
import json
import hashlib
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# ------------------------------------------------------------------------------
# Logging (bullet-proof)
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[% (asctime)s] %(levelname)s  %(message)s",
)
LOG = logging.getLogger("alpha_outbox_orderplace_dryrun")

# ------------------------------------------------------------------------------
# Config helpers
# ------------------------------------------------------------------------------
def load_alpha_config():
    raw = os.getenv("ALPHA_CONFIG_JSON", "{}")
    try:
        return json.loads(raw)
    except Exception:
        LOG.error("Invalid ALPHA_CONFIG_JSON — defaulting empty")
        return {}

CFG = load_alpha_config()

PHASE = CFG.get("phase26", {}).get("dryrun", {})
BUY_MAX_USD = float(PHASE.get("buy_max_usd", 10))
SELL_BASE_AMOUNT = float(PHASE.get("sell_base_amount", 0.00005))
ALLOW_ORDER_PLACE = bool(PHASE.get("allow_order_place", False))

IDEM_PREFIX = os.getenv("ALPHA26E_IDEM_PREFIX", "alpha26e")

DB_URL = os.getenv("DB_URL")
AGENT_ID = os.getenv("AGENT_ID", "edge-primary")

# ------------------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------------------
def db():
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)

def sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()

# ------------------------------------------------------------------------------
# Main logic
# ------------------------------------------------------------------------------
def run():
    if not ALLOW_ORDER_PLACE:
        LOG.info("26E disabled via config")
        return

    inserted = 0
    enqueued = 0

    with db() as conn:
        with conn.cursor() as cur:

            # Pull latest translation that has not been outboxed
            cur.execute("""
                SELECT *
                FROM alpha_translations t
                WHERE NOT EXISTS (
                    SELECT 1 FROM alpha_dryrun_orderplace_outbox o
                    WHERE o.translation_id = t.translation_id
                )
                ORDER BY ts DESC
                LIMIT 1
            """)
            t = cur.fetchone()

            if not t:
                LOG.info("No eligible translations")
                return

            token = t["token"]
            venue = t["venue"]
            symbol = t["symbol"]
            action = t["action"]

            # Decide side
            if action in ("WOULD_BUY", "BUY"):
                side = "BUY"
            elif action in ("WOULD_SELL", "SELL"):
                side = "SELL"
            else:
                LOG.info("Skipping non-trade action=%s", action)
                return

            # Size logic (critical fix)
            payload = {
                "dry_run": True,
                "venue": venue,
                "symbol": symbol,
                "side": side,
                "token": token,
            }

            if side == "BUY":
                payload["amount_usd"] = BUY_MAX_USD
            else:
                payload["amount_base"] = SELL_BASE_AMOUNT

            idem = f"{IDEM_PREFIX}:{t['translation_id']}"
            payload["idempotency_key"] = idem

            intent = {
                "type": "order.place",
                "payload": payload,
            }

            intent_hash = sha(json.dumps(intent, sort_keys=True))

            # Enqueue command
            cur.execute("""
                INSERT INTO commands (agent_id, intent, status)
                VALUES (%s, %s::jsonb, 'queued')
                RETURNING id
            """, (AGENT_ID, json.dumps(intent)))
            cmd_id = cur.fetchone()["id"]
            enqueued += 1

            # Record outbox
            cur.execute("""
                INSERT INTO alpha_dryrun_orderplace_outbox (
                    translation_id,
                    proposal_id,
                    token,
                    venue,
                    symbol,
                    side,
                    cmd_id,
                    intent_hash,
                    intent,
                    note
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                t["translation_id"],
                t["proposal_id"],
                token,
                venue,
                symbol,
                side,
                cmd_id,
                intent_hash,
                json.dumps(intent),
                "26E dryrun order.place",
            ))

            inserted += 1
            conn.commit()

    LOG.info(
        "alpha_outbox_orderplace_dryrun: processed=%d enqueued_new=%d",
        inserted,
        enqueued,
    )

# ------------------------------------------------------------------------------
if __name__ == "__main__":
    run()
