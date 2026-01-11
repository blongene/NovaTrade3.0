# alpha_outbox_orderplace_dryrun.py
# Phase 26E – Dryrun order.place (BUY / SELL)
# Bullet-proof sizing enforcement

import os
import json
import hashlib
from datetime import datetime
import psycopg2

from utils import log

DB_URL = os.environ["DB_URL"]

# Load ALPHA_CONFIG_JSON (single env var strategy)
ALPHA_CONFIG = json.loads(os.environ.get("ALPHA_CONFIG_JSON", "{}"))

DRYRUN_CFG = (
    ALPHA_CONFIG
    .get("phase26", {})
    .get("dryrun", {})
)

BUY_MAX_USD = float(DRYRUN_CFG.get("buy_max_usd", 0))
SELL_BASE_AMOUNT = float(DRYRUN_CFG.get("sell_base_amount", 0))

ALLOW_ORDER_PLACE = bool(DRYRUN_CFG.get("allow_order_place", False))
DRYRUN_ENABLED = bool(DRYRUN_CFG.get("enabled", False))

IDEM_PREFIX = os.environ.get("ALPHA26E_IDEM_PREFIX", "alpha26e")

def make_intent_hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

def run():
    if not DRYRUN_ENABLED or not ALLOW_ORDER_PLACE:
        log.info("alpha_outbox_orderplace_dryrun: disabled by config")
        return

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # Pull newest translation
    cur.execute("""
        select
            translation_id,
            proposal_id,
            token,
            venue,
            symbol,
            action,
            confidence,
            rationale
        from alpha_translations
        order by ts desc
        limit 1;
    """)

    row = cur.fetchone()
    if not row:
        log.info("alpha_outbox_orderplace_dryrun: no translations")
        return

    (
        translation_id,
        proposal_id,
        token,
        venue,
        symbol,
        action,
        confidence,
        rationale
    ) = row

    # Decide side
    if action.upper() in ("WOULD_BUY", "BUY"):
        side = "BUY"
    elif action.upper() in ("WOULD_SELL", "SELL"):
        side = "SELL"
    else:
        log.info(f"alpha_outbox_orderplace_dryrun: action={action} not tradable")
        return

    payload = {
        "dry_run": True,
        "idempotency_key": f"{IDEM_PREFIX}:{translation_id}",
        "meta": {
            "phase": "26E",
            "confidence": float(confidence),
            "rationale": rationale,
            "translation_id": str(translation_id),
            "proposal_id": str(proposal_id),
            "token": token,
        },
    }

    # 🔒 STRICT SIZING RULES
    if side == "BUY":
        if BUY_MAX_USD <= 0:
            log.info("alpha_outbox_orderplace_dryrun: BUY disabled (buy_max_usd <= 0)")
            return
        payload["amount_usd"] = BUY_MAX_USD

    elif side == "SELL":
        if SELL_BASE_AMOUNT <= 0:
            log.info("alpha_outbox_orderplace_dryrun: SELL disabled (sell_base_amount <= 0)")
            return
        payload["amount_base"] = SELL_BASE_AMOUNT

    intent = {
        "type": "order.place",
        "venue": venue,
        "symbol": symbol,
        "side": side,
        "payload": payload,
    }

    intent_hash = make_intent_hash(intent)

    # Insert command
    cur.execute("""
        insert into commands (intent, status)
        values (%s, 'queued')
        returning id;
    """, (json.dumps(intent),))

    cmd_id = cur.fetchone()[0]

    # Mirror into dryrun outbox
    cur.execute("""
        insert into alpha_dryrun_orderplace_outbox (
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
        ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        on conflict (translation_id) do nothing;
    """, (
        translation_id,
        proposal_id,
        token,
        venue,
        symbol,
        side,
        cmd_id,
        intent_hash,
        json.dumps(intent),
        "Phase 26E dryrun order.place",
    ))

    conn.commit()
    cur.close()
    conn.close()

    log.info("alpha_outbox_orderplace_dryrun: processed=1 enqueued_new=1")

if __name__ == "__main__":
    run()
