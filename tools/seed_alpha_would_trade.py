#!/usr/bin/env python3
"""
tools/seed_alpha_would_trade.py

Seed a single synthetic Alpha proposal (WOULD_TRADE) for WNH lane testing.
- No schema guesses beyond required columns used elsewhere (proposal_id, proposal_hash, token, action, confidence, payload)
- Idempotent WITHOUT requiring a unique constraint (WHERE NOT EXISTS)
"""
from __future__ import annotations

import os, json, uuid
from datetime import datetime, timezone
import psycopg2


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def main():
    url = os.getenv("DATABASE_URL") or os.getenv("DB_URL")
    if not url:
        raise RuntimeError("DATABASE_URL/DB_URL not set")

    proposal_id = str(uuid.uuid4())
    token = "TESTTRADE"
    action = "WOULD_TRADE"
    confidence = 0.42

    day = utc_day()
    proposal_hash = f"{token}|{action}|{day}"

    payload = {
        "token": token,
        "action": action,
        "confidence": confidence,
        "source": "seed_alpha_would_trade",
        "note": "Synthetic test proposal for WNH coverage",
        "utc_day": day,
    }

    conn = psycopg2.connect(url)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        """
        insert into alpha_proposals (proposal_id, proposal_hash, token, action, confidence, payload)
        select %s, %s, %s, %s, %s, %s::jsonb
        where not exists (
            select 1 from alpha_proposals where proposal_hash = %s
        )
        """,
        (proposal_id, proposal_hash, token, action, confidence, json.dumps(payload), proposal_hash),
    )
    inserted = (cur.rowcount == 1)

    cur.close()
    conn.close()

    print(json.dumps({
        "ok": True,
        "inserted": inserted,
        "proposal_id": proposal_id,
        "proposal_hash": proposal_hash,
        "token": token,
        "action": action,
        "confidence": confidence
    }, indent=2))


if __name__ == "__main__":
    main()
