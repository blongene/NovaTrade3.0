# tools/seed_alpha_would_trade.py
from __future__ import annotations

import os
import json
import uuid
from datetime import datetime, timezone

import psycopg2


def utc_day():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def main():
    url = os.getenv("DATABASE_URL") or os.getenv("DB_URL")
    if not url:
        raise RuntimeError("DATABASE_URL / DB_URL not set")

    conn = psycopg2.connect(url)
    conn.autocommit = True
    cur = conn.cursor()

    proposal_id = str(uuid.uuid4())
    token = "TESTTRADE"
    action = "WOULD_TRADE"
    confidence = 0.42

    today = utc_day()
    proposal_hash = f"{token}|{action}|{today}"

    payload = {
        "token": token,
        "action": action,
        "confidence": confidence,
        "source": "seed_alpha_would_trade",
        "note": "Synthetic test proposal for WNH coverage",
        "utc_day": today,
    }

    cur.execute(
        """
        insert into alpha_proposals (
            proposal_id,
            proposal_hash,
            token,
            action,
            confidence,
            payload
        )
        values (%s, %s, %s, %s, %s, %s::jsonb)
        on conflict (proposal_hash) do nothing
        """,
        (
            proposal_id,
            proposal_hash,
            token,
            action,
            confidence,
            json.dumps(payload),
        ),
    )

    cur.close()
    conn.close()

    print("Seeded alpha_proposals row:")
    print(
        json.dumps(
            {
                "proposal_id": proposal_id,
                "proposal_hash": proposal_hash,
                "action": action,
                "token": token,
                "confidence": confidence,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
