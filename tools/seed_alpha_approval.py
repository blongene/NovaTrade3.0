#!/usr/bin/env python3
"""
tools/seed_alpha_approval.py

Insert an APPROVE into alpha_approvals for a given proposal_hash.
Matches your schema:
  alpha_approvals columns: approval_id, ts, agent_id, proposal_id, proposal_hash, token, decision, actor, note, source, row_hash

Idempotent: skips if identical approval already exists.
"""
from __future__ import annotations

import os, uuid, hashlib, json
from datetime import datetime, timezone
import psycopg2


def utc_now():
    return datetime.now(timezone.utc)


def main():
    url = os.getenv("DATABASE_URL") or os.getenv("DB_URL")
    if not url:
        raise RuntimeError("DATABASE_URL/DB_URL not set")

    # Default to the TESTTRADE seed; you can override via env
    proposal_hash = os.getenv("SEED_PROPOSAL_HASH") or "TESTTRADE|WOULD_TRADE|%s" % datetime.now(timezone.utc).strftime("%Y-%m-%d")
    token = os.getenv("SEED_TOKEN") or "TESTTRADE"
    decision = os.getenv("SEED_DECISION") or "APPROVE"
    actor = os.getenv("SEED_ACTOR") or "brett"
    agent_id = os.getenv("SEED_AGENT_ID") or "bus"
    source = os.getenv("SEED_SOURCE") or "sheet"
    note = os.getenv("SEED_NOTE") or "Seed approval to test APPROVED_BUT_GATED WNH lane"

    conn = psycopg2.connect(url); conn.autocommit = True
    cur = conn.cursor()

    cur.execute("select proposal_id from alpha_proposals where proposal_hash=%s order by 1 desc limit 1", (proposal_hash,))
    r = cur.fetchone()
    if not r:
        raise RuntimeError("proposal_id not found for proposal_hash=%s" % proposal_hash)
    proposal_id = r[0]

    cur.execute("""
        select 1 from alpha_approvals
        where proposal_hash=%s and decision=%s and actor=%s
        limit 1
    """, (proposal_hash, decision, actor))
    if cur.fetchone():
        print(json.dumps({"ok": True, "inserted": False, "reason": "already_exists"}, indent=2))
        cur.close(); conn.close()
        return

    base = f"{proposal_id}|{proposal_hash}|{token}|{decision}|{actor}|{agent_id}|{source}"
    row_hash = hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]
    approval_id = str(uuid.uuid4())
    ts = utc_now()

    cur.execute("""
        insert into alpha_approvals
        (approval_id, ts, agent_id, proposal_id, proposal_hash, token, decision, actor, note, source, row_hash)
        values
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (approval_id, ts, agent_id, proposal_id, proposal_hash, token, decision, actor, note, source, row_hash))

    cur.close(); conn.close()
    print(json.dumps({"ok": True, "inserted": True, "approval_id": approval_id, "row_hash": row_hash, "proposal_hash": proposal_hash}, indent=2))


if __name__ == "__main__":
    main()
