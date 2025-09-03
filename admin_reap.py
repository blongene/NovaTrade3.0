#!/usr/bin/env python3
"""
admin_reap.py — re-queue expired or specific in_flight commands (cloud)

Examples:
  # Re-queue any in_flight commands whose lease has expired (default agent=ALL)
  python admin_reap.py --expired

  # Same, but only for a specific agent
  python admin_reap.py --expired --agent orion-local

  # Force a command back to pending by id (even if lease not expired)
  python admin_reap.py --force 42

  # See what's currently leased
  python admin_reap.py --list --agent orion-local
"""

import os, sqlite3, time, argparse
DB_PATH = os.getenv("OUTBOX_DB_PATH", "./data/outbox.db")

def _connect():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000;")
    return con

def list_inflight(agent=None):
    q = "SELECT id, agent_id, lease_expires_at FROM commands WHERE status='in_flight'"
    args = []
    if agent:
        q += " AND agent_id=?"; args.append(agent)
    q += " ORDER BY id ASC"
    with _connect() as con:
        rows = con.execute(q, args).fetchall()
    now = int(time.time())
    out = []
    for r in rows:
        exp = r["lease_expires_at"] or 0
        out.append((r["id"], r["agent_id"], exp, exp-now))
    return out

def reap_expired(agent=None):
    now = int(time.time())
    args = [now]
    q = "UPDATE commands SET status='pending', lease_expires_at=0 WHERE status='in_flight' AND lease_expires_at>0 AND lease_expires_at<=?"
    if agent:
        q += " AND agent_id=?"; args.append(agent)
    with _connect() as con:
        cur = con.execute(q, args)
        n = cur.rowcount or 0
    return n

def force_pending(cmd_id: int):
    with _connect() as con:
        cur = con.execute("UPDATE commands SET status='pending', lease_expires_at=0 WHERE id=?", (cmd_id,))
        return cur.rowcount or 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--agent", help="Filter by agent for list/reap")
    ap.add_argument("--expired", action="store_true", help="Re-queue expired leases")
    ap.add_argument("--force", type=int, help="Force a specific id back to pending")
    ap.add_argument("--list", action="store_true", help="List current in_flight leases")
    args = ap.parse_args()

    if args.list:
        rows = list_inflight(args.agent)
        if not rows:
            print("No in_flight commands.")
        else:
            print("in_flight leases:")
            for cid, agent, exp, delta in rows:
                status = "EXPIRED" if exp and exp <= int(time.time()) else f"in {delta}s"
                print(f"  #{cid}  agent={agent}  lease_expires_at={exp} ({status})")

    if args.expired:
        n = reap_expired(args.agent)
        print(f"Re-queued {n} expired in_flight command(s).")

    if args.force is not None:
        n = force_pending(args.force)
        print(f"Forced #{args.force} back to pending (rows={n}).")

if __name__ == "__main__":
    main()
