#!/usr/bin/env python3
"""
admin_peek.py — read-only inspector for the Command Outbox (cloud)

Usage examples:
  # Overall status
  python admin_peek.py --summary

  # List pending (due) commands (default limit 20)
  python admin_peek.py --list --status pending --limit 20

  # List in-flight (leased) commands for a specific agent
  python admin_peek.py --list --status in_flight --agent orion-local

  # Show a single command (with payload + latest receipt)
  python admin_peek.py --show 42

  # List most recent receipts (limit 30) as JSON
  python admin_peek.py --receipts --limit 30 --json
"""

import os, sqlite3, json, time, argparse
from datetime import datetime, timezone

DB_PATH = os.getenv("OUTBOX_DB_PATH", "./data/outbox.db")

def _connect():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    # match outbox_db.py pragmas (read-only is fine without WAL here)
    con.execute("PRAGMA busy_timeout = 5000;")
    return con

def _ts(sec: int|None) -> str:
    if not sec: return ""
    try:
        return datetime.fromtimestamp(int(sec), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + "Z"
    except Exception:
        return str(sec)

def _age(sec: int|None) -> str:
    if not sec: return ""
    try:
        d = time.time() - int(sec)
        if d < 0: return f"-{abs(int(d))}s"
        if d < 90: return f"{int(d)}s"
        if d < 3600: return f"{int(d//60)}m"
        if d < 86400: return f"{int(d//3600)}h"
        return f"{int(d//86400)}d"
    except Exception:
        return ""

def summary(agent: str|None):
    q = """
      SELECT status, COUNT(*) AS n
      FROM commands
      WHERE (? IS NULL OR agent_id = ?)
      GROUP BY status
      ORDER BY status
    """
    q_due = """
      SELECT COUNT(*) AS n_due
      FROM commands
      WHERE (? IS NULL OR agent_id = ?)
        AND status='pending'
        AND (not_before=0 OR not_before<=?)
    """
    q_ndue = """
      SELECT COUNT(*) AS n_not_due
      FROM commands
      WHERE (? IS NULL OR agent_id = ?)
        AND status='pending'
        AND (not_before>?)
    """
    q_oldest = """
      SELECT MIN(created_at) AS oldest, MAX(created_at) AS newest
      FROM commands
      WHERE (? IS NULL OR agent_id = ?)
    """
    q_receipts = """
      SELECT COUNT(*) AS n_receipts FROM receipts
      WHERE (? IS NULL OR agent_id = ?)
    """
    now = int(time.time())
    with _connect() as con:
        rows = con.execute(q, (agent, agent)).fetchall()
        due = con.execute(q_due, (agent, agent, now)).fetchone()["n_due"]
        not_due = con.execute(q_ndue, (agent, agent, now)).fetchone()["n_not_due"]
        rn = {r["status"]: r["n"] for r in rows}
        ro = con.execute(q_oldest, (agent, agent)).fetchone()
        rr = con.execute(q_receipts, (agent, agent)).fetchone()["n_receipts"]

    print("== Outbox Summary ==")
    if agent: print(f"Agent: {agent}")
    print(f"pending:   {rn.get('pending',0)}  (due: {due}, not_due: {not_due})")
    print(f"in_flight: {rn.get('in_flight',0)}")
    print(f"done:      {rn.get('done',0)}")
    print(f"error:     {rn.get('error',0)}")
    print(f"expired:   {rn.get('expired',0)}")
    if ro and (ro["oldest"] or ro["newest"]):
        print(f"oldest:    {_ts(ro['oldest'])}  ({_age(ro['oldest'])} ago)")
        print(f"newest:    {_ts(ro['newest'])}  ({_age(ro['newest'])} ago)")
    print(f"receipts:  {rr}")

def list_commands(status: str, limit: int, agent: str|None, only_due: bool):
    base = """
      SELECT id, agent_id, status, kind, created_at, not_before, dedupe_key
      FROM commands
      WHERE (? IS NULL OR agent_id = ?)
    """
    args = [agent, agent]
    if status:
        base += " AND status = ?"
        args.append(status)
    now = int(time.time())
    if only_due:
        base += " AND (not_before=0 OR not_before<=?)"
        args.append(now)
    base += " ORDER BY id DESC LIMIT ?"
    args.append(limit)

    with _connect() as con:
        rows = con.execute(base, args).fetchall()

    print(f"== Commands (status={status or 'ANY'}, limit={limit}, due_only={only_due}) ==")
    for r in rows:
        due = "" if not r["not_before"] else ("DUE" if r["not_before"]<=now else "ETA")
        eta = "" if not r["not_before"] else f"{_ts(r['not_before'])} ({_age(r['not_before'])})"
        print(
            f"#{r['id']:>6}  {r['status']:<9}  {r['agent_id']:<14}  {r['kind']:<16}  "
            f"created {_ts(r['created_at'])} ({_age(r['created_at'])})  "
            f"{due:>3} {eta:>24}  dedupe={r['dedupe_key'] or ''}"
        )

def list_receipts(limit: int, agent: str|None, as_json: bool):
    q = """
      SELECT r.cmd_id, r.agent_id, r.ok, r.status, r.received_at, r.txid, r.message, r.result,
             c.kind
      FROM receipts r
      LEFT JOIN commands c ON c.id=r.cmd_id
      WHERE (? IS NULL OR r.agent_id = ?)
      ORDER BY r.id DESC LIMIT ?
    """
    with _connect() as con:
        rows = [dict(x) for x in con.execute(q, (agent, agent, limit)).fetchall()]
    if as_json:
        # Decode JSON in 'result' if possible
        for r in rows:
            try:
                r["result"] = json.loads(r["result"]) if isinstance(r.get("result"), str) else r["result"]
            except Exception:
                pass
        print(json.dumps(rows, indent=2, ensure_ascii=False))
        return
    print(f"== Receipts (limit={limit}) ==")
    for r in rows:
        ok = "OK " if r["ok"] else "ERR"
        msg = (r.get("message") or "")[:80]
        print(
            f"cmd#{r['cmd_id']:>6}  {ok} {r.get('status') or 'ok':<8}  {r['agent_id']:<14}  {r.get('kind') or '':<16}  "
            f"{_ts(r['received_at'])}  txid={r.get('txid') or ''}  {msg}"
        )

def show_command(cmd_id: int, as_json: bool):
    qc = "SELECT * FROM commands WHERE id=?"
    qr = "SELECT * FROM receipts WHERE cmd_id=? ORDER BY id DESC LIMIT 1"
    with _connect() as con:
        c = con.execute(qc, (cmd_id,)).fetchone()
        r = con.execute(qr, (cmd_id,)).fetchone()
    if not c:
        print(f"Command #{cmd_id} not found.")
        return
    out = {
        "command": {k: c[k] for k in c.keys()},
        "receipt": ({k: r[k] for k in r.keys()} if r else None),
    }
    # try decode JSON fields
    for fld in ("payload",):
        try:
            out["command"][fld] = json.loads(out["command"][fld])
        except Exception:
            pass
    if out["receipt"] and isinstance(out["receipt"].get("result"), str):
        try:
            out["receipt"]["result"] = json.loads(out["receipt"]["result"])
        except Exception:
            pass
    if as_json:
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return
    c = out["command"]; rec = out["receipt"]
    print("== Command ==")
    print(f"id:           {c['id']}")
    print(f"agent_id:     {c['agent_id']}")
    print(f"status:       {c['status']}")
    print(f"kind:         {c['kind']}")
    print(f"dedupe_key:   {c.get('dedupe_key')}")
    print(f"created_at:   {_ts(c['created_at'])} ({_age(c['created_at'])})")
    nb = c.get("not_before") or 0
    if nb:
        print(f"not_before:   {_ts(nb)} ({_age(nb)})")
    print("payload:")
    print(json.dumps(c["payload"], indent=2, ensure_ascii=False))
    print("\n== Latest Receipt ==")
    if not rec:
        print("(no receipt yet)")
    else:
        print(f"ok:           {bool(rec['ok'])}  status={rec.get('status')}")
        print(f"received_at:  {_ts(rec['received_at'])}")
        print(f"txid:         {rec.get('txid')}")
        if rec.get("message"):
            print(f"message:      {rec['message']}")
        if rec.get("fills"):
            print(f"fills:        {rec['fills']}")
        try:
            res = rec["result"] if isinstance(rec["result"], dict) else json.loads(rec["result"])
        except Exception:
            res = rec["result"]
        print("result:")
        print(json.dumps(res, indent=2, ensure_ascii=False))

def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--summary", action="store_true", help="Show high-level counts")
    g.add_argument("--list",    action="store_true", help="List commands")
    g.add_argument("--receipts",action="store_true", help="List recent receipts")
    g.add_argument("--show",    type=int, metavar="CMD_ID", help="Show single command by ID")

    ap.add_argument("--agent",  help="Filter by agent_id")
    ap.add_argument("--status", choices=["pending","in_flight","done","error","expired"], help="Filter status for --list")
    ap.add_argument("--limit",  type=int, default=20, help="Limit rows (list/receipts)")
    ap.add_argument("--due-only", action="store_true", help="Only due items for --list")
    ap.add_argument("--json",   action="store_true", help="JSON output (for --receipts/--show)")

    args = ap.parse_args()

    if args.summary or (not args.list and not args.receipts and not args.show):
        summary(args.agent)
        return
    if args.list:
        list_commands(args.status, args.limit, args.agent, args.due_only)
        return
    if args.receipts:
        list_receipts(args.limit, args.agent, args.json)
        return
    if args.show is not None:
        show_command(args.show, args.json)
        return

if __name__ == "__main__":
    raise SystemExit(main())
