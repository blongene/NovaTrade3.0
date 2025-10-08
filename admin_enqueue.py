#!/usr/bin/env python3
"""
admin_enqueue.py — seed commands into the Outbox (cloud)

Usage examples:

1) Minimal dry-run test (default agent from AGENT_ID):
   python admin_enqueue.py --kind order.place --payload '{"venue":"MEXC","symbol":"BTC/USDT","side":"BUY","quote_amount":5,"mode":"market"}'

2) Specify agent and dedupe key (prevents duplicate pending/in_flight):
   python admin_enqueue.py --agent edge-nl-1 --kind order.place \
       --payload '{"venue":"MEXC","symbol":"MIND/USDT","side":"BUY","quote_amount":25,"mode":"market"}' \
       --dedupe rebuy:MIND:25

3) Delay execution by 2 minutes (not_before):
   python admin_enqueue.py --kind order.place --payload '{"venue":"MEXC","symbol":"ETH/USDT","side":"SELL","quote_amount":7.5}' \
       --delay 120

Environment:
- OUTBOX_DB_PATH (optional) for SQLite location (default ./data/outbox.db)
- AGENT_ID can be a single value or a CSV allow-list used elsewhere; this script only needs one agent id to target.

This script does NOT hit Flask; it writes directly to the Outbox DB.
"""

import os, json, time, argparse, sys
from outbox_db import init, enqueue

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--agent", default=None, help="Agent ID to target (or set AGENT_ID)")
    p.add_argument("--kind", required=True, help="Command kind, e.g. 'order.place'")
    p.add_argument("--payload", required=True, help="JSON payload string for the command")
    p.add_argument("--dedupe", default=None, help="Optional dedupe_key to suppress duplicate pending/in_flight")
    p.add_argument("--delay", type=int, default=0, help="Seconds from now before command becomes pullable (not_before)")
    args = p.parse_args()

    agent = args.agent or (os.getenv("AGENT_ID") or os.getenv("EDGE_AGENT_ID"))
    if not agent:
        raise SystemExit("agent_id required (use --agent or set AGENT_ID)")

    try:
        payload = json.loads(args.payload)
    except Exception as e:
        print(f"ERROR: payload is not valid JSON: {e}", file=sys.stderr)
        return 2

    not_before = int(time.time()) + max(0, int(args.delay))

    # Ensure DB exists / is upgraded
    init()

    cmd_id = enqueue(agent_id=agent, kind=args.kind, payload=payload,
                     not_before=not_before, dedupe_key=args.dedupe)

    if cmd_id == -1:
        print("SKIPPED: duplicate pending/in_flight command with same dedupe_key")
        return 0

    print(f"ENQUEUED: id={cmd_id} agent={agent} kind={args.kind} not_before={not_before}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
