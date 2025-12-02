#!/usr/bin/env python
"""
Phase 19 - DB Backbone Inspector

Small CLI helper to peek into commands / receipts / telemetry
without touching Sheets.
"""

from pprint import pprint
from typing import List, Dict, Any

from db_backbone import (
    get_recent_commands,
    get_recent_receipts,
    get_recent_telemetry,
)


def _print_section(title: str) -> None:
    bar = "=" * len(title)
    print(f"\n{title}\n{bar}")


def main() -> None:
    # Commands
    _print_section("Recent commands")
    cmds: List[Dict[str, Any]] = get_recent_commands(limit=10)
    for row in cmds:
        print(
            f"[cmd #{row['id']}] agent={row['agent_id']!r} "
            f"status={row.get('status')!r} "
            f"leased_by={row.get('leased_by')!r} "
            f"created_at={row['created_at']}"
        )

    # Receipts
    _print_section("Recent receipts")
    recs: List[Dict[str, Any]] = get_recent_receipts(limit=10)
    for row in recs:
        print(
            f"[rcpt #{row['id']}] cmd_id={row['cmd_id']} "
            f"ok={row['ok']} created_at={row['created_at']}"
        )

    # Telemetry
    _print_section("Recent telemetry")
    tels: List[Dict[str, Any]] = get_recent_telemetry(limit=5)
    for row in tels:
        print(
            f"[tel #{row['id']}] agent={row['agent_id']!r} "
            f"created_at={row['created_at']}"
        )
        # Show just venues summary if present
        payload = row.get("payload") or {}
        snapshot = payload.get("snapshot") or payload
        venues = snapshot.get("venues") or snapshot.get("balances") or {}
        if isinstance(venues, dict):
            print("  venues:", ", ".join(sorted(venues.keys()))[:120])
        else:
            print("  payload keys:", ", ".join(sorted(snapshot.keys()))[:120])


if __name__ == "__main__":
    main()
