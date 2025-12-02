#!/usr/bin/env python
"""
Backfill trades table from receipts that don't yet have a trade row.

Safe to run multiple times; it only inserts for receipts with no trade.
"""

from typing import Any, Dict, List

from db_backbone import _get_conn, record_trade_from_receipt  # type: ignore[attr-defined]


def _fetch_unprocessed_receipts(batch_size: int = 500) -> List[Dict[str, Any]]:
    conn = _get_conn()
    if not conn:
        return []
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT r.id, r.cmd_id, r.ok, r.payload
            FROM receipts r
            LEFT JOIN trades t ON t.receipt_id = r.id
            WHERE t.id IS NULL
            ORDER BY r.id ASC
            LIMIT %s
            """,
            (batch_size,),
        )
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return rows
    finally:
        cur.close()


def main() -> None:
    total_inserted = 0
    while True:
        batch = _fetch_unprocessed_receipts(batch_size=500)
        if not batch:
            break
        for rcpt in batch:
            record_trade_from_receipt(rcpt)
            total_inserted += 1
    print(f"Backfill complete. Inserted {total_inserted} trades.")


if __name__ == "__main__":
    main()
