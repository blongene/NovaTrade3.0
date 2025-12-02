# db_backbone.py
"""
NovaTrade DB backbone (Phase 19, Step 1)

Goals:
- Provide a *safe* Postgres ledger for:
  - Enqueued commands
  - Receipts (Edge ACKs)
  - Telemetry payloads
- Never break the existing command bus flows:
  - If DB_URL / psycopg2 are missing, all functions become no-ops.
  - SQLite outbox + Sheets remain the operational path.

Usage:
- Call record_command_enqueued(...) in /api/commands/enqueue
- Call record_receipt(...) in /api/commands/ack
- Call record_telemetry(...) in telemetry ingestion (optional)
"""

from __future__ import annotations

import json
import os
import threading
import traceback
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None  # type: ignore[assignment]

_DB_URL = os.getenv("DB_URL")
_conn_lock = threading.Lock()
_conn = None  # type: ignore[assignment]
_schema_initialized = False


def _get_conn():
    """Get (and lazily initialize) a global PG connection, or None if unavailable."""
    global _conn
    if not _DB_URL or not psycopg2:
        return None

    with _conn_lock:
        if _conn is not None:
            # Quick health check: if connection is dead, reset.
            try:
                cur = _conn.cursor()
                cur.execute("SELECT 1")
                _conn.commit()
                return _conn
            except Exception:
                try:
                    _conn.close()
                except Exception:
                    pass
                _conn = None

        try:
            _conn = psycopg2.connect(_DB_URL)
            _conn.autocommit = True
            return _conn
        except Exception as e:
            print(f"[db_backbone] Failed to connect to DB_URL: {e}")
            traceback.print_exc()
            _conn = None
            return None


def _ensure_schema() -> None:
    """Create Phase-19 tables if they don't exist yet."""
    global _schema_initialized
    if _schema_initialized:
        return
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nova_commands (
                id          BIGSERIAL PRIMARY KEY,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                agent_id    TEXT NOT NULL,
                payload     JSONB NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nova_receipts (
                id             BIGSERIAL PRIMARY KEY,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                agent_id       TEXT NOT NULL,
                cmd_id         BIGINT,
                ok             BOOLEAN,
                status         TEXT,
                venue          TEXT,
                symbol         TEXT,
                base           TEXT,
                quote          TEXT,
                notional_usd   NUMERIC,
                payload        JSONB NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nova_telemetry (
                id          BIGSERIAL PRIMARY KEY,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                agent_id    TEXT NOT NULL,
                kind        TEXT,
                payload     JSONB NOT NULL
            );
            """
        )
        _schema_initialized = True
    except Exception as e:
        print(f"[db_backbone] Failed to ensure schema: {e}")
        traceback.print_exc()


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return json.dumps({"_bad": True, "repr": repr(obj)})


def record_command_enqueued(agent_id: str, payload: Dict[str, Any]) -> None:
    """
    Log an enqueued command into Postgres.

    This does *not* change how the command bus behaves; it's a ledger-only write.
    """
    conn = _get_conn()
    if not conn:
        return
    _ensure_schema()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO nova_commands (agent_id, payload)
            VALUES (%s, %s)
            """,
            (agent_id, psycopg2.extras.Json(payload)),
        )
    except Exception as e:
        print(f"[db_backbone] record_command_enqueued failed: {e}")
        traceback.print_exc()


def record_receipt(
    agent_id: str,
    cmd_id: Optional[int],
    receipt: Dict[str, Any],
    ok: Optional[bool] = None,
) -> None:
    """
    Log a normalized receipt into Postgres.

    Fields like venue/symbol/notional_usd are *best-effort* extractions
    from the receipt dict; if missing, they remain NULL.
    """
    conn = _get_conn()
    if not conn:
        return
    _ensure_schema()

    ok_val = ok
    if ok_val is None:
        # Try to infer from receipt structure
        if "ok" in receipt:
            try:
                ok_val = bool(receipt["ok"])
            except Exception:
                ok_val = None

    # Try to pull out some top-level fields for fast queries
    venue = None
    symbol = None
    base = None
    quote = None
    notional_usd = None

    try:
        venue = receipt.get("venue") or receipt.get("exchange")
        symbol = receipt.get("symbol")
        base = receipt.get("base")
        quote = receipt.get("quote")
        notional_usd = receipt.get("notional_usd")
    except Exception:
        pass

    status = receipt.get("status")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO nova_receipts
                (agent_id, cmd_id, ok, status,
                 venue, symbol, base, quote, notional_usd, payload)
            VALUES (%s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s)
            """,
            (
                agent_id,
                int(cmd_id) if cmd_id is not None else None,
                ok_val,
                status,
                venue,
                symbol,
                base,
                quote,
                notional_usd,
                psycopg2.extras.Json(receipt),
            ),
        )
    except Exception as e:
        print(f"[db_backbone] record_receipt failed: {e}")
        traceback.print_exc()


def record_telemetry(agent_id: str, payload: Dict[str, Any], kind: str = None) -> None:
    """
    Log telemetry payloads into Postgres.

    'kind' is a short free-text tag like 'balances', 'snapshot', etc.
    """
    conn = _get_conn()
    if not conn:
        return
    _ensure_schema()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO nova_telemetry (agent_id, kind, payload)
            VALUES (%s, %s, %s)
            """,
            (agent_id, kind, psycopg2.extras.Json(payload)),
        )
    except Exception as e:
        print(f"[db_backbone] record_telemetry failed: {e}")
        traceback.print_exc()
      
# --- DB observability helpers (Phase 19 Step 3) -----------------------------
def _fetchall(query: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    """Internal helper to run a query and return dict rows."""
    conn = _get_conn()
    if not conn:
        return []
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return rows
    finally:
        cur.close()

def get_recent_commands(limit: int = 20) -> List[Dict[str, Any]]:
    """
    Return the most recent commands from the DB backbone.

    Schema expectation (db_schema.sql):
      commands(id, agent_id, status, leased_by, payload, created_at, lease_expires_at)
    """
    sql = """
        SELECT id, agent_id, status, leased_by, created_at
        FROM commands
        ORDER BY id DESC
        LIMIT %s
    """
    return _fetchall(sql, (limit,))


def get_recent_receipts(limit: int = 20) -> List[Dict[str, Any]]:
    """
    Return the most recent receipts.

    Schema expectation:
      receipts(id, cmd_id, ok, payload, created_at)
    """
    sql = """
        SELECT id, cmd_id, ok, created_at
        FROM receipts
        ORDER BY id DESC
        LIMIT %s
    """
    return _fetchall(sql, (limit,))


def get_recent_telemetry(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Return the most recent telemetry rows (the raw snapshots being stored
    by telemetry_mirror / Edge pushes).

    Schema expectation:
      telemetry(id, agent_id, payload, created_at)
    """
    sql = """
        SELECT id, agent_id, created_at, payload
        FROM telemetry
        ORDER BY id DESC
        LIMIT %s
    """
    return _fetchall(sql, (limit,))

def get_recent_trades(limit: int = 20) -> List[Dict[str, Any]]:
    """
    Return the most recent normalized trades from the DB.
    """
    sql = """
        SELECT id, venue, symbol, side,
               base_qty, quote_qty, price, status,
               created_at
        FROM trades
        ORDER BY id DESC
        LIMIT %s
    """
    return _fetchall(sql, (limit,))

def record_trade_from_receipt(receipt_row: Dict[str, Any]) -> None:
    """
    Normalize an Edge receipt into the trades table.

    expected receipt_row keys (from receipts table + payload JSON):
      - id, cmd_id, ok, payload (dict)
    """
    conn = _get_conn()
    if not conn:
        return

    payload = receipt_row.get("payload") or {}
    # In case we ever wrap under 'trade'
    trade = payload.get("trade") or payload

    venue = trade.get("venue") or payload.get("venue")
    symbol = trade.get("symbol") or payload.get("symbol")
    side = trade.get("side") or trade.get("direction")
    base_qty = (
        trade.get("filled_base")
        or trade.get("base_qty")
        or trade.get("amount_base")
    )
    quote_qty = (
        trade.get("filled_quote")
        or trade.get("quote_qty")
        or trade.get("amount_quote")
    )
    price = trade.get("price")
    status = (
        trade.get("status")
        or ("ok" if receipt_row.get("ok") else "error")
    )

    # If we somehow can't even tell venue/symbol, don't insert junk.
    if not venue or not symbol:
        return

    cmd_id = receipt_row.get("cmd_id")
    receipt_id = receipt_row.get("id")

    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO trades (
                cmd_id, receipt_id,
                venue, symbol, side,
                base_qty, quote_qty, price,
                status, raw_payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                cmd_id,
                receipt_id,
                venue,
                symbol,
                side,
                base_qty,
                quote_qty,
                price,
                status,
                json.dumps(payload),
            ),
        )
        conn.commit()
    finally:
        cur.close()

def record_trade_live(cmd_id: Optional[int], receipt: Dict[str, Any]) -> None:
    """
    Insert a trade row directly from a live Edge/Bus receipt dict.

    This does NOT depend on the receipts table; it's fed by receipt_bridge.
    """
    conn = _get_conn()
    if not conn:
        return

    # receipt shape we expect from the Bus ACK path:
    # {
    #   "id": 123,          # command id
    #   "ok": true/false,
    #   "status": "ok|error|...",
    #   "venue": "BINANCEUS",
    #   "symbol": "BTC-USD",
    #   "side": "BUY",
    #   "base_qty": ...,
    #   "quote_qty": ...,
    #   "price": ...,
    #   "result": { ... }   # venue-native payload
    # }

    ok = bool(receipt.get("ok", True))
    venue = receipt.get("venue")
    symbol = receipt.get("symbol")
    side = receipt.get("side") or receipt.get("direction")

    # quantities: first check normalized keys, then fall back to nested result
    base_qty = (
        receipt.get("base_qty")
        or receipt.get("filled_base")
        or (receipt.get("result") or {}).get("filled_base")
    )
    quote_qty = (
        receipt.get("quote_qty")
        or receipt.get("filled_quote")
        or (receipt.get("result") or {}).get("filled_quote")
    )
    price = (
        receipt.get("price")
        or (receipt.get("result") or {}).get("price")
    )

    status = receipt.get("status") or ("ok" if ok else "error")

    # If we can't even identify where the trade happened, bail quietly
    if not venue or not symbol:
        return

    raw_payload = json.dumps(receipt)

    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO trades (
                cmd_id, receipt_id,
                venue, symbol, side,
                base_qty, quote_qty, price,
                status, raw_payload
            )
            VALUES (%s, NULL, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(cmd_id) if cmd_id is not None else None,
                venue,
                symbol,
                side,
                base_qty,
                quote_qty,
                price,
                status,
                raw_payload,
            ),
        )
        conn.commit()
    finally:
        cur.close()
