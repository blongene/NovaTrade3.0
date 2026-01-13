import os
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

def _db_url() -> str:
    u = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not u:
        raise RuntimeError("DB_URL (or DATABASE_URL) is required")
    return u

def _get_columns(conn, table: str, schema: str = "public") -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]

def _pick_first(cols: List[str], candidates: List[str]) -> Optional[str]:
    s = set(cols)
    for c in candidates:
        if c in s:
            return c
    return None

def enqueue_command(
    *,
    command_type: str,
    payload: Dict[str, Any],
    idempotency_key: str,
    note: str = "",
    source: str = "alpha26",
    status: str = "queued",
) -> int:
    """
    Insert a row into `commands` with schema adaptation.

    We *discover* the column names at runtime and map:
      - payload column: payload|intent|command|body|data|json
      - type column: type|command_type|kind
      - status column: status|state
      - idempotency: idempotency_key|idem_key|dedupe_key|request_id
      - note: note|reason|memo|message
      - source: source|origin|producer
    """
    conn = psycopg2.connect(_db_url(), sslmode="require")
    try:
        cols = _get_columns(conn, "commands")
        payload_col = _pick_first(cols, ["payload", "intent", "command", "body", "data", "json"])
        type_col = _pick_first(cols, ["type", "command_type", "kind"])
        status_col = _pick_first(cols, ["status", "state"])
        idem_col = _pick_first(cols, ["idempotency_key", "idem_key", "dedupe_key", "request_id"])
        note_col = _pick_first(cols, ["note", "reason", "memo", "message"])
        source_col = _pick_first(cols, ["source", "origin", "producer"])

        if not payload_col:
            raise RuntimeError(f"commands table has no recognized payload column. columns={cols}")

        insert_cols = []
        insert_vals = []

        if type_col:
            insert_cols.append(type_col); insert_vals.append(command_type)
        if status_col:
            insert_cols.append(status_col); insert_vals.append(status)
        if idem_col:
            insert_cols.append(idem_col); insert_vals.append(idempotency_key)
        if note_col:
            insert_cols.append(note_col); insert_vals.append(note)
        if source_col:
            insert_cols.append(source_col); insert_vals.append(source)

        insert_cols.append(payload_col)
        insert_vals.append(psycopg2.extras.Json(payload))

        cols_sql = ", ".join(insert_cols)
        ph_sql = ", ".join(["%s"] * len(insert_vals))

        returning = "id" if "id" in cols else insert_cols[0]
        sql = f"INSERT INTO commands ({cols_sql}) VALUES ({ph_sql}) RETURNING {returning};"

        with conn.cursor() as cur:
            cur.execute(sql, insert_vals)
            new_id = cur.fetchone()[0]
        conn.commit()
        return int(new_id)
    finally:
        conn.close()

def debug_dump_latest_commands(limit: int = 10) -> None:
    conn = psycopg2.connect(_db_url(), sslmode="require")
    try:
        cols = _get_columns(conn, "commands")
        want = []
        for c in [
            "id","ts","created_at",
            "status","state",
            "type","command_type","kind",
            "idempotency_key","idem_key","dedupe_key","request_id",
            "note","reason","memo","message",
            "source","origin","producer",
        ]:
            if c in cols:
                want.append(c)

        payload_col = _pick_first(cols, ["payload", "intent", "command", "body", "data", "json"])
        if payload_col:
            want.append(payload_col)

        proj = ", ".join(want) if want else "*"
        order_col = "id" if "id" in cols else (want[0] if want else "1")

        with conn.cursor() as cur:
            cur.execute(f"SELECT {proj} FROM commands ORDER BY {order_col} DESC LIMIT %s;", (limit,))
            rows = cur.fetchall()

        print("commands.columns =", cols)
        print("---- latest commands ----")
        for r in rows:
            print(r)
    finally:
        conn.close()
