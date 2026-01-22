import os, json
from datetime import datetime, timezone


def _pg_connect():
    import psycopg2  # type: ignore
    url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL/DB_URL not set")
    conn = psycopg2.connect(url)
    conn.autocommit = True
    return conn


def _safe_json(v) -> str:
    try:
        if isinstance(v, str):
            return v
        return json.dumps(v, separators=(",", ":"), default=str)
    except Exception:
        return str(v)


def _append_row_dict_to_sheet(tab: str, row_dict: dict) -> dict:
    """
    Uses your existing, bullet-proof header-mapped append.
    If you want, we can later split Council append into its own helper,
    but for now this keeps it consistent with what already works.
    """
    from wnh_logger import append_row_dict  # your proven helper
    # append_row_dict reads tab from DB_READ_JSON. We need explicit tab routing.
    # So we pass the tab explicitly via a reserved key and handle it in wnh_logger (small patch),
    # OR we do a local append here. Keeping local to avoid touching wnh_logger:
    try:
        from utils import get_ws_cached  # type: ignore
        ws = get_ws_cached(tab, ttl_s=10)
        header = ws.row_values(1) or []
        if not header:
            return {"ok": False, "reason": f"Empty header row in {tab}"}
        out = [row_dict.get(h, "") for h in header]
        try:
            ws.append_row(out, value_input_option="USER_ENTERED")
        except Exception:
            ws.append_row(out)
        try:
            from db_mirror import mirror_append  # type: ignore
            mirror_append(tab, [out])
        except Exception:
            pass
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "reason": f"{e.__class__.__name__}:{e}"}


def _already_mirrored(cur, tab: str, row_hash: str) -> bool:
    try:
        cur.execute(
            "SELECT 1 FROM sheet_mirror_events WHERE tab=%s AND row_hash=%s LIMIT 1",
            (tab, row_hash),
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def _mark_mirrored(cur, tab: str, row_hash: str, payload: dict):
    try:
        cur.execute(
            """
            INSERT INTO sheet_mirror_events(tab, row_hash, payload, created_at)
            VALUES (%s, %s, %s::jsonb, now())
            ON CONFLICT DO NOTHING
            """,
            (tab, row_hash, _safe_json(payload)),
        )
    except Exception:
        pass


def run_sheet_mirror_worker(limit: int = 100) -> dict:
    """
    Flushes DB-first events to Sheets UI.

    Order:
      - council_events -> Council_Insight
      - wnh_events -> Why_Nothing_Happened
    Deduped via sheet_mirror_events.
    """
    written = 0
    conn = None
    try:
        conn = _pg_connect()
        cur = conn.cursor()

        # 1) Council events
        cur.execute(
            """
            SELECT id, tab, row_hash, payload
            FROM council_events
            ORDER BY id ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        for _id, tab, rh, payload in cur.fetchall():
            if _already_mirrored(cur, tab, rh):
                continue
            row_dict = payload if isinstance(payload, dict) else {}
            r = _append_row_dict_to_sheet(tab, row_dict)
            if r.get("ok"):
                written += 1
                _mark_mirrored(cur, tab, rh, row_dict)

        # 2) WNH events
        cur.execute(
            """
            SELECT id, tab, row_hash, payload
            FROM wnh_events
            ORDER BY id ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        for _id, tab, rh, payload in cur.fetchall():
            if _already_mirrored(cur, tab, rh):
                continue
            row_dict = payload if isinstance(payload, dict) else {}
            r = _append_row_dict_to_sheet(tab, row_dict)
            if r.get("ok"):
                written += 1
                _mark_mirrored(cur, tab, rh, row_dict)

        return {"ok": True, "rows": written}

    except Exception as e:
        return {"ok": False, "rows": written, "reason": f"{e.__class__.__name__}:{e}"}
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    print(run_sheet_mirror_worker())
