import os, json, hashlib
from datetime import datetime, timezone

DEFAULT_COUNCIL_TAB = "Council_Insight"
DEFAULT_WNH_TAB = "Why_Nothing_Happened"


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


def row_hash(decision_id: str, payload: dict) -> str:
    s = f"{decision_id}|{_safe_json(payload)}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def put_council_event(decision_id: str, payload: dict, tab: str = DEFAULT_COUNCIL_TAB) -> dict:
    rh = row_hash(decision_id, payload)
    conn = None
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO council_events(decision_id, tab, row_hash, payload)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (tab, row_hash) DO NOTHING
            """,
            (decision_id, tab, rh, _safe_json(payload)),
        )
        return {"ok": True, "row_hash": rh, "tab": tab}
    except Exception as e:
        return {"ok": False, "reason": f"{e.__class__.__name__}:{e}", "tab": tab}
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def put_wnh_event(decision_id: str, payload: dict, tab: str = DEFAULT_WNH_TAB) -> dict:
    rh = row_hash(decision_id, payload)
    conn = None
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO wnh_events(decision_id, tab, row_hash, payload)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (tab, row_hash) DO NOTHING
            """,
            (decision_id, tab, rh, _safe_json(payload)),
        )
        return {"ok": True, "row_hash": rh, "tab": tab}
    except Exception as e:
        return {"ok": False, "reason": f"{e.__class__.__name__}:{e}", "tab": tab}
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
