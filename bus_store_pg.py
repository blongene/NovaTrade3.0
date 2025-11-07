# bus_store_pg.py — 7C durable outbox (Postgres -> fallback SQLite)
import os, json, time, hashlib, hmac, sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None

DB_URL = os.getenv("DB_URL", "")  # e.g., postgres://user:pass@host:5432/dbname
OUTBOX_LEASE_SECONDS = int(os.getenv("OUTBOX_LEASE_SECONDS", "60"))
OUTBOX_MAX_ATTEMPTS  = int(os.getenv("OUTBOX_MAX_ATTEMPTS", "5"))

def _intent_hash(payload: Dict[str, Any]) -> str:
    # Stable, HMAC-safe hash for idempotency
    msg = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(msg).hexdigest()

# ------------------- Postgres Impl -------------------
class PGStore:
    def __init__(self, url: str):
        if not psycopg2:
            raise RuntimeError("psycopg2 not available")
        self.url = url

    @contextmanager
    def cx(self):
        conn = psycopg2.connect(self.url, connect_timeout=5, application_name="novatrade-bus")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def enqueue(self, agent_id: str, intent: Dict[str, Any], dedup_ttl_seconds: int = 900) -> Dict[str, Any]:
        h = _intent_hash(intent)
        with self.cx() as c:
            cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Try insert; on conflict (same intent_hash), return existing row (idempotent)
            cur.execute("""
                insert into commands(agent_id, intent, intent_hash, dedup_ttl_seconds)
                values (%s, %s::jsonb, %s, %s)
                on conflict (intent_hash) do update set
                  attempts = commands.attempts
                returning id, status
            """, (agent_id, json.dumps(intent), h, dedup_ttl_seconds))
            row = cur.fetchone()
            return {"ok": True, "id": row["id"], "status": row["status"], "hash": h}

    def lease(self, agent_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        now = datetime.utcnow()
        exp = now + timedelta(seconds=OUTBOX_LEASE_SECONDS)
        with self.cx() as c:
            cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Free up expired leases
            cur.execute("""
                update commands
                   set status='queued', leased_by=NULL, lease_at=NULL, lease_expires_at=NULL
                 where status='leased' and lease_expires_at < now()
            """)
            # Lease next batch atomically
            cur.execute("""
                update commands
                   set status='leased',
                       leased_by=%s,
                       lease_at=now(),
                       lease_expires_at=%s,
                       attempts=attempts+1
                 where id in (
                   select id from commands
                    where status='queued'
                    order by id asc
                    limit %s
                    for update skip locked
                 )
                returning id, intent
            """, (agent_id, exp, limit))
            rows = cur.fetchall() or []
            return [{"id": r["id"], "intent": r["intent"]} for r in rows]

    def done(self, cmd_id: int):
        with self.cx() as c:
            cur = c.cursor()
            cur.execute("update commands set status='done' where id=%s", (cmd_id,))

    def fail(self, cmd_id: int, reason: str):
        with self.cx() as c:
            cur = c.cursor()
            cur.execute("update commands set status='error' where id=%s", (cmd_id,))

    def save_receipt(self, agent_id: str, cmd_id: Optional[int], receipt: Dict[str, Any], ok: bool=True):
        with self.cx() as c:
            cur = c.cursor()
            cur.execute(
                "insert into receipts(agent_id, cmd_id, receipt, ok) values (%s, %s, %s::jsonb, %s)",
                (agent_id, cmd_id, json.dumps(receipt), ok)
            )

    def save_telemetry(self, agent_id: str, payload: Dict[str, Any]):
        with self.cx() as c:
            cur = c.cursor()
            cur.execute(
                "insert into telemetry(agent_id, payload) values (%s, %s::jsonb)",
                (agent_id, json.dumps(payload))
            )

    def stats(self) -> Dict[str, Any]:
        with self.cx() as c:
            cur = c.cursor()
            cur.execute("select count(*) from commands where status='queued'")
            queued = cur.fetchone()[0]
            cur.execute("select count(*) from commands where status='leased'")
            leased = cur.fetchone()[0]
            cur.execute("select count(*) from commands where status='done'")
            done = cur.fetchone()[0]
            return {"queued": queued, "leased": leased, "done": done}

# ------------------- SQLite Fallback (compat) -------------------
class SQLiteStore:
    def __init__(self, path: str = "/tmp/outbox.sqlite"):
        self.path = path
        self._init()

    def _init(self):
        with sqlite3.connect(self.path) as c:
            c.execute("""
              create table if not exists commands(
                id integer primary key autoincrement,
                created_at text default current_timestamp,
                agent_id text not null,
                intent text not null,
                intent_hash text not null unique,
                status text not null default 'queued',
                leased_by text,
                lease_at text,
                lease_expires_at text,
                attempts integer not null default 0
              )
            """)
            c.execute("""
              create table if not exists receipts(
                id integer primary key autoincrement,
                created_at text default current_timestamp,
                agent_id text not null,
                cmd_id integer,
                receipt text not null,
                ok integer not null default 1
              )
            """)
            c.commit()

    def enqueue(self, agent_id: str, intent: Dict[str, Any], dedup_ttl_seconds: int = 900) -> Dict[str, Any]:
        h = _intent_hash(intent)
        with sqlite3.connect(self.path) as c:
            cur = c.cursor()
            # try insert; ignore on conflict
            try:
                cur.execute("insert into commands(agent_id, intent, intent_hash) values(?,?,?)",
                            (agent_id, json.dumps(intent), h))
                c.commit()
                cmd_id = cur.lastrowid
                return {"ok": True, "id": cmd_id, "status": "queued", "hash": h}
            except sqlite3.IntegrityError:
                # already exists -> fetch id
                cur.execute("select id, status from commands where intent_hash=?", (h,))
                row = cur.fetchone()
                return {"ok": True, "id": row[0], "status": row[1], "hash": h}

    def lease(self, agent_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        now = datetime.utcnow()
        exp = now + timedelta(seconds=OUTBOX_LEASE_SECONDS)
        with sqlite3.connect(self.path) as c:
            cur = c.cursor()
            # free expired
            cur.execute("update commands set status='queued', leased_by=null, lease_at=null, lease_expires_at=null where status='leased' and lease_expires_at < ?", (now.isoformat(),))
            # lease
            cur.execute("select id, intent from commands where status='queued' order by id asc limit ?", (limit,))
            rows = cur.fetchall()
            out = []
            for r in rows:
                cur.execute("update commands set status='leased', leased_by=?, lease_at=?, lease_expires_at=?, attempts=attempts+1 where id=?",
                            (agent_id, now.isoformat(), exp.isoformat(), r[0]))
                out.append({"id": r[0], "intent": json.loads(r[1])})
            c.commit()
            return out

    def done(self, cmd_id: int):
        with sqlite3.connect(self.path) as c:
            c.execute("update commands set status='done' where id=?", (cmd_id,))
            c.commit()

    def fail(self, cmd_id: int, reason: str):
        with sqlite3.connect(self.path) as c:
            c.execute("update commands set status='error' where id=?", (cmd_id,))
            c.commit()

    def save_receipt(self, agent_id: str, cmd_id: Optional[int], receipt: Dict[str, Any], ok: bool=True):
        with sqlite3.connect(self.path) as c:
            c.execute("insert into receipts(agent_id, cmd_id, receipt, ok) values(?,?,?,?)",
                      (agent_id, cmd_id, json.dumps(receipt), 1 if ok else 0))
            c.commit()

    def save_telemetry(self, agent_id: str, payload: Dict[str, Any]):
        with sqlite3.connect(self.path) as c:
            c.execute("insert into telemetry(agent_id, payload) values(?,?)", (agent_id, json.dumps(payload)))
            c.commit()

    def stats(self) -> Dict[str, Any]:
        with sqlite3.connect(self.path) as c:
            cur = c.cursor()
            cur.execute("select count(*) from commands where status='queued'")
            queued = cur.fetchone()[0]
            cur.execute("select count(*) from commands where status='leased'")
            leased = cur.fetchone()[0]
            cur.execute("select count(*) from commands where status='done'")
            done = cur.fetchone()[0]
            return {"queued": queued, "leased": leased, "done": done}

# ------------------- Factory -------------------
def get_store():
    if DB_URL and psycopg2:
        return PGStore(DB_URL)
    return SQLiteStore(os.getenv("OUTBOX_SQLITE_PATH", "/tmp/outbox.sqlite"))
