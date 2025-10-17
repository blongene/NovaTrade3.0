# daily_scheduler.py — in-process daily scheduler (no cron) with once-per-day guard
import os, sqlite3, threading, time, datetime
from typing import Optional

DB = os.getenv("BUS_TELEMETRY_DB", "bus_telemetry.db")
UTC_HOUR = int(os.getenv("HEALTH_UTC_HOUR", "13"))   # 13:00 UTC = 09:00 ET (DST)
UTC_MIN  = int(os.getenv("HEALTH_UTC_MIN",  "0"))

def _conn():
    con = sqlite3.connect(DB, isolation_level=None, timeout=10)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("CREATE TABLE IF NOT EXISTS scheduler_runs (day TEXT PRIMARY KEY, ran_at INTEGER)")
    return con

def _already_ran_today(con, day_str: str) -> bool:
    cur = con.execute("SELECT 1 FROM scheduler_runs WHERE day=?", (day_str,))
    return cur.fetchone() is not None

def _mark_ran(con, day_str: str):
    con.execute("INSERT OR REPLACE INTO scheduler_runs(day, ran_at) VALUES(?, ?)",
                (day_str, int(time.time())))

def _seconds_until_target(now_utc: datetime.datetime) -> int:
    target = now_utc.replace(hour=UTC_HOUR, minute=UTC_MIN, second=0, microsecond=0)
    if now_utc >= target:
        target = target + datetime.timedelta(days=1)
    return int((target - now_utc).total_seconds())

def run_daily(task_func):
    """Spawn a background thread that runs task_func once per day at UTC_HOUR:UTC_MIN."""
    def _loop():
        while True:
            try:
                now = datetime.datetime.utcnow()
                day = now.strftime("%Y-%m-%d")
                wait_s = _seconds_until_target(now)
                time.sleep(max(5, min(wait_s, 24*3600)))
                # At trigger, re-evaluate day & guard
                now2 = datetime.datetime.utcnow()
                day2 = now2.strftime("%Y-%m-%d")
                con = _conn()
                if not _already_ran_today(con, day2):
                    try:
                        task_func()
                    finally:
                        _mark_ran(con, day2)
                con.close()
                # small buffer after run
                time.sleep(10)
            except Exception:
                time.sleep(60)  # backoff on unexpected errors

    t = threading.Thread(target=_loop, name="daily-health-scheduler", daemon=True)
    t.start()
    return t
