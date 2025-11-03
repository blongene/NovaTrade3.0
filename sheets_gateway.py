
# sheets_gateway.py
# Lightweight Sheets cache + write buffer with token-bucket budgets.
# Works with gspread; falls back to no-op adapter if gspread missing.

from __future__ import annotations
import os, time, threading
from typing import Any, Dict, List, Optional, Tuple

try:
    import gspread  # type: ignore
except Exception:  # pragma: no cover
    gspread = None

def _now() -> float:
    return time.time()

class NoopAdapter:
    def __init__(self, *args, **kwargs):
        self.ok = False
        self.err = "gspread not available or GOOGLE_SVC_JSON/ SHEET_URL not set"

    def read_values(self, a1_range: str):
        raise RuntimeError(self.err)

    def batch_update(self, updates):
        raise RuntimeError(self.err)

class GSpreadAdapter:
    def __init__(self, svc_json: str, sheet_url: str):
        if gspread is None:
            raise RuntimeError("gspread not installed")
        if not svc_json or not sheet_url:
            raise RuntimeError("Missing GOOGLE_SVC_JSON or SHEET_URL env")
        if os.path.exists(svc_json):
            self.gc = gspread.service_account(filename=svc_json)  # type: ignore
        else:
            import json
            data = json.loads(svc_json)
            self.gc = gspread.service_account_from_dict(data)  # type: ignore
        self.sh = self.gc.open_by_url(sheet_url)

    @staticmethod
    def _ws_and_range(a1: str):
        if "!" not in a1:
            raise ValueError("Range must include worksheet name, like 'Sheet1!A1:B2'")
        ws_name, rng = a1.split("!", 1)
        return ws_name, rng

    def read_values(self, a1_range: str):
        ws_name, rng = self._ws_and_range(a1_range)
        ws = self.sh.worksheet(ws_name)
        return ws.get(rng)  # type: ignore

    def batch_update(self, updates):
        # updates: [{'range': 'Sheet!A1:B2', 'values': [[...], ...]}]
        reqs = [{"range": u["range"], "values": u["values"]} for u in updates]
        body = {"valueInputOption": "USER_ENTERED", "data": reqs}
        return self.sh.batch_update(body)  # type: ignore

class TokenBucket:
    def __init__(self, capacity: int, per_seconds: int = 60):
        self.capacity = max(1, capacity)
        self.tokens = capacity
        self.per = per_seconds
        self.updated = _now()
        self.lock = threading.Lock()

    def consume(self, n: int) -> bool:
        with self.lock:
            now = _now()
            elapsed = now - self.updated
            if elapsed >= self.per:
                cycles = int(elapsed // self.per)
                self.tokens = min(self.capacity, self.tokens + cycles * self.capacity)
                self.updated += cycles * self.per
            if self.tokens >= n:
                self.tokens -= n
                return True
            return False

    def snapshot(self):
        return {"capacity": self.capacity, "tokens": self.tokens, "per_seconds": self.per}

class SheetsGateway:
    def __init__(self,
                 adapter: Any,
                 ttl_seconds: int = 600,
                 flush_interval: int = 30,
                 max_batch: int = 50,
                 read_budget_per_min: int = 50,
                 write_budget_per_min: int = 30):
        self.adapter = adapter
        self.ttl = ttl_seconds
        self.flush_interval = flush_interval
        self.max_batch = max_batch
        self.read_bucket = TokenBucket(read_budget_per_min, 60)
        self.write_bucket = TokenBucket(write_budget_per_min, 60)
        self.cache: Dict[str, Tuple[float, Any]] = {}
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush: Optional[float] = None
        self.lock = threading.Lock()

    def read(self, a1_range: str):
        now = _now()
        with self.lock:
            hit = self.cache.get(a1_range)
            if hit and (now - hit[0] <= self.ttl):
                return hit[1]
        if not self.read_bucket.consume(1):
            with self.lock:
                if hit:
                    return hit[1]
            raise RuntimeError("Sheets read budget exceeded")
        values = self.adapter.read_values(a1_range)
        with self.lock:
            self.cache[a1_range] = (now, values)
        return values

    def enqueue_write(self, a1_range: str, values):
        with self.lock:
            self.buffer.append({"range": a1_range, "values": values})

    def flush(self):
        with self.lock:
            if not self.buffer:
                self.last_flush = _now()
                return {"ok": True, "flushed": 0, "left": 0, "budget": self.write_bucket.snapshot()}
            batch = self.buffer[: self.max_batch]
            del self.buffer[: self.max_batch]
        need = len(batch)
        if not self.write_bucket.consume(max(1, need)):
            with self.lock:
                self.buffer = batch + self.buffer
            return {"ok": False, "error": "Sheets write budget exceeded", "queued": len(self.buffer),
                    "budget": self.write_bucket.snapshot()}
        res = self.adapter.batch_update(batch)
        self.last_flush = _now()
        return {"ok": True, "flushed": len(batch), "queued": self.queue_depth(), "res": res,
                "budget": self.write_bucket.snapshot()}

    def queue_depth(self) -> int:
        with self.lock:
            return len(self.buffer)

    def cache_size(self) -> int:
        with self.lock:
            return len(self.cache)

    def health(self):
        return {
            "cache_size": self.cache_size(),
            "queued": self.queue_depth(),
            "last_flush": self.last_flush,
            "read_budget": self.read_bucket.snapshot(),
            "write_budget": self.write_bucket.snapshot(),
            "ttl_seconds": self.ttl,
            "flush_interval": self.flush_interval,
            "max_batch": self.max_batch,
        }

def build_gateway_from_env() -> SheetsGateway:
    ttl = int(os.getenv("SHEETS_TTL_SECONDS", "600"))
    flush_iv = int(os.getenv("SHEETS_FLUSH_INTERVAL", "30"))
    max_batch = int(os.getenv("SHEETS_MAX_BATCH", "50"))
    read_b = int(os.getenv("SHEETS_READ_PER_MIN", "50"))
    write_b = int(os.getenv("SHEETS_WRITE_PER_MIN", "30"))
    svc = os.getenv("GOOGLE_SVC_JSON", "")
    url = os.getenv("SHEET_URL", "")
    try:
        adapter = GSpreadAdapter(svc, url)
    except Exception:
        adapter = NoopAdapter()
    return SheetsGateway(adapter, ttl, flush_iv, max_batch, read_b, write_b)
