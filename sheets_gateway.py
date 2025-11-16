from __future__ import annotations

import os
import json
import time
import pathlib
from typing import Any, List, Tuple


def _read_service_json_from_env() -> str:
    """
    Returns JSON string for Google service account, using your existing envs:
      1) GOOGLE_CREDS_JSON_PATH (path to file)
      2) GOOGLE_APPLICATION_CREDENTIALS (path to file)
      3) SVC_JSON (either a filename or raw JSON)
    """
    # 1) explicit path to JSON
    for key in ("GOOGLE_CREDS_JSON_PATH", "GOOGLE_APPLICATION_CREDENTIALS"):
        p = os.getenv(key, "").strip()
        if p and pathlib.Path(p).exists():
            return pathlib.Path(p).read_text(encoding="utf-8")

    # 2) SVC_JSON (filename or raw JSON)
    svc = os.getenv("SVC_JSON", "").strip()
    if svc:
        p = pathlib.Path(svc)
        if p.exists():
            return p.read_text(encoding="utf-8")
        # otherwise assume it's raw JSON
        return svc

    return ""


class NoopAdapter:
    """Adapter used when gspread / creds are unavailable."""
    def read(self, a1: str) -> list[list[Any]]:
        return []

    def append(self, range_a1: str, values: list[list[Any]]) -> dict:
        return {"ok": False, "error": "NoopAdapter: Sheets not configured"}


class GSpreadAdapter:
    """
    Thin wrapper around gspread that can append rows and read ranges
    from the single SHEET_URL workbook.
    """

    def __init__(self):
        raw = _read_service_json_from_env()
        if not raw:
            raise RuntimeError(
                "service JSON not found in GOOGLE_CREDS_JSON_PATH / "
                "GOOGLE_APPLICATION_CREDENTIALS / SVC_JSON"
            )

        try:
            import gspread  # type: ignore
            from google.oauth2.service_account import Credentials  # type: ignore
        except Exception as e:
            raise RuntimeError("gspread or google-auth not installed") from e

        data = json.loads(raw)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(data, scopes=scopes)
        client = gspread.authorize(creds)

        sheet_url = os.getenv("SHEET_URL", "").strip()
        if not sheet_url:
            raise RuntimeError("SHEET_URL not set in environment")

        self._sh = client.open_by_url(sheet_url)

    def _split_a1(self, a1: str) -> Tuple[Any, str]:
        if "!" not in a1:
            raise ValueError(f"range must include worksheet name, got {a1!r}")
        ws_name, rng = a1.split("!", 1)
        ws = self._sh.worksheet(ws_name)
        return ws, rng

    def read(self, a1: str) -> list[list[Any]]:
        ws, rng = self._split_a1(a1)
        return ws.get(rng)

    def append(self, range_a1: str, values: list[list[Any]]) -> dict:
        ws, rng = self._split_a1(range_a1)
        # treat rng as starting cell / table range
        ws.append_rows(values, table_range=rng)
        return {"ok": True, "written": len(values)}
    

class SheetsGateway:
    """
    Simple batching + quota-aware wrapper around a Sheets adapter.

    It batches writes and flushes them via adapter.append().
    """

    def __init__(
        self,
        adapter: Any,
        ttl_seconds: int = 300,
        flush_interval: int = 30,
        max_batch: int = 50,
        read_budget_per_min: int = 30,
        write_budget_per_min: int = 20,
    ):
        self.adapter = adapter
        self.ttl_seconds = ttl_seconds
        self.flush_interval = flush_interval
        self.max_batch = max_batch
        self.read_budget_per_min = read_budget_per_min
        self.write_budget_per_min = write_budget_per_min
        self._queue: List[Tuple[str, list[list[Any]]]] = []

    # --- public API used by sheets_bp / sheets_mirror -----------------------

    def enqueue_write(self, range_a1: str, values: list[list[Any]]) -> None:
        if not values:
            return
        self._queue.append((range_a1, values))

    def queue_depth(self) -> int:
        return len(self._queue)

    def flush(self) -> dict:
        """
        Flush queued writes. Returns a dict with at least:
            {"ok": bool, "written": int, "error": str?}
        """
        if isinstance(self.adapter, NoopAdapter):
            # drain queue but report not-ok
            drained = len(self._queue)
            self._queue.clear()
            return {"ok": False, "written": 0, "error": "NoopAdapter in use", "drained": drained}

        written = 0
        errors: list[str] = []
        while self._queue:
            range_a1, values = self._queue.pop(0)
            if not values:
                continue
            # respect max_batch per append
            batch = values[: self.max_batch]
            try:
                res = self.adapter.append(range_a1, batch)
                if res.get("ok", True):
                    written += len(batch)
                else:
                    errors.append(str(res))
            except Exception as e:  # noqa: BLE001
                errors.append(str(e))

        return {
            "ok": not errors,
            "written": written,
            "errors": errors,
        }

    def health(self) -> dict:
        return {
            "ok": not isinstance(self.adapter, NoopAdapter),
            "queue_depth": len(self._queue),
            "flush_interval": self.flush_interval,
        }


def build_gateway_from_env() -> SheetsGateway:
    """
    Factory used by sheets_bp / sheets_mirror to construct the global
    SheetsGateway based on env variables.
    """
    ttl = int(os.getenv("ACP_TTL_READ_SEC", "300"))
    flush_iv = int(os.getenv("SHEETS_FLUSH_INTERVAL_SEC", "30"))
    read_b = int(os.getenv("SHEETS_READS_PER_MIN", os.getenv("SHEETS_READ_MAX_PER_MIN", "30")))
    write_b = int(os.getenv("SHEETS_WRITES_PER_MIN", os.getenv("SHEETS_WRITE_MAX_PER_MIN", "20")))

    try:
        adapter = GSpreadAdapter()
    except Exception:
        adapter = NoopAdapter()

    return SheetsGateway(
        adapter,
        ttl_seconds=ttl,
        flush_interval=flush_iv,
        max_batch=int(os.getenv("ACP_MAX_WRITES", "50")),
        read_budget_per_min=read_b,
        write_budget_per_min=write_b,
    )
