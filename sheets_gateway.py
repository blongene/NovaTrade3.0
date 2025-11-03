# --- replace build_gateway_from_env() and GSpreadAdapter.__init__ with this ---

def _read_service_json_from_env() -> str:
    """
    Returns JSON string for Google service account, using your existing envs:
      1) GOOGLE_CREDS_JSON_PATH (path to file)
      2) GOOGLE_APPLICATION_CREDENTIALS (path to file)
      3) SVC_JSON (either a filename or raw JSON)
    """
    import json, os, io, pathlib
    # 1) explicit path to JSON
    for key in ("GOOGLE_CREDS_JSON_PATH", "GOOGLE_APPLICATION_CREDENTIALS"):
        p = os.getenv(key, "").strip()
        if p and pathlib.Path(p).exists():
            return pathlib.Path(p).read_text(encoding="utf-8")

    # 2) SVC_JSON: support either a path-like or raw JSON
    svc = os.getenv("SVC_JSON", "").strip()
    if svc:
        if pathlib.Path(svc).exists():
            return pathlib.Path(svc).read_text(encoding="utf-8")
        # otherwise assume raw json
        try:
            json.loads(svc)  # validate
            return svc
        except Exception:
            pass

    raise RuntimeError("No Google service JSON found in GOOGLE_CREDS_JSON_PATH / GOOGLE_APPLICATION_CREDENTIALS / SVC_JSON")

class GSpreadAdapter:
    def __init__(self):
        # Use your SHEET_URL directly
        sheet_url = os.getenv("SHEET_URL", "").strip()
        if not sheet_url:
            raise RuntimeError("SHEET_URL not set")

        # Build client from JSON (raw string)
        import json
        if gspread is None:
            raise RuntimeError("gspread not installed")
        raw = _read_service_json_from_env()
        data = json.loads(raw)
        self.gc = gspread.service_account_from_dict(data)  # type: ignore
        self.sh = self.gc.open_by_url(sheet_url)

def build_gateway_from_env() -> SheetsGateway:
    # TTL / budgets from your existing knobs
    ttl = int(os.getenv("SHEET_CACHE_TTL_SEC", os.getenv("ACP_TTL_READ_SEC", "600")))
    flush_iv = int(os.getenv("MODULE_PAUSE_SEC", "30"))
    # prefer explicit *READS_PER_MIN / *WRITES_PER_MIN; fall back to *_MAX_* for compatibility
    read_b = int(os.getenv("SHEETS_READS_PER_MIN", os.getenv("SHEETS_READ_MAX_PER_MIN", "30")))
    write_b = int(os.getenv("SHEETS_WRITES_PER_MIN", os.getenv("SHEETS_WRITE_MAX_PER_MIN", "20")))

    try:
        adapter = GSpreadAdapter()
    except Exception as e:
        adapter = NoopAdapter()

    return SheetsGateway(adapter,
                         ttl_seconds=ttl,
                         flush_interval=flush_iv,
                         max_batch=int(os.getenv("ACP_MAX_WRITES", "50")),
                         read_budget_per_min=read_b,
                         write_budget_per_min=write_b)
