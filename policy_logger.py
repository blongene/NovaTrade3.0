
# policy_logger.py — Policy & Decisions log to Google Sheets
# Drop-in for NovaTrade (Bus). Robust creds/env discovery + auto-worksheet bootstrap.
import os, gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.getenv("SHEET_URL")
POLICY_LOG_WS = os.getenv("POLICY_LOG_WS", "Policy_Log")

def _creds_path():
    # Honor any of the user's existing variables
    for k in ("GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CREDS_JSON_PATH", "SVC_JSON"):
        v = os.getenv(k)
        if v and os.path.exists(v):
            return v
    # Reasonable fallbacks used in this project
    for v in ("/etc/secrets/sentiment-log-service.json", "sentiment-log-service.json"):
        if os.path.exists(v):
            return v
    raise FileNotFoundError("Google creds JSON not found. Set GOOGLE_APPLICATION_CREDENTIALS.")

def _open():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    svc = _creds_path()
    creds = ServiceAccountCredentials.from_json_keyfile_name(svc, scope)
    gc = gspread.authorize(creds)
    return gc.open_by_url(SHEET_URL)

def log_policy_decision(intent: dict, decision: str, reasons: list[str]):
    """
    Write a single policy decision row to the configured worksheet.
    decision: 'pass' | 'block' | 'hold'
    """
    if not SHEET_URL:
        print("[policy_log] SHEET_URL is not set; skipping log.", flush=True)
        return

    try:
        sh = _open()
        try:
            ws = sh.worksheet(POLICY_LOG_WS)
        except Exception:
            # Bootstrap the worksheet if missing
            ws = sh.add_worksheet(title=POLICY_LOG_WS, rows=2000, cols=12)
            ws.append_row(["Timestamp","Intent_ID","Action","Venue","Symbol","Quote",
                           "Amount_USD","Decision","Reasons","Source"], value_input_option="USER_ENTERED")
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        iid = str(intent.get("id") or intent.get("client_id") or "")
        act = (intent.get("action") or intent.get("side") or "").upper()
        ven = (intent.get("venue") or "").upper()
        sym = intent.get("symbol") or intent.get("product_id") or ""
        qte = (intent.get("quote") or intent.get("to") or "").upper()
        amt = intent.get("amount_usd") or intent.get("amount_quote") or intent.get("quote_amount") or ""
        src = intent.get("source") or ""
        ws.append_row([ts, iid, act, ven, sym, qte, amt, decision, "; ".join(reasons or []), src],
                      value_input_option="USER_ENTERED")
    except Exception as e:
        # Never block execution on logging
        print(f"[policy_log] write error: {e}", flush=True)
