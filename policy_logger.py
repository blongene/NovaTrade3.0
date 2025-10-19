# policy_logger.py
import os, gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.getenv("SHEET_URL")
POLICY_LOG_WS = os.getenv("POLICY_LOG_WS", "Policy_Log")

def _open():
    scope=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    svc = os.getenv("GOOGLE_APPLICATION_CREDENTIALS","/etc/secrets/sentiment-log-service.json")
    creds = ServiceAccountCredentials.from_json_keyfile_name(svc, scope)
    return gspread.authorize(creds).open_by_url(SHEET_URL)

def log_policy_decision(intent: dict, decision: str, reasons: list[str]):
    """
    decision: 'pass' | 'block' | 'hold'
    """
    sh = _open()
    try:
        try:
            ws = sh.worksheet(POLICY_LOG_WS)
        except Exception:
            ws = sh.add_worksheet(title=POLICY_LOG_WS, rows=2000, cols=12)
            ws.append_row(["Timestamp","Intent_ID","Action","Venue","Symbol","Quote",
                           "Amount_USD","Decision","Reasons","Source"])
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        iid = str(intent.get("id") or intent.get("client_id") or "")
        act = (intent.get("action") or intent.get("side") or "").upper()
        ven = (intent.get("venue") or "").upper()
        sym = intent.get("symbol") or intent.get("product_id") or ""
        qte = (intent.get("quote") or intent.get("to") or "").upper()
        amt = intent.get("amount_usd") or intent.get("amount_quote") or intent.get("quote_amount") or ""
        src = intent.get("source") or ""
        ws.append_row([ts, iid, act, ven, sym, qte, amt, decision, "; ".join(reasons or []), src])
    except Exception as e:
        # keep the trade path resilient; just print
        print(f"[policy_log] write error: {e}", flush=True)
