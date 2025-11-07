# council_ledger.py
# Phase 8B — Governance Ledger (Council_Voice + Ashs_Reckoning)

import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.getenv("SHEET_URL", "")

VOICE_WS = os.getenv("COUNCIL_VOICE_WS", "Council_Voice")
RECKON_WS = os.getenv("ASHS_RECKONING_WS", "Ashs_Reckoning")

def _open_sheet():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL)

def _ensure_headers(ws, headers):
    try:
        existing = ws.row_values(1)
    except Exception:
        existing = []
    if existing != headers:
        ws.clear()
        ws.append_row(headers)

def _get_ws(sh, name, headers):
    try:
        ws = sh.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=1000, cols=max(8, len(headers)+2))
    _ensure_headers(ws, headers)
    return ws

def log_council_voice(actor:str, scope:str, message:str, token:str="", ref:str=""):
    """
    actor: 'Brett', 'Nova', 'Ash', 'Orion', 'Edge', etc.
    scope: 'Policy', 'Rotation', 'Rebuy', 'Rebalance', 'Telemetry', 'General'
    message: free-form annotation/decision/rationale
    token: optional token symbol
    ref: optional reference id (cmd_id, receipt_id, txid, policy_rule, etc.)
    """
    sh = _open_sheet()
    ws = _get_ws(sh, VOICE_WS, [
        "Timestamp","Actor","Scope","Message","Token","Ref"
    ])
    ws.append_row([
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        actor, scope, message, token, ref
    ], value_input_option="USER_ENTERED")

def log_reckoning(event:str, ok:bool, reason:str="", token:str="", action:str="",
                  amount_usd="", venue:str="", quote:str="", patched:str="", ref:str=""):
    """
    event: 'policy_check', 'override', 'violation', 'enqueue', 'receipt', etc.
    ok: True/False outcome
    reason: policy reason / override rationale
    patched: optional json string of patched intent
    """
    sh = _open_sheet()
    ws = _get_ws(sh, RECKON_WS, [
        "Timestamp","Event","OK","Reason","Token","Action","Amount_USD",
        "Venue","Quote","Patched","Ref"
    ])
    ws.append_row([
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        event,
        "TRUE" if ok else "FALSE",
        reason, token, action, amount_usd, venue, quote, patched, ref
    ], value_input_option="USER_ENTERED")
