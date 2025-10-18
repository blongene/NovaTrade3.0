# nova_trigger_watcher.py — reads NovaTrigger!A1 and routes manual commands
import os, time, random, gspread
from oauth2client.service_account import ServiceAccountCredentials
from nova_trigger import route_manual

TAB    = os.getenv("NOVA_TRIGGER_TAB","NovaTrigger")
SHEET  = os.getenv("SHEET_URL")
JIT_MIN = float(os.getenv("NOVA_TRIGGER_JITTER_MIN_S","0.3"))
JIT_MAX = float(os.getenv("NOVA_TRIGGER_JITTER_MAX_S","1.2"))

def _open():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds).open_by_url(SHEET)

def check_nova_trigger():
    print("▶ Nova trigger check …")
    time.sleep(random.uniform(JIT_MIN,JIT_MAX))
    sh = _open()
    ws = sh.worksheet(TAB)

    raw = (ws.acell("A1").value or "").strip()
    if not raw:
        print(f"ℹ️ {TAB} empty; no trigger.")
        return

    # Route manual commands only when line starts with MANUAL_REBUY
    if raw.upper().startswith("MANUAL_REBUY"):
        out = route_manual(raw)
        print(f"✅ Manual routed: policy_ok={out['decision'].get('ok')} enq={out['enqueue'].get('ok')}")
        # clear after handling
        ws.update_acell("A1", "")
        return

    # Keep other values for your internal flows (e.g., SOS/FYI); just clear after a ping
    ws.update_acell("A1", "")
    print(f"ℹ️ Cleared non-manual trigger: {raw}")
