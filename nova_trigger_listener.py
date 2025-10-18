# nova_trigger_listener.py — loop-based listener (optional)
import os, time, gspread
from oauth2client.service_account import ServiceAccountCredentials
from nova_trigger import route_manual

SHEET_URL = os.getenv("SHEET_URL")
TAB       = os.getenv("NOVA_TRIGGER_TAB","NovaTrigger")
POLL_SEC  = int(os.getenv("NOVA_TRIGGER_POLL_SEC","30"))

def listen_for_nova_trigger():
    print("🎯 NovaTrigger listener started...")
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    ws = client.open_by_url(SHEET_URL).worksheet(TAB)

    while True:
        try:
            raw = (ws.acell("A1").value or "").strip()
            if raw.upper().startswith("MANUAL_REBUY"):
                out = route_manual(raw)
                print(f"✅ Manual routed: policy_ok={out['decision'].get('ok')} enq={out['enqueue'].get('ok')}")
                ws.update_acell("A1","")
        except Exception as e:
            print(f"❌ NovaTrigger error: {e}")
        time.sleep(POLL_SEC)

if __name__ == "__main__":
    listen_for_nova_trigger()
