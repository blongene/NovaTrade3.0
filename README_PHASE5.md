# NovaTrade 3.0 — Phase 5 Drop-In Pack
**Vault Intelligence & Policy Engine (“Ash’s Reckoning”)**

## Files
- `vault_intelligence.py` — builds the **Vault Intelligence** sheet.
- `policy_engine.py` — loads `policy.yaml`, validates trade intents, logs decisions to **Policy_Log**.
- `rebuy_driver.py` — scans Vault Intelligence, asks Policy Engine, enqueues HMAC-signed ops (dry-run by default).
- `policy.yaml` — human-editable rulebook.

## Required env
```
SHEET_URL=...
VAULT_INTELLIGENCE_WS="Vault Intelligence"

# Policy
POLICY_FILE=policy.yaml
POLICY_LOG_WS=Policy_Log

# Rebuy
REBUY_MODE=dryrun
OUTBOX_SECRET=...
OPS_ENQUEUE_URL=https://<your-bus>/ops/enqueue
MIN_QUOTE_RESERVE_USD=10
```

## Wiring
1. Ensure your service account key is available as `sentiment-log-service.json`.
2. Schedule:
   - `run_vault_intelligence()` hourly
   - `run_rebuy_driver()` every 3 hours
3. Flip `REBUY_MODE=live` only after Policy_Log looks good.
4. `/ops/enqueue` must verify `HMAC_SHA256(OUTBOX_SECRET, json.dumps(payload, sort_keys=True))`.
