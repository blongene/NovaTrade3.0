# Phase 26A — Step 3 Patch Pack

This patch does **two** things (Bus-only, preview-only):

1) **JSON-driven routing (env-slot friendly)**
   - `alpha_proposals_mirror.py` now prefers:
     - `DB_READ_JSON.phase25.alpha.sources.proposals_tab`
     - then falls back to legacy `ALPHA_SHEET_TAB`
     - default: `Alpha_Proposals`

2) **"Silence is intentional" row (daily snapshot)**
   - When there are **no proposals** for today's UTC day, it writes a single row:
     - `action = SILENCE_INTENTIONAL`
     - `primary_blocker = NO_PROPOSALS`
   - Controlled by:
     - `DB_READ_JSON.phase25.alpha.mirror.silence_row` (default: ON)

### Suggested DB_READ_JSON additions

Add these keys (no new env vars required):

```json
"phase25": {
  "alpha": {
    "sources": {
      "proposals_tab": "Alpha_Proposals"
    },
    "mirror": {
      "enabled": 1,
      "silence_row": 1,
      "mode": "daily_snapshot"
    }
  }
}
```

### Testing (Render Web Shell)

```bash
python -m py_compile alpha_proposals_mirror.py
python alpha_proposals_mirror.py
```

Then confirm the sheet `Alpha_Proposals` shows:
- proposal rows for today, OR
- one `SILENCE_INTENTIONAL` row for today.
