# Phase 26A - Step 5 (Command Previews, NO enqueue)

## What it does
- Reads latest approvals per proposal_id from alpha_approvals
- Filters to decision='APPROVE'
- Joins to alpha_proposals
- Builds a dryrun intent preview:
  - WOULD_WATCH => type 'note'
  - WOULD_TRADE => type 'order.place' (dryrun)
- Mirrors results to the sheet tab Alpha_CommandPreviews

## What it does NOT do
- Does NOT enqueue anything to commands
- Does NOT contact Edge / execute trades

## Enable (env)
- PREVIEW_ENABLED=1
- ALPHA_PREVIEW_PROPOSALS_ENABLED=1
- optional: ALPHA_COMMAND_PREVIEWS_MIRROR_ENABLED=1 (default on)

## Sheet tab config (preferred via DB_READ_JSON)
DB_READ_JSON.phase25.alpha.sources.command_previews_tab = "Alpha_CommandPreviews"

(Env override available: ALPHA_COMMAND_PREVIEWS_SHEET_TAB)

## Manual test
python -m py_compile alpha_command_previews_mirror.py
python alpha_command_previews_mirror.py

## Scheduler
python apply_phase26a_step5_patch.py

Default cadence: every 15 minutes (override with ALPHA_COMMAND_PREVIEWS_MIRROR_MINUTES)
