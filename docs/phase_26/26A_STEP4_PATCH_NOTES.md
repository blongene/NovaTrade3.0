# Phase 26A — Step 4 (Approvals Surface Bridge)

This patch adds a **governance-only** bridge from **Alpha Proposals** → **Alpha Approvals**.

## What it does

- Reads today's UTC rows from Postgres `alpha_proposals`
- Mirrors them into Google Sheet tab `Alpha_Approvals`
- Leaves the `decision` column blank so a human can fill:
  - APPROVE / DENY / HOLD

Then, the existing `alpha_approvals_sync.py` ingests any filled decisions back into Postgres `alpha_approvals`.

## Why this matters

- We separate:
  - **system output** (`Alpha_Proposals`) from
  - **human governance input** (`Alpha_Approvals`)
- No trading risk: this patch never enqueues commands.
- It creates the "track" for Phase 27 boring dry-run governance.

## Routing (env-slot friendly)

Preferred:
- `DB_READ_JSON.phase25.alpha.sources.approvals_tab`  → default: `Alpha_Approvals`

Fallback:
- `ALPHA_APPROVALS_SHEET_TAB`

## Test Commands (Render Web Shell)

```bash
python -m py_compile alpha_approvals_requests_mirror.py
python alpha_approvals_requests_mirror.py
```

Expected:
- Alpha_Approvals gets header + today's candidates (or header only if none).

## Optional Scheduler install (main.py)

If your Bus uses `main.py` schedules, run:

```bash
python apply_phase26a_step4_patch.py
```

This inserts:
- Alpha Approval Requests Mirror every 15 minutes

(If you already schedule the Phase 26 tick elsewhere, you can skip this.)
