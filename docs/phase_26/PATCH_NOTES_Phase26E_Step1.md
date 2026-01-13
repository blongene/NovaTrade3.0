# Phase 26E — Step 1 Patch Pack (Commands table is canonical outbox)

## What this patch does
- Adds a schema-adaptive Commands Outbox writer that can insert into your existing `commands` table even if its columns differ from older assumptions (e.g., no `payload` column).
- Adds a Phase 26E worker that:
  1) reads APPROVE decisions from `alpha_approvals`
  2) uses the latest matching row in `alpha_translations`
  3) enqueues a DRYRUN command into `commands` (canonical outbox)
  4) records linkage in `alpha_dryrun_orderplace_outbox` for audit/dedupe

## Files added
- `alpha_command_outbox.py`
- `alpha_phase26e_enqueue.py`

## Patch snippet
- `alpha_phase26_tick.PATCH_SNIPPET.txt` (paste into your `alpha_phase26_tick.py`)

## Config (PHASE26_DELTA_JSON)
Recommended minimal:
```json
{
  "phase26": {
    "enabled": 1,
    "mode": "dryrun_exec",
    "alpha": {
      "execution_enabled": 1,
      "allow_dryrun": 1,
      "require_human_approval": 1
    }
  }
}
```

## Smoke tests
1) Run tick:
   `python alpha_phase26_tick.py`

2) Verify outbox inserts:
   `python -c "from alpha_command_outbox import debug_dump_latest_commands; debug_dump_latest_commands(10)"`

3) Verify audit rail:
   `psql "$DB_URL" -c "SELECT ts, proposal_id, translation_id, cmd_id, intent_hash FROM alpha_dryrun_orderplace_outbox ORDER BY ts DESC LIMIT 10;"`
