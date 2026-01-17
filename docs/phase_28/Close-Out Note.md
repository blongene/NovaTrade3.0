Phase 28 Close-Out Note (for SD / Handoff / Council Ledger)

Phase: 28
Window: ~2026-01-17 (24h observation)
Mode: dryrun_exec (approval-gated, $10 cap, max 1 command/cycle, cooldown enforced)
Primary venues: Coinbase + BinanceUS (Kraken present in balances)

What Phase 28 was meant to prove

Command Bus enqueue → lease → Edge pull → execute (dryrun) → ACK → receipt normalization

Non-trade intents work end-to-end (BALANCE_SNAPSHOT, NOTE)

Idempotency is real (no poisoned reuse, no duplicates)

Telegram operator surface is calm (no summary/digest spam)

No stuck leases, no runaway queues

What we observed (results)

✅ Queue stability: queued=0, leased=0 over 24h
✅ Execution closure: done=18 with normalized receipts
✅ Non-trade intent success: BALANCE_SNAPSHOT executed and ACKed ok=true, multi-venue balances returned
✅ Trade dryrun success: Coinbase + BinanceUS dryrun trades executed, ACKed done, receipts status=dryrun
✅ Idempotency fixed at source: Re-enqueue with same idempotency_key returned same id/hash (cmd 171)
✅ Audit clarity: cmd 162 was superseded via NOTE; cmd 163 identified as same legacy pre-fix artifact

Known artifacts (non-blocking)

cmd 162 + cmd 163: early Phase 28 attempts held due to legacy Edge validator requiring venue/symbol. These occurred before the BALANCE_SNAPSHOT fix and are not regressions.

Polish completed

Edge updated to treat BALANCE_SNAPSHOT as non-symbol command type.

Bus /ops/enqueue updated to dedupe by idempotency_key (prevents poison reuse).

Telegram digest duplication identified at telemetry_digest.py and patched with cross-process daily gating.

Phase 28 Exit Criteria — Status

✅ PASSED
Phase 28 is formally closed and safe to advance.
