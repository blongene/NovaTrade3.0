NovaTrade 3.0 — Phase 19 Completion Report & Phase 20 Work Plan

Date: December 2025
Author: Council / NovaTrade Engineering
Status: Approved

1. Phase 19 — Completion Summary

Phase 19 focused on hardening the trade pipeline, validating telemetry architecture, improving reliability, and unifying the Bus ↔ Edge audit trail. All required objectives have been met.

1.1 Manual Rebuy Governance Path (19.1 Complete)

The manual rebuy pipeline now operates through the fully-governed trade stack:

Trigger → Intent Parse → trade_guard → policy_logger  
        → ops_sign_and_enqueue → Outbox → Edge Execution  
        → Receipt → Policy_Log + Trade_Log

Improvements delivered

Decision IDs now link Policy_Log → Trade_Log → Receipts.

Patched intents are logged and persisted.

Telegram summary layer repaired.

Pipeline correctness validated in dry-run.

1.2 Telemetry Health Architecture (19.2 Complete)

Telemetry ingestion and health reporting have been unified.

Key changes

/api/telemetry/health added with fallback to legacy snapshot.

telemetry_routes and wsgi unified behind a shared state.

RUN_MODE corrected on the Edge to enable telemetry loops.

Edge → Bus telemetry now passing HMAC validation.

Health endpoint now reflects real venue + heartbeat state.

1.3 System State Post-19
Component	Status
Manual Rebuy Pipeline	Stable
Policy Engine	Deterministic, auditable
Outbox → Edge Exec	Verified
Telemetry Health	Operational
Unified Snapshot	Valid, compact
Logs	Consistent, deduped
Operator Signals	Reliable
2. Phase 19 — Recommended Tuning (Non-Blocking)

The following optimizations are recommended to improve reliability and reduce noise before full autonomy in Phase 20.

2.1 Stalled Asset Detector

Current behavior: builds anomaly rows from 100+ wallet entries, often producing excessive output.

Recommended constraints

Limit to 1 anomaly per token per run.

Require wallet row age ≥ 30 minutes.

Prevent duplicate anomaly types per token via stable key.

Daily cap:

STALL_MAX_ANOMALIES_PER_DAY = 20

2.2 Stalled Autotrader (currently “Shadow Mode”)
Recommended constraints
MAX_STALLED_TRADES_PER_RUN  = 1
MAX_STALLED_TRADES_PER_DAY  = 3
MAX_STALLED_USD_PER_TRADE   = 10
COOLDOWN_HOURS              = 6

Recommended filters

Only trade anomalies older than 1 hour.

Only trade anomalies not previously acted upon (decision ID lookup).

Skip if telemetry or liquidity checks fail.

2.3 Wallet Monitor Compaction

Wallet monitor output grows rapidly.

Recommended:

Only write a row when values change by >0.5%.

Optionally suppress rows unless any venue balance changed.

2.4 Unified Snapshot Streamlining

Snapshot table is healthy but should maintain:

Only latest row per (venue, asset).

Compact multi-venue free-balance data where possible.

3. Phase 20 — Autonomy Activation Plan

Phase 20 transitions NovaTrade from a tool-assisted system into a controlled autonomy framework.

3.1 Stalled Autotrader: Shadow → Governed Live Mode

Add environment switch:

STALL_AUTOTRADER_MODE = shadow | live

When live:

Pipeline becomes:

anomaly → patched_intent → trade_guard → policy_logger  
        → ops_sign_and_enqueue → Edge → Receipt  
        → logs


Use the same guard + policy flow validated in Phase 19.

Required safety limits

Maximum 1 stalled trade per run.

Maximum 3 per day.

Max $10 per-trade.

Skip entirely if telemetry health fails.

3.2 Rotation Signal → Trade Integration

The rotation subsystem must transition from sheet-driven logging to pipeline-driven intent execution.

Required updates

Extend rotation_executor to produce structured intents.

New source flag:

source = "rotation_auto"


Apply same venue routing and policy governance used for manual rebuys.

Clip trade amounts using policy budget logic.

3.3 Portfolio Health Checks (Pre-trade Conditions)

Before any autonomous trade:

if telemetry.ok == False → block
if free_cash_ratio < 3% → block
if max_drawdown > 10% → block
if overtrading_flag == True → block


Health checks become first-class citizens before routing intents.

3.4 Council Oversight Layer

Introduce council-level control surfaces:

Endpoints

/api/council/trade_queue

/api/council/disable_autonomy

/api/council/enable_autonomy

Operator-facing notifications

“Rotation Autonomy Active”

“Stalled Autotrader Fired: BTC @ $10.99”

“Autonomy paused due to liquidity threshold”

3.5 Multi-Module Autonomous Strategy Layer

After signals prove stable:

Memory-aware rebuy intents

Undersized rebuy intents

Sentiment-triggered rebuy intents

Multi-signal consensus trading

All modules will ultimately produce standardized intents:

signal → intent → trade_guard → policy_logger → outbox → Edge

4. Conclusion

Phase 19 delivers a stable, auditable, and policy-governed foundation:

Trade path validated end-to-end

Telemetry link operational

Decision logging consistent

Edge/BUS communication secure

Operator surfaces restored

NovaTrade is ready to enter Phase 20, where autonomy expands from manual triggers to controlled, policy-driven execution.

Appendix A — Environment Variables Introduced or Confirmed
RUN_MODE=worker
STALL_AUTOTRADER_MODE=shadow (default)
TELEMETRY_SECRET=<shared>
OUTBOX_SECRET=<shared>
EDGE_MODE=dryrun|live

Appendix B — Artifacts Updated in Phase 19

trade_guard.py

policy_logger.py

nova_trigger.py

telemetry_routes.py

wsgi.py

utils.py (Telegram dedupe fix)
