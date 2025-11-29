# NovaTrade 3.x – Phase Roadmap & Status

_Last updated: 2025-11-29_

This document tracks **what NovaTrade is**, **how it evolved**, and **what remains** — in phase / sub-phase format.

It’s meant to be a living roadmap for the **Bus + Edge** system, not the Google Sheets layer (which will gradually become a view, not the core.)

---

## Legend

- ✅ **Done / Stable**
- 🔄 **In Progress / Polishing**
- 🧩 **Scaffolded / Partial**
- 🔜 **Planned / Not Started**

---

## Era 0 – Foundations (Nova 1.x / 2.x)

### Phase 0 – Seed Sheet (Foundational)

**Goal:** Track a small portfolio and manual rotations inside a single Google Sheet.

- ✅ Basic tabs:
  - `Rotation_Log`
  - Early “Current Positions” / “Watchlist” style sheets
- ✅ Simple Apps Script price pull & ROI math
- ✅ Manual, consistent rotation workflow (but no autonomy)

> Status: **Historical context only.** Still useful as the conceptual root, but not where logic lives anymore.

---

## Era 1 – Sheet-Native Engine (Nova 2.x)

### Phase 1 – Rotation Engine (Sheets-only)

**Goal:** Turn the sheet into a structured system instead of a raw ledger.

- ✅ Structured `Rotation_Log` with:
  - Entries, exits, ROI, holding periods
- ✅ Early “Top Targets / Watchlist” logic
- ✅ Human-driven, but systemized rotation

---

### Phase 2 – Apps Script Automation

**Goal:** Let the sheet self-maintain day-to-day routines.

- ✅ Time-based / on-edit triggers for:
  - Price refreshes
  - ROI milestones / alerts
  - Simple rebalance hints
- ✅ Apps Script helpers to keep data clean

> Status: **Still running**, but now mostly supporting / feeding NovaTrade 3.x rather than being the primary engine.

---

### Phase 3 – Python Helpers (Nova 2.5)

**Goal:** Move heavier logic off Apps Script into Python.

- ✅ Python scripts for:
  - Reading/writing Google Sheets via service account
  - Generating rotation suggestions & summaries
- ✅ Basic logging and “offline brain” outside Sheets

> Status: **Superseded** by NovaTrade 3.0 but important stepping stone.

---

## Era 2 – Vaults, Policy & Bus (Nova 3.0 “B-Series”)

### Phase 4 – Vaults & ROI Tracking

**Goal:** Upgrade simple positions into **vaults** with lifecycle & memory.

- ✅ Vault tabs + ROI tracking (per position, per vault)
- ✅ Lifecycle states:
  - Entered → Held → Rotated → Archived
- ✅ Integration with sheet-level analytics

> Status: **Core concept.** Vaults remain central to how Nova thinks about capital.

---

### Phase 5 – “Ash’s Reckoning” – Vault Intelligence & Policy Engine

**Goal:** Make the system **policy-driven**, not just threshold-based.

- ✅ `vault_intelligence.py`:
  - Gathers vault state & health
- ✅ `policy_engine.py`:
  - Liquidity floors
  - Drawdown rules
  - ROI-based unlocks / cooldowns
- ✅ Policy log for “why” decisions were made

- 🔄 **Ongoing refinement**
  - Tune thresholds (per-vault, per-token)
  - Add richer signals (telemetry, stalled assets, etc.)

---

### Phase 6 – Command Bus & Early Telemetry (6A / 6B / 6C)

#### Phase 6A – Sheets & Telemetry Stabilization

**Goal:** Harden Sheets + telemetry so they *do not* bring the system down.

- ✅ `utils.py` hardened:
  - Token buckets for Sheets reads/writes
  - Backoff & retry
  - Caching of worksheets/rows/values
- ✅ Global Telegram de-dupe:
  - Fewer noisy pings
  - Once-per-boot notices
- ✅ Cleaner boot sequence on Render

> Status: **Stable**; this is the reliability backbone.

---

#### Phase 6B – Telemetry Bus / Edge Sync

**Goal:** Structured telemetry between Edge and Bus.

- ✅ Bus endpoints:
  - `/api/telemetry/push`
  - `/api/telemetry/push_balances`
- ✅ Edge telemetry sender:
  - Aggregates **COINBASE, BINANCEUS, KRAKEN** balances
  - HMAC-signed with `TELEMETRY_SECRET`
- ✅ `_last_tel` snapshot in `wsgi.py` as canonical telemetry source

- 🔄 Telemetry Snapshot path:
  - ✅ `telemetry_mirror.py` → writes into `Wallet_Monitor`
  - ✅ `unified_snapshot.py` → builds `Unified_Snapshot` from `Wallet_Monitor`
  - 🔄 Telemetry summary / logging polish
  - 🔄 Ensure expected grid: `COINBASE`, `BINANCEUS`, `KRAKEN` × `{USD, USDC, USDT}`

---

#### Phase 6C – Dual Kill Switches

**Goal:** Give both Cloud and Edge the ability to “slam the brakes”.

- ✅ Edge:
  - `EDGE_MODE` (`dryrun` vs `live`)
  - `EDGE_HOLD` to skip execution while still polling
- 🧩 Cloud:
  - Policy can effectively “deny” new intents
  - But explicit “big red button” is not fully unified yet

- 🔜 TODO:
  - Make **both sides** explicit:
    - Cloud “NovaTrigger / Policy” kill for certain behaviors or venues
    - Edge hard brake that’s visible in logs + Telegram
  - Clear operator feedback when either side is holding.

---

### Phase 7 – Autonomy Drivers (7A / 7B / 7C)

#### Phase 7A – Extended Vault Intelligence

**Goal:** Let Vault Intel + Telemetry drive decisions automatically.

- ✅ Baseline Vault Intelligence & Policy Engine wired into Bus
- 🧩 Stalled asset detector hooks (telemetry-aware) exist but need polish
- 🔜 Stronger loops:
  - Use telemetry (balances, stalled assets) as direct inputs to policy decisions
  - Feed those decisions into command enqueue (not just sheet notations)

---

#### Phase 7B – Rebuy Driver / Rotation Engine

**Goal:** Smart, policy-safe “buy back in” behavior.

- ✅ Modules in place:
  - `rebuy_driver.py`
  - `rebuy_engine.py`
  - `rebuy_roi_aggregator.py` / `rebuy_roi_tracker.py`
- 🧩 Partial wiring:
  - Logic exists but isn’t yet the primary driver of live intents
- 🔜 TODO:
  - Connect rebuy decisions → command bus enqueue
  - Ensure every auto-action has:
    - Policy justification
    - Log / Telegram explanation

---

#### Phase 7C – Command Persistence Upgrade (DB)

**Goal:** Move from Sheets / local DB scatter to a proper backend.

- ✅ Scaffolding:
  - `bus_store_pg.py`
  - `db_schema.sql`
- 🔄 In-progress thinking:
  - Commands, receipts, telemetry, positions → SQLite/Postgres
  - Sheets become **view** / mirror, not main source of truth

---

## Era 3 – Cloud + Edge Architecture (Nova 3.0 “C-Series”)

### Phase 16 – Cloud Orchestrator Stabilization

**Goal:** Render Bus as quiet, resilient orchestrator.

- ✅ `wsgi.py` hardened:
  - HMAC verification
  - Telemetry endpoints
  - Policy context
- ✅ `gspread_guard.py` + `utils.py` for Sheets:
  - Token buckets
  - Caching
  - Backoff
- ✅ Telegram:
  - De-duped messages
  - Once-per-boot “system online” notice

- 🔄 Ongoing:
  - Continue reducing Sheets 429s
  - Ensure job staggering & cheap scheduled tasks

---

### Phase 17 – Edge Agent 3.0 (Hands of Nova)

**Goal:** One Edge Agent to execute commands on **Coinbase / BinanceUS / Kraken** (MEXC optional), with HMAC and safety rails.

- ✅ `edge_agent.py`:
  - Long-polls `/api/commands/pull`
  - Executes via venue-specific executors
  - ACKs results via `/api/commands/ack` (HMAC signed)
  - Starts balance telemetry in a background thread (`start_balance_pusher`)
- ✅ Edge env (`NovaTrade3.0_Edge_Agent.env`):
  - `EDGE_MODE`, `EDGE_HOLD`
  - `EDGE_SECRET` (shared with Bus)
  - Venue API keys
  - Telemetry secrets + intervals

- 🔄 Polish:
  - Consistent normalized receipts (so Bus can compact + audit cleanly)
  - Clear distinction between:
    - Trade/exchange errors
    - Bus/API/HMAC errors
  - Edge-side telemetry DB / local snapshots for audit + future UI

---

### Phase 18 – Telemetry Mirror & Unified Snapshot 2.0  ← **CURRENT WORKSTREAM**

**Goal:** Always know **per venue** how much “cash” (USD/USDC/USDT) you have, using the same telemetry powering policy.

- ✅ Edge → Bus telemetry pipeline:
  - `telemetry_sender.py` → `/api/telemetry/push_balances` (HMAC)
  - `_last_tel` maintained in `wsgi.py`
- ✅ Telemetry Mirror:
  - `telemetry_mirror.py`:
    - Reads `_last_tel` via `/api/telemetry/last`
    - Writes rows into `Wallet_Monitor`
- ✅ Unified Snapshot:
  - `unified_snapshot.py`:
    - Builds `Unified_Snapshot` from `Wallet_Monitor`
    - One row per `(Venue, Asset)` with equity estimates
- ✅ Wallet monitor hygiene:
  - `wallet_monitor_compactor.py` (or equivalent job):
    - Keeps `Wallet_Monitor` from unbounded growth

- 🔄 Right now:
  - Fix / clean telemetry summary logging (`info()` signature issue)
  - Confirm:
    - `Wallet_Monitor` stays within reasonable row bounds
    - `Unified_Snapshot` consistently shows:
      - **COINBASE, BINANCEUS, KRAKEN × {USD, USDC, USDT}**
      - Reasonable valuations for non-quote assets if/when we include them

---

## Forward Roadmap (Beyond Phase 18)

### Phase 19 – DB Backbone & Telemetry Warehouse

- 🔜 Migrate:
  - Commands, receipts, telemetry, balances → SQLite/Postgres
- 🔜 Use Sheets as:
  - Mirrored report layer (every 5–10 minutes), **not** the hot path
- 🔜 Enable:
  - Richer analytics
  - Historical replay
  - Easier debugging

---

### Phase 20 – Full Policy-Driven Autonomy (with Brakes)

- 🔜 Cloud policy engine is the **single source of truth** for:
  - What can be bought/sold
  - Max risk per vault/venue
  - Cooldowns / unlocks
- 🔜 Edge executes only:
  - Approved, HMAC-signed commands
  - Under `EDGE_MODE` and `EDGE_HOLD` safety rails
- 🔜 Human-in-loop UX:
  - Telegram prompts for large or unusual moves
  - Fully automatic for small, low-risk adjustments

---

### Phase 21 – Snorter Sidecar & Signal Ingestion

- 🔜 Add Snorter Sidecar:
  - Listen to external signals (news, sentiment, on-chain, etc.)
- 🔜 Feed signals into:
  - `policy_bias_engine.py`
  - Vault / rebuy / de-risk decisions

---

### Phase 22 – Legacy & Governance Mode

- 🔜 Couple **Council Ledger** + performance metrics to:
  - Long-term ROI
  - Risk profile
  - “Kids’ vaults” and their trajectories
- 🔜 Governance flows:
  - Track big policy changes as Council decisions
  - Attach rationale to major structural shifts

---

## How to Use This File

- When you ship a meaningful change:
  - Update the relevant Phase/Sub-Phase bullets.
  - Flip checkboxes from 🧩/🔄 to ✅ when something becomes stable.
- When you start a new initiative:
  - Add a new Phase or Sub-Phase at the bottom of the appropriate Era.
  - Reference **file names + env vars** wherever possible.

This roadmap is the “big map” so that future-you (and your kids) can see how NovaTrade evolved from **a single Google Sheet** into a **multi-venue autonomous engine** with guardrails.
