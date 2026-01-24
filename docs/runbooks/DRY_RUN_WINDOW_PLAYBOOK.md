# Dry‑Run Window Playbook (Observation → Controlled Verification)

## Purpose

This playbook defines the **only approved method** for temporarily exercising NovaTrade’s command path while remaining in Observation Mode. It exists to prove wiring, gating, approvals, and rollback — **not** to trade.

The intent is to allow **confidence-building verification** without introducing ambiguity, drift, or execution risk.

---

## Scope

This playbook governs **Phase 26A–26E / Phase 28 dry‑run verification only**.

Explicitly **out of scope**:

* Live execution
* Automated multi‑command cycles
* Ungated approvals
* Strategy tuning or parameter optimization

---

## Preconditions (Must All Be True)

Before opening a Dry‑Run Window, verify:

* Observation Mode is active
* Phase 25 is **decision_only**
* Phase 26A proposals are writing to **DB (alpha_proposals)**
* Phase 26 approvals exist but are **human‑gated**
* Phase 28 enforcement is **disabled**
* Edge agents report:

  * `EDGE_MODE=dryrun`
  * `LIVE_ARMED=NO`
  * `EDGE_HOLD=0`
* No queued or leased commands at start

If **any** condition fails → **do not proceed**.

---

## Allowed Window Parameters (Hard Limits)

During the window:

* Duration: **≤ 30 minutes**
* Max commands: **1 total**
* Max notional: **≤ $10**
* Venues: Coinbase / BinanceUS only
* Command type: dry‑run only (no live)
* Approval: **explicit human approval required**

Anything outside these limits is a violation.

---

## Authorized Actions (In Order)

### Step 1 — Open Window

* Announce intent (for yourself / ledger):

  > “Opening Dry‑Run Window — verification only.”

* Snapshot system state:

  * commands count
  * alpha_command_previews count
  * edge status

---

### Step 2 — Allow Single Enqueue Path

Temporarily allow **exactly one** dry‑run command path:

* Enqueue enabled
* Execution dry‑run only
* Approval required

No other toggles may be changed.

---

### Step 3 — Generate / Approve One Command

* Select **one** proposal
* Review gates, rationale, payload
* Approve manually
* Confirm preview record exists

Do **not** approve more than one.

---

### Step 4 — Observe End‑to‑End Flow

Confirm, in order:

* Command enqueued
* Lease acquired
* Edge pulls command
* Dry‑run execution occurs
* ACK received
* Receipt normalized

Expected outcome:

* `status=dryrun`
* `ok=true`
* No retries, no duplicates

---

## Mandatory Rollback (Immediate)

Once verification completes:

* Disable enqueue
* Confirm queue empty
* Confirm no leased commands
* Re‑assert Observation Mode posture

Rollback is **not optional**, even if nothing happened.

---

## Post‑Run Verification Checklist

All must pass:

* commands count increased by **≤ 1**
* No stuck leases
* No repeated idempotency keys
* Telegram output calm
* Policy_Log entries coherent
* alpha_command_previews stable

If any anomaly appears → stop and investigate before any future window.

---

## Exit Criteria

A Dry‑Run Window is considered **successful** if:

* Exactly one command flowed end‑to‑end
* No side effects persisted
* System returned cleanly to Observation Mode

Failure to meet these criteria means the window **did not count**.

---

## Notes on Phase 28 Close‑Out

Phase 28 has already proven the **mechanics** of the command bus and dry‑run execution path. This playbook exists to allow **selective re‑verification** without reopening Phase 28 wholesale.

Historical artifacts from earlier phases are acknowledged and do not alter current posture.

---

## Non‑Negotiables

* No improvisation
* No stacking windows
* No silent toggles
* No memory drift

If unsure — **stop**.

This playbook exists so that future action is boring, safe, and reversible.
