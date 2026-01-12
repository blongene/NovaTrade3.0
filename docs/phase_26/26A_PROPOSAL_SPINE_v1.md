# Phase 26A — Proposal Spine v1
## NovaTrade 3.0

**Scope:** Bus-only (observability & governance).  
**Non-Goals:** No execution, no commands, no Edge changes.

---

## 1) Purpose
Phase 26A establishes a durable, self-explanatory proposal layer that allows the system to **express intent without authority**. It creates traceable WOULD_* proposals and explicit explanations for inaction, enabling long-horizon trust when no human is present.

---

## 2) Principles (Non-Negotiable)
- **Explain before act:** Reasoning must stand alone months later.
- **Silence is meaningful:** Inaction is a first-class outcome.
- **No authority creep:** Proposals never imply execution.
- **Append-only truth:** Proposals are immutable records.
- **Bus-only cognition:** The Edge remains blind to intent.

---

## 3) Proposal Types (Preview-Only)
- **WOULD_TRADE** — A trade would be considered if approvals existed.
- **WOULD_WATCH** — An asset merits continued observation.
- **WOULD_SKIP** — Conditions were reviewed and rejected.

Each proposal includes a human-readable rationale and a gate snapshot.

---

## 4) Required Fields (Canonical)
- `proposal_id` (UUID)
- `type` (WOULD_TRADE | WOULD_WATCH | WOULD_SKIP)
- `symbol`
- `venue`
- `side` (buy | sell | none)
- `size_usd` (nullable)
- `rationale` (plain English; required)
- `gate_snapshot` (JSON; why allowed/blocked)
- `dedupe_key` (stable across identical states)
- `run_id`
- `created_at` (UTC)

---

## 5) Dedupe & Cadence
- Identical states **must not** emit duplicate proposals.
- At most one proposal per dedupe_key per cadence.
- Cadence favors calm (e.g., 10–15 min).

---

## 6) “Why Nothing Happened” Standard
When no proposals are emitted in a run, the system must record **one** explicit explanation covering:
- What was evaluated
- Why each gate blocked action
- When the next reevaluation occurs

Silence without explanation is a defect.

---

## 7) Surfaces
- **Primary:** Database (append-only).
- **Mirror:** Sheets (read-only; deduped; sparse).
- **Notifications:** Optional, calm summaries only.

---

## 8) Safety & Boundaries
- No commands created.
- No approvals required.
- No learning feedback loops.
- No Edge visibility.

---

## 9) Exit Criteria (26A)
- Stable proposal emission (or justified silence).
- Zero execution side effects.
- Dedupe verified.
- Human can step away without losing understanding.

---

## 10) Rollback
Remove the proposal writer/scheduler hook. No state corruption; records remain as history.
