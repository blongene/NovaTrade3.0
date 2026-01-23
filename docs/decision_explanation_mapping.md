# Decision Explanation Mapping — Canonical v1

Purpose:
Formalize how Phase 25 outputs map into a single, human-legible
"Why Nothing Happened" explanation object.
This document defines meaning only. It does not change behavior.

---

## Canonical Object: Decision_Explanation

Written once per evaluation cycle (or batch), append-only.

---

### Identity

| Field | Source | Rule |
|------|-------|------|
| ts | Policy_Log.ts | Use evaluation timestamp |
| phase | Config | phase=25 |
| mode | Config | observation |

---

### Scope

| Field | Source | Rule |
|------|--------|------|
| assets_evaluated | Scout / Summary | Count of assets scanned |
| venues_considered | Wallet_Monitor / Config | Unique venues with fresh data |

---

### Intent Summary

| Field | Source | Rule |
|------|--------|------|
| would_rebuy[] | Scout Decisions | WOULD_REBUY |
| would_sell[] | Scout Decisions | WOULD_SELL |
| would_watch[] | Scout Decisions | WOULD_WATCH |
| noop | Derived | true if all above lists empty |

---

### Outcome

| Field | Source | Rule |
|------|--------|------|
| executed | Trade_Log | false in Phase 25 |
| expected | Phase 25 Canon | always true when observation mode |

---

### Block Reasons (Closed Set)

Valid codes only:

- OBSERVATION_MODE_ENFORCED
- CAPITAL_FLOOR_NOT_MET
- COOLDOWN_ACTIVE
- CONFIDENCE_BELOW_THRESHOLD
- POLICY_BLOCK_ACTIVE
- APPROVAL_REQUIRED
- VENUE_UNAVAILABLE
- LIQUIDITY_INSUFFICIENT
- RISK_BUDGET_EXCEEDED

Mapping:
- Free-text policy reasons must map to ≥1 code
- Multiple codes allowed
- No new codes without review

---

### Confidence Snapshot

| Field | Source | Rule |
|------|--------|------|
| highest_confidence | Scout Decisions | max(confidence) |
| median_confidence | Derived | median(confidence) |
| signal_strength_distribution | Scout Decisions | LOW / MEDIUM / HIGH counts |

---

### Operator Guidance

| Field | Source | Rule |
|------|--------|------|
| action_required | Phase rules | false in Phase 25 |
| message | Summary | "No action required. System behavior is consistent with policy and observation mode." |

---

## Invariants

- Observation silence is a valid outcome
- No execution without explicit phase change
- Rollbacks must leave explanation intact
- Explanation precedes authority in all future phases
