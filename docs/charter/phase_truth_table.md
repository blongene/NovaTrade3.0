# Phase Truth Table (Canonical Grounding)

## Purpose
This appendix exists to **eliminate ambiguity** about NovaTrade’s operational posture at any moment in time.

It answers, in one place and at a glance:
- What phases exist
- What phases are running
- What phases are capable but dormant
- What phases can enqueue or execute commands

This table is authoritative. If behavior appears to contradict it, the **system is misconfigured**.

---

## Phase Truth Table

| Phase | Capability Exists | Actively Running | Can Enqueue Commands | Can Execute Commands | Notes |
|------|-------------------|------------------|----------------------|----------------------|------|
| **Phase 25** | ✅ Yes | ✅ Yes | ❌ No | ❌ No | Decision framing, diagnostics, Council insight only (`decision_only`). |
| **Phase 26A** | ✅ Yes | ✅ Yes | ❌ No | ❌ No | Alpha proposal generation (`WOULD_*`), DB‑first, deduplicated. |
| **Phase 26B** | ✅ Yes | ❌ Dormant | ❌ No | ❌ No | Approvals layer scaffolded; human‑gated only. |
| **Phase 26C** | ✅ Yes | ❌ Dormant | ❌ No | ❌ No | Preview / patch scaffolding exists; not active. |
| **Phase 26D** | ✅ Yes | ❌ Dormant | ❌ No | ❌ No | Enforcement logic present but disabled. |
| **Phase 26E** | ✅ Yes | ❌ Dormant | ❌ No | ❌ No | Phase‑26 close‑out and reconciliation tooling only. |
| **Phase 27** | ❌ No | ❌ No | ❌ No | ❌ No | Intentionally unentered. |
| **Phase 28** | ✅ Yes | ❌ Dormant | ❌ No | ❌ No | Command bus + dry‑run execution proven and closed. |
| **Edge Agent** | ✅ Yes | ✅ Yes | N/A | ❌ No | Running in `dryrun` with `LIVE_ARMED=NO`. |

---

## Interpretation Rules

- **Capability Exists ≠ Active**
  Presence of code, tables, or docs does not imply execution.

- **Actively Running** means the phase is producing artifacts on its own schedule.

- **Can Enqueue** means the system is allowed to create commands.

- **Can Execute** means the system is allowed to place orders or take irreversible actions.

Any row marked ❌ in the last two columns is **non‑executing by design**.

---

## Relationship to Dry‑Run Windows

- Dry‑Run Windows do **not** change this table
- A window temporarily allows *one controlled exception* under an explicit playbook
- After rollback, this table remains accurate

If this table ever needs to change, it must be updated **before** any configuration is altered.

---

## Canonical Status

This document is part of the NovaTrade charter canon.

It is intended to:
- Prevent re‑litigation of system state
- Anchor future discussions
- Serve as a shared reference for all Council members

When in doubt, defer to this table.
