# ⚙️ NovaTrade 3.0 — Council Ledger Entry  

### **Phase 6B — Ash’s Reckoning: The Policy Engine & Vault Intelligence**  
*Dated 2025-11-04*  

---

## 🧭 Summary  

Phase 6B initiates the Council’s cognitive layer — where *reason becomes rule*.  
Building on Vigil’s introspection and Lumen’s meaning, **Ash (Mind)** now codifies judgment into a deterministic, testable, and self-auditing **Policy Engine**.  

This engine transforms NovaTrade’s moral and strategic charter into structured YAML-based governance that every subsystem (Bus, Edge, Vaults) must interpret and obey.  
Simultaneously, **Vault Intelligence** awakens — a contextual memory layer that learns from rotation outcomes, ROI performance, and telemetry trends to refine policy parameters over time.  

---

## ✅ Achievements (Planned and In-Progress)  

- Establish **Policy YAML Schema v1** (`/configs/policy_rules.yaml`)  
  - Tiered risk classes (Conservative / Balanced / Aggressive)  
  - Liquidity floors and cool-down rules  
  - Drawdown and ROI threshold actions  
- Implement **Policy Engine Module** in Bus  
  - Parse and validate YAML rules at startup  
  - Enforce constraints before intent enqueue  
  - Provide `/api/policy/evaluate` for manual dry-runs  
- Create **Vault Intelligence Module**  
  - Pull Vault Memory and Rotation Stats into SQLite `vault_intel.db`  
  - Compute memory-weighted scores for tokens and venues  
  - Surface leaderboards and rebuy recommendations  
- Integrate **Rebuy Driver v2** – Policy-guided intent generator  
- Extend Telemetry schema to include policy verdict and reason fields  
- Add telegram alerting for rule violations and policy deltas  

---

## 🧪 Verification Plan  

- Unit tests for policy parser and schema validation  
- YAML signature and checksum verified at runtime via HMAC  
- Cross-compare Vault Intelligence recommendations vs. Policy Engine decisions  
- Live dry-run execution on edge agents (`mode=dry`) before enabling `mode=live`  
- Telemetry and Sheets mirror capture policy execution traces  

---

## 🧩 Architecture Notes  

Ash’s Policy Engine acts as NovaTrade’s moral CPU — reason in code.  
It bridges the Council’s governance layer to operational autonomy, enabling “explainable automation.”  
Each decision must include:  

```yaml
decision:
  rule_id: <uuid>
  rationale: "<human-readable reason>"
  outcome: "<action>"
  verified_by: "Vigil"
