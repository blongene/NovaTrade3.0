Current Operating State (Observation Mode — Production-Grade)

NovaTrade is currently operating in Observation Mode with live signal ingestion and proposal generation, while all execution paths remain explicitly disarmed.

Phase 25 runs continuously in decision_only mode for framing, diagnostics, and Council insight. Command enqueue is explicitly disabled.

Phase 26A proposal generation is active and writing daily WOULD_* proposals to alpha_proposals (DB-first). These proposals are deduplicated, gated, and explanatory.

Phase 26B–E (approvals, previews, patches) are fully scaffolded and historically exercised, but currently dormant.

Phase 27–28 execution infrastructure exists but enforcement is disabled. No live or dry-run commands are being generated or leased.

Edge agents are operating in dryrun mode with LIVE_ARMED=NO.

This posture is intentional: the system is as close to full production as possible without allowing command creation or execution, enabling parallel testing, analytics hardening, and proposal quality evaluation.

A2. Phase Truth Table (simple, visual grounding)

Add a table like this (docs or charter appendix):

Phase	Capability Exists	Actively Running	Can Enqueue	Can Execute
25	✅ Yes	✅ Yes	❌ No	❌ No
26A	✅ Yes	✅ Yes	❌ No	❌ No
26B–E	✅ Yes	❌ Dormant	❌ No	❌ No
27	✅ Yes	❌ Dormant	❌ No	❌ No
28	✅ Yes	❌ Dormant	❌ No	❌ No

This makes it impossible to misinterpret status later.

A3. One explicit note about historical artifacts

Add a short note somewhere visible:

Note on Historical Artifacts

Database tables such as commands, nova_commands, and alpha_command_previews may contain historical rows from earlier controlled experiments. These rows are not indicative of current execution behavior. No new command creation has occurred since 2026-01-20.
