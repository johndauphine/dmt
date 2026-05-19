# Documentation

Technical documentation, planning documents, and implementation notes for dmt.

## Contents

| Document | Description |
|----------|-------------|
| [BENCHMARKS.md](BENCHMARKS.md) | Performance benchmarks comparing Go and Rust implementations |
| [RESTARTABILITY.md](RESTARTABILITY.md) | Checkpoint and resume functionality documentation |
| [PRIVILEGES.md](PRIVILEGES.md) | Per-driver minimum DB privileges (source / target / drop_recreate / upsert) — operator-facing GRANT recipes aligned with `dmt preflight` (see [PRIVILEGES.md](PRIVILEGES.md#what-dmt-preflight-actually-probes) for the subset preflight actually probes) |
| [DRIVER_SHARED.md](DRIVER_SHARED.md) | Shared driver refactor boundary and PR sequence |
| [SCHEMA_EVOLUTION.md](SCHEMA_EVOLUTION.md) | Schema drift auto-apply policy and current added-column scope |
| [DAILY_DRIVER.md](DAILY_DRIVER.md) | Daily incremental upsert workflow and validation notes |
| [AI_REFACTOR_HANDOFF.md](AI_REFACTOR_HANDOFF.md) | Execution handoff for AI-led architecture simplification and code reduction |
| [UPSERT_PERF_PLAN.md](UPSERT_PERF_PLAN.md) | Upsert performance improvement plan |

### Planning & Development Notes

| Document | Description |
|----------|-------------|
| [CODEX-PLAN.md](CODEX-PLAN.md) | Implementation plan for same-engine migrations and upsert modes |
| [CODEX-REVIEW.md](CODEX-REVIEW.md) | Code review findings and recommendations |
| [CODEX-UPSERT.md](CODEX-UPSERT.md) | MSSQL upsert/merge performance notes |
| [CODEX_CONTEXT.md](CODEX_CONTEXT.md) | Feature context and implementation notes |
| [GEMINI-PLAN.md](GEMINI-PLAN.md) | Gemini implementation strategy and analysis |
