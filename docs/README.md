# Documentation

Technical documentation, planning documents, and implementation notes for dmt.

## Contents

| Document | Description |
|----------|-------------|
| [RUNBOOK.md](RUNBOOK.md) | Operator runbook: preflight, run, resume, validate, diagnose — CLI and TUI |
| [TUI_COMMANDS.md](TUI_COMMANDS.md) | TUI / CLI / WebUI command parity table and intentionally CLI-only surfaces |
| [WEBUI.md](WEBUI.md) | Browser front-end (`dmt --webui`): security model, remote-deployment guide (nginx/Caddy), maturity notes |
| [BENCHMARKS.md](BENCHMARKS.md) | Performance benchmarks comparing Go and Rust implementations |
| [FIXTURES.md](FIXTURES.md) | Test/bench datasets: CI-friendly fixtures and manual SO2010/SO2013/WWI restores |
| [OBSERVABILITY.md](OBSERVABILITY.md) | Prometheus metrics and OTLP tracing setup |
| [SECURITY.md](SECURITY.md) | Security posture: secrets handling, redaction, permissions |
| [AUDIT-LOG.md](AUDIT-LOG.md) | Audit log format and tamper-evident mode |
| [AI_EVALS.md](AI_EVALS.md) | Repeatable live-provider evals for AI advisory prompt quality |
| [RESTARTABILITY.md](RESTARTABILITY.md) | Checkpoint and resume functionality documentation |
| [PRIVILEGES.md](PRIVILEGES.md) | Per-driver minimum DB privileges (source / target / drop_recreate / upsert) — operator-facing GRANT recipes aligned with `dmt preflight` (see [PRIVILEGES.md](PRIVILEGES.md#what-dmt-preflight-actually-probes) for the subset preflight actually probes) |
| [DRIVER_SHARED.md](DRIVER_SHARED.md) | Shared driver refactor boundary and PR sequence |
| [SCHEMA_CONTRACT.md](SCHEMA_CONTRACT.md) | DLT-style schema contract configuration, including DMT's report mode |
| [SCHEMA_EVOLUTION.md](SCHEMA_EVOLUTION.md) | Deprecated legacy schema drift auto-apply policy |
| [DAILY_DRIVER.md](DAILY_DRIVER.md) | Daily incremental upsert workflow and validation notes |
| [DELETE_HANDLING.md](DELETE_HANDLING.md) | Design proposal for delete propagation in incremental upsert mode |
| [ORCHESTRATOR_FLOW.md](ORCHESTRATOR_FLOW.md) | Map of the fresh run, resume, transfer runner, and target-mode orchestration files |
| [ARCHITECTURE_HARDENING_EPIC.md](ARCHITECTURE_HARDENING_EPIC.md) | Proposed epic to harden correctness, state, AI, and operational contracts after the 2026-05-19 architecture review |
| [ARCHITECTURE_REVIEW_2026-05-19.md](ARCHITECTURE_REVIEW_2026-05-19.md) | Claude architecture review findings and prioritized leads |
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
