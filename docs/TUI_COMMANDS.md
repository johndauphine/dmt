# TUI / CLI Command Parity

The interactive TUI (launch `dmt` with no arguments) exposes the same
operator workflows as the CLI. The authoritative machine-checked
mapping is the parity registry (`internal/command/registry.go`): a test
in `cmd/migrate` enumerates the real CLI tree against it, and a test in
`internal/tui` (`TestTUICommandSurface`) verifies every supported entry
is discoverable in `/help` and autocomplete. This page is the
human-readable summary; if it disagrees with the registry, the registry
wins. (Epic #437.)

## Commands

| CLI | TUI | Notes |
|-----|-----|-------|
| `dmt run` | `/run [@config]` | `--dry-run`, `--ai-schema-advisor`, `--source-schema`, `--target-schema`, `--workers`, `--skip-preflight`; backup confirmation is interactive in the TUI |
| `dmt resume` | `/resume [@config]` | `--force-resume`, `--skip-preflight` |
| `dmt preflight` / `health-check` | `/preflight`, `/health-check` | `--skip-preflight`, `--ai-review` |
| `dmt validate` | `/validate` | `--ai-triage`, `--timeout` |
| `dmt diagnose` | `/diagnose` | `--run ID`, `--ai-triage`, `--timeout` |
| `dmt status` | `/status [-d]` | |
| `dmt history` | `/history [--run ID]` | |
| `dmt analyze` | `/analyze` | `--apply`, `--ai-explain` |
| `dmt ai config-review` / `runbook` | `/ai config-review`, `/ai runbook` | `--timeout`, `--request TEXT` (free text, put it last) |
| `dmt profile save/list/delete/export` | `/profile …` | |
| `dmt setup` | `/setup` | the richer guided path: secrets, config, connection test, optional smartconfig analysis |
| `dmt init` | `/wizard` | lightweight config editor |
| `dmt init-secrets` | `/init-secrets` | `--with-ai`, `--force` |
| `dmt cache clear` | `/cache clear` | `--ai-only`; the TUI additionally requires `--confirm` and names the exact file and scope first |

## Global flags → `/session` keys

Sticky per-session defaults replace repeating global flags on every
command: `config`, `profile`, `state-file`, `verbosity`, `log-format`,
`metrics-addr`, `otel-endpoint`, `audit-dir`, `audit-tamper-evident`,
`no-audit`. Set with `/session KEY VALUE`, inspect with `/session`,
unset with `/session clear [KEY]`. Values are validated when set.

## Intentionally CLI-only

| Surface | Rationale |
|---------|-----------|
| `--output-json`, `--output-file`, `--json` | Automation output; the TUI renders structured blocks and `/logs` saves the session transcript |
| `--progress`, `--progress-interval` | The TUI renders its own live progress |
| `--shutdown-timeout` | The TUI cancels interactively (Ctrl+C) |
| `--run-id` | `/diagnose --run` covers the TUI need; explicit run IDs are an automation concern |
| `--confirm-backup` | The TUI confirms interactively |
| `dmt ai evals` | Developer/eval harness, no operator workflow |
