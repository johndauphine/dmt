# TUI / CLI / WebUI Command Parity

The interactive TUI (launch `dmt` with no arguments) and the WebUI
(`dmt --webui`) expose the same operator workflows as the CLI. The
authoritative machine-checked mapping is the parity registry
(`internal/command/registry.go`): a test in `cmd/migrate` enumerates the
real CLI tree against it, a test in `internal/tui`
(`TestTUICommandSurface`) verifies every TUI-supported entry is
discoverable in `/help` and autocomplete, and a test in `internal/webui`
(`TestWebSurfaceCoversRegistry` / `TestWebSupportedCommandsAreRouted`)
verifies every WebUI-supported command is wired to a live `/api` route.
This page is the human-readable summary; if it disagrees with the
registry, the registry wins. (Epics #437, #577.)

The WebUI column below names the console view that operates each command;
see `docs/WEBUI.md` for the full endpoint map and deployment guide. The
`WebSurface` map in the registry is the source of truth for WebUI
disposition (`WebSupported` / `WebNA`).

## Commands

| CLI | TUI | WebUI | Notes |
|-----|-----|-------|-------|
| `dmt run` | `/run [@config]` | Dashboard | `--dry-run`, `--ai-schema-advisor`, `--source-schema`, `--target-schema`, `--workers`, `--skip-preflight`; backup confirmation is interactive in the TUI |
| `dmt resume` | `/resume [@config]` | Dashboard | `--force-resume`, `--skip-preflight`; destructive `--abandon`/`--abandon-reason` remain explicit CLI-only administration |
| `dmt preflight` / `health-check` | `/preflight`, `/health-check` | Checks | `--skip-preflight`, `--ai-review` |
| `dmt validate` | `/validate` | Checks | `--ai-triage`, `--timeout` |
| `dmt diagnose` | `/diagnose` | Checks | `--run ID`, `--ai-triage`, `--timeout` |
| `dmt status` | `/status [-d]` | Dashboard / History | |
| `dmt history` | `/history [--run ID]` | History | |
| `dmt analyze` | `/analyze` | Checks | `--apply`, `--ai-explain` |
| `dmt ai config-review` / `runbook` | `/ai config-review`, `/ai runbook` | Checks | `--timeout`, `--request TEXT` (free text, put it last) |
| `dmt profile save/list/delete/export` | `/profile …` | Profiles | export writes to `~/.dmt/exports/` in the WebUI (server-side, never a client path) |
| `dmt setup` | `/setup` | Setup | the richer guided path: secrets, config, connection test, optional smartconfig analysis |
| `dmt init` | `/wizard` | — (use Setup) | lightweight config editor; the WebUI uses the richer guided Setup instead |
| `dmt init-secrets` | `/init-secrets` | Settings | `--with-ai`, `--force` |
| `dmt cache clear` | `/cache clear` | Settings | `--ai-only`; the TUI additionally requires `--confirm` and names the exact file and scope first |

Global flags that map to `/session` defaults in the TUI (config, profile,
state-file, verbosity, log-format, metrics-addr, otel-endpoint, audit-*) are
available in the WebUI's **Settings → Session defaults**. The `--webui*`
launch flags, plus `--gui` and `--app-window` (desktop-app mode — see
`docs/WEBUI.md`), are CLI-only (they configure the WebUI itself).

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
