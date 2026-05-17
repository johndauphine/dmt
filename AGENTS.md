# Repository Guidelines

## Project Structure & Module Organization
- `cmd/migrate/` is the CLI entry point and builds the `dmt` binary.
- `internal/` holds core packages (pipeline, driver, orchestrator, config, checkpoint, TUI). Keep new packages internal unless they must be public.
- `docs/` contains technical notes and plans; start at `docs/README.md`.
- `examples/` provides sample YAML configs and benchmark scenarios.
- `config.yaml.example` is the baseline configuration template.

## Build, Test, and Development Commands
- `make build`: compile the CLI into `./dmt` with version metadata.
- `make run`: build then run using `config.yaml`.
- `make test`: run the full Go test suite (`go test -v ./...`).
- `make test-short`: run fast tests (`go test ./... -short`).
- `make test-coverage`: generate `coverage.html`.
- `make fmt`: format code with `go fmt ./...`.
- `make lint`: run `golangci-lint`.
- `make check`: `fmt` + `test`.
- `make test-dbs-up` / `make test-dbs-down`: start/stop local MSSQL + Postgres containers for integration testing.

## Coding Style & Naming Conventions
- Go code should follow standard conventions and be `gofmt`-clean; use `make fmt` before pushing.
- Package names should be short and lowercase; exported identifiers should use Go’s `MixedCaps` style.
- Test files use the standard `*_test.go` naming pattern.
- YAML config examples live under `examples/` and should use clear, descriptive filenames.

## Testing Guidelines
- Tests are co-located with code in `cmd/` and `internal/`.
- Name tests `TestXxx` and keep fast checks compatible with `-short`.
- If you add behavior that depends on external DBs, document the setup and consider using `make test-dbs-up`.

## Commit & Pull Request Guidelines
- Commit messages follow a Conventional Commits style (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`), often with PR numbers like `(#12)`.
- PRs should include a concise summary, the rationale for changes, and the commands used to validate (e.g., `make test`).
- If a change affects configuration, include an updated example under `examples/` or a note referencing `config.yaml.example`.
- PR workflow for agents:
  - Create a branch for each task.
  - Commit code to the branch.
  - Get a clean local review from Claude or Codex before pushing.
  - Push the branch and create a PR.
  - Wait for Copilot's first PR review.
  - Address actionable Copilot comments.
  - Do not wait for Copilot to re-review; Copilot does not re-review PRs.
  - Verify CI/checks are green.
  - Get a clean post-fix review from Claude or Codex.
  - If CI is green and the post-fix review is clean, merge the PR.
  - After merging, delete the local feature branch.

## Configuration & Secrets
- Use `config.yaml.example` as the starting point; keep real credentials out of the repo.
- Local/CI Docker database credentials use the shared test password `TestPass2024` (`SA_PASSWORD`, `MSSQL_PASSWORD`, `POSTGRES_PASSWORD`, `PG_PASSWORD`, and `MYSQL_ROOT_PASSWORD`). Prefer this value for disposable local containers so shell quoting and driver parsing stay boring.
- Local Postgres test and benchmark containers should use the `postgres` user unless a fixture or issue explicitly calls for a different role.
- Sensitive credentials (API keys, encryption keys, webhook URLs) should be stored in `~/.secrets/dmt-config.yaml`.
- AI provider configuration uses the secrets file for API keys and provider settings.
- Slack webhook URLs should be stored in the secrets file under `notifications.slack.webhook_url`.
- Encrypted profiles use the master key from secrets file; document any required secrets in the PR description.
