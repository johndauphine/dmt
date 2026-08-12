# Contributing to dmt

Welcome. This guide covers what you need to know to run dmt's checks locally and reproduce CI failures without guessing.

## Setting up a dev environment

You need:

- **Go 1.25.7+** (`go.mod` pins the floor at 1.25.7)
- **Docker** with at least 8 GB allocated for the test database containers
- **make** (the repo standardizes on `make` targets so the CI workflow and your local commands stay in lockstep)

Optional but recommended:

- **golangci-lint** v2.12.2 — `brew install golangci-lint` or download from the [releases page](https://github.com/golangci/golangci-lint/releases). CI pins this version; running a newer one locally may surface findings your PR doesn't have to fix. Earlier v1.x binaries are built with Go 1.23 and cannot type-check this project's Go-1.25 code.
- **govulncheck** v1.1.4 — `go install golang.org/x/vuln/cmd/govulncheck@v1.1.4` (CI pins this version)

## Running the checks locally

Every check in `.github/workflows/ci.yml` and `.github/workflows/integration.yml` is reproducible via a `make` target. If a CI job fails, run the matching local command to debug without having to push commits.

| CI job | Local command | What it does |
|---|---|---|
| `build-and-test / go build` | `make build` | Compile the `dmt` binary |
| `build-and-test / go vet` | `go vet ./...` | Stdlib correctness lints |
| `build-and-test / go test (unit)` | `make test-short` | Unit tests (`-short` skips integration) |
| `build-and-test / go test -race` | `go test ./... -race -short` | Race detector on unit tests |
| `lint / golangci-lint` | `make lint` | Style + bug-class checks per `.golangci.yml` |
| `vuln / govulncheck` | `govulncheck ./...` | Module vulnerability scan |
| `integration / mssql → postgres` | `make integration-test` | End-to-end migration against running MSSQL+PG containers |

## Reproducing the integration test locally

The CI integration job loads a small SO2010 fixture into MSSQL and runs an end-to-end migration to PostgreSQL. To reproduce locally:

```bash
# 1. Build the binary
make build

# 2. Start the test databases (MSSQL on :1433, PG on :5432)
make test-dbs-up

# 3. Load the SO2010-minimal fixture into MSSQL
./scripts/load-fixture-so2010-minimal.sh

# 4. Run the end-to-end migration + row-count assertions
make integration-test

# When done, tear down the test DBs
make test-dbs-down
```

The integration test:

1. Drops + recreates the target PG database (`so2010_minimal_ci`) so each run is hermetic
2. Wipes a dedicated state directory (`./.dmt-ci-state/` by default, overridable via `DMT_STATE_DIR`) so resume logic doesn't engage on stale state. **Your `~/.dmt/migrate.db` is NOT touched** — `data_dir: ${DMT_STATE_DIR}` in `scripts/fixtures/ci-mssql-pg.yaml` redirects all dmt state writes to the isolated path.
3. Runs `dmt run --config scripts/fixtures/ci-mssql-pg.yaml --confirm-backup`
4. Verifies every target table has the expected row count

If step 3 fails, the dmt log lands at `/tmp/dmt-ci.log` (override with `INTEGRATION_TEST_LOG=…`).

## Branch + commit conventions

- Feature branches: `feat/<issue>-<slug>` (e.g. `feat/230-ci-gating`)
- Fix branches: `fix/<issue>-<slug>`
- Refactors: `refactor/<slug>`
- Never commit directly to `main`. PRs land via squash-merge.

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`. Reference the closing issue with `(#NNN)` in the title or `closes #NNN` in the body.

## Pre-commit hook

Optional but useful:

```bash
make setup-hooks
```

Installs a pre-commit hook that runs `go fmt ./...` and `make test-short` before each commit. Won't catch CI's `-race`, `golangci-lint`, or `govulncheck` runs — those gate the PR, not the commit.

## Filing issues

- **Bug reports**: include the dmt version (`./dmt --version`), source/target driver pair, a config snippet (scrubbed of credentials), and the relevant log excerpt.
- **Feature requests**: explain the use case. dmt aims for "production-grade migration tool", not "kitchen sink"; feature ideas that don't tie back to that goal are likely to be respectfully declined.

## Working with AI assistants

dmt's commit history includes co-authorship attribution for AI assistants when they wrote a non-trivial portion of a change. If you use Claude / Copilot / Cursor etc. during contribution, add the appropriate `Co-Authored-By:` trailer. The existing commits in `main` provide examples of the format.
