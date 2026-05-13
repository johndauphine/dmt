#!/usr/bin/env bash
# Integration test driver (#230). Run the mssql → postgres migration
# end-to-end against running test containers and assert the target is
# populated with the expected row count.
#
# Invoked by:
#   - make integration-test (locally + in CI)
#   - .github/workflows/integration.yml
#
# Has two operating modes:
#   - Local dev (default): connects to PG via `docker exec` against a
#     running pg-test / pg-bench container, and writes state under
#     ./.dmt-ci-state (NOT ~/.dmt — see codex review on #230).
#   - CI (DMT_CI_MODE=1): connects to PG via host-installed psql on
#     localhost:5432, because GHA service containers have generated
#     names not matching `pg-test`/`pg-bench`/`postgres`.
#
# Exits non-zero if any step fails — the CI workflow surfaces logs as
# artifacts on failure for diagnosis (see integration.yml `if: failure()`).

set -euo pipefail

MSSQL_PASSWORD="${MSSQL_PASSWORD:-TestPass2024}"
PG_PASSWORD="${PG_PASSWORD:-TestPass2024}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG="$REPO_ROOT/scripts/fixtures/ci-mssql-pg.yaml"
LOG="${INTEGRATION_TEST_LOG:-/tmp/dmt-ci.log}"

# Isolate dmt state so a local run doesn't clobber the operator's
# ~/.dmt/migrate.db (which holds saved profiles + run history). The
# config file under scripts/fixtures/ci-mssql-pg.yaml sets
# migration.data_dir to this same path so dmt writes there.
DMT_STATE_DIR="${DMT_STATE_DIR:-$REPO_ROOT/.dmt-ci-state}"
export DMT_STATE_DIR

if [[ ! -x "$REPO_ROOT/dmt" ]]; then
    echo "ERROR: dmt binary not found at $REPO_ROOT/dmt — run 'make build' first" >&2
    exit 1
fi

# pg_run wraps either `docker exec <pg-container> psql ...` (local) or
# host-side `psql -h localhost -p 5432 ...` (CI). All other pg calls
# below go through this so the local/CI mode switch is one place.
pg_run() {
    if [[ "${DMT_CI_MODE:-}" == "1" ]]; then
        PGPASSWORD="$PG_PASSWORD" psql -h localhost -p 5432 -U postgres "$@"
        return
    fi
    # Local: discover the running PG container by name.
    local pg_container=""
    for name in pg-test pg-bench; do
        if [[ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" == "true" ]]; then
            pg_container="$name"
            break
        fi
    done
    if [[ -z "$pg_container" ]]; then
        echo "ERROR: no running pg-test / pg-bench container." >&2
        echo "       Run: make test-dbs-up   (or set DMT_CI_MODE=1 to use host psql)" >&2
        exit 1
    fi
    docker exec "$pg_container" psql -U postgres "$@"
}

# Drop+recreate the target DB so the integration test is hermetic — the
# previous run's state doesn't leak into this run's assertions.
pg_run -c "DROP DATABASE IF EXISTS so2010_minimal_ci;" >/dev/null
pg_run -c "CREATE DATABASE so2010_minimal_ci;" >/dev/null

# Clean any prior CI state DB so resume logic doesn't engage on a
# stale incomplete run. We use the isolated DMT_STATE_DIR (not the
# operator's ~/.dmt) so local invocations don't lose profiles/history.
rm -rf "$DMT_STATE_DIR"
mkdir -p "$DMT_STATE_DIR"

echo "=== dmt mssql → postgres (CI integration test) ==="
echo "  config:    $CONFIG"
echo "  state dir: $DMT_STATE_DIR"
echo "  log:       $LOG"
MSSQL_PASSWORD="$MSSQL_PASSWORD" PG_PASSWORD="$PG_PASSWORD" \
    "$REPO_ROOT/dmt" -c "$CONFIG" run --confirm-backup 2>&1 | tee "$LOG"

# dmt run exits non-zero on failure; the pipe above preserves that via
# `set -o pipefail`. If we got here, the migration claims success.

# Verify row count parity for every table the fixture defines. dmt's
# built-in row-count validation (`dmt validate`) is the canonical check
# but we do an explicit sanity check here so the CI failure shows the
# exact mismatch.
#
# Parallel arrays instead of associative arrays — macOS ships bash 3.2
# (no `declare -A`) and we want the script to run identically locally
# and in GHA. dmt's PG target sanitizer lowercases identifiers, so the
# table names below match the actual PG-side spellings.
TABLES=(votetypes posttypes linktypes users posts comments votes badges postlinks)
EXPECTED=(15        8         2         5     3     3        5     4      2)

echo ""
echo "=== Target row count verification ==="
fail=0
for i in $(seq 0 $((${#TABLES[@]} - 1))); do
    table="${TABLES[$i]}"
    want="${EXPECTED[$i]}"
    got="$(pg_run -d so2010_minimal_ci -tAc "SELECT COUNT(*) FROM \"$table\";")"
    got="$(echo "$got" | tr -d '[:space:]')"
    if [[ "$got" != "$want" ]]; then
        echo "  FAIL: $table got=$got want=$want"
        fail=1
    else
        echo "  OK:   $table = $got"
    fi
done

if [[ "$fail" -ne 0 ]]; then
    echo ""
    echo "Integration test FAILED — see $LOG for dmt output."
    exit 1
fi

echo ""
echo "Integration test PASSED."
