#!/usr/bin/env bash
# Integration test driver (#230). Run the mssql → postgres migration
# end-to-end against running test containers and assert the target is
# populated with the expected row count.
#
# Invoked by:
#   - make integration-test (locally + in CI)
#   - .github/workflows/integration.yml
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

if [[ ! -x "$REPO_ROOT/dmt" ]]; then
    echo "ERROR: dmt binary not found at $REPO_ROOT/dmt — run 'make build' first" >&2
    exit 1
fi

# Pick a running PG container (test/bench/CI service). CI service
# containers are named after the service key ("postgres" in our
# integration.yml).
pick_pg_container() {
    for name in pg-test pg-bench postgres; do
        if [[ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" == "true" ]]; then
            echo "$name"
            return 0
        fi
    done
    return 1
}

PG_CONTAINER="$(pick_pg_container)" || {
    echo "ERROR: no running pg-test / pg-bench / postgres container." >&2
    echo "       Run: make test-dbs-up   (or rely on GHA service containers)" >&2
    exit 1
}

# Drop+recreate the target DB so the integration test is hermetic — the
# previous run's state doesn't leak into this one's assertions.
docker exec "$PG_CONTAINER" psql -U postgres -c "DROP DATABASE IF EXISTS so2010_minimal_ci;" >/dev/null
docker exec "$PG_CONTAINER" psql -U postgres -c "CREATE DATABASE so2010_minimal_ci;" >/dev/null

# Clean any prior local state DB so resume logic doesn't engage on a
# stale incomplete run.
rm -f "$HOME/.dmt/migrate.db"

echo "=== dmt mssql → postgres (CI integration test) ==="
echo "  config: $CONFIG"
echo "  log:    $LOG"
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
    got="$(docker exec "$PG_CONTAINER" psql -U postgres -d so2010_minimal_ci -tAc "SELECT COUNT(*) FROM \"$table\";")"
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
