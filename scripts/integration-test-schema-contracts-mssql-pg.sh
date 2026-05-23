#!/usr/bin/env bash
# MSSQL -> Postgres schema_contract integration test.
#
# Proves DLT-style schema_contract settings drive real target ALTER behavior
# for added columns, safe type widening, and nullability relaxation.

set -euo pipefail

MSSQL_PASSWORD="${MSSQL_PASSWORD:-TestPass2024}"
PG_PASSWORD="${PG_PASSWORD:-TestPass2024}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BASELINE_CONFIG="$REPO_ROOT/scripts/fixtures/ci-mssql-pg.yaml"
CONTRACT_CONFIG="$REPO_ROOT/scripts/fixtures/ci-mssql-pg-schema-contract-upsert.yaml"
SOURCE_FIXTURE="$REPO_ROOT/scripts/fixtures/so2010-minimal.sql"
SOURCE_MUTATION="$REPO_ROOT/scripts/fixtures/schema-contract-evolve-mssql.sql"
LOG="${INTEGRATION_TEST_SCHEMA_CONTRACT_LOG:-/tmp/dmt-ci-schema-contract-mssql-pg.log}"

DMT_STATE_DIR="${DMT_STATE_DIR:-$REPO_ROOT/.dmt-ci-schema-contract-mssql-pg-state}"
export DMT_STATE_DIR

if [[ ! -x "$REPO_ROOT/dmt" ]]; then
    echo "ERROR: dmt binary not found at $REPO_ROOT/dmt - run 'make build' first" >&2
    exit 1
fi

pick_container() {
    local env_name="$1"
    local hint="$2"
    shift 2

    local configured="${!env_name:-}"
    if [[ -n "$configured" ]]; then
        if [[ "$(docker inspect -f '{{.State.Running}}' "$configured" 2>/dev/null)" == "true" ]]; then
            printf '%s' "$configured"
            return 0
        fi
        echo "ERROR: $env_name=$configured is not a running container." >&2
        return 1
    fi

    local candidate
    for candidate in "$@"; do
        if [[ "$(docker inspect -f '{{.State.Running}}' "$candidate" 2>/dev/null)" == "true" ]]; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    echo "ERROR: no running $hint container (checked: $*)." >&2
    echo "       Run: make test-dbs-up   (or: make bench-dbs-up)" >&2
    return 1
}

mssql_sqlcmd_path() {
    local container="$1"
    local path
    for path in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
        if docker exec "$container" test -x "$path"; then
            printf '%s' "$path"
            return 0
        fi
    done
    echo "ERROR: sqlcmd not found in $container" >&2
    return 1
}

pg_run() {
    if [[ "${DMT_CI_MODE:-}" == "1" ]]; then
        PGPASSWORD="$PG_PASSWORD" psql -h localhost -p 5432 -U postgres "$@"
        return
    fi

    local container
    container="$(pick_container PG_CONTAINER "PostgreSQL" pg-test pg-bench)" || exit 1
    docker exec -e PGPASSWORD="$PG_PASSWORD" "$container" psql -U postgres "$@"
}

mssql_run_file() {
    local file="$1"
    if [[ "${DMT_CI_MODE:-}" == "1" ]]; then
        sqlcmd -S "localhost,1433" -U sa -P "$MSSQL_PASSWORD" -C -b -i "$file"
        return
    fi

    local container sqlcmd remote_file
    container="$(pick_container MSSQL_CONTAINER "MSSQL" mssql-test mssql-bench)" || exit 1
    sqlcmd="$(mssql_sqlcmd_path "$container")" || exit 1
    remote_file="/tmp/$(basename "$file")"
    docker cp "$file" "$container:$remote_file"
    docker exec "$container" "$sqlcmd" -S localhost -U sa -P "$MSSQL_PASSWORD" -C -b -i "$remote_file"
}

assert_pg() {
    local label="$1"
    local query="$2"
    local want="$3"
    local got

    got="$(pg_run -d so2010_minimal_ci -tAc "$query" | tr -d '[:space:]')"
    if [[ "$got" != "$want" ]]; then
        echo "FAIL: $label got='$got' want='$want'" >&2
        exit 1
    fi
    echo "  OK: $label"
}

rm -rf "$DMT_STATE_DIR"
mkdir -p "$DMT_STATE_DIR"
: > "$LOG"

echo "=== dmt mssql -> postgres schema_contract integration test ==="
echo "  baseline config: $BASELINE_CONFIG"
echo "  contract config: $CONTRACT_CONFIG"
echo "  state dir:       $DMT_STATE_DIR"
echo "  log:             $LOG"

echo ""
echo "=== Load baseline MSSQL source fixture ==="
mssql_run_file "$SOURCE_FIXTURE"

echo ""
echo "=== Prepare PostgreSQL target database ==="
pg_run -c "DROP DATABASE IF EXISTS so2010_minimal_ci;" >/dev/null
pg_run -c "CREATE DATABASE so2010_minimal_ci;" >/dev/null

echo ""
echo "=== Baseline drop_recreate run captures source schema snapshots ==="
MSSQL_PASSWORD="$MSSQL_PASSWORD" PG_PASSWORD="$PG_PASSWORD" \
    "$REPO_ROOT/dmt" -c "$BASELINE_CONFIG" run --confirm-backup 2>&1 | tee -a "$LOG"

echo ""
echo "=== Mutate MSSQL source: added column, widened type, relaxed nullability ==="
mssql_run_file "$SOURCE_MUTATION"

echo ""
echo "=== Upsert run applies schema_contract evolution ==="
MSSQL_PASSWORD="$MSSQL_PASSWORD" PG_PASSWORD="$PG_PASSWORD" \
    "$REPO_ROOT/dmt" -c "$CONTRACT_CONFIG" run --confirm-backup 2>&1 | tee -a "$LOG"

echo ""
echo "=== Verify PostgreSQL target schema and data ==="
assert_pg "public.users.new_note value transferred" \
    "SELECT new_note FROM public.users WHERE id = 1;" \
    "schema-contract-evolve-ok"
assert_pg "public.users.accountid widened to bigint" \
    "SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'accountid';" \
    "bigint"
assert_pg "public.users.displayname relaxed to nullable" \
    "SELECT is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'displayname';" \
    "YES"
assert_pg "public.users row count preserved" \
    "SELECT COUNT(*) FROM public.users;" \
    "5"

echo ""
echo "Schema contract MSSQL -> Postgres integration test PASSED."
