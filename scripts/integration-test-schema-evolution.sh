#!/usr/bin/env bash
# Added-column schema evolution integration test (#306).
#
# This is intentionally separate from scripts/integration-test-pair.sh:
# schema evolution needs two runs that share one DMT_STATE_DIR. The first
# run captures the baseline source snapshot; the second run sees source
# drift, applies a nullable target ADD COLUMN, and upserts the new value.

set -euo pipefail

MSSQL_PASSWORD="${MSSQL_PASSWORD:-TestPass2024}"
PG_PASSWORD="${PG_PASSWORD:-TestPass2024}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BASELINE_CONFIG="$REPO_ROOT/scripts/fixtures/ci-mssql-pg.yaml"
UPSERT_CONFIG="$REPO_ROOT/scripts/fixtures/ci-mssql-pg-upsert-schema-evolution.yaml"
SOURCE_FIXTURE="$REPO_ROOT/scripts/fixtures/so2010-minimal.sql"
SOURCE_MUTATION="$REPO_ROOT/scripts/fixtures/schema-evolution-added-column-mssql.sql"
LOG="${INTEGRATION_TEST_LOG:-/tmp/dmt-ci-schema-evolution.log}"

DMT_STATE_DIR="${DMT_STATE_DIR:-$REPO_ROOT/.dmt-ci-schema-evolution-state}"
export DMT_STATE_DIR

if [[ ! -x "$REPO_ROOT/dmt" ]]; then
    echo "ERROR: dmt binary not found at $REPO_ROOT/dmt — run 'make build' first" >&2
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

rm -rf "$DMT_STATE_DIR"
mkdir -p "$DMT_STATE_DIR"
: > "$LOG"

echo "=== dmt mssql -> postgres schema evolution integration test ==="
echo "  baseline config: $BASELINE_CONFIG"
echo "  upsert config:   $UPSERT_CONFIG"
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

existing_column="$(
    pg_run -d so2010_minimal_ci -tAc \
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'new_note';"
)"
existing_column="$(echo "$existing_column" | tr -d '[:space:]')"
if [[ "$existing_column" != "0" ]]; then
    echo "FAIL: public.users.new_note exists before source drift mutation" >&2
    exit 1
fi

echo ""
echo "=== Mutate MSSQL source: add dbo.Users.new_note and populate row 1 ==="
mssql_run_file "$SOURCE_MUTATION"

echo ""
echo "=== Upsert run auto-applies added-column drift ==="
MSSQL_PASSWORD="$MSSQL_PASSWORD" PG_PASSWORD="$PG_PASSWORD" \
    "$REPO_ROOT/dmt" -c "$UPSERT_CONFIG" run --confirm-backup 2>&1 | tee -a "$LOG"

echo ""
echo "=== Verify PostgreSQL received the evolved column value ==="
got="$(
    pg_run -d so2010_minimal_ci -tAc \
        "SELECT new_note FROM public.users WHERE id = 1;"
)"
got="$(echo "$got" | tr -d '[:space:]')"
if [[ "$got" != "schema-evolution-auto-ok" ]]; then
    echo "FAIL: public.users.new_note for id=1 got='$got' want='schema-evolution-auto-ok'" >&2
    exit 1
fi

echo "  OK: public.users.new_note = $got"
echo ""
echo "Schema evolution integration test PASSED."
