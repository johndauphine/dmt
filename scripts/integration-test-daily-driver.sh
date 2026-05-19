#!/usr/bin/env bash
# Daily-driver integration test (#304).
#
# Proves the documented two-run workflow:
#   1. drop_recreate baseline with date_updated_columns seeds watermarks
#   2. upsert run moves only rows whose date column is newer than the watermark
#   3. unchanged follow-up upsert is a no-op

set -euo pipefail

MSSQL_PASSWORD="${MSSQL_PASSWORD:-TestPass2024}"
PG_PASSWORD="${PG_PASSWORD:-TestPass2024}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BASELINE_CONFIG="$REPO_ROOT/scripts/fixtures/ci-mssql-pg-daily-driver-baseline.yaml"
UPSERT_CONFIG="$REPO_ROOT/scripts/fixtures/ci-mssql-pg-daily-driver-upsert.yaml"
SOURCE_FIXTURE="$REPO_ROOT/scripts/fixtures/so2010-minimal.sql"
SOURCE_MUTATION="$REPO_ROOT/scripts/fixtures/daily-driver-update-mssql.sql"
LOG="${INTEGRATION_TEST_LOG:-/tmp/dmt-ci-daily-driver.log}"

DMT_STATE_DIR="${DMT_STATE_DIR:-$REPO_ROOT/.dmt-ci-daily-driver-state}"
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

assert_log_contains() {
    local pattern="$1"
    local label="$2"
    if ! grep -Eq "$pattern" "$LOG"; then
        echo "FAIL: log missing $label ($pattern)" >&2
        exit 1
    fi
    echo "  OK: log contains $label"
}

log_match_count() {
    local pattern="$1"
    grep -Ec "$pattern" "$LOG" || true
}

assert_log_count_increased() {
    local pattern="$1"
    local label="$2"
    local before="$3"
    local after

    after="$(log_match_count "$pattern")"
    if (( after <= before )); then
        echo "FAIL: log did not add $label ($pattern)" >&2
        exit 1
    fi
    echo "  OK: log added $label"
}

rm -rf "$DMT_STATE_DIR"
mkdir -p "$DMT_STATE_DIR"
: > "$LOG"

echo "=== dmt mssql -> postgres daily-driver integration test ==="
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
echo "=== Baseline drop_recreate run seeds sync timestamps ==="
MSSQL_PASSWORD="$MSSQL_PASSWORD" PG_PASSWORD="$PG_PASSWORD" \
    "$REPO_ROOT/dmt" -c "$BASELINE_CONFIG" run --confirm-backup 2>&1 | tee -a "$LOG"

baseline_count="$(
    pg_run -d so2010_minimal_ci -tAc "SELECT COUNT(*) FROM public.users;"
)"
baseline_count="$(echo "$baseline_count" | tr -d '[:space:]')"
if [[ "$baseline_count" != "5" ]]; then
    echo "FAIL: baseline public.users count got=$baseline_count want=5" >&2
    exit 1
fi

echo ""
echo "=== Mutate one MSSQL row with a fresh LastAccessDate ==="
mssql_run_file "$SOURCE_MUTATION"

echo ""
echo "=== Daily upsert run transfers only the changed row ==="
MSSQL_PASSWORD="$MSSQL_PASSWORD" PG_PASSWORD="$PG_PASSWORD" \
    "$REPO_ROOT/dmt" -c "$UPSERT_CONFIG" run --confirm-backup 2>&1 | tee -a "$LOG"
assert_log_contains "Transfer complete: 1 rows" "one-row incremental transfer"

read -r display_name reputation < <(
    pg_run -d so2010_minimal_ci -tAc \
        "SELECT displayname || '|' || reputation FROM public.users WHERE id = 1;" |
        tr -d '[:space:]' |
        awk -F'|' '{print $1, $2}'
)
if [[ "$display_name" != "JeffAtwoodDailyDriver" || "$reputation" != "9010" ]]; then
    echo "FAIL: target user id=1 got display='$display_name' reputation='$reputation'" >&2
    exit 1
fi
echo "  OK: target user id=1 reflects daily upsert"

watermark="$(
    sqlite3 "$DMT_STATE_DIR/migrate.db" \
        "SELECT last_sync_timestamp FROM table_sync_timestamps WHERE source_schema = 'dbo' AND table_name = 'Users' AND target_schema = 'public';"
)"
if [[ -z "$watermark" ]]; then
    echo "FAIL: no stored sync watermark for dbo.Users -> public" >&2
    exit 1
fi

echo ""
echo "=== Source row equal to the saved watermark is skipped ==="
equal_watermark_mutation="$(mktemp "${TMPDIR:-/tmp}/dmt-daily-driver-equal.XXXXXX.sql")"
trap 'rm -f "$equal_watermark_mutation"' EXIT
cat > "$equal_watermark_mutation" <<SQL
USE StackOverflow2010Minimal;
GO

UPDATE dbo.Users
SET
    DisplayName = N'Geoff Dalgas Equal Watermark',
    Reputation = 9999,
    LastAccessDate = CONVERT(datetime2(7), N'${watermark%Z}', 126)
WHERE Id = 2;
GO
SQL
mssql_run_file "$equal_watermark_mutation"

zero_count_before="$(log_match_count "Transfer complete: 0 rows")"
MSSQL_PASSWORD="$MSSQL_PASSWORD" PG_PASSWORD="$PG_PASSWORD" \
    "$REPO_ROOT/dmt" -c "$UPSERT_CONFIG" run --confirm-backup 2>&1 | tee -a "$LOG"
assert_log_count_increased "Transfer complete: 0 rows" "zero-row equal-watermark transfer" "$zero_count_before"

read -r equal_display equal_reputation < <(
    pg_run -d so2010_minimal_ci -tAc \
        "SELECT displayname || '|' || reputation FROM public.users WHERE id = 2;" |
        tr -d '[:space:]' |
        awk -F'|' '{print $1, $2}'
)
if [[ "$equal_display" != "GeoffDalgas" || "$equal_reputation" != "1234" ]]; then
    echo "FAIL: equal-watermark source update should not transfer, target id=2 got display='$equal_display' reputation='$equal_reputation'" >&2
    exit 1
fi
echo "  OK: equal-watermark source row was not replayed"

echo ""
echo "=== Unchanged follow-up upsert is a no-op ==="
zero_count_before="$(log_match_count "Transfer complete: 0 rows")"
MSSQL_PASSWORD="$MSSQL_PASSWORD" PG_PASSWORD="$PG_PASSWORD" \
    "$REPO_ROOT/dmt" -c "$UPSERT_CONFIG" run --confirm-backup 2>&1 | tee -a "$LOG"
assert_log_count_increased "Transfer complete: 0 rows" "zero-row unchanged follow-up" "$zero_count_before"

final_count="$(
    pg_run -d so2010_minimal_ci -tAc "SELECT COUNT(*) FROM public.users;"
)"
final_count="$(echo "$final_count" | tr -d '[:space:]')"
if [[ "$final_count" != "5" ]]; then
    echo "FAIL: final public.users count got=$final_count want=5" >&2
    exit 1
fi

echo ""
echo "Daily-driver integration test PASSED."
