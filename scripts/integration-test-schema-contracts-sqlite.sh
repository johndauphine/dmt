#!/usr/bin/env bash
# SQLite -> SQLite integration matrix for DLT-style schema_contract behavior.
#
# This complements the regular SO2010 SQLite migration test with focused
# two-run drift scenarios. Each case captures a baseline source snapshot,
# mutates the source schema, reruns with one contract mode, and validates the
# target schema/data plus the persisted source snapshots.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG="$REPO_ROOT/scripts/fixtures/ci-sqlite-schema-contract.yaml"
BASE_FIXTURE="$REPO_ROOT/scripts/fixtures/schema-contract-sqlite-base.sql"
LOG="${INTEGRATION_TEST_SCHEMA_CONTRACT_SQLITE_LOG:-/tmp/dmt-ci-schema-contract-sqlite.log}"

WORK_DIR="${SQLITE_SCHEMA_CONTRACT_WORK_DIR:-$REPO_ROOT/.dmt-ci-schema-contract-sqlite-work}"

if [[ ! -x "$REPO_ROOT/dmt" ]]; then
    echo "ERROR: dmt binary not found at $REPO_ROOT/dmt - run 'make build' first" >&2
    exit 1
fi

if ! command -v sqlite3 >/dev/null 2>&1; then
    echo "ERROR: sqlite3 CLI not found on PATH." >&2
    exit 1
fi

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
: > "$LOG"

echo "=== dmt sqlite -> sqlite schema_contract integration matrix ==="
echo "  config:  $CONFIG"
echo "  fixture: $BASE_FIXTURE"
echo "  work:    $WORK_DIR"
echo "  log:     $LOG"

fail=0

case_dir=""

run_dmt() {
    local target_mode="$1"
    local tables_mode="$2"
    local columns_mode="$3"
    local data_type_mode="$4"
    local label="$5"

    echo "" | tee -a "$LOG"
    echo "=== $label ===" | tee -a "$LOG"
    DMT_SCHEMA_CONTRACT_TARGET_MODE="$target_mode" \
    DMT_SCHEMA_CONTRACT_TABLES="$tables_mode" \
    DMT_SCHEMA_CONTRACT_COLUMNS="$columns_mode" \
    DMT_SCHEMA_CONTRACT_DATA_TYPE="$data_type_mode" \
        "$REPO_ROOT/dmt" -c "$CONFIG" run --confirm-backup 2>&1 | tee -a "$LOG"
}

run_dmt_expect_fail() {
    local target_mode="$1"
    local tables_mode="$2"
    local columns_mode="$3"
    local data_type_mode="$4"
    local label="$5"

    echo "" | tee -a "$LOG"
    echo "=== $label (expected failure) ===" | tee -a "$LOG"
    set +e
    DMT_SCHEMA_CONTRACT_TARGET_MODE="$target_mode" \
    DMT_SCHEMA_CONTRACT_TABLES="$tables_mode" \
    DMT_SCHEMA_CONTRACT_COLUMNS="$columns_mode" \
    DMT_SCHEMA_CONTRACT_DATA_TYPE="$data_type_mode" \
        "$REPO_ROOT/dmt" -c "$CONFIG" run --confirm-backup 2>&1 | tee -a "$LOG"
    local status="${PIPESTATUS[0]}"
    set -e
    if [[ "$status" -eq 0 ]]; then
        echo "  FAIL: $label succeeded, expected schema contract failure" | tee -a "$LOG"
        fail=1
    else
        echo "  OK:   $label failed as expected" | tee -a "$LOG"
    fi
}

reset_case() {
    local name="$1"
    case_dir="$WORK_DIR/$name"
    rm -rf "$case_dir"
    mkdir -p "$case_dir/state"

    export SQLITE_CONTRACT_SOURCE_DB="$case_dir/source.db"
    export SQLITE_CONTRACT_TARGET_DB="$case_dir/target.db"
    export DMT_STATE_DIR="$case_dir/state"

    sqlite3 "$SQLITE_CONTRACT_SOURCE_DB" < "$BASE_FIXTURE"
    run_dmt "drop_recreate" "report" "report" "report" "$name: baseline snapshot"
}

assert_eq() {
    local label="$1"
    local got="$2"
    local want="$3"

    got="$(echo "$got" | tr -d '\r' | sed 's/[[:space:]]*$//')"
    if [[ "$got" != "$want" ]]; then
        echo "  FAIL: $label got='$got' want='$want'" | tee -a "$LOG"
        fail=1
    else
        echo "  OK:   $label" | tee -a "$LOG"
    fi
}

target_sql() {
    sqlite3 "$SQLITE_CONTRACT_TARGET_DB" "$1"
}

target_column_type() {
    local table="$1"
    local column="$2"
    target_sql "SELECT lower(type) FROM pragma_table_info('$table') WHERE name = '$column';"
}

state_sql() {
    sqlite3 "$DMT_STATE_DIR/migrate.db" "$1"
}

assert_target_sql() {
    local label="$1"
    local query="$2"
    local want="$3"
    assert_eq "$label" "$(target_sql "$query")" "$want"
}

latest_snapshot_json() {
    local table="$1"
    state_sql "SELECT schema_json FROM schema_snapshots WHERE table_name = '$table' ORDER BY id DESC LIMIT 1;"
}

snapshot_count() {
    local table="$1"
    state_sql "SELECT COUNT(*) FROM schema_snapshots WHERE table_name = '$table';"
}

assert_snapshot_appended() {
    local table="$1"
    local before="$2"
    assert_eq "snapshot $table appended" "$(snapshot_count "$table")" "$((before + 1))"
}

assert_snapshot_unchanged() {
    local table="$1"
    local before="$2"
    assert_eq "snapshot $table unchanged" "$(snapshot_count "$table")" "$before"
}

assert_snapshot_contains() {
    local table="$1"
    local needle="$2"
    local json
    json="$(latest_snapshot_json "$table")"
    if [[ "$json" == *"$needle"* ]]; then
        echo "  OK:   snapshot $table contains $needle" | tee -a "$LOG"
    else
        echo "  FAIL: snapshot $table missing $needle" | tee -a "$LOG"
        fail=1
    fi
}

assert_snapshot_omits() {
    local table="$1"
    local needle="$2"
    local json
    json="$(latest_snapshot_json "$table")"
    if [[ "$json" == *"$needle"* ]]; then
        echo "  FAIL: snapshot $table unexpectedly contains $needle" | tee -a "$LOG"
        fail=1
    else
        echo "  OK:   snapshot $table omits $needle" | tee -a "$LOG"
    fi
}

rebuild_users_with_age_type() {
    local age_type="$1"
    sqlite3 "$SQLITE_CONTRACT_SOURCE_DB" <<SQL
PRAGMA foreign_keys = OFF;
CREATE TABLE users_next (
    id     INTEGER     NOT NULL PRIMARY KEY,
    name   VARCHAR(20) NOT NULL,
    email  VARCHAR(80) NULL,
    age    $age_type   NULL,
    status VARCHAR(20) NULL,
    notes  TEXT        NULL
);
INSERT INTO users_next (id, name, email, age, status, notes)
SELECT id, name, email, age, status, notes FROM users;
DROP TABLE users;
ALTER TABLE users_next RENAME TO users;
SQL
}

echo ""
echo "=== Scenario: tables=evolve and columns=evolve ==="
reset_case "tables-columns-evolve"
before_users_snapshots="$(snapshot_count "users")"
before_announcements_snapshots="$(snapshot_count "source_announcements")"
sqlite3 "$SQLITE_CONTRACT_SOURCE_DB" <<'SQL'
ALTER TABLE users ADD COLUMN nickname VARCHAR(40) NULL;
UPDATE users SET nickname = 'countess' WHERE id = 1;
CREATE TABLE source_announcements (
    id    INTEGER     NOT NULL PRIMARY KEY,
    label VARCHAR(40) NOT NULL
);
INSERT INTO source_announcements (id, label) VALUES (1, 'new table'), (2, 'second table row');
SQL
run_dmt "upsert" "evolve" "evolve" "report" "tables-columns-evolve: contract follow-up"
assert_target_sql "evolved table row count" "SELECT COUNT(*) FROM source_announcements;" "2"
assert_target_sql "evolved column value transferred" "SELECT nickname FROM users WHERE id = 1;" "countess"
assert_target_sql "evolved column exists on target" "SELECT COUNT(*) FROM pragma_table_info('users') WHERE name = 'nickname';" "1"
assert_snapshot_appended "users" "$before_users_snapshots"
assert_snapshot_appended "source_announcements" "$before_announcements_snapshots"
assert_snapshot_contains "users" '"name":"nickname"'
assert_snapshot_contains "source_announcements" '"name":"source_announcements"'

echo ""
echo "=== Scenario: columns=freeze ==="
reset_case "columns-freeze"
before_users_snapshots="$(snapshot_count "users")"
sqlite3 "$SQLITE_CONTRACT_SOURCE_DB" "ALTER TABLE users ADD COLUMN frozen_note TEXT NULL;"
run_dmt_expect_fail "upsert" "report" "freeze" "report" "columns-freeze: contract follow-up"
assert_target_sql "frozen column not added" "SELECT COUNT(*) FROM pragma_table_info('users') WHERE name = 'frozen_note';" "0"
assert_snapshot_unchanged "users" "$before_users_snapshots"
assert_snapshot_omits "users" '"name":"frozen_note"'

echo ""
echo "=== Scenario: columns=discard_value ==="
reset_case "columns-discard-value"
before_users_snapshots="$(snapshot_count "users")"
sqlite3 "$SQLITE_CONTRACT_SOURCE_DB" <<'SQL'
ALTER TABLE users ADD COLUMN ignored_note TEXT NULL;
UPDATE users SET name = 'Ada Lovelace', ignored_note = 'hidden' WHERE id = 1;
INSERT INTO users (id, name, email, age, status, notes, ignored_note)
VALUES (3, 'Katherine', 'katherine@example.test', 31, 'active', 'new source row', 'hidden too');
SQL
run_dmt "upsert" "report" "discard_value" "report" "columns-discard-value: contract follow-up"
assert_target_sql "discarded column absent" "SELECT COUNT(*) FROM pragma_table_info('users') WHERE name = 'ignored_note';" "0"
assert_target_sql "non-discarded update transferred" "SELECT name FROM users WHERE id = 1;" "Ada Lovelace"
assert_target_sql "new row transferred without discarded column" "SELECT COUNT(*) FROM users;" "3"
assert_snapshot_appended "users" "$before_users_snapshots"
assert_snapshot_omits "users" '"name":"ignored_note"'

echo ""
echo "=== Scenario: columns=discard_row ==="
reset_case "columns-discard-row"
before_users_snapshots="$(snapshot_count "users")"
before_orders_snapshots="$(snapshot_count "orders")"
sqlite3 "$SQLITE_CONTRACT_SOURCE_DB" <<'SQL'
ALTER TABLE users ADD COLUMN row_contract_note TEXT NULL;
UPDATE users SET name = 'Ada skipped', row_contract_note = 'skip this table' WHERE id = 1;
INSERT INTO users (id, name, email, age, status, notes, row_contract_note)
VALUES (3, 'Skipped', 'skipped@example.test', 28, 'active', 'should not transfer', 'skip');
UPDATE orders SET amount = 777 WHERE id = 10;
SQL
run_dmt "upsert" "report" "discard_row" "report" "columns-discard-row: contract follow-up"
assert_target_sql "discard_row leaves users row count unchanged" "SELECT COUNT(*) FROM users;" "2"
assert_target_sql "discard_row skips users update" "SELECT name FROM users WHERE id = 1;" "Ada"
assert_target_sql "discard_row leaves target schema unchanged" "SELECT COUNT(*) FROM pragma_table_info('users') WHERE name = 'row_contract_note';" "0"
assert_target_sql "unaffected table still transfers" "SELECT amount FROM orders WHERE id = 10;" "777"
assert_snapshot_unchanged "users" "$before_users_snapshots"
assert_snapshot_appended "orders" "$before_orders_snapshots"
assert_snapshot_omits "users" '"name":"row_contract_note"'

echo ""
echo "=== Scenario: data_type=freeze ==="
reset_case "data-type-freeze"
baseline_age_target_type="$(target_column_type "users" "age")"
before_users_snapshots="$(snapshot_count "users")"
rebuild_users_with_age_type "TEXT"
run_dmt_expect_fail "upsert" "report" "report" "freeze" "data-type-freeze: contract follow-up"
assert_eq "frozen data type target unchanged" "$(target_column_type "users" "age")" "$baseline_age_target_type"
assert_snapshot_unchanged "users" "$before_users_snapshots"
assert_snapshot_contains "users" '"name":"age","data_type":"integer"'

echo ""
echo "=== Scenario: data_type=discard_value ==="
reset_case "data-type-discard-value"
baseline_age_target_type="$(target_column_type "users" "age")"
before_users_snapshots="$(snapshot_count "users")"
rebuild_users_with_age_type "BIGINT"
sqlite3 "$SQLITE_CONTRACT_SOURCE_DB" "UPDATE users SET name = 'Ada type discard', age = 99 WHERE id = 1;"
run_dmt "upsert" "report" "report" "discard_value" "data-type-discard-value: contract follow-up"
assert_target_sql "data_type discard transfers unaffected column" "SELECT name FROM users WHERE id = 1;" "Ada type discard"
assert_target_sql "data_type discard leaves omitted value unchanged" "SELECT age FROM users WHERE id = 1;" "34"
assert_eq "data_type discard leaves target type unchanged" "$(target_column_type "users" "age")" "$baseline_age_target_type"
assert_snapshot_appended "users" "$before_users_snapshots"
assert_snapshot_contains "users" '"name":"age","data_type":"integer"'

if [[ "$fail" -ne 0 ]]; then
    echo ""
    echo "Schema contract SQLite integration test FAILED - see $LOG."
    exit 1
fi

echo ""
echo "Schema contract SQLite integration test PASSED."
