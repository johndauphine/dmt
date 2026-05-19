#!/usr/bin/env bash
# Generic cross-engine integration test driver (#291).
#
# Used by the nightly matrix job in .github/workflows/integration-nightly.yml
# to run any of the 12 cross-engine directed pairs through dmt end-to-end.
# The two per-PR anchor scripts (integration-test.sh for mssql→postgres and
# integration-test-sqlite.sh for sqlite→sqlite) remain authoritative for
# those pairs — this script is intentionally not a refactor that absorbs
# them; it's the matrix-specific runner.
#
# Usage:
#   integration-test-pair.sh --pair <source>-<target>
#
#   <source>, <target> ∈ {mssql, pg, mysql, sqlite}
#
# Supported pairs (the 12 nightly cross-engine pairs):
#   mssql-pg       mssql-mysql    mssql-sqlite
#   pg-mssql       pg-mysql       pg-sqlite
#   mysql-mssql    mysql-pg       mysql-sqlite
#   sqlite-mssql   sqlite-pg      sqlite-mysql
#
# Same-engine pairs (e.g. mssql-mssql) are out of scope by design — a
# round-trip through the same engine doesn't exercise the cross-DB type
# mapper, which is the whole point of this matrix.
#
# Per-engine prerequisites assumed by the script (the workflow installs
# them upstream):
#   - mssql: sqlcmd on PATH, mssql service reachable on localhost:1433
#   - pg:    psql on PATH,   pg service reachable on localhost:5432
#   - mysql: mysql on PATH,  mysql service reachable on localhost:3306
#   - sqlite: sqlite3 on PATH (no service)
#
# Validation: relies on `dmt validate` as the canonical row-count check.
# That command runs through dmt's own source/target reader pair, which is
# the same machinery that did the migration — so any case-sanitization
# quirks per target engine are handled by dmt itself rather than re-encoded
# here per target.

set -euo pipefail

PAIR=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --pair) PAIR="${2:-}"; shift 2 || { echo "ERROR: --pair requires a value" >&2; exit 2; } ;;
        --pair=*) PAIR="${1#--pair=}"; shift ;;
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
        *) echo "ERROR: unknown argument: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$PAIR" ]]; then
    echo "ERROR: --pair required (e.g. --pair pg-mssql)" >&2
    exit 2
fi

SRC="${PAIR%-*}"
TGT="${PAIR#*-}"

case "$SRC" in mssql|pg|mysql|sqlite) ;; *) echo "ERROR: bad source engine '$SRC' (mssql|pg|mysql|sqlite)" >&2; exit 2 ;; esac
case "$TGT" in mssql|pg|mysql|sqlite) ;; *) echo "ERROR: bad target engine '$TGT' (mssql|pg|mysql|sqlite)" >&2; exit 2 ;; esac
if [[ "$SRC" == "$TGT" ]]; then
    echo "ERROR: same-engine pair '$PAIR' is out of scope for the cross-engine matrix" >&2
    exit 2
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG="$REPO_ROOT/scripts/fixtures/ci-${SRC}-${TGT}.yaml"
LOG="${INTEGRATION_TEST_LOG:-/tmp/dmt-ci-${SRC}-${TGT}.log}"

# Passwords default to the test value used by the per-PR anchors so a
# local run with `make test-dbs-up && make mysql-bench-up` works without
# extra exports.
export MSSQL_PASSWORD="${MSSQL_PASSWORD:-TestPass2024}"
export PG_PASSWORD="${PG_PASSWORD:-TestPass2024}"
export MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-TestPass2024}"

# Isolate dmt state so the matrix runs don't clobber the operator's
# ~/.dmt/migrate.db. Same isolation as the two per-PR anchors.
DMT_STATE_DIR="${DMT_STATE_DIR:-$REPO_ROOT/.dmt-ci-${SRC}-${TGT}-state}"
export DMT_STATE_DIR

# SQLite-specific paths. Always exported so configs that reference these
# (even when sqlite is not in the pair) parse cleanly — dmt's secret
# expander would fail on an unset ${env:VAR}.
WORK_DIR="${SQLITE_WORK_DIR:-$REPO_ROOT/.dmt-ci-${SRC}-${TGT}-work}"
export SQLITE_SOURCE_DB="${SQLITE_SOURCE_DB:-$WORK_DIR/source.db}"
export SQLITE_TARGET_DB="${SQLITE_TARGET_DB:-$WORK_DIR/target.db}"

if [[ ! -x "$REPO_ROOT/dmt" ]]; then
    echo "ERROR: dmt binary not found at $REPO_ROOT/dmt — run 'make build' first" >&2
    exit 1
fi
if [[ ! -f "$CONFIG" ]]; then
    echo "ERROR: config not found at $CONFIG" >&2
    exit 1
fi

echo "=== dmt $SRC → $TGT (cross-engine integration test) ==="
echo "  config:    $CONFIG"
echo "  state dir: $DMT_STATE_DIR"
echo "  log:       $LOG"

# Hermetic reset of dmt state. A stale checkpoint from a previous run
# would otherwise engage the resume path mid-test.
rm -rf "$DMT_STATE_DIR" "$WORK_DIR"
mkdir -p "$DMT_STATE_DIR" "$WORK_DIR"
: > "$LOG"

# ---------- source-side fixture loaders ----------
# Each loader is responsible for putting the SO2010-minimal fixture into
# the source endpoint in whatever shape that endpoint expects. The
# per-engine fixture files (scripts/fixtures/so2010-minimal{,-pg,-mysql,-sqlite}.sql)
# are byte-aligned on data, so any source produces the same target after
# migration.

load_source_mssql() {
    sqlcmd -S "localhost,1433" -U sa -P "$MSSQL_PASSWORD" -C -b \
        -i "$REPO_ROOT/scripts/fixtures/so2010-minimal.sql"
}

load_source_pg() {
    PGPASSWORD="$PG_PASSWORD" psql -h localhost -p 5432 -U postgres -d postgres \
        -v ON_ERROR_STOP=1 -q \
        -c "DROP DATABASE IF EXISTS so2010_minimal_src;" \
        -c "CREATE DATABASE so2010_minimal_src;"
    PGPASSWORD="$PG_PASSWORD" psql -h localhost -p 5432 -U postgres \
        -d so2010_minimal_src -v ON_ERROR_STOP=1 \
        -f "$REPO_ROOT/scripts/fixtures/so2010-minimal-pg.sql"
}

load_source_mysql() {
    mysql -h 127.0.0.1 -P 3306 -uroot -p"$MYSQL_ROOT_PASSWORD" \
        < "$REPO_ROOT/scripts/fixtures/so2010-minimal-mysql.sql"
}

load_source_sqlite() {
    sqlite3 "$SQLITE_SOURCE_DB" < "$REPO_ROOT/scripts/fixtures/so2010-minimal-sqlite.sql"
}

# ---------- target-side preparation ----------
# dmt's drop_recreate target_mode handles table-level reset, but the
# DATABASE itself has to exist before dmt connects. These preparers
# ensure a fresh target DB so prior runs don't leak in.

prep_target_mssql() {
    sqlcmd -S "localhost,1433" -U sa -P "$MSSQL_PASSWORD" -C -b \
        -Q "IF DB_ID('so2010_minimal_ci') IS NOT NULL DROP DATABASE so2010_minimal_ci; CREATE DATABASE so2010_minimal_ci;"
}

prep_target_pg() {
    PGPASSWORD="$PG_PASSWORD" psql -h localhost -p 5432 -U postgres -d postgres \
        -v ON_ERROR_STOP=1 -q \
        -c "DROP DATABASE IF EXISTS so2010_minimal_ci;" \
        -c "CREATE DATABASE so2010_minimal_ci;"
}

prep_target_mysql() {
    mysql -h 127.0.0.1 -P 3306 -uroot -p"$MYSQL_ROOT_PASSWORD" \
        -e "DROP DATABASE IF EXISTS so2010_minimal_ci; CREATE DATABASE so2010_minimal_ci CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
}

prep_target_sqlite() {
    # SQLite "DB creation" is just having an empty file — dmt will create
    # tables on first write. rm is a no-op if the file doesn't exist.
    rm -f "$SQLITE_TARGET_DB"
}

# ---------- dispatch ----------

echo ""
echo "=== Load source fixture ($SRC) ==="
"load_source_$SRC"

echo ""
echo "=== Prepare target ($TGT) ==="
"prep_target_$TGT"

echo ""
echo "=== Run dmt migration ==="
"$REPO_ROOT/dmt" -c "$CONFIG" run --confirm-backup 2>&1 | tee -a "$LOG"

echo ""
echo "=== Validate target (dmt validate) ==="
# dmt validate is the canonical row-count parity check. It runs through
# dmt's own source/target readers, so any target-side case sanitization
# is handled in-engine rather than re-encoded here per target.
"$REPO_ROOT/dmt" -c "$CONFIG" validate 2>&1 | tee -a "$LOG"

echo ""
echo "Integration test PASSED ($SRC → $TGT)."
