#!/usr/bin/env bash
# Load the SO2010-minimal fixture into a running source-engine container.
#
# Originally mssql-only (#178); extended (#291) to also load the PG- and
# MySQL-dialect variants so the cross-engine integration matrix has a
# real source-side fixture for each engine.
#
# Usage:
#   ./scripts/load-fixture-so2010-minimal.sh [--source mssql|pg|mysql]
#
# Default --source is `mssql` for back-compat with the original callers
# (the existing mssql→postgres integration test, docs/FIXTURES.md, and
# `make load-fixture-so2010-minimal`).
#
# Per-source behavior:
#   mssql: Loads scripts/fixtures/so2010-minimal.sql into the running
#          mssql-test (preferred) or mssql-bench container via
#          sqlcmd. The .sql file creates the StackOverflow2010Minimal
#          database itself.
#   pg:    Loads scripts/fixtures/so2010-minimal-pg.sql into the
#          running pg-test (preferred) or pg-bench container via
#          psql. The loader drops+creates the `so2010_minimal_src`
#          database first (PG can't CREATE DATABASE from inside a
#          connection to the same DB).
#   mysql: Loads scripts/fixtures/so2010-minimal-mysql.sql into the
#          running mysql-bench container via the mysql CLI. The .sql
#          file creates `so2010_minimal_src` and USEs it.
#
# For the *full* StackOverflow2010 (~19M rows, 10 GB compressed):
# see docs/FIXTURES.md — that's a manual .bak restore.

set -euo pipefail

SOURCE="mssql"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --source)
            SOURCE="${2:-}"
            shift 2 || { echo "ERROR: --source requires a value" >&2; exit 2; }
            ;;
        --source=*)
            SOURCE="${1#--source=}"
            shift
            ;;
        -h|--help)
            sed -n '2,30p' "$0"
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            echo "Usage: $(basename "$0") [--source mssql|pg|mysql]" >&2
            exit 2
            ;;
    esac
done

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# Reusable: return 0 iff the named container is currently Running.
# `docker inspect` succeeds on stopped containers too, so we have to
# check State.Running explicitly — otherwise a leftover stopped
# `pg-test` (etc.) would shadow an actually-running `pg-bench`
# (Codex review on #178).
is_running() {
    [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == "true" ]]
}

# Pick the first running container from a preference-ordered list, or
# fail with a hint about the right `make ...-up` target.
pick_container() {
    local var_override="$1"; shift
    local hint="$1"; shift
    if [[ -n "${!var_override:-}" ]]; then
        printf '%s' "${!var_override}"
        return 0
    fi
    local candidate
    for candidate in "$@"; do
        if is_running "$candidate"; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    echo "ERROR: no running $hint container (checked: $*)." >&2
    echo "       Run: $hint" >&2
    return 1
}

load_mssql() {
    local sql_file="$SCRIPT_DIR/fixtures/so2010-minimal.sql"
    local container password sqlcmd
    [[ -f "$sql_file" ]] || { echo "ERROR: $sql_file not found" >&2; exit 1; }
    container="$(pick_container MSSQL_CONTAINER "make test-dbs-up   (or: make bench-dbs-up)" mssql-test mssql-bench)" || exit 1
    password="${MSSQL_PASSWORD:-TestPass2024}"

    # Newer SQL Server images stage sqlcmd at /opt/mssql-tools18; older
    # ones at /opt/mssql-tools. Both accept the same -S/-U/-P/-i flags.
    sqlcmd=""
    for path in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
        if docker exec "$container" test -x "$path"; then
            sqlcmd="$path"; break
        fi
    done
    if [[ -z "$sqlcmd" ]]; then
        echo "ERROR: sqlcmd not found in $container (checked mssql-tools18 + mssql-tools)" >&2
        exit 1
    fi

    echo ">>> Loading SO2010-minimal (mssql) into container=$container"
    # Wait for SQL Server to accept logins. State.Running=true is set
    # well before sqlservr finishes initializing tempdb + master (10-30s
    # after first start). Poll up to ~90s; MSSQL can be slow on first
    # boot (Codex review on #178).
    echo ">>> Waiting for SQL Server to accept logins..."
    for i in $(seq 1 45); do
        if docker exec "$container" "$sqlcmd" \
                -S localhost -U sa -P "$password" -C -b -Q "SELECT 1" >/dev/null 2>&1; then
            echo ">>> SQL Server ready after ~$((i*2))s"
            break
        fi
        if [[ $i -eq 45 ]]; then
            echo "ERROR: SQL Server in $container didn't accept logins within 90s" >&2
            exit 1
        fi
        sleep 2
    done

    # -C trusts the self-signed cert; -b makes sqlcmd exit non-zero on
    # T-SQL errors so the script fails loud on schema/data problems.
    docker cp "$sql_file" "$container:/tmp/so2010-minimal.sql"
    docker exec "$container" "$sqlcmd" \
        -S localhost -U sa -P "$password" \
        -C -b -i /tmp/so2010-minimal.sql
    echo ">>> SO2010-minimal (mssql) loaded successfully"
}

load_pg() {
    local sql_file="$SCRIPT_DIR/fixtures/so2010-minimal-pg.sql"
    local container password db
    [[ -f "$sql_file" ]] || { echo "ERROR: $sql_file not found" >&2; exit 1; }
    container="$(pick_container PG_CONTAINER "make test-dbs-up   (or: make bench-dbs-up)" pg-test pg-bench)" || exit 1
    password="${PG_PASSWORD:-TestPass2024}"
    db="${PG_FIXTURE_DB:-so2010_minimal_src}"

    echo ">>> Loading SO2010-minimal (pg) into container=$container db=$db"
    # Wait for PG to accept connections. pg_isready is in postgres:16
    # by default; State.Running=true precedes accepting connections.
    echo ">>> Waiting for PostgreSQL to accept connections..."
    for i in $(seq 1 45); do
        if docker exec "$container" pg_isready -U postgres -q 2>/dev/null; then
            echo ">>> PostgreSQL ready after ~$((i*2))s"
            break
        fi
        if [[ $i -eq 45 ]]; then
            echo "ERROR: PostgreSQL in $container didn't accept connections within 90s" >&2
            exit 1
        fi
        sleep 2
    done

    # Drop+recreate the fixture DB so re-runs are hermetic. CREATE
    # DATABASE has to be issued from a *different* DB than the target,
    # so we run it against the bootstrap `postgres` database.
    docker exec -e PGPASSWORD="$password" "$container" \
        psql -U postgres -d postgres -v ON_ERROR_STOP=1 -q \
        -c "DROP DATABASE IF EXISTS $db;" -c "CREATE DATABASE $db;"

    # Copy the SQL into the container then execute it. Single-tx
    # semantics come from the explicit BEGIN/COMMIT inside the
    # .sql file itself (see scripts/fixtures/so2010-minimal-pg.sql),
    # so we deliberately do not pass psql's `-1` flag here — that
    # would create a nested transaction and produce a harmless but
    # noisy "WARNING: there is already a transaction in progress"
    # on every run. ON_ERROR_STOP=1 still aborts the load on any
    # T-SQL error, which is what CI wants.
    docker cp "$sql_file" "$container:/tmp/so2010-minimal-pg.sql"
    docker exec -e PGPASSWORD="$password" "$container" \
        psql -U postgres -d "$db" -v ON_ERROR_STOP=1 \
        -f /tmp/so2010-minimal-pg.sql
    echo ">>> SO2010-minimal (pg) loaded successfully"
}

load_mysql() {
    local sql_file="$SCRIPT_DIR/fixtures/so2010-minimal-mysql.sql"
    local container password
    [[ -f "$sql_file" ]] || { echo "ERROR: $sql_file not found" >&2; exit 1; }
    # No mysql-test container in the Makefile today — only mysql-bench
    # (see make mysql-bench-up). If a future PR adds mysql-test, prepend
    # it here for symmetry with the mssql/pg paths.
    container="$(pick_container MYSQL_CONTAINER "make mysql-bench-up" mysql-bench)" || exit 1
    password="${MYSQL_ROOT_PASSWORD:-TestPass2024}"

    echo ">>> Loading SO2010-minimal (mysql) into container=$container"
    # Wait for MySQL to accept connections. The mysql:8.0 image emits
    # an early `mysqld --initialize` pass that's listening but not yet
    # accepting client logins; mysqladmin ping is the canonical probe.
    echo ">>> Waiting for MySQL to accept connections..."
    for i in $(seq 1 45); do
        if docker exec "$container" mysqladmin ping -uroot -p"$password" --silent 2>/dev/null; then
            echo ">>> MySQL ready after ~$((i*2))s"
            break
        fi
        if [[ $i -eq 45 ]]; then
            echo "ERROR: MySQL in $container didn't accept connections within 90s" >&2
            exit 1
        fi
        sleep 2
    done

    # The .sql creates so2010_minimal_src + USEs it, so we don't pass
    # -D here. Piping via `docker exec -i ... mysql` rather than
    # docker cp + exec because mysql client reads stdin natively.
    docker exec -i "$container" mysql -uroot -p"$password" < "$sql_file"
    echo ">>> SO2010-minimal (mysql) loaded successfully"
}

case "$SOURCE" in
    mssql) load_mssql ;;
    pg|postgres|postgresql) load_pg ;;
    mysql|mariadb) load_mysql ;;
    "")
        echo "ERROR: --source must be one of: mssql, pg, mysql" >&2
        exit 2
        ;;
    *)
        echo "ERROR: unsupported --source: $SOURCE (expected mssql|pg|mysql)" >&2
        exit 2
        ;;
esac
