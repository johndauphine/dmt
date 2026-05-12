#!/usr/bin/env bash
# Load the SO2010-minimal fixture into a running MSSQL container (#178).
#
# Synthesizes the same 9-table schema + 61 columns as the public
# Brent Ozar StackOverflow2010 dataset but with synthetic seed data
# (~30 rows total). CI-friendly: completes in ~3 seconds, no
# external download. Exercises every column type the real dataset
# uses (int, datetime, nvarchar with explicit lengths, NVARCHAR(MAX)
# via -1, varchar, nullable + NOT NULL mix).
#
# For the *full* StackOverflow2010 (~19M rows, 10 GB compressed):
# see docs/FIXTURES.md — that's a manual .bak restore.

set -euo pipefail

CONTAINER="${MSSQL_CONTAINER:-}"
MSSQL_PASSWORD="${MSSQL_PASSWORD:-TestPass2024}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
SQL_FILE="$SCRIPT_DIR/fixtures/so2010-minimal.sql"

if [[ ! -f "$SQL_FILE" ]]; then
    echo "ERROR: fixture SQL not found at $SQL_FILE" >&2
    exit 1
fi

# Auto-detect a RUNNING container (test preferred, bench fallback).
# `docker inspect` succeeds on stopped containers too, so check
# State.Running explicitly — otherwise a leftover stopped mssql-test
# would shadow an actually-running mssql-bench (Codex review on #178).
is_running() {
    [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == "true" ]]
}

if [[ -z "$CONTAINER" ]]; then
    if is_running mssql-test; then
        CONTAINER=mssql-test
    elif is_running mssql-bench; then
        CONTAINER=mssql-bench
    else
        echo "ERROR: no running mssql-test or mssql-bench container." >&2
        echo "       Run: make test-dbs-up   (or: make bench-dbs-up)" >&2
        exit 1
    fi
fi

# Wait for sqlcmd to be ready (newer SQL Server images stage sqlcmd
# at /opt/mssql-tools18; older ones at /opt/mssql-tools). Both work
# with the same -S/-U/-P/-i flags.
SQLCMD=""
for path in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
    if docker exec "$CONTAINER" test -x "$path"; then
        SQLCMD="$path"
        break
    fi
done
if [[ -z "$SQLCMD" ]]; then
    echo "ERROR: sqlcmd not found in $CONTAINER (checked mssql-tools18 + mssql-tools)" >&2
    exit 1
fi

echo ">>> Loading SO2010-minimal into container=$CONTAINER"

# Wait for SQL Server to accept logins. The MSSQL container reports
# State.Running=true well before sqlservr finishes initializing
# tempdb + master DBs (typically 10-30s after first start). Without
# a readiness loop, fixture loading is flaky in fresh CI / local
# runs (Codex review on #178). Poll up to ~90s — MSSQL can be slow
# on first boot.
echo ">>> Waiting for SQL Server to accept logins..."
for i in $(seq 1 45); do
    if docker exec "$CONTAINER" "$SQLCMD" \
            -S localhost -U sa -P "$MSSQL_PASSWORD" -C -b -Q "SELECT 1" >/dev/null 2>&1; then
        echo ">>> SQL Server ready after ~$((i*2))s"
        break
    fi
    if [[ $i -eq 45 ]]; then
        echo "ERROR: SQL Server in $CONTAINER didn't accept logins within 90s" >&2
        exit 1
    fi
    sleep 2
done

# Copy the SQL into the container then execute it. -C trusts the
# self-signed cert; -b makes sqlcmd exit non-zero on T-SQL errors so
# this script fails loudly on schema/data problems (CI wants that).
docker cp "$SQL_FILE" "$CONTAINER:/tmp/so2010-minimal.sql"
docker exec "$CONTAINER" "$SQLCMD" \
    -S localhost -U sa -P "$MSSQL_PASSWORD" \
    -C -b -i /tmp/so2010-minimal.sql

echo ">>> SO2010-minimal loaded successfully"
