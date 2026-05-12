#!/usr/bin/env bash
# Load the pgbench fixture into the running pg-test or pg-bench container (#178).
#
# pgbench is shipped with PostgreSQL and initializes a four-table TPC-B-like
# benchmark schema (pgbench_branches, pgbench_tellers, pgbench_accounts,
# pgbench_history) via `pgbench -i`. The `-s` scale factor controls size:
#   -s 1   ≈ 100K accounts (~16 MB), runs in <1s — used here for CI.
#   -s 10  ≈ 1M accounts   (~160 MB), runs in seconds — local smoke.
#   -s 100 ≈ 10M accounts  (~1.6 GB), runs in ~minute — bench mode.
#
# We pick -s 1 by default so `make test-fixtures-load` stays under 5s.
# Override with: FIXTURE_SCALE=10 ./scripts/load-fixture-pgbench.sh

set -euo pipefail

CONTAINER="${PG_CONTAINER:-}"
SCALE="${FIXTURE_SCALE:-1}"
DB_NAME="${PG_FIXTURE_DB:-pgbench}"
PG_PASSWORD="${PG_PASSWORD:-TestPass2024}"

# Auto-detect a RUNNING container (test preferred, bench fallback).
# `docker inspect` succeeds on stopped containers too, so check
# State.Running explicitly — otherwise a leftover stopped pg-test
# would shadow an actually-running pg-bench (Codex review on #178).
is_running() {
    [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == "true" ]]
}

if [[ -z "$CONTAINER" ]]; then
    if is_running pg-test; then
        CONTAINER=pg-test
    elif is_running pg-bench; then
        CONTAINER=pg-bench
    else
        echo "ERROR: no running pg-test or pg-bench container." >&2
        echo "       Run: make test-dbs-up   (or: make bench-dbs-up)" >&2
        exit 1
    fi
fi

echo ">>> Loading pgbench (scale=$SCALE) into container=$CONTAINER db=$DB_NAME"

# Wait for Postgres to accept connections. The container can report
# State.Running=true before initdb finishes, so a naïve script that
# fires psql immediately is flaky in fresh CI / local runs (Codex
# review on #178). Poll up to ~60s.
echo ">>> Waiting for Postgres to accept connections..."
for i in $(seq 1 30); do
    if docker exec -e PGPASSWORD="$PG_PASSWORD" "$CONTAINER" \
            psql -U postgres -tAc "SELECT 1" >/dev/null 2>&1; then
        echo ">>> Postgres ready after ~$((i*2))s"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "ERROR: Postgres in $CONTAINER didn't accept connections within 60s" >&2
        exit 1
    fi
    sleep 2
done

# Drop+create for idempotent re-runs. pgbench -i refuses to seed an
# existing schema, and a re-run from a fresh CI checkout should always
# produce identical state.
docker exec -e PGPASSWORD="$PG_PASSWORD" "$CONTAINER" \
    psql -U postgres -tAc "DROP DATABASE IF EXISTS \"$DB_NAME\";" >/dev/null
docker exec -e PGPASSWORD="$PG_PASSWORD" "$CONTAINER" \
    psql -U postgres -tAc "CREATE DATABASE \"$DB_NAME\";" >/dev/null

docker exec -e PGPASSWORD="$PG_PASSWORD" "$CONTAINER" \
    pgbench -i -s "$SCALE" -q -U postgres "$DB_NAME"

# Verify
ROWS=$(docker exec -e PGPASSWORD="$PG_PASSWORD" "$CONTAINER" \
    psql -U postgres -d "$DB_NAME" -tAc "SELECT count(*) FROM pgbench_accounts;")
echo ">>> pgbench loaded: pgbench_accounts has $ROWS rows (scale=$SCALE)"
