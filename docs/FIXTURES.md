# Reference Fixtures

Audit and load procedures for the four reference datasets dmt's tests
and benchmarks target (`#178`). The deterministic type-mapping and DDL
work (`#168` / `#169`) already shipped; this doc keeps the fixture
surface honest going forward and unblocks the CI gating planned in
`#230`.

| Fixture | Source DB | Size | CI-loadable | Loader |
|---|---|---:|---|---|
| **SO2010-minimal** | MSSQL | <1 MB | ✅ | `make load-fixture-so2010-minimal` |
| **pgbench** (scale 1) | PostgreSQL | 16 MB | ✅ | `make load-fixture-pgbench` |
| **SO2010** (full) | MSSQL | ~10 GB compressed, ~40 GB restored | ❌ bench-only | manual `.bak` restore — see below |
| **SO2013** (full) | MSSQL | ~52 GB compressed | ❌ bench-only | manual `.bak` restore — see below |
| **WWI** (Wide World Importers) | MSSQL | ~120 MB compressed | ❌ bench-only (`.bak` restore not yet scripted) | manual restore — see below |

`make test-fixtures-load` runs the CI-friendly subset (SO2010-minimal +
pgbench) in under 5 seconds. Both loaders are idempotent (drop +
recreate on every run) and auto-detect whichever container is up
(`*-test` preferred, `*-bench` fallback).

The full `.bak` archives don't fit a sensible CI step: they each
require multi-GB downloads from third-party hosts (Brent Ozar's CDN
for SO; the Microsoft `sql-server-samples` GitHub release for WWI),
the restored databases run into tens of GB, and restore time is
minutes-to-hours. They stay manual, with the procedures documented
below for repeatable bench runs.

## CI-friendly fixtures

### SO2010-minimal

Synthesizes the exact 9-table schema and 61-column type surface of
the public StackOverflow2010 dataset, then seeds ~30 rows that
exercise every column type the real dataset uses (`INT`, `DATETIME`,
`NVARCHAR(N)` at varied lengths, `NVARCHAR(MAX)`, `VARCHAR(N)`,
nullable + NOT-NULL mix).

```
make bench-dbs-up                          # or `make test-dbs-up`
make load-fixture-so2010-minimal
```

Loads into database `StackOverflow2010Minimal` on the running MSSQL
container. Round-trip migrations (mssql → pg / mssql → mysql) complete
in well under a second.

The SQL itself lives in
[`scripts/fixtures/so2010-minimal.sql`](../scripts/fixtures/so2010-minimal.sql).
A working dmt config that migrates it to Postgres lives at
[`scripts/fixtures/so2010-minimal-test.yaml`](../scripts/fixtures/so2010-minimal-test.yaml).

The DDL is intentionally byte-for-byte compatible with the real
SO2010 column widths (e.g. `Posts.Title NVARCHAR(250)`,
`Comments.Text NVARCHAR(700)`) so a type-mapping change validated
against the minimal fixture is also validated for the full dataset.

### pgbench (scale 1)

PostgreSQL's bundled benchmark fixture. Initializes four tables
(`pgbench_branches`, `pgbench_tellers`, `pgbench_accounts`,
`pgbench_history`) via `pgbench -i`. Default scale 1 produces 100K
accounts in ~16 MB and <1 s wall time.

```
make bench-dbs-up                          # or `make test-dbs-up`
make load-fixture-pgbench
```

For larger smoke runs, override the scale:

```
FIXTURE_SCALE=10 make load-fixture-pgbench   # 1M accounts, ~160 MB
FIXTURE_SCALE=100 make load-fixture-pgbench  # 10M accounts, ~1.6 GB
```

## Bench-only fixtures (manual)

These three reference datasets exist as `.bak` archives that have to
be downloaded and restored manually. They're worth the trouble for
real benchmarks (`docs/BENCHMARKS.md` cites them extensively) but
unsuitable for CI.

### SO2010 (full)

The complete Brent Ozar Stack Overflow 2010 dataset: ~19M rows
across the same 9 tables as SO2010-minimal, ~10 GB compressed and
~40 GB restored.

1. Download `StackOverflow2010.7z` from
   <https://downloads.brentozar.com/StackOverflow2010.7z>.
2. Extract and copy the `.bak` into the running MSSQL container:

   ```bash
   docker cp StackOverflow2010.bak mssql-bench:/var/opt/mssql/backup/
   ```

3. Restore from inside the container:

   ```bash
   docker exec mssql-bench /opt/mssql-tools18/bin/sqlcmd \
       -S localhost -U sa -P "$MSSQL_PASSWORD" -C -Q "
   RESTORE DATABASE StackOverflow2010
   FROM DISK = '/var/opt/mssql/backup/StackOverflow2010.bak'
   WITH MOVE 'StackOverflow2010' TO '/var/opt/mssql/data/StackOverflow2010.mdf',
        MOVE 'StackOverflow2010_log' TO '/var/opt/mssql/data/StackOverflow2010_log.ldf';
   "
   ```

Expected row counts: Posts ~3.7M, Users ~300K, Votes ~9.3M, Comments ~5.5M.

### SO2013 (full)

Same shape as SO2010 but ~5× larger: ~106M rows, ~52 GB compressed.

1. Download `StackOverflow2013.7z` from
   <https://downloads.brentozar.com/StackOverflow2013.7z>.
2. Copy + restore per the SO2010 procedure, substituting "2013" in
   filenames and the `RESTORE DATABASE` target.

Expected row counts: Votes ~52.9M, Comments ~24.5M, Posts ~17.1M,
Badges ~8M, Users ~2.5M.

### WWI (Wide World Importers)

Microsoft's official sample database. Smaller than the SO archives
(~120 MB compressed) but still oversized for CI.

1. Download from the Microsoft sample releases:
   <https://github.com/microsoft/sql-server-samples/releases/tag/wide-world-importers-v1.0>
   (look for `WideWorldImporters-Full.bak` — the OLTP variant).
2. Copy + restore per the SO procedure, target name `WideWorldImporters`.

Scripting WWI as a CI-friendly loader is tracked as a separate
follow-up; it doesn't block the deterministic-driver work that
prompted this audit.

## Verifying a fixture loaded correctly

After running any of the make targets, sanity-check the row counts.
Examples:

```bash
# pgbench
docker exec pg-bench psql -U postgres -d pgbench \
    -c "SELECT count(*) FROM pgbench_accounts;"

# SO2010-minimal
docker exec mssql-bench /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$MSSQL_PASSWORD" -C \
    -d StackOverflow2010Minimal \
    -Q "SELECT 'Users', COUNT(*) FROM Users UNION ALL
        SELECT 'Posts', COUNT(*) FROM Posts UNION ALL
        SELECT 'Votes', COUNT(*) FROM Votes;"
```

A reproducible smoke that round-trips the SO2010-minimal fixture
through dmt to Postgres:

```bash
make build
make bench-dbs-up
make load-fixture-so2010-minimal
docker exec pg-bench psql -U postgres -c "DROP DATABASE IF EXISTS so2010_minimal_target;"
docker exec pg-bench psql -U postgres -c "CREATE DATABASE so2010_minimal_target;"
MSSQL_PASSWORD=TestPass2024 PG_PASSWORD=TestPass2024 \
    ./dmt -c scripts/fixtures/so2010-minimal-test.yaml run
```

Expected: 9 tables, 45 rows total, exit code 0, every table line
shows `OK N rows` in the validation summary.

## Related issues

- #178 — this audit
- #168 / #169 — deterministic type mapping and DDL generation (the
  original consumers of these fixtures)
- #230 — production-readiness CI gating; consumes
  `make test-fixtures-load` to seed cross-DB integration tests
- `docs/BENCHMARKS.md` — full-dataset benchmark results that motivate
  keeping the heavy `.bak` fixtures around even though they aren't
  CI-loadable
