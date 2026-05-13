# Minimum Database Privileges

These are the minimum privileges dmt needs to run a migration. The
`dmt preflight` subcommand verifies every privilege listed here against
the live database — if `dmt preflight -c your-config.yaml` passes,
your grants are sufficient.

> **Source-of-truth**: this doc is hand-derived from
> `internal/driver/postgres/preflight.go`,
> `internal/driver/mssql/preflight.go`, and
> `internal/driver/mysql/preflight.go`. If you find a divergence between
> what preflight checks and what's documented here, the code wins —
> please open an issue.

If the operator running `dmt` is a DBA reviewing a deployment request,
the per-driver `GRANT` blocks below are exactly the SQL to issue. Each
target is split into two sub-roles because `target_mode: drop_recreate`
and `target_mode: upsert` need genuinely different privilege sets — a
production upsert job should NOT carry DDL rights.

## A note on `target_mode: drop_recreate` and the backup acknowledgment

`drop_recreate` against a non-empty target schema is blocked at
preflight unless the operator passes `--confirm-backup`. The probe that
enumerates non-empty tables needs read access to the database's
catalog views:

- **PostgreSQL** — `pg_class`, `pg_namespace` (readable by every role
  by default; only matters if your installation revokes default
  catalog grants)
- **SQL Server** — `sys.tables`, `sys.schemas`, `sys.partitions`
  (granted to `db_datareader` and `db_owner`)
- **MySQL/MariaDB** — `information_schema.TABLES` (granted to every
  authenticated user by default)

If the probe can't run, preflight fails closed with a backup-ack error
rather than letting `drop_recreate` proceed against potentially
populated tables. Operators on locked-down catalogs should grant the
catalog read, fix the connection, or re-run with `--confirm-backup` to
explicitly acknowledge the risk.

---

## PostgreSQL

Tested against PG 12+ (preflight enforces 12 as the floor — earlier
versions are EOL).

### Source role (read-only)

```sql
-- Replace <role>, <password>, <schema> with your values.
CREATE ROLE <role> LOGIN PASSWORD '<password>';

-- USAGE on the schema lets dmt resolve table OIDs and column metadata
-- via pg_catalog. Preflight checks for this explicitly.
GRANT USAGE ON SCHEMA <schema> TO <role>;

-- SELECT on existing tables is needed to stream rows via the COPY
-- protocol. Per-table SELECT is granular but ALL TABLES is what most
-- production deployments use.
GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO <role>;

-- Future-proof against new tables added between deployment and
-- migration. Without this, a table created after the role was
-- provisioned would be invisible.
ALTER DEFAULT PRIVILEGES IN SCHEMA <schema>
  GRANT SELECT ON TABLES TO <role>;
```

### Target role (`target_mode: drop_recreate`)

```sql
CREATE ROLE <role> LOGIN PASSWORD '<password>';

-- USAGE: required to resolve objects in the schema.
-- CREATE: required to CREATE TABLE during the schema-build phase, and
-- to CREATE INDEX / FK / CHECK in the post-load phases. Preflight
-- probes both explicitly.
GRANT USAGE, CREATE ON SCHEMA <schema> TO <role>;

-- ALL on existing tables: drop_recreate may DROP + recreate tables
-- that already exist in the target. INSERT covers the bulk-load
-- phase. ALL is the safest grant for a drop_recreate target.
GRANT ALL ON ALL TABLES IN SCHEMA <schema> TO <role>;

-- Default privileges so any tables dmt creates remain manageable by
-- this role on a subsequent run.
ALTER DEFAULT PRIVILEGES IN SCHEMA <schema>
  GRANT ALL ON TABLES TO <role>;
```

### Target role (`target_mode: upsert`)

```sql
-- Assumes the target tables already exist (upsert never creates DDL).
GRANT USAGE, CREATE ON SCHEMA <schema> TO <role>;

-- SELECT for the merge probe, INSERT for new rows, UPDATE for
-- changed rows. Per-table grants are appropriate here since the
-- table list is known and stable.
GRANT SELECT, INSERT, UPDATE
  ON TABLE <schema>.<table1>, <schema>.<table2>, <schema>.<...>
  TO <role>;
```

> dmt's upsert path uses temp staging tables. Temp tables in
> PostgreSQL live in a per-session schema and don't require `CREATE`
> on the user schema — but preflight still probes for it because the
> identity/sequence-reset phase may need DDL rights. If your
> deployment can't grant `CREATE` to the upsert role, set
> `migration.reset_sequences: false` and skip the `privileges.schema_create`
> check via `--skip-preflight=privileges.schema_create`.

### Optional: pool-headroom probe

dmt's preflight reads `pg_stat_activity` to compute connection-pool
headroom. Every role can read its own sessions; reading other
sessions requires `pg_read_all_stats`:

```sql
GRANT pg_read_all_stats TO <role>;
```

This is **optional** — dmt's preflight degrades gracefully when this
is denied (the pool-headroom finding becomes info-level rather than a
hard fail). See [docs/OBSERVABILITY.md](OBSERVABILITY.md) for the
pool-headroom metric semantics.

---

## SQL Server

Tested against SQL Server 2016+ (preflight enforces major version 13
as the floor — older versions lack JSON and have edge cases dmt's
bulk-copy path doesn't accommodate).

### Source role (read-only)

```sql
-- Replace <login>, <password>, <db>, <user>, <schema> with your values.
USE master;
CREATE LOGIN <login> WITH PASSWORD = '<password>';

USE <db>;
CREATE USER <user> FOR LOGIN <login>;

-- Database-scoped SELECT covers reading any user table in the
-- database. Preflight calls HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE',
-- 'SELECT') and expects 1.
GRANT SELECT TO <user>;

-- db_datareader is the conventional alternative — granting the role
-- is equivalent for preflight and avoids granting blanket SELECT to
-- system schemas:
-- ALTER ROLE db_datareader ADD MEMBER <user>;
```

### Target role (`target_mode: drop_recreate`)

```sql
USE master;
CREATE LOGIN <login> WITH PASSWORD = '<password>';

USE <db>;
CREATE USER <user> FOR LOGIN <login>;

-- CREATE TABLE: required for the schema-build phase. Preflight
-- probes this explicitly. Also needed for CREATE INDEX, CREATE
-- VIEW, and similar DDL — granting CREATE TABLE at database scope
-- generally implies sufficient rights for related DDL.
GRANT CREATE TABLE TO <user>;

-- INSERT: required to bulk-load via the TDS bulk-copy protocol.
-- Preflight probes this explicitly.
GRANT INSERT TO <user>;

-- ALTER + REFERENCES + CONTROL on the target schema covers
-- DROP TABLE (drop_recreate must drop existing tables first),
-- ALTER TABLE ADD CONSTRAINT for FKs, and SCHEMA-level operations.
-- Preflight does not probe these directly, but the drop_recreate
-- strategy needs them to succeed.
GRANT ALTER, REFERENCES, CONTROL ON SCHEMA::<schema> TO <user>;

-- Simpler alternative: db_owner makes everything above implicit.
-- Use this for development environments; production should prefer
-- the granular grants above.
-- ALTER ROLE db_owner ADD MEMBER <user>;
```

### Target role (`target_mode: upsert`)

```sql
USE <db>;

-- INSERT for new rows, UPDATE for changed rows. Preflight probes
-- both explicitly.
GRANT INSERT TO <user>;
GRANT UPDATE TO <user>;

-- SELECT for the merge probe (matches `target_mode: upsert` MERGE
-- statement). Not currently in preflight's MSSQL upsert check, but
-- the merge statement reads from the target before writing.
GRANT SELECT TO <user>;

-- Granular per-table alternative if you want to limit the upsert
-- target to a known table list:
-- GRANT SELECT, INSERT, UPDATE ON <schema>.<table> TO <user>;
```

dmt's MSSQL upsert path uses temp staging tables, which live in
`tempdb` — dmt does NOT need `CREATE TABLE` on the user database for
upsert mode (preflight skips that check on the upsert path).

### Optional: pool-headroom probe (`VIEW SERVER STATE`)

dmt's preflight reads `sys.dm_exec_sessions` to compute
connection-pool headroom. The DMV requires server-scoped
`VIEW SERVER STATE`:

```sql
USE master;
GRANT VIEW SERVER STATE TO <login>;
```

This is **optional** — dmt's preflight degrades gracefully when this
is denied (the pool-headroom finding becomes info-level rather than a
hard fail). See [docs/OBSERVABILITY.md](OBSERVABILITY.md) for context.

On Azure SQL Database `VIEW SERVER STATE` doesn't exist; the
equivalent is `VIEW DATABASE STATE`, which preflight currently does
not check. Pool headroom on Azure SQL DB is best left to the Azure
portal's connection metrics.

---

## MySQL / MariaDB

Tested against MySQL 5.7+ and MariaDB 10.3+ (preflight enforces these
as the floor — earlier versions lack JSON and 4-byte UTF-8
(`utf8mb4`) defaults).

### Source role (read-only)

```sql
-- Replace <user>, <host>, <password>, <db> with your values.
CREATE USER '<user>'@'<host>' IDENTIFIED BY '<password>';

-- SELECT on every table in the source database. Preflight calls
-- SHOW GRANTS and looks for SELECT scoped to <db>.* (or global *.*).
GRANT SELECT ON `<db>`.* TO '<user>'@'<host>';
```

### Target role (`target_mode: drop_recreate`)

```sql
CREATE USER '<user>'@'<host>' IDENTIFIED BY '<password>';

-- CREATE: for CREATE TABLE during schema-build.
-- DROP: drop_recreate replaces existing tables.
-- INSERT: bulk-load via multi-row INSERT.
-- Preflight checks all three via SHOW GRANTS.
GRANT CREATE, DROP, INSERT ON `<db>`.* TO '<user>'@'<host>';

-- ALTER + INDEX + REFERENCES are also needed for the
-- post-load DDL phases (CREATE INDEX, CREATE FOREIGN KEY,
-- CREATE CHECK CONSTRAINT). Preflight does not probe these
-- individually, but they're required for drop_recreate to succeed.
GRANT ALTER, INDEX, REFERENCES ON `<db>`.* TO '<user>'@'<host>';

-- Simpler alternative: ALL PRIVILEGES on the target database.
-- Use this for dev/staging; production should prefer the granular
-- grants above.
-- GRANT ALL PRIVILEGES ON `<db>`.* TO '<user>'@'<host>';
```

### Target role (`target_mode: upsert`)

```sql
CREATE USER '<user>'@'<host>' IDENTIFIED BY '<password>';

-- INSERT for new rows, UPDATE for changed rows, SELECT for the
-- merge probe. Preflight checks all three via SHOW GRANTS.
GRANT INSERT, UPDATE, SELECT ON `<db>`.* TO '<user>'@'<host>';
```

dmt's MySQL upsert uses `INSERT ... ON DUPLICATE KEY UPDATE`, which
needs `INSERT` + `UPDATE` plus `SELECT` for any operations that
read-before-write.

### Probe verification SQL

You can verify your grants without running dmt by running:

```sql
SHOW GRANTS FOR CURRENT_USER();
```

dmt's preflight parses this exact output. The presence of an
`ALL PRIVILEGES` or `ALL` token in the privileges list counts as
satisfying every probe.

---

## Verification

Once the grants above are in place, run preflight to confirm:

```bash
dmt preflight -c your-config.yaml
```

Exit code 0 = all required privileges satisfied. Non-zero output
will name the missing privilege and the `GRANT` statement to fix
it. See [CONTRIBUTING.md](../CONTRIBUTING.md) for local-repro tips
and the test-DB setup used by the integration test.
