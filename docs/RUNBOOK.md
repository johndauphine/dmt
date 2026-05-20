# Production Migration Runbook

This is the operational reference for running a dmt migration against a
database your company depends on. It assumes you've never seen dmt
before and need to take a migration to green by reading one document.

The audience is the on-call SRE the night the migration runs — not the
developer who wrote the config. The format is plain prose, copy-pasteable
commands, and an honest failure catalog. When dmt fails, the symptom you
see in the logs should map to exactly one entry below, and that entry
should tell you how to recover.

## Preface

This document covers:

- The **pre-flight checklist** an operator runs before a migration starts.
- The **failure-mode catalog** — every failure dmt knows about, what it
  looks like, and how to recover.
- The **observability surfaces** you check while a migration is running.
- The **operational commands** (`run`, `resume`, `preflight`, `status`,
  `history`, `validate`, `analyze`, `profile`) and when to use each.
- The **emergency procedures** for "the migration is on fire."

It does NOT cover:

- The dmt mental model, architecture, or capacity planning. Read
  [`../README.md`](../README.md) first if you haven't.
- The mechanics of resume, ROW_NUMBER pagination, and checkpoint
  layout. Those live in [`RESTARTABILITY.md`](./RESTARTABILITY.md).
- The Prometheus / OTLP / JSON log surfaces in detail. Those live in
  [`OBSERVABILITY.md`](./OBSERVABILITY.md) — the runbook tells you when
  to look, not what every metric means.
- The minimum DB privileges per driver. Those live in
  [`PRIVILEGES.md`](./PRIVILEGES.md) once #232 merges.
- Test data and fixture loading. Those live in
  [`FIXTURES.md`](./FIXTURES.md).
- Local repro of the integration test. That lives in
  [`../CONTRIBUTING.md`](../CONTRIBUTING.md).

When this runbook says "see X" it means go read X. Don't re-derive what
X already documents.

## Before you run

Production migrations fail more often from environment problems than
from dmt bugs. Walk this checklist before launching `dmt run`.

### 1. The target is backed up and the backup is restorable

This is non-negotiable. dmt's default `target_mode` is `drop_recreate`,
which drops every target table before re-creating it. If the migration
goes sideways and there's no restorable backup, you have lost data.

Verify the backup exists and verify it restores by test-restoring it to
a scratch instance. A backup that has never been restored is a hope,
not a backup.

### 2. The config is sane

Open the YAML file the migration will use and confirm at minimum:

- `source` and `target` point at the right hosts/databases.
- `migration.target_mode` is what you intend (`drop_recreate` is
  destructive; `upsert` is incremental).
- `include_tables` / `exclude_tables` cover what you want and exclude
  what you don't (audit logs, temp tables, staging tables).
- TLS is configured correctly for both sides — see the SSL/TLS rows in
  the README's config reference. **MySQL defaults to TLS-required since
  #252.** If you're connecting to a non-TLS test instance, set
  `ssl_mode: disable` explicitly.
- Secrets are externalized via `${env:VAR}` or `${file:/path}`. No
  passwords inline.

If you're using encrypted profiles, `DMT_MASTER_KEY` must be in the
environment of the process running dmt.

### 3. Privileges are sufficient

The dmt source role needs read access to the schema's tables and
metadata. The dmt target role needs (in `drop_recreate` mode) CREATE
and DROP on the target schema; in `upsert` mode it needs SELECT, INSERT,
and UPDATE on the existing tables. The detail per driver lives in
[`PRIVILEGES.md`](./PRIVILEGES.md) — read it once and grant accordingly.

### 4. Disk space is sufficient on the target

A rough rule: the target needs at least 1.5x the source's data size in
free space. Indexes get rebuilt; logical layouts differ between
engines; staging tables (used by the ROW_NUMBER idempotent path on
resume) take temporary disk too.

### 5. Encoding matches what you expect

Cross-engine migrations frequently surface encoding gotchas:

- SQL Server `nvarchar` → PG `varchar` works because PG stores UTF-8
  natively. Going the other way (`text` → `nvarchar`), make sure the
  target column type can hold what the source contains.
- MSSQL collation differences vs. PG `LC_COLLATE` can change `ORDER BY`
  behavior on the target.

`dmt preflight` checks server-level encoding compatibility, but it
can't catch every per-column quirk. If you have mixed-encoding source
data, do a dry run on staging first.

### 6. Run `dmt preflight`

This is what closes the loop:

```bash
dmt preflight --config production.yaml
```

`dmt preflight` runs the full pre-flight battery in roughly half a
second: connection ping (both sides), supported DB version (PG 12+,
MSSQL 2016+, MySQL 5.7+ / MariaDB 10.3+), encoding/collation,
connection-pool headroom (`max_connections - current_connections ≥
workers + 5`), per-driver privilege probes, and the backup-acknowledgment
guard (#228). For MSSQL targets, it also warns when the target schema
already has enabled nonclustered indexes because dmt's current MSSQL
writer contract is parallel BCP without TABLOCK by default. It also
doubles as `dmt health-check` — the alias is preserved so existing
Airflow / k8s liveness probes work unchanged.

If `dmt preflight` reports an error, fix it before launching `dmt run`.
The whole point of preflight is to fail fast in 500ms instead of slow
40 minutes into a partial migration.

If you really must skip a check (you're in a controlled environment
where, say, the privilege probe trips a false positive against a custom
RBAC), use `--skip-preflight=privileges` or `--skip-preflight=all`. Skip
deliberately, not reflexively.

### 7. For `drop_recreate` against a non-empty target, pass `--confirm-backup`

This is the safety interlock for the most-dangerous mode against a
target that has data:

```bash
dmt run --config production.yaml --confirm-backup
```

Without `--confirm-backup`, dmt's preflight refuses to start a
`drop_recreate` migration against a target schema that already
contains tables. The flag is your acknowledgment that you have a
restorable backup. dmt is not your backup tool; you are.

### 8. Plan the change window honestly

`dmt analyze` looks at your source schema and recommends config; it
also gives you a rough sizing baseline (see `docs/BENCHMARKS.md` for
throughput by direction). Pad your change window. A migration estimated
at 4 hours that overruns to 6 is normal; one that overruns to 24 means
something's wrong and you'll want to abort and investigate.

Define your kill criteria up front. "If we're not 50% complete by hour
N, we abort and roll back" is a decision easier to make at the start
than at hour N.

## Failure-mode catalog

Every entry below is a real failure dmt has hit and now handles. The
format is identical across entries so you can scan the catalog for the
symptom you're seeing and go straight to the recovery steps. Symptom
first, mechanism second, recovery third, verification last.

### Preflight failure: connection refused

**Exit code / log signature**: Exit code **2 (`ConnectionError`)**.
`dmt preflight` reports `connection refused` or `no such host` or
`i/o timeout` on the source or target side. The error includes which
side failed (source vs target) and the host:port it tried.

**What it means**: The DB hostname is wrong, the port is wrong, the DB
isn't up, or a firewall is between dmt and the DB. dmt cannot make any
progress until network reachability is restored.

**Recover by**: Verify the hostname and port resolve from the host
where dmt runs:

```bash
getent hosts $SOURCE_HOST     # name resolution
nc -vz $SOURCE_HOST $SOURCE_PORT   # TCP reachability
```

Fix whichever layer is broken (DNS, security group, network policy,
container networking). Then re-run preflight.

**Verify the fix**: `dmt preflight --config X.yaml` exits 0 with no
errors.

### Preflight failure: privileges insufficient

**Exit code / log signature**: Exit code **1 (`ConfigError`)**.
Preflight reports something like `user 'dmt_writer' lacks CREATE on
schema 'public'` with a remedy SQL statement to GRANT what's missing.
(Per `PreFlightError.ExitCode()` in `internal/orchestrator/preflight.go`,
every aborting preflight finding — including `privileges.*` — maps to
`ConfigError`. Only a raw ping failure returns `ConnectionError`.)

**What it means**: The DB role dmt is connecting as can't perform an
operation dmt's first DDL phase will need. dmt detected this in <1s
instead of dying minutes in.

**Recover by**: Run the GRANT statement printed in the remedy. The
per-driver minimum grant set lives in [`PRIVILEGES.md`](./PRIVILEGES.md).

**Verify the fix**: `dmt preflight` exits 0.

### Preflight failure: backup not acknowledged

**Exit code / log signature**: Preflight rejects the run with
`drop_recreate against non-empty target requires --confirm-backup`.
Exit code **1 (`ConfigError`)**.

**What it means**: You're about to drop every table in the target
schema and dmt is forcing you to acknowledge you have a restorable
backup before proceeding. This is intentional friction (#228).

**Recover by**: Verify a recent backup exists, can be restored, and
has been test-restored. Then re-run with `--confirm-backup`:

```bash
dmt run --config production.yaml --confirm-backup
```

If the target schema is empty by intent, dmt detects that and doesn't
require the flag — the guard only fires when there's existing data
to lose.

**Verify the fix**: `dmt run --confirm-backup` proceeds past preflight
into the `extracting_schema` phase.

### Mid-transfer: target out of disk

**Exit code / log signature**: Exit code **3 (`TransferError`)**.
Logs show `insert ... failed: no space left on device` or the DB-specific
equivalent (`SQLSTATE 53100` on PG, MSSQL error 1105). The migration
halts with a partial state in the target.

**What it means**: The target ran out of disk mid-migration. dmt's
retry loop will exhaust quickly because every retry hits the same
hard error.

**Recover by**: Free disk on the target (drop unused tables / archive
old data / expand the volume). Do NOT drop dmt's partial output —
that's recoverable. Then resume:

```bash
dmt resume --config production.yaml
```

Resume picks up at the last checkpoint. For keyset-paginated tables
it deletes anything beyond the last checkpointed PK and continues.
For ROW_NUMBER-paginated tables it uses the idempotent insert path
added in #227, so replayed rows are no-ops.

**Verify the fix**: `df -h` on the target's data volume shows
sufficient free space (≥1.5x remaining source size). `dmt resume`
completes without error.

### Mid-transfer: deadlock or timeout

**Exit code / log signature**: Logs show retry attempts:
`WARN Retry 1/3 for <table> after 1s (error: deadlock)` or `connection
reset` or `i/o timeout`. If retries succeed, the run continues. If
they exhaust, exit code **3 (`TransferError`)**.

**What it means**: A transient DB-side error. dmt automatically retries
with exponential backoff (1s, 2s, 4s, 8s) up to `max_retries` (default
3). Most transient errors clear on retry. If they don't, the
underlying condition (lock contention, an aggressive autovacuum, a
hot table elsewhere) needs investigation.

**Recover by**: If retries clear it, no action needed. If they don't:

1. Check the DB's own logs for the error context — dmt sees the
   symptom; the DB sees the cause.
2. If a long-running transaction on the target is blocking dmt, kill
   it (PG: `pg_terminate_backend`; MSSQL: `KILL spid`).
3. Reduce `workers` and `chunk_size` in the config — smaller batches
   take fewer locks for less time.
4. Resume the run.

**Verify the fix**: `dmt resume` completes. Source and target row
counts match in validation.

### Mid-transfer: hung shutdown after writer failure

**Exit code / log signature**: The migration appears stuck after a
writer error. The process doesn't exit cleanly on SIGTERM. Pre-#250
this was a real bug — readers continued blocking on the chunk channel
after consumers gave up. Post-#250 this should NOT happen.

**What it means**: If you see this on a current dmt build, it's a
regression of #250. Capture diagnostics before killing the process.

**Recover by**:

1. Capture goroutine state: `kill -SIGQUIT $PID` dumps all goroutines
   to stderr. Save the output.
2. If after 5 minutes the process still hasn't exited, escalate to
   `kill -9 $PID`.
3. File a bug with the goroutine dump attached.
4. Resume cleanly: `dmt resume` (checkpoints are still valid).

**Verify the fix**: After resume, `dmt status --json` reports
`"status":"success"`.

### Validation failure: row counts disagree

**Exit code / log signature**: Exit code **4 (`ValidationError`)**.
Logs show `Validation failed: table X source=N1 target=N2 (delta=...)`.

**What it means**: The number of rows in the target doesn't match the
source for one or more tables. This is the cheapest validation pass
and it caught something. Either rows were lost (target < source), or
duplicated (target > source), or rows changed under the migration
(source count moved after the transfer).

**Recover by**:

1. Run `dmt validate --config X.yaml` to get the per-table breakdown.
2. Decide whether the source was quiesced during the migration. If not,
   the count drift might be legitimate new writes — that's an
   operational problem, not a dmt bug. Re-run after a proper quiesce.
3. If the source was quiesced, this is a real correctness issue. Do
   not promote the target. Drop the target, fix the underlying cause
   (frequently a permissions issue masking a partial DDL, or a type
   mapping that silently dropped a column), and re-run.
4. Consider enabling deeper validation modes (`migration.validation.mode:
   null_parity` or `sample`) on the re-run to catch sub-row-count
   divergence that the count check misses.

**Verify the fix**: `dmt validate` reports `OK` for every table.

### Validation failure: NULL parity or sample mismatch

**Exit code / log signature**: Exit code **4 (`ValidationError`)**.
Logs show a per-column NULL count delta (`column X: source nulls=N1,
target nulls=N2`) or a sample row mismatch (`row PK=K differs:
source=..., target=...`). Only fires when `validation.mode` is
`null_parity`, `sample`, or stronger (#226).

**What it means**: Row counts agreed but the rows themselves don't.
Most common causes are a type-mapping problem (decimal → float
truncation, character set mismatch corrupting non-ASCII text) or a
canonicalization gap in the validator itself.

**Recover by**: Inspect a sample of the mismatched rows manually on
both sides. If the difference is real data corruption, do not promote
the target — fix the type mapping (see `internal/driver/typemapper.go`
and `docs/RESTARTABILITY.md` for cross-engine type behavior) and
re-run.

**Verify the fix**: `dmt validate` passes with `validation.mode:
null_parity` (or whichever mode you ran).

### Validation timeout

**Exit code / log signature**: Exit code **4 (`ValidationError`)**.
Logs show `validation timeout: COUNT(*) on table X exceeded
ValidationTimeout (30s)`. Since #253, this is a failure by default,
not a warning.

**What it means**: Validation didn't complete. The `COUNT(*)` query
took too long to return — usually because the table is enormous, the
target is under load, or there's no useful index for the count.

**Recover by**: First, decide whether you can trust the migration
without exact-count proof:

- If you ran `validation.mode: null_parity` or `sample`, those passes
  give independent evidence.
- If you only ran the count check, you're flying blind.

Then:

1. Bump `migration.validation.timeout` to give the count more time.
2. Or set `migration.validation.fail_on_timeout: false` to restore
   the pre-#253 lenient behavior — opt-in only.
3. Or run a manual `COUNT(*)` on each side from a session that allows
   for the longer query.

**Verify the fix**: `dmt validate` passes.

### Source schema drift detected

**Exit code / log signature**: By default, exit code 0 if the migration
otherwise succeeds. Logs start with `Schema drift detected`. If
`migration.fail_on_schema_drift: true` is set, dmt aborts before
transfer with exit code **3 (`TransferError`)**.

**What it means**: The source schema no longer matches the schema
snapshot captured after the last successful run for the same source
schema. This is a read-only guard: dmt reports the difference before
transfer, but it does not alter the target schema for you.

The report groups changes by table and labels the category:
`table_added`, `table_dropped`, `added_column`, `dropped_column`,
`type_widened`, `type_narrowed`, `type_changed_lossy`,
`nullability_change`, `default_change`, `pk_change`, `index_added`,
`index_dropped`, `fk_added`, `fk_dropped`, `check_added`, and
`check_dropped`.

**Recover by**:

1. For additive, safe changes in `drop_recreate` mode, confirm the
   target can be rebuilt and run normally.
2. For `upsert` mode, add the corresponding target columns or run a
   controlled `drop_recreate` refresh before trusting the incremental
   run.
3. For dropped columns, narrowed types, lossy type changes, or primary
   key changes, pause promotion and get an explicit schema migration
   decision from the data owner.
4. Set `migration.fail_on_schema_drift: true` in unattended jobs when
   the operator should review every source-side schema change before
   data moves.

**Verify the fix**: The next run logs `No schema drift detected`, or
the remaining drift is explicitly approved and documented.

### Partial migration: exit code 3

**Exit code / log signature**: Exit code **3 (`TransferError`)**.
JSON result has `"status":"partial"` and a non-empty `"failed_tables"`
array. Since #248, partial migrations exit non-zero by default — the
pre-#248 silent partial-as-success behavior is gone.

**What it means**: One or more tables failed but others succeeded.
Pre-#248, Airflow / k8s jobs would treat this as green. They don't
anymore.

**Recover by**:

1. Read the `failed_tables` array in the JSON result for which tables
   failed and why.
2. Address the per-table failure (often a privilege issue on one
   schema, a type-mapping issue on one column, or a constraint
   violation specific to one table).
3. `dmt resume` to retry the failed tables. Successful tables are
   skipped — see [`RESTARTABILITY.md`](./RESTARTABILITY.md) for the
   resume mechanics.

If you genuinely want partial migrations to exit 0 (e.g., you
explicitly tolerate per-table failures), set `migration.allow_partial:
true` in the config. This is an explicit opt-in, not a default. Most
operators should never need it.

**Verify the fix**: `dmt resume` exits 0; `dmt status --json` reports
`"status":"success"` with `tables_failed: 0`.

### Resume blocked: config hash mismatch

**Exit code / log signature**: Exit code **6 (`StateError`)**. Logs:
`config changed since run started (hash 6abfe692 != 1cddb8e0), use
--force-resume to override`.

**What it means**: The config used for the original run hashes
differently than the config you're trying to resume with. dmt is
protecting you from resuming with mismatched source/target settings —
a config change could move which tables get migrated, which target
schema receives the data, or how chunks are paginated. Resuming
across those changes can silently corrupt the target.

**Recover by**: Decide whether the change is intentional:

- **Unintentional** (you edited the config without realizing it would
  affect resume): revert the change, then `dmt resume`.
- **Intentional** (you tightened `exclude_tables` to skip a table that
  failed, or bumped `chunk_size` for the remaining work): re-run with
  `--force-resume`. Understand that any change to source/target
  identity will resume with the new identity. You own the consequences.

**Verify the fix**: `dmt resume` proceeds.

### Resume blocked: stale incomplete run

**Exit code / log signature**: Exit code **6 (`StateError`)**. Logs:
`incomplete run is obsolete: a successful run completed after it
(<run-id> finished <ts>)`, `incomplete run <id> has a stale heartbeat`,
or `run not found` if state is missing.

**What it means**: The state DB still has a running-status row from a
prior crashed migration, but a subsequent migration completed
successfully, or the incomplete run has not refreshed its heartbeat
within the resume safety window. dmt won't resume an obsolete run
because resuming would move stale data into a target that's already
current. dmt also won't automatically attach to a stale heartbeat
because the original process may still be alive and slow.

**Recover by**: If you really want to resume the older run (you
shouldn't — you have a more-recent success), use the SQLite CLI to
inspect state:

```bash
sqlite3 ~/.dmt/migrate.db "SELECT id, started_at, last_heartbeat, status FROM runs ORDER BY started_at DESC LIMIT 10"
```

For a stale heartbeat, first verify no migration process is still
running on the host or in the scheduler. Only after that verification,
resume with `--force-resume`. For an obsolete run, either start a fresh
`dmt run`, or — only if you really mean it — manually update the old
run's status and resume with `--force-resume`. The cleaner path is to
start fresh.

**Verify the fix**: `dmt run` (not `resume`) starts a new run.

While a run is active, both SQLite and YAML file-state backends persist
`last_heartbeat`. Fresh runs and resumed runs refresh it every ~30s.
By default, `dmt resume` treats a running-status row older than ~15m as
stale unless `--force-resume` is supplied.

### Resume blocked: torn state file (file backend)

**Exit code / log signature**: Exit code **6 (`StateError`)**. Logs:
`failed to parse state file: unexpected EOF` or `yaml: line N: did
not find expected key`. Only applies to YAML-state-file deployments
(`--state-file`) under Airflow / k8s.

**What it means**: A previous dmt process was killed mid-write to the
state file, leaving a truncated YAML file. **Post-#254 this should
NOT happen** — `FileState.save()` now uses tmp + fsync + rename +
dir-fsync, so the file is always the pre-write or post-write version,
never torn.

If you see this on a current dmt build, the state file predates the
fix and was written by an older version.

**Recover by**:

1. Look for `<state-file>.tmp` next to the corrupt state file. If it
   exists and contains complete YAML, atomic-rename it over the bad
   file.
2. Otherwise, the state is lost. Start a fresh `dmt run`. With the
   target in `drop_recreate` mode, no data is lost — just rework. In
   `upsert` mode the previous upserts are still in place and a fresh
   run will reach the same end state.

**Verify the fix**: `dmt status --state-file X.yaml` succeeds. Or
fresh `dmt run` completes.

### Resume from incremental sync regressed to full sync

**Exit code / log signature**: An incremental run that used to take
12 seconds now takes 1m 47s (or proportionally longer for your data).
Logs show no errors — but every table is being re-transferred in
full instead of only rows since the last sync.

**What it means**: Pre-#255, the YAML-file state backend's
`GetLastSyncTimestamp` / `UpdateSyncTimestamp` were no-ops, so
incremental sync configured against the file backend silently
degraded to full sync. Post-#255 this is fixed — timestamps persist
in a `sync_timestamps:` map in the YAML state file.

If you're seeing this symptom, you're either on a pre-#255 build, or
the state file was created by a pre-#255 binary and lacks the
`sync_timestamps:` section.

**Recover by**: Upgrade dmt past v[unreleased] (the version that
includes #255). After the upgrade, the first incremental run
populates the timestamps; the second run is fast as designed.

**Verify the fix**: Inspect the state file; it should have a
`sync_timestamps:` section after a run. Subsequent runs against
unchanged data should complete in seconds, not minutes.

### MySQL TLS: connection negotiates plaintext when you wanted encryption

**Exit code / log signature**: Pre-#252, no signature — that was the
problem. Post-#252, MySQL connections without `ssl_mode` configured
default to TLS-required with CA + hostname verification, and a
non-TLS server returns a clear handshake error at preflight time.

**What it means**: Pre-#252, the MySQL DSN used `tls=preferred`,
which allowed silent plaintext fallback. Operators who thought they
were on TLS could have been transmitting credentials and data in the
clear.

**Recover by**: Upgrade to dmt v[unreleased] or later. If you really
need downgradeable TLS (e.g., to a non-TLS local Docker test
instance), set `ssl_mode: preferred` explicitly — that branch still
maps to `tls=preferred`, but it's now an opt-in. For non-TLS local
test, set `ssl_mode: disable`.

**Verify the fix**: `dmt preflight` against a TLS-required MySQL
target succeeds; the same against a plaintext-only test instance
fails clearly (with `ssl_mode` unset) instead of silently succeeding.

### Migration ran clean but content diverges from source

**Exit code / log signature**: Exit code 0 from `dmt run`. Row counts
match. But application smoke tests against the target return
unexpected values, missing rows, or extra rows.

**What it means**: Row-count parity is not data-correctness parity.
Pre-#226 this was the most dangerous failure mode — a migration that
dropped a column, truncated a string, or coerced float-to-int could
pass the row-count validator. Post-#226, you have two additional
validation passes layered on top of the legacy count check
(`null_parity` and `sample`); use them. (`full` is reserved for an
in-DB row-hashing follow-up and is rejected at runtime today — see
`runDeepValidation` in `internal/orchestrator/validator.go`; `sample`
is the strongest pass currently supported.)

**Recover by**: Re-run validation in a stronger mode. The mode is
config-driven — `dmt validate` reads `migration.validation.mode`
from the YAML and has no `--mode` flag. Edit your config (or point
`--config` at a separate file with the deeper mode set) and re-run:

```yaml
# In production.yaml (or a sibling file used only for re-validation):
migration:
  validation:
    mode: sample   # or null_parity for the cheaper pass
```

```bash
dmt validate --config production.yaml
```

If the pass succeeds and the application still complains, the issue
is application-level (an assumption about a constraint, a sequence
value, an index) not data-level. If it fails, you have data
corruption — treat as the "validation failure: NULL parity or sample
mismatch" entry above.

**Verify the fix**: Stronger validation passes; application smoke
test succeeds.

### Reader goroutine leak (legacy, pre-#250)

**Exit code / log signature**: Pre-#250, hung shutdowns after a
writer failure. Post-#250 this is fixed — readers run under a
per-transfer child context and exit cleanly when the consumer gives
up.

**What it means**: Closed; documented here for completeness so
operators who see hung shutdowns on legacy builds know what's
happening and how to upgrade.

**Recover by**: Upgrade to a post-#250 dmt build. Then resume the
crashed run.

**Verify the fix**: A simulated writer failure (kill the target
mid-run, or revoke INSERT privilege mid-run) does not leak goroutines
— the process exits within `shutdown-timeout` seconds.

### Progress race (legacy, pre-#249)

**Exit code / log signature**: Pre-#249, `go test -race ./...` reported
a data race in `internal/progress`. JSON progress output could surface
inconsistent state (`tables_total=0` with `tables_done=3`).
Post-#249 this is fixed.

**What it means**: Closed; documented here for completeness.

**Recover by**: Upgrade.

**Verify the fix**: `go test -race ./...` is green; JSON progress
output is internally consistent.

## Observability — where to look

dmt exposes three coordinated observability surfaces (#229), all off by
default. Pick the one that fits your stack. The full surface is in
[`OBSERVABILITY.md`](./OBSERVABILITY.md); the one-line summary:

- **Structured JSON logs** (`--log-format=json`) — newline-delimited
  JSON on stderr, every line tagged with `run_id`, `phase`, `table`,
  `source_db`, `target_db`. Send to Datadog / Splunk / Loki / ELK.
- **Prometheus metrics** (`--metrics-addr=:9090`) — eleven dmt-prefixed
  metrics (rows/sec, errors, retries, chunk duration, writer queue
  depth, phase duration, runtime-tuning adjustments, AI fallbacks).
  Scrape from your existing Prometheus.
- **OpenTelemetry traces** (`--otel-endpoint=URL`) — one root span per
  run, child span per orchestrator phase. Send to Jaeger / Honeycomb /
  Tempo.

The three surfaces share the same dimension names (`run_id`, `phase`,
`table`, `source_db`, `target_db`) so you can pivot between log,
metric, and trace views in your tooling without re-mapping.

If you're not running any of those, the TUI's live progress bar and
the text-format stderr log are still there. They're enough for a
supervised run; they're not enough for unattended production.

Start your alerting at:

- `rate(dmt_errors_total[5m]) > 0` — any error is worth a look.
- `dmt_writer_queue_depth > 100` — writers are falling behind.
- The phase histogram going long compared to history — `dmt_phase_duration_seconds`.

Tune by environment.

## Operational commands

The CLI surface lives in `cmd/migrate/main.go`. The relevant subset
for an SRE:

| Command | What it does | When to use it |
|---|---|---|
| `dmt run` | Starts a new migration. Runs preflight → schema extract → DDL → transfer → validation in one shot. | First-time migration, or after a clean rollback. |
| `dmt resume` | Continues an interrupted migration from the last checkpoint. Honors config-hash check; use `--force-resume` to override. | After a crash, kill, OOM, or pod eviction. Almost always preferable to a fresh `run` because completed tables are skipped. |
| `dmt preflight` (alias `health-check`) | Runs only the preflight battery (connectivity, version, encoding, privileges, pool headroom, backup ack). Does NOT touch data. | Before launching a production `run`. As a liveness probe in Airflow / k8s. After a config change to verify the new settings still work. |
| `dmt status` | Prints current/last run status. With `--json` emits a structured result suitable for Airflow sensors. | Polling from automation. Quick "is it done yet" check during a long migration. |
| `dmt history` | Lists past runs from the SQLite state DB. `--run <id>` drills into one run. | Post-mortem. "What was different about Tuesday's run?" |
| `dmt validate` | Re-runs validation against an already-completed migration. Mode is read from `migration.validation.mode` in the YAML config (`count_only`, `null_parity`, or `sample`) — there's no `--mode` flag; edit the config (or point `--config` at a sibling file) to pick a deeper pass than the default that ran with `dmt run`. | After a migration completes, before promoting. Whenever you suspect drift. |
| `dmt analyze` | Inspects source schema and resource posture, recommends chunk size, worker count, date-column candidates, and tables to exclude. `--apply` writes suggestions to the analyzed config file's `migration:` section. Analyze is advisory and does not write `ai_tuning_history`; that history is reserved for completed migration measurements. | Once, when authoring a new config. Not part of the migration run itself. |
| `dmt profile save / list / delete / export` | Stores or retrieves an encrypted config profile in the state DB. Requires `DMT_MASTER_KEY` (base64-encoded 32-byte key). | When you don't want plaintext config files on disk — Airflow deployments, shared workstations. |
| `dmt init` / `dmt init-secrets` / `dmt setup` | One-time config bootstrap. Not run during production migrations. | Initial setup; never on the production critical path. |

Two flags worth knowing across all commands:

- `--state-file <path>` — use a YAML state file instead of SQLite.
  Required for Airflow / k8s deployments where SQLite is impractical.
  Post-#254 the file is crash-safe; post-#255 it supports incremental
  sync timestamps too. See [`RESTARTABILITY.md`](./RESTARTABILITY.md)
  for the SQLite-vs-file tradeoff.
- `--output-json` / `--output-file <path>` — emit the structured run
  result on completion. Required for any orchestrator that does
  result-based branching.

## Emergency procedures

The migration is on fire. Pick the symptom; follow the steps.

### Need to stop the migration immediately

```bash
# Graceful — finishes the in-flight chunks, flushes the checkpoint, exits clean.
# In-flight readers and writers respect the context cancel; SIGTERM is the path.
kill -SIGTERM $DMT_PID

# Wait up to shutdown-timeout (default 60s, --shutdown-timeout to override).
```

If it doesn't exit within `shutdown-timeout` seconds, escalate:

```bash
# Last resort — kills mid-chunk, in-flight chunk is lost.
# Resume picks up at the last checkpoint; up to checkpoint_freq × chunk_size
# rows of rework on the next resume.
kill -9 $DMT_PID
```

After SIGKILL, capture the goroutine dump first if possible
(`kill -SIGQUIT $DMT_PID` *before* SIGKILL) — that's how you'd
distinguish a real bug from a slow target.

### The target is in a known-bad partial state and we need to start over

`drop_recreate` mode is the natural way to start clean. If the
partial state can't even be queried:

1. Confirm you have a restorable backup of the target's pre-migration
   state. If not, restore from one.
2. Drop the target schema's tables (or restore from the backup —
   restoring is cleaner because it gets the pre-migration indexes,
   constraints, and sequences back).
3. Clear the dmt state for the failed run so resume doesn't latch
   onto it:
   ```bash
   sqlite3 ~/.dmt/migrate.db "UPDATE runs SET status='failed' WHERE status='running'"
   ```
   (For file-state deployments: `rm <state-file>`.)
4. Re-run from scratch:
   ```bash
   dmt run --config production.yaml --confirm-backup
   ```

### Bad data already landed in the target and downstream systems consumed it

This is past the migration boundary — it's an incident, not a dmt
operation. Treat it as such:

1. Stop the application traffic to the target if it's possible.
2. Triage: which rows are wrong? Were they ever right? Diff target
   vs source for a sample of suspicious PKs.
3. Decide between (a) fix forward (apply a corrective UPDATE), (b)
   roll back the target (restore from backup, replay app activity),
   or (c) roll back the migration entirely (drop target, re-run dmt
   with the bug fixed).
4. Post-mortem: which validation mode would have caught this? Add
   that mode to the standard pre-promotion check, not just this one
   migration.

### Need to know what the migration is actually doing right now

Pick whichever channel has signal in your deployment:

```bash
# TUI (interactive)
dmt    # launches /status

# Logs (any deployment)
tail -f dmt.log         # text format
tail -f dmt.log | jq    # JSON format

# Status (any deployment)
dmt status --config production.yaml          # text
dmt status --config production.yaml --json   # for automation

# Metrics (if --metrics-addr was set)
curl http://localhost:9090/metrics | grep dmt_

# Traces (if --otel-endpoint was set)
# Open your tracing UI and filter by service=dmt
```

If none of the above answer the question, the migration is probably
not the bottleneck — check the source and target's own monitoring.

## Exit code reference

For automation predicates and post-mortem categorization:

| Code | Name | Recoverable? | Meaning |
|---|---|---|---|
| 0 | Success | n/a | Migration completed and validated. |
| 1 | `ConfigError` | no | YAML / JSON parse, missing required field, invalid value. Fix and re-run. |
| 2 | `ConnectionError` | yes | Source or target connection failed. Investigate, then `dmt resume` (or `dmt preflight` to verify before resuming). |
| 3 | `TransferError` | no | Transfer failed (partial migration or unrecoverable retry exhaustion). `dmt resume` after fixing the underlying cause. |
| 4 | `ValidationError` | no | Post-migration validation failed. Do not promote the target. |
| 5 | `Cancelled` | yes | Operator-initiated SIGINT/SIGTERM. `dmt resume` to continue. |
| 6 | `StateError` | no | Checkpoint corruption, config-hash mismatch, missing state. See the resume-blocked entries above. |
| 7 | `IOError` | yes | File I/O (state file path, log path, secrets file). |

`Recoverable` is dmt's own classification — it's the answer to "should
automation auto-retry without operator review?" Codes 2, 5, and 7 are
auto-retry-safe. Everything else needs eyes on it first.
