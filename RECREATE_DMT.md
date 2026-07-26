# Recreate DMT: implementation-neutral engineering handoff

## Prompt to the implementing agent

Build a new data migration tool that is behaviorally compatible with DMT as
specified below. Treat this document as the complete product and system
contract: you must not assume access to an existing DMT repository or copy an
existing implementation.

The result may be implemented in Go, Rust, C++, or another suitable language.
Choose the internal architecture, concurrency model, data structures, storage
libraries, database libraries, and testing framework that produce the clearest
and safest system. An improved internal design is welcome. Preserve the
observable contracts, interoperability boundaries, compatibility rules, and
safety invariants in this document.

This is a reconstruction, not a loose reimagining. Deliver a self-contained
command-line application with interactive terminal and browser operator
surfaces, deterministic database migration behavior, safe restartability, and
production-grade operational controls.

### Requirement vocabulary

- **MUST** and **MUST NOT** are acceptance requirements.
- **SHOULD** and **SHOULD NOT** are strong defaults; deviations require a
  documented operational or safety rationale and equivalent acceptance
  evidence.
- **MAY** identifies an optional behavior or an implementation choice.
- Sections labeled **Non-normative** are suggestions or reference context, not
  requirements.

When requirements appear to conflict, prefer, in this order: prevention of
silent data loss or corruption; durable restartability and fencing; explicit
operator intent; compatibility of public interfaces; performance.

## 1. Product purpose and design rationale (normative)

DMT is an operator-run, high-throughput relational data migration tool. It
moves table schemas and rows between heterogeneous databases while preserving
primary-key identity, mapped column semantics, selected schema objects, and
enough durable state to recover correctly after interruption.

The product MUST:

1. Run deterministically without an AI provider.
2. Support one-shot rebuild migrations, repeatable incremental upserts,
   checkpointed resume, schema drift policy, validation, and optional delete
   convergence.
3. Fail explicitly when it cannot prove a safe action. It MUST NOT silently
   skip data, silently coerce an unsupported schema feature, invent a resume
   position, or declare success without the required evidence.
4. Be a completely standalone and independent project. DMT MUST own and
   implement its required deterministic migrated-schema DDL planning and
   rendering within its own application as specified in
   [Section 6](#6-deterministic-schema-ddl-generation-normative).
5. Expose one shared command and orchestration model through a CLI, terminal
   UI, and embedded browser UI. Front ends MUST NOT implement divergent
   migration logic.
6. Be useful interactively and in unattended schedulers such as Airflow,
   Kubernetes Jobs, and CI systems.
7. Ship as a self-contained executable for supported platforms. Operators
   MUST NOT need a language runtime, a separate UI build, or an AI service to
   perform a migration.

The rationale for these choices is operational: large migrations routinely
outlive terminals, encounter transient database failures, cross dialects with
different type and DDL rules, and run against targets where an incorrect retry
can duplicate or overwrite data. Throughput matters, but only after the tool
can demonstrate safe ownership, replay, and validation.

## 2. Scope and database capability contract (normative)

### 2.1 Registered engines

The following canonical engine names and aliases MUST be accepted:

| Canonical name | Accepted aliases | Source | Target |
|---|---|---:|---:|
| `postgres` | `postgresql`, `pg` | yes | yes |
| `mssql` | `sqlserver`, `sql-server` | yes | yes |
| `mysql` | `mariadb`, `maria` | yes | yes |
| `sqlite` | `sqlite3`, `sqlitedb` | yes | yes |
| `clickhouse` | `ch` | yes | yes |

Aliases MUST canonicalize before target identity, capability, lease, history,
and same-endpoint comparisons.

All distinct source/target combinations SHOULD work in `drop_recreate` mode.
Same-engine migration MUST work when source and target are different
endpoints. A source and target that resolve to the same canonical engine,
host, port, and database/file identity MUST be rejected before mutation.

### 2.2 Capability matrix

Features MUST be selected by explicit engine capability, not by pretending an
unsupported operation succeeded.

| Engine as target | Required bulk path | Upsert | Sequence/identity reset | Post-load FK/CHECK | Strict source consistency |
|---|---|---:|---:|---:|---|
| PostgreSQL | COPY protocol | yes | yes | yes | table and migration scope |
| SQL Server | TDS bulk copy | yes | yes | yes | table and migration scope |
| MySQL/MariaDB | `LOAD DATA LOCAL INFILE` when safely available, strict batched insert fallback | yes | yes | yes | table scope |
| SQLite | bounded batched insert, one writer | yes | yes for `AUTOINCREMENT` | no separate post-load FK/CHECK operation | table scope, one reader |
| ClickHouse | native/batched columnar insert | no | no | no | unsupported |

The following behavior is required:

- Selecting `target_mode: upsert` for a target without upsert capability,
  including ClickHouse, MUST fail before transfer or target mutation.
- A target without sequences MUST skip sequence reset once with a clear
  structured message; it MUST NOT emit per-table fake successes.
- A target without post-load constraint capability MUST either create
  constraints inline when DMT and the engine support that shape or report one
  explicit capability degradation. It MUST NOT claim that omitted objects were
  created.
- SQLite MUST enforce its single-writer constraint regardless of a larger
  configured worker or connection count.
- ClickHouse schema names represent databases. Its tables require an engine and
  ordering definition compatible with DMT's ClickHouse DDL rules. ClickHouse
  primary/order keys are not to be misrepresented as relational uniqueness
  constraints.

### 2.3 Connection and transport behavior

Network engines require host and database. SQLite uses the database field as a
file identity and does not require host, port, user, password, or schema.

Defaults MUST be secure:

- PostgreSQL and MySQL/MariaDB default to required TLS.
- SQL Server defaults to encryption enabled and certificate verification.
- Explicit test-only trust or TLS-disable options MAY be supported but MUST be
  visible in configuration and preflight output.
- Credentials and connection options MUST be escaped using the database
  driver's structured DSN/URL facilities; string concatenation that permits
  DSN injection is forbidden.
- Kerberos/SPNEGO configuration fields MAY be reserved for compatibility, but
  `auth: kerberos` MUST be rejected until runtime drivers and integration tests
  actually support it.

## 3. Public application contract (normative)

### 3.1 Executable and front ends

The executable name MUST be `dmt`.

With no command, `dmt` MUST launch an interactive terminal UI. With `--webui`
and no command, it MUST launch an embedded browser UI. Ordinary subcommands
MUST use the same application services and orchestrator as both interactive
front ends.

The terminal UI MUST provide discoverable slash commands, configuration file
selection, session defaults, live progress, log capture, resume, and setup.
The browser UI MUST provide an authenticated single-operator console for run,
resume, cancel, progress streaming, status/history, preflight, dry-run,
validation, diagnosis, analysis, setup, profiles, cache management, and session
settings.

A machine-checked registry or equivalent contract test MUST ensure that every
CLI command declares whether it is supported, deliberately omitted, or planned
in each interactive front end. Adding a CLI command without a front-end
disposition MUST fail tests.

### 3.2 Stable CLI command surface

The CLI MUST provide these commands and aliases:

- `run`: start a new migration; support dry-run, explicit schema/worker
  overrides, tuning exploration, selective preflight skips, and destructive
  target backup acknowledgment.
- `resume`: continue the newest eligible run for the canonical target; support
  config override acknowledgment, preflight skips, and explicit abandonment
  with an operator reason.
- `status`: show the current or last run, including outcome and resumability;
  support JSON.
- `history`: list runs or show a selected run.
- `validate`: run deterministic validation; optionally request AI advisory
  triage without changing deterministic results.
- `diagnose`: build deterministic failure facts for the current or selected
  run; optionally add AI advisory triage.
- `preflight`, with `health-check` as an alias: test connectivity and readiness.
- `analyze`: inspect source workload and recommend deterministic configuration;
  optionally apply recommendations and request an AI explanation.
- `profile save|list|delete|export`: manage encrypted configuration profiles
  in the full local state backend.
- `ai config-review`, with `ai runbook` as an alias: produce advisory config
  patches and an operator runbook.
- `ai evals`: run or list fixed developer-facing advisory quality scenarios.
- `init`: create configuration interactively.
- `init-secrets`: create a secure secrets template; AI fields are opt-in.
- `setup`: guided secrets, config, connection-test, and analysis workflow.
- `cache clear`: invalidate type-mapping cache entries, optionally only those
  derived from AI.

Global automation controls MUST include configuration/profile selection, an
optional YAML state file, explicit run ID, JSON stdout/file output, log format
and verbosity, graceful shutdown timeout, machine-readable progress interval,
Prometheus bind address, OTLP endpoint, audit settings, and WebUI settings.

Help text is part of the stable public contract. CLI parsing errors and
application errors MUST be centralized so every front end receives the same
semantic outcome.

### 3.3 Exit codes

The process MUST use this stable exit-code contract:

| Code | Meaning | Retry classification |
|---:|---|---|
| 0 | success, including an explicitly accepted partial outcome | no retry needed |
| 1 | configuration or preflight error | operator action required |
| 2 | connection/network error | safe to retry |
| 3 | transfer or schema operation error, including unaccepted partial migration | not automatically safe without diagnosis |
| 4 | validation error | operator action required |
| 5 | cancellation or deadline | safe to resume/retry |
| 6 | state, lease, checkpoint, or resume compatibility error | repair/inspect state first |
| 7 | local file I/O error | retry after repairing I/O |

Errors with an explicit semantic class MUST take precedence over message
matching. Human-readable wording MAY change; exit-code meaning MUST NOT.

### 3.4 JSON and progress output

When JSON result output is enabled, stdout MUST contain only the final
machine-readable result; logs MUST move to stderr. A file-output option MUST
write the same structure with restrictive permissions.

Result, status, history, and WebUI APIs MUST distinguish:

- run outcome: at least `running`, `success`, `partial`, and `failed`;
- whether the run is resumable;
- the resumability reason;
- phase, timestamps, duration, table totals, rows, throughput, failures, and
  deterministic validation facts.

Machine progress MUST be a stream of JSON events on stderr and MUST not corrupt
final JSON output.

## 4. Configuration and secrets contract (normative)

### 4.1 YAML model

Configuration MUST be YAML with top-level `source`, `target`, `migration`, and
optional `profile`, `ai`, and `slack` sections.

Source and target connection settings MUST include:

- `type`, `host`, `port`, `database`, `user`, `password`, and `schema`;
- `ssl_mode`;
- SQL Server `encrypt`, `trust_server_cert`, and `packet_size`;
- optional per-side `chunk_size`;
- reserved authentication fields may be parsed only if unsupported modes are
  rejected clearly.

The migration section MUST support:

- connection limits, workers, chunk size, partitions, large-table threshold,
  reader/writer parallelism, read-ahead, upsert merge size, and memory ceiling;
- include/exclude glob patterns and a platform-appropriate private data
  directory;
- `target_mode: drop_recreate|upsert`;
- schema object toggles for indexes, foreign keys, and checks;
- `strict_consistency` and `strict_consistency_scope: table|migration`;
- schema drift and schema-contract policy;
- validation policy;
- checkpoints, retries, and history retention;
- ordered date-updated column candidates;
- deterministic pre-run tuning and deterministic runtime tuning;
- delete reconciliation policy;
- audit, notification, partial-outcome, and preflight policy.

An illustrative configuration, not a secret-bearing fixture:

```yaml
source:
  type: mssql
  host: source.example.invalid
  port: 1433
  database: source_db
  user: migration_reader
  password: ${env:SOURCE_DB_PASSWORD}
  schema: dbo
  encrypt: true
  trust_server_cert: false

target:
  type: postgres
  host: target.example.invalid
  port: 5432
  database: target_db
  user: migration_writer
  password: ${file:/run/secrets/target-db-password}
  schema: public
  ssl_mode: require

migration:
  target_mode: drop_recreate
  include_tables: ["*"]
  exclude_tables: ["temp_*"]
  create_indexes: true
  create_foreign_keys: true
  create_check_constraints: true
  strict_consistency: false
  strict_consistency_scope: table
  fail_on_schema_drift: false
  validation:
    mode: count_only
    fail_on_mismatch: true
    fail_on_timeout: true
    fail_on_estimate_mismatch: true
  checkpoint_frequency: 10
  max_retries: 3
  history_retention_days: 30
  tuning: auto
  runtime_tuning: true
  runtime_tuning_interval: 5s
  deletes:
    mode: off
```

### 4.2 Defaults and provenance

For compatibility, an omitted source type defaults to SQL Server and an
omitted target type defaults to PostgreSQL. The default target mode is
`drop_recreate`.

Generated tuning defaults SHOULD use CPU, a host/container-aware memory
snapshot, engine characteristics, source schema statistics, and comparable
successful history. Exact tuning formulas are not a public contract.

The following ownership rule is a public contract:

- A value explicitly supplied in the migration config is pinned.
- A value inherited from a global secrets/defaults file is also pinned.
- Driver/formula defaults are generated and MAY be replaced by automatic
  pre-run tuning.
- Runtime tuning operates on separate live runtime state. It MUST NOT rewrite
  the original user-intent configuration used for audit and resume identity.
- Debug/status output MUST disclose the effective value and provenance of each
  tunable.

Memory capacity, currently available memory, and the effective DMT budget MUST
be resolved once from host and container/cgroup evidence. Failure to establish
a safe finite budget MUST fail configuration rather than silently using host
capacity inside a limited container. A user memory setting is a ceiling, not a
replacement for detection.

### 4.3 Template expansion and sensitive values

String scalar values MUST support:

- `${env:NAME}`;
- `${file:/absolute/or/operator-configured/path}`;
- legacy `${NAME}` environment expansion.

Expansion MUST occur after parsing YAML scalars, not by raw textual
substitution over the YAML document. A secret containing `#`, `:`, quotes, or
newlines MUST remain one scalar and MUST NOT inject or truncate configuration.

An editing/raw-load path MUST preserve literal templates so a setup wizard can
round-trip a configuration without resolving and re-serializing secrets.

Configuration, state, logs, output, audit events, notifications, and AI prompts
MUST redact passwords, API keys, bearer tokens, webhook URLs, and DSN
credentials. Sanitized configuration, never original secrets, participates in
run history and resume hashing.

Private directories MUST be owner-only; private state, secret, exported result,
and profile files MUST be owner-readable/writable only. Platform equivalents
are acceptable where POSIX modes do not exist.

Encrypted profiles MUST use authenticated encryption and a separately supplied
master key. Profile storage is optional for minimal/headless state backends but
MUST be explicit as an unsupported capability rather than a fake empty
implementation.

## 5. Logical system contracts (normative)

The internal architecture is intentionally unspecified. Whatever architecture
you choose MUST expose and test these logical responsibilities:

1. **Configuration and secrets**: parsing, expansion, validation, defaults,
   provenance, sanitization, and compatibility hashing.
2. **Engine adapters**: connection, quoting, metadata discovery, read
   pagination, bulk writes, optional capabilities, preflight, and engine
   context.
3. **Canonical schema and DDL service**: translation of discovered metadata
   into deterministic target type mappings and ordered DDL plans, rendering
   those plans for the selected dialect, and executing them.
4. **Migration orchestrator**: lifecycle, phases, target-mode policy,
   scheduling, retries, validation, notifications, audit, and final outcome.
5. **Transfer engine**: bounded pipeline, pagination, conversion, writer
   acknowledgments, checkpoints, replay, and statistics.
6. **State service**: runs, tasks, progress, fences, leases, snapshots,
   watermarks, histories, and backend capabilities.
7. **Operational surfaces**: CLI, terminal UI, WebUI, structured output,
   logging, metrics, tracing, and graceful cancellation.
8. **Deterministic advisory layer**: tuning rules, preflight facts, error
   diagnosis, and optional AI augmentation.

These are responsibility boundaries, not prescribed modules, classes,
processes, or package names.

### 5.1 Canonical metadata model

The discovered schema model MUST preserve enough structured metadata to
reconstruct and validate:

- schema and table identity;
- ordered columns;
- base catalog type plus length, precision, scale, fractional-second
  precision, nullability, default, identity/auto-increment, and spatial SRID;
- MySQL unsigned/zerofill, `TINYINT(1)`, `BIT(N)`, and escaped `ENUM`/`SET`
  members;
- ordered primary-key columns and full PK column metadata;
- row-count estimate and average row width;
- secondary/unique indexes and ordered columns;
- foreign keys, referenced schema/table/columns, and referential actions;
- check constraints;
- a selected incremental date column;
- target identifier normalization where required.

Metadata order and serialization used for schema snapshots MUST be
deterministic.

Identifier quoting MUST be dialect-correct and escape embedded delimiters.
Identifier normalization used during creation, existence checks, transfer,
validation, and resume MUST refer to the same physical object. PostgreSQL's
established normalization behavior, including default `public` handling, MUST
be consistent across all phases.

## 6. Deterministic schema DDL generation (normative)

### 6.1 DMT ownership and sole renderer rule

DMT MUST be completely standalone and independent. It MUST own and implement
the production planning and rendering of all DDL that defines or changes
migrated target schema objects. This behavior MUST be part of the DMT project,
executable, test suite, and release lifecycle. Building, testing, or running DMT
MUST NOT require or assume access to a separate schema-migration project,
codebase, renderer executable, network service, or external conformance target.

The internal architecture and implementation language are choices, but every
production schema change MUST pass through one deterministic DMT planning and
rendering contract. Given the same canonical metadata, source dialect, target
dialect, options, and DMT version, it MUST produce the same ordered plan.

That contract MUST implement:

| DMT operation | Required deterministic behavior |
|---|---|
| Normalize cross-dialect types for PostgreSQL, SQL Server, MySQL, SQLite, and ClickHouse | Map canonical source type metadata to an explicit target type or return a typed/classifiable unsupported-type error. |
| Create schema/database and table | Produce dialect-correct, fully qualified creation statements with ordered columns and explicit creation options. |
| Create inline or standalone primary key | Preserve ordered key columns and use only forms supported by the target capability contract. |
| Create secondary or unique index | Preserve uniqueness and ordered columns; quote every identifier using the target dialect. |
| Create foreign key | Preserve local/referenced column order and supported update/delete actions. |
| Create check constraint | Preserve the discovered expression only when DMT can render it safely for the target; otherwise fail or report the explicit configured degradation. |
| Add column | Render the mapped type, nullability, default, identity, and other supported modifiers from structured metadata. |
| Relax column nullability | Emit only a target-supported change from required to nullable. |
| Widen column type | Emit only a type transition classified as safe by DMT's schema-contract rules. |
| Drop table | Render a fully qualified drop for the exact normalized target identity. |
| Truncate table | Render the target-supported truncate operation or return an explicit capability error. |

DMT MUST NOT schedule any other migrated-schema operation unless its public
policy and canonical metadata model explicitly support it. It MUST NOT maintain
multiple competing production catalogs or bypass the deterministic planner
with ad hoc or AI-generated DDL. Unknown types, unsupported features, and
policy errors MUST remain typed or otherwise machine-classifiable and fail with
actionable object, source-dialect, target-dialect, and operation context.

### 6.2 Metadata fidelity and rendering

DMT MUST keep base source catalog type names separate from structured
modifiers. It MUST NOT treat a declaration such as `varchar(100)` as though
that entire declaration were the catalog base type.

MySQL metadata fidelity and temporal precision distinctions are required:
explicit fractional precision `0` MUST remain distinguishable from
unspecified precision; escaped enum/set members MUST round-trip; unsigned and
zerofill flags MUST not be inferred from lossy strings after the fact.

Source and target dialect, physical target schema, table/column metadata,
constraints, and creation options MUST be explicit planner inputs. Identifier
qualification and escaping MUST be applied structurally, never by interpolating
untrusted catalog text. Rendered statements MUST be executed as the ordered
plan produced by DMT; semantic string-replacement post-processing is forbidden.

Target type mapping, defaults, identity/auto-increment behavior, generated
names, qualification, and statement ordering MUST be covered by deterministic
fixtures for every supported target dialect. If two dialects cannot preserve a
source feature exactly, DMT MUST apply an explicitly documented safe mapping,
an operator-selected degradation allowed by this contract, or a
typed/classifiable failure. It MUST NOT silently discard or approximate the
feature.

### 6.3 Multi-statement batch execution

DMT schema operations may require an ordered batch. The plan and executor MUST
preserve:

- statement order;
- required versus best-effort statement classification;
- a request for single-physical-connection affinity;
- setup, operation, and cleanup phases;
- cleanup on the same connection when affinity is required;
- cleanup under an independent, bounded context after primary failure;
- the primary failure as the returned error, with cleanup failures reported
  separately.

This is a correctness requirement for MySQL foreign-key checks, SQLite
foreign-key pragmas and sequence cleanup, and future multi-statement dialect
operations.

### 6.4 Operational SQL

DMT also owns operational SQL that does not define the migrated schema:

- temporary staging tables and staging-column rewrites used for bulk/upsert or
  replay;
- identity/sequence reseeding after load;
- SQL Server source database snapshots;
- DMT's private checkpoint/state schema and its migrations;
- tuning probes and session-control statements;
- reconciliation queries and bounded target deletes;
- read pagination and validation queries.

These operations MUST remain narrowly scoped and dialect-safe. Schema-changing
DDL MUST still use the single deterministic planning and rendering contract in
this section.

## 7. Migration lifecycle (normative)

### 7.1 Fresh run

A fresh run MUST perform the following observable lifecycle. Adjacent internal
steps MAY be combined, but ordering constraints and failure boundaries are
mandatory:

1. Resolve and validate config, memory budget, engine capabilities, and state
   backend capabilities.
2. Open source and target connections.
3. Acquire exclusive fenced ownership of the canonical target.
4. Create and bind durable run state before recording mutable progress.
5. Initialize audit, logs, metrics, traces, notifications, and cancellation.
6. Run preflight before destructive target mutation.
7. Discover the source schema and selected side objects; apply include/exclude
   filters.
8. Compare deterministic source snapshots with the last successful snapshot
   and enforce schema policy.
9. Derive effective tuning without overwriting pinned user intent.
10. If migration-scoped strict consistency is requested, establish its source
    epoch before partition planning or target DDL.
11. Create every durable transfer task before dropping, truncating, or creating
    its target objects.
12. Prepare target tables according to target mode and schema contract.
13. Transfer rows through bounded jobs.
14. Finalize target sequences, indexes, foreign keys, and checks as supported.
15. Run due delete reconciliation.
16. Validate.
17. Atomically finalize successful task/run state, schema snapshots, and
    watermarks as required.
18. Emit summary, notification, audit completion, and final exit status; release
    snapshot and lease resources.

Any failure MUST leave a truthful run outcome. A process MUST NOT report
success if it lost its lease, failed a required checkpoint write, failed
required validation, or could not durably mark completion.

### 7.2 Dry-run

`run --dry-run` MUST connect, perform deterministic preflight, discover and
filter schema, report drift/policy, estimate row counts and duration when
evidence exists, select pagination, disclose tuning/provenance, and show delete
reconciliation due state. It MUST NOT mutate target data or migrated target
schema, advance watermarks, mark transfer tasks successful, or perform deletes.

Optional AI schema advice is advisory output layered after deterministic facts.

### 7.3 Target modes

#### `drop_recreate`

This mode MUST:

- fail preflight against any non-empty in-scope target unless the operator
  explicitly acknowledges a backup/destructive action;
- create durable tasks before the first drop;
- drop all selected target tables, then recreate tables and primary keys using
  DMT's deterministic DDL planner;
- transfer into empty tables;
- reset identities/sequences and create enabled secondary objects after data;
- surface partial preparation clearly: if drops succeeded but creation failed,
  the error MUST state that rerunning rebuild mode is the recovery path.

The backup gate MAY be suppressed on resume only when durable evidence proves
that the same unchanged run already reached transfer and owns the current target
contents.

#### `upsert`

This mode MUST:

- require an upsert-capable target;
- require every participating source table and existing target table to have a
  primary key;
- require target tables to exist unless `schema_contract.tables: evolve`
  authorizes deterministic creation of newly discovered tables;
- insert new rows and update changed non-key columns by primary key;
- preserve target-only rows unless due delete reconciliation removes them;
- leave existing sequences, indexes, FKs, and checks in place;
- be idempotent under retry and complete-window replay.

### 7.4 Schema drift and contract

On every run and resume, DMT MUST compare the current filtered source schema
with deterministic snapshots from the latest successful applicable run.
Omitting schema-contract configuration means report-only behavior unless
`fail_on_schema_drift` requests a hard gate.

The preferred `migration.schema_contract` surface has entities `tables`,
`columns`, and `data_type`. A scalar mode applies to all three. When the
section is present, omitted entities default to `evolve`.

Required modes and behavior:

| Mode | Required behavior |
|---|---|
| `evolve` | Apply only deterministic compatible changes. In upsert, create newly added eligible tables, add nullable eligible columns, relax nullability, and widen safe types. In rebuild mode, current source shape is recreated. |
| `freeze` | Abort before transfer on drift in the entity. |
| `discard_row` | Skip an added table or any table affected by the configured column/type drift; omit it from transfer, validation, and successful snapshots for this run. |
| `discard_value` | For columns, omit eligible new columns. For data type, omit eligible affected columns while retaining prior snapshot evidence. Remove dependent planned indexes/FKs/checks from the effective plan. |
| `report` | Report only; perform no target schema mutation. This is DMT-specific. |

`tables: discard_value` MUST be rejected. A discarded column MUST NOT be a
primary key, identity, or selected date-tracking column. Unsafe evolution MUST
be blocked: added identity/PK in upsert, nullability tightening, narrowing or
lossy conversions, coupled default/PK drift, and operations DMT cannot render
safely.
Dropped source tables and columns are reported and retained on the target;
DMT does not infer destructive drops from source absence.

Every drift decision MUST be structured and observable with entity, mode,
change kind, object, previous/current evidence, action, and reason.

The deprecated `migration.schema_evolution` surface MAY remain for
compatibility but MUST NOT be accepted together with `schema_contract`.

## 8. Transfer semantics and safety (normative)

### 8.1 Table eligibility and ordering

Tables without a primary key MUST fail the transfer with a clear correctness
error. They MUST NOT be silently migrated with unstable ordering. Include and
exclude filters MUST use documented glob behavior and deterministic ordering.

Rows MUST be selected and written using source column order after any
schema-contract column projection. Source-derived values MUST be converted
without logging row content.

### 8.2 Pagination selection

The chosen pagination strategy MUST be reported per table and MUST satisfy
these safety outcomes:

1. **Integer keyset** for a safe single integer primary key: page with a strict
   or explicitly inclusive lower bound and an inclusive partition upper bound,
   ordered by the primary key. Signed 64-bit extremes MUST not overflow during
   range splitting.
2. **Tuple keyset** for composite or scalar/text primary keys only when source
   binding, nullability, type conversion, and collation semantics prove that
   the comparison order equals the `ORDER BY` order. Persist typed tuple
   watermarks without converting large integers through floating point.
3. **ROW_NUMBER fallback** for a primary key that is not tuple-safe: number
   rows in deterministic primary-key order, page by row-number interval, and
   preserve that exact ordering on resume.

Text tuple keyset MUST be disabled when implicit parameter conversion can use
a different collation/order than the source column. Nullable tuple components,
unsafe unsigned values, converter-touched keys, and date/time keys SHOULD fall
back to ROW_NUMBER unless equivalence is proven by engine-specific tests.

Large eligible tables MAY be split into jobs or work-stealing ranges. Partition
boundaries MUST cover the source domain exactly once. Changing partition count,
range definition, or ROW_NUMBER boundary on resume MUST invalidate affected
stale progress rather than reuse it.

Strict-consistency partition jobs MUST share a source view that spans those
jobs. If the chosen strict mechanism cannot share a view, use one table job
with safe internal parallel readers or serialize it.

### 8.3 Bounded pipeline

The transfer pipeline MUST overlap reading and writing while keeping memory and
database concurrency bounded. The following are observable safety
requirements, not an implementation prescription:

- one migration-wide byte admission budget is shared by all active tables;
- scanned in-memory row size, not only estimated source storage size, counts
  against admission;
- cancellation or writer failure unblocks readers and releases database
  cursors, memory reservations, and worker resources;
- queue size and worker count are bounded by the effective memory/connection
  budget;
- a heap/memory-pressure backstop can reduce work and force collection without
  allowing multiple pipelines to create a collection storm;
- runtime tuning changes take effect only at defined safe boundaries such as
  chunk transitions;
- target protocol limits, including MySQL packet limits and PostgreSQL COPY
  transport constraints, cap the effective chunk below a user request when
  necessary.

### 8.4 Bulk write strictness

- PostgreSQL COPY and SQL Server bulk copy chunks MUST be transactional so a
  failed chunk does not leave an unknown committed prefix.
- SQL Server bulk copy MUST preserve source NULLs rather than substitute target
  defaults.
- MySQL `LOAD DATA LOCAL INFILE` MAY be used only when enabled and when DMT
  verifies that all expected rows were affected and no warning indicates
  truncation, clamping, default substitution, or silent conversion. Otherwise
  the chunk MUST fail. When local infile is unavailable, DMT MUST fall back
  once to strict bounded inserts with a visible warning.
- A non-transactional writer that commits earlier sub-batches before a later
  error MUST return the committed-prefix length. Retry MUST continue after
  that prefix, not replay it as a plain insert.
- SQLite writes MUST respect its bind-variable ceiling and one-writer rule.
- ClickHouse writes MUST use a bounded native or batched columnar path.

### 8.5 Writer acknowledgment and checkpoint frontier

A chunk counts as transferred only after the target acknowledges durable
success. Progress MUST advance in logical source order, not in completion order
of faster parallel writers.

For every range/reader, track enough acknowledgment ordering to derive the
lowest contiguous fully written frontier. Periodic and final checkpoints MUST
represent only that safe frontier. A later chunk completing first MUST NOT move
the checkpoint past an unacknowledged earlier chunk.

Parallel range state MUST persist each range's bounds, completion state, and
typed watermark or an equivalent representation that restores the exact safe
work set. A legacy single-watermark checkpoint MAY resume through a compatible
single-reader path; it MUST NOT be reinterpreted as a different range topology.

### 8.6 Retry behavior

Transient network resets, timeouts, deadlocks, lock timeouts, connection
pressure, server shutdown, broken pipes, and unexpected EOF SHOULD retry with
bounded exponential backoff and cancellation awareness. Default table retry
budget is three retries after the initial attempt.

Data conversion, deterministic DDL policy, missing PK, schema contract,
validation mismatch, lease loss, and unrepaired state errors MUST NOT be
blindly classified as transient.

A retry that might replay already committed ROW_NUMBER or tuple rows in
rebuild mode MUST use an insert-only duplicate-safe path keyed by the complete
primary key:

- PostgreSQL: staging/COPY followed by `ON CONFLICT DO NOTHING` semantics;
- SQL Server: staging followed by insert-only `MERGE` semantics;
- MySQL: duplicate-key no-op assignment, not `INSERT IGNORE`;
- SQLite: conflict-ignore semantics;
- a target without a safe path MUST require table restart rather than risk an
  overwrite.

This replay path MUST NOT update an existing row: a source value may have
changed since the original committed write.

## 9. Incremental sync and delete convergence (normative)

### 9.1 Date-based incremental upsert

The ordered `date_updated_columns` list is evaluated per table; the first
existing compatible date/timestamp column is selected. A table with no
candidate performs a full-table upsert on every run.

The baseline rebuild/full run MUST:

- transfer all rows;
- capture the maximum non-NULL source timestamp;
- commit it as that table's sync watermark only when the table's successful
  completion is durable.

Later upsert runs use the strict lower bound:

```text
selected_timestamp > last_successful_source_watermark
```

Rows equal to the saved watermark are intentionally not replayed.

At the beginning of a new incremental table attempt, DMT MUST sample and
durably persist an immutable upper-fence timestamp for that run. That fence
caps how far the durable watermark may advance; it need not exclude later rows
from the read query. A resume MUST reuse the original fence and MUST NOT sample
a new one.

On incremental resume, DMT MUST discard positional progress and replay the
entire changed-row window from its saved lower watermark. Upsert idempotency
makes this safe. Continuing from a saved PK can permanently miss a row whose
timestamp changed after its key had already passed, so that behavior is
forbidden.

The new watermark and aggregate table-success checkpoint MUST commit
atomically, or with an equivalent protocol that never exposes one without the
other.

### 9.2 Delete reconciliation

Default delete mode is `off`, preserving target-only rows.

The supported opt-in contract is:

```yaml
migration:
  target_mode: upsert
  deletes:
    mode: reconcile
    target_behavior: hard
    reconcile:
      schedule: interval
      interval: 168h
      batch_size: 10000
      require_primary_key: true
```

Reconcile mode MUST:

- be valid only with upsert;
- use an interval schedule and durable last-success state;
- require a primary/stable key;
- compare source and target key sets;
- hard-delete target-only keys in bounded, parameter-safe batches;
- run after data transfer and before validation when due;
- record per-table candidates, deleted rows, skips, reasons, and completion;
- distinguish not-due from ran-with-zero-deletes;
- not advance last-success state after an incomplete reconciliation;
- show due state and candidate impact without mutation during dry-run.

When reconciliation has just completed for the entire applicable scope,
row-count validation is strict. When delete propagation is off or not due,
upsert validation permits target supersets.

## 10. Strict consistency (normative)

`strict_consistency` asks DMT to copy from a stable source view. Unsupported
sources or scopes MUST fail before target mutation.

Required matrix:

| Source/scope | Stable-view contract | Parallel readers | Source write impact |
|---|---|---:|---|
| PostgreSQL/table | exported MVCC snapshot imported by readers | yes | none |
| PostgreSQL/migration | one exported snapshot across all tables and partitions for the process epoch | yes | none; long snapshot may retain history |
| SQL Server/table | one shared table lock/view across readers | yes | writes to that table wait for transfer |
| SQL Server/migration | one run-scoped database snapshot used for planning and reads | yes | no blocking; copy-on-write disk cost |
| MySQL/MariaDB table | InnoDB repeatable-read sessions opened during a brief `LOCK TABLES` window | yes | table writes pause while sessions start |
| SQLite/table | one serializable read transaction | no | file/WAL locking rules apply |
| MySQL, SQLite migration scope | unsupported | — | fail before mutation |
| ClickHouse strict mode | unsupported | — | fail before mutation |

MySQL strict mode MUST verify InnoDB and required `LOCK TABLES` privileges.
SQL Server migration scope MUST verify a supported server/version (not Azure SQL
Database) and a documented database-snapshot permission path.

For a full-table strict job, exact source row count MUST be captured through
the same stable view and persisted as validation evidence before target
success. Validation compares the target to that snapshot count. A later live
source difference is informational, not a failure, when the target matches the
snapshot. Incremental windows continue to use ordinary live-count semantics.

PostgreSQL resume in a new process opens a new snapshot epoch; its cross-table
point-in-time promise applies only within each process epoch, while per-table
replay remains correct. A SQL Server database snapshot survives a crash and
MUST be reused by resume. If missing, resume MUST fail closed instead of
creating a replacement with a different source instant. Normal completion,
failure, or cancellation MUST release owned snapshots; cleanup failure is
operationally visible.

## 11. Durable state, ownership, and resume (normative)

### 11.1 Backend capability contract

DMT MUST support:

1. A full local state backend with durable run history, encrypted profiles,
   tuning history, adjustment history, schema snapshots, and restartability.
2. A user-selected YAML state-file backend for headless use.

Both backends MUST support all restartability data:

- run and task lifecycle;
- table and partition transfer progress;
- sync watermarks and immutable per-run incremental fences;
- delete reconciliation state/results;
- schema snapshots;
- strict snapshot evidence;
- fallback/advisory event counters;
- config hash;
- target lease/fencing metadata.

The YAML backend MAY expose only the current run and omit profiles and
long-lived tuning/history features, but its writes MUST be atomic: write a
complete temporary file, durably flush as appropriate, then replace. A lock
MUST serialize compare-and-write cycles across processes.

The full backend's private schema is not public, MAY evolve between patch
releases, and MUST auto-migrate forward without exposing credentials.

### 11.2 Run and task model

A run record MUST contain at least:

- ID; start/completion/heartbeat timestamps; status; resumable flag and reason;
- phase; source and target identity; sanitized config and compatibility hash;
- profile/config origin; error; lease target key, owner token, and generation.

A task MUST have stable structured identity:

- task type;
- source schema and table;
- optional partition ID;
- state, attempt/retry count, timestamps, and scrubbed error.

Human-readable task keys MUST be collision-free for quoted identifiers
containing dots, colons, percent signs, underscores, slashes, or backslashes.
Correctness MUST NOT depend on parsing ambiguous delimiter-concatenated keys.

Transfer progress MUST include the task, table/partition, rows done/total,
safe watermark, optional range-state envelope, and timestamp.

### 11.3 Required-write rule

Checkpoint state is part of the data correctness protocol, not telemetry.

- Every task MUST exist durably before its associated target destructive
  mutation.
- An unresolved periodic checkpoint error, final progress-save error,
  task-status error, watermark/aggregate completion error, or run-completion
  error MUST stop the success path with state exit code 6.
- Progress/status writes for unknown task IDs MUST be rejected.
- A periodic asynchronous save MAY degrade only if a later synchronous final
  save proves a safe superseding frontier; the degradation MUST be logged and
  audited.
- Because target rows may already be committed when a state write fails, the
  error MUST direct the operator to repair state and resume, not to start a
  competing fresh run.

### 11.4 Exclusive target lease and fencing

Fresh run, resume, and abandonment MUST acquire a lease keyed by canonical
target engine, host/port or file identity, database, and schema.

Each acquisition has a random owner token and monotonically increasing
generation. Acquisition/takeover MUST be atomic. A second live owner receives a
state error; `--force-resume` MUST NOT override it.

The owner renews with its run heartbeat. Every run, task, progress, and
completion mutation MUST verify the bound target key, token, and generation in
the same transaction or atomic lock/write cycle. After stale takeover, a former
owner MUST receive lease-lost error, have its migration context cancelled, and
be unable to report success.

Stale takeover occurs only after the configured TTL and increments generation.
Legacy/pre-fencing runs with a fresh heartbeat MUST be rejected even with force,
because they cannot honor generation fencing.

### 11.5 Outcome versus resumability

Outcome and recoverability are separate:

- interrupted/cancelled and ordinary partial attempts remain resumable;
- `allow_partial: false` makes partial outcome exit 3 and preserves resume;
- `allow_partial: true` explicitly accepts partial outcome, exits 0, and makes
  it non-resumable;
- `resume --abandon --abandon-reason TEXT` preserves history but removes
  eligibility. A partial stays truthfully partial; an interrupted running
  outcome becomes failed.

### 11.6 Resume protocol

Resume MUST:

1. Select the newest `resumable: true` run for the canonical target.
2. Reject it if a later successful run superseded it.
3. Acquire and bind the target lease; reject a live owner.
4. Verify heartbeat/staleness policy.
5. Compare the current data-plane config hash with the stored hash.
6. Allow `--force-resume` to acknowledge compatible policy/config override,
   but never to bypass a live lease or a known unsafe structural change.
7. Re-run preflight, discovery, filters, drift policy, and tuning because the
   environment may have changed.
8. Reuse the original run ID and reactivate its status.
9. Skip a completed table only if its aggregate checkpoint and target row count
   still agree.
10. For incomplete work, restore exact range/partition state or invalidate it
    safely; clean or truncate only according to target mode and pagination
    guarantees.
11. Replay incremental upsert windows from the lower watermark.
12. Use duplicate-safe insert-only replay when a rebuild page may already be
    committed.
13. Finalize, reconcile, validate, and complete the original run.

Config hashing MUST include data-plane behavior and MUST exclude policy-only or
derived runtime fields whose change cannot alter transferred rows, such as
notification choice, audit destination, fail/warn validation policy,
preflight skips, backup acknowledgment, and runtime-probed chunk caps.
Renaming a field during a supported deprecation cycle MUST preserve compatible
hash wire shape.

## 12. Validation contract (normative)

Validation modes are inclusive:

| Mode | Required passes |
|---|---|
| omitted or `count_only` | row counts |
| `null_parity` | row counts plus per-column NULL counts |
| `sample` | row counts, NULL parity, and bounded deterministic PK-selected row comparison |
| `full` | reserved; reject explicitly until whole-table hashing is implemented and proven |

Count validation MUST:

- compare exact counts with a per-table timeout;
- attempt engine estimates only after exact timeout;
- fail by default when exact validation times out, even if estimates happen to
  match;
- fail by default when fallback estimates differ;
- allow explicit log-only policy for timeout or estimated mismatch;
- require equality for rebuild mode;
- permit target supersets for upsert only when delete reconciliation is not
  strict for this run;
- use persisted strict-snapshot counts as authoritative where applicable.

Deep validation MUST bound table concurrency and per-table time. It MUST return
all findings in stable table order. Sample comparison MUST use the complete
primary key and canonicalize cross-driver values with typed, length-delimited
representations so NULL, boolean, integer, float, string, bytes, and timestamp
values cannot collide. Integers of different widths but equal mathematical
value compare equal; timestamps normalize to UTC without dropping represented
precision; binary bytes remain distinct from text.

Validation MUST produce structured deterministic facts even on failure.
Optional AI triage MAY add hypotheses and remediation but MUST NOT alter pass,
fail, counts, or evidence.

## 13. Preflight and operational safety (normative)

Preflight MUST return stable structured findings with severity, dotted check
name, source/target side, message, and remedy. An error finding aborts run
before mutation unless the operator explicitly skips that exact check, a dotted
prefix, or all checks. Skips downgrade visibly; they MUST NOT erase evidence.

Preflight SHOULD cover, when applicable:

- connection and authentication;
- supported server version;
- source read and target mode-specific privileges;
- schema/database existence and usage/create access;
- encoding/charset compatibility;
- connection-pool headroom;
- strict-consistency prerequisites;
- target disk estimate when source size evidence exists;
- destructive backup acknowledgment and non-empty target detection;
- engine-specific capability probes such as MySQL packet/local-infile state
  and SQL Server snapshot support.

Passing preflight is not a promise that every runtime permission exists;
documentation MUST separately state exhaustive minimum privileges.

Signal handling MUST cancel work, stop producing new chunks, attempt required
final checkpointing within a configurable timeout, and exit with cancellation
semantics. Hard kill remains recoverable through checkpoints.

## 14. Deterministic tuning and optional AI (normative)

Migration, type normalization, preflight, error classification, tuning, and
runtime adjustment MUST work without AI.

Automatic pre-run tuning MAY use:

- CPU and the immutable memory envelope;
- engine bulk characteristics and protocol ceilings;
- table counts, row counts, representative widths, key shapes, and date
  columns;
- comparable completed history for the same workload identity;
- deliberate exploration runs.

Pinned configuration MUST not be overwritten. Failed, partial, resumed,
runtime-adjusted, or incomparable historical outcomes MUST not be treated as
clean evidence for a static tuning policy. Any learned recommendation MUST
record its evidence/provenance and actual execution chunk range.

The runtime controller MUST be deterministic and bounded. It MAY adjust chunk
size, writer count, or buffers for memory pressure, queue growth, write errors,
and resource headroom. Safety reductions may override pinned performance values
when required to prevent protocol or memory failure. Resource growth MUST be
gated by complete inventory and trustworthy row-width evidence.

AI is optional and advisory. Supported provider families SHOULD include hosted
Anthropic, OpenAI-compatible, Gemini, and local Ollama/LM Studio endpoints.
AI MAY:

- advise on unmapped or approximate type metadata;
- review deterministic preflight;
- explain deterministic tuning;
- propose config patches and runbooks;
- advise on schema drift;
- triage deterministic validation/failure facts.

AI MUST NOT:

- receive row values or sample data;
- replace or bypass DMT's deterministic target-schema DDL planner;
- overrule deterministic pass/fail facts;
- turn an unsupported feature into silent success;
- be required for ordinary migration.

All AI output MUST be labeled advisory and cached/invalidated separately from
deterministic evidence. Provider failure MUST degrade the advisory surface
without changing migration correctness.

## 15. Security contract (normative)

DMT is operator-trusted software, not a sandbox for malicious configs or a
host already compromised by an attacker. Within that threat model it MUST:

- never log, persist in run state, notify, or send to AI database passwords,
  master keys, API keys, bearer tokens, webhook credentials, DSN userinfo, or
  row content;
- scrub secrets from wrapped driver/HTTP errors while preserving error
  classification and causal chaining;
- use metadata-only AI payloads;
- parameterize values and quote identifiers with dialect-aware escaping;
- prevent a legal identifier containing quotes or delimiters from becoming SQL
  injection in operational statements;
- keep WebUI secrets out of API responses and browser-visible config;
- write setup-entered passwords to protected secret files by default and
  reference them from YAML;
- sanitize all configs before hashing/persisting;
- expose a deterministic no-match error fingerprint using a safe prefix/hash,
  not a full possibly sensitive database error.

Tests MUST inject sentinel secrets through URL DSNs, key/value DSNs, bearer
headers, API-key shapes, Slack webhook shapes, YAML special characters, and
driver errors and prove absence from every output surface.

## 16. Audit, observability, and notifications (normative)

### 16.1 Structured logs

Text logs are default; NDJSON is enabled by `--log-format=json`. Every
structured event MUST include timestamp, level, message, run ID, phase, source
engine, and target engine when a run is active. Resume events identify resume.
Stable phase names MUST cover preflight, schema extraction, target preparation,
transfer, finalization, and validation.

Errors MUST include a stable error class where known. Retries include attempt
and class. Logs MUST remain scrubbed.

### 16.2 Prometheus and OpenTelemetry

Both are opt-in and no-op when disabled.

Prometheus MUST expose, at minimum, rows, estimated bytes, errors by class,
retries, target chunk-write duration, phase duration, writer queue depth,
active writers, runtime adjustments, fallback events, and one run identity
metric. Labels MUST be sufficient to correlate run, table, phase, and engine
pair without leaving stale per-run gauges after completion.

OTLP/HTTP tracing MUST expose one run/resume root span and child phase spans
with run ID and engine attributes. Per-chunk spans are optional; the default
MUST avoid unbounded telemetry.

### 16.3 Audit log

Every run/resume writes an append-only NDJSON audit stream unless explicitly
disabled. It MUST record start/resume, deterministic schema-contract decisions,
validation completion, checkpoint degradation, delete results where
applicable, and terminal status including panic/cancellation.

During a resumable run the file remains appendable. After terminal success,
accepted/abandoned partial, or hard failure, it becomes read-only where the
platform permits.

Optional tamper-evident mode MUST add a monotonic sequence, previous hash, and
SHA-256 hash over canonical event JSON so modification breaks the downstream
chain. It is tamper evidence, not a digital signature.

An audit-open failure SHOULD warn and allow migration to continue unless a
future explicit compliance mode requests fail-closed behavior. This degradation
MUST be visible.

### 16.4 Notifications

Slack webhook notifications are optional. Start is emitted when either success
or failure completion notifications are enabled. Completion payloads MUST use
the same structured run summary as CLI/WebUI and distinguish success, partial,
failure, rows, duration, and failed tables. Webhook URLs are always scrubbed.

## 17. WebUI contract (normative)

The browser assets MUST be embedded in the executable.

- Default bind is loopback.
- Loopback launch MAY generate a strong token and print a one-click URL; the
  UI MUST remove the token from the address bar after exchanging it.
- Non-loopback bind MUST require an explicit token of adequate length and
  either native TLS or an explicit insecure-behind-TLS-proxy acknowledgment.
- Token comparison MUST be constant-time.
- Login exchanges the token for an HttpOnly, SameSite=Strict session cookie;
  bearer authentication remains available for API clients.
- Sessions MAY slide with activity but require an absolute lifetime cap.
- Failed authentication MUST be rate-limited by client identity.
- Forwarded client addresses MUST be trusted only from explicitly configured
  proxy IPs/CIDRs.
- Loopback mode MUST defend against DNS rebinding with Host validation.
- The browser API MUST never expose database passwords, encrypted profile
  blobs, or unrestricted server-side file paths.
- Only one migration may run through a server instance at a time, in addition
  to the cross-process target lease.
- Live progress SHOULD use Server-Sent Events or an equivalent one-way stream.
- Remote/team use remains a single shared-secret model; see non-goals.

## 18. Compatibility and release expectations (normative)

The reconstructed compatibility target is **DMT 5.6.0**.

Use Semantic Versioning:

- removing/renaming stable YAML fields, CLI commands/flags, changing default
  data semantics, exit-code meanings, or structured log fields is major;
- additive fields/commands/drivers and deprecations are minor;
- correctness fixes and internal refactors are patch when they preserve public
  contracts.

From major version 5 onward, every non-experimental field in the shipped config
example and every documented CLI command is stable. A stable rename requires a
minor release that accepts old and new names, gives the new name precedence,
warns on the old name, and names its major removal version.

Compatibility requirements include:

- legacy `ai_adjust` and `ai_adjust_interval` map to
  `runtime_tuning` and `runtime_tuning_interval`, warn, and remain compatible
  until removal in version 6;
- the deprecated schema-evolution surface remains unambiguous with its
  replacement and cannot be combined with it;
- upgraded state auto-migrates;
- ambiguous legacy incomplete task identities fail resume with recovery
  guidance rather than guessing;
- a binary MUST NOT downgrade and resume a newer typed parallel-range
  checkpoint it cannot understand;
- public JSON/status readers SHOULD tolerate additive fields and new audit
  event types.

Release artifacts MUST include self-contained binaries for macOS x86-64 and
ARM64, Linux x86-64 and ARM64, and Windows x86-64, plus SHA-256 checksums.
`dmt --version` MUST report the release version.

## 19. Non-goals for this reconstruction (normative boundaries)

Do not expand initial scope to:

- source-native CDC readers or replication-slot/binlog/LSN management;
- tombstone source mode or target soft-delete mode;
- Kerberos/SPNEGO authentication;
- migration of tables without a primary key;
- ClickHouse upsert, sequences, relational FK/CHECK enforcement, or strict
  snapshot claims;
- MySQL or SQLite migration-scoped strict consistency;
- DLT-style variant columns for unsafe relational type evolution;
- automatically dropping target tables/columns because they disappeared from
  source;
- `validation.mode: full` until a whole-table algorithm and cross-engine proof
  exist;
- multi-user WebUI accounts, RBAC, SSO, or per-user authorization;
- a public library API or stable internal adapter ABI;
- byte-for-byte compatibility of human text logs or tuning formulas;
- guaranteed benchmark numbers independent of hardware and databases;
- a container platform, hosted control plane, or external daemon requirement.

These may be future features only through additive design, explicit capability
gates, and their own acceptance evidence.

## 20. Staged implementation plan (normative deliverables, flexible internals)

The internal design is free, but implement in stages that produce testable
vertical slices. Do not build the WebUI or optional AI before transfer and
restart correctness.

### Stage 0: executable contract and deterministic DDL proof

Deliver:

- versioned executable, config parser, secret expansion/sanitization, semantic
  exit codes, and command skeleton;
- DMT-owned deterministic type mapping and DDL generation for all five
  dialects;
- canonical metadata fixtures and exact create/evolution batch tests;
- a test that proves production migrated-schema DDL cannot bypass DMT's
  deterministic planner.

Exit criterion: a schema fixture can be planned for every target dialect with
deterministic SQL or a typed/classifiable DMT schema-policy error, and no secret
fixture appears in output.

### Stage 1: SQLite end-to-end vertical slice

Deliver SQLite source/target discovery, deterministic DMT schema creation,
bounded transfer, both target modes, validation, local state, YAML state,
leases, audit, and run/resume/status/history.

Exit criterion: an automated SQLite-to-SQLite migration survives forced process
termination at each write/checkpoint boundary and finishes with exact schema
and rows.

### Stage 2: transfer correctness and restartability

Deliver all pagination strategies, partition/range state, safe acknowledgment
frontiers, duplicate-safe replay, partial-write offsets, required state writes,
target fencing, memory admission, cancellation, and retry classification.

Exit criterion: deterministic fault injection proves no skip, duplicate,
overwrite, split-brain completion, or false success under out-of-order writes,
state failures, lease takeover, and boundary changes.

### Stage 3: network engines and capability matrix

Deliver PostgreSQL, SQL Server, and MySQL/MariaDB source/target adapters and
native bulk/upsert behavior; then ClickHouse source/target rebuild behavior.

Exit criterion: all 12 directed cross-engine pairs among PostgreSQL, SQL Server,
MySQL, and SQLite pass a common fixture; same-engine fixtures pass; ClickHouse
has dedicated conformance and live integration; unsupported capabilities fail
up front.

### Stage 4: production data semantics

Deliver incremental watermarks/fences, delete reconciliation, strict
consistency, schema snapshots/contracts, deep validation, spatial/type
metadata, tuning, and preflight.

Exit criterion: concurrent source mutation, schema drift, hard delete, crash,
and resume integration scenarios meet Sections 7 through 13.

### Stage 5: operator surfaces

Deliver stable CLI help/JSON, terminal UI, embedded WebUI security, metrics,
tracing, notifications, encrypted profiles, setup, diagnosis, and optional AI
advisories.

Exit criterion: command/front-end parity tests pass, remote-bind safety rejects
unsafe configurations, and identical command requests produce identical
orchestration outcomes across surfaces.

### Stage 6: release hardening

Deliver cross-platform artifacts, checksums, state upgrade tests, SemVer
deprecation tests, race/thread sanitizer or equivalent concurrency validation,
dependency vulnerability checks, and operator documentation.

Exit criterion: the complete acceptance matrix below is green in clean
environments and on an upgrade fixture.

## 21. Observable acceptance criteria (normative)

A reconstruction is faithful only when all applicable criteria are automated
or accompanied by a reproducible live-database test.

### 21.1 Build and public surface

- [ ] One self-contained `dmt` executable starts on every release platform.
- [ ] `--version`, help, command aliases, global flags, exit codes, JSON routing,
      TUI launch, and WebUI launch match Sections 3 and 18.
- [ ] CLI/TUI/WebUI parity is machine-checked.
- [ ] No AI configuration is needed for build, dry-run, migration, resume, or
      validation.

### 21.2 Configuration and security

- [ ] YAML defaults, aliases, invalid combinations, and same-endpoint guard are
      covered by table-driven tests.
- [ ] Scalar template expansion survives `#`, colon, newline, quote, and
      backslash secrets without changing YAML structure.
- [ ] Raw/edit loading preserves templates.
- [ ] Sentinel passwords, tokens, keys, webhooks, row values, and DSNs are
      absent from logs, JSON, state, audit, notifications, WebUI responses, and
      AI payloads on success and failure.
- [ ] Private files/directories have restrictive platform permissions.
- [ ] A cgroup/container memory-limit fixture cannot inherit unsafe host memory.

### 21.3 Deterministic DDL and schema

- [ ] DMT builds, tests, and runs its schema planner without a separate
      schema-migration project, renderer executable, service, or conformance
      target.
- [ ] Exact DDL fixtures cover create schema/table, PK, unique/non-unique
      index, FK actions, checks, add column, nullability relax, safe widening,
      drop, and truncate for all five dialects.
- [ ] Batch tests cover ordered statements, single-connection affinity,
      required failure, best-effort failure, cancellation-independent cleanup,
      and primary-error preservation.
- [ ] A boundary test fails if production code bypasses DMT's deterministic
      planner with ad hoc or AI-generated migrated-schema DDL.
- [ ] MySQL full type metadata, precision-zero versus unspecified, enum/set
      escaping, spatial SRID, PostgreSQL identifier normalization, and quoted
      delimiter names have regression tests.
- [ ] Unknown/unsupported schema features fail with a typed/classifiable error.

### 21.4 Driver conformance

For each engine:

- [ ] aliases/defaults, secure DSN, quoting/escaping, qualification,
      placeholders, schema discovery, row counts, date columns, pagination,
      table/PK existence, bulk behavior, capability reporting, and preflight
      pass a shared suite;
- [ ] adapter construction and errors never reveal credentials;
- [ ] an unsupported capability is absent/rejected, never a success-returning
      no-op;
- [ ] live version-floor and engine-specific behavior tests run against a real
      server/file.

### 21.5 Transfer and retry correctness

- [ ] Integer keyset covers negative values, gaps, minimum/maximum signed
      64-bit bounds, fresh inclusive minimum, strict resume, and parallel
      ranges without overlap.
- [ ] Tuple keyset covers composite values, large integers above `2^53`,
      collation-sensitive text on eligible engines, NULL rejection, typed range
      checkpoint restore, and work stealing.
- [ ] Unsafe tuple cases route to ROW_NUMBER.
- [ ] ROW_NUMBER partitions cover all rows exactly once in deterministic PK
      order.
- [ ] Injecting out-of-order writer completion never advances a checkpoint over
      an unacknowledged sequence.
- [ ] Writer failure cancels/unblocks all readers and releases memory and
      connections; repeated tests show no worker/thread leak.
- [ ] PostgreSQL/SQL Server chunk failure rolls back; MySQL committed-prefix
      failure resumes after the prefix.
- [ ] MySQL `LOAD DATA` row-count loss or conversion warning fails rather than
      acknowledges the chunk.
- [ ] Replay after write-before-checkpoint does not duplicate or overwrite
      existing rows.
- [ ] Memory use remains within the effective budget under multiple concurrent
      wide-table jobs.

### 21.6 State, lease, and resume

- [ ] Full and YAML backends pass one restartability conformance suite.
- [ ] YAML replacement is atomic under crash and concurrent writers.
- [ ] Unknown task writes and every required-write failure prevent success.
- [ ] Task creation failure occurs before target mutation.
- [ ] Periodic failure plus successful final checkpoint is audited; failed final
      checkpoint is fatal.
- [ ] Two processes racing for one target produce one owner.
- [ ] After TTL takeover, the old generation cannot mutate any run/task/progress
      state or report success.
- [ ] Different canonical targets can run concurrently.
- [ ] Config drift rejects resume; force does not bypass live ownership or
      structurally unsafe incompatibility.
- [ ] Completed tables skip only with target-count agreement.
- [ ] Partition topology changes clear stale progress.
- [ ] Partial, accepted partial, cancellation, abandonment, and superseding
      success produce the required outcome/resumability matrix.
- [ ] State upgrades preserve completed history and fail safely on ambiguous
      incomplete legacy task/range identities.

### 21.7 Incremental and delete behavior

- [ ] Baseline full load records a source-derived watermark only with durable
      table success.
- [ ] Next upsert transfers rows strictly newer than the watermark.
- [ ] A row exactly equal to the watermark is skipped.
- [ ] An unchanged next run transfers zero rows for tables with a date column.
- [ ] A table without a candidate performs a full upsert.
- [ ] A crash after a row behind the saved PK is updated resumes by replaying
      the full changed window and does not lose that update.
- [ ] Resume reuses the original immutable upper fence and cannot advance the
      watermark past it.
- [ ] A due reconciliation removes a source-side hard delete, persists
      per-table counts, and makes validation strict.
- [ ] Off/not-due reconciliation preserves target-only rows and permits the
      documented upsert target superset.
- [ ] Dry-run never deletes and reports due/candidate state.

### 21.8 Strict consistency

- [ ] Concurrent source writes demonstrate PostgreSQL stable snapshots without
      writer blocking at table and migration scope.
- [ ] MySQL opens parallel InnoDB repeatable-read sessions under a short lock
      window and rejects unsupported engines/privileges.
- [ ] SQL Server table scope blocks writers for the table and migration scope
      uses one database snapshot without blocking writers.
- [ ] SQLite uses one stable reader.
- [ ] Unsupported engine/scope fails before target mutation.
- [ ] Target validation uses the persisted snapshot count while later live
      source drift is informational.
- [ ] SQL Server resume reuses a surviving snapshot and fails if it is missing.
- [ ] PostgreSQL resumed process clearly exposes a new epoch.

### 21.9 Schema contract and validation

- [ ] Table/column/type add, drop, safe evolution, unsafe evolution, freeze,
      report, discard-row, and discard-value cases match Section 7.4.
- [ ] Discarded columns prune dependent objects and cannot discard keys,
      identities, or watermark columns.
- [ ] Contract decisions appear in JSON/audit with evidence and reason.
- [ ] Count timeout and estimate mismatch fail by default and honor explicit
      log-only policy.
- [ ] Upsert/reconciliation count policy is correct.
- [ ] NULL parity detects a systematic NULL conversion.
- [ ] Sample canonicalization equates cross-driver representations of equal
      text/integer/time values and distinguishes NULL/string/bytes collisions.
- [ ] `full` mode is rejected, not silently weakened.

### 21.10 Operational surfaces

- [ ] Preflight findings are stable, ordered, actionable, and selectively
      skippable without disappearing.
- [ ] SIGINT/SIGTERM cancels, flushes within timeout when possible, and leaves
      a resumable truthful run.
- [ ] Structured logs, metrics, traces, progress JSON, audit, and notifications
      correlate by run ID and contain no secrets or row content.
- [ ] Tampering with any chained audit event breaks verification.
- [ ] WebUI loopback, token exchange, cookies, bearer auth, TLS/non-loopback
      gate, brute-force limit, trusted proxy handling, DNS rebinding defense,
      single active run, SSE progress, and secret exclusion have integration
      tests.

### 21.11 Live database matrix and CI

CI MUST include:

- fast unit tests and formatting/static analysis;
- concurrency race/thread sanitizer or the strongest equivalent available;
- lint and dependency vulnerability scanning;
- PostgreSQL, SQL Server, MySQL, SQLite, and ClickHouse adapter conformance;
- required live integration on each pull request for at least SQL Server to
  PostgreSQL and SQLite to SQLite, including schema contract and incremental
  workflows;
- a scheduled 12-pair directed cross-engine matrix among SQL Server,
  PostgreSQL, MySQL, and SQLite;
- dedicated ClickHouse live integration;
- schema evolution, daily incremental, delete reconciliation, resume fault,
  strict snapshot, and state-backend scenarios;
- cross-platform release build and checksum verification.

Use a small deterministic fixture with primary keys, composite keys,
identities, NULLs, defaults, text/binary/unicode, decimal/time values, indexes,
FKs, and checks. Larger benchmarks are evidence for tuning, not substitutes for
correctness.

## 22. Delivery definition

Do not declare the reconstruction complete merely because a happy-path copy
works.

Completion requires:

1. public documentation for configuration, CLI, database privileges,
   restartability, strict consistency, schema contracts, security, audit,
   observability, WebUI deployment, fixtures, and incident recovery;
2. a traceable requirements-to-tests matrix covering Section 21;
3. reproducible release artifacts and checksums;
4. a clean no-AI migration across the required live matrix;
5. documented limitations matching Section 19;
6. no production migrated-schema DDL path outside DMT's deterministic planner;
7. demonstrated recovery after process kill, required state failure, and lease
   takeover without silent loss or false success.

If an internal approach differs from the current reference design, document
why it is simpler or safer and point to the black-box, fault-injection, and
interoperability evidence proving that all contracts remain intact.

## 23. Implementation suggestions (non-normative)

These are possible ways to organize the work, not requirements:

- A capability-oriented engine adapter avoids a growing engine-name switch and
  makes unsupported behavior explicit.
- A catalog can hold declarative quoting, discovery, pagination, and context
  facts while imperative native bulk/snapshot operations remain code.
- A single orchestrator service reused by CLI/TUI/WebUI prevents behavioral
  drift.
- State mutation commands can carry a lease token/generation so fencing is hard
  to omit.
- Model transfer progress as an acknowledgment frontier rather than “last row
  read”; it makes parallel correctness easier to reason about.
- Property-based tests are useful for range coverage, identifier escaping,
  canonical value encoding, and task-key collisions.
- A deterministic fault-injection layer around database writes and state writes
  pays for itself early.
- Keep human rendering outside deterministic fact collection so text, JSON,
  TUI, WebUI, AI, and audit reuse one result model.

Equivalent or better designs are encouraged.

## 24. Reference snapshot (non-normative)

This specification was grounded in the DMT 5.6.0 reference snapshot dated
2026-07-26. That snapshot is implemented in Go 1.25.7. The language and module
choices are provenance only. They do not constrain the reconstruction language
or internal architecture.

The reference snapshot organizes responsibilities into configuration/secrets,
catalog-driven drivers, schema planning/rendering, orchestration, transfer,
checkpointing, tuning, validation, audit/observability, command services,
terminal UI, and embedded WebUI. This logical decomposition informed the
contracts above; its exact package layout is intentionally not part of the
requirements.
