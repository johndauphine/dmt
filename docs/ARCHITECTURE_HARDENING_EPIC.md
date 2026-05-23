# Epic: Architecture hardening after the 2026-05-19 review

**Status:** Proposed. Ready to split into GitHub issues.

**Source material:** `docs/ARCHITECTURE_REVIEW_2026-05-19.md` plus Codex source spot-checks on 2026-05-20.

## Goal

Make dmt safer to operate and easier to evolve by fixing the correctness, security,
state, and AI-boundary issues surfaced by the architecture review before doing large
shape-changing refactors.

The review's broad diagnosis is useful: several packages have become hard to reason
about, and docs have drifted from code. The first move should still be contract
hardening, not file consolidation. A repo-wide reshuffle before the runtime contracts are
covered by tests would mostly move risk around.

## Success criteria

- Checkpoint acks are never silently dropped during successful writes.
- Integer PK partitioning cannot overflow or silently skip a range.
- Secret files loaded through `${file:...}` follow the same permission posture as the
  main secrets file.
- Smartconfig history is based on successful transfer outcomes, or explicitly records
  run status and filters failed/partial runs.
- MSSQL parallel BCP behavior is intentional, documented, and guarded by preflight or
  configuration when the target shape is likely to deadlock.
- AI-generated table DDL treats source identifiers as untrusted data and validates the
  returned table/column shape before execution.
- SQLite and YAML state backends have an explicit capability contract, with conformance
  coverage for shared behavior and documented limitations for optional behavior.
- Resume either refuses stale orphaned runs or marks them failed with a clear operator
  path.
- Runtime mutable parameters have one owner: `RuntimeTuner`. Loaded config is treated as
  a pre-run input snapshot.

## Non-goals

- Do not start with a "combine all orchestrator files" PR. That can follow once the
  behavioral contracts below are covered.
- Do not drop the YAML state backend as a drive-by change. First define which backend
  capabilities are required, optional, or unsupported.
- Do not treat the ROW_NUMBER partition cleanup item from the Claude review as confirmed
  current behavior. The current tree already has partition-aware cleanup helpers and
  tests. Add an end-to-end regression test if needed, but do not open a duplicate
  correctness issue without re-verifying against current `main`.

## Workstream A: production correctness and security

### A1. WriterPool ack backpressure

**Problem:** `internal/pool/writer_pool.go` sends write acks with a non-blocking select
and drops the ack if `ackChan` is full. The comment says the checkpoint may not advance,
which means the write succeeded but the durable resume point can lag forever.

**Scope:**
- Replace the drop-on-full path with a blocking send that still exits on context
  cancellation.
- Add a focused unit test with a deliberately saturated ack channel.
- Check shutdown behavior so a blocked ack send does not hang cancellation.

**Acceptance:**
- No code path logs "Ack channel full, skipping ack".
- A slow ack processor applies backpressure to writers instead of losing progress.
- `go test ./internal/pool` passes.

### A2. Safe integer PK range splitting

**Problem:** `internal/transfer/types.go` computes partition boundaries with
`minVal + int64(i)*rangeSize`. Large `bigint` values can overflow and produce wrapped
middle ranges.

**Scope:**
- Replace boundary arithmetic with overflow-safe math.
- Add tests around `math.MaxInt64`, negative-to-positive spans, tiny ranges, and normal
  ranges.

**Acceptance:**
- Generated ranges are monotonic and cover the original interval without wraparound.
- Tests cover at least one prior-overflow shape.
- `go test ./internal/transfer` passes.

### A3. `${file:...}` secret permission checks

**Problem:** `internal/config/templates.go` reads arbitrary file templates without a
permission check, while `~/.secrets/dmt-config.yaml` already has one.

**Scope:**
- Reuse or factor the existing secrets-file permission policy for template file reads.
- Decide and document symlink behavior. A conservative first cut can resolve symlinks and
  validate the final target.
- Add tests for acceptable `0600`/`0400` files and rejected group/world-readable files.

**Acceptance:**
- Loading `${file:/path}` fails with a clear error when the file is too permissive.
- Existing env-template behavior is unchanged.
- `go test ./internal/config ./internal/secrets` passes.

### A4. Smartconfig history only learns from valid outcomes

**Problem:** `internal/orchestrator/run.go` updates AI/smartconfig tuning history after
partial or failed transfer outcomes. A partial run can have misleading throughput and
poison later recommendations.

**Scope:**
- Either gate `UpdateAITuningResult` to successful runs only, or add `run_status` to
  tuning records and filter history/aggregates to successful records.
- Preserve historical rows through schema migration.
- Add tests that failed/partial runs do not influence `GetAITuningHistory` or aggregate
  recommendations.

**Acceptance:**
- Failed and partial runs are not used as positive tuning examples.
- Existing completed successful history still contributes.
- `go test ./internal/checkpoint ./internal/orchestrator` passes.

## Workstream B: operational contracts

### B1. Reconcile MSSQL parallel BCP behavior

**Problem:** `CLAUDE.md` says MSSQL disables writer scaling because TABLOCK serializes
bulk inserts. Current code enables `ScaleWritersWithCores` and explicitly uses parallel
BCP without TABLOCK. Benchmarks imply this may be intentional, but the contract is
undocumented and can deadlock on indexed targets.

**Scope:**
- Decide the intended default for MSSQL target writes.
- If parallel no-TABLOCK remains default, add preflight warnings for risky shapes:
  existing indexed targets, upsert mode, or `write_ahead_writers > 1` where nonclustered
  indexes are present.
- If serialization is safer, change the default and capture a benchmark note explaining
  the throughput tradeoff.
- Update `CLAUDE.md`, `docs/RUNBOOK.md`, and any benchmark footnotes that describe MSSQL
  BCP behavior.

**Acceptance:**
- Docs and code agree on MSSQL writer scaling.
- Operators get an actionable warning or safe default before risky parallel indexed
  writes.
- Any default change includes benchmark or rationale notes.

### B2. Stale/orphan run lifecycle

**Problem:** `checkpoint.GetLastIncompleteRun` selects the newest `status='running'`
record without heartbeat or TTL checks. A killed process can leave a "running" run
forever.

**Scope:**
- Add `last_heartbeat` to SQLite runs and a YAML equivalent where feasible.
- Update heartbeat periodically during run and resume.
- On startup/resume, mark stale runs failed or refuse to resume without an explicit
  force flag.
- Document the operator flow in `docs/RUNBOOK.md`.

**Acceptance:**
- A run whose heartbeat is older than the configured TTL is not silently resumed as a
  normal active run.
- Resume errors explain the original run ID, age, and force option.
- Tests cover fresh, stale, and superseded incomplete runs.

### B3. Config provenance and runtime ownership

**Problem:** Global defaults are applied before `Original*` fields are captured, so the
runtime cannot always distinguish per-config user intent from inherited global defaults.
Separately, loaded config is mutated by pre-run tuning while runtime changes flow through
`RuntimeTuner`.

**Scope:**
- Capture a raw user config snapshot before secrets/defaults/tuning layers.
- Track provenance for tunable fields: user config, secrets default, driver default,
  smartconfig, runtime controller.
- Treat `*config.Config` as immutable once transfer starts. Runtime changes should go
  through `RuntimeTuner` only.
- Update debug output to show provenance without exposing secrets.

**Acceptance:**
- A per-config explicit value is distinguishable from a secrets default in debug output.
- Smartconfig can decide whether inherited defaults are overrideable or pinned, and this
  behavior is documented.
- Runtime controller does not mutate `*config.Config` during transfer.

## Workstream C: AI boundary hardening

### C1. Harden AI table DDL generation

**Problem:** Table names, schema names, and column names are interpolated into a natural
language prompt. A malicious or unusual identifier can be interpreted as instruction
text. The response validation only checks for a `CREATE TABLE` prefix and extracts column
types best-effort.

**Scope:**
- Build the table-DDL prompt from a structured JSON payload embedded as data.
- Add explicit prompt framing that identifiers and DDL snippets are untrusted data.
- Validate returned SQL shape before execution:
  - expected table name/schema,
  - exact expected column set,
  - no extra or missing columns,
  - no FK/index/check constraint creation in this phase,
  - PK columns still present and non-nullable.
- Add tests with instruction-looking identifiers.

**Acceptance:**
- Malicious-looking identifiers are quoted/serialized as data and do not alter prompt
  instructions.
- Invalid AI DDL responses fail closed with clear diagnostics.
- Existing happy-path AI DDL tests still pass.

### C2. Normalize AI provider request defaults

**Problem:** OpenAI-compatible and Gemini paths pin temperature to `0`; Anthropic does
not expose a temperature field in the request struct. Retry behavior also ignores
`Retry-After`.

**Scope:**
- Add deterministic request parameters for Anthropic where supported.
- Centralize provider request defaults for temperature, max tokens, timeout, and retry
  handling.
- Honor `Retry-After` for 429/5xx responses when present.
- Keep local provider compatibility intact.

**Acceptance:**
- Provider request tests assert deterministic defaults for OpenAI-compatible, Gemini, and
  Anthropic.
- Retry tests cover `Retry-After`.
- No provider-specific caller has to reimplement common retry/backoff policy.

### C3. AI fallback/cache lifecycle

**Problem:** AI fallback behavior is spread across type mapping, DDL, diagnosis/history,
and runtime tuning leftovers. Cache entries do not have a clear integrity or lifecycle
story.

**Scope:**
- Inventory current AI entry points and remove stale ones from docs/code where the
  deterministic replacement already shipped.
- Split deterministic results from AI-fallback cache entries, or annotate cache entries
  with source, provider, model, and schema hash.
- Add a cheap integrity check for cache files.
- Add observability counters for AI fallback surfaces.

**Acceptance:**
- Operators can tell when AI was used and why.
- Stale AI cache entries are less likely to be reused for a different schema/provider
  context.
- Removed AI paths no longer appear in docs as active architecture.

## Workstream D: state backend contract

### D1. StateBackend conformance suite

**Problem:** SQLite and YAML implement the same broad interface but not the same feature
set. Some optional methods are intentionally no-op, while others must be equivalent for
resume correctness.

**Scope:**
- Define required vs optional backend capabilities.
- Add a conformance test suite that runs required behavior against both SQLite and YAML:
  runs, tasks, transfer progress, partition progress, sync timestamps, and delete
  reconciliation state if intended.
- For optional capabilities such as encrypted profiles and AI history, expose explicit
  capability checks or documented unsupported behavior.

**Acceptance:**
- A new backend cannot satisfy `StateBackend` while silently skipping required resume
  behavior.
- YAML limitations are documented in `docs/RESTARTABILITY.md` or `docs/RUNBOOK.md`.
- Tests fail if SQLite and YAML drift on required semantics.

### D2. Backend capability cleanup

**Problem:** One large interface encourages ghost implementations. File backend stubs for
AI history and profile-like behavior make it look more capable than it is.

**Scope:**
- Split optional behavior into narrow interfaces such as `ProfileStore`,
  `AITuningStore`, `SchemaSnapshotStore`, and `DeleteReconciliationStore`, or add an
  explicit `Capabilities()` method.
- Update orchestrator/status code to branch on capabilities rather than assuming all
  methods are meaningful.

**Acceptance:**
- Optional backend behavior is explicit at compile time or through a single capability
  surface.
- User-facing errors name the unsupported backend feature instead of silently no-oping.

## Workstream E: maintainability after contracts are covered

### E1. Orchestrator flow map and consolidation

**Problem:** `internal/orchestrator` is split across many small files. The transfer flow
is understandable once traced, but expensive to discover.

**Scope:**
- Add or update a short architecture note that names the run/resume phases and the main
  files involved.
- Consolidate only tightly coupled shards where the call graph proves they are one unit
  of behavior, starting with `transfer_runner*.go`.
- Keep behavior-only PRs separate from move-only PRs where practical.

**Acceptance:**
- A new contributor can find the transfer lifecycle from one doc and one entry file.
- Consolidation PRs are mostly mechanical and covered by existing tests.

### E2. `dbconfig` dependency boundary decision

**Problem:** `internal/dbconfig` exists to break an import cycle between `config` and
`driver`. That is workable, but it documents a dependency compromise rather than a clear
domain boundary.

**Scope:**
- Choose between:
  - keeping `dbconfig` but renaming/framing it as the stable connection-spec package,
  - moving connection types under `driver`,
  - or defining a narrow `driver.ConnSpec` interface.
- Update comments so the package reads as a deliberate boundary instead of a circular
  import workaround.

**Acceptance:**
- The chosen boundary is documented.
- Driver and config packages no longer carry comments saying the package only exists
  because the dependency graph lost an argument.

### E3. Driver conformance expansion

**Problem:** Several secondary findings are driver-contract issues: identifier length
limits, keyset/ROW_NUMBER query builder argument ordering, SQLite single-writer behavior,
and connection pool defaults.

**Scope:**
- Expand per-driver conformance tests for:
  - identifier length validation by target dialect,
  - keyset and ROW_NUMBER SQL/arg ordering with date filters,
  - target writer concurrency constraints,
  - connection pool defaults and documented semantics.
- Treat any currently unverified secondary review finding as a test-first investigation.

**Acceptance:**
- Query builder arg ordering is covered for every dialect.
- Identifier limit failures are caught before DDL execution where feasible.
- SQLite writer concurrency behavior is explicit in validation or docs.

## Suggested PR order

1. A1: ack backpressure.
2. A3: secret file permission checks.
3. A2: PK range overflow.
4. A4: tuning history success filtering.
5. B1: MSSQL BCP contract and docs.
6. C2: Anthropic deterministic defaults and shared retry handling.
7. C1: structured AI DDL prompt and validation.
8. B2: stale run heartbeat.
9. D1/D2: state backend conformance and capabilities.
10. B3: config provenance/runtime ownership.
11. E1-E3: maintainability cleanup after the above tests exist.

## Verify-first queue from the architecture review

These are plausible but should not become issues until reproduced on current `main`:

- `ScaleWorkers` downscale abandoning jobs. Current worker code checks its cancel signal
  after finishing the pulled job, so the original claim may be stale.
- `chunkSizeFn` changing mid-reader breaking resume invariants. This needs a concrete
  failing resume scenario before changing runtime tuning behavior.
- ROW_NUMBER resume cleanup. Current code has `clearResumeProgress`,
  `ClearPartitionTransferProgress`, and tests for partition progress clearing.
- Config-layer "secrets override user values." Global defaults appear to apply only when
  per-config numeric values are zero; the confirmed issue is provenance, not direct
  overwrite of explicit non-zero values.
- AI keys in sanitized output. `Config.Sanitized()` currently redacts source/target
  passwords only, but AI/Slack may be loaded into `Config`. Verify every debug/log output
  path before filing a leak issue.

