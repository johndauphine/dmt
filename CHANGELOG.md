# Changelog

All notable changes to this project will be documented in this file.

> CHANGELOG entries between v3.0.0 and v3.60.0 (Jan 2026 → May 2026)
> weren't maintained; the per-PR commit messages are the canonical
> record for that window. Tracking #233 to formalize CHANGELOG
> discipline going forward.

## [Unreleased]

## [5.5.0] - 2026-07-25

### Added

- Integrated SMT v1.1.0's deterministic DDL renderer for DMT CREATE TABLE generation, retaining DMT compatibility fallbacks and five-dialect parity coverage (#737).

### Removed

- Removed the cardinality-aware memory profile and table-aware chunk caps
  introduced by #729, restoring the completed-epic scalar widest-width safety
  model.

- Removed the effective-candidate projection, pin-aware selection and
  finalization, early runtime-cap materialization, and persisted/executed tuple
  identity introduced by #730, restoring the completed-epic tuning path.

### Fixed

- Restored the pre-epic six-probe cold-start policy and legacy load-time
  defaults after retained-epic regression verification. The retained memory,
  pool, history, and runtime infrastructure now executes the requested global
  policy directly in steady state under shared measured-byte admission and
  MemoryGuard. Per-table complete-inventory checks gate writer-count
  transitions and ratchet future chunk or batch growth only after a transition
  is applied; transition metadata is recorded atomically apart from the learned
  action. The execution-context fingerprint was advanced so history from the
  retired static-projection semantics fails closed; new projected evidence is
  reused only under matching workload and execution context. Resume segments
  stay out of learning, and unusable regression predictions fall back to
  measured bins or baseline.

- Rolled back the cardinality-aware auto-tune memory model and effective-
  candidate projection introduced by #729 and #730 after a native Windows
  regression screen classified the combined change as likely materially
  slower. Earlier auto-tune safety and exploration improvements remain intact
  (#728).

- Removed retired auto-tune suggestion, checkpoint aggregate, and monitor-trend
  APIs after verifying they have no production consumers. Live pool
  recommendations remain available to analyze, apply, Web, and advisory
  surfaces, while effective runtime pools and persisted actuals are unchanged
  (#704).

- AI tuning and runtime-adjustment history now dual-writes UTC epoch
  milliseconds while retaining legacy text for compatibility. Zone-less legacy
  timestamps remain explicitly unresolved, use deterministic ID fallback only
  for count-based reads, and are excluded from chronological calculations (#706).

- Runtime-controller adjustments now capture an explicitly observational
  three-sample before/after delta when equal, uncontaminated windows are
  available. Intervening adjustments censor older observations, and the
  telemetry remains isolated from controller decisions (#710).

- Runtime-adjustment persistence now distinguishes unmeasured outcomes from
  genuine measured zeros. New unmeasured after-metrics are stored as NULL,
  legacy phantom zeros remain preserved but unmeasured, and AI performance
  payloads omit outcomes until a complete measurement exists (#702).

- Smart configuration now obtains table and date statistics through explicit
  catalog capabilities, adding bounded SQLite and ClickHouse analysis while
  keeping unsupported or failed statistics out of tuning history. Portless
  SQLite identities can now participate in exact-workload learning (#699).

- Load-time defaults again use their legacy worker, RAM-shaped chunk, reader,
  writer, buffer, upsert, and checkpoint formulas, independently of the
  history-aware cold-start baseline. Both paths still pass through the retained
  memory envelope and truthful connection-pool finalization, and explicit user
  values remain preserved (#711).

- Connection-pool sizing now uses one overflow-safe formula across tuning and
  configuration, rederives generated limits from the effective worker/reader/
  writer tuple, applies final limits to live pools, and persists the limits
  engines actually accepted in tuning history (#701).

- Smart configuration now batches server-engine date-column metadata into one
  schema query after deterministic tuning and history state are ready. Date
  metadata errors or timeouts emit one warning without discarding valid tuning,
  and out-of-scope tables are filtered without extra round trips (#700).

- Runtime writer growth now checks a recomputable per-table complete-inventory
  ceiling derived from the unified envelope and the table's own observed width.
  The model covers channel queues, reader scan slack, writer encoding, consumer
  handoff, and concurrent pipelines; accepted writer transitions ratchet later
  chunk and batch requests without mutating the ordinary steady policy. Missing
  analysis, unknown pressure, protocol-only limits, and one-row over-budget
  fallbacks disable resource growth (#709).

- Runtime memory rules now use the more constrained host or finite-cgroup
  pressure instead of Go heap allocation divided by runtime-reserved memory.
  Failed or unavailable telemetry is explicit and cannot authorize writer
  growth; heap allocation remains a separate diagnostic (#696).

- Smart configuration now separates representative workload width from the
  widest observed table-average fallback width. The representative width sizes
  the global performance policy, while table-specific widths gate runtime
  writer transitions and measured bytes govern steady admission (falling back
  to the widest observed width when transition evidence is unavailable).
  Unknown-width provenance is preserved, estimates round up, and extreme
  arithmetic saturates instead of wrapping (#703).

- Memory budgeting now resolves one host/cgroup-aware envelope during config
  loading and passes its exact budget to tuning, GC pacing, transfer buffers,
  and MemoryGuard. Linux cgroup v1/v2 limits fail closed when identified but
  unreadable, and small containers no longer inherit the tuner's former 1 GiB
  fallback floor (#708).

- Exploration-tier migrations now suppress runtime writer/chunk growth while
  retaining memory and error backoffs. Planned-grid rotation uses raw pair
  history, so safety-adjusted or otherwise filtered attempts do not pin the
  next sequential run to the same starting cell (#697).

- Completed migrations now stamp throughput and retry results onto the exact
  tuning-history row created for that run, preventing failed analyses or
  concurrent runs from attributing results to another configuration (#695).

- Concurrent table pipelines now share one migration-scoped memory-pressure
  guard, preventing a heap-threshold crossing from electing multiple
  simultaneous `FreeOSMemory` / forced-GC leaders. Direct transfer callers
  retain the prior per-pipeline fallback (#666).

- A successful final checkpoint save now supersedes an earlier trailing
  periodic-checkpoint failure, so a fully copied table is not falsely reported
  as failed. The degradation remains visible through a warning and a
  `checkpoint_periodic_save_degraded` audit event; skipped or failed final
  saves continue to fail closed (#665).

### Changed

- Cold-start autotuning again uses the pre-epic six-probe window over the
  original eight-cell ring. Raw attempts still advance that ring
  deterministically for replacement or forced probes, so safety-adjusted or
  filtered attempts cannot pin a workload to the same cell, and regression
  remains gated by its existing evidence floor (#698).

- Strict MySQL and SQL Server readers now have one-million-row live throughput
  proofs, per-engine blocking/prerequisite documentation, in-VM before/after
  benchmark evidence, and deterministic diagnoses for lock-window, shared-lock,
  and database-snapshot degradation classes (#685).

- Strict composite tuple-keyset transfers now use parallel readers whenever
  the source strategy can share one stable per-job view, including PostgreSQL
  exported snapshots, MySQL lock-window sessions, and SQL Server shared locks
  or database snapshots. Strict table partitioning is enabled only for
  migration-wide PostgreSQL and SQL Server snapshot epochs (#684).

- SQL Server migrations using `strict_consistency_scope: migration` now read
  every table through one engine-native database snapshot while source writers
  continue normally. Snapshots have deterministic run-scoped names, survive a
  process crash for resume, are retried on cleanup, and fail closed when
  permissions, edition, version, or the original resume snapshot are unsuitable
  (#683).

- SQL Server table-scoped `strict_consistency` now holds one audited shared
  table lock while ordinary pooled readers scan in parallel. This replaces
  progressive serializable range locking with a true point-in-time table view;
  error 1222 loudly falls back to the prior single-reader path (#682).

- MySQL/MariaDB `strict_consistency` keyset transfers can now use parallel
  readers by establishing pinned consistent snapshots inside a brief table
  read-lock window. Writers resume immediately after session setup; lock
  timeout or privilege failures emit an audited warning and retain the
  correct single-reader strict fallback (#681).

- Tuple-keyset tables with an int64-safe leading primary-key component now
  split that component into work-stealing reader ranges while retaining tuple
  ordering inside each range. Per-range typed watermarks make crash/resume
  duplicate-safe without target-side tuple cleanup; nonnumeric leading keys,
  converter-touched PKs, strict-consistency transfers, and legacy tuple
  checkpoints retain their established single-reader paths (#667).

- `strict_consistency_scope: migration` now gives PostgreSQL migrations one
  exported MVCC snapshot across every table, retry, and partition in the
  transfer phase. It restores strict table partitioning while preserving one
  source instant, fails closed for non-PostgreSQL sources, and records shared
  snapshot counts on partition tasks for later validation. The default
  `table` scope remains unchanged; a resumed process intentionally starts a
  new epoch (#663).

- PostgreSQL `strict_consistency` keyset transfers can now use multiple source
  readers without giving up their stable table view. DMT exports the lead
  transaction's MVCC snapshot and imports it as the first statement of every
  reader transaction, while reserving one source connection for the lead and
  clamping readers to the remaining pool capacity. Other engines retain the
  existing single-reader strict path, and strict table partitioning remains
  disabled pending a migration-scoped snapshot epoch (#662).

- Strict full-table transfers now persist the exact `COUNT(*)` observed inside
  their pinned source transaction. Validation compares the target with that
  snapshot count, so a faithful copy of a busy source no longer fails solely
  because the source changed afterward; the signed live-source drift is logged
  as informational. SQLite and YAML checkpoint state preserve the evidence
  across process boundaries; incremental date-filter jobs retain their
  live-count behavior (#664).

- `strict_consistency` now gives each source table one stable view across all
  keyset, tuple-keyset, and ROW_NUMBER pages. It starts the source transaction
  before target preparation, pins every query to it, forces one reader and one
  unpartitioned job per table, and fails closed for unsupported sources. The
  modes are PostgreSQL/InnoDB MySQL repeatable-read snapshots, SQL Server serializable
  range locks, and SQLite serializable reads; non-InnoDB MySQL tables fail
  before target mutation. Mutation-between-pages tests cover
  SQLite and live PostgreSQL; docs now state the per-table guarantee and its
  blocking/MVCC operational cost (#640).

### Added

- Runtime diagnostics now expose cumulative shared-memory-budget wait time and
  count to the controller, plus Prometheus `dmt_budget_wait_seconds_total` and
  per-table `dmt_live_writers` metrics. Live-writer samples include workers
  draining after a scale-down, making memory backpressure and writer
  convergence visible without a profiler (#668).

- Partial transfer outcomes now remain durably resumable: SQLite and YAML
  state store outcome separately from resume eligibility, `resume` retries
  failed tables while skipping successful peers, status/history expose the
  policy, and `resume --abandon` provides a target-leased operator workflow
  for accepting a recoverable run as terminal. `migration.allow_partial: true`
  is treated as explicit acceptance and is not auto-selected (#643).

- WebUI run history is now paginated and status-filterable. `GET /api/history`
  accepts `limit` (default 20, max 100), `offset`, and a `status` filter
  validated against the persisted set (`running`/`success`/`partial`/`failed`);
  an unknown status is a 400 rather than a silently-empty page. The response
  carries `{runs, total, limit, offset}` and the History view gains a status
  dropdown and a Prev/Next pager with an "X–Y of Z" summary. History rows now
  populate the Phase column, and the `⌘K` command palette lists every Checks
  command (Diagnose, Analyze, Config check) instead of just Preflight and
  Validate (#612, #613).

- WebUI dashboard telemetry surfaces failed-table count (highlighted once
  non-zero, visible mid-run), tables running now, and total rows alongside rows
  transferred; the terminal run state stamps the failed-table tally so a
  reloaded finished run still shows it (#612).

### Changed

- Transfer scheduling now models partition dependencies **per table** instead of
  through two global phases. Previously every non-partitioned job and every
  table's first partition had to finish before *any* table's remaining
  partitions could start, so one slow unrelated table stalled partition
  parallelism across the whole migration. Now a table's later partitions run as
  soon as that table's own first partition completes — even while an unrelated
  table is still transferring — and they are suppressed entirely if that first
  partition fails, so a table already known to have failed no longer writes more
  partial data. Global worker/connection concurrency stays bounded (#648).

- Keyset pagination now covers **single-column non-integer** primary keys
  (varchar/text, uuid, decimal/numeric) and **mixed-type
  composite** keys, not just integers — these tables previously fell back to
  the slow ROW_NUMBER window. Per-engine safety is built in: comparisons run
  under the source column's own collation (PK uniqueness under that same
  collation makes strict-`>` paging airtight), no MIN()/MAX() aggregate is
  ever issued (PostgreSQL `uuid` has none), crash-resume watermarks persist
  type-tagged (BIGINT precision, MySQL's `[]byte` text scans, SQLite BLOB
  storage-class keys, and invalid-UTF-8 SQLite TEXT keys all round-trip
  exactly), and unsafe cases are excluded by design — SQL Server
  `varchar`/`uniqueidentifier`/date-time PKs, ClickHouse non-unique keys,
  nullable PK components, and `BIGINT UNSIGNED` stay on ROW_NUMBER. Verified
  against live MySQL 8 (case-insensitive collation) and PostgreSQL
  (uuid/numeric) including mid-transfer kill and resume (#629, follow-up to
  #616/epic #621).

- Tables with an **all-integer composite primary key** now page via tuple
  keyset (`WHERE (a,b) > (?,?) ORDER BY a,b`) instead of the slow ROW_NUMBER
  window that re-scans deeper prefixes each chunk. Restricted to integer,
  non-null, non-`BIGINT UNSIGNED` components on engines with unique primary
  keys (PostgreSQL/MySQL/SQL Server/SQLite via the appropriate row-value or
  OR-chain form; ClickHouse and non-integer/mixed composite keys keep
  ROW_NUMBER). Resume preserves the exact int64 watermark tuple (#616, epic
  #621).

- Transfer memory is now bounded by a shared byte budget that charges each
  in-flight chunk its **actual** measured size, instead of pre-sizing channel
  depths from a static per-row estimate. Tables with rows far larger than the
  estimate (big TEXT/blob columns) no longer balloon heap past the configured
  limit and trip the reader-pausing memory guard, and the budget is shared
  across concurrently-migrating tables — a table running alone can use all of
  it — rather than statically split by worker count (#617, epic #621).

- Checkpoint saves during a transfer are now written on a dedicated
  background goroutine (latest-wins, coalescing) instead of inline on the
  chunk-acknowledgement path. A slow checkpoint store (SQLite fsync, a
  YAML-file rewrite) no longer stalls acknowledgement processing or, through
  it, the writers. The final end-of-table checkpoint is still written
  synchronously so resume points stay durable, and a persistently failing
  checkpoint store is now surfaced loudly instead of silently dropping
  resumability (#620, epic #621).

- Keyset transfers now split a table's PK range into many small sub-ranges
  (≈8 per reader, capped) that reader goroutines pull from a shared work
  queue, instead of one fixed contiguous range per reader. Tables with
  skewed primary keys — large deleted-ID gaps, snowflake-style IDs — no
  longer finish at the speed of a single straggling reader; the load
  rebalances across all readers automatically. Resume is unchanged and
  restores whatever sub-range set was persisted (#615, epic #621).

- Internal: the keyset and ROW_NUMBER transfer strategies now share one
  pipeline runner (consumer loop, buffer sizing, memory guard, reader-cancel
  discipline, writer-pool wiring, drain, final progress save) instead of
  duplicating ~180 lines each; ordered-ack sequencing is a single shared
  implementation. No behavior change — SQL, checkpoint encoding, and resume
  semantics are preserved verbatim (#614, PR0 of epic #621).

### Fixed

- Incremental sync no longer permanently drops an update made behind the resume
  cursor. Each run samples an immutable upper fence H1 once at start (persisted
  per run in SQLite and YAML state, read back unchanged on resume), and the sync
  watermark advances only to H1 — never to a value re-sampled during the run —
  so a row updated while the run was down can no longer be skipped by the
  positional cursor *and* fenced out of future runs. Incremental upsert resume
  additionally replays the changed-row window from the start rather than
  continuing from the saved cursor; upsert idempotency makes this exactly-once
  logically. Fence persistence is a required, lease-fenced checkpoint write;
  storage errors and corrupt fence values fail closed before target mutation
  instead of silently re-sampling on resume (#647).

- Writer downscale no longer lets retired idle workers consume new jobs. A
  downscale now waits for idle retirees to acknowledge cancellation before it
  returns, closing the cancel-vs-ready-job select race, while workers already
  mid-write finish that one committed chunk exactly once. Busy retirees are
  tracked as draining; a rapid 4→1→4 defers replacement goroutines until they
  exit, so the live ceiling never reaches seven. Desired, active, draining, and
  live worker-count semantics are documented and observable (#642).

- Migration run and resume now acquire an exclusive lease for the canonical
  target driver/host/database/schema before any target mutation. Live owners
  reject competing processes, stale takeover atomically increments a fencing
  generation, and former owners can no longer update run, task, or transfer
  progress state. SQLite and YAML file state provide equivalent cross-process
  ownership semantics, with lease-loss cancellation and recovery guidance
  (#638).

- Required checkpoint writes now fail closed. Transfer tasks are persisted
  before target DDL/truncation, task creation and status failures cannot fall
  through with task ID zero, unresolved periodic/final progress failures stop
  table success, and failed table/run terminal writes prevent a success
  result. SQLite and file state reject unknown task IDs, terminal SQLite run
  updates verify the run exists, and durability failures carry state exit code
  6 with repair-and-resume guidance. Aggregate table success and incremental
  watermarks are committed atomically so resume cannot skip a watermark retry
  after a partial state write (#645).

- Transfer checkpoint identity is now structured by task type, schema, table,
  and optional partition in both SQLite and file state. Quoted identifiers
  containing dots, colons, percent signs, underscores, or backslashes can no
  longer collide with partition tasks or be swept up by prefix cleanup. New
  task keys are collision-free display values; ambiguous incomplete legacy
  checkpoints fail with fresh-run recovery guidance instead of guessing (#646).

- ClickHouse keyset retry/resume no longer falls through to SQL Server
  bracket/`@p` cleanup SQL and then replays rows after the cleanup error.
  Keyset cleanup is now an explicit target capability: unsupported synchronous
  cleanup and any cleanup execution failure abort before replay writes, with a
  fresh truncate/recreate recovery path; fresh partition transfers remain
  supported because they require no replay cleanup (#644).

- A PostgreSQL COPY sub-batch deadline is now reported as a failed table after
  retries are exhausted, instead of being misclassified as operator/run
  cancellation and leaving the migration falsely interrupted. Only the parent
  run context can select the interrupted/resumable path; sibling table failures
  are preserved in the partial-run result (#641).

- A target-writer failure now immediately cancels in-flight source reads,
  instead of leaving the transfer blocked until an unrelated slow or locked
  source query returned. Pipeline shutdown preserves the original writer
  error and behaves the same with or without the shared memory budget (#639).

- Keyset pagination now includes the minimum representable primary-key value
  instead of simulating an inclusive lower bound by decrementing it. Fresh
  pages use an explicit inclusive comparison, while subsequent pages and
  resume checkpoints remain exclusive; this prevents silent loss at
  `BIGINT`/unsigned boundaries without replaying checkpoint rows (#637).

- A successful timeout retry no longer inflates persisted checkpoint
  `rows_done` — and with it run summaries, `status`/`history` output,
  notifications, and tuning throughput rows (observed: SO2010 `Posts`
  over-reported by 67,289 rows after two COPY timeout retries, while the
  target data itself was correct and validation passed). Checkpoint
  coordinators previously derived `rows_done` from the writer pool's
  write-attempt counter, which runs ahead of the persisted watermark; rows
  counted beyond the watermark were replayed on retry and counted twice.
  Progress is now accumulated from write acks applied in sequence order, so
  every checkpoint's `rows_done` exactly matches the watermark/range state
  saved alongside it. Source row-count estimates are still treated as
  estimates — nothing is clamped to `rows_total` (#632).

- `dmt resume` no longer dead-ends on the `drop_recreate` backup-acknowledgment
  preflight gate. Resuming an interrupted run legitimately finds the run's own
  partial data in the target, which tripped the gate — and its remedy named
  `--confirm-backup`, a flag only `run` defines. Now the gate is skipped for a
  resume that owns the target (verified config hash + the run reached the
  transfer phase), so the target's contents are provably that run's output; a
  run killed before transfer, a legacy run without a config hash, or a drifted
  `--force-resume` still faces the gate so it can't silently `drop_recreate`
  over pre-existing data. When the gate does fire, its remedy now also names the
  resume-side hatch (`--skip-preflight backup`) so `resume` is never dead-ended
  (#623).

- A failing pre-transfer table truncate (permission denied, lock timeout) is
  now logged with a warning that points at the likely consequence, instead of
  being silently discarded and resurfacing later as a confusing duplicate-key
  error against un-truncated rows. A genuinely absent table stays quiet as
  before (#619, epic #621).

- WebUI one-click token login works on Safari < 16: it falls back to
  dispatching a cancelable submit event when `form.requestSubmit()` is
  unavailable, instead of silently leaving the operator at a blank login form
  (#612).

## [5.4.0] - 2026-07-05

### Security

- WebUI gained `--webui-trusted-proxy <cidr>` (repeatable): behind a reverse
  proxy it lets the login/auth limiter and audit logging attribute requests to
  the real client via `X-Forwarded-For` instead of the shared proxy IP, so a
  single client can no longer cause a lockout for everyone. Off by default and
  spoof-safe — `X-Forwarded-For` is honored only when the direct peer is in the
  trusted set (#604).

- Guided setup (`dmt setup`, TUI, and the WebUI setup wizard) now stores DB
  passwords in `0600` sidecar files referenced by `${file:…}` instead of
  writing them as plaintext into the config YAML. A new step lets you opt back
  into plaintext; existing `${env:…}`/`${file:…}` references are left untouched
  (#597).

- WebUI CSP tightened to `script-src 'self'` (no `'unsafe-inline'`) — the SPA
  has no inline scripts, so inline `<script>`/handler injection is now blocked
  outright; the `handleStatusByID` not-found error is scrubbed, keeping the
  vetted single error egress (#603). Added soak coverage (event-hub churn, SSE
  connect/disconnect churn, and repeated-migration goroutine-bound assertions)
  guarding the long-lived server against leaks (#595).

- WebUI remote-deployment hardening (#594, #601, #602): brute-force throttling
  on failed auth (login and bearer) with per-IP lockout; a 16-character minimum
  for an operator-chosen token on a non-loopback bind; sessions now slide while
  in use (so watching a long migration never expires the session or blocks
  Cancel) up to a 7-day absolute cap; and the WebUI-session `metrics-addr` is
  pinned to loopback (the `/metrics` listener is unauthenticated — a public
  bind is a CLI-launch-only decision). The limiter keys on the connecting IP
  and, by default, does not trust `X-Forwarded-For`; see the
  `--webui-trusted-proxy` entry above for the per-client option (#604).

### Added

- WebUI origin picker: a "Browse configs & profiles" dialog lets operators pick
  a saved profile or a config file discovered on the server (new
  `GET /api/configs`, scoped to the launch/config directories) instead of
  typing a path. The History view now scopes to the chosen config/profile,
  fixing the case where it errored on a missing `config.yaml` or omitted runs
  launched with a different config (#590).

- WebUI command-parity registry (`command.WebSurface`) machine-checked against
  the CLI registry, an HTTP-to-SQLite migration integration test, and
  `docs/WEBUI.md` with a server-deployment guide (reverse-proxy TLS) —
  completing the WebUI epic (#583, #577).

- WebUI frontend: a modern single-page operator console (no-build vanilla
  JS/CSS embedded in the binary) — login, a live migration dashboard, checks,
  guided setup, profiles, settings, and a ⌘K command palette. Also gives
  `driver.PreFlightFinding` snake_case JSON tags so the WebUI and CLI `--json`
  emit consistent finding keys (#582).

- WebUI interactive flows: a guided setup wizard (driving the shared
  `internal/setup` state machine), profile management, secrets init, cache
  clear, and server-side session defaults honored by subsequent commands.
  Profile export is confined to `~/.dmt/exports` with a sanitized file name so
  a client-supplied path can't write decrypted secrets anywhere (#581).

- WebUI live migration: run and resume from the browser with progress streamed
  over Server-Sent Events, a single-flight guard (one migration at a time), and
  cancel. Also fixes a latent goroutine leak in the progress tracker that a
  long-lived `SetProgressReporter` user (the WebUI server) was the first to
  expose — `Tracker.Close()` now stops the periodic report loop (#580).

- WebUI read/advisory REST API: authenticated JSON endpoints for status,
  history, validate, diagnose, preflight, config-check (dry run), analyze, and
  AI config-review. Handlers call the orchestrator in-process and return its
  structured results; the history response omits the serialized config so DB
  secrets never reach the browser (#579).

- WebUI foundation: `dmt --webui` launches a browser front-end — the third
  front-end alongside the CLI and TUI (epic #577). Owns the HTTP server
  lifecycle, embedded single-binary assets, and the security baseline:
  loopback-only by default with an auto-generated token; a non-loopback
  (remote) bind requires `--webui-auth-token` plus TLS
  (`--webui-tls-cert`/`--webui-tls-key`) or `--webui-insecure` behind a
  TLS-terminating proxy; token→session-cookie auth with constant-time
  compare, security headers, and a DNS-rebinding Host-header guard (#578).

### Changed

- Updated Anthropic defaults, generated templates, and active configuration
  examples to use Claude Sonnet 5 (`claude-sonnet-5`); Sonnet 5 requests now
  omit unsupported sampling fields, disable adaptive thinking, and use the
  configured Anthropic output token budget for complex SQL/JSON prompts (#535).

### Fixed

- WebUI dashboard now shows the final row/table totals for a finished run after
  a page reload. `/api/run`'s run state carries the counts (stamped at
  completion, keyed by run id), so a client with no live progress stream no
  longer shows zeros (#591).

- Guided setup (`dmt setup` and the WebUI setup wizard) can now configure a SQL
  Server source/target that has no usable TLS certificate. The MSSQL step
  previously only asked "Trust server certificate?", setting `trust_server_cert`
  but never `encrypt` — which defaulted to `true`, so the connection test did a
  TLS handshake that fails against servers like Azure SQL Edge or dev instances
  without TLS. The step now offers `require` / `trust` / `disable`, setting
  `encrypt` accordingly (legacy `y`/`n` still map to trust/require).

- `sanitizeErrorResponse` no longer panics (or mis-redacts) on API error bodies
  containing runes whose `strings.ToLower` changes byte length (e.g. U+023A
  `Ⱥ`). It searched a lowercased copy but sliced the original with those
  offsets, so a length-shifting rune before a key token could push the index
  past the string and cause an out-of-range slice — turning an ordinary API
  error into a run-aborting panic. It now folds only ASCII case
  (length-preserving) for the search (#562).

- The TUI now offers `/run --confirm-backup`, acknowledging the drop_recreate
  backup gate the preflight enforces against a non-empty target. The
  command-parity registry previously claimed the TUI "confirms interactively",
  but no such surface existed — TUI users hit a blocking finding whose only
  workarounds were hand-editing the YAML or skipping preflight entirely. The
  registry now names the real flag (#568).

- TUI robustness cluster: (a) `/run`//`/resume` now mark the migration running
  synchronously, so a second `/run`//`/resume` issued while connections are
  still dialing is rejected instead of starting a second concurrent migration
  (#557); (b) `/status`//`/history` capture no longer deadlocks on output larger
  than the OS pipe buffer (it drains concurrently) and the per-command
  `os.Stdout` redirect is serialized against the migration's, panic-safely, so
  the two can't race the global or restore each other's closed pipe (#556);
  (c) Esc during a running migration now cancels it gracefully (context cancel +
  checkpoint flush) like Ctrl+C instead of killing the process mid-flight (#558);
  (d) the periodic `git` status refresh runs off the Bubble Tea event loop, so a
  slow `git status` (large repo, NFS/WSL2) no longer freezes the UI every tick
  (#559).

- A resume that fails before the transfer phase for an environmental reason
  (preflight check, schema extraction, target preparation, or Ctrl+C) now
  leaves the run resumable instead of marking it `failed`. Previously such a
  failure orphaned all checkpointed progress — `GetLastIncompleteRun` only
  returns `running` runs, so the next `dmt resume` found nothing and the whole
  migration had to restart (#566).

- The resume summary, throughput, and AI tuning history now count only the rows
  moved during that resume (checkpointed cumulative minus the count captured
  before the resume started transferring), not the source-side `RowCount`
  estimates or each table's full size. Feeding cumulative full-migration rows
  over a resume-only duration inflated the throughput persisted to
  `ai_tuning_history` and skewed smartconfig training (#565).

- The AI table-DDL cache key now includes each column's `IsIdentity` and
  `DefaultValue` (both feed the generated CREATE TABLE), so toggling a column's
  identity or default — type unchanged — no longer serves stale cached DDL that
  silently drops the identity/default on the target (#560).

- The AI type-mapping cache (`~/.dmt/type-cache.json`) is now written
  atomically (temp file + fsync + rename) and in-process saves are serialized,
  so concurrent or crash-interrupted writes can't tear the file. A torn file
  fails its checksum on load and discards the whole cache, re-billing every
  previously-paid AI mapping. `dmt cache clear --ai-only` uses the same atomic
  write (#563).

- `GetLastSyncTimestamp` now checks the scan error before inspecting the
  result, so a real DB error (e.g. "database is locked") propagates instead of
  being swallowed as "never synced" — which silently downgraded date-based
  incremental sync to a full-table reload (#564).

- The chunk-size retry after a write error no longer duplicates rows on
  non-transactional (MySQL batched-INSERT) targets. Earlier sub-batches of a
  failed chunk autocommit independently, so retrying the whole chunk from row 0
  re-inserted them (drop_recreate has no PK on the target during transfer to
  absorb the duplicates). The write path now reports how many rows committed
  (`driver.PartialWriteError`) and the retry resumes after that prefix.
  Transactional targets (PostgreSQL/SQL Server/SQLite) roll the failed chunk
  back and are unaffected (#541).

- MySQL `LOAD DATA LOCAL INFILE` bulk loads now verify the load was lossless
  before acking a chunk: a `RowsAffected` mismatch (duplicate-key rows silently
  dropped under LOAD DATA's implicit IGNORE) or an Error/Warning-level
  conversion (string truncation, out-of-range clamping, bad datetime) now fails
  the chunk instead of silently corrupting data. Note-level adjustments (e.g.
  decimal fractional rounding) are tolerated to match the batched-INSERT path's
  strict-mode behavior (#544).

- PostgreSQL source introspection now flags `GENERATED ALWAYS/BY DEFAULT AS
  IDENTITY` columns (PG 10+, which have a NULL default) as identity, not only
  legacy `serial`/`nextval` columns. Previously such columns lost their
  auto-generation on the target (created as plain integers) and were skipped by
  sequence reset; they now map to the target's SERIAL/IDENTITY/AUTO_INCREMENT
  form and get their sequence reset (#546).

- The SQL Server identity reseed (`DBCC CHECKIDENT`) now escapes single quotes
  in the qualified table name before interpolating it into the statement's
  string literal. A legal table name containing an apostrophe (e.g.
  `[It's Data]`) no longer breaks sequence reset, and a hostile source table
  name can no longer terminate the literal to inject SQL into the target
  session (#548).

- SQL Server upsert MERGE change detection now compares values byte-exactly
  (`CONVERT(VARBINARY(MAX), …)`) instead of a collation-sensitive `<>`.
  Under the default case-insensitive, ANSI-padded collation a case-only or
  trailing-space source change (e.g. `smith` → `Smith`) was seen as "no
  change" and silently left the target stale; it now propagates on
  `WHEN MATCHED` (#547).

- SQL Server staging/spatial column introspection now checks `rows.Err()`
  after iterating, so a mid-result-set read error or cancellation surfaces
  instead of silently returning a truncated column list (which could drop a
  spatial column from WKT re-typing) (#567).

- AI-generated finalization DDL (indexes, foreign keys, check constraints) is
  now validated as a single statement that targets the expected table before
  it executes, instead of a prefix-only check. A prompt-injected or misbehaving
  model can no longer smuggle a trailing statement (e.g.
  `... ; DROP TABLE users;`) or retarget another table. The validator rejects
  SQL comments and dialect string-escaping it cannot faithfully model
  (backslash escapes, Postgres dollar-quoting) so a real statement separator
  can't hide from the scan, and the prompts frame source-derived identifiers,
  filters, and expressions as untrusted data (#561).

- Secret template expansion (`${env:}`/`${file:}`/`${VAR}`) now happens
  per-scalar on the parsed YAML node tree instead of as raw text substitution
  over the whole document. A secret value containing `#`, a newline, or `:`
  (e.g. `secret #2024`, a PEM key, a JSON blob) can no longer be truncated as
  a YAML comment or inject/override config structure such as the target
  connection. Embedded and multiple templates within one value still resolve
  (#552).

- `secrets.Save` no longer silently rewrites `~/.secrets/dmt-config.yaml`
  from a zero-value config when the existing file has a YAML syntax error —
  it now surfaces the parse error and refuses to write, preserving the
  encryption master key, other AI providers, the Slack webhook, and
  migration defaults. Both `Save` and `SaveSlackWebhook` now write
  atomically (temp file + fsync + rename) so a crash mid-write can't
  truncate the secrets file and lose the master key (#551).

- PostgreSQL migrations now fail before any DDL when two source identifiers
  would sanitize to the same PostgreSQL name (e.g. `Order Items` /
  `Order-Items`, or `Users` / `USERS` under a case-sensitive source
  collation) instead of silently destroying a colliding table's data in
  drop_recreate. `ident.SanitizePG` also truncates identifiers to
  PostgreSQL's 63-byte limit with a disambiguating hash suffix so distinct
  long names no longer collapse into the same relation (#553).

- Hardened P1 migration safety paths: resume/upsert handling, delete
  reconciliation key matching, ROW_NUMBER/keyset retry behavior, MySQL
  type introspection and DDL execution, secret redaction, file checkpoint
  task IDs, AI config validation, and TUI password masking (#569).

## [5.3.0] - 2026-06-11

### Added

- TUI command discovery now matches the real surface: autocomplete and
  `/help` list every supported command (`/preflight`, `/diagnose`,
  `/ai`, `/session`, `/init-secrets`, `/cache`, ...) and a TUI-side
  parity test pins them against the command registry so CLI/TUI drift
  fails CI. New docs/TUI_COMMANDS.md parity table; README and RUNBOOK
  gained TUI equivalents for preflight, dry-run, AI review, and
  diagnose workflows (#446).

- `/session` gains run observability and audit keys: `metrics-addr`,
  `otel-endpoint` (Prometheus/OTLP wiring identical to the CLI flags),
  `audit-dir`, `audit-tamper-evident`, `no-audit`, and `log-format`
  (applies immediately). Values are validated when set. JSON/file
  output modes stay CLI-only by policy: the TUI renders structured
  blocks and `/logs` saves the session transcript (#445).

- New `/init-secrets` TUI command (`--with-ai`, `--force`) and
  `/cache clear` (`--ai-only`; requires `--confirm` and names the exact
  cache file and scope first). `/setup` is documented as the richer
  guided path and `/wizard` as the lightweight config editor, and TUI
  wording now says "smartconfig analysis" for the deterministic
  analyzer, reserving "AI" for provider-backed features (#443).

- New `/ai config-review` TUI command (alias `/ai runbook`) generates
  the same advisory config patch recommendations and migration runbook
  as `dmt ai config-review`, with `--timeout` and free-text `--request`.
  `/analyze` gains `--ai-explain` for the advisory AI explanation of
  deterministic smartconfig suggestions. `ai evals` stays CLI-only
  (developer harness) (#442).

- `/validate` now supports `--ai-triage` (advisory AI review of
  validation results with deterministic facts rendered first) and
  `--timeout`. New `/diagnose` TUI command mirrors `dmt diagnose`:
  triage the latest or a selected failed run with `--run`,
  `--ai-triage`, and `--timeout` (#441).

- New `/preflight` TUI command (alias `/health-check`) runs the same
  connectivity, privilege, version, and encoding checks as
  `dmt preflight`, with `--skip-preflight` and `--ai-review` for the
  advisory AI readiness review (#440).

- `/run` now accepts the CLI run command's flags: `--dry-run` (with the
  same human-readable plan preview, including delete reconciliation),
  `--ai-schema-advisor`, `--source-schema`, `--target-schema`,
  `--workers`, and `--skip-preflight`. `/resume` gains `--force-resume`
  and `--skip-preflight` (#439).

- TUI slash commands now share one argument parser: `@config` files,
  positionals, `--flag value`/`--flag=value`, and consistent errors for
  unknown flags and missing values across every command. New `/session`
  command holds sticky per-session defaults (config, profile, state-file,
  verbosity) so they don't have to be repeated on each command (#444).

- Added measured override-cost advice: when a pinned `write_ahead_writers`
  value's history bin materially underperforms the best comparable bin,
  every run logs the measured means and delta next to the provenance line
  (#461). Added `migration.tuning: auto|manual` as the coarse switch for
  pre-run parameter derivation; per-knob pins remain the escape hatch.

- Added a run-start `Tuning provenance` log line showing which performance
  parameters are user-pinned (and therefore never tuned) versus
  tuner-derived; `config.yaml.example` now leaves tuning to the deterministic
  tuner by default, with pinning documented as an advanced override (#461).

### Fixed

- Keyset transfers now checkpoint every parallel reader range's watermark
  (`transfer_progress.range_state`); resume continues each range from its
  own position instead of restarting all readers at the single safe
  minimum and deleting the faster readers' completed work. Pre-existing
  checkpoints resume with the legacy single-watermark behavior (#464).

- Set the Go runtime soft memory limit (GOMEMLIMIT) from the effective
  memory budget so GC paces against it natively; the transfer memory guard
  becomes the backstop and now uses a single process-wide heap sampler
  instead of per-chunk `ReadMemStats` calls, firing `FreeOSMemory` once per
  pressure episode instead of every 500ms (#462). An operator-supplied
  GOMEMLIMIT environment variable is honored untouched.

- Fixed tuning-history feedback contamination: runs where the runtime
  controller (or structural write-error adjuster) changed parameters
  mid-migration are now flagged `adjusted_at_runtime` in `ai_tuning_history`,
  and the deterministic tuner excludes them from all training cohorts —
  regression, smoothed bins, drift detection, and the exploration bucket
  count — with the exclusion reported in the tuning reasoning (#451).
- Fixed the deterministic tuner's regression tier rarely engaging: the row
  floor is now degrees-of-freedom-based (12 rows for the production cohort
  shape instead of 30), single-level categorical columns are dropped from
  the design matrix, and the residual outlier filter keeps its conservative
  60-row gate (#452).

## [5.2.0] - 2026-05-31

### Added

- Added repeatable AI advisory eval scenarios and an explicit `dmt ai evals`
  live-provider command for comparing prompt/model quality without making
  normal tests call external providers (#427).
- Added DLT-style `migration.schema_contract` settings with `tables`,
  `columns`, and `data_type` entities, including DMT's `report` mode for
  report-only drift handling (#403).
- Added table-level schema contract handling so `tables: evolve` creates newly
  detected target tables before upsert transfer, finalizes configured secondary
  DDL after transfer, and `tables: discard_row` skips newly added source tables
  for the run (#405).
- `tables: freeze` now blocks table add/drop drift before transfer (#405).
- Added column-level `schema_contract` parity with `columns: discard_row`
  support for skipping affected tables, identity guardrails for
  `columns: discard_value`, and explicit dropped-source-column reporting (#406).
- Added data-type schema contract modes so `data_type: discard_row` skips
  affected tables, `data_type: discard_value` omits non-key, non-identity,
  non-date-tracking columns with data type drift, and unsafe
  `data_type: evolve` drift remains blocked with clearer reporting (#407).
- Added structured schema contract decision records in drift audit payloads and
  JSON result/status output, and made freeze violations identify the exact
  entity, drift kind, table, column, and suggested next policy options (#408).
- Added schema contract parity integration coverage for SQLite and
  MSSQL-to-Postgres migrations, including end-to-end checks for evolved,
  frozen, discarded-row, and discarded-value contract outcomes (#409).
- Added opt-in AI preflight readiness reviews for `dmt preflight`, including
  redacted structured prompts, deterministic fallback behavior, and JSON/human
  output support (#417).
- Added `dmt run --dry-run --ai-schema-advisor` for advisory schema drift and
  schema evolution guidance that preserves deterministic policy gates (#399).
- Added `dmt analyze --ai-explain` performance explanations for deterministic
  smartconfig choices with redacted payloads and terminal-safe AI advisory
  output (#402).
- Added `dmt ai config-review` to produce redacted config patch guidance and
  operator runbooks with unsafe AI recommendations suppressed (#401).
- Added `dmt diagnose` and `dmt validate --ai-triage` advisory failure triage
  flows with redacted AI prompts, constrained suggested commands, and
  deterministic fallbacks (#400).
- Added foundation payloads/parsers/fallbacks for upcoming AI failure triage,
  config/runbook review, and performance explanation copilot flows (#400,
  #401, #402).

### Deprecated

- `migration.schema_evolution` now emits a runtime deprecation warning because
  it is being replaced by DLT-style `migration.schema_contract` settings for
  tables, columns, and data_type (#403).

### Fixed

- Updated OpenAI defaults to `gpt-5.5` and omitted unsupported temperature
  overrides for GPT-5-family OpenAI requests so live AI advisory evals can use
  the latest OpenAI model (#435).
- Hardened AI advisory eval wording checks so cautious evidence-limit phrasing
  is not mistaken for overconfident causality while unqualified root-cause
  assertions and destructive target-action advice remain flagged (#434).
- Hardened AI triage prompt and eval matching so deletion-drift category
  wording is not mistaken for unsafe target-schema action advice (#433).
- Tightened AI advisory prompt contracts and eval evidence so Haiku preserves
  deterministic gates, avoids unsafe command/action wording, and cites
  deterministic support across config review, schema advisor, triage, and
  performance guidance (#432).
- Fixed AI advisory scrubbers so short connection identifiers no longer redact
  substrings inside normal words while real secrets remain protected (#423).
- Calibrated AI triage output so sparse validation mismatches avoid
  unsupported high-confidence root-cause claims and repeated unsafe-command
  suppression text is deduplicated (#425, #426).
- Validated AI advisory config suggestions against the real DMT config surface
  so unsupported paths and enum values are suppressed or marked invalid across
  config review, schema advisor, triage, and performance guidance (#424).
- Fixed AI preflight review follow-ups so absent providers return the intended
  unavailable result, connection error details are omitted from AI-bound
  payloads, and the advisory review gets its own command-scoped timeout (#417).
- Fixed PR review text artifacts by rendering SmartConfig YAML comments with
  ASCII status labels and documenting MySQL enum escaped quotes with the
  correct doubled single-quote form (#393).
- Hardened architecture follow-up paths after the review epic: force-resume
  now rejects incompatible config drift, ROW_NUMBER resume/runtime tuning
  have targeted regression coverage, drop-recreate recovery/finalization paths
  are clearer and safer, and AI fallback calls support `max_requests` with
  table-DDL in-flight deduplication (#388, #389, #390, #391, #392).

## [5.1.0] - 2026-05-22

### Added

- Added read-only source schema drift detection with persisted snapshots,
  structured change reports, and an optional `migration.fail_on_schema_drift`
  pre-transfer safety gate (#305).
- Added initial shared driver SQL-shape helpers and a shared-driver
  architecture note to guide behavior-preserving DRY work across database
  engines (#338).
- Added shared driver SQL helpers for ordered primary-key scans and bounded
  primary-key delete statements, giving delete reconciliation a tested
  cross-engine SQL shape to build on (#338, #351).
- Added `migration.deletes` config parsing and validation for the first
  delete-reconciliation slice, with `off` as the default and `reconcile`
  limited to upsert-mode hard-delete settings (#351).
- Added checkpoint-backed delete reconciliation scheduling state and dry-run
  due/not-due preview output for the #351 runtime rollout.
- Added reusable primary-key reconciliation primitives that scan source/target
  key sets, identify target-only keys, and execute parameter-bounded hard
  deletes for the #351 runtime path.
- Added the first runtime delete-reconciliation pass for upsert mode, wiring
  interval-gated primary-key reconciliation into run and resume before
  validation.
- Added persisted per-table delete-reconciliation counts to run results and
  operator summaries so delete runs report candidate and deleted row counts
  by table (#351).
- Changed upsert row-count validation to require source/target parity after
  the current run completes delete reconciliation, while preserving the target
  superset allowance when reconciliation is disabled or not due (#351).
- Added due-only dry-run candidate delete counts for delete reconciliation,
  including per-table skipped/error reporting without mutating targets (#351).
- Added SQLite end-to-end integration coverage proving delete reconciliation
  removes a source-side hard delete from an upsert target and records counts
  in checkpoint state (#351).
- Added opt-in schema evolution for `added_column` drift in upsert mode,
  including per-policy config, nullable target `ADD COLUMN` support across
  drivers, and operator documentation (#306).
- Added opt-in schema evolution for safe `nullability_change` drift, relaxing
  target columns from `NOT NULL` to `NULL` while leaving tightening out of
  auto-apply (#306).
- Added the MSSQL → Postgres pair to the nightly cross-engine integration
  matrix so the primary migration path has scheduled coverage as well as
  per-PR coverage (#342).
- Added an MSSQL → Postgres schema-evolution integration test that proves
  `added_column: auto` alters the target and transfers the new column value
  during a follow-up upsert run (#306).
- Added `migration.schema_evolution.added_column: discard_value` (with
  `discard` alias) to keep transferring rows while omitting newly added source
  columns from target DDL, writes, validation, and schema snapshots.
- Added explicit `migration.schema_evolution.type_change` policy support so
  operators can opt into widened source type ALTERs while keeping narrowed and
  lossy type drift guarded (#395).
- Added an MSSQL → Postgres daily-driver integration test that proves a
  `drop_recreate` baseline seeds date-column watermarks, one changed source
  row transfers during the next `upsert`, and an unchanged follow-up transfers
  zero rows (#304).
- Added a reusable driver conformance harness and wired SQLite/Postgres into
  the first fast contract checks for future engine work (#368).
- Added daily-driver ops summaries: `run`/`resume` now print a structured
  completion summary, dry runs include preflight/drift checks and duration
  estimates from recent history, and `migration.notify` controls completion
  alerts (#308).
- Added a delete-handling design proposal covering tombstones, periodic key-set
  reconciliation, CDC, and the proposed `migration.deletes` configuration
  surface (#307).

### Changed

- Centralized shared driver raw SQL and row-count helper plumbing across
  matching SQL-backed readers and writers, reducing duplicate driver code
  while keeping engine-specific fast paths local (#338).
- Centralized shared reader sampling scan helpers while keeping
  engine-specific sample queries and casts in concrete drivers (#338).
- Centralized partition-boundary result scanning for readers whose partition
  queries already return the same row shape (#338).
- Centralized schema column metadata scanning for information_schema-backed
  readers while keeping catalog SQL and engine semantics local (#369).
- Moved the Postgres and MySQL readers onto shared streaming/pagination
  control flow while keeping dialect-specific SQL generation local (#338).
- Moved the SQLite reader onto shared streaming/pagination control flow while
  preserving its dialect-owned query generation (#338).
- Moved the MSSQL reader onto shared streaming/pagination control flow while
  preserving table hints and existing query error labels (#338).
- Centralized preflight framework helpers for shared finding construction,
  connection checks, backup-ack gating, and pool-headroom decisions while
  keeping engine-specific probe SQL in concrete drivers (#338).
- Split the P1 oversized production files into focused same-package
  files to improve code readability without changing behavior (#212).
- Split the PostgreSQL writer into focused same-package files for DDL,
  row counts, COPY batching, upsert, raw SQL helpers, and writer lifecycle
  so the first P2 readability story is easier to review and maintain (#212).
- Split the setup wizard state machine into focused same-package files for
  prompts, input processing, connection config expansion, and helper defaults
  so the next P2 readability story is easier to maintain (#212).
- Split the tuning history selector into focused same-package files for
  dispatch, regression argmax, formatting, retry filters, outlier filters,
  and WAW-bin selection as the next P2 readability story (#212).
- Split the SQL Server reader into focused same-package files for schema
  extraction, constraints, row counts, partitioning, sampling, reads, and
  compatibility checks as the next P2 readability story (#212).
- Split the file checkpoint backend into focused same-package files for
  persistence, runs, tasks/progress, sync timestamps, AI-history no-ops,
  fallback events, and lifecycle helpers as the next P2 readability story
  (#212).
- Split the tuning regression model into focused same-package files for
  model definitions, fitting, prediction intervals, math helpers, and
  residual outlier filtering as the next P2 readability story (#212).
- Split the MySQL writer into focused same-package files for DDL,
  batch/upsert writes, row counts, raw SQL helpers, and value conversion
  as the next P2 readability story (#212).
- Split the SQLite reader into focused same-package files for schema
  introspection, streaming reads, row counts, partitioning, and sampling
  as the next P2 readability story (#212).
- Split secrets configuration handling into focused same-package files for
  config file persistence, validation/defaults, and generated templates as
  the next P2 readability story (#212).
- Split the MySQL reader into focused same-package files for schema
  introspection, streaming reads, row counts, partitioning, and sampling
  as the next P2 readability story (#212).
- Split the PostgreSQL reader into focused same-package files for schema
  introspection, streaming reads, row counts, partitioning, and sampling
  as the next P2 readability story (#212).
- Split orchestrator healthcheck and analysis code into focused
  same-package files for dry-run health checks, config analysis, offline
  system suggestions, tuning history, and DB tuning recommendations as the
  next P2 readability story (#212).
- Split the runtime monitor controller into focused same-package files for
  decision evaluation, action application, and controller helpers as the
  next P2 readability story (#212).
- Split the orchestrator transfer runner into focused same-package files for
  pre-transfer setup, job execution, error diagnosis, and transfer logging as
  the final original P2 readability story (#212).
- Split orchestrator status rendering into focused same-package files for
  summary output, detailed status, fallback reporting, history display, and
  result reconstruction as an additional readability cleanup (#212).
- Split the audit logger into focused same-package files for logger
  lifecycle, path resolution, field scrubbing, canonical JSON, and hash
  chaining as an additional readability cleanup (#212).

### Fixed

- Run summaries and superseded-run detection now use newest-inserted ordering
  when multiple runs share the same SQLite timestamp second, avoiding stale
  completion output and same-second resume ambiguity (#308).
- Setup edit mode now raw-loads configs with templated non-string scalars
  such as `${env:DB_PORT}` while still preserving placeholders in string
  fields for safe round-tripping (#283).
- Setup edit mode now distinguishes omitted `create_indexes` and
  `create_foreign_keys` from explicit `false`, preserving the documented
  default when raw-loading and round-tripping existing configs (#282).
- SQL Server bulk-copy writes now convert UTF-8 text values scanned as
  `[]byte` into strings while preserving binary bytes, avoiding odd-byte
  Unicode failures in MySQL-to-SQL Server transfers (#303).
- SQLite incremental sync now accepts explicitly configured `TEXT` date
  columns so ISO-8601 timestamp strings do not silently fall back to full
  sync (#311).
- Upsert count-only validation now permits target row counts greater than the
  source count, so source-side deletes retained on the target do not fail daily
  incremental runs; target counts below source still fail (#310).
- Incremental date filters now use strict `>` watermark comparisons across
  dialects so unchanged rows equal to the prior sync timestamp are not replayed
  on the next daily-driver upsert run (#304).
- Daily-driver watermarks now persist the source table's high timestamp instead
  of the app sync-start clock, avoiding source/app clock skew while preserving
  strict incremental comparisons (#304).
- Diagnosis boxes now truncate long non-ASCII messages on UTF-8 boundaries
  instead of slicing through multibyte runes (#238).
- Runtime Slack notifications now honor the effective per-config
  `slack` settings instead of rereading only global secrets (#284).
- `dmt analyze --apply` now writes `migration.max_memory_mb` alongside
  the other analyzed tuning fields so applied config files preserve the
  full memory-budget recommendation (#246).

### Added

- **Nightly cross-engine integration matrix in CI** (closes #291).
  New `.github/workflows/integration-nightly.yml` runs 12 cross-engine
  directed pairs once a night (`cron: 0 5 * * *`) and on manual
  `workflow_dispatch`. Pairs cover every {mssql, postgres, mysql, sqlite}
  combination and exclude only the four same-engine round-trips (which
  don't exercise the cross-DB type-mapping surface). Matrix runs with
  `fail-fast: false` and `max-parallel: 12` so one driver's failure
  doesn't mask the rest, and the whole matrix wraps in roughly the
  wall-clock of a single pair. New generic runner
  `scripts/integration-test-pair.sh --pair <src>-<tgt>` dispatches
  source-side fixture loading and target-side DB prep per engine,
  then validates via `dmt run` + `dmt validate` (trusts dmt's own
  validate logic as the canonical row-count check rather than
  re-encoding case sanitization per target). Per-pair configs live
  under `scripts/fixtures/ci-{src}-{tgt}.yaml`. Local repro:
  `make integration-test-pair PAIR=mssql-pg`.

- **SO2010-minimal posts.body text aligned across all four engine
  fixtures** (part of #291). Previously the mssql, pg, sqlite, and
  mysql fixtures each used engine-specific phrasing for the row-1
  `posts.body` value (`NVARCHAR(MAX) path`, `TEXT path`, `LONGTEXT
  path`). The per-engine type-mapping context lives in the file
  header comments where it belongs; the seeded data itself is now
  engine-neutral (`testing the wide-text path`) so cross-engine
  row-parity assertions in the upcoming integration matrix won't
  fail on documentary text alone. The sqlite fixture drift predated
  this PR but is fixed here to keep the four files in lockstep.

- **SO2010-minimal PG + MySQL source-side fixtures** (part of #291).
  New `scripts/fixtures/so2010-minimal-pg.sql` and
  `scripts/fixtures/so2010-minimal-mysql.sql` mirror the existing MSSQL
  fixture's 9 tables, 61 columns, and 47 seed rows — translated to
  each engine's dialect (PG: `TIMESTAMP`/`TEXT`/`VARCHAR`; MySQL:
  `DATETIME`/`LONGTEXT`/`VARCHAR` on InnoDB+utf8mb4). Same canonical
  lookup-table values (including the canonical `TagWikiExerpt` typo)
  so cross-engine round-trip tests can assert byte-identical row
  parity regardless of which engine is the source. `scripts/load-fixture-so2010-minimal.sh`
  gained a `--source mssql|pg|mysql` flag (default `mssql` preserves
  the original signature) and discovers a running container for each
  engine (`pg-test`/`pg-bench` for PG, `mysql-bench` for MySQL).
  Three new Make targets — `load-fixture-so2010-minimal-{mssql,pg,mysql}`
  — make the choice explicit at the command line; the bare
  `load-fixture-so2010-minimal` target stays as an alias for the
  mssql case. This is the fixture half of #291; the CI matrix wiring
  that actually exercises these is the follow-up PR.

- **SQLite → SQLite integration test in CI.** New `sqlite-to-sqlite`
  job in `.github/workflows/integration.yml` runs an end-to-end dmt
  migration between two on-disk SQLite databases on every PR. No
  service containers, no client tools beyond the preinstalled
  `sqlite3` CLI, and finishes in seconds — the fastest signal in the
  integration matrix. Fixture (`scripts/fixtures/so2010-minimal-sqlite.sql`)
  mirrors the existing MSSQL SO2010-minimal fixture schema and seed
  rows so the two CI jobs cover the same surface area from different
  driver angles. Includes value spot-checks (UTF-8 round-trip, NULL
  preservation, negative integer PK, canonical lookup-table contents)
  beyond the row-count parity check. Local repro: `make integration-test-sqlite`.

- **SQLite driver for test-only migrations** (#298). New driver under
  `internal/driver/sqlite/` uses the pure-Go `modernc.org/sqlite`
  (already a transitive dependency of the checkpoint store) so dmt can
  be exercised end-to-end without an external database server. SQLite
  works as both source and target. The deterministic typemap learned a
  fourth dialect via `internal/typemap/sqlite.go` plus targeted
  updates in `internal/typemap/ddl/`, so cross-engine type mapping
  (`sqlite ↔ {mssql, postgres, mysql}`) goes through the same path
  the production drivers use. Single-writer pool, single-partition
  reads, `INSERT OR IGNORE` for idempotent replay, and
  `INSERT … ON CONFLICT … DO UPDATE SET … = excluded.*` for upserts.
  `INTEGER PRIMARY KEY AUTOINCREMENT` is emitted only for sole
  integer-PK columns; composite PKs preserve `NOT NULL` on identity
  columns (SQLite, unlike PG/MSSQL/MySQL, does not implicitly
  NOT NULL table-level PK columns). FK and CHECK constraints can
  only be declared inline at CREATE TABLE on SQLite, so the post-
  load Create FK / Create Check phases log a warning and skip — use
  sqlite as source rather than target when FK enforcement matters.

- **TUI `/explore` control** (#182). Adds a slash command to the
  interactive TUI for the tuner's exploration policy. `/explore on`
  arms a one-shot probe consumed by the next `/run` (sets
  `cfg.Migration.Explore=true` for that run only). `/explore
  low|balanced|high` sets the steady-state ε strength for the
  session, mirroring `cfg.Migration.ExploreMode`. `/explore off`
  clears both. Bare `/explore` reports current state. Empty mode
  leaves the loaded config / secrets value alone — the TUI overrides
  only when the operator explicitly sets a mode this session.
  Completes the deferred TUI surface from #179 (PR2).

- **Type cache: partition by source + `cache clear --ai-only`** (#177).
  The on-disk `~/.dmt/type-cache.json` format now records per-entry
  provenance (`source`, `model`, `cached_at`) wrapped in a versioned
  envelope (`{"version": 2, "mappings": {...}}`). Read path is
  backward-compatible: pre-#177 flat-map files are sniffed and
  migrated as `source: "ai"` on the next load (defensive — the old
  code only ever wrote AI mappings). AI write sites (column-level,
  table DDL, drop DDL) record the current effective model so
  `dmt cache clear --ai-only` can invalidate after a model upgrade
  without disturbing other entries. Entries tagged `source:
  "deterministic"` are bypassed on read as defense-in-depth — the
  deterministic mapper is the source of truth for those mappings. New
  CLI: `dmt cache clear` (full wipe) and `dmt cache clear --ai-only`;
  the file-level helper operates directly on the JSON so the CLI
  doesn't need an AI provider configured to invalidate.

### Changed

- **Deterministic tuning searches reader settings and composes epsilon probes** (#294, #295).
  Regression selection now has explicit coverage proving the argmax path
  searches learned `parallel_readers` / `read_ahead_buffers` cells rather
  than carrying baseline reader settings through unchanged. Steady-state
  epsilon exploration can also apply a second nudge on a different knob
  with conditional probability `epsilon`, making multi-knob neighbors
  reachable after cold-start. Composite perturbation reasoning now uses
  an unambiguous comma-separated direction list.

- **Config validation: skip `host` requirement for file-based drivers.**
  `config.validate()` previously required `source.host` and
  `target.host` unconditionally, which rejected valid sqlite configs
  whose connection identity is a file path on `database`. Added an
  `isFileBasedDriver` helper that branches on canonical driver name
  (currently just `sqlite`) so file-driver configs validate cleanly.
  Caught by the new sqlite → sqlite integration test on first local
  run — the network-driver assumption was load-bearing for the
  3-driver world but invalid once a file driver landed.

- **Drop `R²=` from regression-tier reasoning line** (#293). The
  operator-facing log line emitted from
  `internal/tuning/history.go::applyHistoryRegression` no longer
  includes the model's training-set R². On noisy real workloads R²
  is structurally capped low — within-cell variance dominates
  between-cell signal — so the line read `R²=0.13` while the
  regression's actual decisions were near-optimal (1.4% top-1
  regret on the SO2010 → PG sweep that motivated this change, with
  Spearman ρ = +0.68 and 95% CI coverage at 88%). Operators reading
  the log thought the tuner was broken when it wasn't. The 95%
  prediction interval still in the same line carries the relevant
  point-level confidence in units operators understand (MB/s). The
  `model.r2` field is still computed and remains available to debug
  logs and the existing `regression_test.go` assertions. A negative-
  guard assertion in `TestApplyHistory_RegressionTier` catches
  accidental re-introduction.

- **Studentized-residual outlier filter** (#225). Replaces the
  feature-blind `0.5×median` row-level outlier rule in
  `internal/tuning/history.go` with a leverage-adjusted
  studentized-residual filter that scores each row against the
  regression's prediction at its own features. The marginal floor kept
  a host-throttled 575K rows/s run at a config the model predicted to
  be 1.06M (because 575K > 0.5×median = 500K), which dragged R² from
  0.20 down to 0.09 across the next two runs. The residual filter
  reuses the `σ̂²` and `(XᵀX + λI)⁻¹` cached from #216 — no second
  matrix solve. Drops rows with `|t| > 3` AND `ChunkRetryCount == 0`,
  cap at 10% of rows per pass ordered by `|t|` descending. Gated to
  ≥ 2 × `minRowsForRegression` (60 rows) — below that, the marginal
  filter remains as the safety net. Reasoning emission is deferred so
  that when Tune's Tier 1 (identity) cohort drives the selection, only
  the identity cohort's drops appear in `Output.Reasoning` — the
  regime cohort's filter pass is silent (avoids double-reporting
  when the identity cohort is a subset of the regime cohort).

- **Packet cap and memory budget scoped to filtered tables** (#241).
  When `applyAITuning` runs, the orchestrator's
  include/exclude-filtered table set is now passed into the
  smartconfig analyzer via a new `SetTableNameFilter`. Every
  workload-wide derivation — `@@max_allowed_packet`-derived chunk
  cap, avg/max row size, `TotalRows`, `TotalTables`, `LargestTables`,
  `EstimatedMemMB`, the deterministic tuner's `HardChunkLimit` —
  now reflects only the tables that will actually be transferred.
  Previously an excluded wide table (e.g. an archive blob at 16 KB
  rows) still drove the packet cap and clamped chunk_size for the
  narrow tables that DO ship. The `analyze` CLI subcommand keeps its
  pre-#241 unscoped behavior (no filter context to apply).

- **Physical-regime buckets for ClassifyRegime** (#214). Replaces the
  per-field ratio gates (3× total_rows, 2× avg_row_bytes) the
  workload-similarity check used to drop historical rows from the
  regression's training set. Three-axis bucket gating now applies:
  total-bytes band (Tiny < 100 MB / Small < 10 GB / Medium < 100 GB
  / Large < 1 TB / Huge), then largest-table-share skew tier, then
  total-table-count tier. Each axis only triggers when the higher
  axis matches and either side has a defined value. Fixes the two
  failure modes the ratios produced: the boundary discontinuity
  (100M and 305M rows artificially partitioned even though they're
  on the same physical cliff) and the asymmetry (1M→3M and 30M→90M
  have the same ratio but very different physics).
  `LargestTableBytes` and `TotalTables` are added to the
  `tuning.HistoryRecord` IR for the new axes; persistence of
  `LargestTableBytes` to `ai_tuning_history` is deferred to a
  follow-up schema migration. Historical rows that don't carry the
  field fall through as `skewUnknown` — neutral on the secondary
  axis until they get backfilled.

- **Regression predicts bytes/sec, not rows/sec** (#224). The
  deterministic tuner's quadratic regression in
  `internal/tuning/regression.go` now trains on
  `HistoryRecord.FinalThroughputBytes` (rows/sec × avg_row_bytes,
  populated by the smartconfig adapter with the existing
  `safeAvgRowBytes` fallback). Aligns the dependent variable with
  the chunk_size_bytes input feature, drops the dual role
  `log(avg_row_bytes)` was playing as an implicit unit conversion,
  and should measurably improve cross-workload R² in Tier 2.
  Within-workload Tier 1 R² is unchanged (linear y rescaling). The
  reasoning log now reads "predicted 537 MB/s [95% CI: 412 MB/s–663
  MB/s]" via a new `formatBytesPerSec` helper that picks GB/s for
  ≥1 GB/s rates. Other readers of `FinalThroughput` (filterOutliers,
  smoothed-bins aggregator, regime_drift, selectWAW) stay in
  rows/sec — they're within-workload comparators where rows/sec is
  the natural unit.

### Added

- **AI fallback observability** (#176). Every AI-fallback call site
  (column- and table-level type mapping, finalization DDL, errordiag
  catalog miss) now flows through `observability.RecordFallback`,
  which fans an event out to three surfaces in lockstep: the existing
  `dmt_ai_fallback_total{surface=...}` Prometheus counter (#229), an
  in-process counter for the running migration, and a new per-run
  `fallback_events` row on both the SQLite and YAML/FileState
  checkpoint backends. Persisting to the backend means a separate-
  process `dmt status` poll (Airflow's documented workflow) sees the
  running migration's counts. UPSERT keys are
  `(run_id, surface, fingerprint)` so a 10K-Raw-column migration
  produces O(distinct types) rows, not O(occurrences). Errordiag
  fingerprints are normalized to `driver|scrubbed_prefix` (the
  SHA-256 hash stays in the debug log) so a bulk load that fails
  on row-specific error details doesn't explode the table. The
  detailed-status view sorts fingerprints by count and caps the
  inline display at 10 with an "and N more" suffix.
  `CleanupOldRuns` purges the new table alongside the rest of the
  run-scoped state.

- **Setup wizard prompts for Slack webhook URL** (#281). New Phase 1b
  step (`StepSlackWebhook`) interposes between the AI prompts and the
  Phase 2 source database configuration. Runs unconditionally — the
  webhook is independent of AI, so users who skip AI still see the
  prompt. Edit mode pre-populates from the existing
  `~/.secrets/dmt-config.yaml` so Enter preserves the current value;
  blank input on a fresh setup skips writing entirely; `-` or `none`
  explicitly clears the stored webhook. The webhook lives only in
  the global secrets file (matches existing CLAUDE.md guidance: AI
  and Slack are global-only), not in the per-migration config.

- **`secrets.SaveSlackWebhook`** (#281). New helper that sets the
  Slack webhook URL with explicit-clear semantics (empty string
  writes through), unlike `secrets.Save` whose merge logic skips
  zero values. Preserves all other sections of the secrets file
  (AI, Encryption, MigrationDefaults).

- **Setup wizard loads existing config and offers Edit-vs-New** (#279).
  `/setup` (TUI) and `dmt setup -o existing.yaml` (CLI) used to always
  start from a blank slate even when the underlying state machine
  already wired existing values into every prompt's Default. Now both
  entry points load the target config if it exists, route through a
  new `StepEditOrNew` preflight, and seed each prompt with the
  user's prior values so Enter preserves them. New configs
  (`/setup @newfile.yaml`) still start fresh and save to the
  requested path.

- **Wizard prompts for `date_updated_columns` when target_mode=upsert**
  (#279). Without a watermark column, upsert silently degraded to
  full scans on every re-run. The new `StepDateColumns` prompt only
  appears in upsert mode and accepts a comma-separated list (Enter
  keeps the existing list, `-` clears it). drop_recreate bypasses
  this step.

- **`config.LoadRaw` and `config.Expand`** (#279). `LoadRaw` reads a
  config YAML without secret-template expansion / defaults / validation,
  so `${env:DB_PASSWORD}` placeholders survive a wizard round-trip and
  are never written back to disk in cleartext. `Expand` is a public
  single-string template expander used by the wizard's per-side
  connection tests, which now resolve only the side being tested so
  an unrelated missing template on the other side never poisons the
  test (issue noted by codex review pass 3).

### Deprecated

- **`migration.ai_adjust` → `migration.runtime_tuning`** (#211). The
  knob controls a deterministic rule-based controller (post-#172);
  the old name implied AI involvement that no longer exists, which
  was recurring user-facing confusion. Companion field
  `ai_adjust_interval` is renamed to `runtime_tuning_interval`. Both
  names are accepted during the v5-to-v6 deprecation window per the
  policy in [VERSIONING.md](VERSIONING.md): a config that still uses
  `ai_adjust` continues to work and emits a WARN log per migration
  pointing at the new name. When both names are present with
  conflicting values, the new field wins and a clarifying WARN names
  both. The legacy fields will be removed in v6.0.0. The same rename
  applies to `migration_defaults` in `~/.secrets/dmt-config.yaml`.
  The stored resume config hash uses the legacy JSON wire shape (via
  JSON tags), so an in-flight migration started before the upgrade
  resumes cleanly without `--force-resume`.

### Fixed

- **Analyze tuning no longer pollutes global secrets or training history**
  (#246, #247). `dmt analyze --apply` now writes workload-specific
  tuning values into the analyzed config file's `migration:` section
  instead of `~/.secrets/dmt-config.yaml`, preserving unrelated
  config sections and file permissions. Analyze mode is also advisory
  only: it can read prior `ai_tuning_history` rows for recommendations,
  but only completed migration runs persist new training rows.

- **TUI viewport now auto-scrolls to follow new output** (#280). The
  first `WindowSizeMsg` wrote the welcome message into the viewport
  but never called `GotoBottom`, so `YOffset` stayed at 0 with the
  welcome overflowing. From that moment `viewport.AtBottom()` returned
  false for the whole session, and `appendOutput`'s auto-follow check
  skipped `GotoBottom` on every new line — users had to manually
  scroll to see migration progress, wizard prompts, etc. Init path
  now anchors at the bottom so subsequent appends follow. Resize path
  preserves at-bottom-ness across dimension changes. As a bonus
  hardening, viewport width/height are clamped to a minimum of 1 so a
  tiny terminal (fewer rows than the 7-row footer) doesn't panic
  inside bubbles' `visibleLines()` with `slice bounds out of range`.

- **Setup wizard honored loaded values for source/target fields but
  not Phase 6 / TargetMode** (#279). `StepTargetMode` hardcoded
  `drop_recreate` even for configs that set `target_mode: upsert`,
  and `StepCreateIndexes`/`CreateForeignKeys`/`Workers` ignored the
  loaded values entirely. In EditMode the wizard now honors the
  loaded `target_mode`, preserves explicit-false bools, and leaves
  `Workers=0` (auto-tune) intact when the loaded YAML omitted the
  key. Two narrower follow-ups tracked in the LoadRaw godoc:
  `create_indexes`/`create_foreign_keys` should become `*bool` to
  match the AIAdjust precedent, and templated non-string scalars
  (e.g. `port: ${env:DB_PORT}`) still fail `LoadRaw` due to typed
  unmarshal.

## [5.0.0] - 2026-05-13

Production-readiness milestone: closes the [production-readiness
epic #236](https://github.com/johndauphine/dmt/issues/236) end-to-end.
All ten gates green — see `VERSIONING.md` for the gate list and
the note on why this lands as `v5.0.0` rather than re-numbering
to `v1.0.0`.

### Added

- **Per-run immutable audit log** (#235). dmt now writes an append-only
  NDJSON record of every `dmt run` / `dmt resume` to
  `$HOME/.dmt/audit/<run_id>.ndjson` (override with `--audit-dir`).
  Compliance-regime-friendly: each line is one event, the file is
  `chmod 0444` after a successful or hard-failed run (cancelled /
  resumable runs keep the file 0600 so `dmt resume` can append to
  it), and `--audit-tamper-evident` opts
  into hash-chained events (each event carries `seq` / `prev_hash` /
  `hash` so retroactive modification is detectable via a one-liner
  shell verification). Sensitive values flow through the same scrubber
  established by #231 — DSN passwords, API keys, webhook URLs, and
  any field whose key name matches `password` / `api_key` / `token` /
  etc. are redacted before write. Row content is never logged by
  design. Disable entirely with `--no-audit` for environments with
  another compliance mechanism. New `docs/AUDIT-LOG.md` documents the
  event schema, the hash-chain verification procedure, and retention
  recommendations per compliance regime (SOC 2 / HIPAA / PCI-DSS / SOX).

- **Preflight health checks** (#228). New `TaskPreFlight` phase 0
  runs BEFORE schema extraction or DDL: connection ping, supported
  DB version (PG 12+, MSSQL 2016+, MySQL 5.7+/MariaDB 10.3+),
  encoding/collation, pool headroom (`max_connections - current_connections
  ≥ workers + 5`), per-driver privilege probes via information-schema
  introspection, and a backup-acknowledgment guard that requires
  `--confirm-backup` when `drop_recreate` mode runs against a
  non-empty target schema. Misconfigured environments now fail in
  ~0.5s with an actionable remedy instead of dying minutes into a
  partial run. Unified into a single `dmt preflight` subcommand
  (with `health-check` kept as an alias for existing Airflow/k8s
  probes). Opt-out per check via `--skip-preflight=name1,name2`
  or `--skip-preflight=all`.

- **Observability surface** (#229). Three coordinated surfaces, all
  off by default, all sharing the same dimension names (`run_id`,
  `phase`, `table`, `source_db`, `target_db`):
  - `--log-format=json` now emits structured fields on every line
    via slog-style base attributes. Existing printf-style log calls
    work unchanged; new `logging.Event` API for hot-path structured
    calls.
  - `--metrics-addr=:9090` binds a Prometheus `/metrics` endpoint
    with 11 metrics (`rows_total`, `bytes_total`, `errors_total`,
    `retries_total`, `chunk_duration_seconds`, `phase_duration_seconds`,
    `writer_queue_depth`, `writers_active`, `runtime_tuning_adjustments_total`,
    `ai_fallback_total`, `migration_info`). Per-run-labeled gauges
    cleared on RunComplete to bound cardinality.
  - `--otel-endpoint=URL` exports OTLP HTTP traces — one root span
    per run, child spans per orchestrator phase. Chunk milestones
    emit as span events on the phase span rather than separate spans
    (avoids flooding tracing backends on 100M-row migrations).
  - New `docs/OBSERVABILITY.md` + Grafana dashboard JSON.

- **CI gating on every PR** (#230). `.github/workflows/ci.yml` runs
  `go build`, `go vet`, unit tests, `go test -race`, golangci-lint
  v2.12.2 (only-new-issues — pre-existing lint debt isn't a PR
  blocker), and govulncheck v1.1.4. `.github/workflows/integration.yml`
  spins up MSSQL 2022 + Postgres 16 service containers, loads the
  SO2010-minimum fixture, runs a real `dmt mssql → postgres` migration,
  and asserts row-count parity. dmt logs uploaded as artifacts on
  failure. Local reproduction documented in `CONTRIBUTING.md`
  (`make integration-test` runs the exact same script CI does, with
  isolated state at `./.dmt-ci-state/` so the operator's
  `~/.dmt/migrate.db` is never touched). govulncheck is currently
  informational (`continue-on-error: true`) until stdlib `net`
  findings have a released Go toolchain fix.

- **Release policy + process** (#233). New `VERSIONING.md` documents
  SemVer triggers (MAJOR/MINOR/PATCH), stability commitments
  (stable vs experimental fields), and the deprecation cycle for
  breaking changes. New `RELEASE.md` documents how to cut a release,
  release cadence, the hotfix path, and pre-release/RC tagging.
  Goes binding at v1.0.0; best-effort before.

- **Sensitive-value scrubbing audit** (#231). New
  `internal/logging/scrub.go` exposes `Scrub(s)` /
  `ScrubError(err)` with a centralized regex set covering
  URL-style and libpq-style DSN passwords (PG, MSSQL, MySQL
  shapes), `password=`/`passwd=`/`pwd=`/`api_key=`/`secret=`/
  `token=` key/value forms (with `:` and `=` separators both
  preserved), `Authorization: Bearer` headers, `sk-` and
  `sk-ant-` API keys, and Slack incoming-webhook URLs. Threaded
  through every site that wraps a driver-library error: the
  PG / MSSQL / MySQL Reader and Writer constructors plus their
  Ping calls, the setup wizard's `TestConnection`, the
  orchestrator's `dmt preflight` JSON output, and the Slack
  notifier's HTTP error path. New `docs/SECURITY.md`
  documents the threat model, what's scrubbed, what's
  never logged (row content), and how an operator verifies.

### Breaking Changes

- **Partial migrations exit non-zero by default** (#248). When one or
  more tables fail to transfer, `dmt run` and `dmt resume` now exit
  with code 3 (`TransferError`) instead of 0. The run is still
  recorded in state as `partial` and the JSON result still includes
  the per-table breakdown, so consumers that inspect `result.status`
  and `result.failed_tables` see the same information they did before
  — only the exit code changed. Set `migration.allow_partial: true`
  in your config to restore the pre-#248 behavior (exit 0 on
  partial). This closes a silent automation hazard where Airflow/k8s
  jobs treated partial migrations as successful.

- **MySQL TLS defaults to require+verify instead of preferred** (#252).
  When `ssl_mode` is unset (or set to an unrecognized value), the
  MySQL DSN now uses `tls=true` (TLS required, CA + hostname
  verification) instead of the pre-#252 `tls=preferred` (TLS
  attempted, silent plaintext fallback). The MySQL driver's default
  and the setup wizard's default both moved from `"preferred"` to
  `"require"`. Operators who still want downgradeable TLS can
  explicitly set `ssl_mode: preferred` — that branch maps to
  `tls=preferred` as before, but it's now an explicit opt-in, not a
  silent default. Operators connecting to non-TLS MySQL instances
  (e.g. local Docker test containers) must set `ssl_mode: disable`.
  Additionally, `verify-ca` no longer maps to `skip-verify` (no
  verification — the direct inverse of the operator's intent); it
  now maps to `tls=true` (verify CA + hostname), which is strictly
  safer.

- **Validation no longer reports false-positive success on missing
  evidence** (#253). Three policy gaps closed:

  1. Row-count validation **timeouts** were warnings, not failures.
     A `COUNT(*)` that exceeded `ValidationTimeout` (30s default)
     was reported as a warning and the run could still finish
     "successful." Now timeouts fail the run by default; opt out
     via `migration.validation.fail_on_timeout: false`.
  2. **Estimated-count mismatches** were warnings, not failures.
     When the exact `COUNT(*)` timed out on one or both sides dmt
     fell back to estimated counts; if those disagreed, the
     discrepancy logged a warning. Now mismatches under the
     estimated-counts fallback fail the run by default; opt out
     via `migration.validation.fail_on_estimate_mismatch: false`.
  3. **MSSQL exact-count ignored `strict_consistency`**. The
     `GetRowCountExact` query always included `WITH (NOLOCK)`,
     silently overriding any operator who explicitly asked for
     read-committed counts. `strict_consistency: true` now drops
     the NOLOCK hint.

  Combined with #248, this closes both layers of silent acceptance
  between a broken migration and a green status.

### Removed

- **Kerberos auth descoped pending a verifiable test environment**
  (#251). The README and example configs advertised Kerberos /
  SPNEGO support for SQL Server and PostgreSQL, but the runtime
  drivers go through `Dialect.BuildDSN(..., cfg.DSNOptions())` and
  `DSNOptions()` doesn't carry `auth`/`keytab`/`realm`/`SPN`, so an
  `auth: kerberos` config silently fell back to password auth.
  `examples/config-mssql-to-pg-kerberos.yaml` and
  `examples/config-pg-to-mssql-kerberos.yaml` are removed, the
  README no longer claims Kerberos as a supported auth method, and
  config-load now rejects `auth: kerberos` with an error pointing at
  #251. The DSN-builder code path that would emit a correct
  Kerberos DSN is left in place (with its tests) for the eventual
  re-enable.

### Fixed

- **ROW_NUMBER pagination resume safety** (#227, #266, #267). Four
  layered correctness bugs that previously caused silent data loss
  or duplication on resume of composite/varchar-PK tables, all
  closed:
  1. Resume preflight was partition-blind: large ROW_NUMBER tables
     have partition-level checkpoints, but the truncation decision
     checked only the table-level checkpoint and used
     `SupportsKeysetPagination()` (which excludes ROW_NUMBER) to
     gate partition-awareness. Fixed: use `HasPK()` to match the
     actual partitioning decision in `job_builder.go`, and clear
     partition-level checkpoints whenever a target is truncated.
  2. Per-table checkpoints lag acked rows by up to
     `checkpoint_freq - 1` chunks. On crash + resume those chunks
     would replay, and a plain INSERT would crash on duplicate PK.
     Fixed: when the orchestrator dispatches a job as part of a
     Resume() call, the writer routes through driver-specific
     idempotent paths (PG: temp staging + COPY + `INSERT...SELECT
     ON CONFLICT DO NOTHING`; MySQL: `ON DUPLICATE KEY UPDATE
     pk_col = pk_col` — NOT `INSERT IGNORE`, which masks
     data-conversion errors; MSSQL: per-partition staging + insert-only
     `MERGE`). Replayed rows become silent no-ops; non-resume
     `drop_recreate` runs are unchanged.
  3. Stale-checkpoint clears in resume preflight were silently
     swallowing errors, so a failed clear could leave the system in
     the exact pre-#227 state. Fixed: clears are now fatal — the
     run aborts with `ConfigError` if either `ClearTransferProgress`
     or `ClearPartitionTransferProgress` fails.
  4. Resume preflight now also verifies that partition progress
     records are consistent with the partition task graph (#267) —
     a stale partition_id whose task no longer exists in the run
     surfaces as a resume-time error rather than silent skip.

- **Date-based incremental sync now works on the file backend**
  (#255). Pre-#255 the file backend's `GetLastSyncTimestamp` and
  `UpdateSyncTimestamp` were explicit no-ops, so date-based
  incremental sync configured against the Airflow/k8s-recommended
  file backend silently degraded to a full-table copy every run.
  The cost scaled linearly with table size instead of delta size —
  the inverse of what incremental sync is supposed to deliver.
  Timestamps now persist in a `sync_timestamps:` map in the YAML
  state file, keyed by (source schema, table, target schema) to
  match the SQLite backend's UNIQUE constraint. Writes go through
  the crash-safe `atomicWriteFile` path established in #254.

- **File-state writes are now crash-safe** (#254). The YAML file
  backend used by the Airflow/k8s headless mode previously wrote via
  `os.WriteFile`, which is not atomic: a SIGKILL, OOM-kill, or pod
  eviction partway through the write would leave a truncated YAML
  file that fails to parse on resume — exactly the failure mode the
  file backend was added to handle. `FileState.save()` now uses the
  standard tmp + fsync + rename + dir-fsync pattern, so the state
  file is always either the pre-write or post-write version, never
  torn. Also `MkdirAll`s missing parent directories with 0700 perms.

- **Reader goroutines no longer leak when writers fail mid-transfer**
  (#250). In both the keyset and ROW_NUMBER pagination paths, reader
  goroutines used bare `chunkChan <- result` sends. On writer
  failure the consumer would break out of its loop, but blocked
  readers would never unblock — leaking the reader goroutines (and
  the close-channel goroutine waiting on them) and holding source
  DB cursors until the process exited. Readers now run under a
  per-transfer child context and send via a select that also
  watches `ctx.Done()`, and the consumer cancels that context after
  it stops draining. Aborts in-flight source-side queries via
  `QueryContext` as well.

## [4.0.0] - 2026-05-12

Major release: the **AI-optional architecture epic (#167)** ships
end-to-end. dmt no longer requires an AI provider for any migration
path; deterministic catalogs cover type mapping, error diagnosis, DB
tuning, runtime parameter adjustment, and smart-config selection. AI
remains available as an opt-in enhancement for vendor-specific edge
cases (e.g. Oracle `hierarchyid`, MSSQL `geography`).

### Breaking Changes

- **Setup defaults flipped**: `dmt init-secrets` no longer seeds an
  AI provider section; `dmt setup` wizard's AI prompt defaults to
  "skip" rather than "configure". Existing users with AI configured
  in `~/.secrets/dmt-config.yaml` continue to work; the change is
  visible only on fresh installs. (#174)
- **Driver interface additions**: anyone implementing a custom
  `driver.Driver` must now provide `HardChunkLimit(avgRowBytes int64) int`
  and `ProbeTarget(ctx, db) TargetProbe`. Built-in drivers updated.
  (#166)
- **Removed exported types**:
  - `driver.AIErrorDiagnoser`, `driver.GetAIErrorDiagnoser`,
    `driver.NewAIErrorDiagnoser` (replaced by deterministic
    `errordiag` package + dispatch helpers). (#173)
  - `dbtuning.AIQuerier`, `dbtuning.AITuningAnalyzer`,
    `dbtuning.NewAITuningAnalyzer`. (#172)
- **`dbtuning.Analyze` signature**: removed the trailing `aiMapper interface{}`
  parameter. Callers that previously passed `nil` should drop the
  argument. (#172)

### Added

- **Deterministic error diagnosis catalog** with 76 regex-matched
  patterns across PG (26), MSSQL (25), MySQL (25). Replaces the
  AI-driven `ai_errordiag.go` (382 LOC removed). Catalog growth via
  the unmatched-error log signal. (#173)
- **Deterministic DB tuning catalog** with 30 settings across PG (11),
  MSSQL (9), MySQL (10). Replaces the AI-driven `ai_analyzer.go`
  (482 LOC removed). Each setting has a hardcoded SQL query plus a
  pure Go rule producing recommendations. (#172)
- **MySQL `@@max_allowed_packet` probe** drives the `chunk_size` hard
  cap so MySQL targets with default 4MB packet no longer crash on
  wide rows. Probe-derived cap threaded through to the runtime
  controller (`MaxChunkSize`/`MinChunkSize`) so growth rules can't
  exceed the packet limit mid-transfer. (#166)
- **Deep validation passes** layered after the existing row-count
  check: `validation.mode: null_parity` adds per-column NULL count
  parity; `validation.mode: sample` adds value-level row comparison
  via a deterministic MD5(pk)-ordered sample. Default mode unchanged
  (`count_only`); cross-DB canonicalizer normalizes types so source
  and target produce identical bytes for the same value. (#226)
- **CI-loadable fixture loaders**: `make load-fixture-pgbench` +
  `make load-fixture-so2010-minimal`. SO2010-minimal synthesizes
  byte-for-byte-compatible schema and seed for the public Brent Ozar
  dataset; `make test-fixtures-load` chains both in ~5s. Manual
  procedures for full SO2010/SO2013/WWI documented in
  `docs/FIXTURES.md`. (#178)
- **Tier 1 exact-identity workload classifier** for the deterministic
  tuner: matches historical runs by 8-tuple identity
  (source+target host/port/db/schema) before falling back to regime
  filtering. Improved R² on stable-workload runs. (#215)
- **Setup wizard `--with-ai` flag** + `secrets.GenerateTemplateWithAI()`
  for users who explicitly opt into AI features at template creation.
  Default `dmt init-secrets` produces an AI-free file. (#174)
- **Optional AI Enhancements** README section reframes AI as opt-in
  rather than required; Quick Start at the top of the README is
  explicit about no AI being needed. (#174)
- **Pattern of `codex review --base main` after every substantial PR**
  caught real bugs across the epic (~25 P1/P2 findings between Codex
  and Copilot — schema sanitization, integer-division-to-zero on
  packet caps, MySQL parameter syntax mismatches, UTF-8 rune-vs-byte
  handling, `time.Duration` YAML parsing, unbounded goroutine fan-out,
  more). See per-PR commit messages.

### Changed

- **Runtime parameter adjustment** is fully deterministic. The
  field `migration.ai_adjust` is preserved for config backward
  compatibility but no longer involves AI; it controls a rule-based
  controller (4 deterministic rules). A rename to `runtime_tuning`
  with deprecation cycle is tracked in #211.
- **Regression-tier tuner** learns `parallel_readers` and
  `read_ahead_buffers` in addition to `write_ahead_writers` and
  `chunk_size`. Argmax now skips uncovered cube-corner cells
  (refuses to extrapolate beyond training-data support). (#219, #221)
- **MySQL text/blob tiers** preserved via canonical `MaxBytes`,
  fixing nvarchar(max)/text/varbinary(max) → LONGTEXT/LONGBLOB. (#196, #206)

### Removed

- **AI-driven error diagnoser** (`internal/driver/ai_errordiag.go`).
  Error messages no longer egress to third-party LLMs, closing a
  PII-leak surface. (#173)
- **AI-driven DB tuning analyzer** (`internal/driver/dbtuning/ai_analyzer.go`).
  Server-configurable settings no longer require LLM round-trips. (#172)
- **AI smartconfig prompt machinery** (~1500 LOC across earlier PRs
  during the epic): replaced by the deterministic tuner in
  `internal/tuning/`.

### Production-Readiness Tracking

The production-readiness epic #236 was opened and made progress on:
- **#178** CI-loadable fixtures (this release)
- **#226** deep validation passes (this release; full row-hash mode
  reserved for a future iteration after the cost analysis concluded
  it didn't justify its bandwidth)

Remaining work: #227 (ROW_NUMBER resume safety, P0), #228 (preflight
health checks, P1), #229 (structured logs + Prometheus + OTLP, P1),
#230 (CI gating, P1, unblocked by this release's #178), and others.

### Closed-as-not-pursued

- **#244** — same-engine in-DB row hashing was filed during the
  #226 PR and closed after analysis: dmt's headline use case is
  cross-engine, where row-level proof is fundamentally Go-side;
  same-engine migrations have first-party tools (pg_dump, native
  backup-restore) that already provide higher-fidelity guarantees
  than a SQL row-hash sum.

## [3.0.0] - 2026-01-13

### Breaking Changes
- **Renamed environment variable** - `DATA_TRANSFER_TOOL_MASTER_KEY` → `DMT_MASTER_KEY` for profile encryption
- **Default Slack username** - Changed from `data-transfer-tool` to `dmt`

### Changed
- **Updated TUI branding** - New ASCII art logo and version display
- **Centralized version** - Version now managed in `internal/version` package
- **Updated all references** - Makefile, README, docs, and config examples now use `dmt`

### Added
- **Tests for notify package** - 37 test cases covering Slack notifications, error handling, and formatting
- **Tests for progress package** - 32 test cases covering tracker, JSON reporter, and throttling

### Maintenance
- Removed unused dependencies (`lib/pq`, `go-sqlmock`)
- Promoted `gopsutil/v3` to direct dependency

## [2.27.0] - 2026-01-12

### Changed
- **Pluggable driver architecture** - Consolidated Dialect into driver package, eliminated switch statements
- **Project renamed** - `mssql-pg-migrate` → `dmt`

## [2.26.0] - 2026-01-12

### Changed
- **AI-first type mapping** - AI determines all type mappings with aggressive caching
- Static mappings only used as fallback when AI fails

## [2.25.0] - 2026-01-12

### Added
- **Analyze command** - `dmt analyze` for database analysis and config suggestions
- Auto-tuned performance parameters based on system specs
- AI-suggested alternatives with reasoning

## [2.24.0] - 2026-01-12

### Added
- **AI-powered type mapping** - Support for Claude, OpenAI, and Gemini providers
- Intelligent inference for unknown database types
- Row sampling for better context
- Persistent caching to minimize API calls

## [2.23.0] - 2026-01-12

### Maintenance
- Removed legacy pool implementations (4,473 lines of dead code)
- Completed Phase 7 of pluggable database architecture

## [2.22.0] - 2026-01-12

### Changed
- **WriteAheadWriters tuning** - Moved to driver interface for full pluggability
- PostgreSQL scales writers with CPU cores (2-4)
- MSSQL fixed at 2 writers (TABLOCK serialization)

## [2.21.0] - 2026-01-12

### Changed
- **Driver defaults** - Use driver registry instead of hardcoded fallbacks
- Each driver returns its own defaults via `Defaults()` method

## [2.2.0] - 2026-01-12

### Fixed
- **PG→MSSQL geography upsert** - Use DROP/ADD COLUMN instead of ALTER COLUMN for geography types

## [2.1.0] - 2026-01-12

### Changed
- **Pluggable factory** - Factory calls driver methods directly, no switch statements
- **BuildRowNumberQuery fix** - Extract column aliases for outer SELECT in CTE

## [1.43.0] - 2026-01-11

### Security
- **DSN injection fix** - URL-encode credentials in connection strings
- **SQL injection fix** - Whitelist validation for SQLite table names

### Fixed
- Spatial column detection for same-engine migrations
- SRID preservation for PostGIS columns

## [1.42.0] - 2026-01-11

### Fixed
- Security vulnerabilities identified during code review

## [1.41.0] - 2026-01-11

### Fixed
- **PG→MSSQL geography staging** - Query staging table directly for spatial columns

## [1.40.0] - 2026-01-11

### Added
- **packet_size config** - TDS packet size for MSSQL (default: 32KB)

### Performance
- MSSQL→PG: +27% throughput
- MSSQL→MSSQL: +162% throughput

## [1.32.0] - 2026-01-11

### Fixed
- **PG→MSSQL geography upsert** - Convert WKT text via STGeomFromText in MERGE

## [1.31.0] - 2026-01-10

### Added
- **Incremental sync** - Date-based highwater marks for fast delta transfers
- `date_updated_columns` configuration option

## [1.21.0] - 2025-12-30

### Performance
- **Direct Bulk API for MSSQL Inserts** - 73% throughput improvement using `CreateBulkContext` directly (#20)
  - Bypasses `database/sql` prepared statement overhead by using `conn.Raw()` to access driver directly
  - Previous: ~15,500 rows/sec → Now: ~27,000 rows/sec
  - Explicit transaction wrapper for atomicity with proper `defer tx.Rollback()` pattern

## [1.20.0] - 2025-12-30

### Performance
- **Worker Cap at 12** - Capped max workers at 12 for optimal performance (#17)
- **Removed Row Size Adjustment** - Removed chunk_size reduction based on row size that was hurting throughput (#16)

## [1.19.0] - 2025-12-30

### Performance
- **Removed Memory Safety Loop** - Removed conservative memory reduction loop that was limiting throughput (#15)

## [1.18.0] - 2025-12-30

### Features
- **Auto-tune Parallel Readers and Writers** - Automatic tuning of `parallel_readers` and `write_ahead_writers` based on system resources (#14)

## [1.17.0] - 2025-12-30

### Features
- Auto-tuning improvements for migration settings

## [1.16.0] - 2025-12-24

### Features
- Performance improvements and bug fixes

## [1.15.0] - 2025-12-24

### Features
- Additional migration optimizations

## [1.14.0] - 2025-12-23

### Features
- Migration enhancements

## [1.13.0] - 2025-12-21

### Features
- Core migration functionality improvements

## [1.12.0] - 2025-12-21

### Features
- Initial stable release with bidirectional migration support
