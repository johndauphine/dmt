# Epic #705 handoff: native Windows result and auto-tune rollback

This is the execution handoff for the remaining rollback work in epic
[#705](https://github.com/johndauphine/dmt/issues/705). A native Windows
SQL Server-to-PostgreSQL directional comparison is complete. It supersedes the
earlier provisional instruction to keep PRs #729 and #730: the release decision
is to roll back those two commits while retaining the epic work already present
at the completed-epic baseline. Treat the live issue bodies as authoritative if
this file and GitHub ever disagree. Remove this file when the epic closes.

Two corrections are also required reading:

- [Mac diagnostic correction](https://github.com/johndauphine/dmt/issues/728#issuecomment-4963531641)
- [Epic status correction](https://github.com/johndauphine/dmt/issues/705#issuecomment-4963532869)

## Current state

- Completed-epic baseline:
  `53b521f019a6f20f25b507d3ad57aab44f389b1c`
- Final #728 fix/current comparison arm:
  `9fe2f670e70580aae4aff4e025e8364711d1b4b8`
- Cardinality-aware memory modeling merged in PR #729.
- Effective-candidate projection and persisted/executed tuple identity merged
  in PR #730.
- The Mac Docker campaign did not establish a reproducible binary regression.
  It exposed write-path/platform instability and is diagnostic only.
- A native Windows same-boot paired diagnostic subsequently classified the
  combined #729/#730 treatment as likely materially slower. All five measured
  pairs were adverse in both paired signals and passed the predeclared material
  regression screen.
- Definitive release recommendation: roll back PR #730 and PR #729, newest
  first. Do not roll back the rest of epic #705, disable auto-tuning, or add
  linker/layout padding.

Do not put numeric SQL Server benchmark outcomes in this repository, a GitHub
issue, a PR, or another public channel without the separate license/legal review
and any written Microsoft approval required by the SQL Server terms. Keep raw
results under the local benchmark root. Public status may report methodology,
gate pass/fail, and qualitative conclusions only.

## Benchmark protocol status

The sections from `Fixed user constraints and decisions` through
`Hard gates and analysis` are retained as reference for the original formal
#728 protocol and to explain why the pragmatic screen is not formal sign-off.
They are not the current next-agent workflow. Do not rerun the Windows setup,
Stage A, or Stage B before implementing the rollback unless the owner
explicitly reopens formal sign-off. The actionable instructions are in
`Native Windows benchmark outcome and rollback recommendation` and
`Next-agent order of work`.

## Fixed user constraints and decisions

The release-sign-off environment is one dedicated Windows x64 laptop with:

- one internal physical disk;
- native SQL Server 2022 Developer as the source;
- native PostgreSQL 16 as the durable target;
- native `windows/amd64` DMT binaries;
- both engines and DMT on the same host and disk; and
- local loopback connections, with only one benchmark observation running at a
  time.

There is no second machine or second disk. Do not substitute Docker, WSL, AWS,
Airflow, a RAM disk, or a networked database for this sign-off. Use the OEM
full-wattage charger, Windows Best performance mode, a stable full charge or
fixed vendor charge limit, and one repeatable cooling position. The owner values
stability and consistency as well as throughput.

This environment supports a paired causal claim for this one laptop. It does
not support an absolute cross-hardware, networked, cloud, or production-capacity
claim.

### Consequences of the shared disk

SQL Server reads, PostgreSQL data/WAL writes, DMT state/log writes, telemetry,
and Windows activity share one device. That contention is part of the measured
workload; it is not automatically an external invalidation and database-caused
`System` I/O must not be filtered out.

Control it by using identical paths and settings in both arms, counterbalancing
order, resetting the target identically, waiting for measured storage
quiescence, and continuously recording disk latency, queue depth, IOPS, and
throughput. Do not clear OS or database caches inside a block or replicate. If
Stage A cannot meet its stability gates, the result is inconclusive. It is not
evidence to change the auto-tune policy.

## First Windows session: inventory before design

Do not freeze resource limits, a manual tuple, or measured schedules until the
machine has been inventoried and a smoke run proves the telemetry. Record at
least:

- laptop model, CPU model/topology, logical and physical cores, and RAM;
- Windows edition/build, BIOS version, firmware, and last boot time;
- physical SSD model, firmware, interface, size, health, free space, and volume
  layout;
- active power scheme, OEM charger identity/wattage, battery/charge policy, and
  cooling position;
- Go version/toolchain settings, Git version, and repository SHAs;
- exact SQL Server and PostgreSQL builds, service identities, ports, and data,
  log, WAL, and temporary-file locations;
- SQL Server memory/database settings and PostgreSQL settings;
- DMT resolved memory budget and the proposed fixed-tuple estimate; and
- every benchmark binary's SHA-256.

Useful read-only PowerShell starting points are:

```powershell
Get-ComputerInfo
Get-CimInstance Win32_Processor
Get-CimInstance Win32_PhysicalMemory
Get-CimInstance Win32_BIOS
Get-PhysicalDisk | Format-List FriendlyName,MediaType,BusType,Size,FirmwareVersion,HealthStatus
Get-Volume
powercfg /getactivescheme
go version
go env
git --version
```

Save the normalized inventory under `C:\dmt-bench\inventory`; do not commit
machine-specific output or credentials.

## Build the two immutable arms

Use one pinned Go toolchain for both revisions. The module currently declares Go
1.25.0; record the exact installed `go version` and do not upgrade between
builds or observations. Separate detached worktrees avoid accidental source
mixing:

```powershell
$Root = "C:\dmt-bench"
$Repo = (git rev-parse --show-toplevel).Trim()
if ($LASTEXITCODE -ne 0) { throw "Run this block from the pulled DMT clone" }
New-Item -ItemType Directory -Force "$Root\src", "$Root\bin", "$Root\inventory" | Out-Null

git -C "$Repo" worktree add --detach "$Root\src\baseline" 53b521f019a6f20f25b507d3ad57aab44f389b1c
git -C "$Repo" worktree add --detach "$Root\src\final" 9fe2f670e70580aae4aff4e025e8364711d1b4b8

Push-Location "$Root\src\baseline"
go build -ldflags "-s -w -X github.com/johndauphine/dmt/internal/version.Version=53b521f019a6f20f25b507d3ad57aab44f389b1c" -o "$Root\bin\dmt-baseline.exe" ./cmd/migrate
Pop-Location

Push-Location "$Root\src\final"
go build -ldflags "-s -w -X github.com/johndauphine/dmt/internal/version.Version=9fe2f670e70580aae4aff4e025e8364711d1b4b8" -o "$Root\bin\dmt-final.exe" ./cmd/migrate
Pop-Location

Get-FileHash "$Root\bin\dmt-baseline.exe" -Algorithm SHA256
Get-FileHash "$Root\bin\dmt-final.exe" -Algorithm SHA256
```

Capture the console output, `git rev-parse HEAD`, `go version`, and hashes in an
artifact manifest. Do not rebuild an arm after measurement begins; if a rebuild
is unavoidable, discard the campaign and start with new hashes.

## Native database qualification

Before any measured observation:

1. Install and run native SQL Server 2022 Developer and native PostgreSQL 16 as
   Windows services. Connect through fixed loopback endpoints (normally
   `127.0.0.1:1433` and `127.0.0.1:5432`).
2. Restore the nine-table StackOverflow2010 SQL Server fixture. Independently
   verify exactly 19,310,703 rows, retain the per-table counts, and then keep the
   source read-only for the campaign.
3. Use a dedicated disposable PostgreSQL target database. Drop and recreate it
   before every observation with the same script and owner/encoding/locale.
4. Keep PostgreSQL durability enabled. At minimum record `fsync`,
   `synchronous_commit`, `full_page_writes`, `wal_level`, checkpoint settings,
   and WAL sizing. An `fsync=off` result cannot sign off durability or general
   stability.
5. Enable PostgreSQL I/O and WAL timing before smoke (`track_io_timing` and
   `track_wal_io_timing`) and verify the settings took effect.
6. After each target reset, issue the fixed pre-run PostgreSQL `CHECKPOINT`,
   confirm there are no leftover benchmark sessions, and wait for the frozen
   disk-latency/queue quiescence rule.
7. Freeze engine configuration, service startup behavior, autogrowth/WAL
   policy, database locations, and free-space minimum after smoke. Do not tune
   either engine between arms or observations.

Use environment variables or the DMT secrets file for credentials. Never add
passwords to the repository, configs committed to Git, logs, or manifests.

## Harness to establish on Windows

There is no repository-owned Windows benchmark/telemetry harness yet. Build the
machine-specific harness outside the clone under `C:\dmt-bench`; do not begin
measured runs by hand. The next agent should create and smoke-test these local
components:

- inventory/settings snapshot;
- PostgreSQL target drop/create, checkpoint, session, and quiescence checks;
- pre/post PostgreSQL `pg_stat_io`, `pg_stat_wal`, and PostgreSQL 16 checkpoint
  counter snapshots;
- pre/post SQL Server file-I/O, memory, and relevant wait snapshots;
- one-second host/process/disk/AC/thermal telemetry with cadence-gap detection;
- an observation runner that selects an immutable binary and config, records an
  external monotonic wall time and exit code, captures stdout/stderr, and always
  performs post-run evidence collection;
- one independently frozen source per-table/total count manifest, plus target
  per-table/total validation against that manifest after every observation;
- DMT SQLite/checkpoint inspection for the effective tuple, live pool limits,
  retry counts, tuning-history rows, and runtime adjustments;
- a manifest with immutable schedule ID, arm, block/replicate, observation
  order, binary/config hashes, state snapshot hashes, start/end times, and
  validity classification; and
- paired-log analysis implementing #728's block/replicate estimator and gates.

A workable private layout is:

```text
C:\dmt-bench\
  bin\
  config\
  inventory\
  logs\stage-a\... and logs\stage-b\...
  manifests\
  snapshots\
  src\baseline\ and src\final\
  state\stage-a\... and state\stage-b\...
  stats\
  telemetry\
```

Stage A uses an isolated DMT `data_dir` for every arm/block/observation. Stage B
uses a separate persistent history per arm and replicate, with verified
snapshots before each pair. Never let one arm read the other's state.

## Telemetry contract

Continuous one-second samples must span every observation and include:

- total and per-core CPU, processor frequency/performance state, memory
  available/committed, paging evidence, and system uptime;
- CPU, private/working bytes, thread count, and I/O for DMT, `sqlservr`, every
  PostgreSQL process, and relevant `System` activity;
- physical-disk read/write latency, current/average queue depth, bytes/sec,
  IOPS, busy time, errors, and free space;
- SQL Server file read/write counters and PostgreSQL data/WAL/checkpoint
  counters bracketing the run;
- AC/battery/power-mode state; and
- temperature plus a proven hardware-throttle or passive-limit signal.

Do not assume a generic WMI temperature value is valid. Prove the thermal
collector during sustained smoke, using an OEM tool or another collector if
necessary. Record sample timestamps and reject telemetry with gaps beyond the
frozen cadence tolerance.

Freeze the memory rules after smoke. The issue requires at minimum available
memory at or above 10% of physical RAM and committed bytes below 90%. Diagnose
paging only from correlated sustained page I/O, commit, available-memory, and
page-file-growth evidence, not an isolated hard fault or a merely allocated
page file.

Before measured work, exercise the real SQL Server-read/PostgreSQL-durable-write
path continuously for 15 minutes. This is environment conditioning, not a
measured observation.

## Configuration discipline

The complete non-policy configuration must be byte-identical between arms,
apart from `data_dir` and arm-specific artifact paths. Freeze target mode,
consistency, table filters, validation, index/constraint creation, timeouts,
and every database connection option as well as the performance knobs.

Stage A must explicitly pin all of these after inventory/smoke:

- `source.packet_size`, `source.chunk_size`, and `target.chunk_size`;
- `migration.workers`, `migration.chunk_size`,
  `migration.write_ahead_writers`, `migration.parallel_readers`, and
  `migration.read_ahead_buffers`;
- `migration.max_partitions` and `migration.large_table_threshold`;
- `migration.max_source_connections` and
  `migration.max_target_connections`;
- `migration.max_memory_mb`;
- `migration.checkpoint_frequency` and `migration.max_retries`; and
- any other knob found to differ in startup provenance or effective execution.

Stage A also sets:

```yaml
migration:
  tuning: manual
  runtime_tuning: false
```

`tuning` controls the pre-run auto-policy. `runtime_tuning` is a separate
mid-run deterministic controller. Verify Stage A's startup provenance,
effective/executed tuple, source/target batch sizes, and live pool limits in
every observation. There must be zero tuning-history rows and zero runtime
adjustments. A safety projection means the tuple was not truly held fixed;
select a lower tuple during smoke and restart qualification.

Stage B sets `migration.tuning: auto` and keeps
`migration.runtime_tuning: false`. The pre-run tuner must own the intended
axes; do not accidentally carry Stage A pins into the auto-policy configs.

## Stage A: fixed-tuple transfer-path control

Stage A answers whether the final binary changes the pipeline when both arms
execute the identical tuple. It runs before Stage B.

- Four separately rebooted and requalified blocks.
- Each block: one unmeasured warm pair, followed by exactly five measured pairs.
- Blocks 1 and 3 start baseline-first; blocks 2 and 4 start final-first,
  including the warm pair.
- Alternate the first arm on successive measured pairs within each block. For a
  baseline-first block the order is `B,F`, `F,B`, `B,F`, `F,B`, `B,F`; reverse
  it for a final-first block.
- This produces 20 measured pairs (40 observations). Do not add optional runs
  after seeing the results.

For every observation, reset/checkpoint/quiesce PostgreSQL, capture pre-state,
start telemetry, run exactly one binary, capture post-state even on failure,
independently validate the target against the frozen source counts, inspect DMT
state, classify validity, and retain all artifacts before continuing. Do not
rescan the immutable SQL Server source between observations; that would perturb
its cache and shared-disk state.

Do not start Stage B unless Stage A is stable and the post/baseline
`transfer_rps` 95% interval lower bound is at least 0.95 and the post/baseline
external-wall interval upper bound is at most 1.05. If Stage A fails, investigate
the binary/data path and shared-host storage behavior; that result does not
isolate an auto-policy selection problem.

## Stage B: auto-policy comparison

Stage B answers whether the two pre-run auto-policies choose materially
different effective parameters or outcomes after Stage A has cleared the
pipeline.

- Four separately rebooted, freshly qualified, fresh-history replicates,
  preferably spread across multiple days.
- Replicates 1 and 3 start baseline-first; replicates 2 and 4 start final-first.
- Warm-ups use disposable histories and never enter measured histories.
- Maintain an independent state/history for each arm and replicate.
- Before every pair, snapshot and hash both arms' SQLite/checkpoint state.
- Continue the predeclared paired learning schedule until both arms reach the
  regression tier. The final-arm history in every replicate must reach
  regression by attempt 24; if it does not, the hard gate fails. #728 currently
  defines no attempt ceiling for a baseline that has not reached regression. If
  that edge occurs at final-arm attempt 24, pause and obtain a predeclared
  amendment to #728 before running further pairs; do not improvise an unbounded
  or post-hoc stopping rule in the harness.
- Disclose the first pair at which both arms have reached regression. After that
  pair, run exactly ten scheduled measured pairs per replicate, with
  counterbalanced/alternating order. Do not treat the resulting 40 pairs as
  independent observations.
- Keep tier reversals in the scheduled estimator.

An externally invalidated pair may be rerun under the same schedule ID only
after both pre-pair state snapshots have been atomically restored and verified.
If that cannot be done, restart the replicate. Never restore or replace a
product failure to improve `n`.

## Invalidation rules

Freeze a process allowlist before measurement. External invalidation requires
retained evidence of at least one of:

- AC loss, power-mode change, or thermal throttling;
- Windows Update/reboot, telemetry failure, or storage error/disconnect;
- an antivirus or backup scan; or
- a non-allowlisted process exceeding 5% total CPU, 512 MiB working-set growth,
  or 25 MiB/s physical-disk traffic for at least five seconds.

Workload-induced disk saturation, database-caused `System` I/O, DMT retries,
OOMs, deadlocks, partial results, crashes, and tuple mismatches are product or
environment outcomes. Retain them; do not relabel them as external noise.

## Hard gates and analysis

Apply #728's full current gates. In particular:

- every scheduled observation must have exact parity for all nine tables and
  19,310,703 total rows; do not call this content equality unless deterministic
  null and sampled-content validation are also enabled;
- there must be no partial result, corruption, OOM, deadlock, retry exhaustion,
  executed-versus-persisted tuple mismatch, or runtime adjustment;
- DMT internal budget headroom must be at least 10%, calculated as
  `1 - peak_accounted_inflight_bytes / resolved_pipeline_budget_bytes`;
  process private bytes and host headroom are separate measures;
- all four final-arm Stage-B histories must reach regression by attempt 24,
  with no increase in retry-bearing runs or reworked-row share and no run above
  1% rework;
- every Stage-A block must yield five analyzable scheduled pairs and every
  Stage-B replicate ten;
- reduce each block/replicate to one equally weighted mean paired log effect;
  calculate a two-sided 95% t interval across the four effects (3 degrees of
  freedom), then exponentiate to a ratio;
- post/baseline transfer-throughput interval lower bound must be at least 0.95,
  and post/baseline external-wall interval upper bound at most 1.05; claim a
  material speedup only if the throughput lower bound exceeds 1.05;
- pooled final transfer CV must be at most 10%, per-replicate CV at most 12.5%,
  and pooled final CV no more than two percentage points above baseline;
- final p90/median wall time must be at most 1.15 and no more than 10% worse
  than baseline; final worst/median must be at most 1.25; and
- no replicate may be worse than -10% throughput, with replicate effects
  spanning at most ten percentage points.

For a formal #728 performance claim, a native safety or stability gate failure
blocks sign-off even though performance is not a unit-test correctness gate.
Under that protocol, an unstable Stage A is inconclusive and Stage B cannot
isolate an auto-policy problem. The pragmatic release decision below does not
claim to satisfy formal #728 sign-off; it supersedes the old keep instruction
and directs a conservative rollback of the tested treatment.

## Native Windows benchmark outcome and rollback recommendation

The completed campaign was a pragmatic, same-boot directional release screen,
not the full four-replicate formal sign-off described above. It used:

- native SQL Server 2022 Developer and PostgreSQL 16 on the same Windows x64
  laptop and physical disk, with the StackOverflow2010 source fixture;
- immutable binaries for baseline
  `53b521f019a6f20f25b507d3ad57aab44f389b1c` and final
  `9fe2f670e70580aae4aff4e025e8364711d1b4b8`;
- no Docker or WSL database layer;
- disposable warm-ups, separate persistent tuning histories for each arm,
  alternating arm order, and pre-pair state snapshots;
- `migration.tuning: auto` with runtime tuning disabled;
- exact transfer/parity and persisted-versus-executed state assertions for
  every observation; and
- thirteen paired learning runs, followed by five measured pairs after both
  arms reached the regression tier together on learning pair 13.

The run completed without a transfer, parity, retry, product-state, or
orchestration failure. The final arm was adverse in both paired performance
signals in every measured pair, and the aggregate crossed the materiality rule
declared before measurement. The recorded qualitative decision is
`likely_materially_slower`.

Windows telemetry marked background environmental activity during every pair,
so there was no clean-only sensitivity subset and this is not formal #728
sign-off or a cross-hardware performance claim. That caveat does not change the
release decision: the user explicitly chose rollback for a measured material
slowdown, and this run met that branch of the decision rule rather than the
inconclusive branch.

Do not report this result as a formal performance-sign-off pass. Report it as a
qualitative native-Windows regression screen that triggered the predeclared
rollback decision.

The rollback implementation, unit tests, review, and PR may be completed from
a clean clone or worktree on macOS. The dedicated Windows environment is
required only for native performance evidence. Do not rerun the old Mac Docker
campaign, treat Mac or Docker timings as a substitute for the Windows result,
or use them to reverse this rollback decision.

Keep raw SQL Server results private. The local decision record and supporting
artifacts are under:

```text
C:\dmt-bench\manifests\stage-b-auto-pragmatic-same-boot\20260715T023109889Z\result.json
C:\dmt-bench\manifests\stage-b-auto-pragmatic-same-boot\20260715T023109889Z\
C:\dmt-bench\logs\stage-b-auto-pragmatic-same-boot\
C:\dmt-bench\telemetry\stage-b-auto-pragmatic-same-boot\20260715T023109889Z\
```

The two temporary Microsoft Defender exclusions for `C:\dmt-bench` and the
PostgreSQL 16 data directory were removed and verified absent after the run. No
reboot is required for rollback implementation.

### Rollback boundary

The tested treatment is exactly two adjacent commits:

1. PR #729, `0e913ee86e87b8caa05b0c77d503e83c7675e737`, whose direct
   parent is the baseline; and
2. PR #730, `9fe2f670e70580aae4aff4e025e8364711d1b4b8`, whose direct
   parent is PR #729.

The campaign compared the baseline with the combined treatment. It cannot
attribute the regression to only one commit or hunk. Revert both commits,
newest first. Do not reset the branch to the baseline SHA and do not revert the
whole epic.

The rollback removes the cardinality-aware memory model from #729 and the
candidate projection, pin-aware selection/finalization, early runtime-cap
materialization, and unobserved-fallback shrink behavior from #730. There is no
checkpoint schema or persisted-data migration to reverse; these additions are
runtime-only.

Everything through the baseline remains. That retains the earlier epic
benefits, including balanced/safe exploration, unified memory budgeting,
separated safety widths, bounded runtime growth, system-pressure handling,
connection-pool truth, canonical defaults, schema-stat coverage,
runtime-adjustment measurement, UTC timestamp handling, and the dead-code
sweep.

The negative tunable-value guard added in `internal/config/validation.go` by
#730 is standalone and does not affect valid benchmark configurations. Prefer
an exact two-commit rollback first. If this validation is worth retaining,
reapply only that guard and
`TestValidateRejectsNegativeCandidateTunables` from
`internal/config/tunable_validation_test.go` in a separate, reviewable change.
Treat pin-domain projection and persisted/executed tuple identity as later
salvage candidates; they currently share the behavior-changing projection and
materialization path and must be reintroduced independently with fresh
performance evidence.

## Next-agent order of work

1. Read #705, #728, this outcome section, and both correction comments. Confirm
   current main still descends from the two exact treatment commits.
2. On the Mac, pull current main into a clean clone or worktree and create a
   dedicated rollback branch. Do not copy or work from
   `C:\dmt-bench\src\dmt`: that Windows benchmark checkout is currently empty
   with its tracked tree reported as deleted. Do not use `git reset --hard`
   against it or any other dirty checkout.
3. Revert the treatment newest first, preferably as one intentional rollback
   commit:

   ```text
   git revert --no-commit 9fe2f670e70580aae4aff4e025e8364711d1b4b8
   git revert --no-commit 0e913ee86e87b8caa05b0c77d503e83c7675e737
   git commit -m "revert: roll back auto-tune regressions from #729 and #730"
   ```

4. At current main `0d403261d7613fe263b477298846d9adf02d7f76`, production
   code after the rollback should match the tested baseline exactly:

   ```text
   git diff --exit-code 53b521f019a6f20f25b507d3ad57aab44f389b1c HEAD -- cmd internal
   ```

   If main has acquired unrelated production changes, preserve them and verify
   baseline parity only for the files and symbols changed by #729/#730.
5. Search `cmd` and `internal` for orphaned treatment-only symbols:

   ```text
   MemoryProfile
   candidateProjection
   BeginRuntimeChunkSizeProjection
   MaterializeRuntimeChunkSizeCap
   SetPinnedWorkers
   SetPinnedChunkSize
   TunableWorkers
   TunableChunkSize
   ```

6. Remove the superseded #729/#730 changelog claims and add a qualitative
   rollback entry. Do not publish raw SQL Server measurements or ratios.
7. Run the focused and repository-wide verification suite:

   ```text
   go test ./internal/tuning ./internal/config ./internal/driver ./internal/orchestrator -short
   go test ./... -short
   go vet ./...
   go test -race -short -timeout 5m ./...
   golangci-lint run
   ```

8. Build and test the native macOS binary for implementation confidence, and
   let CI exercise its declared platform matrix. Confirm the baseline
   exploration, regression selection, scalar safety clamp, connection pool,
   and runtime-adjustment recording tests remain intact. A macOS build or
   cross-compiled Windows binary is not native Windows performance evidence.
9. If release policy requires performance confirmation, return to the native
   Windows laptop and use fresh isolated histories to compare the rollback
   binary with the frozen baseline in a short, predeclared same-boot paired
   check. Do not reuse either final-arm learning state or select extra runs
   after seeing results. Keep raw outcomes private; do not use Mac Docker
   timings for this step.
10. Open a rollback PR explaining that the native result supports reverting the
    combined treatment while retaining the earlier epic. Obtain clean review
    and green CI, then update #728/#705 with qualitative status. Reintroduce any
    salvage candidate only in a separate PR with a no-pin/no-projection golden
    test and fresh performance evidence.
