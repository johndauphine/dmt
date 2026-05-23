# Orchestrator flow

This note is a map for finding the migration lifecycle in `internal/orchestrator`.
Files are intentionally split so each one stays readable; the map is the starting
point when you need the whole story.

## Run path

`Run()` in `run.go` is the top-level path for a fresh migration.

1. Initialize run context, audit logging, metrics, and phase spans.
2. Run preflight checks.
3. Discover source schema and apply include/exclude filters.
4. Report source schema drift.
5. Apply deterministic smartconfig tuning.
6. Prepare target tables through `target_mode.go`.
7. Execute transfer through `transfer_all.go` and `transfer_runner.go`.
8. Finalize target objects through the selected target-mode strategy.
9. Reconcile deletes when configured.
10. Validate transferred data.
11. Mark the run `success`, `partial`, or `failed`, then notify and audit.

## Resume path

`Resume()` in `resume.go` follows the same broad lifecycle, but starts from the
last incomplete run.

1. Find the latest incomplete run and reject superseded runs.
2. Validate the run heartbeat unless `--force-resume` was supplied.
3. Validate the config hash unless `--force-resume` was supplied.
4. Re-run preflight and schema discovery because the environment may have changed.
5. Recompute the filtered table list and smartconfig values.
6. Skip tables that are already complete and still have matching target row counts.
7. For incomplete tables, use table-level and partition-level progress to decide
   whether to resume, recreate, clear progress, or truncate.
8. Execute transfer for only the incomplete tables.
9. Finalize, reconcile deletes, validate, and complete the original run.

## Config ownership

Config loading keeps a provenance trail for tunable values so the orchestrator
can tell human intent from generated defaults.

- `LoadBytes()` captures the raw YAML/template-expanded config before secrets,
  driver defaults, smartconfig, or runtime tuning are applied.
- Values that came from the migration config are tagged `config`. Values
  inherited from `~/.secrets/dmt-config.yaml` are tagged `secrets default`.
  Driver/formula defaults are tagged separately.
- Smartconfig may replace generated defaults, but it must not overwrite values
  tagged `config` or `secrets default`. The debug dump prints the source label
  next to each tunable so operators can see which layer owns it.
- Runtime tuning is intentionally separate from `*config.Config`: the transfer
  runner seeds a `RuntimeTuner` snapshot from config, and the rule-based
  controller adjusts that runtime state while the original config remains the
  audit/resume baseline.

## Transfer runner files

The transfer runner is split by responsibility:

- `transfer_all.go`: high-level transfer entry point used by run and resume.
- `transfer_runner.go`: runner setup, runtime tuning/controller setup, result assembly.
- `transfer_runner_prepare.go`: source/target column preparation.
- `transfer_runner_jobs.go`: job execution, retries, and per-job accounting.
- `transfer_runner_errors.go`: retry/error classification helpers.
- `transfer_runner_logging.go`: transfer stats and profile logging.
- `job_builder.go`: converts tables into table or partition transfer jobs.
- `resume_progress.go`: resume preflight helpers for table and partition progress.

## Target mode files

- `target_mode.go`: strategy interface and implementations for target setup/finalize.
- `schema_evolution.go`: opt-in target schema drift application.
- `schema_drift.go`: source drift reporting before transfer.
- `delete_reconciliation.go`: target delete propagation for incremental upsert.

## Supporting surfaces

- `preflight.go`: minimum privilege and environment checks.
- `validator.go` and `validation/`: row count, sample, and null-parity validation.
- `regime_capture.go`: DB tuning snapshot captured for smartconfig history.
- `healthcheck*.go`: analyze/health-check command surfaces.
- `status*.go` and `summary.go`: run history and status presentation.

## Consolidation guidance

Prefer keeping files small enough to read in one sitting. Consolidate only when
two files form one conceptual unit and the move reduces navigation cost. For
example, `transfer_runner*.go` can be reviewed for naming and grouping after the
runtime/checkpoint contracts are covered by tests; it should not be collapsed
back into a multi-thousand-line file.
