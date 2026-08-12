package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"github.com/johndauphine/dmt/v5/internal/audit"
	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/notify"
	"github.com/johndauphine/dmt/v5/internal/observability"
	"github.com/johndauphine/dmt/v5/internal/pool"
	"github.com/johndauphine/dmt/v5/internal/progress"
	"github.com/johndauphine/dmt/v5/internal/source"
	"github.com/johndauphine/dmt/v5/internal/target"
	"github.com/johndauphine/dmt/v5/internal/version"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// New creates a new orchestrator with default options (SQLite state).
func New(cfg *config.Config) (*Orchestrator, error) {
	return NewWithOptions(cfg, Options{})
}

// NewWithOptions creates a new orchestrator with custom options.
func NewWithOptions(cfg *config.Config, opts Options) (*Orchestrator, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}
	envelope := cfg.AutoConfig().MemoryEnvelope
	if envelope.CapacityMB <= 0 || envelope.AvailableMB <= 0 || envelope.BudgetMB <= 0 {
		return nil, fmt.Errorf("config memory envelope is unresolved (capacity=%d MB available=%d MB budget=%d MB); load config through config.Load or config.LoadBytes before constructing an orchestrator",
			envelope.CapacityMB, envelope.AvailableMB, envelope.BudgetMB)
	}

	// Pace the GC against the same budget the pipeline buffers are sized
	// from (#462). Idempotent and env-override-aware; the transfer memory
	// guard remains the backstop.
	cfg.ApplyRuntimeMemoryLimit()

	// Create source pool using factory
	sourcePool, err := pool.NewSourcePool(&cfg.Source, cfg.Migration.MaxSourceConnections)
	if err != nil {
		return nil, fmt.Errorf("creating source pool: %w", err)
	}

	// For source-only mode (analyze command), skip target/state/notifier
	if opts.SourceOnly {
		return &Orchestrator{
			config:     cfg,
			sourcePool: sourcePool,
			opts:       opts,
		}, nil
	}

	// Get the type mapper. Defaults to deterministic-only when no AI is
	// configured; returns a deterministic+AI fallback chain when AI is
	// configured (#170). The unmapped_type_action knob from config
	// chooses what the chain does for Raw types when no AI is available;
	// the approx_type_action knob (#197) opts approx-bearing tables
	// into AI routing for potentially better DDL.
	action := driver.UnmappedAction(cfg.Migration.UnmappedTypeAction)
	approxAction := driver.ApproxAction(cfg.Migration.ApproxTypeAction)
	typeMapper, err := driver.GetTypeMapper(action, approxAction)
	if err != nil {
		sourcePool.Close()
		return nil, fmt.Errorf("loading type mapper: %w", err)
	}

	// Create target pool using factory
	// Canonicalize source type to handle aliases (e.g., "sqlserver" -> "mssql")
	sourceType := driver.Canonicalize(cfg.Source.Type)
	targetPool, err := pool.NewTargetPool(&cfg.Target, cfg.Migration.MaxTargetConnections, sourceType, typeMapper)
	if err != nil {
		sourcePool.Close()
		return nil, fmt.Errorf("creating target pool: %w", err)
	}

	// target_mode=upsert requires the Upserter capability (#476) —
	// reject up front with a clear message instead of failing mid-transfer.
	if cfg.Migration.TargetMode == "upsert" {
		if _, ok := targetPool.(driver.Upserter); !ok {
			sourcePool.Close()
			targetPool.Close()
			return nil, fmt.Errorf("target_mode \"upsert\" is not supported by target engine %s", targetPool.DBType())
		}
	}

	state, err := newStateBackend(cfg, opts, true)
	if err != nil {
		sourcePool.Close()
		targetPool.Close()
		return nil, err
	}

	// Create notifier from the effective config. Config defaults already
	// merge global secrets when no per-config Slack webhook is set.
	notifier := notify.NewFromConfig(cfg.Slack)

	// Create target mode strategy
	targetModeStrategy := NewTargetModeStrategy(
		cfg.Migration.TargetMode,
		targetPool,
		cfg.Target.Schema,
		cfg.Migration.CreateIndexesEnabled(),
		cfg.Migration.CreateForeignKeysEnabled(),
		cfg.Migration.CreateCheckConstraints,
		cfg.Source.Type,
		cfg.Target.Type,
	)

	// Reset the process-wide Metrics surface to noop. A prior orchestrator
	// instance (long-running TUI, tests, sidecar deployments) may have
	// installed a real Registry via SetMetrics; without this reset, a
	// subsequent NewWithOptions caller that doesn't pass --metrics-addr
	// would still emit metrics through observability.Global() from the
	// stale registry. setupObservability() reinstalls a real surface only
	// when the flag is set AND Start() succeeds (Copilot review).
	observability.SetGlobal(observability.Noop())

	return &Orchestrator{
		config:     cfg,
		sourcePool: sourcePool,
		targetPool: targetPool,
		state:      state,
		progress:   progress.New(),
		metrics:    observability.Noop(),
		auditor:    audit.Disabled(),
		notifier:   notifier,
		opts:       opts,
		targetMode: targetModeStrategy,
	}, nil
}

// NewDiagnosticsWithOptions creates an orchestrator that can inspect checkpoint
// state without opening source or target database pools.
func NewDiagnosticsWithOptions(cfg *config.Config, opts Options) (*Orchestrator, error) {
	state, err := newStateBackend(cfg, opts, false)
	if err != nil {
		return nil, err
	}

	observability.SetGlobal(observability.Noop())

	return &Orchestrator{
		config:   cfg,
		state:    state,
		progress: progress.New(),
		metrics:  observability.Noop(),
		auditor:  audit.Disabled(),
		opts:     opts,
	}, nil
}

func newStateBackend(cfg *config.Config, opts Options, cleanupHistory bool) (checkpoint.StateBackend, error) {
	if opts.StateFile != "" {
		state, err := checkpoint.NewFileState(opts.StateFile)
		if err != nil {
			return nil, fmt.Errorf("creating file state manager: %w", err)
		}
		return state, nil
	}

	sqliteState, err := checkpoint.New(cfg.Migration.DataDir)
	if err != nil {
		return nil, fmt.Errorf("creating state manager: %w", err)
	}
	if !cleanupHistory {
		return sqliteState, nil
	}

	retentionDays := cfg.Migration.HistoryRetentionDays
	if retentionDays <= 0 {
		retentionDays = 30
	}
	if deleted, cleanupErr := sqliteState.CleanupOldRuns(retentionDays); cleanupErr != nil {
		logging.Warn("History cleanup failed: %v", cleanupErr)
	} else if deleted > 0 {
		logging.Info("Cleaned up %d old migration runs (retention: %d days)", deleted, retentionDays)
	}

	return sqliteState, nil
}

// Close releases all resources
func (o *Orchestrator) Close() {
	// Stop the progress tracker's periodic reporting goroutine. SetProgressReporter
	// starts it; without this a long-lived caller (the WebUI server, which builds
	// one orchestrator per run) leaks a goroutine per migration. Idempotent with
	// the transfer runner's Finish().
	if o.progress != nil {
		o.progress.Close()
	}
	if o.sourcePool != nil {
		o.sourcePool.Close()
	}
	if o.targetPool != nil {
		o.targetPool.Close()
	}
	if o.state != nil {
		o.state.Close()
	}
}

// SetRunContext sets metadata for the current run (profile name or config path).
func (o *Orchestrator) SetRunContext(profileName, configPath string) {
	o.runProfile = profileName
	o.runConfig = configPath
}

// setPhase updates the progress tracker AND the structured-logging
// base attribute so every subsequent log line, metric label, and trace
// span carries the current phase (#229). Phase names are short snake_case
// (e.g. "transfer", "creating_tables") so they're stable identifiers in
// observability tooling.
//
// setPhase also records the duration of the OUTGOING phase against the
// phase_duration_seconds histogram and ends its OTLP trace span before
// stamping the new phase. The FINAL phase's duration is captured by
// endPhaseSpan() which Run()/Resume() invoke via defer.
func (o *Orchestrator) setPhase(phase string) {
	if !o.phaseStart.IsZero() && o.phaseName != "" {
		o.metrics.ObservePhaseDuration(o.phaseName, time.Since(o.phaseStart).Seconds())
	}
	if o.phaseSpan != nil {
		o.phaseSpan.End()
		o.phaseSpan = nil
	}
	o.progress.SetPhase(phase)
	logging.SetBaseAttr("phase", phase)
	o.phaseStart = time.Now()
	o.phaseName = phase
	// Start a new phase span attached to the run span — observability.Tracer
	// returns a no-op tracer when OTLP isn't configured, so this is free
	// when traces are disabled.
	if o.traceCtx != nil {
		_, o.phaseSpan = observability.Tracer().StartSpan(o.traceCtx, "phase."+phase, "phase", phase)
	}
}

// endPhaseSpan ends the current phase span and records final duration to
// metrics. Called by Run/Resume in a defer so the last phase doesn't get
// stuck "open" after the run ends. Safe to call when no phase is active.
func (o *Orchestrator) endPhaseSpan() {
	if !o.phaseStart.IsZero() && o.phaseName != "" {
		o.metrics.ObservePhaseDuration(o.phaseName, time.Since(o.phaseStart).Seconds())
		o.phaseStart = time.Time{}
		o.phaseName = ""
	}
	if o.phaseSpan != nil {
		o.phaseSpan.End()
		o.phaseSpan = nil
	}
}

// classifyRunOutcome distills the deferred audit handler's two inputs
// (the named-return error from Run/Resume, the panic value from recover)
// into a (status, error_string, resumable) triple. The deferred handler
// uses `resumable` to decide whether to keep the audit file writable
// for a future resume (Codex review on #235 — Ctrl-C / context cancel
// must NOT chmod-lock the file out from under the next dmt resume).
//
// Status taxonomy — these are the values the audit log's `status` field
// can carry on `run_complete` / `resume_complete`. Documented in
// docs/AUDIT-LOG.md; downstream consumers should accept all five:
//   - "success"   — runErr is nil
//   - "failed"    — runErr is non-nil and not a context cancellation;
//     covers preflight, schema, transfer, validation errors
//   - "cancelled" — runErr is context.Canceled or context.DeadlineExceeded
//     (or wraps one of those). Operationally distinct from
//     "failed" because the operator can `dmt resume`.
//   - "panic"     — go runtime panic during the run; rec is non-nil
//   - "partial"   — one or more tables failed and the durable outcome
//     remains eligible for `dmt resume`
//
// Resumable taxonomy:
//   - resumable=true  for "cancelled" and "partial" (`dmt resume` is the
//     right next step — leave the audit file 0600 so resume can reopen it)
//   - resumable=false otherwise (chmod 0444 locks the file as the
//     terminal record of what happened)
func classifyRunOutcome(runErr error, rec any) (status, errStr string, resumable bool) {
	if rec != nil {
		return "panic", fmt.Sprintf("panic: %v", rec), false
	}
	if runErr == nil {
		return "success", "", false
	}
	errStr = runErr.Error()
	var partialErr *PartialMigrationError
	if errors.As(runErr, &partialErr) {
		return "partial", errStr, true
	}
	// Treat context cancellation / deadline as resumable so an
	// interrupted run's audit file stays writable for the eventual
	// `dmt resume`. The exitcodes package recognizes "context
	// canceled" / "context deadline" — use the same string match for
	// consistency.
	low := strings.ToLower(errStr)
	if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) ||
		strings.Contains(low, "context canceled") || strings.Contains(low, "context deadline") {
		return "cancelled", errStr, true
	}
	return "failed", errStr, false
}

// openAuditor opens an append-only NDJSON audit log for this run
// (#235). Honors the config's AuditDir / TamperEvident / NoAudit
// settings. Failures to open the audit log degrade to a disabled
// auditor with a warning; compliance benefits less from a refused
// migration than from a successful one with a missing audit record,
// so the migration always proceeds.
//
// The resume flag drives one piece of post-open behavior: when true
// and the file did NOT pre-exist (the prior Run() ran without audit
// enabled, or somebody deleted the file), we emit an `audit_missing_on_resume`
// event before any other audit traffic so the audit reader can
// detect the gap. The audit file's hash chain (when tamper-evident)
// will start fresh from GENESIS in that case — there's no prior chain
// to continue.
func (o *Orchestrator) openAuditor(runID string, resume bool) {
	o.schemaEvolution().SetRunID(runID)
	if o.config.Migration.NoAudit {
		o.auditor = audit.Disabled()
		return
	}
	// Note whether the file pre-existed before we open it, so we can
	// distinguish "resume reopening the original audit" from "resume
	// creating fresh because the original was missing". audit.New's
	// O_CREATE makes this check have to happen first.
	preexisting := true
	if resume {
		path, _ := audit.ResolveFilePath(o.config.Migration.AuditDir, runID)
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			preexisting = false
		}
	}
	logger, err := audit.New(audit.Options{
		Dir:           o.config.Migration.AuditDir,
		RunID:         runID,
		TamperEvident: o.config.Migration.AuditTamperEvident,
	})
	if err != nil {
		logging.Warn("audit log disabled: %v", err)
		o.auditor = audit.Disabled()
		return
	}
	o.auditor = logger
	logging.Debug("audit log opened: %s", logger.Path())
	if resume && !preexisting {
		// Operator started Resume on a run that has no prior audit
		// file — either the original Run() had --no-audit set, or
		// somebody moved/deleted the file. Either way, this is the
		// first audit event for the run; mark the discontinuity
		// explicitly so the auditor sees a "this isn't the whole
		// story" signal at the top.
		o.auditEvent("audit_missing_on_resume", map[string]any{
			"note": "no prior audit file found for this run_id; resume is creating a fresh audit log",
		})
	}
}

// auditEvent records one audit event with the standard typed shape.
// Failure to record is logged but never propagates — see openAuditor's
// rationale.
func (o *Orchestrator) auditEvent(typeName string, fields map[string]any) {
	if err := o.auditor.RecordEvent(audit.Event{Type: typeName, Fields: fields}); err != nil {
		logging.Warn("audit record %q: %v", typeName, err)
	}
}

// operatorLabel returns a short identity string for the audit log:
// "user@hostname". Best-effort — empty fields are fine for the auditor.
// Future enhancement: read from $DMT_OPERATOR if set to support service-
// account scenarios where the OS user is generic.
func operatorLabel() string {
	user := os.Getenv("USER")
	if user == "" {
		user = os.Getenv("USERNAME") // Windows
	}
	host, _ := os.Hostname()
	if user == "" && host == "" {
		return "unknown"
	}
	if user == "" {
		return "@" + host
	}
	if host == "" {
		return user
	}
	return user + "@" + host
}

// versionString returns dmt's build version from internal/version,
// matching what `dmt --version` reports. Available for audit-log
// "dmt_version" field.
func versionString() string {
	return version.Version
}

// SetMetrics installs a non-noop metrics implementation. Called by the
// CLI when --metrics-addr is set and the Registry has been started.
// Safe to call before Run/Resume; effects take hold on the next phase
// transition.
//
// Also installs m as the process-wide Metrics surface (observability.Global)
// so hot-path call sites in transfer/writer pool can record metrics
// without threading the surface through every constructor.
func (o *Orchestrator) SetMetrics(m observability.Metrics) {
	if m == nil {
		o.metrics = observability.Noop()
		observability.SetGlobal(observability.Noop())
		return
	}
	o.metrics = m
	observability.SetGlobal(m)
}

// SetProgressReporter configures JSON progress reporting for Airflow/automation.
// When enabled, disables the terminal progress bar and emits JSON updates to stderr.
func (o *Orchestrator) SetProgressReporter(reporter progress.Reporter, interval time.Duration) {
	o.progress.SetReporter(reporter, interval)
}

// enforcePGIdentifierCollisionGate fails the migration before any DDL runs when
// the target is PostgreSQL and two source identifiers would sanitize to the
// same name. In drop_recreate a table-name collision silently destroys one
// table's data and a column-name collision produces invalid DDL, so this is a
// hard gate rather than a warning (#553).
//
// It is keyed on targetPool.DBType() — the canonical, alias-resolved engine
// name — NOT the raw config value: PostgreSQL is also selected via the "pg" and
// "postgresql" aliases, and the sanitization / drop_recreate / transfer paths
// all key on DBType() == "postgres". Keying this gate on the raw config string
// would let those aliases skip the gate while the destructive paths still run,
// reintroducing the silent data loss.
func (o *Orchestrator) enforcePGIdentifierCollisionGate(tables []source.Table) error {
	if !o.targetIsPostgres() {
		return nil
	}
	return o.checkPGIdentifierCollisions(tables)
}

// targetIsPostgres reports whether the target engine is PostgreSQL by its
// canonical name, resolving aliases like "pg"/"postgresql".
func (o *Orchestrator) targetIsPostgres() bool {
	return o.targetPool.DBType() == "postgres"
}

func (o *Orchestrator) checkPGIdentifierCollisions(tables []source.Table) error {
	tableInfos := make([]target.TableInfo, len(tables))
	for i := range tables {
		tableInfos[i] = &tables[i]
	}
	return target.DetectPGIdentifierCollisions(tableInfos)
}

// logPGIdentifierChanges logs any identifier name changes applied during PostgreSQL migration
func (o *Orchestrator) logPGIdentifierChanges(tables []source.Table) {
	// Convert to TableInfo interface slice
	tableInfos := make([]target.TableInfo, len(tables))
	for i := range tables {
		tableInfos[i] = &tables[i]
	}

	report := target.CollectPGIdentifierChanges(tableInfos)
	if !report.HasChanges() {
		return
	}

	logging.Debug("")
	logging.Debug("PostgreSQL identifier changes applied:")

	for _, tc := range report.Tables {
		if tc.HasTableChange {
			logging.Debug("  Table: '%s' → '%s'", tc.TableName.Original, tc.TableName.Sanitized)
		}
		for _, cc := range tc.ColumnChanges {
			tableName := tc.TableName.Sanitized
			if !tc.HasTableChange {
				tableName = tc.TableName.Original
			}
			logging.Debug("    %s: column '%s' → '%s'", tableName, cc.Original, cc.Sanitized)
		}
	}

	logging.Debug("")
	logging.Debug("Summary: %d table(s) renamed, %d column(s) renamed across %d table(s)",
		report.TotalTableChanges, report.TotalColumnChanges, report.TablesWithChanges)
}

// notifyFailure sends a failure notification when migration.notify.on_failure
// allows it.
func (o *Orchestrator) notifyFailure(runID string, err error, duration time.Duration) {
	if !o.config.Migration.NotifyOnFailure() {
		return
	}
	o.notifier.MigrationFailed(runID, err, duration)
}

func (o *Orchestrator) notifyCompletion(runID string, startTime time.Time, duration time.Duration, tableCount int, rowCount int64, throughput float64) {
	if !o.config.Migration.NotifyOnSuccess() {
		return
	}
	o.notifier.MigrationCompleted(runID, startTime, duration, tableCount, rowCount, throughput)
}

func (o *Orchestrator) notifyCompletionWithErrors(
	runID string,
	startTime time.Time,
	duration time.Duration,
	successTables int,
	failedTables int,
	rowCount int64,
	throughput float64,
	failures []string,
) {
	if !o.config.Migration.NotifyOnFailure() {
		return
	}
	o.notifier.MigrationCompletedWithErrors(
		runID, startTime, duration, successTables, failedTables, rowCount, throughput, failures,
	)
}

func (o *Orchestrator) completeRunRequired(runID, status, errorMessage string) error {
	err := o.state.CompleteRun(runID, status, errorMessage)
	return checkpoint.RequiredWrite(fmt.Sprintf("recording run %s terminal status %q", runID, status), err)
}

func (o *Orchestrator) completeRunResumableRequired(runID, status, errorMessage, reason string) error {
	err := o.state.CompleteRunResumable(runID, status, errorMessage, reason)
	return checkpoint.RequiredWrite(fmt.Sprintf("recording run %s resumable outcome %q", runID, status), err)
}

func (o *Orchestrator) completePartialRunRequired(runID, errorMessage string) error {
	if o.config.Migration.AllowPartial {
		return o.completeRunRequired(runID, "partial", errorMessage)
	}
	return o.completeRunResumableRequired(
		runID,
		"partial",
		errorMessage,
		checkpoint.RunResumabilityPartialFailure,
	)
}

func (o *Orchestrator) markRunAsResumedRequired(runID string) error {
	err := o.state.MarkRunAsResumed(runID)
	return checkpoint.RequiredWrite(fmt.Sprintf("resetting running tasks for resumed run %s", runID), err)
}

// tableNamesForTuning collects table names from the filtered table set
// so the smartconfig analyzer can scope its workload-wide derivations
// (packet cap, avg/max row size, memory budget) to what's actually in
// the run (#241). Returns nil when tables is empty so the analyzer's
// "no filter" path is taken — that keeps the analyze CLI subcommand
// (which has no filter context) on the pre-#241 behavior.
func tableNamesForTuning(tables []source.Table) []string {
	if len(tables) == 0 {
		return nil
	}
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Name)
	}
	return names
}

// filterTables filters tables based on include/exclude patterns
func (o *Orchestrator) filterTables(tables []source.Table) []source.Table {
	include := o.config.Migration.IncludeTables
	exclude := o.config.Migration.ExcludeTables

	// If no filters configured, return all tables
	if len(include) == 0 && len(exclude) == 0 {
		return tables
	}

	var filtered []source.Table
	var skipped []string

	for _, t := range tables {
		tableName := strings.ToLower(t.Name)

		// Check include patterns (if specified, table must match at least one)
		if len(include) > 0 {
			matched := false
			for _, pattern := range include {
				if match, _ := filepath.Match(strings.ToLower(pattern), tableName); match {
					matched = true
					break
				}
			}
			if !matched {
				skipped = append(skipped, t.Name)
				continue
			}
		}

		// Check exclude patterns (table must not match any)
		excluded := false
		for _, pattern := range exclude {
			if match, _ := filepath.Match(strings.ToLower(pattern), tableName); match {
				excluded = true
				skipped = append(skipped, t.Name)
				break
			}
		}
		if excluded {
			continue
		}

		filtered = append(filtered, t)
	}

	if len(skipped) > 0 {
		logging.Debug("Skipped %d tables by filter: %v", len(skipped), skipped)
	}

	return filtered
}

// loadSchemaMetadata intentionally ignores the target DDL creation flags.
// create_indexes/create_foreign_keys/create_check_constraints decide what dmt
// creates on the target; schema drift detection needs the complete source shape
// so toggling those flags does not manufacture false index/FK/check drift.
func (o *Orchestrator) loadSchemaMetadata(ctx context.Context, tables []source.Table) {
	for i := range tables {
		t := &tables[i]

		if err := o.sourcePool.LoadIndexes(ctx, t); err != nil {
			logging.Warn("Warning: loading indexes for %s: %v", t.Name, err)
		}

		if err := o.sourcePool.LoadForeignKeys(ctx, t); err != nil {
			logging.Warn("Warning: loading FKs for %s: %v", t.Name, err)
		}

		if err := o.sourcePool.LoadCheckConstraints(ctx, t); err != nil {
			logging.Warn("Warning: loading check constraints for %s: %v", t.Name, err)
		}
	}
}

func finalizableTables(tables []source.Table, failedTableNames map[string]bool) []source.Table {
	success := make([]source.Table, 0, len(tables))
	for _, table := range tables {
		if failedTableNames[table.Name] {
			continue
		}

		finalTable := table
		if len(table.ForeignKeys) > 0 {
			finalTable.ForeignKeys = filterFinalizationForeignKeys(table, failedTableNames)
		}
		success = append(success, finalTable)
	}
	return success
}

func filterFinalizationForeignKeys(table source.Table, failedTableNames map[string]bool) []source.ForeignKey {
	foreignKeys := make([]source.ForeignKey, 0, len(table.ForeignKeys))
	for _, fk := range table.ForeignKeys {
		if failedTableNames[fk.RefTable] {
			logging.Warn("Skipping FK %s on %s because referenced table %s failed transfer",
				fk.Name, table.Name, fk.RefTable)
			continue
		}
		foreignKeys = append(foreignKeys, fk)
	}
	return foreignKeys
}

// fallbackSink is the observability.FallbackSink adapter that pins
// every RecordFallback write to the current run's row in the
// checkpoint's fallback_events table. The orchestrator installs an
// instance at Run/Resume start and clears it at teardown so a
// long-running process (TUI, sidecar) doesn't keep writing to a
// completed run's state.
//
// state is the backend the run is using (SQLite or FileState — both
// implement SaveFallbackEvent). runID scopes every write so cross-
// process `dmt status` queries can read just this run's counts (#176).
type fallbackSink struct {
	state checkpoint.StateBackend
	runID string
}

func newFallbackSink(state checkpoint.StateBackend, runID string) *fallbackSink {
	return &fallbackSink{state: state, runID: runID}
}

// SaveFallbackEvent forwards an in-process RecordFallback to the
// checkpoint backend. Errors propagate to the observability package,
// which logs them at debug — a state-write failure is not worth
// failing the migration over, and the in-memory + Prometheus counters
// still observed the event.
func (s *fallbackSink) SaveFallbackEvent(surface, fingerprint string) error {
	if s == nil || s.state == nil {
		return nil
	}
	return s.state.SaveFallbackEvent(s.runID, surface, fingerprint)
}
