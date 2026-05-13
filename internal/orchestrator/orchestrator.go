package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/johndauphine/dmt/internal/audit"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/notify"
	"github.com/johndauphine/dmt/internal/observability"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/source"
	"github.com/johndauphine/dmt/internal/target"
	"github.com/johndauphine/dmt/internal/version"
)

// TaskType defines the type of migration task
type TaskType string

const (
	TaskExtractSchema  TaskType = "extract_schema"
	TaskCreateTables   TaskType = "create_tables"
	TaskTransfer       TaskType = "transfer"
	TaskResetSequences TaskType = "reset_sequences"
	TaskCreatePKs      TaskType = "create_pks"
	TaskCreateIndexes  TaskType = "create_indexes"
	TaskCreateFKs      TaskType = "create_fks"
	TaskCreateChecks   TaskType = "create_checks"
	TaskValidate       TaskType = "validate"
)

// TableFailure records a table transfer failure
type TableFailure struct {
	TableName string
	Error     error
}

// PartialMigrationError is returned by Run/Resume when at least one
// table failed to transfer and migration.allow_partial is not enabled.
// Pre-#248 these scenarios returned nil and exited 0, which silently
// promoted incomplete migrations in unattended automation. The error
// implements an ExitCode() method so exitcodes.FromError maps it to
// TransferError without needing to import the orchestrator package.
type PartialMigrationError struct {
	Failed []TableFailure
}

func (e *PartialMigrationError) Error() string {
	names := make([]string, len(e.Failed))
	for i, f := range e.Failed {
		names[i] = f.TableName
	}
	return fmt.Sprintf("migration completed with %d failed table(s): %s",
		len(e.Failed), strings.Join(names, ", "))
}

// ExitCode reports the CLI exit code this error should map to.
// exitcodes only depends on stdlib, so importing it here is acyclic
// (orchestrator → exitcodes is a one-way edge); using the constant
// rather than a magic 3 keeps the two in sync if TransferError is
// ever renumbered.
func (e *PartialMigrationError) ExitCode() int { return exitcodes.TransferError }

// Orchestrator coordinates the migration process
type Orchestrator struct {
	config     *config.Config
	sourcePool pool.SourcePool
	targetPool pool.TargetPool
	state      checkpoint.StateBackend
	progress   *progress.Tracker
	notifier   notify.Provider
	tables     []source.Table
	runProfile string
	runConfig  string
	opts       Options
	targetMode TargetModeStrategy

	// Set by transferAll after each run; used by UpdateAITuningResult to persist
	// retry pressure into ai_tuning_history.
	lastChunkRetryCount int

	// metrics is the observability surface for #229. Always non-nil —
	// observability.Noop() satisfies the interface when metrics are
	// disabled, so call sites can invoke methods unconditionally without
	// nil checks. SetMetrics() swaps in a real Registry when --metrics-addr
	// is supplied.
	metrics observability.Metrics

	// phaseStart tracks when the current phase began so setPhase can record
	// the duration of the OUTGOING phase against the metrics histogram.
	// Zero value means "no prior phase" — first call records nothing.
	phaseStart time.Time
	phaseName  string

	// traceCtx carries the active run span's context for downstream phase
	// spans (#229). Set in Run/Resume after the run span starts; setPhase
	// derives a phase span from it. Nil outside Run/Resume.
	traceCtx context.Context

	// phaseSpan is the active phase span — ended when setPhase transitions
	// or when Run/Resume returns. Nil before the first setPhase call.
	phaseSpan observability.Span

	// auditor writes the immutable per-run NDJSON record (#235). Always
	// non-nil — audit.Disabled() satisfies the interface when --no-audit
	// is set so call sites stay unconditional. Initialized in Run/Resume
	// once the run_id is known.
	auditor *audit.Logger
}

// Options configures the orchestrator.
type Options struct {
	// StateFile overrides SQLite with a YAML state file (for Airflow).
	// If empty, uses SQLite in DataDir.
	StateFile string

	// RunID allows specifying a deterministic run ID (for Airflow).
	// If empty, a UUID is generated.
	RunID string

	// ForceResume bypasses config hash validation on resume.
	ForceResume bool

	// SourceOnly creates orchestrator with only source pool (for analyze command).
	// When true, target pool is not created and analyze operations only work.
	SourceOnly bool
}

// computeConfigHash returns a short hex hash of the sanitized config.
func computeConfigHash(cfg *config.Config) string {
	configJSON, _ := json.Marshal(cfg.Sanitized())
	hash := sha256.Sum256(configJSON)
	return hex.EncodeToString(hash[:8])
}

// isRetryableError determines if an error is transient and worth retrying.
// This includes connection errors, timeouts, and deadlocks.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"connection reset",
		"connection refused",
		"connection timed out",
		"deadlock",
		"lock timeout",
		"too many connections",
		"server is shutting down",
		"broken pipe",
		"unexpected eof",
		"i/o timeout",
		"context deadline exceeded",
		"retry",
	}
	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}
	return false
}

// MigrationResult contains the outcome of a migration run.
type MigrationResult struct {
	RunID           string        `json:"run_id"`
	Status          string        `json:"status"`
	StartedAt       time.Time     `json:"started_at"`
	CompletedAt     time.Time     `json:"completed_at"`
	DurationSeconds float64       `json:"duration_seconds"`
	TablesTotal     int           `json:"tables_total"`
	TablesSuccess   int           `json:"tables_success"`
	TablesFailed    int           `json:"tables_failed"`
	RowsTransferred int64         `json:"rows_transferred"`
	RowsPerSecond   int64         `json:"rows_per_second"`
	FailedTables    []string      `json:"failed_tables"`
	TableStats      []TableResult `json:"table_stats"`
	Error           string        `json:"error,omitempty"`
}

// TableResult contains the outcome for a single table.
type TableResult struct {
	Name   string `json:"name"`
	Rows   int64  `json:"rows"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// StatusResult contains the current status of a migration.
type StatusResult struct {
	RunID           string    `json:"run_id"`
	Status          string    `json:"status"`
	Phase           string    `json:"phase"`
	StartedAt       time.Time `json:"started_at"`
	TablesTotal     int       `json:"tables_total"`
	TablesComplete  int       `json:"tables_complete"`
	TablesRunning   int       `json:"tables_running"`
	TablesPending   int       `json:"tables_pending"`
	TablesFailed    int       `json:"tables_failed"`
	RowsTransferred int64     `json:"rows_transferred"`
	ProgressPercent float64   `json:"progress_percent"`
}

// HealthCheckResult contains connection health information and preflight
// findings (#228). Connections are pinged in parallel; preflight checks
// run sequentially after both pings succeed (driver-level findings need
// a live DB anyway). PreFlightFindings is empty when both sides pass
// cleanly; Healthy is true only when connections succeed AND no
// SeverityError findings remain.
type HealthCheckResult struct {
	Timestamp         string                     `json:"timestamp"`
	SourceConnected   bool                       `json:"source_connected"`
	SourceLatencyMs   int64                      `json:"source_latency_ms"`
	SourceDBType      string                     `json:"source_db_type"`
	SourceTableCount  int                        `json:"source_table_count,omitempty"`
	SourceError       string                     `json:"source_error,omitempty"`
	TargetConnected   bool                       `json:"target_connected"`
	TargetLatencyMs   int64                      `json:"target_latency_ms"`
	TargetDBType      string                     `json:"target_db_type"`
	TargetError       string                     `json:"target_error,omitempty"`
	Healthy           bool                       `json:"healthy"`
	PreFlightFindings []driver.PreFlightFinding  `json:"preflight_findings,omitempty"`
	PreFlightAborted  bool                       `json:"preflight_aborted,omitempty"`
}

// DryRunResult contains the migration plan preview.
type DryRunResult struct {
	SourceType     string        `json:"source_type"`
	TargetType     string        `json:"target_type"`
	SourceSchema   string        `json:"source_schema"`
	TargetSchema   string        `json:"target_schema"`
	Tables         []DryRunTable `json:"tables"`
	TotalRows      int64         `json:"total_rows"`
	TotalTables    int           `json:"total_tables"`
	EstimatedMemMB int64         `json:"estimated_memory_mb"`
	Workers        int           `json:"workers"`
	ChunkSize      int           `json:"chunk_size"`
	TargetMode     string        `json:"target_mode"`
}

// DryRunTable contains preview information for a single table.
type DryRunTable struct {
	Name             string `json:"name"`
	RowCount         int64  `json:"row_count"`
	PaginationMethod string `json:"pagination_method"`
	Partitions       int    `json:"partitions"`
	HasPK            bool   `json:"has_pk"`
	Columns          int    `json:"columns"`
}

// New creates a new orchestrator with default options (SQLite state).
func New(cfg *config.Config) (*Orchestrator, error) {
	return NewWithOptions(cfg, Options{})
}

// NewWithOptions creates a new orchestrator with custom options.
func NewWithOptions(cfg *config.Config, opts Options) (*Orchestrator, error) {
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

	// Create state manager based on options
	var state checkpoint.StateBackend
	if opts.StateFile != "" {
		// Use file-based state (for Airflow/headless)
		state, err = checkpoint.NewFileState(opts.StateFile)
		if err != nil {
			sourcePool.Close()
			targetPool.Close()
			return nil, fmt.Errorf("creating file state manager: %w", err)
		}
	} else {
		// Use SQLite state (default for desktop)
		sqliteState, err := checkpoint.New(cfg.Migration.DataDir)
		if err != nil {
			sourcePool.Close()
			targetPool.Close()
			return nil, fmt.Errorf("creating state manager: %w", err)
		}

		// Cleanup old runs based on retention policy
		retentionDays := cfg.Migration.HistoryRetentionDays
		if retentionDays <= 0 {
			retentionDays = 30 // Default
		}
		if deleted, cleanupErr := sqliteState.CleanupOldRuns(retentionDays); cleanupErr != nil {
			logging.Warn("History cleanup failed: %v", cleanupErr)
		} else if deleted > 0 {
			logging.Info("Cleaned up %d old migration runs (retention: %d days)", deleted, retentionDays)
		}

		state = sqliteState
	}

	// Create notifier from global secrets
	notifier := notify.NewFromSecrets()

	// Create target mode strategy
	targetModeStrategy := NewTargetModeStrategy(
		cfg.Migration.TargetMode,
		targetPool,
		cfg.Target.Schema,
		cfg.Migration.CreateIndexes,
		cfg.Migration.CreateForeignKeys,
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

// Close releases all resources
func (o *Orchestrator) Close() {
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
// docs/AUDIT-LOG.md; downstream consumers should accept all four:
//   - "success"   — runErr is nil
//   - "failed"    — runErr is non-nil and not a context cancellation;
//                   covers preflight, schema, transfer, validation errors
//   - "cancelled" — runErr is context.Canceled or context.DeadlineExceeded
//                   (or wraps one of those). Operationally distinct from
//                   "failed" because the operator can `dmt resume`.
//   - "panic"     — go runtime panic during the run; rec is non-nil
//
// Resumable taxonomy:
//   - resumable=true  for "cancelled" (operator interrupted the run
//                     intentionally; `dmt resume` is the right next
//                     step — leave the audit file 0600 so resume can
//                     reopen it)
//   - resumable=false otherwise (chmod 0444 locks the file as the
//                     terminal record of what happened)
func classifyRunOutcome(runErr error, rec any) (status, errStr string, resumable bool) {
	if rec != nil {
		return "panic", fmt.Sprintf("panic: %v", rec), false
	}
	if runErr == nil {
		return "success", "", false
	}
	errStr = runErr.Error()
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

// Run executes a new migration.
func (o *Orchestrator) Run(ctx context.Context) (runErr error) {
	// Use provided run ID or generate a new one
	runID := o.opts.RunID
	if runID == "" {
		runID = uuid.New().String()[:8]
	}
	startTime := time.Now()

	// #229 observability: stamp run_id + source/target DB type + an
	// initial phase ("starting") onto every log line, metric label,
	// and trace span for the rest of this run. Without the initial
	// phase value, the Info("Starting migration run...") lines below
	// would log with `phase=<missing>` until the first setPhase call
	// (Copilot review). Cleared via defer so process-resident state
	// (TUI, tests) doesn't leak attributes from a prior run into the next.
	logging.WithFields(map[string]any{
		"run_id":    runID,
		"source_db": o.sourcePool.DBType(),
		"target_db": o.targetPool.DBType(),
		"phase":     "starting",
	})
	o.metrics.RunStarted(runID, o.sourcePool.DBType(), o.targetPool.DBType())
	defer logging.ClearBaseAttrs()
	defer o.metrics.RunComplete(runID)

	// One root span per run; phase spans below attach as children (#229).
	// No-op when OTLP isn't configured.
	runCtx, runSpan := observability.Tracer().StartSpan(ctx, "dmt.run",
		"run_id", runID, "source_db", o.sourcePool.DBType(), "target_db", o.targetPool.DBType())
	o.traceCtx = runCtx
	defer func() {
		o.endPhaseSpan()
		runSpan.End()
		o.traceCtx = nil
	}()

	// #235 audit log: per-run immutable NDJSON. Set up before any other
	// run-side work so a panic in the early phases is still recorded.
	// The deferred close emits run_complete (with final status pulled
	// from a captured named return value below) and chmods the file
	// to 0444. Compliance auditors get a self-describing file on
	// every exit path — success, partial, error, even a panic.
	o.openAuditor(runID, false /*resume*/)
	defer func() {
		rec := recover()
		status, errStr, resumable := classifyRunOutcome(runErr, rec)
		o.auditEvent("run_complete", map[string]any{
			"status":      status,
			"error":       errStr,
			"duration_ms": time.Since(startTime).Milliseconds(),
		})
		// Resumable interruption (Ctrl-C / context cancel) MUST leave
		// the audit file writable so the eventual `dmt resume` can
		// append its own resume_start / resume_complete events.
		// CloseResumable() skips the chmod-0444 step; a successful or
		// hard-failed run gets the usual lockdown (Codex review on #235).
		if resumable {
			if err := o.auditor.CloseResumable(); err != nil {
				logging.Warn("audit close: %v", err)
			}
		} else {
			if err := o.auditor.Close(); err != nil {
				logging.Warn("audit close: %v", err)
			}
		}
		if rec != nil {
			// re-raise after the audit event has been written to the
			// OS (tamper-evident mode additionally fsyncs); the OS may
			// still hold dirty pages but the audit record is no longer
			// in dmt's process memory only.
			panic(rec)
		}
	}()
	o.auditEvent("run_start", map[string]any{
		"operator":    operatorLabel(),
		"dmt_version": versionString(),
		"source": map[string]any{
			"driver":   o.sourcePool.DBType(),
			"host":     o.config.Source.Host,
			"database": o.config.Source.Database,
			"schema":   o.config.Source.Schema,
		},
		"target": map[string]any{
			"driver":   o.targetPool.DBType(),
			"host":     o.config.Target.Host,
			"database": o.config.Target.Database,
			"schema":   o.config.Target.Schema,
		},
		"config_hash": computeConfigHash(o.config),
	})

	logging.Info("Starting migration run: %s", runID)
	logging.Info("Migration: %s -> %s", o.sourcePool.DBType(), o.targetPool.DBType())

	// Log comprehensive configuration dump
	logging.Debug("%s", o.config.DebugDump())

	// Set runtime memory limit using Go's soft limit mechanism
	// This tells the GC to work harder to stay under the limit
	effectiveMemMB := o.config.AutoConfig().EffectiveMaxMemoryMB
	if effectiveMemMB > 0 {
		memLimitBytes := effectiveMemMB * 1024 * 1024
		debug.SetMemoryLimit(memLimitBytes)
		logging.Debug("Runtime memory limit set to %d MB (Go GC soft limit)", effectiveMemMB)
	}

	if err := o.state.CreateRun(runID, o.config.Source.Schema, o.config.Target.Schema, o.config.Sanitized(), o.runProfile, o.runConfig); err != nil {
		return fmt.Errorf("creating run: %w", err)
	}

	// Preflight (phase 0): verify the environment satisfies the assumptions
	// downstream phases make — privileges, version floor, encoding, connection
	// headroom. Fail loud and early rather than minutes into a partial run
	// (#228). Run AFTER CreateRun so the failed-preflight outcome shows up
	// in the run history; the operator can grep `dmt history` for it later.
	o.setPhase("preflight")
	logging.Debug("Running preflight checks...")
	if err := o.runPreFlight(ctx); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	// Extract schema
	o.setPhase("extracting_schema")
	logging.Debug("Extracting schema...")
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("extracting schema: %w", err)
	}

	// Load additional metadata if enabled
	for i := range tables {
		t := &tables[i]

		if o.config.Migration.CreateIndexes {
			if err := o.sourcePool.LoadIndexes(ctx, t); err != nil {
				logging.Warn("Warning: loading indexes for %s: %v", t.Name, err)
			}
		}

		if o.config.Migration.CreateForeignKeys {
			if err := o.sourcePool.LoadForeignKeys(ctx, t); err != nil {
				logging.Warn("Warning: loading FKs for %s: %v", t.Name, err)
			}
		}

		if o.config.Migration.CreateCheckConstraints {
			if err := o.sourcePool.LoadCheckConstraints(ctx, t); err != nil {
				logging.Warn("Warning: loading check constraints for %s: %v", t.Name, err)
			}
		}

	}

	// Apply table filters
	tables = o.filterTables(tables)
	if len(tables) == 0 {
		o.state.CompleteRun(runID, "failed", "no tables to migrate after applying filters")
		return fmt.Errorf("no tables to migrate after applying filters")
	}

	o.tables = tables
	o.progress.SetTablesTotal(len(tables))
	logging.Debug("Found %d tables", len(tables))

	// Apply AI-recommended parameters (if AI is available)
	o.applyAITuning(ctx)

	// Refine memory settings based on actual row sizes from database stats
	tableRowSizes := make([]config.TableRowSize, len(tables))
	for i, t := range tables {
		tableRowSizes[i] = config.TableRowSize{
			Name:             t.Name,
			RowCount:         t.RowCount,
			EstimatedRowSize: t.EstimatedRowSize,
		}
	}
	if adjusted, changes := o.config.RefineSettingsForRowSizes(tableRowSizes); adjusted {
		logging.Info("%s", changes)
	} else if changes != "" {
		logging.Debug("%s", changes)
	}

	// Persist the post-tuning config so `dmt history --run <id>` reflects what actually ran.
	if err := o.state.UpdateRunConfig(runID, o.config.Sanitized()); err != nil {
		logging.Warn("failed to persist post-tuning config: %v", err)
	}

	// Print pagination strategy summary
	keysetCount := 0
	rowNumberCount := 0
	for _, t := range tables {
		if t.SupportsKeysetPagination() {
			keysetCount++
		} else if t.HasPK() {
			rowNumberCount++
		}
	}
	logging.Debug("Pagination: %d keyset, %d ROW_NUMBER, %d no PK",
		keysetCount, rowNumberCount, len(tables)-keysetCount-rowNumberCount)

	// Send start notification
	o.notifier.MigrationStarted(runID, o.config.Source.Database, o.config.Target.Database, len(tables))

	// Create target schema and tables
	o.setPhase("creating_tables")
	if err := o.targetPool.CreateSchema(ctx, o.config.Target.Schema); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("creating schema: %w", err)
	}

	// Prepare target tables using the appropriate strategy
	if err := o.targetMode.PrepareTables(ctx, tables); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	// Transfer data
	o.setPhase("transfer")
	logging.Debug("Transferring data...")
	o.state.UpdatePhase(runID, "transferring")
	transferStart := time.Now()
	tableFailures, err := o.transferAll(ctx, runID, tables, false)
	transferDuration := time.Since(transferStart)
	if err != nil {
		// If context was canceled (Ctrl+C), leave run as "running" so resume works
		// but reset any "running" tasks to "pending" so status shows correctly
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			o.state.MarkRunAsResumed(runID) // Reset running tasks to pending
			logging.Info("Migration interrupted - run 'resume' to continue")
			return fmt.Errorf("transferring data: %w", err)
		}
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("transferring data: %w", err)
	}

	// Log summary of table failures (individual failures already logged)
	if len(tableFailures) > 0 {
		logging.Warn("%d table(s) failed during transfer:", len(tableFailures))
		for _, f := range tableFailures {
			logging.Warn("  - %s: %v", f.TableName, f.Error)
		}
	}

	// Filter out failed tables for finalize/validate
	failedTableNames := make(map[string]bool)
	for _, f := range tableFailures {
		failedTableNames[f.TableName] = true
	}
	var successTables []source.Table
	for _, t := range tables {
		if !failedTableNames[t.Name] {
			successTables = append(successTables, t)
		}
	}

	// Finalize (only for successful tables)
	o.setPhase("finalizing")
	logging.Debug("Finalizing...")
	o.state.UpdatePhase(runID, "finalizing")
	if err := o.targetMode.Finalize(ctx, successTables); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("finalizing: %w", err)
	}

	// Validate (only for successful tables)
	o.setPhase("validating")
	logging.Debug("Validating...")
	o.state.UpdatePhase(runID, "validating")
	o.tables = successTables // Update for validation
	if err := o.Validate(ctx); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	// Sample validation if enabled
	if o.config.Migration.SampleValidation {
		logging.Debug("Running sample validation...")
		if err := o.validateSamples(ctx); err != nil {
			logging.Warn("Warning: sample validation failed: %v", err)
		}
	}

	// Calculate stats for notification
	duration := time.Since(startTime)
	var totalRows int64
	for _, t := range successTables {
		totalRows += t.RowCount
	}
	throughput := float64(totalRows) / duration.Seconds()

	// Determine final status and send appropriate notification
	partialErr := false
	if len(tableFailures) > 0 {
		// Partial success
		failureNames := make([]string, len(tableFailures))
		for i, f := range tableFailures {
			failureNames[i] = f.TableName
		}
		o.state.CompleteRun(runID, "partial", fmt.Sprintf("%d tables failed", len(tableFailures)))
		o.notifier.MigrationCompletedWithErrors(runID, startTime, duration,
			len(successTables), len(tableFailures), totalRows, throughput, failureNames)
		logging.Warn("Migration completed with errors: %d tables succeeded, %d tables failed, %d rows in %s (%.0f rows/sec)",
			len(successTables), len(tableFailures), totalRows, duration.Round(time.Second), throughput)
		partialErr = !o.config.Migration.AllowPartial
	} else {
		// Full success
		o.state.CompleteRun(runID, "success", "")
		o.notifier.MigrationCompleted(runID, startTime, duration, len(tables), totalRows, throughput)
		logging.Info("Migration complete: %d tables, %d rows in %s (%.0f rows/sec)",
			len(tables), totalRows, duration.Round(time.Second), throughput)
		o.auditEvent("validation_complete", map[string]any{
			"tables":     len(tables),
			"rows_total": totalRows,
		})
	}

	// Record transfer-only throughput in AI tuning history for future learning
	// transferDuration captured right after transferAll returns (excludes schema
	// extraction, DDL creation, finalization, and validation)
	transferDurationSecs := transferDuration.Seconds()
	var transferThroughput float64
	if transferDurationSecs > 0 {
		transferThroughput = float64(totalRows) / transferDurationSecs
	}
	if err := o.state.UpdateAITuningResult(transferThroughput, transferDurationSecs, o.lastChunkRetryCount); err != nil {
		logging.Debug("Failed to update AI tuning result: %v", err)
	}

	// Log identifier changes for PostgreSQL targets
	if o.config.Target.Type == "postgres" {
		o.logPGIdentifierChanges(tables)
	}

	if partialErr {
		return &PartialMigrationError{Failed: tableFailures}
	}
	return nil
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

// notifyFailure sends a failure notification
func (o *Orchestrator) notifyFailure(runID string, err error, duration time.Duration) {
	o.notifier.MigrationFailed(runID, err, duration)
}

// markTableComplete marks a table transfer task as complete
func (o *Orchestrator) markTableComplete(runID, taskKey string) {
	o.state.MarkTaskComplete(runID, taskKey)
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

func (o *Orchestrator) transferAll(ctx context.Context, runID string, tables []source.Table, resume bool) ([]TableFailure, error) {
	// Build jobs using JobBuilder
	builder := NewJobBuilder(o.sourcePool, o.state, o.config)
	buildResult, err := builder.Build(ctx, runID, tables)
	if err != nil {
		return nil, fmt.Errorf("building jobs: %w", err)
	}

	// Execute jobs using TransferRunner. Error diagnosis runs through the
	// deterministic catalog in internal/driver/errordiag (#173); the
	// former AI-driven diagnoser was removed to avoid sending error
	// messages (which routinely contain row data) to a third-party LLM.
	runner := NewTransferRunner(
		o.sourcePool,
		o.targetPool,
		o.state,
		o.config,
		o.progress,
		o.notifier,
		o.targetMode,
	)

	result, err := runner.Run(ctx, runID, buildResult, tables, resume)
	if err != nil {
		return nil, err
	}

	// Stash chunk retry count so the orchestrator can persist it with the run's
	// final tuning result. Read by the UpdateAITuningResult call sites in Run/Resume.
	o.lastChunkRetryCount = result.ChunkRetryCount

	return result.TableFailures, nil
}

// applyAITuning runs the deterministic tuner (internal/tuning) to set
// migration parameters before transfer begins. Only overrides
// formula-computed values (where Original* == 0), never user-specified
// values. Function name kept for now; rename pending in a follow-up
// cleanup (#175 PR1 minimized public-API churn). Falls back to baseline
// on any failure (logged at Debug/Warn level).
func (o *Orchestrator) applyAITuning(ctx context.Context) {
	logging.Debug("Running parameter tuning...")

	// Create analyzer (same pattern as AnalyzeConfig in healthcheck.go).
	analyzer := driver.NewSmartConfigAnalyzer(o.sourcePool.DB(), o.sourcePool.DBType())

	// Set up history provider for learning from past runs
	if o.state != nil {
		analyzer.SetHistoryProvider(&stateHistoryAdapter{state: o.state})
	}

	// Set target DB type and migration mode for cross-engine awareness
	if o.targetPool != nil {
		analyzer.SetTargetDBType(o.targetPool.DBType())

		// Probe target for runtime values that affect chunk_size
		// selection (#166). MySQL surfaces @@max_allowed_packet here
		// so the tuner's HardChunkLimit reflects the live cap rather
		// than the static 0; without this the migration path could
		// pick a chunk_size that exceeds the packet limit and crash
		// mid-transfer. PG/MSSQL return empty probes.
		if td, err := driver.Get(o.targetPool.DBType()); err == nil {
			probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
			analyzer.SetTargetProbe(td.ProbeTarget(probeCtx, o.targetPool.DB()))
			probeCancel()
		}
	}
	analyzer.SetTargetMode(o.config.Migration.TargetMode)

	// Capture the probe-derived hard cap so transfer_runner can carry it
	// into the runtime controller. Without this, runtime growth rules can
	// push chunk_size above the packet limit and crash mid-migration
	// (Codex review on #166). Set after Analyze runs (below) so the
	// uncapped row size is populated.
	defer func() {
		if o.targetPool != nil {
			o.config.Migration.TargetHardChunkLimit = analyzer.TargetHardChunkLimit()
		}
	}()

	// Pass user-configured memory cap
	if o.config.Migration.MaxMemoryMB > 0 {
		analyzer.SetMaxMemoryMB(o.config.Migration.MaxMemoryMB)
	}

	// Wire exploration policy (#179): --explore flag forces a planned-grid
	// pick this run; ExploreMode controls steady-state ε strength.
	analyzer.SetExploration(o.config.Migration.Explore, o.config.Migration.ExploreMode)

	// Wire workload identity (#215). Together these form the tuple the
	// Tier 1 exact-identity classifier uses to find historically-
	// comparable runs. Values come verbatim from cfg.Source / cfg.Target —
	// the user wrote them, the user understands them. Stored without
	// normalization so the SQL equality match is what the user expects.
	analyzer.SetWorkloadIdentity(
		o.config.Source.Host, o.config.Source.Port,
		o.config.Source.Database, o.config.Source.Schema,
		o.config.Target.Host, o.config.Target.Port,
		o.config.Target.Database, o.config.Target.Schema,
	)

	// Capture effective DB tuning at run start (#144). Used for both:
	//   (a) smartconfig prompt regime classification (per trajectory row)
	//   (b) persisting on the saved AITuningRecord so future runs can
	//       classify against THIS run.
	// Best-effort: failures yield zero-valued fields, which the render path
	// and classifier treat as "unknown".
	tuning := captureDBTuning(ctx, o.sourcePool.DB(), o.targetPool.DB(),
		o.config.Source.Type, o.config.Target.Type)
	analyzer.SetCurrentTuning(driver.DBTuningSnapshot{
		TargetSharedBuffersMB:   tuning.TargetSharedBuffersMB,
		TargetSyncCommit:        tuning.TargetSyncCommit,
		TargetFsync:             tuning.TargetFsync,
		TargetFullPageWrites:    tuning.TargetFullPageWrites,
		TargetMaxWALSizeMB:      tuning.TargetMaxWALSizeMB,
		TargetWALLevel:          tuning.TargetWALLevel,
		SourceMaxServerMemoryMB: tuning.SourceMaxServerMemoryMB,
	})

	// Deterministic tuning is fast (no network round-trip). 30s is generous
	// for the SQL probing inside Analyze (getTables + per-table date-column
	// detection); leave headroom for slow source DBs.
	analyzeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	suggestions, err := analyzer.Analyze(analyzeCtx, o.config.Source.Schema)
	if err != nil {
		logging.Warn("Tuning analysis failed, using formula defaults: %v", err)
		return
	}

	// Apply suggestions only where user didn't specify values
	changes := o.config.ApplyAISuggestions(suggestions)
	if len(changes) > 0 {
		logging.Info("Tuning applied %d parameter(s):", len(changes))
		for _, c := range changes {
			logging.Info("  %s: %d -> %d", c.Name, c.OldValue, c.NewValue)
		}
	} else {
		logging.Info("Tuning: no changes (recommendation matches current config)")
	}
	// Always log the tuner's reasoning. The deterministic tuner
	// guarantees a non-empty Reasoning + Tier on every run (#202 — silence
	// hid which tier picked when the value matched the baseline anchor).
	// sanitizeForLog guards against multi-line content breaking log parsers.
	logging.Info("Tuning reasoning [%s]: %s",
		suggestions.Tier, sanitizeForLog(suggestions.Reasoning))

	// Save tuning history with actual params used (after user overrides).
	// `tuning` was captured earlier so the smartconfig prompt could classify
	// against it; same snapshot is persisted here so future runs see this
	// run's effective tuning when classifying their own trajectory rows.
	analyzer.SaveTuningWithActualParams(driver.ActualParams{
		Workers:           o.config.Migration.Workers,
		ChunkSize:         o.config.Migration.ChunkSize,
		ReadAheadBuffers:  o.config.Migration.ReadAheadBuffers,
		WriteAheadWriters: o.config.Migration.WriteAheadWriters,
		ParallelReaders:   o.config.Migration.ParallelReaders,
		MaxPartitions:     o.config.Migration.MaxPartitions,
		// #144 regime fields. Platform comes from gopsutil-detected runtime
		// (already in AutoTuneInput); the DB-tuning fields come from
		// captureDBTuning above.
		TargetSharedBuffersMB:   tuning.TargetSharedBuffersMB,
		TargetSyncCommit:        tuning.TargetSyncCommit,
		TargetFsync:             tuning.TargetFsync,
		TargetFullPageWrites:    tuning.TargetFullPageWrites,
		TargetMaxWALSizeMB:      tuning.TargetMaxWALSizeMB,
		TargetWALLevel:          tuning.TargetWALLevel,
		SourceMaxServerMemoryMB: tuning.SourceMaxServerMemoryMB,
	})
}

// Resume continues an interrupted migration
func (o *Orchestrator) Resume(ctx context.Context) (resumeErr error) {
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return fmt.Errorf("finding incomplete run: %w", err)
	}
	if run == nil {
		return fmt.Errorf("no incomplete run found - use 'run' to start a new migration")
	}

	// Check if this incomplete run has been superseded by a later successful run
	superseded, err := o.state.HasSuccessfulRunAfter(run)
	if err != nil {
		return fmt.Errorf("checking for superseding runs: %w", err)
	}
	if superseded {
		// Mark the old incomplete run as failed since it's obsolete
		o.state.CompleteRun(run.ID, "failed", "superseded by later successful migration")
		return fmt.Errorf("incomplete run %s is obsolete - a later migration with the same schemas completed successfully. Use 'run' to start a new migration", run.ID)
	}

	// Validate config hash if stored (prevents resuming with different config)
	if run.ConfigHash != "" && !o.opts.ForceResume {
		currentHash := computeConfigHash(o.config)
		if run.ConfigHash != currentHash {
			return fmt.Errorf("config changed since run started (hash %s != %s), use --force-resume to override",
				run.ConfigHash, currentHash)
		}
	}

	startTime := time.Now()

	// #229 observability: same per-run attribute decoration as Run(),
	// including the initial "starting" phase so early log lines carry
	// a phase attr (Copilot review). resume=true differentiates the
	// two flows in log queries.
	logging.WithFields(map[string]any{
		"run_id":    run.ID,
		"source_db": o.sourcePool.DBType(),
		"target_db": o.targetPool.DBType(),
		"resume":    true,
		"phase":     "starting",
	})
	o.metrics.RunStarted(run.ID, o.sourcePool.DBType(), o.targetPool.DBType())
	defer logging.ClearBaseAttrs()
	defer o.metrics.RunComplete(run.ID)

	// One root span per Resume(), same shape as Run() but with resume=true.
	runCtx, runSpan := observability.Tracer().StartSpan(ctx, "dmt.resume",
		"run_id", run.ID, "source_db", o.sourcePool.DBType(), "target_db", o.targetPool.DBType(), "resume", true)
	o.traceCtx = runCtx
	defer func() {
		o.endPhaseSpan()
		runSpan.End()
		o.traceCtx = nil
	}()

	// #235 audit log: resume runs append to the same audit file the
	// original Run() opened (same run_id). The audit-dir code path is
	// idempotent — if the file is 0444 from a Close() in the earlier
	// crash, OpenFile fails and the auditor degrades to disabled. We
	// log a warning but don't fail the resume — compliance is best-
	// effort here; the original Run()'s record remains intact.
	o.openAuditor(run.ID, true /*resume*/)
	defer func() {
		rec := recover()
		status, errStr, resumable := classifyRunOutcome(resumeErr, rec)
		o.auditEvent("resume_complete", map[string]any{
			"status":      status,
			"error":       errStr,
			"duration_ms": time.Since(startTime).Milliseconds(),
		})
		if resumable {
			if err := o.auditor.CloseResumable(); err != nil {
				logging.Warn("audit close: %v", err)
			}
		} else {
			if err := o.auditor.Close(); err != nil {
				logging.Warn("audit close: %v", err)
			}
		}
		if rec != nil {
			panic(rec)
		}
	}()
	o.auditEvent("resume_start", map[string]any{
		"operator":            operatorLabel(),
		"dmt_version":         versionString(),
		"original_started_at": run.StartedAt.UTC().Format(time.RFC3339),
	})

	logging.Info("Resuming run: %s (started %s)", run.ID, run.StartedAt.Format(time.RFC3339))

	// Preflight (phase 0) — same gate as Run(). A resume can fail if the
	// environment changed between runs (privileges revoked, version
	// downgraded, target unreachable). Operator can opt out per-check with
	// --skip-preflight if they know what they're doing. #228.
	o.setPhase("preflight")
	logging.Debug("Running preflight checks...")
	if err := o.runPreFlight(ctx); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return err
	}

	// Reset any running tasks to pending
	if err := o.state.MarkRunAsResumed(run.ID); err != nil {
		return fmt.Errorf("resetting tasks: %w", err)
	}

	// Extract schema (needed to know all tables)
	logging.Debug("Extracting schema...")
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("extracting schema: %w", err)
	}

	// Apply table filters
	tables = o.filterTables(tables)
	if len(tables) == 0 {
		o.state.CompleteRun(run.ID, "failed", "no tables to migrate after applying filters")
		return fmt.Errorf("no tables to migrate after applying filters")
	}

	o.tables = tables
	logging.Debug("Found %d tables in source", len(tables))

	// Apply AI-recommended parameters (if AI is available)
	o.applyAITuning(ctx)

	// Persist the post-tuning config so `dmt history --run <id>` reflects what actually ran.
	if err := o.state.UpdateRunConfig(run.ID, o.config.Sanitized()); err != nil {
		logging.Warn("failed to persist post-tuning config: %v", err)
	}

	// Get tables that were successfully transferred in the previous run
	completedTables, err := o.state.GetCompletedTables(run.ID)
	if err != nil {
		return fmt.Errorf("getting completed tables: %w", err)
	}

	// Check target row counts to determine which tables need re-transfer
	var tablesToTransfer []source.Table
	var skippedTables []string

	for _, t := range tables {
		// Check if table was marked complete AND has correct row count
		taskKey := fmt.Sprintf("transfer:%s.%s", t.Schema, t.Name)
		if completedTables[taskKey] {
			// Verify row count matches
			targetCount, err := o.targetPool.GetRowCount(ctx, o.config.Target.Schema, t.Name)
			if err == nil && targetCount == t.RowCount {
				skippedTables = append(skippedTables, t.Name)
				continue
			}
		}
		tablesToTransfer = append(tablesToTransfer, t)
	}

	if len(skippedTables) > 0 {
		logging.Debug("Skipping %d already-complete tables: %v", len(skippedTables), skippedTables)
	}

	if len(tablesToTransfer) == 0 {
		logging.Info("All tables already transferred - completing migration")
		o.tables = tables // Use all tables for finalize/validate

		// Finalize
		o.setPhase("finalizing")
		logging.Debug("Finalizing...")
		if err := o.targetMode.Finalize(ctx, tables); err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return fmt.Errorf("finalizing: %w", err)
		}

		// Validate
		o.setPhase("validating")
		logging.Debug("Validating...")
		if err := o.Validate(ctx); err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return err
		}

		o.state.CompleteRun(run.ID, "success", "")
		duration := time.Since(startTime)
		if err := o.state.UpdateAITuningResult(0, duration.Seconds(), o.lastChunkRetryCount); err != nil {
			logging.Debug("Failed to update AI tuning result: %v", err)
		}
		logging.Info("Resume complete!")
		return nil
	}

	logging.Debug("Resuming transfer of %d tables", len(tablesToTransfer))

	// For tables that need transfer, ensure target tables exist
	// Check for chunk-level progress to avoid unnecessary truncation
	progressSaver := checkpoint.NewProgressSaver(o.state)
	for _, t := range tablesToTransfer {
		taskKey := fmt.Sprintf("transfer:%s.%s", t.Schema, t.Name)
		taskID, _ := o.state.CreateTask(run.ID, "transfer", taskKey)

		exists, err := o.targetPool.TableExists(ctx, o.config.Target.Schema, t.Name)
		if err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			return fmt.Errorf("checking table %s: %w", t.Name, err)
		}
		if !exists {
			// Table doesn't exist - clear any stale progress before creating it.
			// If cleanup fails, leaving the target missing is safer than creating
			// an empty target beside stale partition checkpoints (#266).
			if err := o.clearResumeProgress(run.ID, taskKey, taskID, t.Name); err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return err
			}
			if err := o.targetPool.CreateTable(ctx, &t, o.config.Target.Schema); err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return fmt.Errorf("creating table %s: %w", t.Name, err)
			}
			// Idempotent-INSERT-on-resume depends on the PK existing on the
			// target. AI-generated CREATE TABLE DDL usually includes the PK
			// inline, but CreatePrimaryKey is idempotent (no-op if PK exists)
			// so call it defensively when re-creating a missing table on resume.
			if err := o.targetPool.CreatePrimaryKey(ctx, &t, o.config.Target.Schema); err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return fmt.Errorf("ensuring PK on %s: %w", t.Name, err)
			}
		} else {
			// Table exists - check if we have saved chunk progress
			lastPK, rowsDone, err := progressSaver.GetProgress(taskID)
			if err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return fmt.Errorf("getting progress for %s: %w", t.Name, err)
			}

			// Match the partitioning decision made in job_builder.go:
			// large + HasPK is partitioned (keyset for single-int PK,
			// ROW_NUMBER otherwise).
			isPartitioned := t.IsLarge(o.config.Migration.LargeTableThreshold) && t.HasPK()
			expectedRows, hasProgress, err := o.expectedResumeRows(
				run.ID, taskKey, isPartitioned, lastPK, rowsDone,
			)
			if err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return err
			}

			if !hasProgress {
				if isPartitioned {
					continue
				}
				// No chunk progress - truncate to ensure clean re-transfer.
				if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, t.Name); err != nil {
					o.state.CompleteRun(run.ID, "failed", err.Error())
					return fmt.Errorf("truncating table %s: %w", t.Name, err)
				}
				continue
			}

			// Have saved progress - verify target row count matches it.
			// If target has fewer rows than saved progress, data was lost;
			// clear all table + partition progress and start fresh.
			targetCount, err := o.targetPool.GetRowCount(ctx, o.config.Target.Schema, t.Name)
			if err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return fmt.Errorf("getting row count for %s: %w", t.Name, err)
			}
			if targetCount < expectedRows {
				logging.Warn("  Warning: %s has %d rows but expected %d - restarting transfer",
					t.Name, targetCount, expectedRows)
				if err := o.clearResumeProgress(run.ID, taskKey, taskID, t.Name); err != nil {
					o.state.CompleteRun(run.ID, "failed", err.Error())
					return err
				}
				if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, t.Name); err != nil {
					o.state.CompleteRun(run.ID, "failed", err.Error())
					return fmt.Errorf("truncating table %s: %w", t.Name, err)
				}
			}
			// If target has >= expectedRows, resume from saved progress.
		}
	}

	// Transfer only the incomplete tables
	o.setPhase("transfer")
	logging.Debug("Transferring data...")
	transferStart := time.Now()
	tableFailures, err := o.transferAll(ctx, run.ID, tablesToTransfer, true)
	transferDuration := time.Since(transferStart)
	if err != nil {
		// If context was canceled (Ctrl+C), leave run as "running" so resume works
		// but reset any "running" tasks to "pending" so status shows correctly
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			o.state.MarkRunAsResumed(run.ID) // Reset running tasks to pending
			logging.Info("Migration interrupted - run 'resume' to continue")
			return fmt.Errorf("transferring data: %w", err)
		}
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("transferring data: %w", err)
	}

	// Log summary of table failures (individual failures already logged)
	if len(tableFailures) > 0 {
		logging.Warn("%d table(s) failed during transfer:", len(tableFailures))
		for _, f := range tableFailures {
			logging.Warn("  - %s: %v", f.TableName, f.Error)
		}
	}

	// Filter out failed tables for finalize/validate
	failedTableNames := make(map[string]bool)
	for _, f := range tableFailures {
		failedTableNames[f.TableName] = true
	}
	var successTables []source.Table
	for _, t := range tables {
		if !failedTableNames[t.Name] {
			successTables = append(successTables, t)
		}
	}

	// Finalize (uses successful tables for constraints)
	o.tables = successTables
	o.setPhase("finalizing")
	logging.Debug("Finalizing...")
	if err := o.targetMode.Finalize(ctx, successTables); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("finalizing: %w", err)
	}

	// Validate successful tables
	o.setPhase("validating")
	logging.Debug("Validating...")
	if err := o.Validate(ctx); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return err
	}

	// Sample validation if enabled
	if o.config.Migration.SampleValidation {
		logging.Debug("Running sample validation...")
		if err := o.validateSamples(ctx); err != nil {
			logging.Warn("Warning: sample validation failed: %v", err)
		}
	}

	// Calculate stats - only count rows from tables we attempted to transfer that succeeded
	duration := time.Since(startTime)
	var totalRows int64
	for _, t := range tablesToTransfer {
		if !failedTableNames[t.Name] {
			totalRows += t.RowCount
		}
	}
	throughput := float64(totalRows) / duration.Seconds()

	// Determine final status and send appropriate notification
	successCount := 0
	for _, t := range tablesToTransfer {
		if !failedTableNames[t.Name] {
			successCount++
		}
	}

	partialErr := false
	if len(tableFailures) > 0 {
		// Partial success
		failureNames := make([]string, len(tableFailures))
		for i, f := range tableFailures {
			failureNames[i] = f.TableName
		}
		o.state.CompleteRun(run.ID, "partial", fmt.Sprintf("%d tables failed", len(tableFailures)))
		o.notifier.MigrationCompletedWithErrors(run.ID, startTime, duration,
			successCount, len(tableFailures), totalRows, throughput, failureNames)
		logging.Warn("Resume completed with errors: %d tables succeeded, %d tables failed, %d rows in %s (%.0f rows/sec)",
			successCount, len(tableFailures), totalRows, duration.Round(time.Second), throughput)
		partialErr = !o.config.Migration.AllowPartial
	} else {
		// Full success
		o.state.CompleteRun(run.ID, "success", "")
		o.notifier.MigrationCompleted(run.ID, startTime, duration, len(tablesToTransfer), totalRows, throughput)
		logging.Info("Resume complete: %d tables, %d rows in %s (%.0f rows/sec)",
			len(tablesToTransfer), totalRows, duration.Round(time.Second), throughput)
	}

	// Record transfer-only throughput in AI tuning history for future learning
	// transferDuration captured right after transferAll returns
	transferDurationSecs := transferDuration.Seconds()
	var transferThroughput float64
	if transferDurationSecs > 0 {
		transferThroughput = float64(totalRows) / transferDurationSecs
	}
	if err := o.state.UpdateAITuningResult(transferThroughput, transferDurationSecs, o.lastChunkRetryCount); err != nil {
		logging.Debug("Failed to update AI tuning result: %v", err)
	}

	// Log identifier changes for PostgreSQL targets
	if o.config.Target.Type == "postgres" {
		o.logPGIdentifierChanges(tablesToTransfer)
	}

	if partialErr {
		return &PartialMigrationError{Failed: tableFailures}
	}
	return nil
}

func (o *Orchestrator) clearResumeProgress(runID, taskKey string, taskID int64, tableName string) error {
	if err := o.state.ClearPartitionTransferProgress(runID, taskKey); err != nil {
		return fmt.Errorf("clearing partition progress for %s: %w", tableName, err)
	}
	if err := o.state.ClearTransferProgress(taskID); err != nil {
		return fmt.Errorf("clearing transfer progress for %s: %w", tableName, err)
	}
	return nil
}

func (o *Orchestrator) expectedResumeRows(
	runID, taskKey string,
	isPartitioned bool,
	tableLastPK any,
	tableRowsDone int64,
) (int64, bool, error) {
	expectedRows := tableRowsDone
	hasProgress := tableLastPK != nil

	if !isPartitioned {
		return expectedRows, hasProgress, nil
	}

	summary, err := o.state.GetPartitionTransferProgressSummary(runID, taskKey)
	if err != nil {
		return 0, false, fmt.Errorf("getting partition progress for %s: %w", taskKey, err)
	}
	if !summary.HasProgress() {
		return expectedRows, hasProgress, nil
	}
	if !hasProgress || summary.RowsDone > expectedRows {
		expectedRows = summary.RowsDone
	}
	return expectedRows, true, nil
}

// sanitizeForLog flattens AI-supplied strings to a single line before logging.
// Newlines, carriage returns, and tabs become spaces; runs of whitespace
// collapse. Without this, multi-paragraph reasoning from the model would
// produce log entries where the second+ lines lack timestamps/levels and
// break log parsers (issue #143 / PR #151 review).
func sanitizeForLog(s string) string {
	r := strings.NewReplacer("\n", " ", "\r", " ", "\t", " ")
	flat := r.Replace(s)
	// Collapse repeated spaces.
	for strings.Contains(flat, "  ") {
		flat = strings.ReplaceAll(flat, "  ", " ")
	}
	return strings.TrimSpace(flat)
}
