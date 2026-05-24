package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/audit"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/notify"
	"github.com/johndauphine/dmt/internal/observability"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/source"
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

	// deleteReconciliationStrictValidation is set when the current run has
	// completed an eligible delete reconciliation pass. Upsert validation can
	// then require source/target row-count parity instead of allowing extra
	// target rows that reconciliation should have removed.
	deleteReconciliationStrictValidation bool

	// auditor writes the immutable per-run NDJSON record (#235). Always
	// non-nil — audit.Disabled() satisfies the interface when --no-audit
	// is set so call sites stay unconditional. Initialized in Run/Resume
	// once the run_id is known.
	auditor *audit.Logger

	schemaContractDecisionRunID string
	lastSchemaContractDecisions []SchemaContractDecision
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

	// RunHeartbeatTTL controls how old a running run heartbeat may be before
	// resume requires --force-resume. Zero uses the default.
	RunHeartbeatTTL time.Duration

	// RunHeartbeatInterval controls how often Run/Resume refresh the heartbeat.
	// Zero uses the default.
	RunHeartbeatInterval time.Duration

	// SourceOnly creates orchestrator with only source pool (for analyze command).
	// When true, target pool is not created and analyze operations only work.
	SourceOnly bool

	// AIReviewClientFactory overrides the default AI provider lookup for
	// the preflight advisory path. Production leaves this nil; tests
	// inject a fake factory so success/error/unavailable behavior is
	// covered without touching global secrets or provider singletons.
	AIReviewClientFactory func() aicopilot.TextClient
}

// SchemaContractDecision describes how one schema drift change was handled by
// the DLT-style schema contract policy.
type SchemaContractDecision = schemaContractDecision

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
	RunID                   string                       `json:"run_id"`
	Status                  string                       `json:"status"`
	StartedAt               time.Time                    `json:"started_at"`
	CompletedAt             time.Time                    `json:"completed_at"`
	DurationSeconds         float64                      `json:"duration_seconds"`
	TablesTotal             int                          `json:"tables_total"`
	TablesSuccess           int                          `json:"tables_success"`
	TablesFailed            int                          `json:"tables_failed"`
	RowsTransferred         int64                        `json:"rows_transferred"`
	RowsPerSecond           int64                        `json:"rows_per_second"`
	FailedTables            []string                     `json:"failed_tables"`
	TableStats              []TableResult                `json:"table_stats"`
	DeleteReconciliation    *DeleteReconciliationSummary `json:"delete_reconciliation,omitempty"`
	SchemaContractDecisions []SchemaContractDecision     `json:"schema_contract_decisions,omitempty"`
	Error                   string                       `json:"error,omitempty"`
}

// TableResult contains the outcome for a single table.
type TableResult struct {
	Name   string `json:"name"`
	Rows   int64  `json:"rows"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// DeleteReconciliationSummary contains per-run hard-delete reconciliation
// counts.
type DeleteReconciliationSummary struct {
	CandidateRows int64                              `json:"candidate_rows"`
	DeletedRows   int64                              `json:"deleted_rows"`
	Tables        []DeleteReconciliationTableSummary `json:"tables"`
}

// DeleteReconciliationTableSummary contains one table's reconciliation counts.
type DeleteReconciliationTableSummary struct {
	Table         string `json:"table"`
	CandidateRows int64  `json:"candidate_rows"`
	DeletedRows   int64  `json:"deleted_rows"`
	Skipped       bool   `json:"skipped,omitempty"`
	SkipReason    string `json:"skip_reason,omitempty"`
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

	// AIFallbacks reports per-surface AI fallback counts for the run
	// referenced by RunID (#176). Keys are observability.Surface* values
	// (typemap | ddl | errordiag). Omitted from JSON when empty so the
	// no-fallback case stays clean. Source is the checkpoint backend
	// (SQLite or YAML/FileState), so a separate-process `dmt status`
	// poll sees the running migration's counts. Rows persist until
	// CleanupOldRuns purges them with the rest of the run-scoped state.
	AIFallbacks             map[string]int64         `json:"ai_fallbacks,omitempty"`
	SchemaContractDecisions []SchemaContractDecision `json:"schema_contract_decisions,omitempty"`
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
	AIPreflightReview *aicopilot.PreflightReview `json:"ai_preflight_review,omitempty"`
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
	// EstimatedDurationSeconds is populated when recent same-direction
	// throughput history is available.
	EstimatedDurationSeconds float64                      `json:"estimated_duration_seconds,omitempty"`
	EstimatedRowsPerSecond   int64                        `json:"estimated_rows_per_second,omitempty"`
	Workers                  int                          `json:"workers"`
	ChunkSize                int                          `json:"chunk_size"`
	TargetMode               string                       `json:"target_mode"`
	DeleteReconciliation     *DeleteReconciliationPreview `json:"delete_reconciliation,omitempty"`
	SchemaContractDecisions  []SchemaContractDecision     `json:"schema_contract_decisions,omitempty"`
}

// DeleteReconciliationPreview describes whether the opt-in #351 delete
// reconciliation pass would run for this migration.
type DeleteReconciliationPreview struct {
	Enabled           bool                               `json:"enabled"`
	Due               bool                               `json:"due"`
	Reason            string                             `json:"reason,omitempty"`
	Interval          string                             `json:"interval,omitempty"`
	LastSuccessAt     *time.Time                         `json:"last_success_at,omitempty"`
	NextDueAt         *time.Time                         `json:"next_due_at,omitempty"`
	EligibleTables    int                                `json:"eligible_tables"`
	SkippedNoPKTables int                                `json:"skipped_no_pk_tables"`
	CandidateRows     *int64                             `json:"candidate_rows,omitempty"`
	Tables            []DeleteReconciliationTablePreview `json:"tables,omitempty"`
}

// DeleteReconciliationTablePreview describes one dry-run delete candidate
// count. Error is set when the non-mutating key scan could not complete.
type DeleteReconciliationTablePreview struct {
	Table         string `json:"table"`
	CandidateRows int64  `json:"candidate_rows"`
	Skipped       bool   `json:"skipped,omitempty"`
	SkipReason    string `json:"skip_reason,omitempty"`
	Error         string `json:"error,omitempty"`
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
