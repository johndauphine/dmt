package orchestrator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/johndauphine/mssql-pg-migrate/internal/checkpoint"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
	"github.com/johndauphine/mssql-pg-migrate/internal/notify"
	"github.com/johndauphine/mssql-pg-migrate/internal/pool"
	"github.com/johndauphine/mssql-pg-migrate/internal/progress"
	"github.com/johndauphine/mssql-pg-migrate/internal/source"
	"github.com/johndauphine/mssql-pg-migrate/internal/target"
	"github.com/johndauphine/mssql-pg-migrate/internal/transfer"
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

// Orchestrator coordinates the migration process
type Orchestrator struct {
	config     *config.Config
	sourcePool pool.SourcePool
	targetPool pool.TargetPool
	state      checkpoint.StateBackend
	progress   *progress.Tracker
	notifier   *notify.Notifier
	tables     []source.Table
	runProfile string
	runConfig  string
	opts       Options
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
}

// computeConfigHash returns a short hex hash of the sanitized config.
func computeConfigHash(cfg *config.Config) string {
	configJSON, _ := json.Marshal(cfg.Sanitized())
	hash := sha256.Sum256(configJSON)
	return hex.EncodeToString(hash[:8])
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

// HealthCheckResult contains connection health information.
type HealthCheckResult struct {
	Timestamp        string `json:"timestamp"`
	SourceConnected  bool   `json:"source_connected"`
	SourceLatencyMs  int64  `json:"source_latency_ms"`
	SourceDBType     string `json:"source_db_type"`
	SourceTableCount int    `json:"source_table_count,omitempty"`
	SourceError      string `json:"source_error,omitempty"`
	TargetConnected  bool   `json:"target_connected"`
	TargetLatencyMs  int64  `json:"target_latency_ms"`
	TargetDBType     string `json:"target_db_type"`
	TargetError      string `json:"target_error,omitempty"`
	Healthy          bool   `json:"healthy"`
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
	// Determine max connections based on source/target types
	maxSourceConns := cfg.Migration.MaxMssqlConnections
	maxTargetConns := cfg.Migration.MaxPgConnections
	if cfg.Source.Type == "postgres" {
		maxSourceConns = cfg.Migration.MaxPgConnections
	}
	if cfg.Target.Type == "mssql" {
		maxTargetConns = cfg.Migration.MaxMssqlConnections
	}

	// Create source pool using factory
	sourcePool, err := pool.NewSourcePool(&cfg.Source, maxSourceConns)
	if err != nil {
		return nil, fmt.Errorf("creating source pool: %w", err)
	}

	// Create target pool using factory
	targetPool, err := pool.NewTargetPool(&cfg.Target, maxTargetConns, cfg.Migration.MSSQLRowsPerBatch, cfg.Source.Type)
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
		state, err = checkpoint.New(cfg.Migration.DataDir)
		if err != nil {
			sourcePool.Close()
			targetPool.Close()
			return nil, fmt.Errorf("creating state manager: %w", err)
		}
	}

	// Create notifier
	notifier := notify.New(&cfg.Slack)

	return &Orchestrator{
		config:     cfg,
		sourcePool: sourcePool,
		targetPool: targetPool,
		state:      state,
		progress:   progress.New(),
		notifier:   notifier,
		opts:       opts,
	}, nil
}

// Close releases all resources
func (o *Orchestrator) Close() {
	o.sourcePool.Close()
	o.targetPool.Close()
	o.state.Close()
}

// SetRunContext sets metadata for the current run (profile name or config path).
func (o *Orchestrator) SetRunContext(profileName, configPath string) {
	o.runProfile = profileName
	o.runConfig = configPath
}

// Run executes a new migration.
func (o *Orchestrator) Run(ctx context.Context) error {
	// Use provided run ID or generate a new one
	runID := o.opts.RunID
	if runID == "" {
		runID = uuid.New().String()[:8]
	}
	startTime := time.Now()
	logging.Info("Starting migration run: %s", runID)
	logging.Info("Migration: %s -> %s", o.sourcePool.DBType(), o.targetPool.DBType())

	// Log comprehensive configuration dump (always visible at INFO level)
	logging.Info("%s", o.config.DebugDump())

	// Set runtime memory limit using Go's soft limit mechanism
	// This tells the GC to work harder to stay under the limit
	effectiveMemMB := o.config.AutoConfig().EffectiveMaxMemoryMB
	if effectiveMemMB > 0 {
		memLimitBytes := effectiveMemMB * 1024 * 1024
		debug.SetMemoryLimit(memLimitBytes)
		logging.Info("Runtime memory limit set to %d MB (Go GC soft limit)", effectiveMemMB)
	}

	if err := o.state.CreateRun(runID, o.config.Source.Schema, o.config.Target.Schema, o.config.Sanitized(), o.runProfile, o.runConfig); err != nil {
		return fmt.Errorf("creating run: %w", err)
	}

	// Extract schema
	logging.Info("Extracting schema...")
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
	logging.Info("Found %d tables", len(tables))

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
	if err := o.targetPool.CreateSchema(ctx, o.config.Target.Schema); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("creating schema: %w", err)
	}

	if o.config.Migration.TargetMode == "upsert" {
		logging.Info("Preparing target tables (upsert mode)...")
		for i, t := range tables {
			logging.Debug("  [%d/%d] %s", i+1, len(tables), t.Name)
			// Validate source table has PK for upsert (required for ON CONFLICT / MERGE)
			if !t.HasPK() {
				o.state.CompleteRun(runID, "failed", fmt.Sprintf("table %s has no primary key", t.Name))
				err := fmt.Errorf("table %s has no primary key - upsert mode requires primary keys", t.Name)
				o.notifyFailure(runID, err, time.Since(startTime))
				return err
			}
			exists, err := o.targetPool.TableExists(ctx, o.config.Target.Schema, t.Name)
			if err != nil {
				o.state.CompleteRun(runID, "failed", err.Error())
				o.notifyFailure(runID, err, time.Since(startTime))
				return fmt.Errorf("checking if table %s exists: %w", t.Name, err)
			}
			if !exists {
				// Create table if it doesn't exist
				if err := o.targetPool.CreateTable(ctx, &t, o.config.Target.Schema); err != nil {
					o.state.CompleteRun(runID, "failed", err.Error())
					o.notifyFailure(runID, err, time.Since(startTime))
					return fmt.Errorf("creating table %s: %w", t.FullName(), err)
				}
				// Create PK immediately for upsert mode (needed for ON CONFLICT)
				if err := o.targetPool.CreatePrimaryKey(ctx, &t, o.config.Target.Schema); err != nil {
					o.state.CompleteRun(runID, "failed", err.Error())
					o.notifyFailure(runID, err, time.Since(startTime))
					return fmt.Errorf("creating primary key for %s: %w", t.Name, err)
				}
			} else {
				// Table exists - verify it has a primary key (required for ON CONFLICT / MERGE)
				hasPK, err := o.targetPool.HasPrimaryKey(ctx, o.config.Target.Schema, t.Name)
				if err != nil {
					o.state.CompleteRun(runID, "failed", err.Error())
					o.notifyFailure(runID, err, time.Since(startTime))
					return fmt.Errorf("checking primary key for %s: %w", t.Name, err)
				}
				if !hasPK {
					// Try to create the PK on existing table
					logging.Info("  Creating missing PK on existing table %s...", t.Name)
					if err := o.targetPool.CreatePrimaryKey(ctx, &t, o.config.Target.Schema); err != nil {
						o.state.CompleteRun(runID, "failed", err.Error())
						errMsg := fmt.Errorf("target table %s exists but has no primary key - cannot create PK: %w", t.Name, err)
						o.notifyFailure(runID, errMsg, time.Since(startTime))
						return errMsg
					}
				}
			}
			// Do NOT truncate - upsert mode preserves existing data
		}
	} else if o.config.Migration.TargetMode == "truncate" {
		logging.Info("Preparing target tables (truncate mode)...")
		// First check which tables exist vs need to be created
		var tablesToTruncate []source.Table
		var tablesToCreate []source.Table
		for _, t := range tables {
			exists, err := o.targetPool.TableExists(ctx, o.config.Target.Schema, t.Name)
			if err != nil {
				o.state.CompleteRun(runID, "failed", err.Error())
				o.notifyFailure(runID, err, time.Since(startTime))
				return fmt.Errorf("checking if table %s exists: %w", t.Name, err)
			}
			if exists {
				tablesToTruncate = append(tablesToTruncate, t)
			} else {
				tablesToCreate = append(tablesToCreate, t)
			}
		}

		// Parallel truncation
		if len(tablesToTruncate) > 0 {
			logging.Debug("  Truncating %d existing tables in parallel...", len(tablesToTruncate))
			var truncWg sync.WaitGroup
			truncErrs := make(chan error, len(tablesToTruncate))
			for _, t := range tablesToTruncate {
				truncWg.Add(1)
				go func(table source.Table) {
					defer truncWg.Done()
					if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, table.Name); err != nil {
						truncErrs <- fmt.Errorf("truncating table %s: %w", table.Name, err)
					}
				}(t)
			}
			truncWg.Wait()
			close(truncErrs)
			if err := <-truncErrs; err != nil {
				o.state.CompleteRun(runID, "failed", err.Error())
				o.notifyFailure(runID, err, time.Since(startTime))
				return err
			}
		}

		// Parallel table creation for non-existing tables
		if len(tablesToCreate) > 0 {
			logging.Debug("  Creating %d new tables in parallel...", len(tablesToCreate))
			var createWg sync.WaitGroup
			createErrs := make(chan error, len(tablesToCreate))
			for _, t := range tablesToCreate {
				createWg.Add(1)
				go func(table source.Table) {
					defer createWg.Done()
					if err := o.targetPool.CreateTable(ctx, &table, o.config.Target.Schema); err != nil {
						createErrs <- fmt.Errorf("creating table %s: %w", table.FullName(), err)
					}
				}(t)
			}
			createWg.Wait()
			close(createErrs)
			if err := <-createErrs; err != nil {
				o.state.CompleteRun(runID, "failed", err.Error())
				o.notifyFailure(runID, err, time.Since(startTime))
				return err
			}
		}
	} else {
		// Default: drop_recreate
		logging.Info("Creating target tables (drop and recreate)...")

		// Phase 1: Drop all tables in parallel (no FK dependencies since we're dropping)
		logging.Debug("  Dropping %d tables in parallel...", len(tables))
		var dropWg sync.WaitGroup
		dropErrs := make(chan error, len(tables))
		for _, t := range tables {
			dropWg.Add(1)
			go func(table source.Table) {
				defer dropWg.Done()
				if err := o.targetPool.DropTable(ctx, o.config.Target.Schema, table.Name); err != nil {
					dropErrs <- fmt.Errorf("dropping table %s: %w", table.Name, err)
				}
			}(t)
		}
		dropWg.Wait()
		close(dropErrs)
		if err := <-dropErrs; err != nil {
			o.state.CompleteRun(runID, "failed", err.Error())
			o.notifyFailure(runID, err, time.Since(startTime))
			return err
		}

		// Phase 2: Create all tables in parallel (no FKs yet, just tables)
		logging.Debug("  Creating %d tables in parallel...", len(tables))
		var createWg sync.WaitGroup
		createErrs := make(chan error, len(tables))
		for _, t := range tables {
			createWg.Add(1)
			go func(table source.Table) {
				defer createWg.Done()
				if err := o.targetPool.CreateTable(ctx, &table, o.config.Target.Schema); err != nil {
					createErrs <- fmt.Errorf("creating table %s: %w", table.FullName(), err)
				}
			}(t)
		}
		createWg.Wait()
		close(createErrs)
		if err := <-createErrs; err != nil {
			o.state.CompleteRun(runID, "failed", err.Error())
			o.notifyFailure(runID, err, time.Since(startTime))
			return err
		}
	}

	// Transfer data
	logging.Info("Transferring data...")
	o.state.UpdatePhase(runID, "transferring")
	tableFailures, err := o.transferAll(ctx, runID, tables, false)
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
	logging.Info("Finalizing...")
	o.state.UpdatePhase(runID, "finalizing")
	if err := o.finalize(ctx, successTables); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("finalizing: %w", err)
	}

	// Validate (only for successful tables)
	logging.Info("Validating...")
	o.state.UpdatePhase(runID, "validating")
	o.tables = successTables // Update for validation
	if err := o.Validate(ctx); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	// Sample validation if enabled
	if o.config.Migration.SampleValidation {
		logging.Info("Running sample validation...")
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
	} else {
		// Full success
		o.state.CompleteRun(runID, "success", "")
		o.notifier.MigrationCompleted(runID, startTime, duration, len(tables), totalRows, throughput)
		logging.Info("Migration complete: %d tables, %d rows in %s (%.0f rows/sec)",
			len(tables), totalRows, duration.Round(time.Second), throughput)
	}

	return nil
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
	var jobs []transfer.Job

	// Create progress saver for chunk-level resume
	progressSaver := checkpoint.NewProgressSaver(o.state)

	// Track jobs per table for completion tracking
	tableJobs := make(map[string]int) // tableName -> number of jobs

	// Count tables that need partitioning
	var largeKeysetTables, largeRowNumTables int
	for _, t := range tables {
		if t.IsLarge(o.config.Migration.LargeTableThreshold) {
			if t.SupportsKeysetPagination() {
				largeKeysetTables++
			} else if t.HasPK() {
				largeRowNumTables++
			}
		}
	}
	// Track partition query timing
	var partitionStartTime time.Time
	var totalPartitionTime time.Duration
	if largeKeysetTables > 0 {
		logging.Info("Calculating partition boundaries for %d large tables...", largeKeysetTables)
		partitionStartTime = time.Now()
	}

	for _, t := range tables {
		logging.Debug("Processing table %s (rows=%d, large=%v)", t.Name, t.RowCount, t.IsLarge(o.config.Migration.LargeTableThreshold))
		if t.IsLarge(o.config.Migration.LargeTableThreshold) && t.SupportsKeysetPagination() {
			// Partition large tables with keyset pagination (PK-based boundaries)
			numPartitions := min(
				int(t.RowCount/int64(o.config.Migration.ChunkSize))+1,
				o.config.Migration.MaxPartitions,
			)

			tableStart := time.Now()
			partitions, err := o.sourcePool.GetPartitionBoundaries(ctx, &t, numPartitions)
			tableElapsed := time.Since(tableStart)
			logging.Info("  %s: %d partitions (%s)", t.Name, len(partitions), tableElapsed.Round(time.Millisecond))
			if err != nil {
				return nil, fmt.Errorf("partitioning %s: %w", t.FullName(), err)
			}

			tableJobs[t.Name] = len(partitions)
			for _, p := range partitions {
				// Create task for chunk-level tracking
				taskKey := fmt.Sprintf("transfer:%s.%s:p%d", t.Schema, t.Name, p.PartitionID)
				taskID, _ := o.state.CreateTask(runID, "transfer", taskKey)

				jobs = append(jobs, transfer.Job{
					Table:     t,
					Partition: &p,
					TaskID:    taskID,
					Saver:     progressSaver,
				})
			}
		} else if t.IsLarge(o.config.Migration.LargeTableThreshold) && t.HasPK() {
			// Partition large tables with ROW_NUMBER pagination (row-based boundaries)
			numPartitions := min(
				int(t.RowCount/int64(o.config.Migration.ChunkSize))+1,
				o.config.Migration.MaxPartitions,
			)
			if numPartitions < 1 {
				numPartitions = 1
			}

			logging.Debug("  Partitioning %s (%d rows, %d partitions, row-number)...", t.Name, t.RowCount, numPartitions)
			rowsPerPartition := t.RowCount / int64(numPartitions)
			tableJobs[t.Name] = numPartitions

			for i := 0; i < numPartitions; i++ {
				startRow := int64(i) * rowsPerPartition
				endRow := startRow + rowsPerPartition
				if i == numPartitions-1 {
					endRow = t.RowCount // Last partition gets remaining rows
				}

				p := source.Partition{
					TableName:   t.Name,
					PartitionID: i + 1,
					StartRow:    startRow,
					EndRow:      endRow,
					RowCount:    endRow - startRow,
				}

				taskKey := fmt.Sprintf("transfer:%s.%s:p%d", t.Schema, t.Name, p.PartitionID)
				taskID, _ := o.state.CreateTask(runID, "transfer", taskKey)

				jobs = append(jobs, transfer.Job{
					Table:     t,
					Partition: &p,
					TaskID:    taskID,
					Saver:     progressSaver,
				})
			}
		} else {
			tableJobs[t.Name] = 1
			// Create task for chunk-level tracking
			taskKey := fmt.Sprintf("transfer:%s.%s", t.Schema, t.Name)
			taskID, _ := o.state.CreateTask(runID, "transfer", taskKey)

			jobs = append(jobs, transfer.Job{
				Table:     t,
				Partition: nil,
				TaskID:    taskID,
				Saver:     progressSaver,
			})
		}
	}

	// Log total partition query time
	if largeKeysetTables > 0 {
		totalPartitionTime = time.Since(partitionStartTime)
		logging.Info("Partition queries completed in %s", totalPartitionTime.Round(time.Millisecond))
	}

	// Initialize progress
	logging.Debug("Created %d jobs, calculating total rows", len(jobs))
	var totalRows int64
	for _, j := range jobs {
		if j.Partition != nil {
			totalRows += j.Partition.RowCount
		} else {
			totalRows += j.Table.RowCount
		}
	}
	logging.Debug("Setting progress total to %d rows", totalRows)
	o.progress.SetTotal(totalRows)
	logging.Debug("Progress initialized")

	// Stats collection per table
	type tableStats struct {
		mu           sync.Mutex
		stats        *transfer.TransferStats
		jobsComplete int
		jobsFailed   int
	}
	statsMap := make(map[string]*tableStats)
	for _, t := range tables {
		statsMap[t.Name] = &tableStats{stats: &transfer.TransferStats{}}
	}

	// Pre-truncate partitioned tables BEFORE dispatching jobs (prevents race condition).
	// Skip this on resume so completed partitions aren't wiped.
	// Skip this in upsert mode - upserts are idempotent and don't require truncation.
	// Non-partitioned tables are truncated inside transfer.Execute as before.
	if !resume && o.config.Migration.TargetMode != "upsert" {
		// Collect unique table names that need truncation
		tablesToTruncate := make(map[string]bool)
		for _, j := range jobs {
			if j.Partition != nil {
				tablesToTruncate[j.Table.Name] = true
			}
		}

		// Parallel truncation of partitioned tables
		if len(tablesToTruncate) > 0 {
			logging.Debug("Pre-truncating %d partitioned tables in parallel...", len(tablesToTruncate))
			var truncWg sync.WaitGroup
			truncErrs := make(chan error, len(tablesToTruncate))
			for tableName := range tablesToTruncate {
				truncWg.Add(1)
				go func(tname string) {
					defer truncWg.Done()
					if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, tname); err != nil {
						truncErrs <- fmt.Errorf("pre-truncating table %s: %w", tname, err)
					}
				}(tableName)
			}
			truncWg.Wait()
			close(truncErrs)
			if err := <-truncErrs; err != nil {
				return nil, err
			}
		}
	}

	// For upsert mode with MSSQL target, handle staging tables
	// On resume: check if staging exists with correct row count - if so, skip bulk insert
	// On fresh run: prepare empty staging tables
	tablesWithStagingReady := make(map[string]bool)
	if o.config.Migration.TargetMode == "upsert" && o.targetPool.DBType() == "mssql" {
		preparedTables := make(map[string]bool)
		for _, j := range jobs {
			if !preparedTables[j.Table.Name] {
				// On resume, check if staging table exists with matching row count
				if resume {
					exists, stagingRows, err := o.targetPool.CheckUpsertStagingReady(ctx, o.config.Target.Schema, j.Table.Name)
					if err != nil {
						logging.Warn("Error checking staging for %s: %v", j.Table.Name, err)
					} else if exists && stagingRows == j.Table.RowCount {
						// Staging is ready with correct row count - skip bulk insert for this table
						logging.Info("Staging table for %s has %d rows (matches source) - skipping bulk insert", j.Table.Name, stagingRows)
						tablesWithStagingReady[j.Table.Name] = true
						preparedTables[j.Table.Name] = true
						continue
					} else if exists && stagingRows > 0 {
						logging.Info("Staging table for %s has %d rows (source has %d) - will re-stage", j.Table.Name, stagingRows, j.Table.RowCount)
					}
				}
				// Fresh run or staging incomplete - prepare empty staging table
				if err := o.targetPool.PrepareUpsertStaging(ctx, o.config.Target.Schema, j.Table.Name); err != nil {
					return nil, fmt.Errorf("preparing upsert staging for %s: %w", j.Table.Name, err)
				}
				preparedTables[j.Table.Name] = true
			}
		}
	}

	// Execute jobs with worker pool
	logging.Debug("Starting worker pool with %d workers, %d jobs", o.config.Migration.Workers, len(jobs))
	sem := make(chan struct{}, o.config.Migration.Workers)
	var wg sync.WaitGroup

	// Track failures per table
	type tableError struct {
		tableName string
		err       error
	}
	errCh := make(chan tableError, len(jobs))

	// For MSSQL upsert mode with staging ready, run MERGE directly (skip bulk insert)
	if o.config.Migration.TargetMode == "upsert" && o.targetPool.DBType() == "mssql" && len(tablesWithStagingReady) > 0 {
		mergedTables := make(map[string]bool)
		for _, j := range jobs {
			if tablesWithStagingReady[j.Table.Name] && !mergedTables[j.Table.Name] {
				logging.Info("Running upsert MERGE for %s (staging ready from previous run)", j.Table.Name)
				cols := make([]string, len(j.Table.Columns))
				for i, c := range j.Table.Columns {
					cols[i] = c.Name
				}
				if err := o.targetPool.ExecuteUpsertMerge(ctx, o.config.Target.Schema, j.Table.Name, cols, j.Table.PrimaryKey, o.config.Migration.UpsertMergeChunkSize); err != nil {
					logging.Error("ExecuteUpsertMerge failed for %s: %v", j.Table.Name, err)
					return nil, fmt.Errorf("upsert merge for %s: %w", j.Table.Name, err)
				}
				logging.Info("Upsert MERGE complete for %s", j.Table.Name)
				// Mark all jobs for this table as complete
				for _, jj := range jobs {
					if jj.Table.Name == j.Table.Name {
						o.state.UpdateTaskStatus(jj.TaskID, "success", "")
					}
				}
				taskKey := fmt.Sprintf("transfer:%s.%s", j.Table.Schema, j.Table.Name)
				o.markTableComplete(runID, taskKey)
				mergedTables[j.Table.Name] = true
			}
		}
	}

	// Filter out jobs for tables with staging already processed
	var jobsToRun []transfer.Job
	for _, j := range jobs {
		if !tablesWithStagingReady[j.Table.Name] {
			jobsToRun = append(jobsToRun, j)
		}
	}

	for _, job := range jobsToRun {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(j transfer.Job) {
			defer wg.Done()
			defer func() { <-sem }()

			// Mark task as running when worker starts processing
			o.state.UpdateTaskStatus(j.TaskID, "running", "")

			stats, err := transfer.Execute(ctx, o.sourcePool, o.targetPool, o.config, j, o.progress)

			// Update table stats and completion tracking
			ts := statsMap[j.Table.Name]
			ts.mu.Lock()
			if err != nil {
				ts.jobsFailed++
				ts.mu.Unlock()
				// Mark this task as failed
				o.state.UpdateTaskStatus(j.TaskID, "failed", err.Error())
				errCh <- tableError{tableName: j.Table.Name, err: err}
				// Log the failure immediately
				logging.Error("Table %s failed: %v", j.Table.Name, err)
				// Notify via Slack
				o.notifier.TableTransferFailed(runID, j.Table.Name, err)
				return
			}

			// Mark this task/partition as complete
			o.state.UpdateTaskStatus(j.TaskID, "success", "")

			if stats != nil {
				ts.stats.QueryTime += stats.QueryTime
				ts.stats.ScanTime += stats.ScanTime
				ts.stats.WriteTime += stats.WriteTime
				ts.stats.Rows += stats.Rows
			}
			ts.jobsComplete++

			// If all jobs for this table are complete, mark table task as success
			if ts.jobsComplete == tableJobs[j.Table.Name] && ts.jobsFailed == 0 {
				// For MSSQL upsert mode, execute the final MERGE now that all data is staged
				if o.config.Migration.TargetMode == "upsert" && o.targetPool.DBType() == "mssql" {
					cols := make([]string, len(j.Table.Columns))
					for i, c := range j.Table.Columns {
						cols[i] = c.Name
					}
					if mergeErr := o.targetPool.ExecuteUpsertMerge(ctx, o.config.Target.Schema, j.Table.Name, cols, j.Table.PrimaryKey, o.config.Migration.UpsertMergeChunkSize); mergeErr != nil {
						logging.Error("ExecuteUpsertMerge failed for %s: %v", j.Table.Name, mergeErr)
						ts.jobsFailed++
						ts.mu.Unlock()
						errCh <- tableError{tableName: j.Table.Name, err: fmt.Errorf("upsert merge: %w", mergeErr)}
						return
					}
					logging.Info("Upsert MERGE complete for %s", j.Table.Name)
				}
				taskKey := fmt.Sprintf("transfer:%s.%s", j.Table.Schema, j.Table.Name)
				o.markTableComplete(runID, taskKey)
			}
			ts.mu.Unlock()
		}(job)
	}

	wg.Wait()
	close(errCh)

	// Collect table failures (deduplicate by table name)
	failedTables := make(map[string]error)
	for te := range errCh {
		// Check for context cancellation
		if errors.Is(te.err, context.Canceled) || errors.Is(te.err, context.DeadlineExceeded) {
			o.progress.Finish()
			return nil, context.Canceled
		}
		// Only keep first error per table
		if _, exists := failedTables[te.tableName]; !exists {
			failedTables[te.tableName] = te.err
		}
	}

	o.progress.Finish()

	// Print pool stats (based on pool type) - debug level
	if logging.IsDebug() {
		logging.Debug("\nConnection Pool Usage:")
		switch p := o.sourcePool.(type) {
		case *source.Pool:
			stats := p.Stats()
			logging.Debug("  Source (mssql): %d/%d active, %d idle, %d waits (%.1fms avg)",
				stats.InUse, stats.MaxOpenConnections, stats.Idle,
				stats.WaitCount, float64(stats.WaitDuration)/float64(max(stats.WaitCount, 1)))
		case *source.PostgresPool:
			logging.Debug("  Source (postgres): max connections=%d", p.MaxConns())
		}
		switch p := o.targetPool.(type) {
		case *target.Pool:
			stats := p.Stats()
			logging.Debug("  Target (postgres): %d/%d active, %d idle, %d acquires (%d waited)",
				stats.AcquiredConns, stats.MaxConns, stats.IdleConns,
				stats.AcquireCount, stats.EmptyAcquireCount)
		case *target.MSSQLPool:
			logging.Debug("  Target (mssql): max connections=%d", p.MaxConns())
		}

		// Print profiling stats
		logging.Debug("\nTransfer Profile (per table):")
		logging.Debug("------------------------------")
		var totalQuery, totalScan, totalWrite time.Duration
		for _, t := range tables {
			ts := statsMap[t.Name]
			if ts.stats.Rows > 0 {
				logging.Debug("%-25s %s", t.Name, ts.stats.String())
				totalQuery += ts.stats.QueryTime
				totalScan += ts.stats.ScanTime
				totalWrite += ts.stats.WriteTime
			}
		}
		totalTime := totalQuery + totalScan + totalWrite
		if totalTime > 0 {
			logging.Debug("------------------------------")
			logging.Debug("%-25s query=%.1fs (%.0f%%), scan=%.1fs (%.0f%%), write=%.1fs (%.0f%%)",
				"TOTAL",
				totalQuery.Seconds(), float64(totalQuery)/float64(totalTime)*100,
				totalScan.Seconds(), float64(totalScan)/float64(totalTime)*100,
				totalWrite.Seconds(), float64(totalWrite)/float64(totalTime)*100)
		}
	}

	// Convert map to slice
	var failures []TableFailure
	for tableName, err := range failedTables {
		failures = append(failures, TableFailure{TableName: tableName, Error: err})
	}

	return failures, nil
}

func (o *Orchestrator) finalize(ctx context.Context, tables []source.Table) error {
	// Phase 1: Reset sequences (parallel - no dependencies between tables)
	logging.Debug("  Resetting sequences...")
	var seqWg sync.WaitGroup
	for _, t := range tables {
		seqWg.Add(1)
		go func(table source.Table) {
			defer seqWg.Done()
			if err := o.targetPool.ResetSequence(ctx, o.config.Target.Schema, &table); err != nil {
				logging.Warn("Warning: resetting sequence for %s: %v", table.Name, err)
			}
		}(t)
	}
	seqWg.Wait()

	// Phase 2: Create primary keys (parallel - no dependencies between tables)
	logging.Debug("  Creating primary keys...")
	var pkWg sync.WaitGroup
	for _, t := range tables {
		pkWg.Add(1)
		go func(table source.Table) {
			defer pkWg.Done()
			if err := o.targetPool.CreatePrimaryKey(ctx, &table, o.config.Target.Schema); err != nil {
				logging.Warn("Warning: creating PK for %s: %v", table.Name, err)
			}
		}(t)
	}
	pkWg.Wait()

	// Phase 3: Create indexes (if enabled) - parallel per table
	if o.config.Migration.CreateIndexes {
		// Count total indexes for logging
		totalIndexes := 0
		for _, t := range tables {
			totalIndexes += len(t.Indexes)
		}
		if totalIndexes > 0 {
			logging.Debug("  Creating %d indexes in parallel...", totalIndexes)
		}

		var idxWg sync.WaitGroup
		for _, t := range tables {
			for _, idx := range t.Indexes {
				idxWg.Add(1)
				go func(table source.Table, index source.Index) {
					defer idxWg.Done()
					if err := o.targetPool.CreateIndex(ctx, &table, &index, o.config.Target.Schema); err != nil {
						logging.Warn("Warning: creating index %s on %s: %v", index.Name, table.Name, err)
					}
				}(t, idx)
			}
		}
		idxWg.Wait()
	}

	// Phase 4: Create foreign keys (if enabled) - parallel per table
	// Note: FKs can be created in parallel since all tables and PKs exist at this point
	if o.config.Migration.CreateForeignKeys {
		totalFKs := 0
		for _, t := range tables {
			totalFKs += len(t.ForeignKeys)
		}
		if totalFKs > 0 {
			logging.Debug("  Creating %d foreign keys in parallel...", totalFKs)
		}

		var fkWg sync.WaitGroup
		for _, t := range tables {
			for _, fk := range t.ForeignKeys {
				fkWg.Add(1)
				go func(table source.Table, foreignKey source.ForeignKey) {
					defer fkWg.Done()
					if err := o.targetPool.CreateForeignKey(ctx, &table, &foreignKey, o.config.Target.Schema); err != nil {
						logging.Warn("Warning: creating FK %s on %s: %v", foreignKey.Name, table.Name, err)
					}
				}(t, fk)
			}
		}
		fkWg.Wait()
	}

	// Phase 5: Create check constraints (if enabled) - parallel per table
	if o.config.Migration.CreateCheckConstraints {
		totalChecks := 0
		for _, t := range tables {
			totalChecks += len(t.CheckConstraints)
		}
		if totalChecks > 0 {
			logging.Debug("  Creating %d check constraints in parallel...", totalChecks)
		}

		var chkWg sync.WaitGroup
		for _, t := range tables {
			for _, chk := range t.CheckConstraints {
				chkWg.Add(1)
				go func(table source.Table, check source.CheckConstraint) {
					defer chkWg.Done()
					if err := o.targetPool.CreateCheckConstraint(ctx, &table, &check, o.config.Target.Schema); err != nil {
						logging.Warn("Warning: creating CHECK %s on %s: %v", check.Name, table.Name, err)
					}
				}(t, chk)
			}
		}
		chkWg.Wait()
	}

	return nil
}

// Validate checks row counts between source and target
func (o *Orchestrator) Validate(ctx context.Context) error {
	if o.tables == nil {
		tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
		if err != nil {
			return err
		}
		o.tables = tables
	}

	logging.Info("\nValidation Results:")
	logging.Info("-------------------")

	var failed bool
	for _, t := range o.tables {
		// Query fresh counts from both source and target (don't use cached t.RowCount)
		sourceCount, err := o.sourcePool.GetRowCount(ctx, o.config.Source.Schema, t.Name)
		if err != nil {
			logging.Error("%-30s ERROR getting source count: %v", t.Name, err)
			failed = true
			continue
		}

		targetCount, err := o.targetPool.GetRowCount(ctx, o.config.Target.Schema, t.Name)
		if err != nil {
			logging.Error("%-30s ERROR getting target count: %v", t.Name, err)
			failed = true
			continue
		}

		if targetCount == sourceCount {
			logging.Info("%-30s OK %d rows", t.Name, targetCount)
		} else {
			logging.Error("%-30s FAIL source=%d target=%d (diff=%d)",
				t.Name, sourceCount, targetCount, sourceCount-targetCount)
			failed = true
		}
	}

	if failed {
		return fmt.Errorf("validation failed")
	}
	return nil
}

// validateSamples performs sample data validation by comparing random rows
func (o *Orchestrator) validateSamples(ctx context.Context) error {
	sampleSize := o.config.Migration.SampleSize
	if sampleSize <= 0 {
		sampleSize = 100
	}

	logging.Info("\nSample Validation (n=%d per table):", sampleSize)
	logging.Info("------------------------------------")

	var failed bool
	for _, t := range o.tables {
		if !t.HasPK() {
			logging.Debug("%-30s SKIP (no PK)", t.Name)
			continue
		}

		// Build sample query based on source database type
		var sampleQuery string
		if o.sourcePool.DBType() == "postgres" {
			// PostgreSQL source syntax
			pkCols := make([]string, len(t.PrimaryKey))
			for i, col := range t.PrimaryKey {
				pkCols[i] = fmt.Sprintf("%q", col)
			}
			pkColList := strings.Join(pkCols, ", ")
			sampleQuery = fmt.Sprintf(`
				SELECT %s FROM %s.%q
				ORDER BY random()
				LIMIT %d
			`, pkColList, t.Schema, t.Name, sampleSize)
		} else {
			// SQL Server source syntax
			pkCols := make([]string, len(t.PrimaryKey))
			for i, col := range t.PrimaryKey {
				pkCols[i] = fmt.Sprintf("[%s]", col)
			}
			pkColList := strings.Join(pkCols, ", ")
			tableHint := "WITH (NOLOCK)"
			if o.config.Migration.StrictConsistency {
				tableHint = ""
			}
			sampleQuery = fmt.Sprintf(`
				SELECT TOP %d %s FROM [%s].[%s] %s
				ORDER BY NEWID()
			`, sampleSize, pkColList, t.Schema, t.Name, tableHint)
		}

		rows, err := o.sourcePool.DB().QueryContext(ctx, sampleQuery)
		if err != nil {
			logging.Error("%-30s ERROR: %v", t.Name, err)
			continue
		}

		// Collect sample PK tuples (each is a slice of values)
		var pkTuples [][]any
		for rows.Next() {
			// Create slice to hold all PK column values
			pkValues := make([]any, len(t.PrimaryKey))
			pkPtrs := make([]any, len(t.PrimaryKey))
			for i := range pkValues {
				pkPtrs[i] = &pkValues[i]
			}

			if err := rows.Scan(pkPtrs...); err != nil {
				continue
			}
			pkTuples = append(pkTuples, pkValues)
		}
		rows.Close()

		if len(pkTuples) == 0 {
			logging.Debug("%-30s SKIP (no rows)", t.Name)
			continue
		}

		// Check if these PK tuples exist in target
		missingCount := 0
		for _, pkTuple := range pkTuples {
			exists, err := o.checkRowExistsInTarget(ctx, t, pkTuple)
			if err != nil || !exists {
				missingCount++
			}
		}

		if missingCount == 0 {
			logging.Info("%-30s OK (%d samples)", t.Name, len(pkTuples))
		} else {
			logging.Error("%-30s FAIL (%d/%d missing)", t.Name, missingCount, len(pkTuples))
			failed = true
		}
	}

	if failed {
		return fmt.Errorf("sample validation failed")
	}
	return nil
}

// checkRowExistsInTarget checks if a row with the given PK values exists in target
func (o *Orchestrator) checkRowExistsInTarget(ctx context.Context, t source.Table, pkTuple []any) (bool, error) {
	switch p := o.targetPool.(type) {
	case *target.Pool:
		// PostgreSQL target
		whereClauses := make([]string, len(t.PrimaryKey))
		for i, col := range t.PrimaryKey {
			whereClauses[i] = fmt.Sprintf("%q = $%d", col, i+1)
		}
		whereClause := strings.Join(whereClauses, " AND ")
		checkQuery := fmt.Sprintf(
			`SELECT EXISTS(SELECT 1 FROM %s.%q WHERE %s)`,
			o.config.Target.Schema, t.Name, whereClause,
		)
		var exists bool
		err := p.Pool().QueryRow(ctx, checkQuery, pkTuple...).Scan(&exists)
		return exists, err

	case *target.MSSQLPool:
		// SQL Server target
		whereClauses := make([]string, len(t.PrimaryKey))
		args := make([]any, len(t.PrimaryKey))
		for i, col := range t.PrimaryKey {
			whereClauses[i] = fmt.Sprintf("[%s] = @p%d", col, i+1)
			args[i] = sql.Named(fmt.Sprintf("p%d", i+1), pkTuple[i])
		}
		whereClause := strings.Join(whereClauses, " AND ")
		checkQuery := fmt.Sprintf(
			`SELECT CASE WHEN EXISTS(SELECT 1 FROM [%s].[%s] WHERE %s) THEN 1 ELSE 0 END`,
			o.config.Target.Schema, t.Name, whereClause,
		)
		var exists int
		err := p.DB().QueryRowContext(ctx, checkQuery, args...).Scan(&exists)
		return exists == 1, err

	default:
		return false, fmt.Errorf("unsupported target pool type: %T", o.targetPool)
	}
}

// Resume continues an interrupted migration
func (o *Orchestrator) Resume(ctx context.Context) error {
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
	logging.Info("Resuming run: %s (started %s)", run.ID, run.StartedAt.Format(time.RFC3339))

	// Reset any running tasks to pending
	if err := o.state.MarkRunAsResumed(run.ID); err != nil {
		return fmt.Errorf("resetting tasks: %w", err)
	}

	// Extract schema (needed to know all tables)
	logging.Info("Extracting schema...")
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
	logging.Info("Found %d tables in source", len(tables))

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
		logging.Info("Finalizing...")
		if err := o.finalize(ctx, tables); err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return fmt.Errorf("finalizing: %w", err)
		}

		// Validate
		logging.Info("Validating...")
		if err := o.Validate(ctx); err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return err
		}

		o.state.CompleteRun(run.ID, "success", "")
		logging.Info("Resume complete!")
		return nil
	}

	logging.Info("Resuming transfer of %d tables", len(tablesToTransfer))

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
			// Table doesn't exist - create it and clear any stale progress
			if err := o.targetPool.CreateTable(ctx, &t, o.config.Target.Schema); err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return fmt.Errorf("creating table %s: %w", t.Name, err)
			}
			// Clear any saved progress since we're starting fresh
			o.state.ClearTransferProgress(taskID)
		} else {
			// Table exists - check if we have saved chunk progress
			lastPK, rowsDone, _ := progressSaver.GetProgress(taskID)

			// For partitioned tables, progress is saved at partition level, not table level.
			// Skip table-level truncation - partition cleanup in transfer.go handles partial data.
			isPartitioned := t.IsLarge(o.config.Migration.LargeTableThreshold) && t.SupportsKeysetPagination()

			if lastPK == nil && !isPartitioned {
				// No chunk progress and not partitioned - truncate to ensure clean re-transfer
				if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, t.Name); err != nil {
					o.state.CompleteRun(run.ID, "failed", err.Error())
					return fmt.Errorf("truncating table %s: %w", t.Name, err)
				}
			} else if lastPK != nil {
				// Have chunk progress - verify target row count matches saved progress
				// If target has fewer rows than saved progress, data was lost; start fresh
				targetCount, err := o.targetPool.GetRowCount(ctx, o.config.Target.Schema, t.Name)
				if err != nil {
					o.state.CompleteRun(run.ID, "failed", err.Error())
					return fmt.Errorf("getting row count for %s: %w", t.Name, err)
				}
				if targetCount < rowsDone {
					// Target has fewer rows than expected - clear progress and truncate
					logging.Warn("  Warning: %s has %d rows but expected %d - restarting transfer",
						t.Name, targetCount, rowsDone)
					o.state.ClearTransferProgress(taskID)
					if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, t.Name); err != nil {
						o.state.CompleteRun(run.ID, "failed", err.Error())
						return fmt.Errorf("truncating table %s: %w", t.Name, err)
					}
				}
				// If target has >= rowsDone, resume from saved progress
			}
		}
	}

	// Transfer only the incomplete tables
	logging.Info("Transferring data...")
	tableFailures, err := o.transferAll(ctx, run.ID, tablesToTransfer, true)
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
	logging.Info("Finalizing...")
	if err := o.finalize(ctx, successTables); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("finalizing: %w", err)
	}

	// Validate successful tables
	logging.Info("Validating...")
	if err := o.Validate(ctx); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return err
	}

	// Sample validation if enabled
	if o.config.Migration.SampleValidation {
		logging.Info("Running sample validation...")
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
	} else {
		// Full success
		o.state.CompleteRun(run.ID, "success", "")
		o.notifier.MigrationCompleted(run.ID, startTime, duration, len(tablesToTransfer), totalRows, throughput)
		logging.Info("Resume complete: %d tables, %d rows in %s (%.0f rows/sec)",
			len(tablesToTransfer), totalRows, duration.Round(time.Second), throughput)
	}

	return nil
}

// ShowStatus displays status of current/last run
func (o *Orchestrator) ShowStatus() error {
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return err
	}
	if run == nil {
		fmt.Println("No active migration")
		return nil
	}

	// Check if a successful run supersedes this incomplete run
	superseded, err := o.state.HasSuccessfulRunAfter(run)
	if err != nil {
		return err
	}
	if superseded {
		fmt.Println("No active migration")
		return nil
	}

	total, pending, running, success, failed, err := o.state.GetRunStats(run.ID)
	if err != nil {
		return err
	}

	// Use phase to determine if migration is active vs interrupted
	// Phases: initializing -> transferring -> finalizing -> validating -> complete
	phase := run.Phase
	if phase == "" {
		phase = "initializing"
	}

	// Determine if migration is interrupted:
	// If in transferring phase with no running tasks and incomplete tasks remain
	// (pending or failed), the migration was cancelled or crashed.
	// Workers would be actively processing if the migration were truly running.
	if phase == "transferring" && running == 0 && (pending > 0 || failed > 0) {
		fmt.Printf("Run: %s\n", run.ID)
		fmt.Printf("Status: interrupted (%d/%d tasks completed)\n", success, total)
		fmt.Printf("Started: %s\n", run.StartedAt.Format(time.RFC3339))
		fmt.Printf("Tasks: %d total, %d pending, %d running, %d success, %d failed\n",
			total, pending, running, success, failed)
		fmt.Println("Run 'resume' to continue.")
		return nil
	}

	fmt.Printf("Run: %s\n", run.ID)
	fmt.Printf("Status: %s (%s)\n", run.Status, phase)
	fmt.Printf("Started: %s\n", run.StartedAt.Format(time.RFC3339))
	if total > 0 {
		fmt.Printf("Tasks: %d total, %d pending, %d running, %d success, %d failed\n",
			total, pending, running, success, failed)
	}

	return nil
}

// ShowDetailedStatus displays detailed task-level status for a migration run
func (o *Orchestrator) ShowDetailedStatus() error {
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return err
	}
	if run == nil {
		fmt.Println("No active migration")
		return nil
	}

	// Check if a successful run supersedes this incomplete run
	superseded, err := o.state.HasSuccessfulRunAfter(run)
	if err != nil {
		return err
	}
	if superseded {
		fmt.Println("No active migration")
		return nil
	}

	total, pending, running, success, failed, err := o.state.GetRunStats(run.ID)
	if err != nil {
		return err
	}

	phase := run.Phase
	if phase == "" {
		phase = "initializing"
	}

	fmt.Printf("Run: %s\n", run.ID)
	fmt.Printf("Status: %s (%s)\n", run.Status, phase)
	fmt.Printf("Started: %s\n", run.StartedAt.Format(time.RFC3339))
	fmt.Printf("Tasks: %d total, %d pending, %d running, %d success, %d failed\n\n",
		total, pending, running, success, failed)

	// Get all tasks with progress
	tasks, err := o.state.GetTasksWithProgress(run.ID)
	if err != nil {
		return err
	}

	if len(tasks) == 0 {
		fmt.Println("No tasks found")
		return nil
	}

	// Print task details
	fmt.Printf("%-30s %-10s %-15s %-20s %s\n", "Task", "Status", "Progress", "Rows", "Error")
	fmt.Println(strings.Repeat("-", 100))

	for _, t := range tasks {
		// Format progress
		progress := ""
		rows := ""
		if t.RowsTotal > 0 {
			pct := float64(t.RowsDone) / float64(t.RowsTotal) * 100
			progress = fmt.Sprintf("%.1f%%", pct)
			rows = fmt.Sprintf("%d/%d", t.RowsDone, t.RowsTotal)
		} else if t.RowsDone > 0 {
			rows = fmt.Sprintf("%d", t.RowsDone)
		}

		// Truncate task key if too long
		taskKey := t.TaskKey
		if len(taskKey) > 28 {
			taskKey = taskKey[:25] + "..."
		}

		// Truncate error if too long
		errorMsg := t.ErrorMessage
		if len(errorMsg) > 30 {
			errorMsg = errorMsg[:27] + "..."
		}

		// Status indicator
		statusIcon := ""
		switch t.Status {
		case "success":
			statusIcon = "✓"
		case "failed":
			statusIcon = "✗"
		case "running":
			statusIcon = "►"
		case "pending":
			statusIcon = "○"
		}

		fmt.Printf("%-30s %s %-8s %-15s %-20s %s\n",
			taskKey, statusIcon, t.Status, progress, rows, errorMsg)
	}

	return nil
}

// ShowHistory displays all migration runs
func (o *Orchestrator) ShowHistory() error {
	runs, err := o.state.GetAllRuns()
	if err != nil {
		return err
	}

	if len(runs) == 0 {
		fmt.Println("No migration history")
		return nil
	}

	fmt.Printf("%-10s %-20s %-20s %-10s %-30s\n", "ID", "Started", "Completed", "Status", "Origin")
	fmt.Println("--------------------------------------------------------------------------------------")

	for _, r := range runs {
		completed := "-"
		if r.CompletedAt != nil {
			completed = r.CompletedAt.Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%-10s %-20s %-20s %-10s %-30s\n",
			r.ID, r.StartedAt.Format("2006-01-02 15:04:05"), completed, r.Status, runOrigin(&r))
		if r.Error != "" {
			fmt.Printf("           Error: %s\n", r.Error)
		}
	}

	fmt.Println("\nUse 'history --run <ID>' to view run configuration")
	return nil
}

// ShowRunDetails displays detailed information for a specific run
func (o *Orchestrator) ShowRunDetails(runID string) error {
	run, err := o.state.GetRunByID(runID)
	if err != nil {
		return fmt.Errorf("getting run: %w", err)
	}
	if run == nil {
		return fmt.Errorf("run not found: %s", runID)
	}

	fmt.Printf("Run ID:        %s\n", run.ID)
	fmt.Printf("Status:        %s\n", run.Status)
	if run.Error != "" {
		fmt.Printf("Error:         %s\n", run.Error)
	}
	fmt.Printf("Started:       %s\n", run.StartedAt.Format("2006-01-02 15:04:05"))
	if run.CompletedAt != nil {
		fmt.Printf("Completed:     %s\n", run.CompletedAt.Format("2006-01-02 15:04:05"))
		duration := run.CompletedAt.Sub(run.StartedAt)
		fmt.Printf("Duration:      %s\n", duration.Round(time.Second))
	}
	fmt.Printf("Source Schema: %s\n", run.SourceSchema)
	fmt.Printf("Target Schema: %s\n", run.TargetSchema)
	if origin := runOrigin(run); origin != "" {
		fmt.Printf("Origin:        %s\n", origin)
	}

	// Task stats
	total, pending, running, success, failed, err := o.state.GetRunStats(run.ID)
	if err == nil && total > 0 {
		fmt.Printf("\nTasks: %d total, %d success, %d failed, %d pending, %d running\n",
			total, success, failed, pending, running)
	}

	// Config (if stored)
	if run.Config != "" {
		fmt.Println("\nConfiguration:")
		fmt.Println("--------------")
		// Pretty print the JSON config
		var cfg config.Config
		if err := json.Unmarshal([]byte(run.Config), &cfg); err == nil {
			prettyJSON, _ := json.MarshalIndent(cfg, "", "  ")
			fmt.Println(string(prettyJSON))
		} else {
			// Fall back to raw output if parsing fails
			fmt.Println(run.Config)
		}
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func runOrigin(r *checkpoint.Run) string {
	if r == nil {
		return ""
	}
	if r.ProfileName != "" {
		return "profile:" + r.ProfileName
	}
	if r.ConfigPath != "" {
		return "config:" + r.ConfigPath
	}
	return ""
}

// GetLastRunResult builds a MigrationResult from the last run.
func (o *Orchestrator) GetLastRunResult() (*MigrationResult, error) {
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return nil, err
	}
	if run == nil {
		// Try to get the most recent run from history
		runs, err := o.state.GetAllRuns()
		if err != nil {
			return nil, err
		}
		if len(runs) == 0 {
			return nil, fmt.Errorf("no runs found")
		}
		run = &runs[0] // Most recent
	}
	return o.buildResultFromRun(run)
}

// GetRunResult builds a MigrationResult for a specific run ID.
func (o *Orchestrator) GetRunResult(runID string) (*MigrationResult, error) {
	run, err := o.state.GetRunByID(runID)
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, fmt.Errorf("run %s not found", runID)
	}
	return o.buildResultFromRun(run)
}

func (o *Orchestrator) buildResultFromRun(run *checkpoint.Run) (*MigrationResult, error) {
	tasks, err := o.state.GetTasksWithProgress(run.ID)
	if err != nil {
		return nil, err
	}

	result := &MigrationResult{
		RunID:      run.ID,
		Status:     run.Status,
		StartedAt:  run.StartedAt,
		TableStats: make([]TableResult, 0),
	}

	if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
		result.CompletedAt = *run.CompletedAt
		result.DurationSeconds = run.CompletedAt.Sub(run.StartedAt).Seconds()
	} else if run.Status == "running" {
		result.DurationSeconds = time.Since(run.StartedAt).Seconds()
	}

	if run.Error != "" {
		result.Error = run.Error
	}

	var totalRows int64
	tableMap := make(map[string]*TableResult)

	for _, t := range tasks {
		if !strings.HasPrefix(t.TaskKey, "transfer:") {
			continue
		}

		// Extract table name from task key (transfer:schema.table or transfer:schema.table:pN)
		tableName := strings.TrimPrefix(t.TaskKey, "transfer:")
		if idx := strings.LastIndex(tableName, ":p"); idx > 0 {
			tableName = tableName[:idx] // Remove partition suffix
		}

		if _, exists := tableMap[tableName]; !exists {
			tableMap[tableName] = &TableResult{
				Name:   tableName,
				Status: "pending",
			}
			result.TablesTotal++
		}

		tr := tableMap[tableName]
		tr.Rows += t.RowsDone
		totalRows += t.RowsDone

		// Update status (success only if all partitions succeed)
		switch t.Status {
		case "success":
			if tr.Status != "failed" {
				tr.Status = "success"
			}
		case "failed":
			tr.Status = "failed"
			tr.Error = t.ErrorMessage
		case "running":
			if tr.Status != "failed" {
				tr.Status = "running"
			}
		}
	}

	// Build table stats list, count successes/failures, and sort for deterministic output
	tableNames := make([]string, 0, len(tableMap))
	for name := range tableMap {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	for _, name := range tableNames {
		tr := tableMap[name]
		result.TableStats = append(result.TableStats, *tr)
		switch tr.Status {
		case "success":
			result.TablesSuccess++
		case "failed":
			result.TablesFailed++
			result.FailedTables = append(result.FailedTables, tr.Name)
		}
	}

	result.RowsTransferred = totalRows
	if result.DurationSeconds > 0 {
		result.RowsPerSecond = int64(float64(totalRows) / result.DurationSeconds)
	}

	if result.FailedTables == nil {
		result.FailedTables = []string{}
	}

	return result, nil
}

// GetStatusResult builds a StatusResult for the current/last run.
func (o *Orchestrator) GetStatusResult() (*StatusResult, error) {
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, fmt.Errorf("no active migration")
	}

	// Check if superseded
	superseded, err := o.state.HasSuccessfulRunAfter(run)
	if err != nil {
		return nil, err
	}
	if superseded {
		return nil, fmt.Errorf("no active migration")
	}

	total, pending, running, success, failed, err := o.state.GetRunStats(run.ID)
	if err != nil {
		return nil, err
	}

	tasks, err := o.state.GetTasksWithProgress(run.ID)
	if err != nil {
		return nil, err
	}

	var totalRows, totalRowsDone int64
	for _, t := range tasks {
		if strings.HasPrefix(t.TaskKey, "transfer:") {
			totalRows += t.RowsTotal
			totalRowsDone += t.RowsDone
		}
	}

	phase := run.Phase
	if phase == "" {
		phase = "initializing"
	}

	var progressPct float64
	if totalRows > 0 {
		progressPct = float64(totalRowsDone) / float64(totalRows) * 100
	}

	return &StatusResult{
		RunID:           run.ID,
		Status:          run.Status,
		Phase:           phase,
		StartedAt:       run.StartedAt,
		TablesTotal:     total,
		TablesComplete:  success,
		TablesRunning:   running,
		TablesPending:   pending,
		TablesFailed:    failed,
		RowsTransferred: totalRowsDone,
		ProgressPercent: progressPct,
	}, nil
}

// HealthCheck tests connectivity to source and target databases.
func (o *Orchestrator) HealthCheck(ctx context.Context) (*HealthCheckResult, error) {
	result := &HealthCheckResult{
		Timestamp:    time.Now().Format(time.RFC3339),
		SourceDBType: o.sourcePool.DBType(),
		TargetDBType: o.targetPool.DBType(),
	}

	// Test source connection
	sourceStart := time.Now()
	if db := o.sourcePool.DB(); db != nil {
		if err := db.PingContext(ctx); err != nil {
			result.SourceError = err.Error()
		} else {
			result.SourceConnected = true
			// Get table count
			tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
			if err == nil {
				result.SourceTableCount = len(tables)
			}
		}
	}
	result.SourceLatencyMs = time.Since(sourceStart).Milliseconds()

	// Test target connection
	targetStart := time.Now()
	if err := o.targetPool.Ping(ctx); err != nil {
		result.TargetError = err.Error()
	} else {
		result.TargetConnected = true
	}
	result.TargetLatencyMs = time.Since(targetStart).Milliseconds()

	result.Healthy = result.SourceConnected && result.TargetConnected
	return result, nil
}

// DryRun performs a migration preview without transferring data.
func (o *Orchestrator) DryRun(ctx context.Context) (*DryRunResult, error) {
	logging.Info("Performing dry run (no data will be transferred)...")

	// Extract schema
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		return nil, fmt.Errorf("extracting schema: %w", err)
	}

	// Apply table filters
	tables = o.filterTables(tables)

	result := &DryRunResult{
		SourceType:   o.config.Source.Type,
		TargetType:   o.config.Target.Type,
		SourceSchema: o.config.Source.Schema,
		TargetSchema: o.config.Target.Schema,
		Workers:      o.config.Migration.Workers,
		ChunkSize:    o.config.Migration.ChunkSize,
		TargetMode:   o.config.Migration.TargetMode,
		TotalTables:  len(tables),
	}

	// Calculate estimated memory
	bufferMem := int64(o.config.Migration.Workers) *
		int64(o.config.Migration.ReadAheadBuffers) *
		int64(o.config.Migration.ChunkSize) *
		500 // bytes per row estimate
	result.EstimatedMemMB = bufferMem / (1024 * 1024)

	// Analyze each table
	for _, t := range tables {
		rowCount, err := o.sourcePool.GetRowCount(ctx, o.config.Source.Schema, t.Name)
		if err != nil {
			logging.Warn("Failed to get row count for %s.%s: %v (assuming 0)", o.config.Source.Schema, t.Name, err)
			rowCount = 0
		}
		result.TotalRows += rowCount

		// Determine pagination method
		paginationMethod := "full_table"
		partitions := 1
		hasPK := len(t.PKColumns) > 0

		if hasPK {
			if len(t.PKColumns) == 1 && isIntegerType(t.PKColumns[0].DataType) {
				paginationMethod = "keyset"
			} else {
				paginationMethod = "row_number"
			}

			// Estimate partitions for large tables
			if rowCount > int64(o.config.Migration.LargeTableThreshold) {
				partitions = o.config.Migration.MaxPartitions
			}
		}

		result.Tables = append(result.Tables, DryRunTable{
			Name:             t.Name,
			RowCount:         rowCount,
			PaginationMethod: paginationMethod,
			Partitions:       partitions,
			HasPK:            hasPK,
			Columns:          len(t.Columns),
		})
	}

	return result, nil
}

// isIntegerType checks if a data type is an integer type.
func isIntegerType(dataType string) bool {
	dataType = strings.ToLower(dataType)
	intTypes := []string{"int", "bigint", "smallint", "tinyint", "integer", "int4", "int8", "int2"}
	for _, t := range intTypes {
		if strings.Contains(dataType, t) {
			return true
		}
	}
	return false
}

// Unused import suppression
var _ = sql.Named
var _ = strings.Join
