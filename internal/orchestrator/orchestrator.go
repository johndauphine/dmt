package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/johndauphine/mssql-pg-migrate/internal/checkpoint"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/notify"
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

// Orchestrator coordinates the migration process
type Orchestrator struct {
	config     *config.Config
	sourcePool *source.Pool
	targetPool *target.Pool
	state      *checkpoint.State
	progress   *progress.Tracker
	notifier   *notify.Notifier
	tables     []source.Table
}

// New creates a new orchestrator
func New(cfg *config.Config) (*Orchestrator, error) {
	// Create source pool
	sourcePool, err := source.NewPool(&cfg.Source, cfg.Migration.MaxMssqlConnections)
	if err != nil {
		return nil, fmt.Errorf("creating source pool: %w", err)
	}

	// Create target pool
	targetPool, err := target.NewPool(&cfg.Target, cfg.Migration.MaxPgConnections)
	if err != nil {
		sourcePool.Close()
		return nil, fmt.Errorf("creating target pool: %w", err)
	}

	// Create state manager
	state, err := checkpoint.New(cfg.Migration.DataDir)
	if err != nil {
		sourcePool.Close()
		targetPool.Close()
		return nil, fmt.Errorf("creating state manager: %w", err)
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
	}, nil
}

// Close releases all resources
func (o *Orchestrator) Close() {
	o.sourcePool.Close()
	o.targetPool.Close()
	o.state.Close()
}

// Run executes a new migration
func (o *Orchestrator) Run(ctx context.Context) error {
	runID := uuid.New().String()[:8]
	startTime := time.Now()
	fmt.Printf("Starting migration run: %s\n", runID)
	fmt.Printf("Connection pools: MSSQL=%d, PostgreSQL=%d\n",
		o.sourcePool.MaxConns(), o.targetPool.MaxConns())

	if err := o.state.CreateRun(runID, o.config.Source.Schema, o.config.Target.Schema, o.config); err != nil {
		return fmt.Errorf("creating run: %w", err)
	}

	// Extract schema
	fmt.Println("Extracting schema...")
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		o.state.CompleteRun(runID, "failed")
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("extracting schema: %w", err)
	}

	// Load additional metadata if enabled
	for i := range tables {
		t := &tables[i]

		if o.config.Migration.CreateIndexes {
			if err := o.sourcePool.LoadIndexes(ctx, t); err != nil {
				fmt.Printf("Warning: loading indexes for %s: %v\n", t.Name, err)
			}
		}

		if o.config.Migration.CreateForeignKeys {
			if err := o.sourcePool.LoadForeignKeys(ctx, t); err != nil {
				fmt.Printf("Warning: loading FKs for %s: %v\n", t.Name, err)
			}
		}

		if o.config.Migration.CreateCheckConstraints {
			if err := o.sourcePool.LoadCheckConstraints(ctx, t); err != nil {
				fmt.Printf("Warning: loading check constraints for %s: %v\n", t.Name, err)
			}
		}
	}

	// Apply table filters
	tables = o.filterTables(tables)
	if len(tables) == 0 {
		o.state.CompleteRun(runID, "failed")
		return fmt.Errorf("no tables to migrate after applying filters")
	}

	o.tables = tables
	fmt.Printf("Found %d tables\n", len(tables))

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
	fmt.Printf("Pagination: %d keyset, %d ROW_NUMBER, %d no PK\n",
		keysetCount, rowNumberCount, len(tables)-keysetCount-rowNumberCount)

	// Send start notification
	o.notifier.MigrationStarted(runID, o.config.Source.Database, o.config.Target.Database, len(tables))

	// Create target schema and tables
	if err := o.targetPool.CreateSchema(ctx, o.config.Target.Schema); err != nil {
		o.state.CompleteRun(runID, "failed")
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("creating schema: %w", err)
	}

	if o.config.Migration.TargetMode == "truncate" {
		fmt.Println("Preparing target tables (truncate mode)...")
		for _, t := range tables {
			exists, err := o.targetPool.TableExists(ctx, o.config.Target.Schema, t.Name)
			if err != nil {
				o.state.CompleteRun(runID, "failed")
				o.notifyFailure(runID, err, time.Since(startTime))
				return fmt.Errorf("checking if table %s exists: %w", t.Name, err)
			}
			if exists {
				if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, t.Name); err != nil {
					o.state.CompleteRun(runID, "failed")
					o.notifyFailure(runID, err, time.Since(startTime))
					return fmt.Errorf("truncating table %s: %w", t.Name, err)
				}
			} else {
				if err := o.targetPool.CreateTable(ctx, &t, o.config.Target.Schema); err != nil {
					o.state.CompleteRun(runID, "failed")
					o.notifyFailure(runID, err, time.Since(startTime))
					return fmt.Errorf("creating table %s: %w", t.FullName(), err)
				}
			}
		}
	} else {
		// Default: drop_recreate
		fmt.Println("Creating target tables (drop and recreate)...")
		for _, t := range tables {
			if err := o.targetPool.DropTable(ctx, o.config.Target.Schema, t.Name); err != nil {
				o.state.CompleteRun(runID, "failed")
				o.notifyFailure(runID, err, time.Since(startTime))
				return fmt.Errorf("dropping table %s: %w", t.Name, err)
			}
			if err := o.targetPool.CreateTable(ctx, &t, o.config.Target.Schema); err != nil {
				o.state.CompleteRun(runID, "failed")
				o.notifyFailure(runID, err, time.Since(startTime))
				return fmt.Errorf("creating table %s: %w", t.FullName(), err)
			}
		}
	}

	// Transfer data
	fmt.Println("Transferring data...")
	if err := o.transferAll(ctx, runID, tables); err != nil {
		o.state.CompleteRun(runID, "failed")
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("transferring data: %w", err)
	}

	// Finalize
	fmt.Println("Finalizing...")
	if err := o.finalize(ctx, tables); err != nil {
		o.state.CompleteRun(runID, "failed")
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("finalizing: %w", err)
	}

	// Validate
	fmt.Println("Validating...")
	if err := o.Validate(ctx); err != nil {
		o.state.CompleteRun(runID, "failed")
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	// Sample validation if enabled
	if o.config.Migration.SampleValidation {
		fmt.Println("Running sample validation...")
		if err := o.validateSamples(ctx); err != nil {
			fmt.Printf("Warning: sample validation failed: %v\n", err)
		}
	}

	// Calculate stats for notification
	duration := time.Since(startTime)
	var totalRows int64
	for _, t := range tables {
		totalRows += t.RowCount
	}
	throughput := float64(totalRows) / duration.Seconds()

	o.state.CompleteRun(runID, "success")
	o.notifier.MigrationCompleted(runID, startTime, duration, len(tables), totalRows, throughput)
	fmt.Println("Migration complete!")

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
		fmt.Printf("Skipped %d tables by filter: %v\n", len(skipped), skipped)
	}

	return filtered
}

func (o *Orchestrator) transferAll(ctx context.Context, runID string, tables []source.Table) error {
	var jobs []transfer.Job

	// Create progress saver for chunk-level resume
	progressSaver := checkpoint.NewProgressSaver(o.state)

	// Track jobs per table for completion tracking
	tableJobs := make(map[string]int) // tableName -> number of jobs

	for _, t := range tables {
		if t.IsLarge(o.config.Migration.LargeTableThreshold) && t.HasSinglePK() {
			// Partition large tables
			numPartitions := min(
				int(t.RowCount/int64(o.config.Migration.ChunkSize))+1,
				o.config.Migration.MaxPartitions,
			)

			partitions, err := o.sourcePool.GetPartitionBoundaries(ctx, &t, numPartitions)
			if err != nil {
				return fmt.Errorf("partitioning %s: %w", t.FullName(), err)
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

	// Initialize progress
	var totalRows int64
	for _, j := range jobs {
		if j.Partition != nil {
			totalRows += j.Partition.RowCount
		} else {
			totalRows += j.Table.RowCount
		}
	}
	o.progress.SetTotal(totalRows)

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

	// Pre-truncate partitioned tables BEFORE dispatching jobs (prevents race condition)
	// Non-partitioned tables will be truncated inside transfer.Execute as before
	truncatedTables := make(map[string]bool)
	for _, j := range jobs {
		if j.Partition != nil && !truncatedTables[j.Table.Name] {
			if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, j.Table.Name); err != nil {
				return fmt.Errorf("pre-truncating table %s: %w", j.Table.Name, err)
			}
			truncatedTables[j.Table.Name] = true
		}
	}

	// Execute jobs with worker pool
	sem := make(chan struct{}, o.config.Migration.Workers)
	var wg sync.WaitGroup
	errCh := make(chan error, len(jobs))

	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(j transfer.Job) {
			defer wg.Done()
			defer func() { <-sem }()

			stats, err := transfer.Execute(ctx, o.sourcePool, o.targetPool, o.config, j, o.progress)

			// Update table stats and completion tracking
			ts := statsMap[j.Table.Name]
			ts.mu.Lock()
			if err != nil {
				ts.jobsFailed++
				ts.mu.Unlock()
				errCh <- fmt.Errorf("transfer %s: %w", j.Table.FullName(), err)
				return
			}

			if stats != nil {
				ts.stats.QueryTime += stats.QueryTime
				ts.stats.ScanTime += stats.ScanTime
				ts.stats.WriteTime += stats.WriteTime
				ts.stats.Rows += stats.Rows
			}
			ts.jobsComplete++

			// If all jobs for this table are complete, mark task as success
			if ts.jobsComplete == tableJobs[j.Table.Name] && ts.jobsFailed == 0 {
				taskKey := fmt.Sprintf("transfer:%s.%s", j.Table.Schema, j.Table.Name)
				o.markTableComplete(runID, taskKey)
			}
			ts.mu.Unlock()
		}(job)
	}

	wg.Wait()
	close(errCh)

	// Collect errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	o.progress.Finish()

	// Print pool stats
	mssqlStats := o.sourcePool.Stats()
	pgStats := o.targetPool.Stats()
	fmt.Printf("\nConnection Pool Usage:\n")
	fmt.Printf("  MSSQL:      %d/%d active, %d idle, %d waits (%.1fms avg)\n",
		mssqlStats.InUse, mssqlStats.MaxOpenConnections, mssqlStats.Idle,
		mssqlStats.WaitCount, float64(mssqlStats.WaitDuration)/float64(max(mssqlStats.WaitCount, 1)))
	fmt.Printf("  PostgreSQL: %d/%d active, %d idle, %d acquires (%d waited)\n",
		pgStats.AcquiredConns, pgStats.MaxConns, pgStats.IdleConns,
		pgStats.AcquireCount, pgStats.EmptyAcquireCount)

	// Print profiling stats
	fmt.Println("\nTransfer Profile (per table):")
	fmt.Println("------------------------------")
	var totalQuery, totalScan, totalWrite time.Duration
	for _, t := range tables {
		ts := statsMap[t.Name]
		if ts.stats.Rows > 0 {
			fmt.Printf("%-25s %s\n", t.Name, ts.stats.String())
			totalQuery += ts.stats.QueryTime
			totalScan += ts.stats.ScanTime
			totalWrite += ts.stats.WriteTime
		}
	}
	totalTime := totalQuery + totalScan + totalWrite
	if totalTime > 0 {
		fmt.Println("------------------------------")
		fmt.Printf("%-25s query=%.1fs (%.0f%%), scan=%.1fs (%.0f%%), write=%.1fs (%.0f%%)\n",
			"TOTAL",
			totalQuery.Seconds(), float64(totalQuery)/float64(totalTime)*100,
			totalScan.Seconds(), float64(totalScan)/float64(totalTime)*100,
			totalWrite.Seconds(), float64(totalWrite)/float64(totalTime)*100)
	}

	if len(errs) > 0 {
		return fmt.Errorf("%d transfer errors: %v", len(errs), errs[0])
	}

	return nil
}

func (o *Orchestrator) finalize(ctx context.Context, tables []source.Table) error {
	// Phase 1: Reset sequences
	fmt.Println("  Resetting sequences...")
	for _, t := range tables {
		if err := o.targetPool.ResetSequence(ctx, o.config.Target.Schema, &t); err != nil {
			fmt.Printf("Warning: resetting sequence for %s: %v\n", t.Name, err)
		}
	}

	// Phase 2: Create primary keys
	fmt.Println("  Creating primary keys...")
	for _, t := range tables {
		if err := o.targetPool.CreatePrimaryKey(ctx, &t, o.config.Target.Schema); err != nil {
			fmt.Printf("Warning: creating PK for %s: %v\n", t.Name, err)
		}
	}

	// Phase 3: Create indexes (if enabled)
	if o.config.Migration.CreateIndexes {
		fmt.Println("  Creating indexes...")
		for _, t := range tables {
			for _, idx := range t.Indexes {
				if err := o.targetPool.CreateIndex(ctx, &t, &idx, o.config.Target.Schema); err != nil {
					fmt.Printf("Warning: creating index %s on %s: %v\n", idx.Name, t.Name, err)
				}
			}
		}
	}

	// Phase 4: Create foreign keys (if enabled)
	if o.config.Migration.CreateForeignKeys {
		fmt.Println("  Creating foreign keys...")
		for _, t := range tables {
			for _, fk := range t.ForeignKeys {
				if err := o.targetPool.CreateForeignKey(ctx, &t, &fk, o.config.Target.Schema); err != nil {
					fmt.Printf("Warning: creating FK %s on %s: %v\n", fk.Name, t.Name, err)
				}
			}
		}
	}

	// Phase 5: Create check constraints (if enabled)
	if o.config.Migration.CreateCheckConstraints {
		fmt.Println("  Creating check constraints...")
		for _, t := range tables {
			for _, chk := range t.CheckConstraints {
				if err := o.targetPool.CreateCheckConstraint(ctx, &t, &chk, o.config.Target.Schema); err != nil {
					fmt.Printf("Warning: creating CHECK %s on %s: %v\n", chk.Name, t.Name, err)
				}
			}
		}
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

	fmt.Println("\nValidation Results:")
	fmt.Println("-------------------")

	var failed bool
	for _, t := range o.tables {
		targetCount, err := o.targetPool.GetRowCount(ctx, o.config.Target.Schema, t.Name)
		if err != nil {
			fmt.Printf("%-30s ERROR: %v\n", t.Name, err)
			failed = true
			continue
		}

		if targetCount == t.RowCount {
			fmt.Printf("%-30s OK %d rows\n", t.Name, targetCount)
		} else {
			fmt.Printf("%-30s FAIL source=%d target=%d (diff=%d)\n",
				t.Name, t.RowCount, targetCount, t.RowCount-targetCount)
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

	fmt.Printf("\nSample Validation (n=%d per table):\n", sampleSize)
	fmt.Println("------------------------------------")

	var failed bool
	for _, t := range o.tables {
		if !t.HasPK() {
			fmt.Printf("%-30s SKIP (no PK)\n", t.Name)
			continue
		}

		// Build PK column list for SELECT (supports composite PKs)
		pkCols := make([]string, len(t.PrimaryKey))
		for i, col := range t.PrimaryKey {
			pkCols[i] = fmt.Sprintf("[%s]", col)
		}
		pkColList := strings.Join(pkCols, ", ")

		tableHint := "WITH (NOLOCK)"
		if o.config.Migration.StrictConsistency {
			tableHint = ""
		}

		// Sample random rows with all PK columns
		sampleQuery := fmt.Sprintf(`
			SELECT TOP %d %s FROM [%s].[%s] %s
			ORDER BY NEWID()
		`, sampleSize, pkColList, t.Schema, t.Name, tableHint)

		rows, err := o.sourcePool.DB().QueryContext(ctx, sampleQuery)
		if err != nil {
			fmt.Printf("%-30s ERROR: %v\n", t.Name, err)
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
			fmt.Printf("%-30s SKIP (no rows)\n", t.Name)
			continue
		}

		// Check if these PK tuples exist in target
		missingCount := 0
		for _, pkTuple := range pkTuples {
			// Build WHERE clause: col1 = $1 AND col2 = $2 AND ...
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
			err := o.targetPool.Pool().QueryRow(ctx, checkQuery, pkTuple...).Scan(&exists)
			if err != nil || !exists {
				missingCount++
			}
		}

		if missingCount == 0 {
			fmt.Printf("%-30s OK (%d samples)\n", t.Name, len(pkTuples))
		} else {
			fmt.Printf("%-30s FAIL (%d/%d missing)\n", t.Name, missingCount, len(pkTuples))
			failed = true
		}
	}

	if failed {
		return fmt.Errorf("sample validation failed")
	}
	return nil
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

	startTime := time.Now()
	fmt.Printf("Resuming run: %s (started %s)\n", run.ID, run.StartedAt.Format(time.RFC3339))

	// Reset any running tasks to pending
	if err := o.state.MarkRunAsResumed(run.ID); err != nil {
		return fmt.Errorf("resetting tasks: %w", err)
	}

	// Extract schema (needed to know all tables)
	fmt.Println("Extracting schema...")
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		o.state.CompleteRun(run.ID, "failed")
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("extracting schema: %w", err)
	}

	// Apply table filters
	tables = o.filterTables(tables)
	if len(tables) == 0 {
		o.state.CompleteRun(run.ID, "failed")
		return fmt.Errorf("no tables to migrate after applying filters")
	}

	o.tables = tables
	fmt.Printf("Found %d tables in source\n", len(tables))

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
		fmt.Printf("Skipping %d already-complete tables: %v\n", len(skippedTables), skippedTables)
	}

	if len(tablesToTransfer) == 0 {
		fmt.Println("All tables already transferred - completing migration")
		o.tables = tables // Use all tables for finalize/validate

		// Finalize
		fmt.Println("Finalizing...")
		if err := o.finalize(ctx, tables); err != nil {
			o.state.CompleteRun(run.ID, "failed")
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return fmt.Errorf("finalizing: %w", err)
		}

		// Validate
		fmt.Println("Validating...")
		if err := o.Validate(ctx); err != nil {
			o.state.CompleteRun(run.ID, "failed")
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return err
		}

		o.state.CompleteRun(run.ID, "success")
		fmt.Println("Resume complete!")
		return nil
	}

	fmt.Printf("Resuming transfer of %d tables\n", len(tablesToTransfer))

	// For tables that need transfer, ensure target tables exist
	// Check for chunk-level progress to avoid unnecessary truncation
	progressSaver := checkpoint.NewProgressSaver(o.state)
	for _, t := range tablesToTransfer {
		exists, err := o.targetPool.TableExists(ctx, o.config.Target.Schema, t.Name)
		if err != nil {
			o.state.CompleteRun(run.ID, "failed")
			return fmt.Errorf("checking table %s: %w", t.Name, err)
		}
		if !exists {
			if err := o.targetPool.CreateTable(ctx, &t, o.config.Target.Schema); err != nil {
				o.state.CompleteRun(run.ID, "failed")
				return fmt.Errorf("creating table %s: %w", t.Name, err)
			}
		} else {
			// Check if we have saved chunk progress for this table
			taskKey := fmt.Sprintf("transfer:%s.%s", t.Schema, t.Name)
			taskID, _ := o.state.CreateTask(run.ID, "transfer", taskKey)
			lastPK, _, _ := progressSaver.GetProgress(taskID)

			if lastPK == nil {
				// No chunk progress - truncate to ensure clean re-transfer
				if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, t.Name); err != nil {
					o.state.CompleteRun(run.ID, "failed")
					return fmt.Errorf("truncating table %s: %w", t.Name, err)
				}
			}
			// If we have chunk progress, don't truncate - transfer.Execute will handle cleanup
		}
	}

	// Transfer only the incomplete tables
	fmt.Println("Transferring data...")
	if err := o.transferAll(ctx, run.ID, tablesToTransfer); err != nil {
		o.state.CompleteRun(run.ID, "failed")
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("transferring data: %w", err)
	}

	// Finalize (uses all tables for constraints)
	o.tables = tables
	fmt.Println("Finalizing...")
	if err := o.finalize(ctx, tables); err != nil {
		o.state.CompleteRun(run.ID, "failed")
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("finalizing: %w", err)
	}

	// Validate all tables
	fmt.Println("Validating...")
	if err := o.Validate(ctx); err != nil {
		o.state.CompleteRun(run.ID, "failed")
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return err
	}

	// Sample validation if enabled
	if o.config.Migration.SampleValidation {
		fmt.Println("Running sample validation...")
		if err := o.validateSamples(ctx); err != nil {
			fmt.Printf("Warning: sample validation failed: %v\n", err)
		}
	}

	duration := time.Since(startTime)
	var totalRows int64
	for _, t := range tablesToTransfer {
		totalRows += t.RowCount
	}
	throughput := float64(totalRows) / duration.Seconds()

	o.state.CompleteRun(run.ID, "success")
	o.notifier.MigrationCompleted(run.ID, startTime, duration, len(tablesToTransfer), totalRows, throughput)
	fmt.Println("Resume complete!")

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

	total, pending, running, success, failed, err := o.state.GetRunStats(run.ID)
	if err != nil {
		return err
	}

	fmt.Printf("Run: %s\n", run.ID)
	fmt.Printf("Status: %s\n", run.Status)
	fmt.Printf("Started: %s\n", run.StartedAt.Format(time.RFC3339))
	fmt.Printf("Tasks: %d total, %d pending, %d running, %d success, %d failed\n",
		total, pending, running, success, failed)

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

	fmt.Printf("%-10s %-20s %-20s %-10s\n", "ID", "Started", "Completed", "Status")
	fmt.Println("--------------------------------------------------------------")

	for _, r := range runs {
		completed := "-"
		if r.CompletedAt != nil {
			completed = r.CompletedAt.Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%-10s %-20s %-20s %-10s\n",
			r.ID, r.StartedAt.Format("2006-01-02 15:04:05"), completed, r.Status)
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

// Unused import suppression
var _ = sql.Named
var _ = strings.Join
