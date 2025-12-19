package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/johndauphine/mssql-pg-migrate/internal/checkpoint"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
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
	TaskValidate       TaskType = "validate"
)

// Task dependencies - what must complete before each task type can run
var taskDependencies = map[TaskType][]TaskType{
	TaskExtractSchema:  {},
	TaskCreateTables:   {TaskExtractSchema},
	TaskTransfer:       {TaskCreateTables},
	TaskResetSequences: {TaskTransfer},
	TaskCreatePKs:      {TaskResetSequences},
	TaskValidate:       {TaskCreatePKs},
}

// Orchestrator coordinates the migration process
type Orchestrator struct {
	config     *config.Config
	sourcePool *source.Pool
	targetPool *target.Pool
	state      *checkpoint.State
	progress   *progress.Tracker
	tables     []source.Table
}

// New creates a new orchestrator
func New(cfg *config.Config) (*Orchestrator, error) {
	// Create source pool
	sourcePool, err := source.NewPool(&cfg.Source, cfg.Migration.MaxConnections)
	if err != nil {
		return nil, fmt.Errorf("creating source pool: %w", err)
	}

	// Create target pool
	targetPool, err := target.NewPool(&cfg.Target, cfg.Migration.MaxConnections)
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

	return &Orchestrator{
		config:     cfg,
		sourcePool: sourcePool,
		targetPool: targetPool,
		state:      state,
		progress:   progress.New(),
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
	fmt.Printf("Starting migration run: %s\n", runID)

	if err := o.state.CreateRun(runID, o.config.Source.Schema, o.config.Target.Schema, o.config); err != nil {
		return fmt.Errorf("creating run: %w", err)
	}

	// Extract schema
	fmt.Println("Extracting schema...")
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		return fmt.Errorf("extracting schema: %w", err)
	}
	o.tables = tables
	fmt.Printf("Found %d tables\n", len(tables))

	// Create target schema and tables
	fmt.Println("Creating target tables...")
	if err := o.targetPool.CreateSchema(ctx, o.config.Target.Schema); err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	for _, t := range tables {
		if err := o.targetPool.CreateTable(ctx, &t, o.config.Target.Schema); err != nil {
			return fmt.Errorf("creating table %s: %w", t.FullName(), err)
		}
	}

	// Transfer data
	fmt.Println("Transferring data...")
	if err := o.transferAll(ctx, runID, tables); err != nil {
		o.state.CompleteRun(runID, "failed")
		return fmt.Errorf("transferring data: %w", err)
	}

	// Finalize
	fmt.Println("Finalizing...")
	if err := o.finalize(ctx, tables); err != nil {
		return fmt.Errorf("finalizing: %w", err)
	}

	// Validate
	fmt.Println("Validating...")
	if err := o.Validate(ctx); err != nil {
		o.state.CompleteRun(runID, "failed")
		return err
	}

	o.state.CompleteRun(runID, "success")
	fmt.Println("Migration complete!")

	return nil
}

func (o *Orchestrator) transferAll(ctx context.Context, runID string, tables []source.Table) error {
	var jobs []transfer.Job

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

			for _, p := range partitions {
				jobs = append(jobs, transfer.Job{
					Table:     t,
					Partition: &p,
				})
			}
		} else {
			jobs = append(jobs, transfer.Job{
				Table:     t,
				Partition: nil,
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

			err := transfer.Execute(ctx, o.sourcePool, o.targetPool, o.config, j, o.progress)
			if err != nil {
				errCh <- fmt.Errorf("transfer %s: %w", j.Table.FullName(), err)
			}
		}(job)
	}

	wg.Wait()
	close(errCh)

	// Collect errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("%d transfer errors: %v", len(errs), errs[0])
	}

	o.progress.Finish()
	return nil
}

func (o *Orchestrator) finalize(ctx context.Context, tables []source.Table) error {
	for _, t := range tables {
		// Convert to logged table
		if err := o.targetPool.ConvertToLogged(ctx, o.config.Target.Schema, t.Name); err != nil {
			fmt.Printf("Warning: converting %s to logged: %v\n", t.Name, err)
		}

		// Reset sequences
		if err := o.targetPool.ResetSequence(ctx, o.config.Target.Schema, &t); err != nil {
			fmt.Printf("Warning: resetting sequence for %s: %v\n", t.Name, err)
		}

		// Create primary key
		if err := o.targetPool.CreatePrimaryKey(ctx, &t, o.config.Target.Schema); err != nil {
			fmt.Printf("Warning: creating PK for %s: %v\n", t.Name, err)
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
			fmt.Printf("%-30s ✓ %d rows\n", t.Name, targetCount)
		} else {
			fmt.Printf("%-30s ✗ source=%d target=%d (diff=%d)\n",
				t.Name, t.RowCount, targetCount, t.RowCount-targetCount)
			failed = true
		}
	}

	if failed {
		return fmt.Errorf("validation failed")
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
		return fmt.Errorf("no incomplete run found")
	}

	fmt.Printf("Resuming run: %s (started %s)\n", run.ID, run.StartedAt.Format(time.RFC3339))

	// Reset running tasks to pending
	tasks, err := o.state.GetPendingTasks(run.ID)
	if err != nil {
		return err
	}

	fmt.Printf("Found %d pending tasks\n", len(tasks))

	// Continue with run
	return o.Run(ctx)
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
