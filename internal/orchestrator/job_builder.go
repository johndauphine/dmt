package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/source"
	"github.com/johndauphine/dmt/internal/transfer"
)

// JobBuilder creates transfer jobs from tables.
// It handles date filtering, partitioning, and task creation.
type JobBuilder struct {
	sourcePool pool.SourcePool
	state      checkpoint.StateBackend
	config     *config.Config
}

// NewJobBuilder creates a new JobBuilder.
func NewJobBuilder(sourcePool pool.SourcePool, state checkpoint.StateBackend, cfg *config.Config) *JobBuilder {
	return &JobBuilder{
		sourcePool: sourcePool,
		state:      state,
		config:     cfg,
	}
}

// BuildResult contains the jobs and metadata from building.
type BuildResult struct {
	Jobs                []transfer.Job
	TableDateFilters    map[string]*transfer.DateFilter
	TableSyncStartTimes map[string]time.Time
	TableJobCounts      map[string]int
	ProgressSaver       *checkpoint.ProgressSaver
	Summary             JobSummary
}

// JobSummary contains summary statistics about job building.
type JobSummary struct {
	TablesIncremental  int
	TablesFirstSync    int
	TablesNoDateColumn int
	NoDateColumnTables []string
}

// Build creates transfer jobs for the given tables.
func (b *JobBuilder) Build(ctx context.Context, runID string, tables []source.Table) (*BuildResult, error) {
	result := &BuildResult{
		Jobs:                make([]transfer.Job, 0),
		TableDateFilters:    make(map[string]*transfer.DateFilter),
		TableSyncStartTimes: make(map[string]time.Time),
		TableJobCounts:      make(map[string]int),
		ProgressSaver:       checkpoint.NewProgressSaver(b.state),
	}

	// Check if date column tracking is enabled
	dateTrackingEnabled := len(b.config.Migration.DateUpdatedColumns) > 0
	applyDateFilter := b.config.Migration.TargetMode == "upsert"

	// Count and log partitioning info
	var largeKeysetTables int
	for _, t := range tables {
		if t.IsLarge(b.config.Migration.LargeTableThreshold) && t.SupportsKeysetPagination() {
			largeKeysetTables++
		}
	}

	var partitionStartTime time.Time
	if largeKeysetTables > 0 {
		logging.Debug("Calculating partition boundaries for %d large tables...", largeKeysetTables)
		partitionStartTime = time.Now()
	}

	// Build jobs for each table
	for _, t := range tables {
		logging.Debug("Processing table %s (rows=%d, large=%v)",
			t.Name, t.RowCount, t.IsLarge(b.config.Migration.LargeTableThreshold))

		// Determine date filter
		dateFilter := b.buildDateFilter(ctx, &t, dateTrackingEnabled, applyDateFilter, result)

		// Create jobs based on table size and PK type
		if err := b.createJobsForTable(ctx, runID, t, dateFilter, result); err != nil {
			return nil, err
		}
	}

	// Log partition query time
	if largeKeysetTables > 0 {
		logging.Debug("Partition queries completed in %s", time.Since(partitionStartTime).Round(time.Millisecond))
	}

	// Log date tracking summary
	b.logDateTrackingSummary(dateTrackingEnabled, applyDateFilter, &result.Summary)

	return result, nil
}

// buildDateFilter determines the date filter for a table.
func (b *JobBuilder) buildDateFilter(ctx context.Context, t *source.Table, dateTrackingEnabled, applyDateFilter bool, result *BuildResult) *transfer.DateFilter {
	if !dateTrackingEnabled {
		return nil
	}

	colName, colType, found := b.sourcePool.GetDateColumnInfo(
		ctx, t.Schema, t.Name, b.config.Migration.DateUpdatedColumns)

	if !found {
		result.Summary.TablesNoDateColumn++
		result.Summary.NoDateColumnTables = append(result.Summary.NoDateColumnTables, t.Name)
		if applyDateFilter {
			logging.Debug("Table %s: full sync - no date column, syncing all %d rows (repeated each run)",
				t.Name, t.RowCount)
		} else {
			logging.Debug("Table %s: full load - no date column configured", t.Name)
		}
		return nil
	}

	t.DateColumn = colName
	t.DateColumnType = colType

	// Record sync start time
	syncStartTime := time.Now().UTC()
	result.TableSyncStartTimes[t.Name] = syncStartTime
	result.TableDateFilters[t.Name] = nil // Mark for timestamp recording

	if !applyDateFilter {
		result.Summary.TablesFirstSync++
		logging.Debug("Table %s: full load - recording %s timestamp for future incremental syncs",
			t.Name, colName)
		return nil
	}

	// Get last sync timestamp
	lastSync, err := b.state.GetLastSyncTimestamp(t.Schema, t.Name, b.config.Target.Schema)
	if err != nil {
		logging.Warn("Failed to get last sync timestamp for %s: %v", t.Name, err)
	}

	if lastSync != nil {
		dateFilter := &transfer.DateFilter{
			Column:    colName,
			Timestamp: *lastSync,
		}
		result.TableDateFilters[t.Name] = dateFilter
		result.Summary.TablesIncremental++
		logging.Debug("Table %s: incremental - syncing rows where %s > %v",
			t.Name, colName, lastSync.Format(time.RFC3339))
		return dateFilter
	}

	// First sync
	result.Summary.TablesFirstSync++
	logging.Debug("Table %s: first sync - loading all %d rows, will use %s for future incremental syncs",
		t.Name, t.RowCount, colName)
	return nil
}

// createJobsForTable creates transfer jobs for a single table.
func (b *JobBuilder) createJobsForTable(ctx context.Context, runID string, t source.Table, dateFilter *transfer.DateFilter, result *BuildResult) error {
	if t.IsLarge(b.config.Migration.LargeTableThreshold) && t.SupportsKeysetPagination() {
		return b.createKeysetPartitionJobs(ctx, runID, t, dateFilter, result)
	}

	if t.IsLarge(b.config.Migration.LargeTableThreshold) && t.HasPK() {
		return b.createRowNumberPartitionJobs(runID, t, dateFilter, result)
	}

	return b.createSingleJob(runID, t, dateFilter, result)
}

// createKeysetPartitionJobs creates jobs using keyset pagination.
func (b *JobBuilder) createKeysetPartitionJobs(ctx context.Context, runID string, t source.Table, dateFilter *transfer.DateFilter, result *BuildResult) error {
	numPartitions := min(
		int(t.RowCount/int64(b.config.Migration.ChunkSize))+1,
		b.config.Migration.MaxPartitions,
	)

	tableStart := time.Now()
	partitions, err := b.sourcePool.GetPartitionBoundaries(ctx, &t, numPartitions)
	tableElapsed := time.Since(tableStart)

	if err != nil {
		return fmt.Errorf("partitioning %s: %w", t.FullName(), err)
	}

	// If partitions is nil (e.g., query timed out), fall back to single job
	if partitions == nil || len(partitions) == 0 {
		logging.Debug("  %s: no partitions (timeout or error) - using single job (%s)", t.Name, tableElapsed.Round(time.Millisecond))
		return b.createSingleJob(runID, t, dateFilter, result)
	}

	logging.Debug("  %s: %d partitions (%s)", t.Name, len(partitions), tableElapsed.Round(time.Millisecond))

	result.TableJobCounts[t.Name] = len(partitions)
	for _, p := range partitions {
		// Mark the first partition for coordinated cleanup during retries
		p.IsFirstPartition = (p.PartitionID == 1)

		taskKey := fmt.Sprintf("transfer:%s.%s:p%d", t.Schema, t.Name, p.PartitionID)
		taskID, _ := b.state.CreateTask(runID, "transfer", taskKey)

		result.Jobs = append(result.Jobs, transfer.Job{
			Table:      t,
			Partition:  &p,
			TaskID:     taskID,
			Saver:      result.ProgressSaver,
			DateFilter: dateFilter,
		})
	}

	return nil
}

// createRowNumberPartitionJobs creates jobs using ROW_NUMBER pagination.
func (b *JobBuilder) createRowNumberPartitionJobs(runID string, t source.Table, dateFilter *transfer.DateFilter, result *BuildResult) error {
	numPartitions := min(
		int(t.RowCount/int64(b.config.Migration.ChunkSize))+1,
		b.config.Migration.MaxPartitions,
	)
	if numPartitions < 1 {
		numPartitions = 1
	}

	logging.Debug("  Partitioning %s (%d rows, %d partitions, row-number)...", t.Name, t.RowCount, numPartitions)
	rowsPerPartition := t.RowCount / int64(numPartitions)
	result.TableJobCounts[t.Name] = numPartitions

	for i := 0; i < numPartitions; i++ {
		startRow := int64(i) * rowsPerPartition
		endRow := startRow + rowsPerPartition
		if i == numPartitions-1 {
			endRow = t.RowCount
		}

		p := source.Partition{
			TableName:        t.Name,
			PartitionID:      i + 1,
			StartRow:         startRow,
			EndRow:           endRow,
			RowCount:         endRow - startRow,
			IsFirstPartition: i == 0, // First partition for coordinated cleanup during retries
		}

		taskKey := fmt.Sprintf("transfer:%s.%s:p%d", t.Schema, t.Name, p.PartitionID)
		taskID, _ := b.state.CreateTask(runID, "transfer", taskKey)

		result.Jobs = append(result.Jobs, transfer.Job{
			Table:      t,
			Partition:  &p,
			TaskID:     taskID,
			Saver:      result.ProgressSaver,
			DateFilter: dateFilter,
		})
	}

	return nil
}

// createSingleJob creates a single job for a table.
func (b *JobBuilder) createSingleJob(runID string, t source.Table, dateFilter *transfer.DateFilter, result *BuildResult) error {
	result.TableJobCounts[t.Name] = 1
	taskKey := fmt.Sprintf("transfer:%s.%s", t.Schema, t.Name)
	taskID, _ := b.state.CreateTask(runID, "transfer", taskKey)

	result.Jobs = append(result.Jobs, transfer.Job{
		Table:      t,
		Partition:  nil,
		TaskID:     taskID,
		Saver:      result.ProgressSaver,
		DateFilter: dateFilter,
	})

	return nil
}

// logDateTrackingSummary logs summary of date tracking configuration.
func (b *JobBuilder) logDateTrackingSummary(dateTrackingEnabled, applyDateFilter bool, summary *JobSummary) {
	if !dateTrackingEnabled {
		return
	}

	if summary.TablesIncremental > 0 || summary.TablesFirstSync > 0 || summary.TablesNoDateColumn > 0 {
		if applyDateFilter {
			logging.Debug("Incremental sync summary: %d tables incremental, %d tables first sync, %d tables full sync (no date column)",
				summary.TablesIncremental, summary.TablesFirstSync, summary.TablesNoDateColumn)
		} else {
			logging.Debug("Timestamp recording: %d tables with date columns, %d tables without",
				summary.TablesFirstSync, summary.TablesNoDateColumn)
		}
	}

	if summary.TablesNoDateColumn > 0 && applyDateFilter {
		logging.Debug("Tables without date columns will sync all rows every run: %v", summary.NoDateColumnTables)
	}
}
