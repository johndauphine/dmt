package orchestrator

import (
	"context"
	"fmt"
	"github.com/johndauphine/dmt/internal/driver"
	"strings"
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
	TableSyncWatermarks map[string]time.Time
	TableJobCounts      map[string]int
	TableTaskIDs        map[string]int64
	TableTaskKeys       map[string]string
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

func (r *BuildResult) recordTableTask(table string, taskID int64, taskKey string) {
	if r.TableTaskIDs == nil {
		r.TableTaskIDs = make(map[string]int64)
	}
	if r.TableTaskKeys == nil {
		r.TableTaskKeys = make(map[string]string)
	}
	r.TableTaskIDs[table] = taskID
	r.TableTaskKeys[table] = taskKey
}

// Build creates transfer jobs for the given tables.
func (b *JobBuilder) Build(ctx context.Context, runID string, tables []source.Table) (*BuildResult, error) {
	result := &BuildResult{
		Jobs:                make([]transfer.Job, 0),
		TableDateFilters:    make(map[string]*transfer.DateFilter),
		TableSyncWatermarks: make(map[string]time.Time),
		TableJobCounts:      make(map[string]int),
		TableTaskIDs:        make(map[string]int64),
		TableTaskKeys:       make(map[string]string),
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
		dateFilter, err := b.buildDateFilter(ctx, runID, &t, dateTrackingEnabled, applyDateFilter, result)
		if err != nil {
			return nil, err
		}

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
func (b *JobBuilder) buildDateFilter(ctx context.Context, runID string, t *source.Table, dateTrackingEnabled, applyDateFilter bool, result *BuildResult) (*transfer.DateFilter, error) {
	if !dateTrackingEnabled {
		return nil, nil
	}

	// Date tracking is an optional source capability (#476). Without it
	// the table takes the existing "no date column" full-sync path; the
	// builder logs the engine-level reason once via logDateTrackingSummary.
	dateReader, ok := b.sourcePool.(driver.IncrementalDateReader)
	if !ok {
		result.Summary.TablesNoDateColumn++
		result.Summary.NoDateColumnTables = append(result.Summary.NoDateColumnTables, t.Name)
		logging.Debug("Table %s: full sync - source engine %s does not support date-column tracking", t.Name, b.sourcePool.DBType())
		return nil, nil
	}

	candidates := dateColumnCandidatesForTable(t, b.config.Migration.DateUpdatedColumns)
	colName, colType, found := dateReader.GetDateColumnInfo(
		ctx, t.Schema, t.Name, candidates)

	if !found {
		result.Summary.TablesNoDateColumn++
		result.Summary.NoDateColumnTables = append(result.Summary.NoDateColumnTables, t.Name)
		if applyDateFilter {
			logging.Debug("Table %s: full sync - no date column, syncing all %d rows (repeated each run)",
				t.Name, t.RowCount)
		} else {
			logging.Debug("Table %s: full load - no date column configured", t.Name)
		}
		return nil, nil
	}

	t.DateColumn = colName
	t.DateColumnType = colType

	result.TableDateFilters[t.Name] = nil // Mark for timestamp recording

	lastSync := b.lastSyncTimestamp(t, applyDateFilter)

	if applyDateFilter && lastSync != nil {
		// Incremental sync: read rows where updated_at > T0. The immutable fence
		// H1 is sampled once at run start and used only as the watermark this
		// run may advance to — never a value re-sampled on resume. That, plus
		// replaying the window on resume (see transfer.Execute), prevents a row
		// updated behind the positional cursor from being permanently lost
		// (#647).
		dateFilter := &transfer.DateFilter{
			Column:    colName,
			Timestamp: *lastSync,
		}
		fence, err := b.incrementalUpperFence(ctx, runID, t, colName)
		if err != nil {
			return nil, err
		}
		if fence != nil && fence.After(*lastSync) {
			// Advance the watermark only when the fence is genuinely ahead of
			// the last sync; a non-advancing fence (no new rows, or rows
			// removed) leaves the baseline untouched.
			result.TableSyncWatermarks[t.Name] = *fence
		}
		result.TableDateFilters[t.Name] = dateFilter
		result.Summary.TablesIncremental++
		if watermark, ok := result.TableSyncWatermarks[t.Name]; ok {
			logging.Debug("Table %s: incremental - syncing rows where %s > %v (watermark fence %v)",
				t.Name, colName, lastSync.Format(time.RFC3339), watermark.Format(time.RFC3339))
		} else {
			logging.Debug("Table %s: incremental - syncing rows where %s > %v (source watermark did not advance)",
				t.Name, colName, lastSync.Format(time.RFC3339))
		}
		return dateFilter, nil
	}

	// First sync or full load: record a fresh source high watermark as the
	// baseline for future incremental runs (no prior watermark to fence yet).
	b.recordSourceHighWatermark(ctx, t, colName, lastSync, result)
	result.Summary.TablesFirstSync++
	if applyDateFilter {
		logging.Debug("Table %s: first sync - loading all %d rows, will use %s for future incremental syncs",
			t.Name, t.RowCount, colName)
	} else {
		logging.Debug("Table %s: full load - recording %s timestamp for future incremental syncs",
			t.Name, colName)
	}
	return nil, nil
}

func dateColumnCandidatesForTable(t *source.Table, candidates []string) []string {
	if len(t.Columns) == 0 {
		return candidates
	}

	filtered := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if tableColumnExists(t, candidate) {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

func tableColumnExists(t *source.Table, columnName string) bool {
	for _, column := range t.Columns {
		if strings.EqualFold(column.Name, columnName) {
			return true
		}
	}
	return false
}

func (b *JobBuilder) lastSyncTimestamp(t *source.Table, applyDateFilter bool) *time.Time {
	if !applyDateFilter {
		return nil
	}

	lastSync, err := b.state.GetLastSyncTimestamp(t.Schema, t.Name, b.config.Target.Schema)
	if err != nil {
		logging.Warn("Failed to get last sync timestamp for %s: %v", t.Name, err)
		return nil
	}
	return lastSync
}

func (b *JobBuilder) recordSourceHighWatermark(ctx context.Context, t *source.Table, colName string, lastSync *time.Time, result *BuildResult) {
	dateReader, ok := b.sourcePool.(driver.IncrementalDateReader)
	if !ok {
		return
	}
	highWatermark, err := dateReader.GetMaxDateColumnValue(ctx, t.Schema, t.Name, colName)
	if err != nil {
		logging.Warn("Failed to get source high watermark for %s.%s.%s: %v", t.Schema, t.Name, colName, err)
		return
	}
	if highWatermark == nil {
		logging.Debug("Table %s: no non-NULL %s values, leaving sync timestamp unchanged", t.Name, colName)
		return
	}
	if lastSync != nil && !highWatermark.After(*lastSync) {
		logging.Debug("Table %s: source high watermark %s did not advance past %s",
			t.Name, highWatermark.Format(time.RFC3339Nano), lastSync.Format(time.RFC3339Nano))
		return
	}

	result.TableSyncWatermarks[t.Name] = *highWatermark
}

// incrementalUpperFence returns the immutable upper fence H1 for this run's
// incremental sync of t: the persisted fence on resume, or a freshly sampled
// source high watermark (persisted for future resumes) on the first build. A
// nil result means the source has no non-NULL date values to fence, in which
// case the caller keeps the lower-bound-only filter (#647).
func (b *JobBuilder) incrementalUpperFence(ctx context.Context, runID string, t *source.Table, colName string) (*time.Time, error) {
	if existing, err := b.state.GetIncrementalFence(runID, t.Schema, t.Name, b.config.Target.Schema); err != nil {
		return nil, checkpoint.RequiredWrite(
			fmt.Sprintf("loading durable incremental fence for run %s table %s.%s", runID, t.Schema, t.Name),
			err,
		)
	} else if existing != nil {
		logging.Debug("Table %s: reusing persisted incremental fence %s (resume)",
			t.Name, existing.Format(time.RFC3339Nano))
		return existing, nil
	}

	dateReader, ok := b.sourcePool.(driver.IncrementalDateReader)
	if !ok {
		return nil, nil
	}
	highWatermark, err := dateReader.GetMaxDateColumnValue(ctx, t.Schema, t.Name, colName)
	if err != nil {
		logging.Warn("Failed to sample incremental fence for %s.%s.%s: %v", t.Schema, t.Name, colName, err)
		return nil, nil
	}
	if highWatermark == nil {
		return nil, nil
	}
	if err := b.state.SetIncrementalFence(runID, t.Schema, t.Name, b.config.Target.Schema, *highWatermark); err != nil {
		return nil, checkpoint.RequiredWrite(
			fmt.Sprintf("persisting immutable incremental fence for run %s table %s.%s", runID, t.Schema, t.Name),
			err,
		)
	}
	return highWatermark, nil
}

// usesTupleKeyset reports whether table t will be paged by tuple keyset
// (#616/#629): a composite PK or single-column non-integer PK of safe types
// on a source engine whose primary keys are unique. It shares
// driver.TupleKeysetRoutable with transfer.Execute so job partitioning and
// pagination cannot diverge. (Execute additionally applies a runtime
// converter-touches-PK gate that can only narrow further — a table the
// builder left unpartitioned then runs as a single ROW_NUMBER job, which is
// slower but correct; the reverse divergence, tuple pagination over
// ROW_NUMBER partitions, is impossible.)
func (b *JobBuilder) usesTupleKeyset(t source.Table) bool {
	if b.sourcePool == nil {
		// Unit tests build a JobBuilder without a source pool; production
		// always has one. ROW_NUMBER partitioning is the safe answer here —
		// transfer.Execute (which requires a live pool) can only route to
		// tuple keyset when this returned true, so no divergence is possible.
		return false
	}
	return driver.TupleKeysetRoutable(&t, b.sourcePool.DBType())
}

// createJobsForTable creates transfer jobs for a single table.
func (b *JobBuilder) createJobsForTable(ctx context.Context, runID string, t source.Table, dateFilter *transfer.DateFilter, result *BuildResult) error {
	if b.config.Migration.StrictConsistency && b.config.Migration.StrictConsistencyScope != "migration" {
		// Table-scoped strict snapshots are one source transaction per table.
		// Partition jobs would open independent snapshots, so disable both table
		// partitioning and cross-partition concurrency for this mode (#640).
		logging.Debug("Table %s: table-scoped strict_consistency uses one unpartitioned source snapshot", t.Name)
		return b.createSingleJob(runID, t, dateFilter, result)
	}

	if t.IsLarge(b.config.Migration.LargeTableThreshold) && t.SupportsKeysetPagination() {
		return b.createKeysetPartitionJobs(ctx, runID, t, dateFilter, result)
	}

	// All-integer composite PKs page via a single-reader tuple keyset job
	// (#616), so they must be excluded from ROW_NUMBER partitioning — a
	// StartRow/EndRow partition means nothing to the tuple-keyset reader,
	// which would read the whole table per partition and duplicate rows. The
	// exclusion only applies when the source engine actually uses tuple
	// keyset (SupportsCompositeKeyset); on engines that keep ROW_NUMBER
	// (e.g. ClickHouse), composite tables must retain their partitioning.
	if t.IsLarge(b.config.Migration.LargeTableThreshold) && t.HasPK() && !b.usesTupleKeyset(t) {
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

	// Check if a previous run had a different partition count.
	// Partition boundaries are derived from PK ranges, so changing the count
	// changes boundaries. If we resume with stale checkpoints from different
	// boundaries, parallel readers will produce overlapping data.
	existingPartitions, err := checkpoint.CountTransferPartitionTasks(b.state, runID, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("counting partition tasks for %s: %w", t.FullName(), err)
	}
	if existingPartitions > 0 && existingPartitions != numPartitions {
		logging.Warn("Partition count changed for %s (%d -> %d), clearing stale checkpoints",
			t.Name, existingPartitions, numPartitions)
		// Clear progress for all old partition tasks so they restart cleanly
		for i := 1; i <= existingPartitions; i++ {
			partitionID := i
			oldTaskID, oldKey, taskErr := b.createTransferTask(runID, checkpoint.TransferTaskIdentity{Schema: t.Schema, Table: t.Name, PartitionID: &partitionID})
			if taskErr != nil {
				return taskErr
			}
			if clearErr := b.state.ClearTransferProgress(oldTaskID); clearErr != nil {
				return checkpoint.RequiredWrite(fmt.Sprintf("clearing stale progress for %s task %d", oldKey, oldTaskID), clearErr)
			}
		}
	}

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

	// Persist the aggregate table task as well as every partition task before
	// target preparation. CompleteTransferTask updates this row only after
	// every partition succeeds.
	tableTaskID, tableTaskKey, err := b.createTransferTask(runID, checkpoint.TransferTaskIdentity{Schema: t.Schema, Table: t.Name})
	if err != nil {
		return err
	}
	result.recordTableTask(t.Name, tableTaskID, tableTaskKey)
	result.TableJobCounts[t.Name] = len(partitions)
	for _, p := range partitions {
		// Mark the first partition for coordinated cleanup during retries
		p.IsFirstPartition = (p.PartitionID == 1)

		partitionID := p.PartitionID
		taskID, _, err := b.createTransferTask(runID, checkpoint.TransferTaskIdentity{Schema: t.Schema, Table: t.Name, PartitionID: &partitionID})
		if err != nil {
			return err
		}

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

	// Detect partition count changes (same logic as keyset partitions)
	existingPartitions, err := checkpoint.CountTransferPartitionTasks(b.state, runID, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("counting partition tasks for %s: %w", t.FullName(), err)
	}
	if existingPartitions > 0 && existingPartitions != numPartitions {
		logging.Warn("Partition count changed for %s (%d -> %d), clearing stale checkpoints",
			t.Name, existingPartitions, numPartitions)
		for i := 1; i <= existingPartitions; i++ {
			partitionID := i
			oldTaskID, oldKey, taskErr := b.createTransferTask(runID, checkpoint.TransferTaskIdentity{Schema: t.Schema, Table: t.Name, PartitionID: &partitionID})
			if taskErr != nil {
				return taskErr
			}
			if clearErr := b.state.ClearTransferProgress(oldTaskID); clearErr != nil {
				return checkpoint.RequiredWrite(fmt.Sprintf("clearing stale progress for %s task %d", oldKey, oldTaskID), clearErr)
			}
		}
	}

	logging.Debug("  Partitioning %s (%d rows, %d partitions, row-number)...", t.Name, t.RowCount, numPartitions)
	rowsPerPartition := t.RowCount / int64(numPartitions)
	tableTaskID, tableTaskKey, err := b.createTransferTask(runID, checkpoint.TransferTaskIdentity{Schema: t.Schema, Table: t.Name})
	if err != nil {
		return err
	}
	result.recordTableTask(t.Name, tableTaskID, tableTaskKey)
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

		partitionID := p.PartitionID
		taskID, taskKey, err := b.createTransferTask(runID, checkpoint.TransferTaskIdentity{Schema: t.Schema, Table: t.Name, PartitionID: &partitionID})
		if err != nil {
			return err
		}
		if err := b.clearRowNumberProgressOnBoundaryChange(taskID, taskKey, p.RowCount); err != nil {
			return err
		}

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

func (b *JobBuilder) clearRowNumberProgressOnBoundaryChange(taskID int64, taskKey string, rowCount int64) error {
	progress, err := b.state.GetTransferProgress(taskID)
	if err != nil {
		return fmt.Errorf("getting existing ROW_NUMBER progress for %s: %w", taskKey, err)
	}
	if progress == nil || progress.RowsTotal == 0 || progress.RowsTotal == rowCount {
		return nil
	}
	logging.Warn("ROW_NUMBER partition boundary changed for %s (rows_total %d -> %d), clearing stale checkpoint",
		taskKey, progress.RowsTotal, rowCount)
	if err := b.state.ClearTransferProgress(taskID); err != nil {
		return checkpoint.RequiredWrite(fmt.Sprintf("clearing stale ROW_NUMBER progress for %s task %d", taskKey, taskID), err)
	}
	return nil
}

// createSingleJob creates a single job for a table.
func (b *JobBuilder) createSingleJob(runID string, t source.Table, dateFilter *transfer.DateFilter, result *BuildResult) error {
	result.TableJobCounts[t.Name] = 1
	taskID, taskKey, err := b.createTransferTask(runID, checkpoint.TransferTaskIdentity{Schema: t.Schema, Table: t.Name})
	if err != nil {
		return err
	}
	result.recordTableTask(t.Name, taskID, taskKey)

	result.Jobs = append(result.Jobs, transfer.Job{
		Table:      t,
		Partition:  nil,
		TaskID:     taskID,
		Saver:      result.ProgressSaver,
		DateFilter: dateFilter,
	})

	return nil
}

func (b *JobBuilder) createTransferTask(runID string, identity checkpoint.TransferTaskIdentity) (int64, string, error) {
	taskID, taskKey, err := checkpoint.CreateTransferTask(b.state, runID, identity)
	operation := fmt.Sprintf("creating transfer task for %s.%s", identity.Schema, identity.Table)
	if identity.PartitionID != nil {
		operation += fmt.Sprintf(" partition %d", *identity.PartitionID)
	}
	if err != nil {
		return 0, taskKey, checkpoint.RequiredWrite(operation, err)
	}
	if taskID <= 0 {
		return 0, taskKey, checkpoint.RequiredWrite(operation, fmt.Errorf("backend returned non-positive task ID %d", taskID))
	}
	return taskID, taskKey, nil
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
