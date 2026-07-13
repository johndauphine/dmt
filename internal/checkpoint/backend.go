package checkpoint

import (
	"encoding/json"
	"time"
)

// BackendCapabilities names the behaviors a state backend can provide. The
// restartability capabilities are required for every StateBackend; history and
// profile capabilities are optional and callers must not infer support from
// no-op methods.
type BackendCapabilities struct {
	// Required for restart and status correctness.
	RunLifecycle         bool
	TaskLifecycle        bool
	TransferProgress     bool
	PartitionProgress    bool
	SyncTimestamps       bool
	DeleteReconciliation bool
	SchemaSnapshots      bool
	FallbackEvents       bool

	// Optional history/config features.
	RunHistory               bool
	RunConfigSnapshots       bool
	Profiles                 bool
	RuntimeAdjustmentHistory bool
	TuningHistory            bool
}

// HasRequiredRestartability returns true when all capabilities needed for safe
// resume/status behavior are available.
func (c BackendCapabilities) HasRequiredRestartability() bool {
	return c.RunLifecycle &&
		c.TaskLifecycle &&
		c.TransferProgress &&
		c.PartitionProgress &&
		c.SyncTimestamps &&
		c.DeleteReconciliation &&
		c.SchemaSnapshots &&
		c.FallbackEvents
}

// StateBackend defines the interface for state persistence.
// Implementations include SQLite (full featured) and file-based (minimal, for Airflow).
type StateBackend interface {
	Capabilities() BackendCapabilities

	// Run management
	CreateRun(id, sourceSchema, targetSchema string, config any, profileName, configPath string) error
	UpdateRunConfig(id string, config any) error // Persist post-AI-tuning config snapshot
	CompleteRun(id string, status string, errorMsg string) error
	CompleteRunResumable(id string, status string, errorMsg string, reason string) error
	AbandonRun(id string, reason string) error
	GetLastIncompleteRun() (*Run, error)
	UpdateRunHeartbeat(runID string, at time.Time) error
	HasSuccessfulRunAfter(run *Run) (bool, error) // Check if a successful run supersedes this incomplete run
	MarkRunAsResumed(runID string) error
	UpdatePhase(runID, phase string) error

	// Task management
	CreateTask(runID, taskType, taskKey string) (int64, error)
	UpdateTaskStatus(taskID int64, status string, errorMsg string) error
	MarkTaskComplete(runID, taskKey string) error
	GetCompletedTables(runID string) (map[string]bool, error)
	GetRunStats(runID string) (total, pending, running, success, failed int, err error)
	GetTasksWithProgress(runID string) ([]TaskWithProgress, error)

	// Progress tracking (for chunk-level resume)
	// rangeState is the keyset per-range watermark JSON (#464); "" for
	// ROW_NUMBER tasks and stores as NULL.
	SaveTransferProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64, rangeState string) error
	GetTransferProgress(taskID int64) (*TransferProgress, error)
	// GetPartitionTransferProgressSummary aggregates saved partition progress
	// for one table.
	GetPartitionTransferProgressSummary(runID, tableTaskKey string) (PartitionProgressSummary, error)
	ClearTransferProgress(taskID int64) error                        // Clear progress for fresh re-transfer
	ClearPartitionTransferProgress(runID, tableTaskKey string) error // Clear ALL partition-level progress for a table (#227 resume preflight)
	CountPartitionTasks(runID, taskKeyPrefix string) (int, error)    // Count partition tasks for a table

	// History. Check Capabilities().RunHistory before expecting historical
	// runs beyond the current file-backed run.
	GetAllRuns() ([]Run, error)
	// GetRunsPage returns one status-filtered page plus the total match count.
	GetRunsPage(status string, limit, offset int) ([]Run, int, error)
	GetRunByID(runID string) (*Run, error)

	// Date-based incremental sync.
	GetLastSyncTimestamp(sourceSchema, tableName, targetSchema string) (*time.Time, error)
	UpdateSyncTimestamp(sourceSchema, tableName, targetSchema string, ts time.Time) error

	// Immutable incremental upper fence, per run (#647). Set once when a run
	// first builds a table's incremental job; read back unchanged on resume.
	GetIncrementalFence(runID, sourceSchema, tableName, targetSchema string) (*time.Time, error)
	SetIncrementalFence(runID, sourceSchema, tableName, targetSchema string, upper time.Time) error

	// Delete reconciliation state (#351). Stores the last successful
	// reconciliation per source/target schema pair so interval scheduling is
	// stable across retries, resumes, and separate CLI invocations.
	GetDeleteReconciliationState(sourceSchema, targetSchema string) (*DeleteReconciliationState, error)
	RecordDeleteReconciliationSuccess(runID, sourceSchema, targetSchema string, completedAt time.Time) error
	SaveDeleteReconciliationTable(runID string, record DeleteReconciliationTableRecord) error
	GetDeleteReconciliationTables(runID string) ([]DeleteReconciliationTableRecord, error)

	// Source schema drift snapshots (#305). Stored per source schema/table so a
	// fresh run can compare the current source definition to the last successful
	// source definition before it transfers data.
	SaveSchemaSnapshot(runID, sourceSchema, tableName, schemaJSON string) error
	GetLatestSchemaSnapshots(sourceSchema string) ([]SchemaSnapshotRecord, error)

	// Lifecycle
	Close() error

	// AI adjustment history. Check Capabilities().RuntimeAdjustmentHistory before
	// expecting durable history; unsupported backends return empty/no-op.
	SaveRuntimeAdjustment(runID string, record RuntimeAdjustmentRecord) error
	GetRuntimeAdjustments(limit int) ([]RuntimeAdjustmentRecord, error)
	GetRuntimeAdjustmentsByAction(action string, limit int) ([]RuntimeAdjustmentRecord, error)

	// tuning history for analyze command. Check Capabilities().TuningHistory
	// before expecting durable history; unsupported backends return empty/no-op.
	SaveTuningRecord(record TuningRecord) (int64, error)
	GetTuningHistory(limit int, sourceType, targetType string) ([]TuningRecord, error)
	// GetAITuningAggregatesByWaw returns per-write_ahead_writers aggregates over the
	// FULL ai_tuning_history (no limit). Pulled via SQL GROUP BY so the smartconfig
	// can show bounded recent trajectory rows in the prompt while still presenting
	// honest retry-rate denominators across all history (issue #141).
	GetAITuningAggregatesByWaw(sourceType, targetType string) ([]WawAggregateRecord, error)
	// GetAITuningAggregatesByChunkSize returns per-chunk_size aggregates over the
	// FULL ai_tuning_history. Same rationale as GetAITuningAggregatesByWaw.
	GetAITuningAggregatesByChunkSize(sourceType, targetType string) ([]ChunkSizeAggregateRecord, error)
	// adjustedAtRuntime flags the row as runtime-adjusted (#451): the
	// run's throughput blends multiple configs, so the deterministic
	// tuner excludes the row from its training cohorts.
	UpdateTuningResult(rowID int64, throughput float64, durationSecs float64, chunkRetryCount int, adjustedAtRuntime bool) error

	// AI fallback events (#176). UPSERT semantics: each call bumps the
	// count for (run_id, surface, fingerprint). The file backend is the
	// Airflow-friendly path and must persist these so a separate
	// ``dmt status'' poll sees the running migration's counters; the
	// SQLite backend covers the same need for TUI / desktop use.
	SaveFallbackEvent(runID, surface, fingerprint string) error
	GetFallbackEventsByRun(runID string) ([]FallbackEventRecord, error)
}

// StrictSnapshotState persists the evidence needed to validate a strict
// transfer against the source snapshot it actually copied. It remains a
// narrow extension rather than expanding StateBackend so focused test fakes
// and third-party adapters do not silently claim this correctness capability.
// Production SQLite and FileState implementations both provide it (#664).
type StrictSnapshotState interface {
	// SetRunStrictConsistency records that the run's transfer jobs use strict
	// source snapshots. It is set once when a new run owns its migration lease.
	SetRunStrictConsistency(runID string, strict bool) error

	// SaveStrictSnapshotRowCount records the exact source count observed in the
	// pinned table snapshot. A retry may replace it with the retry's snapshot.
	SaveStrictSnapshotRowCount(taskID, rowCount int64) error

	// GetStrictSnapshotRowCount returns the table count from one strict run. An
	// empty runID means the most recent matching run, used by `dmt validate`
	// after the original process has exited. Nil means no usable strict
	// full-table snapshot exists (for example an incremental DateFilter job).
	GetStrictSnapshotRowCount(runID, sourceSchema, targetSchema, tableName string) (*int64, error)
}

// FallbackEventRecord is one (run, surface, fingerprint) row from
// fallback_events. Surface is one of observability.Surface* values
// (typemap | ddl | errordiag). Count is the number of times that
// exact fingerprint fired during the run. Fingerprint is "" for
// call sites that pass no fingerprint (the counter still aggregates
// under one row for that surface).
type FallbackEventRecord struct {
	RunID       string    `json:"run_id"`
	Surface     string    `json:"surface"`
	Fingerprint string    `json:"fingerprint"`
	Count       int64     `json:"count"`
	FirstSeen   time.Time `json:"first_seen"`
	LastSeen    time.Time `json:"last_seen"`
}

// SchemaSnapshotRecord is the latest persisted source schema shape for
// one table. SchemaJSON is deterministic JSON produced by the drift package.
type SchemaSnapshotRecord struct {
	RunID        string    `json:"run_id"`
	SourceSchema string    `json:"source_schema"`
	TableName    string    `json:"table_name"`
	CapturedAt   time.Time `json:"captured_at"`
	SchemaJSON   string    `json:"schema_json"`
}

// DeleteReconciliationState records the latest successful hard-delete
// reconciliation for a source/target schema pair.
type DeleteReconciliationState struct {
	SourceSchema  string    `json:"source_schema"`
	TargetSchema  string    `json:"target_schema"`
	LastRunID     string    `json:"last_run_id"`
	LastSuccessAt time.Time `json:"last_success_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// DeleteReconciliationTableRecord records one table's delete reconciliation
// result for a run.
type DeleteReconciliationTableRecord struct {
	RunID         string    `json:"run_id"`
	TableName     string    `json:"table_name"`
	CandidateRows int64     `json:"candidate_rows"`
	DeletedRows   int64     `json:"deleted_rows"`
	Skipped       bool      `json:"skipped"`
	SkipReason    string    `json:"skip_reason,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// PartitionProgressSummary summarizes saved progress across a table's
// partition transfer tasks.
type PartitionProgressSummary struct {
	RowsDone               int64
	PartitionsWithProgress int
}

func (s PartitionProgressSummary) HasProgress() bool {
	return s.PartitionsWithProgress > 0
}

// HistoryBackend extends StateBackend with profile management.
// Only SQLite implements this; file backend does not support profiles.
type HistoryBackend interface {
	StateBackend

	// Profile management (encrypted config storage)
	SaveProfile(name, description string, config []byte) error
	GetProfile(name string) ([]byte, error)
	ListProfiles() ([]ProfileInfo, error)
	DeleteProfile(name string) error
}

// Ensure State implements HistoryBackend
var _ HistoryBackend = (*State)(nil)

// TuningRecord represents historical tuning data from completed migrations.
// These completed migration measurements feed future tuning recommendations.
type TuningRecord struct {
	ID              int64     `json:"id"`
	Timestamp       time.Time `json:"timestamp"`
	SourceDBType    string    `json:"source_db_type"`
	TargetDBType    string    `json:"target_db_type"`
	TotalTables     int       `json:"total_tables"`
	TotalRows       int64     `json:"total_rows"`
	AvgRowSizeBytes int64     `json:"avg_row_size_bytes"`
	CPUCores        int       `json:"cpu_cores"`
	MemoryGB        int       `json:"memory_gb"`

	// Recommended parameters
	Workers             int   `json:"workers"`
	ChunkSize           int   `json:"chunk_size"`
	ReadAheadBuffers    int   `json:"read_ahead_buffers"`
	WriteAheadWriters   int   `json:"write_ahead_writers"`
	ParallelReaders     int   `json:"parallel_readers"`
	MaxPartitions       int   `json:"max_partitions"`
	LargeTableThreshold int64 `json:"large_table_threshold"`
	MaxSourceConns      int   `json:"max_source_connections"`
	MaxTargetConns      int   `json:"max_target_connections"`
	EstimatedMemoryMB   int64 `json:"estimated_memory_mb"`

	// AI metadata
	Reasoning string `json:"ai_reasoning"`
	WasAIUsed bool   `json:"was_ai_used"` // Whether AI was used or formula fallback

	// Post-migration results (updated after run completes)
	FinalThroughput   float64 `json:"final_throughput,omitempty"`       // rows/sec from completed migration
	FinalDurationSecs float64 `json:"final_duration_seconds,omitempty"` // total migration duration in seconds
	ChunkRetryCount   int     `json:"chunk_retry_count,omitempty"`      // chunk retries observed during the run (0 = clean)
	// AdjustedAtRuntime is true when the runtime controller (or write-error
	// adjuster) changed parameters mid-run (#451). The run's throughput is a
	// blend across configs, so the tuner must not attribute it to the
	// recorded parameters. False for pre-migration rows.
	AdjustedAtRuntime bool `json:"adjusted_at_runtime,omitempty"`

	// Effective DB tuning captured at run start. Used by the smartconfig
	// trajectory rendering to compare history against the current run's
	// regime (#144 follow-up). Empty/zero values indicate the field was
	// either unknown at capture time or persisted before this column existed.
	Platform                string `json:"platform,omitempty"`
	TargetSharedBuffersMB   int64  `json:"target_shared_buffers_mb,omitempty"`
	TargetSyncCommit        string `json:"target_synchronous_commit,omitempty"`
	TargetFsync             string `json:"target_fsync,omitempty"`
	TargetFullPageWrites    string `json:"target_full_page_writes,omitempty"`
	TargetMaxWALSizeMB      int64  `json:"target_max_wal_size_mb,omitempty"`
	TargetWALLevel          string `json:"target_wal_level,omitempty"`
	SourceMaxServerMemoryMB int64  `json:"source_max_server_memory_mb,omitempty"`

	// Workload identity (#215). Together these form the tuple the
	// Tier 1 classifier uses to find historically-comparable runs:
	// same exact (source endpoint, target endpoint) = same workload.
	// Empty values indicate either pre-#215 rows whose identity is
	// unrecoverable (correctly excluded from Tier 1 matches by SQL
	// equality semantics) or runs that didn't propagate the identity
	// for some reason. Stored verbatim as the user wrote them in the
	// config; not normalized (no localhost↔127.0.0.1 fold, no
	// case-fold for case-sensitive dialects like PG).
	SourceHost     string `json:"source_host,omitempty"`
	SourcePort     int    `json:"source_port,omitempty"`
	SourceDatabase string `json:"source_database,omitempty"`
	SourceSchema   string `json:"source_schema,omitempty"`
	TargetHost     string `json:"target_host,omitempty"`
	TargetPort     int    `json:"target_port,omitempty"`
	TargetDatabase string `json:"target_database,omitempty"`
	TargetSchema   string `json:"target_schema,omitempty"`
}

// WawAggregateRecord pre-aggregates ai_tuning_history rows by write_ahead_writers.
// Used by smartconfig to keep retry-rate denominators honest while bounding the
// per-row trajectory in the prompt (issue #141). Ordered by WriteAheadWriters ASC.
type WawAggregateRecord struct {
	WriteAheadWriters int     `json:"write_ahead_writers"`
	TotalRuns         int     `json:"total_runs"`
	RunsWithRetries   int     `json:"runs_with_retries"`
	TotalRetries      int     `json:"total_retries"`
	PeakThroughput    float64 `json:"peak_throughput"`
	MeanThroughput    float64 `json:"mean_throughput"`
}

// ChunkSizeAggregateRecord pre-aggregates ai_tuning_history rows by chunk_size.
// Ordered by ChunkSize ASC.
type ChunkSizeAggregateRecord struct {
	ChunkSize     int     `json:"chunk_size"`
	Runs          int     `json:"runs"`
	AvgThroughput float64 `json:"avg_throughput"`
}

// RuntimeAdjustmentRecord represents a historical runtime adjustment decision.
type RuntimeAdjustmentRecord struct {
	ID               int64          `json:"id"`
	RunID            string         `json:"run_id"`
	AdjustmentNumber int            `json:"adjustment_number"`
	Timestamp        time.Time      `json:"timestamp"`
	Action           string         `json:"action"`
	Adjustments      map[string]int `json:"adjustments"`
	ThroughputBefore float64        `json:"throughput_before"`
	EffectMeasured   bool           `json:"effect_measured"`
	ThroughputAfter  float64        `json:"throughput_after"`
	EffectPercent    float64        `json:"effect_percent"`
	CPUBefore        float64        `json:"cpu_before"`
	CPUAfter         float64        `json:"cpu_after"`
	MemoryBefore     float64        `json:"memory_before"`
	MemoryAfter      float64        `json:"memory_after"`
	Reasoning        string         `json:"reasoning"`
	Confidence       string         `json:"confidence"`
}

// AdjustmentsJSON returns the adjustments as a JSON string for storage.
func (r RuntimeAdjustmentRecord) AdjustmentsJSON() string {
	if r.Adjustments == nil {
		return "{}"
	}
	b, err := json.Marshal(r.Adjustments)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// ParseAdjustments parses a JSON string into the adjustments map.
func ParseAdjustments(s string) map[string]int {
	var m map[string]int
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return make(map[string]int)
	}
	return m
}
