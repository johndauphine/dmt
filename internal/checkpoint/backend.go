package checkpoint

import (
	"encoding/json"
	"time"
)

// StateBackend defines the interface for state persistence.
// Implementations include SQLite (full featured) and file-based (minimal, for Airflow).
type StateBackend interface {
	// Run management
	CreateRun(id, sourceSchema, targetSchema string, config any, profileName, configPath string) error
	CompleteRun(id string, status string, errorMsg string) error
	GetLastIncompleteRun() (*Run, error)
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
	SaveTransferProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64) error
	GetTransferProgress(taskID int64) (*TransferProgress, error)
	ClearTransferProgress(taskID int64) error // Clear progress for fresh re-transfer

	// History (optional - file backend may return empty)
	GetAllRuns() ([]Run, error)
	GetRunByID(runID string) (*Run, error)

	// Date-based incremental sync (optional - file backend returns nil/no-op)
	GetLastSyncTimestamp(sourceSchema, tableName, targetSchema string) (*time.Time, error)
	UpdateSyncTimestamp(sourceSchema, tableName, targetSchema string, ts time.Time) error

	// Lifecycle
	Close() error

	// AI adjustment history (optional - file backend returns empty/no-op)
	SaveAIAdjustment(runID string, record AIAdjustmentRecord) error
	GetAIAdjustments(limit int) ([]AIAdjustmentRecord, error)
	GetAIAdjustmentsByAction(action string, limit int) ([]AIAdjustmentRecord, error)

	// AI tuning history for analyze command (optional - file backend returns empty/no-op)
	SaveAITuning(record AITuningRecord) error
	GetAITuningHistory(limit int) ([]AITuningRecord, error)
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

// AITuningRecord represents a historical AI tuning recommendation from analyze command.
// This stores the context and recommendations for learning from past analyses.
type AITuningRecord struct {
	ID               int64     `json:"id"`
	Timestamp        time.Time `json:"timestamp"`
	SourceDBType     string    `json:"source_db_type"`
	TargetDBType     string    `json:"target_db_type"`
	TotalTables      int       `json:"total_tables"`
	TotalRows        int64     `json:"total_rows"`
	AvgRowSizeBytes  int64     `json:"avg_row_size_bytes"`
	CPUCores         int       `json:"cpu_cores"`
	MemoryGB         int       `json:"memory_gb"`

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
	AIReasoning string `json:"ai_reasoning"`
	WasAIUsed   bool   `json:"was_ai_used"` // Whether AI was used or formula fallback
}

// AIAdjustmentRecord represents a historical AI adjustment decision.
type AIAdjustmentRecord struct {
	ID               int64          `json:"id"`
	RunID            string         `json:"run_id"`
	AdjustmentNumber int            `json:"adjustment_number"`
	Timestamp        time.Time      `json:"timestamp"`
	Action           string         `json:"action"`
	Adjustments      map[string]int `json:"adjustments"`
	ThroughputBefore float64        `json:"throughput_before"`
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
func (r AIAdjustmentRecord) AdjustmentsJSON() string {
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
