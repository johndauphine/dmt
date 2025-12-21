package checkpoint

// StateBackend defines the interface for state persistence.
// Implementations include SQLite (full featured) and file-based (minimal, for Airflow).
type StateBackend interface {
	// Run management
	CreateRun(id, sourceSchema, targetSchema string, config any, profileName, configPath string) error
	CompleteRun(id string, status string) error
	GetLastIncompleteRun() (*Run, error)
	MarkRunAsResumed(runID string) error

	// Task management
	CreateTask(runID, taskType, taskKey string) (int64, error)
	UpdateTaskStatus(taskID int64, status string, errorMsg string) error
	MarkTaskComplete(runID, taskKey string) error
	GetCompletedTables(runID string) (map[string]bool, error)
	GetRunStats(runID string) (total, pending, running, success, failed int, err error)

	// Progress tracking (for chunk-level resume)
	SaveTransferProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64) error
	GetTransferProgress(taskID int64) (*TransferProgress, error)

	// History (optional - file backend may return empty)
	GetAllRuns() ([]Run, error)
	GetRunByID(runID string) (*Run, error)

	// Lifecycle
	Close() error
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
