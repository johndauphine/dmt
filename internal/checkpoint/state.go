package checkpoint

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// State manages migration state in SQLite
type State struct {
	db *sql.DB
}

// Capabilities reports the SQLite backend's supported behavior.
func (s *State) Capabilities() BackendCapabilities {
	return BackendCapabilities{
		RunLifecycle:         true,
		TaskLifecycle:        true,
		TransferProgress:     true,
		PartitionProgress:    true,
		SyncTimestamps:       true,
		DeleteReconciliation: true,
		SchemaSnapshots:      true,
		FallbackEvents:       true,

		RunHistory:          true,
		RunConfigSnapshots:  true,
		Profiles:            true,
		AIAdjustmentHistory: true,
		AITuningHistory:     true,
	}
}

// Task represents a migration task
type Task struct {
	ID           int64
	RunID        string
	TaskType     string
	TaskKey      string
	Status       string
	StartedAt    *time.Time
	CompletedAt  *time.Time
	RetryCount   int
	MaxRetries   int
	ErrorMessage string
}

// Run represents a migration run
type Run struct {
	ID            string
	StartedAt     time.Time
	CompletedAt   *time.Time
	LastHeartbeat time.Time
	Status        string
	Phase         string // Current phase: initializing, transferring, finalizing, validating, complete
	SourceSchema  string
	TargetSchema  string
	Config        string
	// ConfigHash is the hash of the migration config, used for change detection on resume.
	// Both SQLite and file-based backends persist this field for config validation.
	ConfigHash  string
	ProfileName string
	ConfigPath  string
	Error       string // Error message if status is "failed"
}

// TransferProgress tracks chunk-level progress
type TransferProgress struct {
	TaskID      int64
	TableName   string
	PartitionID *int
	LastPK      string
	RowsDone    int64
	RowsTotal   int64
	UpdatedAt   time.Time
}

// TaskWithProgress combines task info with transfer progress
type TaskWithProgress struct {
	ID           int64
	RunID        string
	TaskType     string
	TaskKey      string
	Status       string
	StartedAt    *time.Time
	CompletedAt  *time.Time
	RetryCount   int
	ErrorMessage string
	RowsDone     int64
	RowsTotal    int64
}

// New creates a new state manager
func New(dataDir string) (*State, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}
	// Enforce permissions in case umask relaxed them.
	if err := os.Chmod(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("setting data dir permissions: %w", err)
	}

	dbPath := filepath.Join(dataDir, "migrate.db")
	// Ensure the DB file exists with restrictive permissions before sql.Open creates it.
	if _, err := os.Stat(dbPath); errors.Is(err, fs.ErrNotExist) {
		if f, createErr := os.OpenFile(dbPath, os.O_CREATE|os.O_EXCL, 0600); createErr == nil {
			f.Close()
		} else {
			return nil, fmt.Errorf("creating db file: %w", createErr)
		}
	}
	// WAL mode for better concurrency, busy_timeout to retry on lock contention
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(30000)")
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Configure connection pool for multi-process access:
	// - MaxIdleConns(0): Close connections after use to ensure fresh reads across processes
	// - MaxOpenConns(1): Single connection at a time to avoid lock contention
	// This ensures each query sees the latest committed data from other processes
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(1)

	s := &State{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating schema: %w", err)
	}

	return s, nil
}

// Close closes the database connection
func (s *State) Close() error {
	return s.db.Close()
}
