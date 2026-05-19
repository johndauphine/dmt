package checkpoint

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// FileState implements StateBackend using a single YAML file.
// Designed for Airflow and headless environments where SQLite is impractical.
type FileState struct {
	path  string
	mu    sync.RWMutex
	state *fileStateData
}

// fileStateData is the YAML structure for the state file.
type fileStateData struct {
	RunID        string                `yaml:"run_id"`
	StartedAt    time.Time             `yaml:"started_at"`
	CompletedAt  *time.Time            `yaml:"completed_at,omitempty"`
	Status       string                `yaml:"status"` // running, success, failed
	Phase        string                `yaml:"phase"`  // initializing, transferring, finalizing, validating, complete
	Error        string                `yaml:"error,omitempty"`
	SourceSchema string                `yaml:"source_schema"`
	TargetSchema string                `yaml:"target_schema"`
	ConfigHash   string                `yaml:"config_hash,omitempty"`
	ProfileName  string                `yaml:"profile_name,omitempty"`
	ConfigPath   string                `yaml:"config_path,omitempty"`
	Tables       map[string]tableState `yaml:"tables"`
	// SyncTimestamps records the last successful sync time per
	// (source schema, table, target schema) triple, used by
	// date-based incremental sync. Pre-#255 the file backend
	// no-op'd Get/UpdateSyncTimestamp, so an incremental sync
	// configured against the recommended Airflow/k8s file backend
	// silently degraded to a full-table copy every run.
	//
	// Encoded as nested maps (sourceSchema -> table -> targetSchema ->
	// ts) rather than a delimiter-joined flat key - quoted
	// identifiers can contain almost anything (including pipes,
	// dots, brackets), and a flat-key encoding had alias
	// collisions where different triples mapped to the same key
	// (Codex review on this PR). The nested form serializes
	// cleanly in YAML too:
	//
	//	sync_timestamps:
	//	  dbo:
	//	    Orders:
	//	      public: 2026-05-12T08:00:00Z
	SyncTimestamps map[string]map[string]map[string]time.Time `yaml:"sync_timestamps,omitempty"`

	// FallbackEvents persists AI fallback occurrences (#176) for
	// cross-process status visibility - the Airflow polling case where
	// ``dmt status'' is invoked in a separate process from ``dmt run''
	// and would otherwise see only an empty in-memory counter. Keyed as
	// run_id -> surface -> fingerprint -> record, mirroring the SQLite
	// schema. Empty fingerprints collapse to a single record per surface.
	FallbackEvents map[string]map[string]map[string]fallbackEventState `yaml:"fallback_events,omitempty"`

	// SchemaSnapshots persists the latest successful source schema shape
	// for drift detection (#305). Keyed as source_schema -> table ->
	// snapshot so the file backend keeps the same run-to-run safety
	// contract as SQLite in Airflow/k8s deployments.
	SchemaSnapshots map[string]map[string]schemaSnapshotState `yaml:"schema_snapshots,omitempty"`

	// DeleteReconciliations persists the latest successful delete
	// reconciliation per source/target schema pair (#351). Keyed as
	// sourceSchema -> targetSchema to avoid delimiter collisions in
	// quoted identifiers.
	DeleteReconciliations map[string]map[string]deleteReconciliationState `yaml:"delete_reconciliations,omitempty"`
}

// fallbackEventState is one persisted fallback record. Mirrors
// FallbackEventRecord but without RunID/Surface/Fingerprint, which
// are encoded in the parent map keys.
type fallbackEventState struct {
	Count     int64     `yaml:"count"`
	FirstSeen time.Time `yaml:"first_seen"`
	LastSeen  time.Time `yaml:"last_seen"`
}

// schemaSnapshotState is one persisted source schema snapshot. The source
// schema and table name are encoded in parent map keys.
type schemaSnapshotState struct {
	RunID      string    `yaml:"run_id"`
	CapturedAt time.Time `yaml:"captured_at"`
	SchemaJSON string    `yaml:"schema_json"`
}

type deleteReconciliationState struct {
	LastRunID     string    `yaml:"last_run_id"`
	LastSuccessAt time.Time `yaml:"last_success_at"`
	UpdatedAt     time.Time `yaml:"updated_at"`
}

// tableState tracks per-table progress.
type tableState struct {
	Status    string `yaml:"status"` // pending, running, success, failed
	LastPK    any    `yaml:"last_pk,omitempty"`
	RowsDone  int64  `yaml:"rows_done,omitempty"`
	RowsTotal int64  `yaml:"rows_total,omitempty"`
	TaskID    int64  `yaml:"task_id,omitempty"` // Synthetic task ID for compatibility
	Error     string `yaml:"error,omitempty"`
}

// NewFileState creates a file-based state manager.
// If the file exists, it loads the existing state.
func NewFileState(path string) (*FileState, error) {
	fs := &FileState{
		path: path,
		state: &fileStateData{
			Tables: make(map[string]tableState),
		},
	}

	// Load existing state if file exists
	if _, err := os.Stat(path); err == nil {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading state file: %w", err)
		}
		if err := yaml.Unmarshal(data, fs.state); err != nil {
			return nil, fmt.Errorf("parsing state file: %w", err)
		}
		if fs.state.Tables == nil {
			fs.state.Tables = make(map[string]tableState)
		}
	}

	return fs, nil
}

// save writes the current state to the YAML file using the standard
// temp + fsync + rename + dir-fsync atomic-write pattern. Pre-#254
// this used os.WriteFile, which is *not* crash-safe: a SIGKILL,
// OOM-kill, or pod eviction partway through the write would leave a
// truncated YAML file that fails to parse on resume - exactly the
// failure mode the file backend was added to handle (Airflow/k8s).
func (fs *FileState) save() error {
	data, err := yaml.Marshal(fs.state)
	if err != nil {
		return fmt.Errorf("marshaling state: %w", err)
	}
	return atomicWriteFile(fs.path, data, 0600)
}

// atomicWriteFile writes data to path atomically: it stages the
// content in a temp file in the same directory, fsyncs the file,
// renames it into place, and then fsyncs the parent directory so the
// rename itself is durable across power loss. Returns the existing
// file unchanged if any step fails (the partially-written temp is
// cleaned up by the deferred Remove).
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)

	// Ensure parent directory exists. 0700 mirrors the restrictive
	// permissions used elsewhere in checkpoint storage.
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("creating state dir: %w", err)
	}

	// Stage in the same directory as the target so the eventual rename
	// stays within one filesystem (rename across filesystems is not
	// atomic and may fall back to copy+delete).
	f, err := os.CreateTemp(dir, filepath.Base(path)+".tmp.")
	if err != nil {
		return fmt.Errorf("creating temp state file: %w", err)
	}
	tmpName := f.Name()
	// On the happy path the rename below removes the temp from this
	// name, making this Remove a no-op. On any error path it cleans
	// up the partial file so we don't leak temps next to the state.
	defer os.Remove(tmpName)

	// Match the final file's mode exactly so the temp doesn't widen
	// permissions during the brief window before the rename. CreateTemp
	// defaults to 0600 on most platforms, but Chmod nails it down.
	if err := f.Chmod(perm); err != nil {
		f.Close()
		return fmt.Errorf("chmod temp state file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("writing temp state file: %w", err)
	}

	// fsync the file data to disk before the rename. Without this,
	// the rename can land but the file's contents may still be in the
	// page cache and lost on power failure.
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync temp state file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing temp state file: %w", err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("renaming state file into place: %w", err)
	}

	// fsync the parent dir on POSIX so the rename itself is durably
	// committed. Skipped on Windows: although os.Open(dir) succeeds
	// (Go uses FILE_FLAG_BACKUP_SEMANTICS), File.Sync calls
	// FlushFileBuffers on the read-only directory handle which is
	// not a supported operation and returns an error. dmt builds
	// Windows binaries; failing every save on that platform would be
	// strictly worse than the rename-already-landed durability we get
	// without the explicit dir fsync.
	//
	// On POSIX every error in this block is surfaced - both an Open
	// failure (e.g. dir mode 0300 / EACCES, ACL restriction) and a
	// Sync failure (PVC/NFS/disk-full-on-metadata) mean the
	// crash-safety contract this function advertises wasn't actually
	// delivered, and the caller must know that.
	if runtime.GOOS != "windows" {
		d, err := os.Open(dir)
		if err != nil {
			return fmt.Errorf("opening state dir for fsync: %w", err)
		}
		syncErr := d.Sync()
		_ = d.Close() // close error on a read handle is uninteresting
		if syncErr != nil {
			return fmt.Errorf("fsync state dir: %w", syncErr)
		}
	}
	return nil
}
