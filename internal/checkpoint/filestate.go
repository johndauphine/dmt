package checkpoint

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

// FileState implements StateBackend using a single YAML file.
// Designed for Airflow and headless environments where SQLite is impractical.
type FileState struct {
	path  string
	mu    sync.RWMutex
	state *fileStateData
}

// Capabilities reports the file backend's supported behavior.
func (fs *FileState) Capabilities() BackendCapabilities {
	return BackendCapabilities{
		RunLifecycle:         true,
		TaskLifecycle:        true,
		TransferProgress:     true,
		PartitionProgress:    true,
		SyncTimestamps:       true,
		DeleteReconciliation: true,
		SchemaSnapshots:      true,
		FallbackEvents:       true,

		RunHistory:          false,
		RunConfigSnapshots:  false,
		Profiles:            false,
		AIAdjustmentHistory: false,
		AITuningHistory:     false,
	}
}

// fileStateData is the YAML structure for the state file.
type fileStateData struct {
	RunID         string                `yaml:"run_id"`
	StartedAt     time.Time             `yaml:"started_at"`
	CompletedAt   *time.Time            `yaml:"completed_at,omitempty"`
	LastHeartbeat time.Time             `yaml:"last_heartbeat,omitempty"`
	Status        string                `yaml:"status"` // running, success, failed
	Phase         string                `yaml:"phase"`  // initializing, transferring, finalizing, validating, complete
	Error         string                `yaml:"error,omitempty"`
	SourceSchema  string                `yaml:"source_schema"`
	TargetSchema  string                `yaml:"target_schema"`
	ConfigHash    string                `yaml:"config_hash,omitempty"`
	ProfileName   string                `yaml:"profile_name,omitempty"`
	ConfigPath    string                `yaml:"config_path,omitempty"`
	Tables        map[string]tableState `yaml:"tables"`
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

	// DeleteReconciliationTables records per-table delete reconciliation
	// results for the current run.
	DeleteReconciliationTables map[string]deleteReconciliationTableState `yaml:"delete_reconciliation_tables,omitempty"`
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

type deleteReconciliationTableState struct {
	CandidateRows int64     `yaml:"candidate_rows"`
	DeletedRows   int64     `yaml:"deleted_rows"`
	Skipped       bool      `yaml:"skipped,omitempty"`
	SkipReason    string    `yaml:"skip_reason,omitempty"`
	UpdatedAt     time.Time `yaml:"updated_at"`
}

// tableState tracks per-table progress.
type tableState struct {
	Status      string `yaml:"status"` // pending, running, success, failed
	TaskType    string `yaml:"task_type,omitempty"`
	TableName   string `yaml:"table_name,omitempty"`
	PartitionID *int   `yaml:"partition_id,omitempty"`
	LastPK      any    `yaml:"last_pk,omitempty"`
	RangeState  string `yaml:"range_state,omitempty"`
	RowsDone    int64  `yaml:"rows_done,omitempty"`
	RowsTotal   int64  `yaml:"rows_total,omitempty"`
	TaskID      int64  `yaml:"task_id,omitempty"` // Synthetic task ID for compatibility
	Error       string `yaml:"error,omitempty"`
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

func (fs *FileState) Close() error {
	return nil
}

// Path returns the state file path.
func (fs *FileState) Path() string {
	return fs.path
}

// Ensure FileState implements StateBackend
var _ StateBackend = (*FileState)(nil)

func (fs *FileState) GetLastSyncTimestamp(sourceSchema, tableName, targetSchema string) (*time.Time, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if fs.state == nil || fs.state.SyncTimestamps == nil {
		return nil, nil
	}
	tables, ok := fs.state.SyncTimestamps[sourceSchema]
	if !ok {
		return nil, nil
	}
	targets, ok := tables[tableName]
	if !ok {
		return nil, nil
	}
	ts, ok := targets[targetSchema]
	if !ok {
		return nil, nil
	}
	return &ts, nil
}

// UpdateSyncTimestamp persists the sync timestamp for the given
// (sourceSchema, tableName, targetSchema) triple. Uses the
// crash-safe atomic-write path established in #254, so a
// SIGKILL/eviction partway through the save can't tear the file.
func (fs *FileState) UpdateSyncTimestamp(sourceSchema, tableName, targetSchema string, ts time.Time) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.state == nil {
		fs.state = &fileStateData{Tables: make(map[string]tableState)}
	}
	if fs.state.SyncTimestamps == nil {
		fs.state.SyncTimestamps = make(map[string]map[string]map[string]time.Time)
	}
	if fs.state.SyncTimestamps[sourceSchema] == nil {
		fs.state.SyncTimestamps[sourceSchema] = make(map[string]map[string]time.Time)
	}
	if fs.state.SyncTimestamps[sourceSchema][tableName] == nil {
		fs.state.SyncTimestamps[sourceSchema][tableName] = make(map[string]time.Time)
	}
	fs.state.SyncTimestamps[sourceSchema][tableName][targetSchema] = ts
	return fs.save()
}

func (fs *FileState) SaveAIAdjustment(runID string, record AIAdjustmentRecord) error {
	return nil
}

// GetAIAdjustments returns empty slice for file state (doesn't persist AI history).
func (fs *FileState) GetAIAdjustments(limit int) ([]AIAdjustmentRecord, error) {
	return nil, nil
}

// GetAIAdjustmentsByAction returns empty slice for file state.
func (fs *FileState) GetAIAdjustmentsByAction(action string, limit int) ([]AIAdjustmentRecord, error) {
	return nil, nil
}

// SaveAITuning is a no-op for file state (doesn't persist tuning history).
func (fs *FileState) SaveAITuning(record AITuningRecord) error {
	return nil
}

// UpdateAITuningResult is a no-op for file state.
func (fs *FileState) UpdateAITuningResult(throughput float64, durationSecs float64, chunkRetryCount int, adjustedAtRuntime bool) error {
	return nil
}

// GetAITuningHistory returns empty slice for file state (doesn't persist tuning history).
func (fs *FileState) GetAITuningHistory(limit int, sourceType, targetType string) ([]AITuningRecord, error) {
	return nil, nil
}

// GetAITuningAggregatesByWaw returns nil for file state - no history persisted.
// len(nil) == 0 so the smartconfig format helper correctly skips the section.
func (fs *FileState) GetAITuningAggregatesByWaw(sourceType, targetType string) ([]WawAggregateRecord, error) {
	return nil, nil
}

// GetAITuningAggregatesByChunkSize returns nil for file state - no history persisted.
func (fs *FileState) GetAITuningAggregatesByChunkSize(sourceType, targetType string) ([]ChunkSizeAggregateRecord, error) {
	return nil, nil
}

// SaveFallbackEvent persists an AI fallback occurrence (#176) so a
// separate "dmt status" poll can read counts even though the running
// migration's in-memory counters are inaccessible. UPSERTs into the
// nested run_id -> surface -> fingerprint map. Fingerprint may be ""
// (call sites with no fingerprint still accumulate under one record
// for the surface).
//
// The file backend is the Airflow path; cross-process visibility is
// the whole reason this method exists rather than being a no-op like

func (fs *FileState) SaveSchemaSnapshot(runID, sourceSchema, tableName, schemaJSON string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.state == nil {
		fs.state = &fileStateData{Tables: make(map[string]tableState)}
	}
	if fs.state.SchemaSnapshots == nil {
		fs.state.SchemaSnapshots = make(map[string]map[string]schemaSnapshotState)
	}
	if fs.state.SchemaSnapshots[sourceSchema] == nil {
		fs.state.SchemaSnapshots[sourceSchema] = make(map[string]schemaSnapshotState)
	}
	fs.state.SchemaSnapshots[sourceSchema][tableName] = schemaSnapshotState{
		RunID:      runID,
		CapturedAt: time.Now().UTC(),
		SchemaJSON: schemaJSON,
	}
	return fs.save()
}

func (fs *FileState) GetLatestSchemaSnapshots(sourceSchema string) ([]SchemaSnapshotRecord, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state == nil || fs.state.SchemaSnapshots == nil {
		return nil, nil
	}
	tables := fs.state.SchemaSnapshots[sourceSchema]
	if len(tables) == 0 {
		return nil, nil
	}

	names := make([]string, 0, len(tables))
	for tableName := range tables {
		names = append(names, tableName)
	}
	sort.Strings(names)

	records := make([]SchemaSnapshotRecord, 0, len(names))
	for _, tableName := range names {
		snapshot := tables[tableName]
		records = append(records, SchemaSnapshotRecord{
			RunID:        snapshot.RunID,
			SourceSchema: sourceSchema,
			TableName:    tableName,
			CapturedAt:   snapshot.CapturedAt,
			SchemaJSON:   snapshot.SchemaJSON,
		})
	}
	return records, nil
}

func (fs *FileState) SaveFallbackEvent(runID, surface, fingerprint string) error {
	if runID == "" || surface == "" {
		return nil
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.state.FallbackEvents == nil {
		fs.state.FallbackEvents = map[string]map[string]map[string]fallbackEventState{}
	}
	bySurface, ok := fs.state.FallbackEvents[runID]
	if !ok {
		bySurface = map[string]map[string]fallbackEventState{}
		fs.state.FallbackEvents[runID] = bySurface
	}
	byFP, ok := bySurface[surface]
	if !ok {
		byFP = map[string]fallbackEventState{}
		bySurface[surface] = byFP
	}
	now := time.Now().UTC()
	rec, ok := byFP[fingerprint]
	if !ok {
		rec = fallbackEventState{FirstSeen: now}
	}
	rec.Count++
	rec.LastSeen = now
	byFP[fingerprint] = rec
	return fs.save()
}

// GetFallbackEventsByRun returns the persisted fallback events for a
// run, sorted by (surface, fingerprint) for deterministic status
// rendering. Empty slice (not nil) when nothing has been recorded.
func (fs *FileState) GetFallbackEventsByRun(runID string) ([]FallbackEventRecord, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	out := make([]FallbackEventRecord, 0)
	bySurface, ok := fs.state.FallbackEvents[runID]
	if !ok {
		return out, nil
	}
	surfaces := make([]string, 0, len(bySurface))
	for s := range bySurface {
		surfaces = append(surfaces, s)
	}
	sort.Strings(surfaces)
	for _, surface := range surfaces {
		byFP := bySurface[surface]
		fps := make([]string, 0, len(byFP))
		for fp := range byFP {
			fps = append(fps, fp)
		}
		sort.Strings(fps)
		for _, fp := range fps {
			rec := byFP[fp]
			out = append(out, FallbackEventRecord{
				RunID:       runID,
				Surface:     surface,
				Fingerprint: fp,
				Count:       rec.Count,
				FirstSeen:   rec.FirstSeen,
				LastSeen:    rec.LastSeen,
			})
		}
	}
	return out, nil
}
