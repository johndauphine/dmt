// Package driver — smartconfig analyzer.
//
// PR1 of #175 replaced the AI-driven prompt path with the deterministic
// internal/tuning package. The "AI" prefix on file names and exported
// types is preserved to minimize the public-API churn in this PR; a
// follow-up cleanup will rename ai_smartconfig.go → smartconfig.go and
// drop the unused AISuggestions field on SmartConfigSuggestions.
//
// What lives in this file:
//   - SmartConfigAnalyzer scaffolding: connection wiring, schema-stats
//     collection, date-column / exclude-table heuristics
//   - The bridge that converts the analyzer's collected stats into a
//     tuning.Input and applies tuning.Output back onto SmartConfigSuggestions
//   - AITuningRecord persistence (the SQL ai_tuning_history schema is
//     unchanged; was_ai_used now always records false)
//   - SmartConfigSuggestions.FormatYAML for human-readable output
//
// What got lifted out (now in internal/tuning/):
//   - effectiveMemoryBudgetMB, computeSafeChunkSize, ComputeEstimatedMemMB,
//     clampChunkSizeToBudget body, classifyRegime + DBTuningSnapshot
//
// What got deleted (~1500 LOC, all AI-prompt machinery):
//   - getAIAutoTune, applyAISuggestions, applyDefaultSuggestions,
//     formatHistoricalContext{,ForInput}, buildMemoryBudgetBlock,
//     formatWaw/ChunkSize aggregate blocks, formatTuningSnippet*,
//     detectParameterTrend (PR2 #179 reintroduces a regime-drift
//     variant), extractJSON, truncate, GetOfflineAutoTune,
//     buildOfflinePrompt
package driver

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/driver/dbtuning"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/tuning"
	"github.com/shirou/gopsutil/v3/mem"
)

// DBTuningSnapshot is the per-run effective DB-tuning capture used for
// regime classification (#144). Aliased to tuning.DBTuning so external
// callers (orchestrator, healthcheck) keep the existing type name while
// the new tuner consumes the same struct directly.
type DBTuningSnapshot = tuning.DBTuning

// SmartConfigSuggestions contains the analyzer's recommendations for
// migration parameters plus the schema-derived metadata (date columns,
// exclude candidates, DB tuning advice) that smartconfig surfaces.
type SmartConfigSuggestions struct {
	// DateColumns maps table names to suggested date_updated_columns
	DateColumns map[string][]string

	// ExcludeTables lists tables that should probably be excluded
	ExcludeTables []string

	// ChunkSizeRecommendation is the suggested chunk size based on table analysis
	ChunkSizeRecommendation int

	// Auto-tuned performance parameters
	Workers             int
	ReadAheadBuffers    int
	WriteAheadWriters   int
	ParallelReaders     int
	MaxPartitions       int
	LargeTableThreshold int64

	// Connection pool tuning
	MaxSourceConnections int
	MaxTargetConnections int

	// Additional tuning parameters
	UpsertMergeChunkSize int
	CheckpointFrequency  int
	MaxRetries           int

	// Database statistics
	TotalTables     int
	TotalRows       int64
	AvgRowSizeBytes int64
	EstimatedMemMB  int64

	// Warnings contains any issues detected during analysis
	Warnings []string

	// AISuggestions is preserved for backwards compatibility with code
	// that reads it; PR1 always leaves it nil (no AI path runs). Removed
	// in a follow-up cleanup PR.
	AISuggestions *AutoTuneOutput

	// Reasoning is the tuning engine's short explanation of any non-baseline
	// choices, populated from tuning.Output.Reasoning.
	Reasoning string

	// Database tuning recommendations
	SourceTuning *dbtuning.DatabaseTuning
	TargetTuning *dbtuning.DatabaseTuning
}

// AutoTuneInput is the analyzer's collected snapshot of system + workload
// state. The bridge converts this into tuning.Input.
type AutoTuneInput struct {
	CPUCores          int
	MemoryGB          int
	AvailableMemoryMB int64
	SwapTotalMB       int64
	Platform          string
	MaxMemoryMB       int64

	DatabaseType string
	TargetType   string
	TargetMode   string
	TotalTables  int
	TotalRows    int64
	AvgRowBytes  int64

	LargestTables []TableStats
}

// TableStats is per-table metadata used by the analyzer's avg-row-size
// estimation and (when populated) the largest-tables list on the input.
type TableStats struct {
	Name        string
	RowCount    int64
	AvgRowBytes int64
}

// AutoTuneOutput is preserved for backwards compatibility — see the
// SmartConfigSuggestions.AISuggestions field. PR1 doesn't populate it;
// a follow-up cleanup PR removes the type entirely.
type AutoTuneOutput struct {
	Workers              int    `json:"workers"`
	ChunkSize            int    `json:"chunk_size"`
	ReadAheadBuffers     int    `json:"read_ahead_buffers"`
	WriteAheadWriters    int    `json:"write_ahead_writers"`
	ParallelReaders      int    `json:"parallel_readers"`
	MaxPartitions        int    `json:"max_partitions"`
	LargeTableThreshold  int64  `json:"large_table_threshold"`
	MaxSourceConnections int    `json:"max_source_connections"`
	MaxTargetConnections int    `json:"max_target_connections"`
	UpsertMergeChunkSize int    `json:"upsert_merge_chunk_size"`
	CheckpointFrequency  int    `json:"checkpoint_frequency"`
	MaxRetries           int    `json:"max_retries"`
	ObservedRetryRates   string `json:"observed_retry_rates"`
	Reasoning            string `json:"reasoning,omitempty"`
}

// TuningHistoryProvider supplies past-run data to the analyzer. PR1
// trimmed the interface — the per-WAW / per-chunk_size aggregate methods
// the AI prompt used were dropped; the new tuner reads raw records and
// does its own aggregation in-package.
type TuningHistoryProvider interface {
	// GetAIAdjustments returns recent runtime AI adjustments. Used by
	// the runtime monitor (separate AI surface; not retired in PR1).
	GetAIAdjustments(limit int) ([]AIAdjustmentRecord, error)

	// GetAITuningHistory returns recent tuning recommendations filtered by
	// migration direction, ordered by Timestamp DESC. limit > 0 bounds the
	// slice; the bridge passes 0 for unbounded.
	GetAITuningHistory(limit int, sourceType, targetType string) ([]AITuningRecord, error)

	// SaveAITuning saves a tuning recommendation for future reference.
	SaveAITuning(record AITuningRecord) error

	// UpdateAITuningResult updates the most recent tuning record with final
	// throughput and chunk retry count from the completed run.
	UpdateAITuningResult(throughput float64, durationSecs float64, chunkRetryCount int) error
}

// AIAdjustmentRecord represents a historical AI adjustment from runtime
// migration. Used by the runtime monitor (separate from smartconfig).
type AIAdjustmentRecord struct {
	Action           string         `json:"action"`
	Adjustments      map[string]int `json:"adjustments"`
	ThroughputBefore float64        `json:"throughput_before"`
	ThroughputAfter  float64        `json:"throughput_after"`
	EffectPercent    float64        `json:"effect_percent"`
	Reasoning        string         `json:"reasoning"`
}

// AITuningRecord is one row in the SQL ai_tuning_history table. Schema is
// unchanged from the AI era; the WasAIUsed column now always records false
// and AIReasoning carries the deterministic tuner's reasoning string.
type AITuningRecord struct {
	Timestamp            time.Time `json:"timestamp"`
	SourceDBType         string    `json:"source_db_type"`
	TargetDBType         string    `json:"target_db_type"`
	TotalTables          int       `json:"total_tables"`
	TotalRows            int64     `json:"total_rows"`
	AvgRowSizeBytes      int64     `json:"avg_row_size_bytes"`
	CPUCores             int       `json:"cpu_cores"`
	MemoryGB             int       `json:"memory_gb"`
	Workers              int       `json:"workers"`
	ChunkSize            int       `json:"chunk_size"`
	ReadAheadBuffers     int       `json:"read_ahead_buffers"`
	WriteAheadWriters    int       `json:"write_ahead_writers"`
	ParallelReaders      int       `json:"parallel_readers"`
	MaxPartitions        int       `json:"max_partitions"`
	LargeTableThreshold  int64     `json:"large_table_threshold"`
	MaxSourceConnections int       `json:"max_source_connections"`
	MaxTargetConnections int       `json:"max_target_connections"`
	EstimatedMemoryMB    int64     `json:"estimated_memory_mb"`
	AIReasoning          string    `json:"ai_reasoning"`
	WasAIUsed            bool      `json:"was_ai_used"`
	FinalThroughput      float64   `json:"final_throughput,omitempty"`
	FinalDurationSecs    float64   `json:"final_duration_seconds,omitempty"`
	ChunkRetryCount      int       `json:"chunk_retry_count,omitempty"`

	// Effective DB tuning captured at run start (#144). Mirrors the
	// fields in checkpoint.AITuningRecord.
	Platform                string `json:"platform,omitempty"`
	TargetSharedBuffersMB   int64  `json:"target_shared_buffers_mb,omitempty"`
	TargetSyncCommit        string `json:"target_synchronous_commit,omitempty"`
	TargetFsync             string `json:"target_fsync,omitempty"`
	TargetFullPageWrites    string `json:"target_full_page_writes,omitempty"`
	TargetMaxWALSizeMB      int64  `json:"target_max_wal_size_mb,omitempty"`
	TargetWALLevel          string `json:"target_wal_level,omitempty"`
	SourceMaxServerMemoryMB int64  `json:"source_max_server_memory_mb,omitempty"`
}

// SmartConfigAnalyzer analyzes source database metadata to suggest optimal
// configuration. PR1 dropped the aiMapper / useAI fields — the deterministic
// tuner doesn't need them.
type SmartConfigAnalyzer struct {
	db              *sql.DB
	dbType          string
	targetDBType    string
	targetMode      string
	suggestions     *SmartConfigSuggestions
	historyProvider TuningHistoryProvider
	maxMemoryMB     int64
	pendingSave     *pendingTuningSave
	currentTuning   DBTuningSnapshot
}

// NewSmartConfigAnalyzer creates a new smart config analyzer.
func NewSmartConfigAnalyzer(db *sql.DB, dbType string) *SmartConfigAnalyzer {
	return &SmartConfigAnalyzer{
		db:     db,
		dbType: dbType,
		suggestions: &SmartConfigSuggestions{
			DateColumns:   make(map[string][]string),
			ExcludeTables: []string{},
			Warnings:      []string{},
		},
	}
}

// SetHistoryProvider sets the history provider for learning from past analyses.
func (s *SmartConfigAnalyzer) SetHistoryProvider(provider TuningHistoryProvider) {
	s.historyProvider = provider
}

// SetCurrentTuning records the effective DB tuning captured at run start
// for regime classification (issue #144).
func (s *SmartConfigAnalyzer) SetCurrentTuning(t DBTuningSnapshot) {
	s.currentTuning = t
}

// SetMaxMemoryMB sets the user-configured memory cap.
func (s *SmartConfigAnalyzer) SetMaxMemoryMB(mb int64) {
	s.maxMemoryMB = mb
}

// SetTargetDBType sets the target database type for per-target tuning.
func (s *SmartConfigAnalyzer) SetTargetDBType(targetType string) {
	s.targetDBType = targetType
}

// SetTargetMode sets the migration target mode (drop_recreate or upsert).
func (s *SmartConfigAnalyzer) SetTargetMode(mode string) {
	s.targetMode = mode
}

// Analyze performs smart configuration detection on the source database.
func (s *SmartConfigAnalyzer) Analyze(ctx context.Context, schema string) (*SmartConfigSuggestions, error) {
	logging.Debug("Analyzing database schema for configuration suggestions...")

	tables, err := s.getTables(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("getting tables: %w", err)
	}

	s.suggestions.TotalTables = len(tables)
	var totalRows int64
	for _, t := range tables {
		totalRows += t.RowCount
	}
	s.suggestions.TotalRows = totalRows

	for _, table := range tables {
		dateColumns, err := s.detectDateColumns(ctx, schema, table.Name)
		if err != nil {
			logging.Warn("Warning: analyzing date columns for %s: %v", table.Name, err)
			continue
		}
		if len(dateColumns) > 0 {
			s.suggestions.DateColumns[table.Name] = dateColumns
		}
		if s.shouldExcludeTable(table.Name) {
			s.suggestions.ExcludeTables = append(s.suggestions.ExcludeTables, table.Name)
		}
	}

	s.calculateAutoTuneParams(tables)

	logging.Debug("Smart config analysis complete:")
	logging.Debug("  - Tables: %d (%s rows)", s.suggestions.TotalTables, formatRowCount(s.suggestions.TotalRows))
	logging.Debug("  - Tables with date columns: %d", len(s.suggestions.DateColumns))
	logging.Debug("  - Suggested exclude tables: %d", len(s.suggestions.ExcludeTables))
	logging.Debug("  - Recommended: workers=%d, chunk_size=%d, read_ahead=%d",
		s.suggestions.Workers, s.suggestions.ChunkSizeRecommendation, s.suggestions.ReadAheadBuffers)
	logging.Debug("  - Estimated memory: %dMB", s.suggestions.EstimatedMemMB)

	return s.suggestions, nil
}

// calculateAutoTuneParams runs the deterministic tuner and applies its
// output to s.suggestions. Replaces the AI/default branch — there's no
// AI path anymore; the tuner returns a complete parameter set every time.
func (s *SmartConfigAnalyzer) calculateAutoTuneParams(tables []tableInfo) {
	avgRowSize := s.calculateAvgRowSize(tables)
	s.suggestions.AvgRowSizeBytes = avgRowSize

	input := s.buildAutoTuneInput(tables, avgRowSize)
	output := tuning.Tune(
		s.toTuningInput(input),
		s.toTuningProfile(),
		s.toTuningHistory(),
		s.currentTuning,
	)
	s.applyTuningOutput(output)

	s.pendingSave = &pendingTuningSave{input: input, reasoning: output.Reasoning}
}

// toTuningInput maps the analyzer's AutoTuneInput onto tuning.Input.
func (s *SmartConfigAnalyzer) toTuningInput(in AutoTuneInput) tuning.Input {
	return tuning.Input{
		CPUCores:          in.CPUCores,
		MemoryGB:          in.MemoryGB,
		AvailableMemoryMB: in.AvailableMemoryMB,
		MaxMemoryMB:       in.MaxMemoryMB,
		Platform:          in.Platform,
		SourceDBType:      in.DatabaseType,
		TargetDBType:      in.TargetType,
		TargetMode:        in.TargetMode,
		TotalTables:       in.TotalTables,
		TotalRows:         in.TotalRows,
		AvgRowBytes:       in.AvgRowBytes,
	}
}

// toTuningProfile builds the per-target DriverProfile from the registered
// driver. Unknown target → zero-valued profile, which the tuner treats as
// "use 10 MB conservative chunk fallback + WAW=1 floor."
func (s *SmartConfigAnalyzer) toTuningProfile() tuning.DriverProfile {
	profile := tuning.DriverProfile{Name: s.targetDBType, BaselineWAW: 2}
	d, err := Get(s.targetDBType)
	if err != nil {
		return profile
	}
	defaults := d.Defaults()
	profile.BaselineWAW = defaults.WriteAheadWriters
	profile.ScaleWritersWithCores = defaults.ScaleWritersWithCores
	profile.OptimumBulkChunkBytes = defaults.OptimumBulkChunkBytes
	profile.HardChunkLimit = d.HardChunkLimit(0)
	return profile
}

// toTuningHistory wraps the analyzer's TuningHistoryProvider in an
// adapter that satisfies tuning.HistoryProvider. Returns nil when no
// provider is configured so Tune treats it as cold-start.
func (s *SmartConfigAnalyzer) toTuningHistory() tuning.HistoryProvider {
	if s.historyProvider == nil {
		return nil
	}
	return &tuningHistoryAdapter{base: s.historyProvider}
}

// tuningHistoryAdapter converts the analyzer's TuningHistoryProvider into
// the tuning package's HistoryProvider — fetches AITuningRecord rows and
// translates each into a tuning.HistoryRecord.
type tuningHistoryAdapter struct {
	base TuningHistoryProvider
}

// Records returns all history rows for the (source, target) pair. limit=0
// = no bound; the tuner does its own filtering downstream.
func (a *tuningHistoryAdapter) Records(sourceDBType, targetDBType string) ([]tuning.HistoryRecord, error) {
	rows, err := a.base.GetAITuningHistory(0, sourceDBType, targetDBType)
	if err != nil {
		return nil, err
	}
	out := make([]tuning.HistoryRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, tuning.HistoryRecord{
			SourceDBType:            r.SourceDBType,
			TargetDBType:            r.TargetDBType,
			Workers:                 r.Workers,
			ChunkSize:               r.ChunkSize,
			WriteAheadWriters:       r.WriteAheadWriters,
			FinalThroughput:         r.FinalThroughput,
			ChunkRetryCount:         r.ChunkRetryCount,
			CPUCores:                r.CPUCores,
			MemoryGB:                r.MemoryGB,
			Platform:                r.Platform,
			TargetSharedBuffersMB:   r.TargetSharedBuffersMB,
			TargetSyncCommit:        r.TargetSyncCommit,
			TargetFsync:             r.TargetFsync,
			TargetFullPageWrites:    r.TargetFullPageWrites,
			TargetMaxWALSizeMB:      r.TargetMaxWALSizeMB,
			TargetWALLevel:          r.TargetWALLevel,
			SourceMaxServerMemoryMB: r.SourceMaxServerMemoryMB,
		})
	}
	return out, nil
}

// applyTuningOutput copies the tuner's recommendations onto the
// analyzer's suggestions struct.
func (s *SmartConfigAnalyzer) applyTuningOutput(out tuning.Output) {
	s.suggestions.Workers = out.Workers
	s.suggestions.ChunkSizeRecommendation = out.ChunkSize
	s.suggestions.ReadAheadBuffers = out.ReadAheadBuffers
	s.suggestions.WriteAheadWriters = out.WriteAheadWriters
	s.suggestions.ParallelReaders = out.ParallelReaders
	s.suggestions.MaxPartitions = out.MaxPartitions
	s.suggestions.LargeTableThreshold = out.LargeTableThreshold
	s.suggestions.MaxSourceConnections = out.MaxSourceConnections
	s.suggestions.MaxTargetConnections = out.MaxTargetConnections
	s.suggestions.UpsertMergeChunkSize = out.UpsertMergeChunkSize
	s.suggestions.CheckpointFrequency = out.CheckpointFrequency
	s.suggestions.MaxRetries = out.MaxRetries
	s.suggestions.EstimatedMemMB = out.EstimatedMemMB
	s.suggestions.Reasoning = out.Reasoning
}

// pendingTuningSave holds data for deferred tuning history save.
type pendingTuningSave struct {
	input     AutoTuneInput
	reasoning string
}

// ActualParams holds the actual migration parameters used after user overrides,
// plus effective DB tuning captured at run start (#144).
type ActualParams struct {
	Workers           int
	ChunkSize         int
	ReadAheadBuffers  int
	WriteAheadWriters int
	ParallelReaders   int
	MaxPartitions     int

	// Regime fields (#144). Populated by the orchestrator from
	// captureDBTuning before this struct reaches SaveTuningWithActualParams.
	Platform                string
	TargetSharedBuffersMB   int64
	TargetSyncCommit        string
	TargetFsync             string
	TargetFullPageWrites    string
	TargetMaxWALSizeMB      int64
	TargetWALLevel          string
	SourceMaxServerMemoryMB int64
}

// SaveTuningWithActualParams saves tuning history with the actual params
// used (after user overrides), not the recommendations.
func (s *SmartConfigAnalyzer) SaveTuningWithActualParams(actual ActualParams) {
	if s.pendingSave == nil {
		return
	}
	ps := s.pendingSave
	s.pendingSave = nil

	s.suggestions.Workers = actual.Workers
	s.suggestions.ChunkSizeRecommendation = actual.ChunkSize
	s.suggestions.ReadAheadBuffers = actual.ReadAheadBuffers
	s.suggestions.WriteAheadWriters = actual.WriteAheadWriters
	s.suggestions.ParallelReaders = actual.ParallelReaders
	s.suggestions.MaxPartitions = actual.MaxPartitions
	// Re-derive EstimatedMemMB so the persisted history reflects the
	// post-override params, not the pre-override estimate (#160).
	s.suggestions.EstimatedMemMB = tuning.EstimatedMemMB(
		actual.Workers,
		actual.ReadAheadBuffers,
		actual.WriteAheadWriters,
		actual.ChunkSize,
		ps.input.AvgRowBytes,
	)

	s.saveTuningResult(ps.input, ps.reasoning, actual)
}

// saveTuningResult saves the tuning recommendation to history.
func (s *SmartConfigAnalyzer) saveTuningResult(input AutoTuneInput, reasoning string, actual ActualParams) {
	if s.historyProvider == nil {
		return
	}

	record := AITuningRecord{
		Timestamp:               time.Now(),
		SourceDBType:            s.dbType,
		TargetDBType:            s.targetDBType,
		TotalTables:             s.suggestions.TotalTables,
		TotalRows:               s.suggestions.TotalRows,
		AvgRowSizeBytes:         s.suggestions.AvgRowSizeBytes,
		CPUCores:                input.CPUCores,
		MemoryGB:                input.MemoryGB,
		Workers:                 s.suggestions.Workers,
		ChunkSize:               s.suggestions.ChunkSizeRecommendation,
		ReadAheadBuffers:        s.suggestions.ReadAheadBuffers,
		WriteAheadWriters:       s.suggestions.WriteAheadWriters,
		ParallelReaders:         s.suggestions.ParallelReaders,
		MaxPartitions:           s.suggestions.MaxPartitions,
		LargeTableThreshold:     s.suggestions.LargeTableThreshold,
		MaxSourceConnections:    s.suggestions.MaxSourceConnections,
		MaxTargetConnections:    s.suggestions.MaxTargetConnections,
		EstimatedMemoryMB:       s.suggestions.EstimatedMemMB,
		AIReasoning:             reasoning, // deterministic tuner's reasoning string
		WasAIUsed:               false,     // PR1 dropped the AI path entirely
		Platform:                firstNonEmpty(actual.Platform, input.Platform),
		TargetSharedBuffersMB:   actual.TargetSharedBuffersMB,
		TargetSyncCommit:        actual.TargetSyncCommit,
		TargetFsync:             actual.TargetFsync,
		TargetFullPageWrites:    actual.TargetFullPageWrites,
		TargetMaxWALSizeMB:      actual.TargetMaxWALSizeMB,
		TargetWALLevel:          actual.TargetWALLevel,
		SourceMaxServerMemoryMB: actual.SourceMaxServerMemoryMB,
	}

	if err := s.historyProvider.SaveAITuning(record); err != nil {
		logging.Debug("Failed to save tuning history: %v", err)
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// calculateAvgRowSize returns the average row size from the largest tables.
func (s *SmartConfigAnalyzer) calculateAvgRowSize(tables []tableInfo) int64 {
	var totalSize int64
	var count int
	for i, t := range tables {
		if i >= 5 || t.RowCount == 0 {
			break
		}
		if t.AvgRowSizeBytes > 0 {
			totalSize += t.AvgRowSizeBytes
			count++
		}
	}
	avgRowSize := int64(500)
	if count > 0 {
		avgRowSize = totalSize / int64(count)
	}
	if avgRowSize > 2000 {
		avgRowSize = 2000
	}
	return avgRowSize
}

// buildAutoTuneInput constructs input for the tuner.
func (s *SmartConfigAnalyzer) buildAutoTuneInput(tables []tableInfo, avgRowSize int64) AutoTuneInput {
	cores := runtime.NumCPU()
	memoryGB := 8
	var availableMemoryMB, swapTotalMB int64
	if v, err := mem.VirtualMemory(); err == nil {
		memoryGB = int(v.Total / (1024 * 1024 * 1024))
		availableMemoryMB = int64(v.Available / (1024 * 1024))
	}
	if availableMemoryMB == 0 {
		availableMemoryMB = int64(memoryGB) * 1024 / 2
	}
	if sw, err := mem.SwapMemory(); err == nil {
		swapTotalMB = int64(sw.Total / (1024 * 1024))
	}

	var largestTables []TableStats
	for i, t := range tables {
		if i >= 5 {
			break
		}
		largestTables = append(largestTables, TableStats{
			Name:        t.Name,
			RowCount:    t.RowCount,
			AvgRowBytes: t.AvgRowSizeBytes,
		})
	}

	return AutoTuneInput{
		CPUCores:          cores,
		MemoryGB:          memoryGB,
		AvailableMemoryMB: availableMemoryMB,
		SwapTotalMB:       swapTotalMB,
		Platform:          DetectPlatform(),
		MaxMemoryMB:       s.maxMemoryMB,
		DatabaseType:      s.dbType,
		TargetType:        s.targetDBType,
		TargetMode:        s.targetMode,
		TotalTables:       s.suggestions.TotalTables,
		TotalRows:         s.suggestions.TotalRows,
		AvgRowBytes:       avgRowSize,
		LargestTables:     largestTables,
	}
}

// DetectPlatform returns the runtime platform, detecting WSL2 specifically.
// Exported for callers (e.g. healthcheck.GetSystemBasedSuggestions) that
// build a tuning.Input without going through the analyzer.
func DetectPlatform() string {
	if runtime.GOOS != "linux" {
		return runtime.GOOS
	}
	data, err := os.ReadFile("/proc/version")
	if err == nil && strings.Contains(strings.ToLower(string(data)), "microsoft") {
		return "wsl2"
	}
	return "linux"
}

// formatRowCount formats large row counts with K/M/B suffixes.
func formatRowCount(count int64) string {
	if count >= 1000000000 {
		return fmt.Sprintf("%.1fB", float64(count)/1000000000)
	}
	if count >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(count)/1000000)
	}
	if count >= 1000 {
		return fmt.Sprintf("%.1fK", float64(count)/1000)
	}
	return fmt.Sprintf("%d", count)
}

// tableInfo holds basic table metadata.
type tableInfo struct {
	Name            string
	RowCount        int64
	AvgRowSizeBytes int64
}

// getTables retrieves table metadata from the source database.
func (s *SmartConfigAnalyzer) getTables(ctx context.Context, schema string) ([]tableInfo, error) {
	var query string
	switch s.dbType {
	case "mssql":
		query = `
			SELECT
				t.name AS table_name,
				p.rows AS row_count,
				ISNULL(SUM(a.total_pages) * 8 * 1024 / NULLIF(p.rows, 0), 0) AS avg_row_size
			FROM sys.tables t
			INNER JOIN sys.indexes i ON t.object_id = i.object_id
			INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
			INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
			INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
			WHERE s.name = @p1 AND i.index_id <= 1
			GROUP BY t.name, p.rows
			ORDER BY p.rows DESC`
	case "postgres":
		query = `
			SELECT
				relname AS table_name,
				COALESCE(n_live_tup, 0) AS row_count,
				CASE WHEN n_live_tup > 0
					THEN pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(relname)) / n_live_tup
					ELSE 0
				END AS avg_row_size
			FROM pg_stat_user_tables
			WHERE schemaname = $1
			ORDER BY n_live_tup DESC`
	case "mysql":
		query = `
			SELECT
				TABLE_NAME AS table_name,
				IFNULL(TABLE_ROWS, 0) AS row_count,
				IFNULL(AVG_ROW_LENGTH, 0) AS avg_row_size
			FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = ?
			  AND TABLE_TYPE = 'BASE TABLE'
			ORDER BY TABLE_ROWS DESC`
	default:
		return nil, fmt.Errorf("unsupported database type: %s", s.dbType)
	}

	rows, err := s.db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []tableInfo
	for rows.Next() {
		var t tableInfo
		if err := rows.Scan(&t.Name, &t.RowCount, &t.AvgRowSizeBytes); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}

	return tables, rows.Err()
}

// detectDateColumns finds columns that could be used for incremental sync.
func (s *SmartConfigAnalyzer) detectDateColumns(ctx context.Context, schema, table string) ([]string, error) {
	var query string
	switch s.dbType {
	case "mssql":
		query = `
			SELECT c.name
			FROM sys.columns c
			INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
			INNER JOIN sys.tables tbl ON c.object_id = tbl.object_id
			INNER JOIN sys.schemas s ON tbl.schema_id = s.schema_id
			WHERE s.name = @p1 AND tbl.name = @p2
			  AND t.name IN ('datetime', 'datetime2', 'datetimeoffset', 'date', 'timestamp')
			ORDER BY c.column_id`
	case "postgres":
		query = `
			SELECT column_name
			FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2
			  AND data_type IN ('timestamp without time zone', 'timestamp with time zone', 'date')
			ORDER BY ordinal_position`
	case "mysql":
		query = `
			SELECT COLUMN_NAME
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
			  AND DATA_TYPE IN ('datetime', 'timestamp', 'date')
			ORDER BY ORDINAL_POSITION`
	default:
		return nil, fmt.Errorf("unsupported database type: %s", s.dbType)
	}

	rows, err := s.db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dateColumns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		dateColumns = append(dateColumns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return s.rankDateColumns(dateColumns), nil
}

// rankDateColumns sorts date columns by likelihood of being update timestamps.
func (s *SmartConfigAnalyzer) rankDateColumns(columns []string) []string {
	patterns := []string{
		`(?i)^updated_?at$`,
		`(?i)^modified_?(at|date|time)?$`,
		`(?i)^last_?modified`,
		`(?i)^changed_?(at|date)?$`,
		`(?i)update`,
		`(?i)modif`,
		`(?i)^created_?at$`,
		`(?i)^creation_?date$`,
		`(?i)create`,
	}

	type rankedCol struct {
		name  string
		score int
	}

	ranked := make([]rankedCol, 0, len(columns))
	for _, col := range columns {
		score := len(patterns) + 1
		for i, pattern := range patterns {
			if matched, _ := regexp.MatchString(pattern, col); matched {
				score = i
				break
			}
		}
		ranked = append(ranked, rankedCol{name: col, score: score})
	}

	for i := 0; i < len(ranked)-1; i++ {
		for j := i + 1; j < len(ranked); j++ {
			if ranked[j].score < ranked[i].score {
				ranked[i], ranked[j] = ranked[j], ranked[i]
			}
		}
	}

	result := make([]string, len(ranked))
	for i, r := range ranked {
		result[i] = r.name
	}
	return result
}

// shouldExcludeTable determines if a table should be excluded from migration.
func (s *SmartConfigAnalyzer) shouldExcludeTable(tableName string) bool {
	lower := strings.ToLower(tableName)

	excludePatterns := []string{
		`^temp_`, `_temp$`, `^tmp_`, `_tmp$`,
		`^log_`, `_log$`, `_logs$`,
		`^audit_`, `_audit$`,
		`^archive_`, `_archive$`, `_archived$`,
		`^backup_`, `_backup$`, `_bak$`,
		`^staging_`, `_staging$`,
		`^test_`, `_test$`,
		`^__`, `_history$`, `^sysdiagrams$`, `^aspnet_`, `^elmah`,
	}

	for _, pattern := range excludePatterns {
		if matched, _ := regexp.MatchString(pattern, lower); matched {
			return true
		}
	}
	return false
}

// FormatYAML returns the suggestions formatted as YAML config.
func (s *SmartConfigSuggestions) FormatYAML() string {
	var sb strings.Builder

	sb.WriteString("# Smart Configuration Suggestions\n")
	sb.WriteString(fmt.Sprintf("# Database: %d tables, %s rows, ~%d bytes/row avg\n\n",
		s.TotalTables, formatRowCount(s.TotalRows), s.AvgRowSizeBytes))

	sb.WriteString("migration:\n")
	sb.WriteString("  # Deterministic tuner (internal/tuning) — see #175\n")

	sb.WriteString(fmt.Sprintf("  workers: %d\n", s.Workers))
	sb.WriteString(fmt.Sprintf("  chunk_size: %d\n", s.ChunkSizeRecommendation))
	sb.WriteString(fmt.Sprintf("  read_ahead_buffers: %d\n", s.ReadAheadBuffers))
	sb.WriteString(fmt.Sprintf("  write_ahead_writers: %d\n", s.WriteAheadWriters))
	sb.WriteString(fmt.Sprintf("  parallel_readers: %d\n", s.ParallelReaders))
	sb.WriteString(fmt.Sprintf("  max_partitions: %d\n", s.MaxPartitions))
	sb.WriteString(fmt.Sprintf("  large_table_threshold: %d\n", s.LargeTableThreshold))
	sb.WriteString(fmt.Sprintf("  max_source_connections: %d\n", s.MaxSourceConnections))
	sb.WriteString(fmt.Sprintf("  max_target_connections: %d\n", s.MaxTargetConnections))
	sb.WriteString(fmt.Sprintf("  upsert_merge_chunk_size: %d\n", s.UpsertMergeChunkSize))
	sb.WriteString(fmt.Sprintf("  checkpoint_frequency: %d\n", s.CheckpointFrequency))
	sb.WriteString(fmt.Sprintf("  max_retries: %d\n", s.MaxRetries))
	sb.WriteString(fmt.Sprintf("  # Estimated memory: ~%dMB\n", s.EstimatedMemMB))

	if s.Reasoning != "" {
		sb.WriteString(fmt.Sprintf("  # Tuning reasoning: %s\n", s.Reasoning))
	}
	sb.WriteString("\n")

	if len(s.DateColumns) > 0 {
		sb.WriteString("  # Date columns for incremental sync (priority order)\n")
		sb.WriteString("  date_updated_columns:\n")
		seen := make(map[string]bool)
		var columns []string
		for _, cols := range s.DateColumns {
			for _, col := range cols {
				if !seen[col] {
					seen[col] = true
					columns = append(columns, col)
				}
			}
		}
		for _, col := range columns {
			sb.WriteString(fmt.Sprintf("    - %s\n", col))
		}
		sb.WriteString("\n")
	}

	if len(s.ExcludeTables) > 0 {
		sb.WriteString("  # Tables to exclude (temp/log/archive patterns)\n")
		sb.WriteString("  exclude_tables:\n")
		for _, table := range s.ExcludeTables {
			sb.WriteString(fmt.Sprintf("    - %s\n", table))
		}
		sb.WriteString("\n")
	}

	if s.SourceTuning != nil {
		sb.WriteString(s.formatDatabaseTuning(s.SourceTuning))
	}
	if s.TargetTuning != nil {
		sb.WriteString(s.formatDatabaseTuning(s.TargetTuning))
	}

	if len(s.Warnings) > 0 {
		sb.WriteString("# Warnings:\n")
		for _, w := range s.Warnings {
			sb.WriteString(fmt.Sprintf("# - %s\n", w))
		}
	}

	return sb.String()
}

// formatDatabaseTuning formats database tuning recommendations in a
// human-readable format.
func (s *SmartConfigSuggestions) formatDatabaseTuning(tuning *dbtuning.DatabaseTuning) string {
	var sb strings.Builder

	sb.WriteString("\n")
	sb.WriteString("#" + strings.Repeat("=", 78) + "\n")
	sb.WriteString(fmt.Sprintf("# %s DATABASE TUNING (%s)\n", strings.ToUpper(tuning.Role), strings.ToUpper(tuning.DatabaseType)))
	sb.WriteString("#" + strings.Repeat("=", 78) + "\n")
	sb.WriteString(fmt.Sprintf("# Tuning Potential: %s\n", strings.ToUpper(tuning.TuningPotential)))
	sb.WriteString(fmt.Sprintf("# Impact: %s\n", tuning.EstimatedImpact))
	sb.WriteString("#" + strings.Repeat("-", 78) + "\n\n")

	if len(tuning.Recommendations) == 0 {
		if tuning.TuningPotential == "unknown" {
			sb.WriteString(fmt.Sprintf("# ⚠ Unable to analyze %s database tuning\n", tuning.Role))
			sb.WriteString(fmt.Sprintf("# Reason: %s\n\n", tuning.EstimatedImpact))
		} else {
			sb.WriteString(fmt.Sprintf("# ✓ No tuning needed - %s database is already well-configured!\n\n", tuning.Role))
		}
		return sb.String()
	}

	priority1 := []dbtuning.TuningRecommendation{}
	priority2 := []dbtuning.TuningRecommendation{}
	priority3 := []dbtuning.TuningRecommendation{}

	for _, rec := range tuning.Recommendations {
		switch rec.Priority {
		case 1:
			priority1 = append(priority1, rec)
		case 2:
			priority2 = append(priority2, rec)
		case 3:
			priority3 = append(priority3, rec)
		}
	}

	if len(priority1) > 0 {
		sb.WriteString("# 🔴 CRITICAL (Priority 1) - High Impact Changes\n")
		sb.WriteString("#" + strings.Repeat("-", 78) + "\n")
		for i, rec := range priority1 {
			sb.WriteString(s.formatRecommendation(i+1, rec))
		}
		sb.WriteString("\n")
	}

	if len(priority2) > 0 {
		sb.WriteString("# 🟡 IMPORTANT (Priority 2) - Medium Impact Changes\n")
		sb.WriteString("#" + strings.Repeat("-", 78) + "\n")
		for i, rec := range priority2 {
			sb.WriteString(s.formatRecommendation(i+1, rec))
		}
		sb.WriteString("\n")
	}

	if len(priority3) > 0 {
		sb.WriteString("# 🟢 OPTIONAL (Priority 3) - Nice to Have\n")
		sb.WriteString("#" + strings.Repeat("-", 78) + "\n")
		for i, rec := range priority3 {
			sb.WriteString(s.formatRecommendation(i+1, rec))
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

// formatRecommendation formats a single tuning recommendation.
func (s *SmartConfigSuggestions) formatRecommendation(num int, rec dbtuning.TuningRecommendation) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("#\n# %d. %s\n", num, rec.Parameter))
	sb.WriteString(fmt.Sprintf("#    Current:     %v\n", rec.CurrentValue))
	sb.WriteString(fmt.Sprintf("#    Recommended: %v\n", rec.RecommendedValue))
	sb.WriteString(fmt.Sprintf("#    Impact:      %s\n", strings.ToUpper(rec.Impact)))
	sb.WriteString("#\n")
	sb.WriteString("#    Why: " + s.wrapText(rec.Reason, 75, "#         ") + "\n")

	if rec.CanApplyRuntime && rec.SQLCommand != "" {
		sb.WriteString("#\n")
		sb.WriteString("#    ✓ Can apply at runtime (no restart needed):\n")
		sqlLines := strings.Split(rec.SQLCommand, ";")
		for _, line := range sqlLines {
			line = strings.TrimSpace(line)
			if line != "" {
				sb.WriteString("#      " + line + ";\n")
			}
		}
	} else if rec.RequiresRestart {
		sb.WriteString("#\n")
		sb.WriteString("#    ⚠ Requires database restart\n")
		if rec.ConfigFile != "" {
			sb.WriteString("#    Add to config file:\n")
			lines := strings.Split(rec.ConfigFile, "\n")
			for _, line := range lines {
				if line != "" {
					sb.WriteString("#      " + line + "\n")
				}
			}
		}
	}

	return sb.String()
}

// wrapText wraps text to maxWidth characters with the given prefix for
// continuation lines.
func (s *SmartConfigSuggestions) wrapText(text string, maxWidth int, contPrefix string) string {
	if len(text) <= maxWidth {
		return text
	}

	var result strings.Builder
	words := strings.Fields(text)
	lineLen := 0

	for i, word := range words {
		wordLen := len(word)

		if i == 0 {
			result.WriteString(word)
			lineLen = wordLen
		} else if lineLen+1+wordLen > maxWidth {
			result.WriteString("\n" + contPrefix + word)
			lineLen = len(contPrefix) + wordLen
		} else {
			result.WriteString(" " + word)
			lineLen += 1 + wordLen
		}
	}

	return result.String()
}
