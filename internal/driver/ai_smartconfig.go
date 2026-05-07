package driver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/driver/dbtuning"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/shirou/gopsutil/v3/mem"
)

// extractJSON finds the first JSON object in a string, handling cases where
// models wrap JSON in conversational text or markdown code blocks.
func extractJSON(s string) (string, error) {
	s = strings.TrimSpace(s)

	// Strip markdown ```json ... ``` blocks
	if idx := strings.Index(s, "```json"); idx >= 0 {
		inner := s[idx+7:]
		if end := strings.Index(inner, "```"); end >= 0 {
			return strings.TrimSpace(inner[:end]), nil
		}
	}
	// Strip generic ``` ... ``` blocks containing JSON
	if idx := strings.Index(s, "```"); idx >= 0 {
		inner := s[idx+3:]
		if end := strings.Index(inner, "```"); end >= 0 {
			candidate := strings.TrimSpace(inner[:end])
			if strings.HasPrefix(candidate, "{") {
				return candidate, nil
			}
		}
	}

	// Fallback: find first { and use json.Decoder to extract a valid
	// JSON object. This is robust against braces inside string values
	// and handles all edge cases a regex/depth counter would miss.
	start := strings.Index(s, "{")
	if start < 0 {
		return "", fmt.Errorf("no JSON object found in response")
	}

	dec := json.NewDecoder(strings.NewReader(s[start:]))
	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return "", fmt.Errorf("no valid JSON object found in response")
	}

	return string(raw), nil
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

// SmartConfigSuggestions contains AI-detected configuration suggestions.
type SmartConfigSuggestions struct {
	// DateColumns maps table names to suggested date_updated_columns
	DateColumns map[string][]string

	// ExcludeTables lists tables that should probably be excluded
	ExcludeTables []string

	// ChunkSizeRecommendation is the suggested chunk size based on table analysis
	ChunkSizeRecommendation int

	// Auto-tuned performance parameters
	Workers             int   // Recommended worker count (based on CPU cores)
	ReadAheadBuffers    int   // Recommended read-ahead buffers
	WriteAheadWriters   int   // Recommended write-ahead writers per job
	ParallelReaders     int   // Recommended parallel readers per job
	MaxPartitions       int   // Recommended max partitions for large tables
	LargeTableThreshold int64 // Row count threshold for partitioning

	// Connection pool tuning
	MaxSourceConnections int // Recommended max source database connections
	MaxTargetConnections int // Recommended max target database connections

	// Additional tuning parameters
	UpsertMergeChunkSize int // Recommended chunk size for upsert operations
	CheckpointFrequency  int // Recommended checkpoint frequency (chunks)
	MaxRetries           int // Recommended max retries for failed operations

	// Database statistics
	TotalTables     int   // Number of tables analyzed
	TotalRows       int64 // Total rows across all tables
	AvgRowSizeBytes int64 // Average row size in bytes
	EstimatedMemMB  int64 // Estimated memory usage with these settings

	// Warnings contains any issues detected during analysis
	Warnings []string

	// AISuggestions contains AI-recommended values (if AI was used)
	AISuggestions *AutoTuneOutput

	// Database tuning recommendations (NEW)
	SourceTuning *dbtuning.DatabaseTuning
	TargetTuning *dbtuning.DatabaseTuning
}

// AutoTuneInput contains system and database info for AI auto-tuning.
type AutoTuneInput struct {
	// System info
	CPUCores          int    `json:"cpu_cores"`
	MemoryGB          int    `json:"memory_gb"`
	AvailableMemoryMB int64  `json:"available_memory_mb"` // Free + reclaimable memory
	SwapTotalMB       int64  `json:"swap_total_mb"`
	Platform          string `json:"platform"`      // "linux", "wsl2", "darwin", "windows"
	MaxMemoryMB       int64  `json:"max_memory_mb"` // User-configured cap (0 = none)

	// Database info
	DatabaseType string `json:"database_type"`           // source: "mssql", "postgres", "mysql", "oracle"
	TargetType   string `json:"target_type,omitempty"`   // target database type
	TargetMode   string `json:"target_mode,omitempty"`   // "drop_recreate" or "upsert"
	TotalTables  int    `json:"total_tables"`
	TotalRows    int64  `json:"total_rows"`
	AvgRowBytes  int64  `json:"avg_row_bytes"`

	// Largest tables (top 5)
	LargestTables []TableStats `json:"largest_tables"`
}

// TableStats contains stats for a single table.
type TableStats struct {
	Name        string `json:"name"`
	RowCount    int64  `json:"row_count"`
	AvgRowBytes int64  `json:"avg_row_bytes"`
}

// AutoTuneOutput contains AI-recommended configuration values.
type AutoTuneOutput struct {
	Workers             int   `json:"workers"`
	ChunkSize           int   `json:"chunk_size"`
	ReadAheadBuffers    int   `json:"read_ahead_buffers"`
	WriteAheadWriters   int   `json:"write_ahead_writers"`
	ParallelReaders     int   `json:"parallel_readers"`
	MaxPartitions       int   `json:"max_partitions"`
	LargeTableThreshold int64 `json:"large_table_threshold"`
	EstimatedMemoryMB   int64 `json:"estimated_memory_mb"`

	// Connection pool tuning
	MaxSourceConnections int `json:"max_source_connections"`
	MaxTargetConnections int `json:"max_target_connections"`

	// Additional tuning
	UpsertMergeChunkSize int    `json:"upsert_merge_chunk_size"`
	CheckpointFrequency  int    `json:"checkpoint_frequency"`
	MaxRetries           int    `json:"max_retries"`
	ObservedRetryRates   string `json:"observed_retry_rates,omitempty"`
	Reasoning            string `json:"reasoning,omitempty"`
}

// TuningHistoryProvider provides access to historical tuning data.
// This allows the analyzer to learn from past analyses and migrations.
type TuningHistoryProvider interface {
	// GetAIAdjustments returns recent runtime AI adjustments from migrations
	GetAIAdjustments(limit int) ([]AIAdjustmentRecord, error)
	// GetAITuningHistory returns recent tuning recommendations filtered by migration direction.
	GetAITuningHistory(limit int, sourceType, targetType string) ([]AITuningRecord, error)
	// SaveAITuning saves a tuning recommendation for future reference
	SaveAITuning(record AITuningRecord) error
	// UpdateAITuningResult updates the most recent tuning record with final
	// throughput and the cumulative chunk retry count observed during the run.
	UpdateAITuningResult(throughput float64, durationSecs float64, chunkRetryCount int) error
}

// AIAdjustmentRecord represents a historical AI adjustment from runtime migration.
type AIAdjustmentRecord struct {
	Action           string         `json:"action"`
	Adjustments      map[string]int `json:"adjustments"`
	ThroughputBefore float64        `json:"throughput_before"`
	ThroughputAfter  float64        `json:"throughput_after"`
	EffectPercent    float64        `json:"effect_percent"`
	Reasoning        string         `json:"reasoning"`
}

// AITuningRecord represents a historical AI tuning recommendation.
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
	FinalThroughput      float64   `json:"final_throughput,omitempty"`      // rows/sec from completed migration
	FinalDurationSecs    float64   `json:"final_duration_seconds,omitempty"` // total migration duration in seconds
	ChunkRetryCount      int       `json:"chunk_retry_count,omitempty"`     // chunk retries observed during the run (0 = clean)
}

// SmartConfigAnalyzer analyzes source database metadata to suggest optimal configuration.
type SmartConfigAnalyzer struct {
	db              *sql.DB
	dbType          string // "mssql" or "postgres"
	targetDBType    string // target database type (if known)
	targetMode      string // "drop_recreate" or "upsert"
	aiMapper        *AITypeMapper
	useAI           bool
	suggestions     *SmartConfigSuggestions
	historyProvider TuningHistoryProvider
	maxMemoryMB     int64 // user-configured memory cap (passed to AI)
	pendingSave     *pendingTuningSave
}

// NewSmartConfigAnalyzer creates a new smart config analyzer.
func NewSmartConfigAnalyzer(db *sql.DB, dbType string, aiMapper *AITypeMapper) *SmartConfigAnalyzer {
	return &SmartConfigAnalyzer{
		db:       db,
		dbType:   dbType,
		aiMapper: aiMapper,
		useAI:    aiMapper != nil,
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

// SetMaxMemoryMB sets the user-configured memory cap so AI can respect it.
func (s *SmartConfigAnalyzer) SetMaxMemoryMB(mb int64) {
	s.maxMemoryMB = mb
}

// SetTargetDBType sets the target database type for more accurate recommendations.
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

	// Get all tables with their metadata
	tables, err := s.getTables(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("getting tables: %w", err)
	}

	// Calculate database statistics
	s.suggestions.TotalTables = len(tables)
	var totalRows int64
	for _, t := range tables {
		totalRows += t.RowCount
	}
	s.suggestions.TotalRows = totalRows

	// Analyze each table for date columns and exclude candidates
	for _, table := range tables {
		// Detect date columns
		dateColumns, err := s.detectDateColumns(ctx, schema, table.Name)
		if err != nil {
			logging.Warn("Warning: analyzing date columns for %s: %v", table.Name, err)
			continue
		}
		if len(dateColumns) > 0 {
			s.suggestions.DateColumns[table.Name] = dateColumns
		}

		// Detect exclude candidates
		if s.shouldExcludeTable(table.Name) {
			s.suggestions.ExcludeTables = append(s.suggestions.ExcludeTables, table.Name)
		}
	}

	// Calculate auto-tuned parameters
	s.calculateAutoTuneParams(ctx, tables)

	// Log summary
	logging.Debug("Smart config analysis complete:")
	logging.Debug("  - Tables: %d (%s rows)", s.suggestions.TotalTables, formatRowCount(s.suggestions.TotalRows))
	logging.Debug("  - Tables with date columns: %d", len(s.suggestions.DateColumns))
	logging.Debug("  - Suggested exclude tables: %d", len(s.suggestions.ExcludeTables))
	logging.Debug("  - Recommended: workers=%d, chunk_size=%d, read_ahead=%d",
		s.suggestions.Workers, s.suggestions.ChunkSizeRecommendation, s.suggestions.ReadAheadBuffers)
	logging.Debug("  - Estimated memory: %dMB", s.suggestions.EstimatedMemMB)

	return s.suggestions, nil
}

// calculateAutoTuneParams calculates all auto-tuned performance parameters.
// Always uses formula-based calculation, optionally gets AI suggestions too.
func (s *SmartConfigAnalyzer) calculateAutoTuneParams(ctx context.Context, tables []tableInfo) {
	// First calculate avg row size
	avgRowSize := s.calculateAvgRowSize(tables)
	s.suggestions.AvgRowSizeBytes = avgRowSize

	// Build input for AI tuning
	input := s.buildAutoTuneInput(tables, avgRowSize)

	// Try AI tuning
	wasAIUsed := false
	var aiReasoning string

	if s.useAI && s.aiMapper != nil {
		output, err := s.getAIAutoTune(ctx, input)
		if err == nil && output != nil {
			s.suggestions.AISuggestions = output
			s.applyAISuggestions(output)
			wasAIUsed = true
			aiReasoning = output.Reasoning
			logging.Debug("AI tuning applied")
		} else {
			logging.Warn("AI tuning unavailable: %v - using sensible defaults", err)
			s.applyDefaultSuggestions(input)
			aiReasoning = "AI unavailable - using sensible defaults based on system resources"
		}
	} else {
		logging.Info("AI provider not configured - using sensible defaults")
		s.applyDefaultSuggestions(input)
		aiReasoning = "AI not configured - using sensible defaults based on system resources"
	}

	// Save tuning result for future reference (deferred until SaveTuningWithActualParams is called)
	s.pendingSave = &pendingTuningSave{input: input, wasAIUsed: wasAIUsed, aiReasoning: aiReasoning}
}

// pendingTuningSave holds data for deferred tuning history save.
type pendingTuningSave struct {
	input       AutoTuneInput
	wasAIUsed   bool
	aiReasoning string
}

// ActualParams holds the actual migration parameters used after user overrides.
type ActualParams struct {
	Workers           int
	ChunkSize         int
	ReadAheadBuffers  int
	WriteAheadWriters int
	ParallelReaders   int
	MaxPartitions     int
}

// SaveTuningWithActualParams saves tuning history with the actual params used
// (after user overrides), not the AI recommendations.
func (s *SmartConfigAnalyzer) SaveTuningWithActualParams(actual ActualParams) {
	if s.pendingSave == nil {
		return
	}
	ps := s.pendingSave
	s.pendingSave = nil

	// Override with actual values used
	s.suggestions.Workers = actual.Workers
	s.suggestions.ChunkSizeRecommendation = actual.ChunkSize
	s.suggestions.ReadAheadBuffers = actual.ReadAheadBuffers
	s.suggestions.WriteAheadWriters = actual.WriteAheadWriters
	s.suggestions.ParallelReaders = actual.ParallelReaders
	s.suggestions.MaxPartitions = actual.MaxPartitions

	s.saveTuningResult(ps.input, ps.wasAIUsed, ps.aiReasoning)
}

// saveTuningResult saves the tuning recommendation to history for future analyses.
func (s *SmartConfigAnalyzer) saveTuningResult(input AutoTuneInput, wasAIUsed bool, aiReasoning string) {
	if s.historyProvider == nil {
		return
	}

	record := AITuningRecord{
		Timestamp:            time.Now(),
		SourceDBType:         s.dbType,
		TargetDBType:         s.targetDBType,
		TotalTables:          s.suggestions.TotalTables,
		TotalRows:            s.suggestions.TotalRows,
		AvgRowSizeBytes:      s.suggestions.AvgRowSizeBytes,
		CPUCores:             input.CPUCores,
		MemoryGB:             input.MemoryGB,
		Workers:              s.suggestions.Workers,
		ChunkSize:            s.suggestions.ChunkSizeRecommendation,
		ReadAheadBuffers:     s.suggestions.ReadAheadBuffers,
		WriteAheadWriters:    s.suggestions.WriteAheadWriters,
		ParallelReaders:      s.suggestions.ParallelReaders,
		MaxPartitions:        s.suggestions.MaxPartitions,
		LargeTableThreshold:  s.suggestions.LargeTableThreshold,
		MaxSourceConnections: s.suggestions.MaxSourceConnections,
		MaxTargetConnections: s.suggestions.MaxTargetConnections,
		EstimatedMemoryMB:    s.suggestions.EstimatedMemMB,
		AIReasoning:          aiReasoning,
		WasAIUsed:            wasAIUsed,
	}

	if err := s.historyProvider.SaveAITuning(record); err != nil {
		logging.Debug("Failed to save tuning history: %v", err)
	}
}

// applyAISuggestions applies AI-recommended values to the suggestions.
func (s *SmartConfigAnalyzer) applyAISuggestions(ai *AutoTuneOutput) {
	s.suggestions.Workers = ai.Workers
	s.suggestions.ChunkSizeRecommendation = ai.ChunkSize
	s.suggestions.ReadAheadBuffers = ai.ReadAheadBuffers
	s.suggestions.WriteAheadWriters = ai.WriteAheadWriters
	s.suggestions.ParallelReaders = ai.ParallelReaders
	s.suggestions.MaxPartitions = ai.MaxPartitions
	s.suggestions.LargeTableThreshold = ai.LargeTableThreshold
	s.suggestions.MaxSourceConnections = ai.MaxSourceConnections
	s.suggestions.MaxTargetConnections = ai.MaxTargetConnections
	s.suggestions.UpsertMergeChunkSize = ai.UpsertMergeChunkSize
	s.suggestions.CheckpointFrequency = ai.CheckpointFrequency
	s.suggestions.MaxRetries = ai.MaxRetries
	s.suggestions.EstimatedMemMB = ai.EstimatedMemoryMB
}

// applyDefaultSuggestions applies sensible defaults based on system resources.
func (s *SmartConfigAnalyzer) applyDefaultSuggestions(input AutoTuneInput) {
	// Workers: CPU cores minus 2 for OS, minimum 2
	workers := input.CPUCores - 2
	if workers < 2 {
		workers = 2
	}

	// Simple defaults that scale with available resources
	s.suggestions.Workers = workers
	s.suggestions.ChunkSizeRecommendation = 50000
	s.suggestions.ReadAheadBuffers = 4
	s.suggestions.WriteAheadWriters = 2
	s.suggestions.ParallelReaders = 2
	s.suggestions.MaxPartitions = workers
	s.suggestions.LargeTableThreshold = 1000000
	s.suggestions.MaxSourceConnections = workers + 4
	s.suggestions.MaxTargetConnections = workers*2 + 4
	s.suggestions.UpsertMergeChunkSize = 5000
	s.suggestions.CheckpointFrequency = 20
	s.suggestions.MaxRetries = 3

	// Estimate memory usage
	s.suggestions.EstimatedMemMB = int64(workers) * 4 * int64(s.suggestions.ChunkSizeRecommendation) * input.AvgRowBytes / 1024 / 1024
}

// calculateAvgRowSize calculates average row size from top 5 largest tables.
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
	avgRowSize := int64(500) // Default estimate
	if count > 0 {
		avgRowSize = totalSize / int64(count)
	}
	// Cap at reasonable max (very wide tables skew estimates)
	if avgRowSize > 2000 {
		avgRowSize = 2000
	}
	return avgRowSize
}

// buildAutoTuneInput constructs input for AI auto-tuning.
func (s *SmartConfigAnalyzer) buildAutoTuneInput(tables []tableInfo, avgRowSize int64) AutoTuneInput {
	// Get system info
	cores := runtime.NumCPU()
	memoryGB := 8 // Default
	var availableMemoryMB, swapTotalMB int64
	if v, err := mem.VirtualMemory(); err == nil {
		memoryGB = int(v.Total / (1024 * 1024 * 1024))
		availableMemoryMB = int64(v.Available / (1024 * 1024))
	}
	// Fallback: if available memory is unknown, estimate as 50% of total
	if availableMemoryMB == 0 {
		availableMemoryMB = int64(memoryGB) * 1024 / 2
	}
	if sw, err := mem.SwapMemory(); err == nil {
		swapTotalMB = int64(sw.Total / (1024 * 1024))
	}

	// Build largest tables list
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
		Platform:          detectPlatform(),
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

// detectPlatform returns the runtime platform, detecting WSL2 specifically.
func detectPlatform() string {
	if runtime.GOOS != "linux" {
		return runtime.GOOS
	}
	data, err := os.ReadFile("/proc/version")
	if err == nil && strings.Contains(strings.ToLower(string(data)), "microsoft") {
		return "wsl2"
	}
	return "linux"
}

// formatHistoricalContext builds a historical context string from past tuning data.
func (s *SmartConfigAnalyzer) formatHistoricalContext() string {
	if s.historyProvider == nil {
		return ""
	}

	var sb strings.Builder

	// Section 1: Parameter trajectory from past tuning runs (filtered by direction)
	tuningHistory, err := s.historyProvider.GetAITuningHistory(0, s.dbType, s.targetDBType)
	if err == nil && len(tuningHistory) > 0 {
		sb.WriteString("\nPARAMETER TRAJECTORY (starting parameters from successive analyses, oldest first):\n")
		for i := len(tuningHistory) - 1; i >= 0; i-- {
			h := tuningHistory[i]
			sb.WriteString(fmt.Sprintf("  %d. %s (%s, %d tables, %s rows):\n",
				len(tuningHistory)-i, h.SourceDBType, h.Timestamp.Format("2006-01-02"),
				h.TotalTables, formatRowCount(h.TotalRows)))
			sb.WriteString(fmt.Sprintf("     workers=%d, chunk_size=%d, read_ahead_buffers=%d, write_ahead_writers=%d, parallel_readers=%d, max_partitions=%d, large_table_threshold=%d, max_source_connections=%d, max_target_connections=%d",
				h.Workers, h.ChunkSize, h.ReadAheadBuffers, h.WriteAheadWriters,
				h.ParallelReaders, h.MaxPartitions, h.LargeTableThreshold,
				h.MaxSourceConnections, h.MaxTargetConnections))
			if h.FinalThroughput > 0 {
				// Always render chunk_retry_count (including 0) so the AI has
				// positive evidence of retry-clean runs. Silently omitting the
				// 0 case primes the model to confabulate retries when applying
				// the retry-rate rule (issue #139).
				sb.WriteString(fmt.Sprintf(" → result: %.0f rows/sec (%.0fs), %d chunk retries",
					h.FinalThroughput, h.FinalDurationSecs, h.ChunkRetryCount))
			}
			sb.WriteString("\n")
		}

		// Detect and warn about downward trends
		if trend := detectParameterTrend(tuningHistory); trend != "" {
			sb.WriteString(fmt.Sprintf("  WARNING: %s\n", trend))
		}

		// Summarize chunk_size vs throughput relationship if we have data
		if summary := summarizeChunkPerformance(tuningHistory); summary != "" {
			sb.WriteString(summary)
		}

		// Summarize retry rate per write_ahead_writers value so the AI sees
		// a pre-computed per-configuration retry rate instead of having to
		// count individual run lines (which it tends to cherry-pick).
		if summary := summarizeWriteAheadWritersRetryRate(tuningHistory); summary != "" {
			sb.WriteString(summary)
		}
	}

	// Section 2: Runtime adjustments as reactive context (NOT recommendations)
	adjustments, err := s.historyProvider.GetAIAdjustments(10)
	if err == nil && len(adjustments) > 0 {
		sb.WriteString("\nRUNTIME ADJUSTMENT LOG (reactive changes made DURING migrations):\n")
		sb.WriteString("  NOTE: These were responses to specific runtime conditions (memory pressure,\n")
		sb.WriteString("  CPU saturation, throughput drops). They are NOT recommendations for starting parameters.\n")
		shown := 0
		for _, adj := range adjustments {
			if shown >= 5 {
				break
			}
			// Show actual parameter values from the adjustment (sorted for deterministic output)
			keys := make([]string, 0, len(adj.Adjustments))
			for k := range adj.Adjustments {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			paramStr := ""
			for _, param := range keys {
				if paramStr != "" {
					paramStr += ", "
				}
				paramStr += fmt.Sprintf("%s→%d", param, adj.Adjustments[param])
			}
			if paramStr == "" {
				paramStr = "(no parameter changes)"
			}
			sb.WriteString(fmt.Sprintf("  - %s: %s (throughput at time: %.0f rows/s)\n",
				adj.Action, paramStr, adj.ThroughputBefore))
			shown++
		}
	}

	return sb.String()
}

// detectParameterTrend checks if key parameters are monotonically decreasing
// across tuning history (which may indicate a downward spiral).
// History is ordered newest-first from the database query.
func detectParameterTrend(history []AITuningRecord) string {
	if len(history) < 2 {
		return ""
	}

	var warnings []string

	// Check for monotonic decrease (newest-first, so history[0] is most recent)
	chunkDecreasing := true
	workerDecreasing := true
	for i := 0; i < len(history)-1; i++ {
		if !chunkDecreasing && !workerDecreasing {
			break
		}
		// i is newer, i+1 is older
		if history[i].ChunkSize >= history[i+1].ChunkSize {
			chunkDecreasing = false
		}
		if history[i].Workers >= history[i+1].Workers {
			workerDecreasing = false
		}
	}

	oldest := history[len(history)-1]
	newest := history[0]

	if chunkDecreasing && oldest.ChunkSize > 0 {
		pctDrop := 100.0 * float64(oldest.ChunkSize-newest.ChunkSize) / float64(oldest.ChunkSize)
		if pctDrop > 30 {
			warnings = append(warnings, fmt.Sprintf(
				"chunk_size decreased %.0f%% over %d runs (%d → %d) — this may be a feedback loop, not a genuine constraint",
				pctDrop, len(history), oldest.ChunkSize, newest.ChunkSize))
		}
	}

	if workerDecreasing && oldest.Workers > 0 {
		pctDrop := 100.0 * float64(oldest.Workers-newest.Workers) / float64(oldest.Workers)
		if pctDrop > 30 {
			warnings = append(warnings, fmt.Sprintf(
				"workers decreased %.0f%% over %d runs (%d → %d) — this may be a feedback loop, not a genuine constraint",
				pctDrop, len(history), oldest.Workers, newest.Workers))
		}
	}

	return strings.Join(warnings, "; ")
}

// summarizeWriteAheadWritersRetryRate aggregates retry rate by
// write_ahead_writers so the per-configuration retry pressure is unmissable
// in the AI prompt. Without this summary the AI tends to cherry-pick the
// clean runs of a high-retry configuration and rationalize away the retried
// ones (observed empirically: an 11-run history with 3 retried runs at waw=2
// was summarized by the AI as "recent runs show zero retries").
func summarizeWriteAheadWritersRetryRate(history []AITuningRecord) string {
	type wawStats struct {
		totalRuns       int
		runsWithRetries int
		totalRetries    int
	}
	byWaw := make(map[int]*wawStats)
	for _, h := range history {
		if h.FinalThroughput <= 0 {
			continue // skip records without a completed run (no retry data either)
		}
		s, ok := byWaw[h.WriteAheadWriters]
		if !ok {
			s = &wawStats{}
			byWaw[h.WriteAheadWriters] = s
		}
		s.totalRuns++
		if h.ChunkRetryCount > 0 {
			s.runsWithRetries++
			s.totalRetries += h.ChunkRetryCount
		}
	}

	if len(byWaw) == 0 {
		return ""
	}

	waws := make([]int, 0, len(byWaw))
	for w := range byWaw {
		waws = append(waws, w)
	}
	sort.Ints(waws)

	var sb strings.Builder
	sb.WriteString("\n  WRITE_AHEAD_WRITERS vs CHUNK RETRY RATE:\n")
	for _, w := range waws {
		s := byWaw[w]
		if s.totalRuns == 0 {
			continue
		}
		retryRate := float64(s.runsWithRetries) / float64(s.totalRuns) * 100
		sb.WriteString(fmt.Sprintf("    write_ahead_writers=%d → %d/%d runs retried (%.0f%% retry rate, %d total chunk retries)\n",
			w, s.runsWithRetries, s.totalRuns, retryRate, s.totalRetries))
	}
	sb.WriteString("    Read this as a per-configuration observation. The retry-rate rule applies ONLY to waw values that show a non-zero retry rate above. If every row above shows 0% retry rate over a meaningful sample, the rule is silent — the choice must be driven by observed throughput from the trajectory, not by an assumed mechanism. Retries are factual events from the chunk_retry_count column; do not invent or repeat causal explanations (e.g. \"transport saturation\", \"connection pool exhaustion\") that are not visible in this column.\n")

	return sb.String()
}

// summarizeChunkPerformance aggregates throughput by chunk_size to show
// the relationship between chunk size and actual performance.
func summarizeChunkPerformance(history []AITuningRecord) string {
	// Group throughput by chunk_size
	type chunkStats struct {
		totalThroughput float64
		count           int
	}
	byChunk := make(map[int]*chunkStats)
	for _, h := range history {
		if h.FinalThroughput <= 0 {
			continue
		}
		s, ok := byChunk[h.ChunkSize]
		if !ok {
			s = &chunkStats{}
			byChunk[h.ChunkSize] = s
		}
		s.totalThroughput += h.FinalThroughput
		s.count++
	}

	if len(byChunk) < 2 {
		return ""
	}

	// Sort chunk sizes for consistent output
	sizes := make([]int, 0, len(byChunk))
	for size := range byChunk {
		sizes = append(sizes, size)
	}
	sort.Ints(sizes)

	var sb strings.Builder
	sb.WriteString("\n  CHUNK SIZE vs THROUGHPUT (averaged across runs):\n")
	for _, size := range sizes {
		s := byChunk[size]
		avg := s.totalThroughput / float64(s.count)
		sb.WriteString(fmt.Sprintf("    chunk_size=%d → avg %.0f rows/sec (%d runs)\n", size, avg, s.count))
	}

	return sb.String()
}

// getAIAutoTune calls the AI to get auto-tuned parameters.
func (s *SmartConfigAnalyzer) getAIAutoTune(ctx context.Context, input AutoTuneInput) (*AutoTuneOutput, error) {
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshaling input: %w", err)
	}

	// Get historical context from past analyses and migrations
	historicalContext := s.formatHistoricalContext()

	// Build memory constraint description
	memConstraint := fmt.Sprintf("Total RAM: %dGB, Available: %dMB", input.MemoryGB, input.AvailableMemoryMB)
	if input.MaxMemoryMB > 0 {
		memConstraint += fmt.Sprintf(", User cap: %dMB", input.MaxMemoryMB)
	}
	if input.SwapTotalMB > 0 {
		memConstraint += fmt.Sprintf(", Swap: %dMB", input.SwapTotalMB)
	}
	if input.Platform == "wsl2" {
		memConstraint += " (WSL2 - shared memory with Windows host, OOM kills crash the VM)"
	}

	// Calculate baseline defaults so the AI knows what "good" looks like
	baselineWorkers := input.CPUCores - 2
	if baselineWorkers < 2 {
		baselineWorkers = 2
	}

	prompt := fmt.Sprintf(`You are a database migration performance tuner. Optimize configuration for MAXIMUM THROUGHPUT while staying within memory limits.

System and Database Info:
%s
%s
Host Environment:
- Platform: %s
- CPU cores: %d
- Memory: %s
- Memory formula: workers * (read_ahead_buffers + write_ahead_writers) * chunk_size * avg_row_bytes / 1024 / 1024
  NOTE: This is the theoretical maximum — actual usage is lower because buffers are not all full simultaneously.

Reference baseline (what the system would use without AI tuning):
- workers: %d (cpu_cores - 2, minimum 2)
- chunk_size: 50000
- read_ahead_buffers: 4
- write_ahead_writers: 2
- parallel_readers: 2
- max_partitions: %d
Your job is to BEAT this baseline using the historical data and table characteristics. Do not recommend fewer workers or smaller buffers than the baseline unless memory is genuinely constrained (estimated_memory_mb > 80%% of available_memory_mb).

Memory constraint:
- estimated_memory_mb must not exceed available_memory_mb minus 2GB headroom
- If a user memory cap (max_memory_mb) is set, stay within it
- On WSL2, exceeding available memory crashes the VM — be more conservative on WSL2 only

Parameters to tune:
- workers: Parallel migration workers (scale with CPU cores, baseline is cpu_cores - 2)
- chunk_size: Rows per batch (50000 is a strong default — only change if historical data shows a better value)
- read_ahead_buffers: Read buffers per worker (4 is a strong default)
- write_ahead_writers: Write threads per worker (2 is a strong default)
- parallel_readers: Parallel readers for large tables (increase for tables with millions of rows)
- max_partitions: Large table partitions (typically matches workers)
- large_table_threshold: Row count before partitioning
- max_source_connections: Source connection pool (workers * parallel_readers + 4)
- max_target_connections: Target connection pool (workers * write_ahead_writers + 4)
- upsert_merge_chunk_size: Batch size for upsert operations
- checkpoint_frequency: How often to checkpoint progress
- max_retries: Retry count for transient failures

Guidelines:
1. MAXIMIZE THROUGHPUT. Use available resources aggressively — the runtime monitor will scale down if needed.
2. Workers should be cpu_cores - 2 unless memory is the bottleneck. Do NOT under-provision workers.
3. chunk_size=50000 is well-tested. Only deviate if historical throughput data clearly shows a better value.
4. read_ahead_buffers=4 is a well-tested floor — do not reduce below this. For write_ahead_writers, consult the WRITE_AHEAD_WRITERS vs CHUNK RETRY RATE summary in the historical context. The rule is mechanical and fires only on observed retries:
   (a) IF a waw value has a non-zero retry rate AND a lower waw has either zero retry rate or hasn't been tried, you MUST pick the lower value.
   (b) IF every waw row in the summary has 0%% retry rate over a meaningful sample (>=3 runs each), the rule does NOT apply — choose by observed throughput from the trajectory.
   (c) IF only one waw value has been tried, the rule does NOT apply — pick by throughput estimate or stay at baseline.
   When you invoke or decline the rule, your "observed_retry_rates" field MUST verbatim cite the literal counts from the summary (e.g. "waw=1: 0/27 (0%%); waw=2: 0/4 (0%%)"). Do not assert a causal mechanism for the choice (e.g. "transport saturation", "memory pressure", "connection saturation") unless that mechanism is directly visible in the data shown to you. If you cannot point at a non-zero retry count or other concrete signal in this prompt, do not name a mechanism — say "lower observed throughput, mechanism unknown" instead.
5. Runtime adjustments in the log were REACTIVE to runtime conditions — do not use them as starting-point recommendations.
6. Row count does not affect optimal parameters — each worker processes one chunk at a time regardless of total rows. Large individual tables benefit from higher parallel_readers.
7. When historical throughput data is available, prefer the parameter combination that achieved the highest measured throughput AND zero chunk retries. A configuration with high peak throughput but recurring retries (>=20%% of runs) is worse than one with slightly lower peak but no retries — the retries cost wall-clock time and predictability. Ignore outlier runs with abnormally low throughput (e.g., less than 50%% of the median) only when chunk_retry_count is also 0 — low throughput WITH retries is a load-related signal, not noise.

Respond with ONLY a JSON object:
{
  "workers": <int>,
  "chunk_size": <int>,
  "read_ahead_buffers": <int>,
  "write_ahead_writers": <int>,
  "parallel_readers": <int>,
  "max_partitions": <int>,
  "large_table_threshold": <int>,
  "max_source_connections": <int>,
  "max_target_connections": <int>,
  "upsert_merge_chunk_size": <int>,
  "checkpoint_frequency": <int>,
  "max_retries": <int>,
  "estimated_memory_mb": <int>,
  "observed_retry_rates": "<verbatim citation from the WRITE_AHEAD_WRITERS vs CHUNK RETRY RATE summary, e.g. 'waw=1: 0/27 (0%%); waw=2: 0/4 (0%%); waw=4: 0/4 (0%%)'. If the summary is empty (no history), write 'no history'.>",
  "reasoning": "<brief explanation; must be consistent with observed_retry_rates and must not assert mechanisms (saturation, pressure, contention) unless backed by data shown in this prompt>"
}`, string(inputJSON), historicalContext, input.Platform, input.CPUCores, memConstraint,
		baselineWorkers, baselineWorkers)

	response, err := s.aiMapper.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("calling AI: %w", err)
	}

	// Extract JSON from response — handles markdown blocks and conversational text
	jsonStr, err := extractJSON(response)
	if err != nil {
		return nil, fmt.Errorf("parsing AI response: %w", err)
	}

	var output AutoTuneOutput
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		logging.Debug("AI response JSON parse error: %s", truncate(jsonStr, 200))
		return nil, fmt.Errorf("parsing AI response JSON: %w", err)
	}

	// Trust AI recommendations - only apply minimal sanity checks for obviously invalid values
	if output.Workers < 1 {
		output.Workers = 1
	}
	if output.ChunkSize < 1000 {
		output.ChunkSize = 1000
	}
	if output.ReadAheadBuffers < 1 {
		output.ReadAheadBuffers = 2
	}
	if output.WriteAheadWriters < 1 {
		output.WriteAheadWriters = 1
	}
	if output.ParallelReaders < 1 {
		output.ParallelReaders = 1
	}
	if output.MaxPartitions < 1 {
		output.MaxPartitions = output.Workers
	}
	if output.MaxSourceConnections < 1 {
		output.MaxSourceConnections = output.Workers + output.ParallelReaders + 2
	}
	if output.MaxTargetConnections < 1 {
		output.MaxTargetConnections = output.Workers*output.WriteAheadWriters + 2
	}
	if output.CheckpointFrequency < 1 {
		output.CheckpointFrequency = 10
	}
	if output.MaxRetries < 1 {
		output.MaxRetries = 3
	}

	return &output, nil
}

// GetOfflineAutoTune calls AI for parameter recommendations without needing a database connection.
// This is useful when analyze is run without source/target connectivity.
func GetOfflineAutoTune(ctx context.Context, input AutoTuneInput) (*AutoTuneOutput, error) {
	aiMapper, err := NewAITypeMapperFromSecrets()
	if err != nil {
		return nil, fmt.Errorf("creating AI mapper: %w", err)
	}

	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshaling input: %w", err)
	}

	// Build memory constraint description
	memConstraint := fmt.Sprintf("Total RAM: %dGB, Available: %dMB", input.MemoryGB, input.AvailableMemoryMB)
	if input.MaxMemoryMB > 0 {
		memConstraint += fmt.Sprintf(", User cap: %dMB", input.MaxMemoryMB)
	}
	if input.SwapTotalMB > 0 {
		memConstraint += fmt.Sprintf(", Swap: %dMB", input.SwapTotalMB)
	}
	if input.Platform == "wsl2" {
		memConstraint += " (WSL2 - shared memory with Windows host, OOM kills crash the VM)"
	}

	// Calculate baseline defaults
	baselineWorkers := input.CPUCores - 2
	if baselineWorkers < 2 {
		baselineWorkers = 2
	}

	prompt := fmt.Sprintf(`You are a database migration performance tuner. Optimize configuration for MAXIMUM THROUGHPUT while staying within memory limits.

System and Database Info:
%s

Host Environment:
- Platform: %s
- CPU cores: %d
- Memory: %s
- Memory formula: workers * (read_ahead_buffers + write_ahead_writers) * chunk_size * avg_row_bytes / 1024 / 1024
  NOTE: This is the theoretical maximum — actual usage is lower because buffers are not all full simultaneously.

Reference baseline (what the system would use without AI tuning):
- workers: %d (cpu_cores - 2, minimum 2)
- chunk_size: 50000
- read_ahead_buffers: 4
- write_ahead_writers: 2
- parallel_readers: 2
- max_partitions: %d
Your job is to BEAT this baseline. Do not recommend fewer workers or smaller read/parallel buffers than the baseline unless memory is genuinely constrained (estimated_memory_mb > 80%% of available_memory_mb). The exception is write_ahead_writers — see guideline 3.

Memory constraint:
- estimated_memory_mb must not exceed available_memory_mb minus 2GB headroom
- If a user memory cap (max_memory_mb) is set, stay within it
- On WSL2, exceeding available memory crashes the VM — be more conservative on WSL2 only

Guidelines:
1. MAXIMIZE THROUGHPUT. Use available resources aggressively — the runtime monitor will scale down if needed.
2. Workers should be cpu_cores - 2 unless memory is the bottleneck.
3. chunk_size=50000 and read_ahead_buffers=4 are well-tested defaults — do not reduce. write_ahead_writers=2 is the default but drop to 1 on platforms with virtualized network transports between the dmt host and the target database (Docker Desktop on macOS or Windows, WSL2, vSphere with vSwitch). On these platforms, 2 write threads × workers concurrent COPY connections can saturate the per-flow throughput limit and produce transient stalls. Native Linux deployments where dmt and the target share a host (Unix socket) or a real NIC should keep write_ahead_writers=2.
4. Connection pool sizes should accommodate workers * readers/writers plus overhead.

Respond with ONLY a JSON object:
{
  "workers": <int>,
  "chunk_size": <int>,
  "read_ahead_buffers": <int>,
  "write_ahead_writers": <int>,
  "parallel_readers": <int>,
  "max_partitions": <int>,
  "large_table_threshold": <int>,
  "max_source_connections": <int>,
  "max_target_connections": <int>,
  "upsert_merge_chunk_size": <int>,
  "checkpoint_frequency": <int>,
  "max_retries": <int>,
  "estimated_memory_mb": <int>,
  "reasoning": "<brief explanation>"
}`, string(inputJSON), input.Platform, input.CPUCores, memConstraint,
		baselineWorkers, baselineWorkers)

	response, err := aiMapper.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("calling AI: %w", err)
	}

	// Extract JSON from response — handles markdown blocks and conversational text
	jsonStr, err := extractJSON(response)
	if err != nil {
		return nil, fmt.Errorf("parsing AI response: %w", err)
	}

	var output AutoTuneOutput
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		logging.Debug("AI response JSON parse error: %s", truncate(jsonStr, 200))
		return nil, fmt.Errorf("parsing AI response JSON: %w", err)
	}

	// Minimal sanity checks
	if output.Workers < 1 {
		output.Workers = 1
	}
	if output.ChunkSize < 1000 {
		output.ChunkSize = 1000
	}
	if output.ReadAheadBuffers < 1 {
		output.ReadAheadBuffers = 2
	}
	if output.WriteAheadWriters < 1 {
		output.WriteAheadWriters = 1
	}
	if output.ParallelReaders < 1 {
		output.ParallelReaders = 1
	}
	if output.MaxPartitions < 1 {
		output.MaxPartitions = output.Workers
	}
	if output.MaxSourceConnections < 1 {
		output.MaxSourceConnections = output.Workers + output.ParallelReaders + 2
	}
	if output.MaxTargetConnections < 1 {
		output.MaxTargetConnections = output.Workers*output.WriteAheadWriters + 2
	}
	if output.CheckpointFrequency < 1 {
		output.CheckpointFrequency = 10
	}
	if output.MaxRetries < 1 {
		output.MaxRetries = 3
	}

	return &output, nil
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

	// Rank columns by likelihood of being "updated at" columns
	return s.rankDateColumns(dateColumns), nil
}

// rankDateColumns sorts date columns by likelihood of being update timestamps.
func (s *SmartConfigAnalyzer) rankDateColumns(columns []string) []string {
	// Common patterns for update timestamp columns (in priority order)
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
		score := len(patterns) + 1 // Default low priority
		for i, pattern := range patterns {
			if matched, _ := regexp.MatchString(pattern, col); matched {
				score = i
				break
			}
		}
		ranked = append(ranked, rankedCol{name: col, score: score})
	}

	// Sort by score (lower is better)
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

	// Common patterns for tables that should be excluded
	excludePatterns := []string{
		`^temp_`,
		`_temp$`,
		`^tmp_`,
		`_tmp$`,
		`^log_`,
		`_log$`,
		`_logs$`,
		`^audit_`,
		`_audit$`,
		`^archive_`,
		`_archive$`,
		`_archived$`,
		`^backup_`,
		`_backup$`,
		`_bak$`,
		`^staging_`,
		`_staging$`,
		`^test_`,
		`_test$`,
		`^__`,           // Double underscore prefix (internal/system)
		`_history$`,     // History tables
		`^sysdiagrams$`, // SQL Server diagram table
		`^aspnet_`,      // ASP.NET membership tables
		`^elmah`,        // ELMAH error logging
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

	// Indicate source of tuning
	if s.AISuggestions != nil {
		sb.WriteString("  # AI-tuned parameters (powered by AI analysis)\n")
	} else {
		sb.WriteString("  # Formula-based parameters (configure AI for smarter tuning)\n")
	}

	// Performance parameters
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

	// Show AI reasoning if available
	if s.AISuggestions != nil && s.AISuggestions.Reasoning != "" {
		sb.WriteString(fmt.Sprintf("  # AI reasoning: %s\n", s.AISuggestions.Reasoning))
	}
	sb.WriteString("\n")

	// Date columns
	if len(s.DateColumns) > 0 {
		sb.WriteString("  # Date columns for incremental sync (priority order)\n")
		sb.WriteString("  date_updated_columns:\n")

		// Collect unique column names in priority order
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

	// Exclude tables
	if len(s.ExcludeTables) > 0 {
		sb.WriteString("  # Tables to exclude (temp/log/archive patterns)\n")
		sb.WriteString("  exclude_tables:\n")
		for _, table := range s.ExcludeTables {
			sb.WriteString(fmt.Sprintf("    - %s\n", table))
		}
		sb.WriteString("\n")
	}

	// Database tuning recommendations
	if s.SourceTuning != nil {
		sb.WriteString(s.formatDatabaseTuning(s.SourceTuning))
	}

	if s.TargetTuning != nil {
		sb.WriteString(s.formatDatabaseTuning(s.TargetTuning))
	}

	// Warnings
	if len(s.Warnings) > 0 {
		sb.WriteString("# Warnings:\n")
		for _, w := range s.Warnings {
			sb.WriteString(fmt.Sprintf("# - %s\n", w))
		}
	}

	return sb.String()
}

// formatDatabaseTuning formats database tuning recommendations in a human-readable format.
func (s *SmartConfigSuggestions) formatDatabaseTuning(tuning *dbtuning.DatabaseTuning) string {
	var sb strings.Builder

	// Header with visual separator
	sb.WriteString("\n")
	sb.WriteString("#" + strings.Repeat("=", 78) + "\n")
	sb.WriteString(fmt.Sprintf("# %s DATABASE TUNING (%s)\n", strings.ToUpper(tuning.Role), strings.ToUpper(tuning.DatabaseType)))
	sb.WriteString("#" + strings.Repeat("=", 78) + "\n")
	sb.WriteString(fmt.Sprintf("# Tuning Potential: %s\n", strings.ToUpper(tuning.TuningPotential)))
	sb.WriteString(fmt.Sprintf("# Impact: %s\n", tuning.EstimatedImpact))
	sb.WriteString("#" + strings.Repeat("-", 78) + "\n\n")

	if len(tuning.Recommendations) == 0 {
		if tuning.TuningPotential == "unknown" {
			// Analysis failed or wasn't performed
			sb.WriteString(fmt.Sprintf("# ⚠ Unable to analyze %s database tuning\n", tuning.Role))
			sb.WriteString(fmt.Sprintf("# Reason: %s\n\n", tuning.EstimatedImpact))
		} else {
			sb.WriteString(fmt.Sprintf("# ✓ No tuning needed - %s database is already well-configured!\n\n", tuning.Role))
		}
		return sb.String()
	}

	// Group by priority
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

	// Format recommendations by priority
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

// formatRecommendation formats a single tuning recommendation in a human-readable format.
func (s *SmartConfigSuggestions) formatRecommendation(num int, rec dbtuning.TuningRecommendation) string {
	var sb strings.Builder

	// Parameter name and number
	sb.WriteString(fmt.Sprintf("#\n# %d. %s\n", num, rec.Parameter))

	// Current vs Recommended (side by side for easy comparison)
	sb.WriteString(fmt.Sprintf("#    Current:     %v\n", rec.CurrentValue))
	sb.WriteString(fmt.Sprintf("#    Recommended: %v\n", rec.RecommendedValue))
	sb.WriteString(fmt.Sprintf("#    Impact:      %s\n", strings.ToUpper(rec.Impact)))

	// Wrap reason text to 75 characters for readability
	sb.WriteString("#\n")
	sb.WriteString("#    Why: " + s.wrapText(rec.Reason, 75, "#         ") + "\n")

	// Show how to apply the change
	if rec.CanApplyRuntime && rec.SQLCommand != "" {
		sb.WriteString("#\n")
		sb.WriteString("#    ✓ Can apply at runtime (no restart needed):\n")
		// Wrap long SQL commands
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

// wrapText wraps text to maxWidth characters with the given prefix for continuation lines
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
			// First word always goes on first line
			result.WriteString(word)
			lineLen = wordLen
		} else if lineLen+1+wordLen > maxWidth {
			// Start new line
			result.WriteString("\n" + contPrefix + word)
			lineLen = len(contPrefix) + wordLen
		} else {
			// Add to current line with space
			result.WriteString(" " + word)
			lineLen += 1 + wordLen
		}
	}

	return result.String()
}
