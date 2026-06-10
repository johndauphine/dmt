// Package driver - smartconfig analyzer.
//
// PR1 of #175 replaced the AI-driven prompt path with the deterministic
// internal/tuning package. The "AI" prefix on file names and exported
// types is preserved to minimize the public-API churn in this PR; a
// follow-up cleanup will rename ai_smartconfig.go -> smartconfig.go and
// drop the unused AISuggestions field on SmartConfigSuggestions.
//
// What lives in this file:
//   - SmartConfigAnalyzer scaffolding and schema-stats collection entrypoint
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
	"github.com/johndauphine/dmt/internal/checkpoint"
	"strings"

	"github.com/johndauphine/dmt/internal/driver/dbtuning"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/tuning"
)

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

	// Reasoning is the tuning engine's short explanation of the picking
	// path, populated from tuning.Output.Reasoning. Always non-empty after
	// the deterministic tuner runs (#202 â€” silence is not a valid signal).
	Reasoning string

	// Tier names which selector picked the WAW/ChunkSize values, populated
	// from tuning.Output.Tier. One of tuning.Tier* constants (#202).
	Tier string

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
	AvgRowBytes  int64 // capped at 2000B (memory-budget math)

	// UncappedAvgRowBytes is the same average without the 2KB cap (#214).
	// Used by the regime classifier so wide-row workloads get bucketed
	// by their actual physical size; AvgRowBytes alone would mis-bucket
	// an 8KB-row workload into a smaller band. Zero is "unknown" and the
	// classifier falls through to AvgRowBytes.
	UncappedAvgRowBytes int64

	// LargestTableBytes is max(RowCount Ã— AvgRowSizeBytes) across ALL
	// tables in the workload (#214). Used by the skew tier classifier.
	// Calculated alongside the avg/max-row passes in calculateAvgRowSize
	// so it sees every table, not just the top-5-by-row-count slice
	// surfaced via LargestTables.
	LargestTableBytes int64

	// Workload identity (#215). Together these form the tuple the
	// Tier 1 exact-identity classifier uses to find historically-
	// comparable runs. Caller (orchestrator) populates from
	// dbconfig.SourceConfig / TargetConfig.
	SourceHost     string
	SourcePort     int
	SourceDatabase string
	SourceSchema   string
	TargetHost     string
	TargetPort     int
	TargetDatabase string
	TargetSchema   string

	LargestTables []TableStats
}

// TableStats is per-table metadata used by the analyzer's avg-row-size
// estimation and (when populated) the largest-tables list on the input.
type TableStats struct {
	Name        string
	RowCount    int64
	AvgRowBytes int64
}

// AutoTuneOutput is preserved for backwards compatibility â€” see the
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
// trimmed the interface â€” the per-WAW / per-chunk_size aggregate methods
// the AI prompt used were dropped; the new tuner reads raw records and
// does its own aggregation in-package.
type TuningHistoryProvider interface {
	// GetRuntimeAdjustments returns recent runtime adjustments. Used by
	// the runtime monitor (separate AI surface; not retired in PR1).
	GetRuntimeAdjustments(limit int) ([]checkpoint.RuntimeAdjustmentRecord, error)

	// GetTuningHistory returns recent tuning recommendations filtered by
	// migration direction, ordered by Timestamp DESC. limit > 0 bounds the
	// slice; the bridge passes 0 for unbounded.
	GetTuningHistory(limit int, sourceType, targetType string) ([]checkpoint.TuningRecord, error)

	// SaveTuningRecord saves a tuning recommendation for future reference.
	SaveTuningRecord(record checkpoint.TuningRecord) error

	// UpdateTuningResult updates the most recent tuning record with final
	// throughput and chunk retry count from the completed run.
	UpdateTuningResult(throughput float64, durationSecs float64, chunkRetryCount int, adjustedAtRuntime bool) error
}

// SmartConfigAnalyzer analyzes source database metadata to suggest optimal
// configuration. PR1 dropped the aiMapper / useAI fields â€” the deterministic
// tuner doesn't need them. PR2 added forceExplore + exploreMode for the
// exploration policy in internal/tuning (#179).
type SmartConfigAnalyzer struct {
	db                       *sql.DB
	dbType                   string
	targetDBType             string
	targetMode               string
	suggestions              *SmartConfigSuggestions
	historyProvider          TuningHistoryProvider
	maxMemoryMB              int64
	pendingSave              *pendingTuningSave
	currentTuning            DBTuningSnapshot
	forceExplore             bool        // mirrors cfg.Migration.Explore
	exploreMode              string      // mirrors cfg.Migration.ExploreMode
	targetProbe              TargetProbe // populated via SetTargetProbe (#166)
	uncappedAvgRowBytes      int64       // pre-cap average row size from sampled tables (#166)
	maxSampledRowBytes       int64       // widest row in sampled tables (#166) â€” used for the packet cap so wide tables can't exceed @@max_allowed_packet
	largestSampledTableBytes int64       // max RowCount Ã— AvgRowSizeBytes across ALL tables (#214) â€” feeds skew-tier classifier; iterating LargestTables[:5] alone would miss a wide-row low-row-count outlier

	// tableNameFilter restricts Analyze to a caller-supplied set of table
	// names (#241). The orchestrator applies include/exclude filters
	// before tuning runs; without scoping Analyze to that same set, an
	// excluded-but-wide table (e.g. an archive blob) would still drive
	// the packet cap and clamp chunk_size for the narrow tables that
	// actually ship. nil means "no filter â€” analyze every table the
	// schema returned" (e.g. the `analyze` CLI subcommand).
	tableNameFilter map[string]bool

	// Workload identity (#215). Populated by SetWorkloadIdentity from
	// the orchestrator's cfg.Source / cfg.Target. Flows through
	// buildAutoTuneInput â†’ toTuningInput â†’ tuning.Input where the
	// Tier 1 classifier reads it.
	identitySourceHost     string
	identitySourcePort     int
	identitySourceDatabase string
	identitySourceSchema   string
	identityTargetHost     string
	identityTargetPort     int
	identityTargetDatabase string
	identityTargetSchema   string
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

// SetHistoryProvider sets the history provider for learning from completed migrations.
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

// SetTargetProbe records the result of a target-side probe (#166).
// The orchestrator probes the target via Driver.ProbeTarget before
// running Analyze; results flow into toTuningProfile so MySQL's
// HardChunkLimit can be derived from the live @@max_allowed_packet
// value rather than guessed.
func (s *SmartConfigAnalyzer) SetTargetProbe(probe TargetProbe) {
	s.targetProbe = probe
}

// TargetHardChunkLimit returns the effective chunk_size cap that the
// orchestrator carries past smartconfig into the runtime controller
// (which would otherwise grow chunks above the packet limit during
// mid-migration tuning â€” Codex review on #166).
//
// Resolution order:
//   - If a target-side probe surfaced a protocol cap (today: MySQL
//     @@max_allowed_packet), the packet-derived limit wins.
//   - Otherwise the driver's static HardChunkLimit applies (0 for all
//     drivers today â€” the runtime controller treats 0 as "no cap").
//
// Returns 0 only when neither a probe nor a static limit applies,
// which is the common case for PG and MSSQL targets. (Copilot review
// on #166 â€” earlier doc claimed "returns 0 when no probe is set",
// which understated the static-limit fallthrough path.)
//
// The packet calc uses s.maxSampledRowBytes (the widest row across
// sampled tables) rather than the average â€” chunk_size is global
// across all tables in a migration, so a workload that mixes narrow
// and wide tables would otherwise let chunks for the wide table
// exceed @@max_allowed_packet. Falls back to the uncapped average if
// max isn't populated.
func (s *SmartConfigAnalyzer) TargetHardChunkLimit() int {
	d, err := Get(s.targetDBType)
	staticLimit := 0
	if err == nil {
		staticLimit = d.HardChunkLimit(s.suggestions.AvgRowSizeBytes)
	}
	packetRowBytes := s.maxSampledRowBytes
	if packetRowBytes == 0 {
		packetRowBytes = s.uncappedAvgRowBytes
	}
	if packetRowBytes == 0 {
		packetRowBytes = s.suggestions.AvgRowSizeBytes
	}
	return chunkLimitFromProbe(staticLimit, s.targetProbe, packetRowBytes)
}

// SetTargetMode sets the migration target mode (drop_recreate or upsert).
func (s *SmartConfigAnalyzer) SetTargetMode(mode string) {
	s.targetMode = mode
}

// SetExploration wires the exploration policy fields from config into the
// analyzer. force corresponds to cfg.Migration.Explore (--explore CLI
// flag); mode corresponds to cfg.Migration.ExploreMode (Îµ strength â€”
// "off" | "low" | "balanced" | "high"). See #179.
func (s *SmartConfigAnalyzer) SetExploration(force bool, mode string) {
	s.forceExplore = force
	s.exploreMode = mode
}

// SetTableNameFilter restricts Analyze to the given set of table names
// (#241). Names are compared case-insensitively. Passing an empty or nil
// set clears any prior filter (all schema tables are analyzed).
//
// Wired from the orchestrator after include/exclude filters have been
// applied to the extracted schema, so derived values that go global
// across the migration â€” the @@max_allowed_packet-derived chunk cap and
// the memory-budget row-size assumptions â€” reflect only the tables that
// will actually be transferred.
func (s *SmartConfigAnalyzer) SetTableNameFilter(allowed []string) {
	if len(allowed) == 0 {
		s.tableNameFilter = nil
		return
	}
	filter := make(map[string]bool, len(allowed))
	for _, name := range allowed {
		filter[strings.ToLower(name)] = true
	}
	s.tableNameFilter = filter
}

// SetWorkloadIdentity wires the (source endpoint, target endpoint) tuple
// from cfg.Source / cfg.Target into the analyzer so the Tier 1 exact-
// identity classifier (#215) can find historically-comparable rows.
// Empty values are stored as-is and naturally skip the Tier 1 lookup
// (the classifier's hasExactIdentity gate rejects empties).
func (s *SmartConfigAnalyzer) SetWorkloadIdentity(sourceHost string, sourcePort int, sourceDB, sourceSchema string, targetHost string, targetPort int, targetDB, targetSchema string) {
	s.identitySourceHost = sourceHost
	s.identitySourcePort = sourcePort
	s.identitySourceDatabase = sourceDB
	s.identitySourceSchema = sourceSchema
	s.identityTargetHost = targetHost
	s.identityTargetPort = targetPort
	s.identityTargetDatabase = targetDB
	s.identityTargetSchema = targetSchema
}

// Analyze performs smart configuration detection on the source database.
func (s *SmartConfigAnalyzer) Analyze(ctx context.Context, schema string) (*SmartConfigSuggestions, error) {
	logging.Debug("Analyzing database schema for configuration suggestions...")

	tables, err := s.getTables(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("getting tables: %w", err)
	}

	// Scope to caller-supplied table set if one was wired in (#241).
	// The orchestrator filters with include/exclude before tuning runs;
	// applying the same scope here keeps the packet cap, avg/max row
	// sizes, and memory-budget math aligned with the actual workload â€”
	// otherwise an excluded wide table can still drive the global cap.
	tables = s.applyTableNameFilter(tables)

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
// output to s.suggestions. Replaces the AI/default branch â€” there's no
