// Package driver contains the smart-configuration analyzer that collects
// schema and system inputs for the deterministic internal/tuning package.
package driver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/driver/dbtuning"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/tuning"
)

const dateColumnDetectionTimeout = 10 * time.Second

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

	// Connection-pool recommendations remain part of the analysis result for
	// YAML/--apply, Web API, and advisory payload consumers. Runtime config
	// independently re-derives its effective pools after applying pinned knobs.
	MaxSourceConnections int
	MaxTargetConnections int

	// Additional tuning parameters
	UpsertMergeChunkSize int
	CheckpointFrequency  int
	MaxRetries           int

	// Database statistics
	TotalTables            int
	TotalRows              int64
	AvgRowSizeBytes        int64 // legacy capped top-five feature
	RepresentativeRowBytes int64 // row-count-weighted width across all in-scope tables
	SafetyRowBytes         int64 // widest observed positive table-average width, or fallback estimate
	SafetyRowBytesKnown    bool  // true only when a positive schema width was observed
	EstimatedMemMB         int64
	// MemoryEstimateOverBudget remains true when a one-row minimum-progress
	// recommendation still exceeds the modeled memory budget.
	MemoryEstimateOverBudget bool

	// Warnings contains any issues detected during analysis
	Warnings []string

	// Reasoning is the tuning engine's short explanation of the picking
	// path, populated from tuning.Output.Reasoning. Always non-empty after
	// the deterministic tuner runs (#202 — silence is not a valid signal).
	Reasoning string

	// Tier names which selector picked the WAW/ChunkSize values, populated
	// from tuning.Output.Tier. One of tuning.Tier* constants (#202).
	Tier string

	// PinnedAdvice carries measured override-cost findings (#461) from
	// tuning.Output; logged by the orchestrator after the reasoning line.
	PinnedAdvice []string

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
	Platform          string
	MaxMemoryMB       int64 // compatibility projection of MemoryBudgetMB
	MemoryBudgetMB    int64

	DatabaseType string
	TargetType   string
	TargetMode   string
	TotalTables  int
	TotalRows    int64
	AvgRowBytes  int64 // legacy capped top-five feature used by persisted history/model.Predict

	// RepresentativeRowBytes is the row-count-weighted width across every
	// in-scope table. SafetyRowBytes is the widest observed positive table
	// average, or the 500-byte fallback when schema widths are unavailable.
	// SafetyRowBytesKnown distinguishes those two cases; a table average is a
	// modeled safety width, not a hard bound on every serialized row.
	RepresentativeRowBytes int64
	SafetyRowBytes         int64
	SafetyRowBytesKnown    bool

	// UncappedAvgRowBytes is the same average without the 2KB cap (#214).
	// Used by the regime classifier so wide-row workloads get bucketed
	// by their actual physical size; AvgRowBytes alone would mis-bucket
	// an 8KB-row workload into a smaller band. Zero is "unknown" and the
	// classifier falls through to AvgRowBytes.
	UncappedAvgRowBytes int64

	// LargestTableBytes is max(RowCount × AvgRowSizeBytes) across ALL
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
	SourcePortless bool
	SourceDatabase string
	SourceSchema   string
	TargetHost     string
	TargetPort     int
	TargetPortless bool
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

// TuningHistoryProvider supplies raw past-run data to the analyzer. The
// deterministic tuner performs its own filtering and aggregation in-package.
type TuningHistoryProvider interface {
	// GetRuntimeAdjustments returns normalized rows first by UTC epoch and ID,
	// followed by unresolved legacy rows in descending ID order.
	GetRuntimeAdjustments(limit int) ([]checkpoint.RuntimeAdjustmentRecord, error)

	// GetTuningHistory returns recommendations filtered by migration direction.
	// Resolved UTC epochs sort first (epoch DESC, ID DESC), followed by
	// unresolved legacy rows (ID DESC). limit > 0 bounds the mixed-order slice;
	// the bridge passes 0 for unbounded count-based tuning.
	GetTuningHistory(limit int, sourceType, targetType string) ([]checkpoint.TuningRecord, error)

	// SaveTuningRecord saves a tuning recommendation for future reference.
	SaveTuningRecord(record checkpoint.TuningRecord) (int64, error)

	// UpdateTuningResult stamps final metrics onto the tuning row created by
	// the completed run.
	UpdateTuningResult(rowID int64, throughput float64, durationSecs float64, chunkRetryCount int, adjustedAtRuntime bool) error
}

// SmartConfigAnalyzer analyzes source database metadata and delegates parameter
// selection to the deterministic tuner.
type SmartConfigAnalyzer struct {
	db                       *sql.DB
	dbType                   string
	targetDBType             string
	targetMode               string
	suggestions              *SmartConfigSuggestions
	historyProvider          TuningHistoryProvider
	memoryCapacityMB         int64
	availableMemoryMB        int64
	memoryBudgetMB           int64
	pendingSave              *pendingTuningSave
	currentTuning            DBTuningSnapshot
	forceExplore             bool        // mirrors cfg.Migration.Explore
	pinnedWriteAheadWriters  *int        // user-pinned WAW for override-cost advice (#461); nil = tuner-managed
	pinnedParallelReaders    *int        // user-pinned PR — advice filters history to the settings that will run
	pinnedReadAheadBuffers   *int        // user-pinned RAB — same
	exploreMode              string      // mirrors cfg.Migration.ExploreMode
	targetProbe              TargetProbe // populated via SetTargetProbe (#166)
	uncappedAvgRowBytes      int64       // legacy uncapped top-five average used by regime classification
	representativeRowBytes   int64       // row-count-weighted width across all in-scope row-bearing tables (#703)
	safetyRowBytes           int64       // widest positive table-average width, or the fallback estimate (#703)
	safetyRowBytesKnown      bool        // true only when at least one positive schema width was observed (#703)
	largestSampledTableBytes int64       // saturated max RowCount Ã— AvgRowSizeBytes across ALL tables (#214/#703)

	// tableNameFilter restricts Analyze to a caller-supplied set of table
	// names (#241). The orchestrator applies include/exclude filters
	// before tuning runs; without scoping Analyze to that same set, an
	// excluded-but-wide table (e.g. an archive blob) would still drive
	// the packet cap and clamp chunk_size for the narrow tables that
	// actually ship. nil means "no filter â€” analyze every table the
	// schema returned" (e.g. the `analyze` CLI subcommand).
	tableNameFilter map[string]bool

	// schemaStatsReader is a package-test seam. Production leaves it nil and
	// resolves the source driver's explicit SchemaStatsProvider capability.
	schemaStatsReader SchemaStatsReader

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

// SetMemoryEnvelope supplies the one config-resolved memory envelope used by
// tuning (#708). Capacity is stable regime metadata; available and budget are
// the effective host/cgroup values resolved during config loading. The
// analyzer must not probe or derive a second memory policy.
func (s *SmartConfigAnalyzer) SetMemoryEnvelope(capacityMB, availableMB, budgetMB int64) {
	s.memoryCapacityMB = capacityMB
	s.availableMemoryMB = availableMB
	s.memoryBudgetMB = budgetMB
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
// The packet calculation uses SafetyRowBytes, the widest observed table
// average. This is the best available sizing estimate, not a guarantee that
// every serialized row fits; structural write-error reduction remains the
// backstop for unusually wide individual rows.
func (s *SmartConfigAnalyzer) TargetHardChunkLimit() int {
	d, err := Get(s.targetDBType)
	staticLimit := 0
	packetRowBytes := s.packetSizingRowBytes()
	if err == nil {
		staticLimit = d.HardChunkLimit(packetRowBytes)
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

// SetPinnedWriteAheadWriters records the user-pinned WAW value so the
// tuner can emit measured override-cost advice (#461). Call only when
// config provenance marks write_ahead_writers as pinned.
func (s *SmartConfigAnalyzer) SetPinnedWriteAheadWriters(v int) {
	s.pinnedWriteAheadWriters = &v
}

// SetPinnedParallelReaders records a user-pinned parallel_readers value
// so override-cost advice compares against the cohort that will run.
func (s *SmartConfigAnalyzer) SetPinnedParallelReaders(v int) {
	s.pinnedParallelReaders = &v
}

// SetPinnedReadAheadBuffers records a user-pinned read_ahead_buffers value.
func (s *SmartConfigAnalyzer) SetPinnedReadAheadBuffers(v int) {
	s.pinnedReadAheadBuffers = &v
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
	s.resetAnalysisState()

	statsReader, err := s.resolveSchemaStatsReader()
	if err != nil {
		return s.formulaOnlySuggestions(err), nil
	}
	tables, err := statsReader.TableStats(ctx, s.db, schema, s.tableFilter())
	if err != nil {
		if cancellationErr := analysisCancellationError(ctx, err); cancellationErr != nil {
			return nil, cancellationErr
		}
		return s.formulaOnlySuggestions(fmt.Errorf("reading table statistics: %w", err)), nil
	}

	s.suggestions.TotalTables = len(tables)
	var totalRows int64
	for _, t := range tables {
		if t.RowCount > 0 {
			totalRows, _ = saturatingAddPositive(totalRows, t.RowCount)
		}
		if s.shouldExcludeTable(t.Name) {
			s.suggestions.ExcludeTables = append(s.suggestions.ExcludeTables, t.Name)
		}
	}
	s.suggestions.TotalRows = totalRows

	// Date-column metadata is advisory. Complete deterministic tuning and arm
	// its pending history save before entering this separate failure domain so
	// a slow or unavailable catalog query cannot discard otherwise valid work.
	s.calculateAutoTuneParams(tables)

	dateCtx, cancelDateLookup := context.WithTimeout(ctx, dateColumnDetectionTimeout)
	dateColumns, dateErr := s.detectDateColumns(dateCtx, statsReader, schema, tables)
	cancelDateLookup()
	if dateErr != nil {
		// The bounded child timeout is an optional-metadata failure, but a
		// caller cancellation must still abort Analyze and must not leave a
		// history record armed for an analysis the caller stopped.
		if cancellationErr := analysisCancellationError(ctx, dateErr); cancellationErr != nil {
			s.pendingSave = nil
			return nil, cancellationErr
		}
		warning := fmt.Sprintf("date-column detection failed: %v", dateErr)
		s.suggestions.Warnings = append(s.suggestions.Warnings, warning)
		logging.Warn("Warning: %s", warning)
	} else {
		s.suggestions.DateColumns = dateColumns
	}

	logging.Debug("Smart config analysis complete:")
	logging.Debug("  - Tables: %d (%s rows)", s.suggestions.TotalTables, formatRowCount(s.suggestions.TotalRows))
	logging.Debug("  - Tables with date columns: %d", len(s.suggestions.DateColumns))
	logging.Debug("  - Suggested exclude tables: %d", len(s.suggestions.ExcludeTables))
	logging.Debug("  - Recommended: workers=%d, chunk_size=%d, read_ahead=%d",
		s.suggestions.Workers, s.suggestions.ChunkSizeRecommendation, s.suggestions.ReadAheadBuffers)
	logging.Debug("  - Estimated memory: %dMB", s.suggestions.EstimatedMemMB)

	return s.suggestions, nil
}

func analysisCancellationError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	// DateColumns has a child timeout, so DeadlineExceeded remains advisory
	// while the parent context is still live.
	return nil
}

func (s *SmartConfigAnalyzer) resetAnalysisState() {
	s.pendingSave = nil
	s.uncappedAvgRowBytes = 0
	s.representativeRowBytes = 0
	s.safetyRowBytes = 0
	s.safetyRowBytesKnown = false
	s.largestSampledTableBytes = 0
	s.suggestions = &SmartConfigSuggestions{
		DateColumns:   make(map[string][]string),
		ExcludeTables: []string{},
		Warnings:      []string{},
	}
}

func (s *SmartConfigAnalyzer) resolveSchemaStatsReader() (SchemaStatsReader, error) {
	if s.schemaStatsReader != nil {
		return s.schemaStatsReader, nil
	}
	d, err := Get(s.dbType)
	if err != nil {
		return nil, err
	}
	provider, ok := d.(SchemaStatsProvider)
	if !ok {
		return nil, fmt.Errorf("source driver %q does not declare schema-statistics support", d.Name())
	}
	reader, supported := provider.SchemaStatsReader()
	if !supported || reader == nil {
		return nil, fmt.Errorf("source driver %q does not support schema statistics", d.Name())
	}
	return reader, nil
}

func (s *SmartConfigAnalyzer) formulaOnlySuggestions(cause error) *SmartConfigSuggestions {
	s.calculateFormulaOnlyParams()
	warning := fmt.Sprintf("schema-statistics analysis unavailable: %v; using formula-only tuning", cause)
	s.suggestions.Warnings = append(s.suggestions.Warnings, warning)
	logging.Warn("Warning: %s", warning)
	// Formula-only output is deliberately not training data. In particular,
	// never let a reused analyzer retain or create a pending history save.
	s.pendingSave = nil
	return s.suggestions
}

// calculateAutoTuneParams runs the deterministic tuner and applies its
// output to s.suggestions. Replaces the AI/default branch â€” there's no
