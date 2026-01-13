package driver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"runtime"
	"strings"

	"github.com/johndauphine/data-transfer-tool/internal/logging"
	"github.com/shirou/gopsutil/v3/mem"
)

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
	MaxPartitions       int   // Recommended max partitions for large tables
	LargeTableThreshold int64 // Row count threshold for partitioning

	// Database statistics
	TotalTables    int   // Number of tables analyzed
	TotalRows      int64 // Total rows across all tables
	AvgRowSizeBytes int64 // Average row size in bytes
	EstimatedMemMB int64 // Estimated memory usage with these settings

	// Warnings contains any issues detected during analysis
	Warnings []string

	// AISuggestions contains AI-recommended values (if AI was used)
	AISuggestions *AutoTuneOutput
}

// AutoTuneInput contains system and database info for AI auto-tuning.
type AutoTuneInput struct {
	// System info
	CPUCores     int   `json:"cpu_cores"`
	MemoryGB     int   `json:"memory_gb"`

	// Database info
	DatabaseType string `json:"database_type"` // "mssql" or "postgres"
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
	MaxPartitions       int   `json:"max_partitions"`
	LargeTableThreshold int64 `json:"large_table_threshold"`
	EstimatedMemoryMB   int64 `json:"estimated_memory_mb"`
	Reasoning           string `json:"reasoning,omitempty"`
}

// SmartConfigAnalyzer analyzes source database metadata to suggest optimal configuration.
type SmartConfigAnalyzer struct {
	db          *sql.DB
	dbType      string // "mssql" or "postgres"
	aiMapper    *AITypeMapper
	useAI       bool
	suggestions *SmartConfigSuggestions
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

// Analyze performs smart configuration detection on the source database.
func (s *SmartConfigAnalyzer) Analyze(ctx context.Context, schema string) (*SmartConfigSuggestions, error) {
	logging.Info("Analyzing database schema for configuration suggestions...")

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
	logging.Info("Smart config analysis complete:")
	logging.Info("  - Tables: %d (%s rows)", s.suggestions.TotalTables, formatRowCount(s.suggestions.TotalRows))
	logging.Info("  - Tables with date columns: %d", len(s.suggestions.DateColumns))
	logging.Info("  - Suggested exclude tables: %d", len(s.suggestions.ExcludeTables))
	logging.Info("  - Recommended: workers=%d, chunk_size=%d, read_ahead=%d",
		s.suggestions.Workers, s.suggestions.ChunkSizeRecommendation, s.suggestions.ReadAheadBuffers)
	logging.Info("  - Estimated memory: %dMB", s.suggestions.EstimatedMemMB)

	return s.suggestions, nil
}

// calculateAutoTuneParams calculates all auto-tuned performance parameters.
// Always uses formula-based calculation, optionally gets AI suggestions too.
func (s *SmartConfigAnalyzer) calculateAutoTuneParams(ctx context.Context, tables []tableInfo) {
	// First calculate avg row size
	avgRowSize := s.calculateAvgRowSize(tables)
	s.suggestions.AvgRowSizeBytes = avgRowSize

	// Always calculate formula-based params
	s.calculateFormulaBasedParams(tables, avgRowSize)

	// Also get AI suggestions if available (for comparison)
	if s.useAI && s.aiMapper != nil {
		input := s.buildAutoTuneInput(tables, avgRowSize)
		output, err := s.getAIAutoTune(ctx, input)
		if err == nil && output != nil {
			s.suggestions.AISuggestions = output
			logging.Info("AI suggestions available (see output for comparison)")
		} else {
			logging.Warn("AI auto-tune unavailable: %v", err)
		}
	}
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
	if v, err := mem.VirtualMemory(); err == nil {
		memoryGB = int(v.Total / (1024 * 1024 * 1024))
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
		CPUCores:      cores,
		MemoryGB:      memoryGB,
		DatabaseType:  s.dbType,
		TotalTables:   s.suggestions.TotalTables,
		TotalRows:     s.suggestions.TotalRows,
		AvgRowBytes:   avgRowSize,
		LargestTables: largestTables,
	}
}

// getAIAutoTune calls the AI to get auto-tuned parameters.
func (s *SmartConfigAnalyzer) getAIAutoTune(ctx context.Context, input AutoTuneInput) (*AutoTuneOutput, error) {
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshaling input: %w", err)
	}

	prompt := fmt.Sprintf(`You are a database migration performance expert. Given the following system and database statistics, recommend optimal configuration parameters for a high-performance database migration tool.

System and Database Info:
%s

The migration tool uses these parameters:
- workers: Number of parallel worker goroutines (typically 4-12)
- chunk_size: Rows per batch (typically 50,000-500,000)
- read_ahead_buffers: Buffers per worker for pipelining (typically 2-16)
- max_partitions: Max parallel partitions for large tables (typically matches workers)
- large_table_threshold: Row count above which tables get partitioned (typically 1M-10M)

Guidelines:
- workers should be cores-2 but capped at 12 (diminishing returns beyond)
- chunk_size should target ~50MB per chunk based on avg_row_bytes
- read_ahead_buffers should balance memory usage vs throughput
- Estimate memory as: workers * read_ahead_buffers * 2 * chunk_size * avg_row_bytes
- For MSSQL sources, slightly smaller chunks work better due to TDS protocol
- For PostgreSQL sources, larger chunks work well with COPY protocol

Respond with ONLY a JSON object (no markdown, no explanation outside JSON):
{
  "workers": <int>,
  "chunk_size": <int>,
  "read_ahead_buffers": <int>,
  "max_partitions": <int>,
  "large_table_threshold": <int>,
  "estimated_memory_mb": <int>,
  "reasoning": "<brief 1-2 sentence explanation>"
}`, string(inputJSON))

	response, err := s.aiMapper.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("calling AI: %w", err)
	}

	// Parse JSON response
	response = strings.TrimSpace(response)
	// Remove markdown code blocks if present
	response = strings.TrimPrefix(response, "```json")
	response = strings.TrimPrefix(response, "```")
	response = strings.TrimSuffix(response, "```")
	response = strings.TrimSpace(response)

	var output AutoTuneOutput
	if err := json.Unmarshal([]byte(response), &output); err != nil {
		return nil, fmt.Errorf("parsing AI response: %w (response: %s)", err, response)
	}

	// Validate output ranges
	if output.Workers < 1 || output.Workers > 24 {
		return nil, fmt.Errorf("invalid workers value: %d", output.Workers)
	}
	if output.ChunkSize < 1000 || output.ChunkSize > 1000000 {
		return nil, fmt.Errorf("invalid chunk_size value: %d", output.ChunkSize)
	}
	if output.ReadAheadBuffers < 1 || output.ReadAheadBuffers > 64 {
		return nil, fmt.Errorf("invalid read_ahead_buffers value: %d", output.ReadAheadBuffers)
	}

	return &output, nil
}

// calculateFormulaBasedParams calculates parameters using formulas (fallback).
func (s *SmartConfigAnalyzer) calculateFormulaBasedParams(tables []tableInfo, avgRowSize int64) {
	// Workers: based on CPU cores (cores - 2, clamped to 4-12)
	cores := runtime.NumCPU()
	s.suggestions.Workers = cores - 2
	if s.suggestions.Workers < 4 {
		s.suggestions.Workers = 4
	}
	if s.suggestions.Workers > 12 {
		s.suggestions.Workers = 12
	}

	// Chunk size: based on average row size (target ~50MB per chunk)
	s.suggestions.ChunkSizeRecommendation = s.calculateChunkSize(tables)

	// ReadAheadBuffers: based on memory budget and workers
	// Target ~200MB total memory, divided by workers and chunk size
	targetMemoryMB := int64(200)
	bytesPerChunk := int64(s.suggestions.ChunkSizeRecommendation) * avgRowSize
	buffersPerWorker := (targetMemoryMB * 1024 * 1024) / int64(s.suggestions.Workers) / bytesPerChunk
	s.suggestions.ReadAheadBuffers = int(buffersPerWorker)
	if s.suggestions.ReadAheadBuffers < 2 {
		s.suggestions.ReadAheadBuffers = 2
	}
	if s.suggestions.ReadAheadBuffers > 16 {
		s.suggestions.ReadAheadBuffers = 16
	}

	// MaxPartitions: match workers for parallel processing
	s.suggestions.MaxPartitions = s.suggestions.Workers

	// LargeTableThreshold: tables above this get partitioned
	// Default 1M rows - tables larger than this benefit from parallel partitioning
	s.suggestions.LargeTableThreshold = 1000000

	// EstimatedMemoryMB: calculate expected memory usage
	// Formula: workers * (read_ahead + write_ahead) * chunk_size * avg_row_size
	totalBuffers := int64(s.suggestions.ReadAheadBuffers) * 2 // read + write queues
	s.suggestions.EstimatedMemMB = (int64(s.suggestions.Workers) * totalBuffers *
		int64(s.suggestions.ChunkSizeRecommendation) * avgRowSize) / (1024 * 1024)
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
	Name     string
	RowCount int64
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

// calculateChunkSize recommends an optimal chunk size based on table characteristics.
func (s *SmartConfigAnalyzer) calculateChunkSize(tables []tableInfo) int {
	if len(tables) == 0 {
		return 100000 // Default
	}

	// Find average row size of largest tables
	var totalSize int64
	var count int
	for i, t := range tables {
		if i >= 5 || t.RowCount == 0 { // Only consider top 5 largest tables
			break
		}
		totalSize += t.AvgRowSizeBytes
		count++
	}

	if count == 0 {
		return 100000 // Default
	}

	avgRowSize := totalSize / int64(count)

	// Target ~50MB per chunk for good throughput
	targetChunkBytes := int64(50 * 1024 * 1024)

	chunkSize := int(targetChunkBytes / avgRowSize)

	// Clamp to reasonable range
	if chunkSize < 10000 {
		chunkSize = 10000
	}
	if chunkSize > 500000 {
		chunkSize = 500000
	}

	// Round to nice number
	if chunkSize >= 100000 {
		chunkSize = (chunkSize / 50000) * 50000
	} else {
		chunkSize = (chunkSize / 10000) * 10000
	}

	return chunkSize
}

// FormatYAML returns the suggestions formatted as YAML config.
func (s *SmartConfigSuggestions) FormatYAML() string {
	var sb strings.Builder

	sb.WriteString("# Smart Configuration Suggestions\n")
	sb.WriteString(fmt.Sprintf("# Database: %d tables, %s rows, ~%d bytes/row avg\n\n",
		s.TotalTables, formatRowCount(s.TotalRows), s.AvgRowSizeBytes))

	sb.WriteString("migration:\n")

	// Auto-tuned performance parameters (formula-based)
	sb.WriteString("  # Auto-tuned performance parameters (formula-based)\n")
	sb.WriteString(fmt.Sprintf("  workers: %d                    # CPU cores - 2, capped 4-12\n", s.Workers))
	sb.WriteString(fmt.Sprintf("  chunk_size: %d              # ~50MB per chunk\n", s.ChunkSizeRecommendation))
	sb.WriteString(fmt.Sprintf("  read_ahead_buffers: %d           # memory budget / chunk size\n", s.ReadAheadBuffers))
	sb.WriteString(fmt.Sprintf("  max_partitions: %d              # matches worker count\n", s.MaxPartitions))
	sb.WriteString(fmt.Sprintf("  large_table_threshold: %d  # rows before partitioning\n", s.LargeTableThreshold))
	sb.WriteString(fmt.Sprintf("  # Estimated memory: ~%dMB\n", s.EstimatedMemMB))

	// Show AI suggestions if available
	if s.AISuggestions != nil {
		ai := s.AISuggestions
		sb.WriteString("\n  # AI-suggested alternatives (configure ai.api_key to enable):\n")
		sb.WriteString(fmt.Sprintf("  # workers: %d\n", ai.Workers))
		sb.WriteString(fmt.Sprintf("  # chunk_size: %d\n", ai.ChunkSize))
		sb.WriteString(fmt.Sprintf("  # read_ahead_buffers: %d\n", ai.ReadAheadBuffers))
		sb.WriteString(fmt.Sprintf("  # max_partitions: %d\n", ai.MaxPartitions))
		sb.WriteString(fmt.Sprintf("  # large_table_threshold: %d\n", ai.LargeTableThreshold))
		sb.WriteString(fmt.Sprintf("  # Estimated memory: ~%dMB\n", ai.EstimatedMemoryMB))
		if ai.Reasoning != "" {
			sb.WriteString(fmt.Sprintf("  # Reasoning: %s\n", ai.Reasoning))
		}
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

	// Warnings
	if len(s.Warnings) > 0 {
		sb.WriteString("# Warnings:\n")
		for _, w := range s.Warnings {
			sb.WriteString(fmt.Sprintf("# - %s\n", w))
		}
	}

	return sb.String()
}
