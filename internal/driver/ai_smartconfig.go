package driver

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
)

// SmartConfigSuggestions contains AI-detected configuration suggestions.
type SmartConfigSuggestions struct {
	// DateColumns maps table names to suggested date_updated_columns
	DateColumns map[string][]string

	// ExcludeTables lists tables that should probably be excluded
	ExcludeTables []string

	// ChunkSizeRecommendation is the suggested chunk size based on table analysis
	ChunkSizeRecommendation int

	// Warnings contains any issues detected during analysis
	Warnings []string
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

	// Analyze each table
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

	// Calculate chunk size recommendation based on largest tables
	s.suggestions.ChunkSizeRecommendation = s.calculateChunkSize(tables)

	// Log summary
	logging.Info("Smart config analysis complete:")
	logging.Info("  - Tables with date columns: %d", len(s.suggestions.DateColumns))
	logging.Info("  - Suggested exclude tables: %d", len(s.suggestions.ExcludeTables))
	logging.Info("  - Recommended chunk size: %d", s.suggestions.ChunkSizeRecommendation)

	return s.suggestions, nil
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

	sb.WriteString("# AI-detected configuration suggestions\n\n")

	// Date columns
	if len(s.DateColumns) > 0 {
		sb.WriteString("# Recommended date_updated_columns for incremental sync:\n")
		sb.WriteString("# (Listed in priority order - first match per table is used)\n")
		sb.WriteString("migration:\n")
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
		sb.WriteString("# Suggested tables to exclude:\n")
		sb.WriteString("  exclude_tables:\n")
		for _, table := range s.ExcludeTables {
			sb.WriteString(fmt.Sprintf("    - %s\n", table))
		}
		sb.WriteString("\n")
	}

	// Chunk size
	if s.ChunkSizeRecommendation > 0 {
		sb.WriteString(fmt.Sprintf("  # Recommended chunk size based on table analysis\n"))
		sb.WriteString(fmt.Sprintf("  chunk_size: %d\n", s.ChunkSizeRecommendation))
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
