package driver

import (
	"context"
	"fmt"
	"math"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
)

const (
	fallbackRowBytes       int64 = 500
	legacyMaxAvgRowBytes   int64 = 2000
	legacyRowBearingSample       = 5
)

func (s *SmartConfigAnalyzer) applyTableNameFilter(tables []tableInfo) []tableInfo {
	if len(s.tableNameFilter) == 0 {
		return tables
	}
	kept := make([]tableInfo, 0, len(tables))
	for _, t := range tables {
		if s.tableNameFilter[strings.ToLower(t.Name)] {
			kept = append(kept, t)
		}
	}
	return kept
}

// calculateAvgRowSize returns the legacy capped top-five width used by
// persisted history and model.Predict. It also derives the additive #703
// representative and safety widths in one overflow-safe pass across every
// in-scope table.
//
// The legacy values remain the arithmetic mean of positive widths among the
// first five row-bearing input tables: uncapped in uncappedAvgRowBytes and
// capped at 2 KiB in the return value. New fields must not be aliased to that
// historical feature.
func (s *SmartConfigAnalyzer) calculateAvgRowSize(tables []tableInfo) int64 {
	var legacyWidths [legacyRowBearingSample]int64
	legacyRowBearing := 0
	legacyWidthCount := 0
	var weightedRows int64
	var weightedBytes int64
	weightedOverflow := false
	var safetyRowBytes int64
	safetyKnown := false
	var maxTableBytes int64

	for _, t := range tables {
		// Preserve the historical feature: the first five row-bearing input
		// tables define the sample, and only their positive widths contribute.
		if legacyRowBearing < legacyRowBearingSample && t.RowCount > 0 {
			legacyRowBearing++
			if t.AvgRowSizeBytes > 0 {
				legacyWidths[legacyWidthCount] = t.AvgRowSizeBytes
				legacyWidthCount++
			}
		}

		if t.AvgRowSizeBytes > 0 {
			safetyKnown = true
			if t.AvgRowSizeBytes > safetyRowBytes {
				safetyRowBytes = t.AvgRowSizeBytes
			}
		}

		if t.RowCount <= 0 || t.AvgRowSizeBytes <= 0 {
			continue
		}
		tableBytes, productOverflow := saturatingMultiplyPositive(t.RowCount, t.AvgRowSizeBytes)
		var rowsOverflow, bytesOverflow bool
		weightedRows, rowsOverflow = saturatingAddPositive(weightedRows, t.RowCount)
		weightedBytes, bytesOverflow = saturatingAddPositive(weightedBytes, tableBytes)
		weightedOverflow = weightedOverflow || productOverflow || rowsOverflow || bytesOverflow
		if tableBytes > maxTableBytes {
			maxTableBytes = tableBytes
		}
	}

	uncapped := fallbackRowBytes
	if legacyWidthCount > 0 {
		uncapped = overflowSafeMean(legacyWidths[:legacyWidthCount])
	}
	representative := fallbackRowBytes
	if weightedOverflow {
		// Once the weighted numerator or denominator exceeds int64, dividing
		// independently saturated values can manufacture a tiny width (for
		// example MaxInt64/MaxInt64 == 1). Use the observed safety width for
		// these pathological inputs so overflow never looks deceptively narrow.
		representative = safetyRowBytes
	} else if weightedRows > 0 {
		representative = weightedBytes / weightedRows
		if representative <= 0 {
			representative = fallbackRowBytes
		}
	}
	if !safetyKnown {
		safetyRowBytes = fallbackRowBytes
	}

	s.uncappedAvgRowBytes = uncapped
	s.representativeRowBytes = representative
	s.safetyRowBytes = safetyRowBytes
	s.safetyRowBytesKnown = safetyKnown
	s.largestSampledTableBytes = maxTableBytes

	if uncapped > legacyMaxAvgRowBytes {
		return legacyMaxAvgRowBytes
	}
	return uncapped
}

func saturatingMultiplyPositive(left, right int64) (value int64, overflow bool) {
	if left <= 0 || right <= 0 {
		return 0, false
	}
	if left > math.MaxInt64/right {
		return math.MaxInt64, true
	}
	return left * right, false
}

func saturatingAddPositive(left, right int64) (value int64, overflow bool) {
	if left < 0 {
		left = 0
	}
	if right <= 0 {
		return left, false
	}
	if left > math.MaxInt64-right {
		return math.MaxInt64, true
	}
	return left + right, false
}

// overflowSafeMean returns the exact floor of the arithmetic mean without
// summing the inputs into an int64 first. The slice is bounded to five values,
// but each width may itself be near MaxInt64 in malformed fixture data.
func overflowSafeMean(values []int64) int64 {
	if len(values) == 0 {
		return fallbackRowBytes
	}
	count := int64(len(values))
	var quotientSum int64
	var remainderSum int64
	for _, value := range values {
		quotientSum += value / count
		remainderSum += value % count
	}
	return quotientSum + remainderSum/count
}

// buildAutoTuneInput constructs input for the tuner.
func (s *SmartConfigAnalyzer) buildAutoTuneInput(tables []tableInfo, avgRowSize int64) AutoTuneInput {
	cores := runtime.NumCPU()
	memoryGB := int((s.memoryCapacityMB + 1023) / 1024)
	representativeRowBytes := s.representativeRowBytes
	if representativeRowBytes <= 0 {
		representativeRowBytes = fallbackRowBytes
	}
	safetyRowBytes := s.safetyRowBytes
	if safetyRowBytes <= 0 {
		safetyRowBytes = fallbackRowBytes
	}
	uncappedAvgRowBytes := s.uncappedAvgRowBytes
	if uncappedAvgRowBytes <= 0 {
		uncappedAvgRowBytes = avgRowSize
		if uncappedAvgRowBytes <= 0 {
			uncappedAvgRowBytes = fallbackRowBytes
		}
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
		CPUCores:               cores,
		MemoryGB:               memoryGB,
		AvailableMemoryMB:      s.availableMemoryMB,
		Platform:               DetectPlatform(),
		MaxMemoryMB:            s.memoryBudgetMB,
		MemoryBudgetMB:         s.memoryBudgetMB,
		DatabaseType:           s.dbType,
		TargetType:             s.targetDBType,
		TargetMode:             s.targetMode,
		TotalTables:            s.suggestions.TotalTables,
		TotalRows:              s.suggestions.TotalRows,
		AvgRowBytes:            avgRowSize,
		RepresentativeRowBytes: representativeRowBytes,
		SafetyRowBytes:         safetyRowBytes,
		SafetyRowBytesKnown:    s.safetyRowBytesKnown,
		UncappedAvgRowBytes:    uncappedAvgRowBytes,
		LargestTableBytes:      s.largestSampledTableBytes,
		LargestTables:          largestTables,
		// Workload identity passthrough (#215).
		SourceHost:     s.identitySourceHost,
		SourcePort:     s.identitySourcePort,
		SourceDatabase: s.identitySourceDatabase,
		SourceSchema:   s.identitySourceSchema,
		TargetHost:     s.identityTargetHost,
		TargetPort:     s.identityTargetPort,
		TargetDatabase: s.identityTargetDatabase,
		TargetSchema:   s.identityTargetSchema,
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

// detectDateColumns finds columns that could be used for incremental sync in
// one schema-wide metadata query. Results for tables outside the already
// scoped stats set are discarded client-side.
func (s *SmartConfigAnalyzer) detectDateColumns(ctx context.Context, schema string, allowedTables []tableInfo) (map[string][]string, error) {
	dateColumns := make(map[string][]string)
	if len(allowedTables) == 0 {
		return dateColumns, nil
	}

	var query string
	switch s.dbType {
	case "mssql":
		query = `
			SELECT tbl.name, c.name
			FROM sys.columns c
			INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
			INNER JOIN sys.tables tbl ON c.object_id = tbl.object_id
			INNER JOIN sys.schemas s ON tbl.schema_id = s.schema_id
			WHERE s.name = @p1
			  AND t.name IN ('datetime', 'datetime2', 'datetimeoffset', 'date', 'timestamp')
			ORDER BY tbl.name, c.column_id`
	case "postgres":
		query = `
			SELECT table_name, column_name
			FROM information_schema.columns
			WHERE table_schema = $1
			  AND data_type IN ('timestamp without time zone', 'timestamp with time zone', 'date')
			ORDER BY table_name, ordinal_position`
	case "mysql":
		query = `
			SELECT TABLE_NAME, COLUMN_NAME
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = ?
			  AND DATA_TYPE IN ('datetime', 'timestamp', 'date')
			ORDER BY TABLE_NAME, ORDINAL_POSITION`
	default:
		return nil, fmt.Errorf("unsupported database type: %s", s.dbType)
	}

	rows, err := s.db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Keep the table name returned by the stats query as the public map key,
	// matching the prior per-table behavior. Exact matches take precedence;
	// the case-folded lookup only handles engines whose metadata surfaces use
	// different casing, and rejects ambiguous case-only table names.
	allowedExact := make(map[string]string, len(allowedTables))
	allowedFolded := make(map[string]string, len(allowedTables))
	for _, table := range allowedTables {
		allowedExact[table.Name] = table.Name
		folded := strings.ToLower(table.Name)
		if existing, ok := allowedFolded[folded]; ok && existing != table.Name {
			allowedFolded[folded] = ""
		} else if !ok {
			allowedFolded[folded] = table.Name
		}
	}

	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			return nil, err
		}

		canonical, allowed := allowedExact[table]
		if !allowed {
			canonical, allowed = allowedFolded[strings.ToLower(table)]
			allowed = allowed && canonical != ""
		}
		if !allowed {
			continue
		}
		dateColumns[canonical] = append(dateColumns[canonical], column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for table, columns := range dateColumns {
		dateColumns[table] = s.rankDateColumns(columns)
	}
	return dateColumns, nil
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

	// The schema query supplies ordinal order. A stable sort raises likely
	// update timestamps while preserving that database order for equal scores.
	sort.SliceStable(ranked, func(i, j int) bool {
		return ranked[i].score < ranked[j].score
	})

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
