package driver

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"

	"github.com/shirou/gopsutil/v3/mem"
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

// calculateAvgRowSize returns the average row size from the largest
// tables, capped at 2000 bytes for the memory-budget math (the cap
// keeps a single wide table from inflating the per-chunk memory
// estimate beyond what it would actually consume in practice).
//
// Side effects (#166 and #214):
//   - s.uncappedAvgRowBytes: pre-cap average across sampled tables.
//     Used by the regime classifier (#214) so wide-row workloads land
//     in the right band; ClassifyRegime would otherwise see a 2KB-
//     capped value and undercount total_bytes.
//   - s.maxSampledRowBytes: widest row in sampled tables (#166). The
//     packet cap must hold for the worst-case row, not the average â€”
//     chunk_size is global across all tables in a migration, so a mix
//     of narrow and wide tables would otherwise allow chunks that
//     exceed @@max_allowed_packet when inserting the wide one (Codex
//     review on #166).
//   - s.largestSampledTableBytes: max RowCount Ã— AvgRowSizeBytes across
//     ALL tables (#214). Used by the skew tier classifier â€” picking
//     "largest table" from a slice ordered by row count would miss a
//     low-row but extremely wide table that's actually the bytes
//     heavyweight (Copilot review on PR #288).
func (s *SmartConfigAnalyzer) calculateAvgRowSize(tables []tableInfo) int64 {
	// Average: top-5 largest tables only â€” they dominate runtime, so
	// averaging across them approximates the steady-state row size
	// for memory-budget purposes.
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
	uncapped := int64(500)
	if count > 0 {
		uncapped = totalSize / int64(count)
	}
	s.uncappedAvgRowBytes = uncapped

	// Max-row-size AND max-table-bytes both walk all tables (Codex
	// review on #166 + Copilot review on PR #288). For #214, ordering
	// LargestTables by row count and picking [0] would miss a small-
	// row-count, wide-row table that's the actual bytes heavyweight.
	var maxRow int64
	var maxTableBytes int64
	for _, t := range tables {
		if t.AvgRowSizeBytes > maxRow {
			maxRow = t.AvgRowSizeBytes
		}
		if t.RowCount > 0 && t.AvgRowSizeBytes > 0 {
			bytes := t.RowCount * t.AvgRowSizeBytes
			if bytes > maxTableBytes {
				maxTableBytes = bytes
			}
		}
	}
	s.maxSampledRowBytes = maxRow
	s.largestSampledTableBytes = maxTableBytes

	if uncapped > 2000 {
		return 2000
	}
	return uncapped
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
		CPUCores:            cores,
		MemoryGB:            memoryGB,
		AvailableMemoryMB:   availableMemoryMB,
		SwapTotalMB:         swapTotalMB,
		Platform:            DetectPlatform(),
		MaxMemoryMB:         s.maxMemoryMB,
		DatabaseType:        s.dbType,
		TargetType:          s.targetDBType,
		TargetMode:          s.targetMode,
		TotalTables:         s.suggestions.TotalTables,
		TotalRows:           s.suggestions.TotalRows,
		AvgRowBytes:         avgRowSize,
		UncappedAvgRowBytes: s.uncappedAvgRowBytes,
		LargestTableBytes:   s.largestSampledTableBytes,
		LargestTables:       largestTables,
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
