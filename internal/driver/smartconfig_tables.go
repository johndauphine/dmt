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

	"github.com/johndauphine/dmt/internal/tuning"
)

const (
	fallbackRowBytes       int64 = 500
	legacyMaxAvgRowBytes   int64 = 2000
	legacyRowBearingSample       = 5
)

func (s *SmartConfigAnalyzer) applyTableNameFilter(tables []TableStatRow) []TableStatRow {
	if len(s.tableNameFilter) == 0 {
		return tables
	}
	kept := make([]TableStatRow, 0, len(tables))
	for _, t := range tables {
		if s.tableNameFilter[strings.ToLower(t.Name)] {
			kept = append(kept, t)
		}
	}
	return kept
}

func (s *SmartConfigAnalyzer) tableFilter() TableFilter {
	if len(s.tableNameFilter) == 0 {
		return nil
	}
	return func(name string) bool {
		return s.tableNameFilter[strings.ToLower(name)]
	}
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
func (s *SmartConfigAnalyzer) calculateAvgRowSize(tables []TableStatRow) int64 {
	memoryTables := make([]tuning.TableMemoryStat, 0, len(tables))
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
		memoryTables = append(memoryTables, tuning.TableMemoryStat{
			Name:        t.Name,
			RowCount:    t.RowCount,
			AvgRowBytes: t.AvgRowSizeBytes,
		})
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
	expectedTableCount := len(tables)
	if len(s.tableNameFilter) > 0 {
		expectedTableCount = len(s.tableNameFilter)
	}
	s.memoryProfile = tuning.NewMemoryProfileForTableCount(memoryTables, expectedTableCount)
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
func (s *SmartConfigAnalyzer) buildAutoTuneInput(tables []TableStatRow, avgRowSize int64) AutoTuneInput {
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
		MemoryProfile:          s.memoryProfile,
		UncappedAvgRowBytes:    uncappedAvgRowBytes,
		LargestTableBytes:      s.largestSampledTableBytes,
		LargestTables:          largestTables,
		// Workload identity passthrough (#215).
		SourceHost:     s.identitySourceHost,
		SourcePort:     s.identitySourcePort,
		SourcePortless: driverIsPortless(s.dbType),
		SourceDatabase: s.identitySourceDatabase,
		SourceSchema:   s.identitySourceSchema,
		TargetHost:     s.identityTargetHost,
		TargetPort:     s.identityTargetPort,
		TargetPortless: driverIsPortless(s.targetDBType),
		TargetDatabase: s.identityTargetDatabase,
		TargetSchema:   s.identityTargetSchema,
	}
}

func driverIsPortless(dbType string) bool {
	d, err := Get(dbType)
	return err == nil && d.Defaults().Portless
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

// detectDateColumns delegates catalog access to the resolved reader and keeps
// the analyzer's user-facing ranking policy independent of the database.
func (s *SmartConfigAnalyzer) detectDateColumns(
	ctx context.Context,
	reader SchemaStatsReader,
	schema string,
	tables []TableStatRow,
) (map[string][]string, error) {
	allowed := make([]string, 0, len(tables))
	for _, table := range tables {
		allowed = append(allowed, table.Name)
	}
	dateColumns, err := reader.DateColumns(ctx, s.db, schema, allowed)
	if err != nil {
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
