package driver

import (
	"fmt"
	"github.com/johndauphine/dmt/internal/driver/dbtuning"
	"strings"
)

func (s *SmartConfigSuggestions) FormatYAML() string {
	var sb strings.Builder

	sb.WriteString("# Smart Configuration Suggestions\n")
	fmt.Fprintf(&sb, "# Database: %d tables, %s rows, ~%d bytes/row legacy top-five avg\n",
		s.TotalTables, formatRowCount(s.TotalRows), s.AvgRowSizeBytes)
	fmt.Fprintf(&sb, "# Representative row width: %d bytes (row-count weighted)\n", s.RepresentativeRowBytes)
	safetySource := "500-byte fallback estimate; no positive schema width observed"
	if s.SafetyRowBytesKnown {
		safetySource = "widest observed table-average model; not a per-row bound"
	}
	fmt.Fprintf(&sb, "# Safety row width: %d bytes (%s)\n\n", s.SafetyRowBytes, safetySource)

	sb.WriteString("migration:\n")
	sb.WriteString("  # Deterministic tuner (internal/tuning) -- see #175\n")

	fmt.Fprintf(&sb, "  workers: %d\n", s.Workers)
	fmt.Fprintf(&sb, "  chunk_size: %d\n", s.ChunkSizeRecommendation)
	fmt.Fprintf(&sb, "  read_ahead_buffers: %d\n", s.ReadAheadBuffers)
	fmt.Fprintf(&sb, "  write_ahead_writers: %d\n", s.WriteAheadWriters)
	fmt.Fprintf(&sb, "  parallel_readers: %d\n", s.ParallelReaders)
	fmt.Fprintf(&sb, "  max_partitions: %d\n", s.MaxPartitions)
	fmt.Fprintf(&sb, "  large_table_threshold: %d\n", s.LargeTableThreshold)
	fmt.Fprintf(&sb, "  max_source_connections: %d\n", s.MaxSourceConnections)
	fmt.Fprintf(&sb, "  max_target_connections: %d\n", s.MaxTargetConnections)
	fmt.Fprintf(&sb, "  upsert_merge_chunk_size: %d\n", s.UpsertMergeChunkSize)
	fmt.Fprintf(&sb, "  checkpoint_frequency: %d\n", s.CheckpointFrequency)
	fmt.Fprintf(&sb, "  max_retries: %d\n", s.MaxRetries)
	fmt.Fprintf(&sb, "  # Estimated memory at representative width: ~%dMB (steady transfer uses shared measured-byte admission/MemoryGuard)\n", s.EstimatedMemMB)
	if s.MemoryEstimateOverBudget {
		sb.WriteString("  # WARNING: the final modeled configuration exceeds the memory budget\n")
	}

	if s.Reasoning != "" {
		fmt.Fprintf(&sb, "  # Tuning reasoning: %s\n", s.Reasoning)
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
			fmt.Fprintf(&sb, "    - %s\n", col)
		}
		sb.WriteString("\n")
	}

	if len(s.ExcludeTables) > 0 {
		sb.WriteString("  # Tables to exclude (temp/log/archive patterns)\n")
		sb.WriteString("  exclude_tables:\n")
		for _, table := range s.ExcludeTables {
			fmt.Fprintf(&sb, "    - %s\n", table)
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
			fmt.Fprintf(&sb, "# - %s\n", w)
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
	fmt.Fprintf(&sb, "# %s DATABASE TUNING (%s)\n", strings.ToUpper(tuning.Role), strings.ToUpper(tuning.DatabaseType))
	sb.WriteString("#" + strings.Repeat("=", 78) + "\n")
	fmt.Fprintf(&sb, "# Tuning Potential: %s\n", strings.ToUpper(tuning.TuningPotential))
	fmt.Fprintf(&sb, "# Impact: %s\n", tuning.EstimatedImpact)
	sb.WriteString("#" + strings.Repeat("-", 78) + "\n\n")

	if len(tuning.Recommendations) == 0 {
		if tuning.TuningPotential == "unknown" {
			fmt.Fprintf(&sb, "# WARNING: Unable to analyze %s database tuning\n", tuning.Role)
			fmt.Fprintf(&sb, "# Reason: %s\n\n", tuning.EstimatedImpact)
		} else {
			fmt.Fprintf(&sb, "# OK: No tuning needed - %s database is already well-configured!\n\n", tuning.Role)
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
		sb.WriteString("# CRITICAL (Priority 1) - High Impact Changes\n")
		sb.WriteString("#" + strings.Repeat("-", 78) + "\n")
		for i, rec := range priority1 {
			sb.WriteString(s.formatRecommendation(i+1, rec))
		}
		sb.WriteString("\n")
	}

	if len(priority2) > 0 {
		sb.WriteString("# IMPORTANT (Priority 2) - Medium Impact Changes\n")
		sb.WriteString("#" + strings.Repeat("-", 78) + "\n")
		for i, rec := range priority2 {
			sb.WriteString(s.formatRecommendation(i+1, rec))
		}
		sb.WriteString("\n")
	}

	if len(priority3) > 0 {
		sb.WriteString("# OPTIONAL (Priority 3) - Nice to Have\n")
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

	fmt.Fprintf(&sb, "#\n# %d. %s\n", num, rec.Parameter)
	fmt.Fprintf(&sb, "#    Current:     %v\n", rec.CurrentValue)
	fmt.Fprintf(&sb, "#    Recommended: %v\n", rec.RecommendedValue)
	fmt.Fprintf(&sb, "#    Impact:      %s\n", strings.ToUpper(rec.Impact))
	sb.WriteString("#\n")
	sb.WriteString("#    Why: " + s.wrapText(rec.Reason, 75, "#         ") + "\n")

	if rec.CanApplyRuntime && rec.SQLCommand != "" {
		sb.WriteString("#\n")
		sb.WriteString("#    OK: Can apply at runtime (no restart needed):\n")
		sqlLines := strings.Split(rec.SQLCommand, ";")
		for _, line := range sqlLines {
			line = strings.TrimSpace(line)
			if line != "" {
				sb.WriteString("#      " + line + ";\n")
			}
		}
	} else if rec.RequiresRestart {
		sb.WriteString("#\n")
		sb.WriteString("#    WARNING: Requires database restart\n")
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
