package orchestrator

import (
	"fmt"
	"strings"
	"time"
)

// MigrationSummary is the human-readable daily-ops view of a completed run.
type MigrationSummary struct {
	RunID                string
	Status               string
	StartedAt            time.Time
	CompletedAt          time.Time
	Duration             time.Duration
	TablesTotal          int
	TablesSuccess        int
	TablesFailed         int
	RowsTransferred      int64
	RowsPerSecond        int64
	TableStats           []TableResult
	DeleteReconciliation *DeleteReconciliationSummary
	FailedTables         []string
	Error                string
}

// BuildMigrationSummary projects persisted run result data into the summary
// shape used by operator-facing completion output.
func BuildMigrationSummary(result *MigrationResult) *MigrationSummary {
	if result == nil {
		return nil
	}

	return &MigrationSummary{
		RunID:           result.RunID,
		Status:          result.Status,
		StartedAt:       result.StartedAt,
		CompletedAt:     result.CompletedAt,
		Duration:        time.Duration(result.DurationSeconds * float64(time.Second)).Round(time.Second),
		TablesTotal:     result.TablesTotal,
		TablesSuccess:   result.TablesSuccess,
		TablesFailed:    result.TablesFailed,
		RowsTransferred: result.RowsTransferred,
		RowsPerSecond:   result.RowsPerSecond,
		TableStats:      append([]TableResult(nil), result.TableStats...),
		DeleteReconciliation: cloneDeleteReconciliationSummary(
			result.DeleteReconciliation,
		),
		FailedTables: append([]string(nil), result.FailedTables...),
		Error:        result.Error,
	}
}

// FormatMigrationSummary renders a concise structured block for operators.
func FormatMigrationSummary(result *MigrationResult) string {
	return FormatSummary(BuildMigrationSummary(result))
}

// FormatSummary renders a MigrationSummary. It is intentionally plain text so
// the same block works in terminals, logs, and Slack code blocks.
func FormatSummary(summary *MigrationSummary) string {
	if summary == nil {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("\n==========================================\n")
	sb.WriteString("DMT run summary\n")
	sb.WriteString("==========================================\n")
	writeSummaryLine(&sb, "Run ID", summary.RunID)
	writeSummaryLine(&sb, "Status", summary.Status)
	if !summary.StartedAt.IsZero() {
		writeSummaryLine(&sb, "Started", summary.StartedAt.UTC().Format("2006-01-02 15:04:05 UTC"))
	}
	if !summary.CompletedAt.IsZero() {
		writeSummaryLine(&sb, "Completed", summary.CompletedAt.UTC().Format("2006-01-02 15:04:05 UTC"))
	}
	if summary.Duration > 0 {
		writeSummaryLine(&sb, "Duration", summary.Duration.String())
	}
	writeSummaryLine(&sb, "Tables", fmt.Sprintf("%d total, %d succeeded, %d failed",
		summary.TablesTotal, summary.TablesSuccess, summary.TablesFailed))

	rowsLine := fmt.Sprintf("%s transferred", FormatCount(summary.RowsTransferred))
	if summary.RowsPerSecond > 0 {
		rowsLine += fmt.Sprintf(" (%s rows/sec)", FormatCount(summary.RowsPerSecond))
	}
	writeSummaryLine(&sb, "Rows", rowsLine)

	if len(summary.TableStats) > 0 {
		sb.WriteString("\nTables:\n")
		for _, table := range summary.TableStats {
			line := fmt.Sprintf("  %-30s %12s rows  %s",
				table.Name, FormatCount(table.Rows), table.Status)
			if table.Error != "" {
				line += ": " + table.Error
			}
			sb.WriteString(line)
			sb.WriteByte('\n')
		}
	}

	if summary.DeleteReconciliation != nil {
		writeSummaryLine(&sb, "Deletes", fmt.Sprintf("%s candidate, %s deleted",
			FormatCount(summary.DeleteReconciliation.CandidateRows),
			FormatCount(summary.DeleteReconciliation.DeletedRows)))
		if len(summary.DeleteReconciliation.Tables) > 0 {
			sb.WriteString("\nDelete reconciliation:\n")
			for _, table := range summary.DeleteReconciliation.Tables {
				line := fmt.Sprintf("  %-30s %12s candidate  %12s deleted",
					table.Table,
					FormatCount(table.CandidateRows),
					FormatCount(table.DeletedRows))
				if table.Skipped {
					line += "  skipped"
					if table.SkipReason != "" {
						line += ": " + table.SkipReason
					}
				}
				sb.WriteString(line)
				sb.WriteByte('\n')
			}
		}
	}

	if len(summary.FailedTables) > 0 {
		sb.WriteString("\nFailed tables:\n")
		for _, table := range summary.FailedTables {
			fmt.Fprintf(&sb, "  - %s\n", table)
		}
	}
	if summary.Error != "" {
		writeSummaryLine(&sb, "Error", summary.Error)
	}
	sb.WriteString("==========================================\n")
	return sb.String()
}

func cloneDeleteReconciliationSummary(
	summary *DeleteReconciliationSummary,
) *DeleteReconciliationSummary {
	if summary == nil {
		return nil
	}
	out := *summary
	out.Tables = append([]DeleteReconciliationTableSummary(nil), summary.Tables...)
	return &out
}

func writeSummaryLine(sb *strings.Builder, label, value string) {
	if value == "" {
		return
	}
	fmt.Fprintf(sb, "%-16s: %s\n", label, value)
}

// FormatCount formats integers for operator-facing summaries.
func FormatCount(n int64) string {
	sign := ""
	if n < 0 {
		sign = "-"
		n = -n
	}

	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return sign + s
	}

	var sb strings.Builder
	first := len(s) % 3
	if first == 0 {
		first = 3
	}
	sb.WriteString(s[:first])
	for i := first; i < len(s); i += 3 {
		sb.WriteByte(',')
		sb.WriteString(s[i : i+3])
	}
	return sign + sb.String()
}
