package calibration

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// RunStatus represents the outcome of a calibration run.
type RunStatus string

const (
	StatusSuccess RunStatus = "success"
	StatusFailed  RunStatus = "failed"
)

// ErrorCategory classifies the type of error that occurred.
type ErrorCategory string

const (
	ErrorCategoryNone       ErrorCategory = ""
	ErrorCategoryOOM        ErrorCategory = "oom"
	ErrorCategoryTimeout    ErrorCategory = "timeout"
	ErrorCategoryPermission ErrorCategory = "permission"
	ErrorCategoryDeadlock   ErrorCategory = "deadlock"
	ErrorCategoryOther      ErrorCategory = "other"
)

// RunMetrics captures measurements from a single calibration run.
type RunMetrics struct {
	ConfigName       string
	Config           CalibrationConfig
	Status           RunStatus
	ErrorCategory    ErrorCategory
	Duration         time.Duration
	RowsTransferred  int64
	RowsPerSecond    float64
	QueryTimePercent float64
	ScanTimePercent  float64
	WriteTimePercent float64
	Error            string
}

// RecommendedConfig is the AI-recommended or best-observed configuration.
type RecommendedConfig struct {
	ChunkSize           int    `json:"chunk_size"`
	Workers             int    `json:"workers"`
	ReadAheadBuffers    int    `json:"read_ahead_buffers"`
	ParallelReaders     int    `json:"parallel_readers,omitempty"`
	WriteAheadWriters   int    `json:"write_ahead_writers,omitempty"`
	PacketSize          int    `json:"packet_size,omitempty"` // MSSQL only
	EstimatedRowsPerSec int64  `json:"estimated_rows_per_sec"`
	Confidence          string `json:"confidence"` // "high", "medium", "low"
	// Extended parameters
	MaxPartitions         int   `json:"max_partitions,omitempty"`
	LargeTableThreshold   int64 `json:"large_table_threshold,omitempty"`
	MSSQLRowsPerBatch     int   `json:"mssql_rows_per_batch,omitempty"`     // MSSQL source/target only
	UpsertMergeChunkSize  int   `json:"upsert_merge_chunk_size,omitempty"`
	MaxSourceConnections  int   `json:"max_source_connections,omitempty"`  // Source connection pool
	MaxTargetConnections  int   `json:"max_target_connections,omitempty"`  // Target connection pool
}

// CalibrationResult contains all runs and the final recommendation.
type CalibrationResult struct {
	Runs             []RunMetrics
	TablesUsed       []string
	TotalSampleRows  int64
	SourceLatencyMs  float64
	TargetLatencyMs  float64
	Recommendation   *RecommendedConfig
	AIReasoning      string
	PatternsDetected []string
	Warnings         []string
	AIUsed           bool
}

// CategorizeError classifies an error based on its message.
func CategorizeError(err error) ErrorCategory {
	if err == nil {
		return ErrorCategoryNone
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "out of memory") || strings.Contains(msg, "oom") ||
		strings.Contains(msg, "memory") && strings.Contains(msg, "alloc"):
		return ErrorCategoryOOM
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline") ||
		strings.Contains(msg, "context canceled"):
		return ErrorCategoryTimeout
	case strings.Contains(msg, "permission") || strings.Contains(msg, "denied") ||
		strings.Contains(msg, "access"):
		return ErrorCategoryPermission
	case strings.Contains(msg, "deadlock"):
		return ErrorCategoryDeadlock
	default:
		return ErrorCategoryOther
	}
}

// BestSuccessfulRun returns the run with the highest throughput that succeeded.
func (r *CalibrationResult) BestSuccessfulRun() *RunMetrics {
	var best *RunMetrics
	for i := range r.Runs {
		run := &r.Runs[i]
		if run.Status != StatusSuccess {
			continue
		}
		if best == nil || run.RowsPerSecond > best.RowsPerSecond {
			best = run
		}
	}
	return best
}

// SuccessCount returns the number of successful runs.
func (r *CalibrationResult) SuccessCount() int {
	count := 0
	for _, run := range r.Runs {
		if run.Status == StatusSuccess {
			count++
		}
	}
	return count
}

// FormatResultsTable returns a formatted table of calibration results.
func (r *CalibrationResult) FormatResultsTable() string {
	var sb strings.Builder

	// Header
	sb.WriteString(fmt.Sprintf("  %-24s %-8s %10s %8s %8s %8s\n",
		"Config", "Status", "Rows/sec", "Query%", "Scan%", "Write%"))
	sb.WriteString(fmt.Sprintf("  %s\n", strings.Repeat("-", 72)))

	// Rows
	for _, run := range r.Runs {
		status := "OK"
		rowsPerSec := fmt.Sprintf("%d", int64(run.RowsPerSecond))
		queryPct := fmt.Sprintf("%.0f%%", run.QueryTimePercent)
		scanPct := fmt.Sprintf("%.0f%%", run.ScanTimePercent)
		writePct := fmt.Sprintf("%.0f%%", run.WriteTimePercent)

		if run.Status == StatusFailed {
			status = "FAILED"
			rowsPerSec = "-"
			queryPct = "-"
			scanPct = "-"
			writePct = "-"
			if run.ErrorCategory != ErrorCategoryNone {
				status = fmt.Sprintf("FAILED (%s)", run.ErrorCategory)
			}
		}

		sb.WriteString(fmt.Sprintf("  %-24s %-8s %10s %8s %8s %8s\n",
			run.ConfigName, status, rowsPerSec, queryPct, scanPct, writePct))
	}

	return sb.String()
}

// FormatRecommendation returns a formatted recommendation string.
func (r *CalibrationResult) FormatRecommendation() string {
	if r.Recommendation == nil {
		return "No recommendation available (all runs failed)"
	}

	var sb strings.Builder

	source := "Best observed"
	if r.AIUsed {
		source = "AI"
	}

	sb.WriteString(fmt.Sprintf("\n%s Recommendation: %d rows/sec\n", source, r.Recommendation.EstimatedRowsPerSec))
	sb.WriteString(fmt.Sprintf("Confidence: %s\n", r.Recommendation.Confidence))

	if r.AIReasoning != "" {
		sb.WriteString(fmt.Sprintf("\nReasoning: %s\n", r.AIReasoning))
	}

	if len(r.PatternsDetected) > 0 {
		sb.WriteString("\nPatterns detected:\n")
		for _, p := range r.PatternsDetected {
			sb.WriteString(fmt.Sprintf("  - %s\n", p))
		}
	}

	sb.WriteString("\nRecommended config:\n")
	sb.WriteString(fmt.Sprintf("  chunk_size: %d\n", r.Recommendation.ChunkSize))
	sb.WriteString(fmt.Sprintf("  workers: %d\n", r.Recommendation.Workers))
	sb.WriteString(fmt.Sprintf("  read_ahead_buffers: %d\n", r.Recommendation.ReadAheadBuffers))
	if r.Recommendation.ParallelReaders > 0 {
		sb.WriteString(fmt.Sprintf("  parallel_readers: %d\n", r.Recommendation.ParallelReaders))
	}
	if r.Recommendation.WriteAheadWriters > 0 {
		sb.WriteString(fmt.Sprintf("  write_ahead_writers: %d\n", r.Recommendation.WriteAheadWriters))
	}
	if r.Recommendation.PacketSize > 0 {
		sb.WriteString(fmt.Sprintf("  mssql_packet_size: %d\n", r.Recommendation.PacketSize))
	}
	// Extended parameters
	if r.Recommendation.MaxPartitions > 0 {
		sb.WriteString(fmt.Sprintf("  max_partitions: %d\n", r.Recommendation.MaxPartitions))
	}
	if r.Recommendation.LargeTableThreshold > 0 {
		sb.WriteString(fmt.Sprintf("  large_table_threshold: %d\n", r.Recommendation.LargeTableThreshold))
	}
	if r.Recommendation.MSSQLRowsPerBatch > 0 {
		sb.WriteString(fmt.Sprintf("  mssql_rows_per_batch: %d\n", r.Recommendation.MSSQLRowsPerBatch))
	}
	if r.Recommendation.UpsertMergeChunkSize > 0 {
		sb.WriteString(fmt.Sprintf("  upsert_merge_chunk_size: %d\n", r.Recommendation.UpsertMergeChunkSize))
	}
	if r.Recommendation.MaxSourceConnections > 0 {
		sb.WriteString(fmt.Sprintf("  max_source_connections: %d\n", r.Recommendation.MaxSourceConnections))
	}
	if r.Recommendation.MaxTargetConnections > 0 {
		sb.WriteString(fmt.Sprintf("  max_target_connections: %d\n", r.Recommendation.MaxTargetConnections))
	}

	if len(r.Warnings) > 0 {
		sb.WriteString("\nWarnings:\n")
		for _, w := range r.Warnings {
			sb.WriteString(fmt.Sprintf("  - %s\n", w))
		}
	}

	return sb.String()
}

// FormatYAML returns the recommended config as YAML for easy copy-paste.
func (r *CalibrationResult) FormatYAML() string {
	if r.Recommendation == nil {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("migration:\n")
	sb.WriteString(fmt.Sprintf("  chunk_size: %d\n", r.Recommendation.ChunkSize))
	sb.WriteString(fmt.Sprintf("  workers: %d\n", r.Recommendation.Workers))
	sb.WriteString(fmt.Sprintf("  read_ahead_buffers: %d\n", r.Recommendation.ReadAheadBuffers))

	// Include optional AI-recommended settings when provided
	if r.Recommendation.ParallelReaders > 0 {
		sb.WriteString(fmt.Sprintf("  parallel_readers: %d\n", r.Recommendation.ParallelReaders))
	}
	if r.Recommendation.WriteAheadWriters > 0 {
		sb.WriteString(fmt.Sprintf("  write_ahead_writers: %d\n", r.Recommendation.WriteAheadWriters))
	}
	if r.Recommendation.PacketSize > 0 {
		sb.WriteString(fmt.Sprintf("  mssql_packet_size: %d\n", r.Recommendation.PacketSize))
	}
	// Extended parameters
	if r.Recommendation.MaxPartitions > 0 {
		sb.WriteString(fmt.Sprintf("  max_partitions: %d\n", r.Recommendation.MaxPartitions))
	}
	if r.Recommendation.LargeTableThreshold > 0 {
		sb.WriteString(fmt.Sprintf("  large_table_threshold: %d\n", r.Recommendation.LargeTableThreshold))
	}
	if r.Recommendation.MSSQLRowsPerBatch > 0 {
		sb.WriteString(fmt.Sprintf("  mssql_rows_per_batch: %d\n", r.Recommendation.MSSQLRowsPerBatch))
	}
	if r.Recommendation.UpsertMergeChunkSize > 0 {
		sb.WriteString(fmt.Sprintf("  upsert_merge_chunk_size: %d\n", r.Recommendation.UpsertMergeChunkSize))
	}
	if r.Recommendation.MaxSourceConnections > 0 {
		sb.WriteString(fmt.Sprintf("  max_source_connections: %d\n", r.Recommendation.MaxSourceConnections))
	}
	if r.Recommendation.MaxTargetConnections > 0 {
		sb.WriteString(fmt.Sprintf("  max_target_connections: %d\n", r.Recommendation.MaxTargetConnections))
	}

	sb.WriteString(fmt.Sprintf("  # Estimated throughput: ~%d rows/sec\n", r.Recommendation.EstimatedRowsPerSec))

	return sb.String()
}

// ApplyToConfigFile updates an existing config file with the recommended values.
// It preserves the existing structure, comments, and non-migration settings.
func (rec *RecommendedConfig) ApplyToConfigFile(configPath string) error {
	if rec == nil {
		return fmt.Errorf("no recommendation available")
	}

	// Read existing file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse as yaml.Node to preserve structure and comments
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	// Find or create the migration section
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return fmt.Errorf("invalid YAML structure")
	}

	docContent := root.Content[0]
	if docContent.Kind != yaml.MappingNode {
		return fmt.Errorf("expected mapping at root")
	}

	// Find migration key
	var migrationNode *yaml.Node
	for i := 0; i < len(docContent.Content)-1; i += 2 {
		if docContent.Content[i].Value == "migration" {
			migrationNode = docContent.Content[i+1]
			break
		}
	}

	// Create migration section if it doesn't exist
	if migrationNode == nil {
		keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: "migration"}
		migrationNode = &yaml.Node{Kind: yaml.MappingNode}
		docContent.Content = append(docContent.Content, keyNode, migrationNode)
	}

	// Helper to set a value in the migration mapping
	setMigrationValue := func(key string, value interface{}) {
		// Look for existing key
		for i := 0; i < len(migrationNode.Content)-1; i += 2 {
			if migrationNode.Content[i].Value == key {
				// Update existing value
				migrationNode.Content[i+1].Value = fmt.Sprintf("%v", value)
				return
			}
		}
		// Add new key-value pair
		keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: key}
		valueNode := &yaml.Node{Kind: yaml.ScalarNode, Value: fmt.Sprintf("%v", value)}
		migrationNode.Content = append(migrationNode.Content, keyNode, valueNode)
	}

	// Apply recommended values
	setMigrationValue("chunk_size", rec.ChunkSize)
	setMigrationValue("workers", rec.Workers)
	setMigrationValue("read_ahead_buffers", rec.ReadAheadBuffers)

	if rec.ParallelReaders > 0 {
		setMigrationValue("parallel_readers", rec.ParallelReaders)
	}
	if rec.WriteAheadWriters > 0 {
		setMigrationValue("write_ahead_writers", rec.WriteAheadWriters)
	}
	if rec.PacketSize > 0 {
		setMigrationValue("mssql_packet_size", rec.PacketSize)
	}
	if rec.MaxPartitions > 0 {
		setMigrationValue("max_partitions", rec.MaxPartitions)
	}
	if rec.MaxSourceConnections > 0 {
		setMigrationValue("max_source_connections", rec.MaxSourceConnections)
	}
	if rec.MaxTargetConnections > 0 {
		setMigrationValue("max_target_connections", rec.MaxTargetConnections)
	}
	if rec.LargeTableThreshold > 0 {
		setMigrationValue("large_table_threshold", rec.LargeTableThreshold)
	}
	if rec.MSSQLRowsPerBatch > 0 {
		setMigrationValue("mssql_rows_per_batch", rec.MSSQLRowsPerBatch)
	}
	if rec.UpsertMergeChunkSize > 0 {
		setMigrationValue("upsert_merge_chunk_size", rec.UpsertMergeChunkSize)
	}

	// Marshal back to YAML
	var buf strings.Builder
	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(2)
	if err := encoder.Encode(&root); err != nil {
		return fmt.Errorf("failed to encode config: %w", err)
	}
	encoder.Close()

	// Write back to file
	if err := os.WriteFile(configPath, []byte(buf.String()), 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}
