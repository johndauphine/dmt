package calibration

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/shirou/gopsutil/v3/mem"
)

// AIAnalyzer uses AI to analyze calibration results and recommend configuration.
type AIAnalyzer struct {
	aiMapper *driver.AITypeMapper
}

// NewAIAnalyzer creates a new AI analyzer.
// Returns nil if aiMapper is nil (AI not configured).
func NewAIAnalyzer(aiMapper *driver.AITypeMapper) *AIAnalyzer {
	if aiMapper == nil {
		return nil
	}
	return &AIAnalyzer{aiMapper: aiMapper}
}

// AICalibrationInput represents the data sent to AI for analysis.
type AICalibrationInput struct {
	Runs    []AIRunMetrics `json:"runs"`
	System  AISystemInfo   `json:"system"`
	Drivers AIDriverInfo   `json:"drivers"`
}

// AIRunMetrics is the JSON representation of a calibration run for AI.
type AIRunMetrics struct {
	ConfigName       string  `json:"config_name"`
	ChunkSize        int     `json:"chunk_size"`
	Workers          int     `json:"workers"`
	ReadAheadBuffers int     `json:"read_ahead_buffers"`
	Status           string  `json:"status"`
	ErrorCategory    string  `json:"error_category,omitempty"`
	RowsPerSecond    float64 `json:"rows_per_sec,omitempty"`
	DurationSeconds  float64 `json:"duration_seconds,omitempty"`
	QueryTimePercent float64 `json:"query_pct,omitempty"`
	WriteTimePercent float64 `json:"write_pct,omitempty"`
	Error            string  `json:"error,omitempty"`
}

// AISystemInfo contains system information for AI context.
type AISystemInfo struct {
	CPUCores         int     `json:"cpu_cores"`
	MemoryGB         int     `json:"memory_gb"`
	SourceDBType     string  `json:"source_db_type"`
	TargetDBType     string  `json:"target_db_type"`
	SourceLatencyMs  float64 `json:"source_latency_ms"`
	TargetLatencyMs  float64 `json:"target_latency_ms"`
	TablesCount      int     `json:"tables_count"`
	TotalRowsSampled int64   `json:"total_rows_sampled"`
}

// AIDriverInfo contains driver-specific parameters for AI tuning decisions.
type AIDriverInfo struct {
	SourceDriver *AIDriverDefaults `json:"source_driver,omitempty"`
	TargetDriver *AIDriverDefaults `json:"target_driver,omitempty"`
}

// AIDriverDefaults contains all driver defaults for AI tuning analysis.
// AI uses these to make database-specific recommendations.
type AIDriverDefaults struct {
	Name                  string `json:"name"`
	Port                  int    `json:"port,omitempty"`
	Schema                string `json:"schema,omitempty"`
	SSLMode               string `json:"ssl_mode,omitempty"`               // PostgreSQL
	Encrypt               bool   `json:"encrypt,omitempty"`                // MSSQL
	PacketSize            int    `json:"packet_size,omitempty"`            // MSSQL TDS packet size (affects throughput)
	WriteAheadWriters     int    `json:"write_ahead_writers"`              // Parallel writers for bulk inserts
	ScaleWritersWithCores bool   `json:"scale_writers_with_cores"`         // Whether writers can scale with CPU cores
}

// AIRecommendation is the expected response from AI.
type AIRecommendation struct {
	RecommendedConfig struct {
		ChunkSize         int `json:"chunk_size"`
		Workers           int `json:"workers"`
		ReadAheadBuffers  int `json:"read_ahead_buffers"`
		ParallelReaders   int `json:"parallel_readers"`
		WriteAheadWriters int `json:"write_ahead_writers"`
		PacketSize        int `json:"packet_size,omitempty"` // MSSQL only
		// Extended parameters
		MaxPartitions        int   `json:"max_partitions,omitempty"`
		LargeTableThreshold  int64 `json:"large_table_threshold,omitempty"`
		SourceChunkSize      int   `json:"source_chunk_size,omitempty"`      // Batch size for reading
		TargetChunkSize      int   `json:"target_chunk_size,omitempty"`      // Batch size for writing
		UpsertMergeChunkSize int   `json:"upsert_merge_chunk_size,omitempty"`
		MaxSourceConnections int   `json:"max_source_connections,omitempty"` // Source connection pool
		MaxTargetConnections int   `json:"max_target_connections,omitempty"` // Target connection pool
	} `json:"recommended_config"`
	EstimatedRowsPerSec int64    `json:"estimated_rows_per_sec"`
	Confidence          string   `json:"confidence"`
	Reasoning           string   `json:"reasoning"`
	PatternsDetected    []string `json:"patterns_detected"`
	Warnings            []string `json:"warnings"`
}

// AnalyzeInput contains all inputs needed for AI analysis.
type AnalyzeInput struct {
	Result                *CalibrationResult
	SourceDBType          string
	TargetDBType          string
	SourceDriverDefaults  *driver.DriverDefaults
	TargetDriverDefaults  *driver.DriverDefaults
}

// Analyze sends calibration results to AI and returns recommendations.
func (a *AIAnalyzer) Analyze(ctx context.Context, input *AnalyzeInput) (*AIRecommendation, error) {
	if a == nil || a.aiMapper == nil {
		return nil, fmt.Errorf("AI analyzer not configured")
	}

	// Build AI input
	aiInput := a.buildInput(input)

	// Build prompt
	prompt := a.buildPrompt(aiInput)

	logging.Debug("Sending calibration results to AI for analysis...")

	// Call AI
	response, err := a.aiMapper.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("AI analysis failed: %w", err)
	}

	// Parse response
	rec, err := a.parseResponse(response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse AI response: %w", err)
	}

	return rec, nil
}

func (a *AIAnalyzer) buildInput(input *AnalyzeInput) *AICalibrationInput {
	result := input.Result
	aiInput := &AICalibrationInput{
		Runs: make([]AIRunMetrics, len(result.Runs)),
		System: AISystemInfo{
			CPUCores:         runtime.NumCPU(),
			SourceDBType:     input.SourceDBType,
			TargetDBType:     input.TargetDBType,
			SourceLatencyMs:  result.SourceLatencyMs,
			TargetLatencyMs:  result.TargetLatencyMs,
			TablesCount:      len(result.TablesUsed),
			TotalRowsSampled: result.TotalSampleRows,
		},
		Drivers: AIDriverInfo{},
	}

	// Get memory
	if v, err := mem.VirtualMemory(); err == nil {
		aiInput.System.MemoryGB = int(v.Available / 1024 / 1024 / 1024)
	}

	// Include all driver defaults for AI tuning decisions
	if input.SourceDriverDefaults != nil {
		aiInput.Drivers.SourceDriver = &AIDriverDefaults{
			Name:                  input.SourceDBType,
			Port:                  input.SourceDriverDefaults.Port,
			Schema:                input.SourceDriverDefaults.Schema,
			SSLMode:               input.SourceDriverDefaults.SSLMode,
			Encrypt:               input.SourceDriverDefaults.Encrypt,
			PacketSize:            input.SourceDriverDefaults.PacketSize,
			WriteAheadWriters:     input.SourceDriverDefaults.WriteAheadWriters,
			ScaleWritersWithCores: input.SourceDriverDefaults.ScaleWritersWithCores,
		}
	}
	if input.TargetDriverDefaults != nil {
		aiInput.Drivers.TargetDriver = &AIDriverDefaults{
			Name:                  input.TargetDBType,
			Port:                  input.TargetDriverDefaults.Port,
			Schema:                input.TargetDriverDefaults.Schema,
			SSLMode:               input.TargetDriverDefaults.SSLMode,
			Encrypt:               input.TargetDriverDefaults.Encrypt,
			PacketSize:            input.TargetDriverDefaults.PacketSize,
			WriteAheadWriters:     input.TargetDriverDefaults.WriteAheadWriters,
			ScaleWritersWithCores: input.TargetDriverDefaults.ScaleWritersWithCores,
		}
	}

	// Convert runs
	for i, run := range result.Runs {
		aiInput.Runs[i] = AIRunMetrics{
			ConfigName:       run.ConfigName,
			ChunkSize:        run.Config.ChunkSize,
			Workers:          run.Config.Workers,
			ReadAheadBuffers: run.Config.ReadAheadBuffers,
			Status:           string(run.Status),
			ErrorCategory:    string(run.ErrorCategory),
			RowsPerSecond:    run.RowsPerSecond,
			DurationSeconds:  run.Duration.Seconds(),
			QueryTimePercent: run.QueryTimePercent,
			WriteTimePercent: run.WriteTimePercent,
			Error:            run.Error,
		}
	}

	return aiInput
}

func (a *AIAnalyzer) buildPrompt(input *AICalibrationInput) string {
	inputJSON, _ := json.MarshalIndent(input, "", "  ")

	return fmt.Sprintf(`Analyze these calibration run results and recommend optimal configuration.

## Calibration Results
%s

Note: Failed runs indicate the configuration was too aggressive for this system.
Error categories: oom (out of memory), timeout (exceeded time limit), permission, deadlock, other.

## Driver-Specific Parameters
The "drivers" section contains database driver defaults. Use these for database-aware tuning:

### Performance Parameters
- write_ahead_writers: parallel writers for bulk inserts
  - If scale_writers_with_cores=true: can scale up to min(cores/4, workers/2)
  - If scale_writers_with_cores=false: use the default (more writers cause contention)
- packet_size (MSSQL only): TDS packet size in bytes
  - Default 32767 (32KB max) - larger packets improve throughput
  - Can be tuned down if network is unreliable or memory constrained
  - Values: 512 to 32767 (must be multiple of 512)

### Database Characteristics
- PostgreSQL: COPY command handles large batches and parallelism efficiently
- MSSQL: TABLOCK hint serializes bulk writes, limiting parallel writer benefit

## Guidelines
1. Identify highest throughput configuration that succeeded
2. If aggressive configs failed, recommend more conservative settings
3. High network latency (>10ms) may favor smaller chunk sizes
4. Look for patterns:
   - Throughput plateaus (diminishing returns at certain worker counts)
   - Memory pressure indicators (OOM errors)
   - Write bottlenecks (high write%% with lower throughput)
5. Consider the time breakdown:
   - High query%% suggests slow source reads
   - High write%% suggests target is the bottleneck
6. IMPORTANT: Use driver defaults to set write_ahead_writers and parallel_readers appropriately
7. ALWAYS explain WHY you chose specific values in the reasoning field - cite the data that led to each decision

## Extended Parameters (ALWAYS include these with sensible values)
- max_partitions: number of partitions for large tables (recommend: workers count)
- large_table_threshold: row count to trigger partitioning (recommend: 1-10 million based on chunk_size)
- source_chunk_size: batch size for reading from source (recommend: chunk_size)
- target_chunk_size: batch size for writing to target (recommend: chunk_size, or 5000-10000 for Oracle targets)
- upsert_merge_chunk_size: batch size for upsert UPDATE+INSERT (recommend: 5000-20000)
- max_source_connections: source connection pool size (recommend: workers * 2 + 4)
- max_target_connections: target connection pool size (recommend: workers * 2 + 4)

Return ONLY valid JSON (no markdown, no explanation outside JSON):
{
  "recommended_config": {
    "chunk_size": <int>,
    "workers": <int>,
    "read_ahead_buffers": <int>,
    "parallel_readers": <int>,
    "write_ahead_writers": <int>,
    "packet_size": <int or omit if not MSSQL>,
    "max_partitions": <int>,
    "large_table_threshold": <int>,
    "source_chunk_size": <int>,
    "target_chunk_size": <int>,
    "upsert_merge_chunk_size": <int>,
    "max_source_connections": <int>,
    "max_target_connections": <int>
  },
  "estimated_rows_per_sec": <int>,
  "confidence": "<high|medium|low>",
  "reasoning": "<2-3 sentences explaining WHY you chose these specific values based on the calibration data>",
  "patterns_detected": ["<pattern 1>", "<pattern 2>"],
  "warnings": ["<warning if any>"]
}`, string(inputJSON))
}

func (a *AIAnalyzer) parseResponse(response string) (*AIRecommendation, error) {
	// Clean up response - AI might include markdown code blocks
	response = strings.TrimSpace(response)
	response = strings.TrimPrefix(response, "```json")
	response = strings.TrimPrefix(response, "```")
	response = strings.TrimSuffix(response, "```")
	response = strings.TrimSpace(response)

	var rec AIRecommendation
	if err := json.Unmarshal([]byte(response), &rec); err != nil {
		return nil, fmt.Errorf("invalid JSON response: %w\nResponse was: %s", err, response)
	}

	// Validate
	if rec.RecommendedConfig.ChunkSize <= 0 {
		return nil, fmt.Errorf("invalid chunk_size in AI response: %d", rec.RecommendedConfig.ChunkSize)
	}
	if rec.RecommendedConfig.Workers <= 0 {
		return nil, fmt.Errorf("invalid workers in AI response: %d", rec.RecommendedConfig.Workers)
	}
	if rec.RecommendedConfig.ReadAheadBuffers <= 0 {
		return nil, fmt.Errorf("invalid read_ahead_buffers in AI response: %d", rec.RecommendedConfig.ReadAheadBuffers)
	}

	// Default confidence if not provided
	if rec.Confidence == "" {
		rec.Confidence = "medium"
	}

	return &rec, nil
}

// FallbackRecommendation creates a recommendation based on the best observed run.
// Note: parallel_readers and write_ahead_writers are not included since they
// require driver-specific knowledge. The caller should apply driver defaults.
func FallbackRecommendation(result *CalibrationResult) *RecommendedConfig {
	best := result.BestSuccessfulRun()
	if best == nil {
		return nil
	}

	return &RecommendedConfig{
		ChunkSize:           best.Config.ChunkSize,
		Workers:             best.Config.Workers,
		ReadAheadBuffers:    best.Config.ReadAheadBuffers,
		ParallelReaders:     best.Config.ParallelReaders,
		EstimatedRowsPerSec: int64(best.RowsPerSecond),
		Confidence:          "low", // Lower confidence since AI couldn't analyze
	}
}
