package calibration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

// AIGuidedCalibrator uses AI to guide the entire calibration process.
type AIGuidedCalibrator struct {
	aiMapper      *driver.AITypeMapper
	systemContext *SystemContext
	runHistory    []AIRunResult
}

// AIRunResult captures a calibration run result for AI analysis.
type AIRunResult struct {
	Config  CalibrationConfig `json:"config"`
	Metrics RunMetrics        `json:"metrics"`
}

// AIAnalysisRequest is sent to AI for analysis.
type AIAnalysisRequest struct {
	Phase         string          `json:"phase"` // "initial", "mid_calibration", "final", "runtime"
	SystemContext *SystemContext  `json:"system_context"`
	RunHistory    []AIRunResult   `json:"run_history,omitempty"`
	CurrentMetrics *RuntimeMetrics `json:"current_metrics,omitempty"`
	Question      string          `json:"question"`
}

// RuntimeMetrics captures current migration performance.
type RuntimeMetrics struct {
	ElapsedSeconds    float64 `json:"elapsed_seconds"`
	RowsTransferred   int64   `json:"rows_transferred"`
	CurrentRowsPerSec float64 `json:"current_rows_per_sec"`
	MemoryUsedMB      int64   `json:"memory_used_mb"`
	MemoryPercent     float64 `json:"memory_percent"`
	QueryTimePercent  float64 `json:"query_time_percent"`
	WriteTimePercent  float64 `json:"write_time_percent"`
	ErrorCount        int     `json:"error_count"`
	RetryCount        int     `json:"retry_count"`
}

// AIConfigSuggestion is the AI's recommended configuration.
type AIConfigSuggestion struct {
	Config      CalibrationConfig `json:"config"`
	Reasoning   string            `json:"reasoning"`
	Confidence  string            `json:"confidence"`
	Warnings    []string          `json:"warnings,omitempty"`
	ShouldStop  bool              `json:"should_stop,omitempty"`  // For adaptive: stop early if confident
	NextAction  string            `json:"next_action,omitempty"` // For runtime: "continue", "adjust", "pause"
}

// NewAIGuidedCalibrator creates a new AI-guided calibrator.
func NewAIGuidedCalibrator(aiMapper *driver.AITypeMapper, sysCtx *SystemContext) *AIGuidedCalibrator {
	return &AIGuidedCalibrator{
		aiMapper:      aiMapper,
		systemContext: sysCtx,
		runHistory:    make([]AIRunResult, 0),
	}
}

// GetInitialConfig asks AI to suggest initial calibration parameters.
func (g *AIGuidedCalibrator) GetInitialConfig(ctx context.Context) (*AIConfigSuggestion, error) {
	if g.aiMapper == nil {
		return nil, fmt.Errorf("AI not configured")
	}

	prompt := g.buildInitialPrompt()
	logging.Debug("AI Initial Analysis Prompt:\n%s", prompt)

	response, err := g.aiMapper.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("AI analysis failed: %w", err)
	}

	return g.parseConfigSuggestion(response)
}

// GetNextConfig asks AI what parameters to test next based on run history.
func (g *AIGuidedCalibrator) GetNextConfig(ctx context.Context, lastRun *RunMetrics) (*AIConfigSuggestion, error) {
	if g.aiMapper == nil {
		return nil, fmt.Errorf("AI not configured")
	}

	// Add to history
	if lastRun != nil {
		g.runHistory = append(g.runHistory, AIRunResult{
			Config:  lastRun.Config,
			Metrics: *lastRun,
		})
	}

	prompt := g.buildAdaptivePrompt()
	logging.Debug("AI Adaptive Analysis Prompt:\n%s", prompt)

	response, err := g.aiMapper.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("AI analysis failed: %w", err)
	}

	return g.parseConfigSuggestion(response)
}

// GetFinalRecommendation asks AI for final recommendation after all runs.
func (g *AIGuidedCalibrator) GetFinalRecommendation(ctx context.Context) (*AIRecommendation, error) {
	if g.aiMapper == nil {
		return nil, fmt.Errorf("AI not configured")
	}

	prompt := g.buildFinalPrompt()
	logging.Debug("AI Final Analysis Prompt:\n%s", prompt)

	response, err := g.aiMapper.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("AI analysis failed: %w", err)
	}

	return parseAIRecommendation(response)
}

// GetRuntimeAdvice asks AI for advice during actual migration.
func (g *AIGuidedCalibrator) GetRuntimeAdvice(ctx context.Context, metrics *RuntimeMetrics) (*AIConfigSuggestion, error) {
	if g.aiMapper == nil {
		return nil, fmt.Errorf("AI not configured")
	}

	prompt := g.buildRuntimePrompt(metrics)

	response, err := g.aiMapper.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("AI analysis failed: %w", err)
	}

	return g.parseConfigSuggestion(response)
}

func (g *AIGuidedCalibrator) buildInitialPrompt() string {
	sysCtxJSON, _ := json.MarshalIndent(g.systemContext, "", "  ")

	return fmt.Sprintf(`You are an expert database migration performance tuner. Analyze this system and suggest initial calibration parameters.

## System Context
%s

## Task
Based on the system resources, database configurations, and network characteristics, suggest optimal STARTING parameters for calibration testing.

Consider:
1. **Memory constraints**: Available memory limits chunk_size and read_ahead_buffers
2. **CPU cores**: Workers should scale with cores but not exceed practical limits
3. **Network latency**: Higher latency may favor larger chunks to amortize overhead
4. **Database capabilities**: Source and target server configurations
5. **Data characteristics**: Table count, total rows, average row width

## Guidelines
- Start conservative if uncertain - we can be more aggressive later
- For high-latency networks (>10ms), prefer larger chunk sizes
- For memory-constrained hosts (<8GB available), limit workers and buffers
- Consider database max connections when setting workers

Return ONLY valid JSON:
{
  "config": {
    "name": "ai_initial",
    "chunk_size": <int>,
    "workers": <int>,
    "read_ahead_buffers": <int>,
    "parallel_readers": <int>
  },
  "reasoning": "<2-3 sentences explaining WHY you chose these specific values>",
  "confidence": "high|medium|low",
  "warnings": ["<any concerns about this system>"]
}`, string(sysCtxJSON))
}

func (g *AIGuidedCalibrator) buildAdaptivePrompt() string {
	sysCtxJSON, _ := json.MarshalIndent(g.systemContext, "", "  ")
	historyJSON, _ := json.MarshalIndent(g.runHistory, "", "  ")

	return fmt.Sprintf(`You are an expert database migration performance tuner. Analyze calibration results and decide what to test next.

## System Context
%s

## Calibration History (most recent last)
%s

## Task
Based on the run history, decide:
1. What parameters to test next to find optimal configuration
2. OR if we have enough data to make a confident recommendation (set should_stop=true)

## Strategy Guidelines
- If throughput is still increasing, try more aggressive settings
- If a run failed (OOM/timeout), back off that parameter
- If throughput plateaued across 2+ configs, we may have found the limit
- Look for patterns: write-bound? read-bound? memory-bound?
- Test one dimension at a time when possible (chunk OR workers, not both)
- Maximum 7 total runs - stop earlier if confident

Return ONLY valid JSON:
{
  "config": {
    "name": "ai_adaptive_N",
    "chunk_size": <int>,
    "workers": <int>,
    "read_ahead_buffers": <int>,
    "parallel_readers": <int>
  },
  "reasoning": "<explain what you learned and why you're testing these values>",
  "confidence": "high|medium|low",
  "should_stop": <true if confident we've found optimal, false to continue>,
  "warnings": ["<any concerns>"]
}`, string(sysCtxJSON), string(historyJSON))
}

func (g *AIGuidedCalibrator) buildFinalPrompt() string {
	sysCtxJSON, _ := json.MarshalIndent(g.systemContext, "", "  ")
	historyJSON, _ := json.MarshalIndent(g.runHistory, "", "  ")

	return fmt.Sprintf(`You are an expert database migration performance tuner. Provide final configuration recommendation.

## System Context
%s

## All Calibration Runs
%s

## Task
Analyze ALL runs and provide the FINAL recommended configuration for production migration.

Consider:
1. Which configuration achieved the best stable throughput?
2. Are there any reliability concerns (near OOM, high variance)?
3. What's the optimal balance of performance vs stability?
4. Any settings that should be adjusted for full migration (vs small sample)?

## Database-Specific Tuning
- For MSSQL source: packet_size (512-32767) affects network efficiency
- For PostgreSQL target: write_ahead_writers limited by COPY protocol
- parallel_readers: scale with workers but watch connection limits

## Extended Parameters (ALWAYS include these with sensible values)
- max_partitions: partitions for large tables (recommend: workers count)
- large_table_threshold: row count to trigger partitioning (recommend: 1-10 million)
- source_chunk_size: batch size for reading from source (recommend: chunk_size)
- target_chunk_size: batch size for writing to target (recommend: chunk_size, or 5000-10000 for Oracle targets)
- upsert_merge_chunk_size: batch size for upsert operations (recommend: 5000-20000)
- max_source_connections: source pool size (recommend: workers * 2 + 4)
- max_target_connections: target pool size (recommend: workers * 2 + 4)

Return ONLY valid JSON:
{
  "recommended_config": {
    "chunk_size": <int>,
    "workers": <int>,
    "read_ahead_buffers": <int>,
    "parallel_readers": <int>,
    "write_ahead_writers": <int>,
    "packet_size": <int, MSSQL only, omit if not applicable>,
    "max_partitions": <int>,
    "large_table_threshold": <int>,
    "source_chunk_size": <int>,
    "target_chunk_size": <int>,
    "upsert_merge_chunk_size": <int>,
    "max_source_connections": <int>,
    "max_target_connections": <int>
  },
  "estimated_rows_per_sec": <int>,
  "confidence": "high|medium|low",
  "reasoning": "<comprehensive explanation of your recommendation>",
  "patterns_detected": ["<pattern 1>", "<pattern 2>"],
  "warnings": ["<any production concerns>"]
}`, string(sysCtxJSON), string(historyJSON))
}

func (g *AIGuidedCalibrator) buildRuntimePrompt(metrics *RuntimeMetrics) string {
	sysCtxJSON, _ := json.MarshalIndent(g.systemContext, "", "  ")
	metricsJSON, _ := json.MarshalIndent(metrics, "", "  ")

	return fmt.Sprintf(`You are monitoring a live database migration. Analyze current performance and advise.

## System Context
%s

## Current Migration Metrics
%s

## Task
Analyze the current migration performance and provide advice:
1. Is performance within expected range?
2. Are there any concerning patterns (memory growth, error rate)?
3. Should any parameters be adjusted mid-migration?

Return ONLY valid JSON:
{
  "next_action": "continue|adjust|pause",
  "reasoning": "<brief explanation>",
  "config": {
    "chunk_size": <int, only if adjusting>,
    "workers": <int, only if adjusting>
  },
  "warnings": ["<any immediate concerns>"]
}`, string(sysCtxJSON), string(metricsJSON))
}

func (g *AIGuidedCalibrator) parseConfigSuggestion(response string) (*AIConfigSuggestion, error) {
	response = cleanJSONResponse(response)

	var suggestion AIConfigSuggestion
	if err := json.Unmarshal([]byte(response), &suggestion); err != nil {
		return nil, fmt.Errorf("invalid JSON response: %w\nResponse: %s", err, response)
	}

	// Validate
	if suggestion.Config.ChunkSize <= 0 {
		return nil, fmt.Errorf("invalid chunk_size: %d", suggestion.Config.ChunkSize)
	}
	if suggestion.Config.Workers <= 0 {
		return nil, fmt.Errorf("invalid workers: %d", suggestion.Config.Workers)
	}

	return &suggestion, nil
}

func parseAIRecommendation(response string) (*AIRecommendation, error) {
	response = cleanJSONResponse(response)

	var rec AIRecommendation
	if err := json.Unmarshal([]byte(response), &rec); err != nil {
		return nil, fmt.Errorf("invalid JSON response: %w\nResponse: %s", err, response)
	}

	// Validate
	if rec.RecommendedConfig.ChunkSize <= 0 {
		return nil, fmt.Errorf("invalid chunk_size: %d", rec.RecommendedConfig.ChunkSize)
	}
	if rec.RecommendedConfig.Workers <= 0 {
		return nil, fmt.Errorf("invalid workers: %d", rec.RecommendedConfig.Workers)
	}

	if rec.Confidence == "" {
		rec.Confidence = "medium"
	}

	return &rec, nil
}

func cleanJSONResponse(response string) string {
	response = strings.TrimSpace(response)
	response = strings.TrimPrefix(response, "```json")
	response = strings.TrimPrefix(response, "```")
	response = strings.TrimSuffix(response, "```")
	return strings.TrimSpace(response)
}
