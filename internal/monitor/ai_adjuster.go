package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pipeline"
)

// AdjustmentDecision represents AI's recommendation for parameter adjustments.
type AdjustmentDecision struct {
	Action      string         `json:"action"` // "continue", "scale_up", "scale_down", "reduce_chunk", etc.
	Adjustments map[string]int `json:"adjustments"`
	Reasoning   string         `json:"reasoning"`
	Warnings    []string       `json:"warnings,omitempty"`
	Confidence  string         `json:"confidence"`
}

// AIAdjuster uses AI to analyze metrics and recommend parameter adjustments.
type AIAdjuster struct {
	aiMapper       *driver.AITypeMapper
	collector      *MetricsCollector
	pipeline       *pipeline.Pipeline
	lastAdjustment time.Time
	adjustmentsMu  sync.Mutex

	// Cost control
	callInterval   time.Duration
	lastAICall     time.Time
	cacheDuration  time.Duration
	cachedDecision *AdjustmentDecision

	// Circuit breaker
	failureCount     int
	failureThreshold int
	resetTimeout     time.Duration
	circuitOpen      bool
}

// NewAIAdjuster creates a new AI adjuster.
func NewAIAdjuster(aiMapper *driver.AITypeMapper, collector *MetricsCollector, p *pipeline.Pipeline) *AIAdjuster {
	return &AIAdjuster{
		aiMapper:         aiMapper,
		collector:        collector,
		pipeline:         p,
		callInterval:     30 * time.Second, // Throttle to 30s intervals
		cacheDuration:    60 * time.Second, // Cache decisions for 60s
		failureThreshold: 3,
		resetTimeout:     5 * time.Minute,
		circuitOpen:      false,
	}
}

// Evaluate analyzes current metrics and returns an adjustment recommendation.
func (aa *AIAdjuster) Evaluate(ctx context.Context) (*AdjustmentDecision, error) {
	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()

	// Check circuit breaker
	if aa.circuitOpen {
		return nil, fmt.Errorf("circuit breaker OPEN - AI adjustments temporarily disabled")
	}

	// Check cache
	if aa.cachedDecision != nil && time.Since(aa.lastAICall) < aa.cacheDuration {
		logging.Debug("Using cached AI decision (age %.0fs)", time.Since(aa.lastAICall).Seconds())
		return aa.cachedDecision, nil
	}

	// Throttle AI calls
	if time.Since(aa.lastAICall) < aa.callInterval {
		return nil, fmt.Errorf("throttled - next AI call available in %.0fs", (aa.callInterval - time.Since(aa.lastAICall)).Seconds())
	}

	// Build prompt
	prompt := aa.buildAdjustmentPrompt()

	// Call AI
	response, err := aa.aiMapper.CallAI(ctx, prompt)
	if err != nil {
		aa.recordFailure()
		logging.Warn("AI adjustment failed: %v, using fallback rules", err)
		return aa.fallbackRules(), nil
	}

	// Parse response
	decision, err := aa.parseDecision(response)
	if err != nil {
		aa.recordFailure()
		logging.Warn("Failed to parse AI response: %v, using fallback rules", err)
		return aa.fallbackRules(), nil
	}

	// Success - reset circuit breaker
	aa.failureCount = 0

	// Cache decision
	aa.cachedDecision = decision
	aa.lastAICall = time.Now()

	return decision, nil
}

// buildAdjustmentPrompt constructs the prompt for AI analysis.
func (aa *AIAdjuster) buildAdjustmentPrompt() string {
	metrics := aa.collector.GetRecentMetrics(3) // Last 3 samples (90 seconds)
	trends := aa.collector.AnalyzeTrends()
	config := aa.pipeline.GetConfig()

	// Build metrics summary
	var metricsStr string
	if len(metrics) > 0 {
		latest := metrics[len(metrics)-1]
		metricsStr = fmt.Sprintf(`
Current Performance (latest):
- Rows: %d (%.0f rows/sec)
- Throughput trend: %.1f%%
- Memory: %dMB (%.1f%%)
- CPU: %.1f%%
- Elapsed: %.0f seconds
`, latest.RowsProcessed, latest.Throughput, latest.ThroughputTrend, latest.MemoryUsedMB, latest.MemoryPercent, latest.CPUPercent, latest.ElapsedSeconds)
	}

	prompt := fmt.Sprintf(`Real-time database migration parameter adjustment.

## Current Status
%s

## Current Configuration
- chunk_size: %d (10K-200K)
- workers: %d (1-16)
- parallel_readers: %d (1-4)
- read_ahead_buffers: %d (4-32)

## Performance Trends
- Throughput decreasing: %v
- Memory increasing: %v
- CPU saturated (>90%%): %v
- Memory saturated (>85%%): %v

## Guidelines for Adjustment
1. **If performance stable** → action: "continue" (no changes)
2. **If throughput declining + resources available** → "scale_up" (more workers or larger chunks)
3. **If memory >85%%** → "reduce_chunk" (smaller chunks)
4. **If CPU >90%% + latency high** → "scale_down" (fewer workers)

Important: Only recommend changes that make sense. If performance is good, continue.

Return ONLY valid JSON:
{
  "action": "continue|scale_up|scale_down|reduce_chunk|optimize",
  "adjustments": {
    "chunk_size": <new value or omit>,
    "workers": <new value or omit>
  },
  "reasoning": "<2-3 sentences explaining why>",
  "warnings": ["<warning if any>"],
  "confidence": "high|medium|low"
}`, metricsStr, config.ChunkSize, config.WriteAheadWriters, config.ParallelReaders, config.ReadAheadBuffers,
		trends.ThroughputDecreasing, trends.MemoryIncreasing, trends.CPUSaturated, trends.MemorySaturated)

	return prompt
}

// parseDecision parses the AI response into an AdjustmentDecision.
func (aa *AIAdjuster) parseDecision(response string) (*AdjustmentDecision, error) {
	// Clean up response
	response = strings.TrimSpace(response)
	response = strings.TrimPrefix(response, "```json")
	response = strings.TrimPrefix(response, "```")
	response = strings.TrimSuffix(response, "```")
	response = strings.TrimSpace(response)

	var decision AdjustmentDecision
	if err := json.Unmarshal([]byte(response), &decision); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w\nResponse: %s", err, response)
	}

	// Validate
	if decision.Action == "" {
		return nil, fmt.Errorf("missing action in response")
	}

	if decision.Reasoning == "" {
		decision.Reasoning = "(no reasoning provided)"
	}

	if decision.Confidence == "" {
		decision.Confidence = "medium"
	}

	return &decision, nil
}

// ApplyDecision applies an adjustment decision to the pipeline.
func (aa *AIAdjuster) ApplyDecision(decision *AdjustmentDecision) error {
	if decision.Action == "continue" {
		return nil // No changes
	}

	if len(decision.Adjustments) == 0 {
		return nil // No adjustments to apply
	}

	// Validate adjustments
	for param, value := range decision.Adjustments {
		if err := aa.validateParameter(param, value); err != nil {
			return fmt.Errorf("invalid adjustment for %s: %w", param, err)
		}
	}

	// Build config update
	update := pipeline.ConfigUpdate{
		ApplyAt: pipeline.ApplyNextChunk,
	}

	for param, value := range decision.Adjustments {
		switch param {
		case "chunk_size":
			update.ChunkSize = &value
		case "workers":
			update.WriteAheadWriters = &value
		}
	}

	// Apply to pipeline
	if err := aa.pipeline.UpdateConfig(update); err != nil {
		return fmt.Errorf("failed to apply config update: %w", err)
	}

	logging.Info("AI adjustment applied: %s - %s (confidence: %s)",
		decision.Action, decision.Reasoning, decision.Confidence)

	aa.lastAdjustment = time.Now()
	return nil
}

// fallbackRules returns a decision based on simple heuristic rules.
func (aa *AIAdjuster) fallbackRules() *AdjustmentDecision {
	trends := aa.collector.AnalyzeTrends()
	if trends.Insufficient {
		return &AdjustmentDecision{
			Action:      "continue",
			Reasoning:   "Insufficient data for analysis",
			Confidence:  "low",
			Adjustments: make(map[string]int),
		}
	}

	metrics := aa.collector.GetRecentMetrics(1)
	if len(metrics) == 0 {
		return &AdjustmentDecision{
			Action:      "continue",
			Reasoning:   "No metrics available",
			Confidence:  "low",
			Adjustments: make(map[string]int),
		}
	}

	latest := metrics[0]
	config := aa.pipeline.GetConfig()

	// Rule 1: Memory saturation
	if trends.MemorySaturated {
		newChunkSize := config.ChunkSize / 2
		if newChunkSize < 5000 {
			newChunkSize = 5000
		}
		return &AdjustmentDecision{
			Action:      "reduce_chunk",
			Reasoning:   "Memory saturated - reducing chunk size to free memory",
			Confidence:  "high",
			Adjustments: map[string]int{"chunk_size": newChunkSize},
		}
	}

	// Rule 2: CPU saturation with latency increase
	if trends.CPUSaturated && latest.CPUPercent > 85 {
		newWorkers := config.WriteAheadWriters - 1
		if newWorkers < 1 {
			newWorkers = 1
		}
		return &AdjustmentDecision{
			Action:      "scale_down",
			Reasoning:   "CPU saturated - reducing workers to decrease contention",
			Confidence:  "medium",
			Adjustments: map[string]int{"workers": newWorkers},
		}
	}

	// Rule 3: Throughput declining with available resources
	if trends.ThroughputDecreasing && !trends.CPUSaturated && !trends.MemorySaturated {
		newWorkers := config.WriteAheadWriters + 1
		if newWorkers > 16 {
			newWorkers = 16
		}
		return &AdjustmentDecision{
			Action:      "scale_up",
			Reasoning:   "Throughput declining and resources available - increasing workers",
			Confidence:  "medium",
			Adjustments: map[string]int{"workers": newWorkers},
		}
	}

	// Default: continue
	return &AdjustmentDecision{
		Action:      "continue",
		Reasoning:   "Performance stable (fallback rules)",
		Confidence:  "low",
		Adjustments: make(map[string]int),
	}
}

// validateParameter checks if a parameter value is within valid ranges.
func (aa *AIAdjuster) validateParameter(param string, value int) error {
	switch param {
	case "chunk_size":
		if value < 1000 || value > 500000 {
			return fmt.Errorf("chunk_size %d out of range (1K-500K)", value)
		}
	case "workers":
		if value < 1 || value > 16 {
			return fmt.Errorf("workers %d out of range (1-16)", value)
		}
	default:
		return fmt.Errorf("unknown parameter: %s", param)
	}
	return nil
}

// recordFailure tracks AI call failures for circuit breaker logic.
func (aa *AIAdjuster) recordFailure() {
	aa.failureCount++
	if aa.failureCount >= aa.failureThreshold {
		aa.circuitOpen = true
		logging.Warn("AI adjustment circuit breaker OPEN after %d failures - will retry in %v",
			aa.failureCount, aa.resetTimeout)

		// Schedule reset
		go func() {
			time.Sleep(aa.resetTimeout)
			aa.adjustmentsMu.Lock()
			defer aa.adjustmentsMu.Unlock()
			aa.circuitOpen = false
			aa.failureCount = 0
			aa.cachedDecision = nil
			logging.Info("AI adjustment circuit breaker CLOSED - resuming")
		}()
	}
}
