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
	startTime      time.Time
	lastAdjustment time.Time
	adjustmentsMu  sync.Mutex

	// Smarter adjustment control
	lastActionType      string    // Track last action to prevent repeated same-action
	lastActionTime      time.Time // When last non-continue action was applied
	sameActionCooldown  time.Duration
	warmupPeriod        time.Duration
	adjustmentCount     int // Total non-continue adjustments made

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
		aiMapper:           aiMapper,
		collector:          collector,
		pipeline:           p,
		startTime:          time.Now(),
		warmupPeriod:       2 * time.Minute,  // Wait 2 min before any adjustments (let auto-tune stabilize)
		sameActionCooldown: 3 * time.Minute,  // Wait 3 min between same-type adjustments
		callInterval:       30 * time.Second, // Throttle to 30s intervals
		cacheDuration:      60 * time.Second, // Cache decisions for 60s
		failureThreshold:   3,
		resetTimeout:       5 * time.Minute,
		circuitOpen:        false,
	}
}

// Evaluate analyzes current metrics and returns an adjustment recommendation.
func (aa *AIAdjuster) Evaluate(ctx context.Context) (*AdjustmentDecision, error) {
	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()

	// Check warmup period - let auto-tuned defaults stabilize before adjusting
	if time.Since(aa.startTime) < aa.warmupPeriod {
		remaining := aa.warmupPeriod - time.Since(aa.startTime)
		logging.Debug("AI adjustment warmup: %.0fs remaining before first adjustment", remaining.Seconds())
		return &AdjustmentDecision{
			Action:      "continue",
			Reasoning:   fmt.Sprintf("Warmup period - waiting %.0fs for auto-tuned defaults to stabilize", remaining.Seconds()),
			Confidence:  "high",
			Adjustments: make(map[string]int),
		}, nil
	}

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

	// Check same-action cooldown to prevent repeated adjustments
	if decision.Action == aa.lastActionType && time.Since(aa.lastActionTime) < aa.sameActionCooldown {
		remaining := aa.sameActionCooldown - time.Since(aa.lastActionTime)
		logging.Debug("AI adjustment cooldown: skipping %s (same action applied %.0fs ago, wait %.0fs)",
			decision.Action, time.Since(aa.lastActionTime).Seconds(), remaining.Seconds())
		return nil
	}

	// Check if we're already at limits for this action type
	config := aa.pipeline.GetConfig()
	if decision.Action == "scale_up" {
		// Don't scale up if already at max workers
		if config.WriteAheadWriters >= 16 {
			logging.Debug("AI adjustment skipped: already at max workers (%d)", config.WriteAheadWriters)
			return nil
		}
	}
	if decision.Action == "scale_down" {
		// Don't scale down if already at min workers
		if config.WriteAheadWriters <= 1 {
			logging.Debug("AI adjustment skipped: already at min workers (%d)", config.WriteAheadWriters)
			return nil
		}
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
		case "parallel_readers":
			update.ParallelReaders = &value
		case "read_ahead_buffers":
			update.ReadAheadBuffers = &value
		default:
			logging.Info("AIAdjuster: unknown adjustment parameter %q (value=%d); ignoring", param, value)
		}
	}

	// Apply to pipeline
	if err := aa.pipeline.UpdateConfig(update); err != nil {
		return fmt.Errorf("failed to apply config update: %w", err)
	}

	// Track this adjustment
	aa.lastActionType = decision.Action
	aa.lastActionTime = time.Now()
	aa.adjustmentCount++

	logging.Info("AI adjustment #%d applied: %s - %s (confidence: %s)",
		aa.adjustmentCount, decision.Action, decision.Reasoning, decision.Confidence)

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

	// Rule 1: Memory saturation (high priority)
	if trends.MemorySaturated {
		newChunkSize := config.ChunkSize / 2
		if newChunkSize < 5000 {
			newChunkSize = 5000
		}
		if newChunkSize == config.ChunkSize {
			// Already at minimum, nothing to do
			return &AdjustmentDecision{
				Action:      "continue",
				Reasoning:   "Memory saturated but already at minimum chunk size",
				Confidence:  "medium",
				Adjustments: make(map[string]int),
			}
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
		if config.WriteAheadWriters <= 1 {
			// Already at minimum, nothing to do
			return &AdjustmentDecision{
				Action:      "continue",
				Reasoning:   "CPU saturated but already at minimum workers",
				Confidence:  "medium",
				Adjustments: make(map[string]int),
			}
		}
		newWorkers := config.WriteAheadWriters - 1
		return &AdjustmentDecision{
			Action:      "scale_down",
			Reasoning:   "CPU saturated - reducing workers to decrease contention",
			Confidence:  "medium",
			Adjustments: map[string]int{"workers": newWorkers},
		}
	}

	// Rule 3: Throughput declining significantly (>20%) with available resources
	// Only suggest scale_up if not already at max and decline is significant
	if trends.ThroughputDecreasing && !trends.CPUSaturated && !trends.MemorySaturated {
		if config.WriteAheadWriters >= 16 {
			// Already at maximum, nothing to do
			return &AdjustmentDecision{
				Action:      "continue",
				Reasoning:   fmt.Sprintf("Throughput declining %.0f%% but already at max workers", trends.ThroughputDecline),
				Confidence:  "medium",
				Adjustments: make(map[string]int),
			}
		}
		newWorkers := config.WriteAheadWriters + 1
		return &AdjustmentDecision{
			Action:      "scale_up",
			Reasoning:   fmt.Sprintf("Throughput declining %.0f%% with resources available - increasing workers", trends.ThroughputDecline),
			Confidence:  "medium",
			Adjustments: map[string]int{"workers": newWorkers},
		}
	}

	// Default: continue - performance is stable
	return &AdjustmentDecision{
		Action:      "continue",
		Reasoning:   fmt.Sprintf("Performance stable (throughput trend: %.1f%%)", trends.ThroughputDecline),
		Confidence:  "high",
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
	case "parallel_readers":
		if value < 1 || value > 4 {
			return fmt.Errorf("parallel_readers %d out of range (1-4)", value)
		}
	case "read_ahead_buffers":
		if value < 4 || value > 32 {
			return fmt.Errorf("read_ahead_buffers %d out of range (4-32)", value)
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
