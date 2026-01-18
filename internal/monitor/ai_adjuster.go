package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pipeline"
	"github.com/shirou/gopsutil/v3/mem"
)

const (
	// effectivenessThreshold defines the minimum percentage improvement
	// for an adjustment to be considered effective
	effectivenessThreshold = 5.0
)

// AdjustmentDecision represents AI's recommendation for parameter adjustments.
type AdjustmentDecision struct {
	Action      string         `json:"action"` // "continue", "scale_up", "scale_down", "reduce_chunk", etc.
	Adjustments map[string]int `json:"adjustments"`
	Reasoning   string         `json:"reasoning"`
	Warnings    []string       `json:"warnings,omitempty"`
	Confidence  string         `json:"confidence"`
}

// AdjustmentRecord tracks a past adjustment and its effect.
type AdjustmentRecord struct {
	AdjustmentNumber int
	Timestamp        time.Time
	Action           string
	Adjustments      map[string]int
	ThroughputBefore float64
	ThroughputAfter  float64 // Measured 30s after adjustment
	EffectPercent    float64 // Positive = improvement
}

// SystemResources captures the execution environment for AI context.
type SystemResources struct {
	CPUCores             int
	MemoryTotalMB        int64
	MemoryAvailableMB    int64
	MaxSourceConnections int
	MaxTargetConnections int
}

// AIAdjuster uses AI to analyze metrics and recommend parameter adjustments.
type AIAdjuster struct {
	aiMapper       *driver.AITypeMapper
	collector      *MetricsCollector
	pipeline       *pipeline.Pipeline
	startTime      time.Time
	lastAdjustment time.Time
	adjustmentsMu  sync.Mutex

	// System resources for AI context
	systemResources SystemResources

	// Baseline metrics
	baselineMetrics   *PerformanceSnapshot
	baselineCaptured  bool

	// Adjustment history for AI learning
	adjustmentHistory []AdjustmentRecord
	maxHistorySize    int

	// Adjustment tracking
	lastActionType  string    // Track last action for history
	lastActionTime  time.Time // When last non-continue action was applied
	adjustmentCount int       // Total non-continue adjustments made

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

	// Persistent history
	state checkpoint.StateBackend
	runID string
}

// NewAIAdjuster creates a new AI adjuster.
func NewAIAdjuster(aiMapper *driver.AITypeMapper, collector *MetricsCollector, p *pipeline.Pipeline) *AIAdjuster {
	aa := &AIAdjuster{
		aiMapper:          aiMapper,
		collector:         collector,
		pipeline:          p,
		startTime:         time.Now(),
		callInterval:      30 * time.Second, // Throttle to 30s intervals
		cacheDuration:     60 * time.Second, // Cache decisions for 60s
		failureThreshold:  3,
		resetTimeout:      5 * time.Minute,
		circuitOpen:       false,
		maxHistorySize:    10, // Keep last 10 adjustments
		adjustmentHistory: make([]AdjustmentRecord, 0, 10),
	}

	// Gather system resources
	aa.gatherSystemResources()

	return aa
}

// SetConnectionLimits sets the max connection limits for source and target.
func (aa *AIAdjuster) SetConnectionLimits(maxSource, maxTarget int) {
	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()
	aa.systemResources.MaxSourceConnections = maxSource
	aa.systemResources.MaxTargetConnections = maxTarget
}

// SetStateBackend sets the state backend for persistent history.
func (aa *AIAdjuster) SetStateBackend(state checkpoint.StateBackend, runID string) {
	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()
	aa.state = state
	aa.runID = runID

	// Load historical patterns from past migrations
	if state != nil {
		if history, err := state.GetAIAdjustments(50); err == nil && len(history) > 0 {
			logging.Debug("AI adjuster loaded %d historical adjustments from past migrations", len(history))
		}
	}
}

// gatherSystemResources collects system information for AI context.
func (aa *AIAdjuster) gatherSystemResources() {
	aa.systemResources.CPUCores = runtime.NumCPU()

	if memInfo, err := mem.VirtualMemory(); err == nil {
		aa.systemResources.MemoryTotalMB = int64(memInfo.Total / 1024 / 1024)
		aa.systemResources.MemoryAvailableMB = int64(memInfo.Available / 1024 / 1024)
	}
}

// captureBaseline captures baseline metrics when sufficient data is available.
func (aa *AIAdjuster) captureBaseline() {
	if aa.baselineCaptured {
		return
	}

	metrics := aa.collector.GetRecentMetrics(3)
	if len(metrics) < 3 {
		return
	}

	// Average the metrics for a stable baseline
	var avgThroughput float64
	var avgCPU float64
	var avgMemory float64
	for _, m := range metrics {
		avgThroughput += m.Throughput
		avgCPU += m.CPUPercent
		avgMemory += m.MemoryPercent
	}
	count := float64(len(metrics))

	aa.baselineMetrics = &PerformanceSnapshot{
		Throughput:    avgThroughput / count,
		CPUPercent:    avgCPU / count,
		MemoryPercent: avgMemory / count,
		Timestamp:     time.Now(),
	}
	aa.baselineCaptured = true

	logging.Debug("AI adjuster baseline captured: %.0f rows/sec, CPU %.1f%%, Memory %.1f%%",
		aa.baselineMetrics.Throughput, aa.baselineMetrics.CPUPercent, aa.baselineMetrics.MemoryPercent)
}

// Evaluate analyzes current metrics and returns an adjustment recommendation.
func (aa *AIAdjuster) Evaluate(ctx context.Context) (*AdjustmentDecision, error) {
	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()

	// Capture baseline when we have enough data
	aa.captureBaseline()

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

	// Build prompt with full context
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

// buildAdjustmentPrompt constructs the prompt for AI analysis with full context.
func (aa *AIAdjuster) buildAdjustmentPrompt() string {
	metrics := aa.collector.GetRecentMetrics(5) // Last 5 samples for trend
	trends := aa.collector.AnalyzeTrends()
	config := aa.pipeline.GetConfig()

	var sb strings.Builder

	sb.WriteString("Real-time database migration parameter adjustment.\n\n")

	// System Resources
	sb.WriteString("## System Resources\n")
	sb.WriteString(fmt.Sprintf("- CPU cores: %d\n", aa.systemResources.CPUCores))
	sb.WriteString(fmt.Sprintf("- Memory: %d MB available / %d MB total\n",
		aa.systemResources.MemoryAvailableMB, aa.systemResources.MemoryTotalMB))
	if aa.systemResources.MaxSourceConnections > 0 {
		sb.WriteString(fmt.Sprintf("- Max source connections: %d\n", aa.systemResources.MaxSourceConnections))
	}
	if aa.systemResources.MaxTargetConnections > 0 {
		sb.WriteString(fmt.Sprintf("- Max target connections: %d\n", aa.systemResources.MaxTargetConnections))
	}
	sb.WriteString("\n")

	// Baseline metrics
	if aa.baselineMetrics != nil {
		sb.WriteString("## Baseline Performance\n")
		sb.WriteString(fmt.Sprintf("- Baseline throughput: %.0f rows/sec\n", aa.baselineMetrics.Throughput))
		sb.WriteString(fmt.Sprintf("- Baseline CPU: %.1f%%\n", aa.baselineMetrics.CPUPercent))
		sb.WriteString(fmt.Sprintf("- Baseline memory: %.1f%%\n", aa.baselineMetrics.MemoryPercent))
		sb.WriteString("\n")
	}

	// Current performance
	sb.WriteString("## Current Performance\n")
	if len(metrics) > 0 {
		latest := metrics[len(metrics)-1]
		sb.WriteString(fmt.Sprintf("- Current throughput: %.0f rows/sec", latest.Throughput))
		if aa.baselineMetrics != nil && aa.baselineMetrics.Throughput > 0 {
			pctChange := (latest.Throughput - aa.baselineMetrics.Throughput) / aa.baselineMetrics.Throughput * 100
			sb.WriteString(fmt.Sprintf(" (%.1f%% vs baseline)", pctChange))
		}
		sb.WriteString("\n")
		sb.WriteString(fmt.Sprintf("- CPU: %.1f%%\n", latest.CPUPercent))
		sb.WriteString(fmt.Sprintf("- Memory: %d MB (%.1f%%)\n", latest.MemoryUsedMB, latest.MemoryPercent))
		sb.WriteString(fmt.Sprintf("- Elapsed: %.0f seconds\n", latest.ElapsedSeconds))
		sb.WriteString(fmt.Sprintf("- Rows processed: %d\n", latest.RowsProcessed))
	}
	sb.WriteString("\n")

	// Recent metrics trend
	sb.WriteString("## Recent Metrics (last 5 samples)\n")
	if len(metrics) >= 3 {
		sb.WriteString("- Throughput: ")
		for i, m := range metrics {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%.0f", m.Throughput))
		}
		sb.WriteString(" rows/sec\n")

		sb.WriteString("- CPU: ")
		for i, m := range metrics {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%.0f%%", m.CPUPercent))
		}
		sb.WriteString("\n")

		sb.WriteString("- Memory: ")
		for i, m := range metrics {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%.0f%%", m.MemoryPercent))
		}
		sb.WriteString("\n")
	}
	sb.WriteString(fmt.Sprintf("- Throughput trend: %.1f%% (>20%% decline = significant)\n", trends.ThroughputDecline))
	sb.WriteString("\n")

	// Current config
	sb.WriteString("## Current Configuration\n")
	sb.WriteString(fmt.Sprintf("- workers: %d\n", config.WriteAheadWriters))
	sb.WriteString(fmt.Sprintf("- chunk_size: %d\n", config.ChunkSize))
	sb.WriteString(fmt.Sprintf("- parallel_readers: %d\n", config.ParallelReaders))
	sb.WriteString(fmt.Sprintf("- read_ahead_buffers: %d\n", config.ReadAheadBuffers))
	sb.WriteString("\n")

	// Current session adjustment history
	if len(aa.adjustmentHistory) > 0 {
		sb.WriteString("## Current Session Adjustments\n")
		for _, adj := range aa.adjustmentHistory {
			sb.WriteString(fmt.Sprintf("- %s ago: %s → throughput %+.1f%%\n",
				time.Since(adj.Timestamp).Round(time.Second),
				adj.Action,
				adj.EffectPercent))
		}
		sb.WriteString("\n")
	} else {
		sb.WriteString("## Current Session Adjustments\n- No adjustments made yet\n\n")
	}

	// Historical patterns from past migrations
	if aa.state != nil {
		if history, err := aa.state.GetAIAdjustments(50); err == nil && len(history) > 0 {
			// Calculate effectiveness by action type
			actionStats := make(map[string]struct {
				count      int
				effective  int
				avgEffect  float64
				totalEffect float64
			})

			for _, h := range history {
				stats := actionStats[h.Action]
				stats.count++
				stats.totalEffect += h.EffectPercent
				if h.EffectPercent > effectivenessThreshold {
					stats.effective++
				}
				actionStats[h.Action] = stats
			}

			sb.WriteString("## Historical Patterns (from past migrations)\n")
			for action, stats := range actionStats {
				if stats.count > 0 && action != "continue" {
					avgEffect := stats.totalEffect / float64(stats.count)
					sb.WriteString(fmt.Sprintf("- %s: effective %d/%d times (avg %+.1f%% throughput)\n",
						action, stats.effective, stats.count, avgEffect))
				}
			}
			sb.WriteString("\n")
		}
	}

	// Guidelines
	sb.WriteString(`## Guidelines
Based on system resources above, determine safe parameter ranges:
- workers: Should not exceed CPU cores or max DB connections
- chunk_size: Larger = better throughput, but watch memory usage

Decision rules:
1. **If within ±10% of baseline** → "continue" (stable, no changes needed)
2. **If >20% below baseline + CPU/memory available** → consider "scale_up"
3. **If memory >85%** → "reduce_chunk"
4. **If CPU >90% sustained** → consider "scale_down"
5. **If past adjustment didn't help** → don't repeat same action

Important: Only adjust if there's a significant problem. Stability is preferred.

Return ONLY valid JSON:
{
  "action": "continue|scale_up|scale_down|reduce_chunk",
  "adjustments": {
    "workers": <new value or omit>,
    "chunk_size": <new value or omit>
  },
  "reasoning": "<2-3 sentences explaining decision based on data>",
  "confidence": "high|medium|low"
}`)

	return sb.String()
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

	// Record throughput before adjustment for history
	var throughputBefore float64
	metrics := aa.collector.GetRecentMetrics(1)
	if len(metrics) > 0 {
		throughputBefore = metrics[0].Throughput
	}

	// Validate adjustments against system resources (AI-informed limits)
	for param, value := range decision.Adjustments {
		if err := aa.validateParameterWithResources(param, value); err != nil {
			logging.Warn("AI adjustment rejected: %v", err)
			return nil
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
			logging.Debug("AIAdjuster: unknown adjustment parameter %q (value=%d); ignoring", param, value)
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

	// Capture current CPU and memory for history
	var cpuBefore, memoryBefore float64
	if len(metrics) > 0 {
		cpuBefore = metrics[0].CPUPercent
		memoryBefore = metrics[0].MemoryPercent
	}

	// Record in history (we'll update the effect later)
	record := AdjustmentRecord{
		AdjustmentNumber: aa.adjustmentCount,
		Timestamp:        time.Now(),
		Action:           decision.Action,
		Adjustments:      decision.Adjustments,
		ThroughputBefore: throughputBefore,
	}
	aa.adjustmentHistory = append(aa.adjustmentHistory, record)
	if len(aa.adjustmentHistory) > aa.maxHistorySize {
		aa.adjustmentHistory = aa.adjustmentHistory[1:]
	}

	// Persist to database for cross-migration learning
	if aa.state != nil && aa.runID != "" {
		dbRecord := checkpoint.AIAdjustmentRecord{
			AdjustmentNumber: aa.adjustmentCount,
			Timestamp:        time.Now(),
			Action:           decision.Action,
			Adjustments:      decision.Adjustments,
			ThroughputBefore: throughputBefore,
			CPUBefore:        cpuBefore,
			MemoryBefore:     memoryBefore,
			Reasoning:        decision.Reasoning,
			Confidence:       decision.Confidence,
		}
		if err := aa.state.SaveAIAdjustment(aa.runID, dbRecord); err != nil {
			logging.Debug("Failed to persist AI adjustment: %v", err)
		}
	}

	// Schedule effect measurement (30s later)
	go aa.measureAdjustmentEffect(len(aa.adjustmentHistory) - 1)

	logging.Debug("AI adjustment #%d applied: %s - %s (confidence: %s)",
		aa.adjustmentCount, decision.Action, decision.Reasoning, decision.Confidence)

	aa.lastAdjustment = time.Now()
	return nil
}

// measureAdjustmentEffect measures the effect of an adjustment after 30 seconds.
func (aa *AIAdjuster) measureAdjustmentEffect(historyIndex int) {
	time.Sleep(30 * time.Second)

	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()

	if historyIndex >= len(aa.adjustmentHistory) {
		return
	}

	metrics := aa.collector.GetRecentMetrics(1)
	if len(metrics) == 0 {
		return
	}

	record := &aa.adjustmentHistory[historyIndex]
	record.ThroughputAfter = metrics[0].Throughput

	if record.ThroughputBefore > 0 {
		record.EffectPercent = (record.ThroughputAfter - record.ThroughputBefore) / record.ThroughputBefore * 100
		logging.Debug("AI adjustment effect: %s → throughput %+.1f%% (%.0f → %.0f rows/sec)",
			record.Action, record.EffectPercent, record.ThroughputBefore, record.ThroughputAfter)
	}

	// Update database with effect measurement
	if aa.state != nil && aa.runID != "" {
		// Use the original adjustment number captured at initial save time
		dbRecord := checkpoint.AIAdjustmentRecord{
			AdjustmentNumber: record.AdjustmentNumber,
			Timestamp:        record.Timestamp,
			Action:           record.Action,
			Adjustments:      record.Adjustments,
			ThroughputBefore: record.ThroughputBefore,
			ThroughputAfter:  record.ThroughputAfter,
			EffectPercent:    record.EffectPercent,
			CPUAfter:         metrics[0].CPUPercent,
			MemoryAfter:      metrics[0].MemoryPercent,
		}
		if err := aa.state.SaveAIAdjustment(aa.runID, dbRecord); err != nil {
			logging.Debug("Failed to update AI adjustment effect: %v", err)
		}
	}
}

// validateParameterWithResources validates against system resources.
func (aa *AIAdjuster) validateParameterWithResources(param string, value int) error {
	switch param {
	case "chunk_size":
		if value < 1000 || value > 500000 {
			return fmt.Errorf("chunk_size %d out of safe range (1K-500K)", value)
		}
	case "workers":
		// Max workers shouldn't exceed CPU cores or connection limits
		maxWorkers := aa.systemResources.CPUCores
		if aa.systemResources.MaxTargetConnections > 0 && aa.systemResources.MaxTargetConnections < maxWorkers {
			maxWorkers = aa.systemResources.MaxTargetConnections
		}
		if maxWorkers < 1 {
			maxWorkers = 16 // Fallback
		}
		if value < 1 || value > maxWorkers {
			return fmt.Errorf("workers %d out of safe range (1-%d based on system resources)", value, maxWorkers)
		}
	case "parallel_readers":
		maxReaders := 4
		if aa.systemResources.MaxSourceConnections > 0 && aa.systemResources.MaxSourceConnections < maxReaders {
			maxReaders = aa.systemResources.MaxSourceConnections
		}
		if value < 1 || value > maxReaders {
			return fmt.Errorf("parallel_readers %d out of safe range (1-%d)", value, maxReaders)
		}
	case "read_ahead_buffers":
		if value < 2 || value > 32 {
			return fmt.Errorf("read_ahead_buffers %d out of safe range (2-32)", value)
		}
	default:
		return fmt.Errorf("unknown parameter: %s", param)
	}
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

	// Check if we're close to baseline (stable)
	if aa.baselineMetrics != nil && aa.baselineMetrics.Throughput > 0 {
		pctFromBaseline := (latest.Throughput - aa.baselineMetrics.Throughput) / aa.baselineMetrics.Throughput * 100
		if pctFromBaseline >= -10 && pctFromBaseline <= 10 {
			return &AdjustmentDecision{
				Action:      "continue",
				Reasoning:   fmt.Sprintf("Throughput within ±10%% of baseline (%.1f%%)", pctFromBaseline),
				Confidence:  "high",
				Adjustments: make(map[string]int),
			}
		}
	}

	// Rule 1: Memory saturation (high priority)
	if trends.MemorySaturated {
		newChunkSize := config.ChunkSize / 2
		if newChunkSize < 5000 {
			newChunkSize = 5000
		}
		if newChunkSize == config.ChunkSize {
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

	// Rule 2: CPU saturation
	if trends.CPUSaturated && latest.CPUPercent > 85 {
		if config.WriteAheadWriters <= 1 {
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

	// Rule 3: Significant throughput decline (>20%)
	if trends.ThroughputDecreasing && !trends.CPUSaturated && !trends.MemorySaturated {
		maxWorkers := aa.systemResources.CPUCores
		if maxWorkers < 1 {
			maxWorkers = 16
		}
		if config.WriteAheadWriters >= maxWorkers {
			return &AdjustmentDecision{
				Action:      "continue",
				Reasoning:   fmt.Sprintf("Throughput declining %.0f%% but at max workers for system", trends.ThroughputDecline),
				Confidence:  "medium",
				Adjustments: make(map[string]int),
			}
		}
		newWorkers := config.WriteAheadWriters + 1
		return &AdjustmentDecision{
			Action:      "scale_up",
			Reasoning:   fmt.Sprintf("Throughput declining %.0f%% with resources available", trends.ThroughputDecline),
			Confidence:  "medium",
			Adjustments: map[string]int{"workers": newWorkers},
		}
	}

	return &AdjustmentDecision{
		Action:      "continue",
		Reasoning:   fmt.Sprintf("Performance stable (throughput trend: %.1f%%)", trends.ThroughputDecline),
		Confidence:  "high",
		Adjustments: make(map[string]int),
	}
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
			logging.Debug("AI adjustment circuit breaker CLOSED - resuming")
		}()
	}
}
