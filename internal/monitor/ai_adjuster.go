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
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/stats"
	"github.com/johndauphine/dmt/internal/transfer"
	"github.com/shirou/gopsutil/v3/mem"
)

const (
	// effectivenessThreshold defines the minimum percentage improvement
	// for an adjustment to be considered effective
	effectivenessThreshold = 5.0

	// adjustmentCooldown is the minimum time between applying adjustments.
	// This gives ~3 metric samples (at 30s intervals) to stabilize before
	// the next adjustment, preventing cascading changes.
	adjustmentCooldown = 90 * time.Second

	// negativeEffectThreshold is the number of consecutive adjustments with
	// negative impact before the effectiveness circuit breaker fires.
	negativeEffectThreshold = 3
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

// TableSummary captures aggregate table metadata for AI context.
type TableSummary struct {
	TotalTables int
	TotalRows   int64
	AvgRowBytes int64
}

// AIAdjuster uses AI to analyze metrics and recommend parameter adjustments.
type AIAdjuster struct {
	aiMapper       *driver.AITypeMapper
	collector      *MetricsCollector
	tuner          transfer.RuntimeTuner
	startTime      time.Time
	lastAdjustment time.Time
	adjustmentsMu  sync.Mutex

	// System resources for AI context
	systemResources SystemResources

	// Baseline metrics
	baselineMetrics  *PerformanceSnapshot
	baselineCaptured bool

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

	// Circuit breaker (API failures)
	failureCount     int
	failureThreshold int
	resetTimeout     time.Duration
	circuitOpen      bool

	// Effectiveness circuit breaker (consecutive bad adjustments)
	consecutiveNegative     int
	effectivenessOpenedAt   time.Time     // when effectiveness breaker fired
	effectivenessResetAfter time.Duration // auto-reset timeout

	// Transfer progress
	totalRows  int64
	targetMode string // "drop_recreate" or "upsert"

	// Persistent history
	state checkpoint.StateBackend
	runID string

	// Connection pool stats callback (live, called at prompt-build time)
	poolStatsFunc func() (stats.PoolStats, stats.PoolStats)

	// Table-level completion progress
	progressTracker *progress.Tracker

	// Static table metadata
	tableSummary TableSummary
}

// NewAIAdjuster creates a new AI adjuster.
func NewAIAdjuster(aiMapper *driver.AITypeMapper, collector *MetricsCollector, tuner transfer.RuntimeTuner) *AIAdjuster {
	aa := &AIAdjuster{
		aiMapper:          aiMapper,
		collector:         collector,
		tuner:             tuner,
		startTime:         time.Now(),
		callInterval:      30 * time.Second, // Throttle to 30s intervals
		cacheDuration:     60 * time.Second, // Cache decisions for 60s
		failureThreshold:  3,
		resetTimeout:           5 * time.Minute,
		circuitOpen:            false,
		effectivenessResetAfter: 10 * time.Minute,
		maxHistorySize:         10, // Keep last 10 adjustments
		adjustmentHistory:      make([]AdjustmentRecord, 0, 10),
	}

	// Gather system resources
	aa.gatherSystemResources()

	return aa
}

// SetTotalRows sets the total rows for the migration so the adjuster can
// skip adjustments when the transfer is nearly complete.
func (aa *AIAdjuster) SetTotalRows(total int64) {
	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()
	aa.totalRows = total
}

// SetConnectionLimits sets the max connection limits for source and target.
func (aa *AIAdjuster) SetConnectionLimits(maxSource, maxTarget int) {
	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()
	aa.systemResources.MaxSourceConnections = maxSource
	aa.systemResources.MaxTargetConnections = maxTarget
}

// SetTargetMode sets the migration target mode (drop_recreate or upsert).
func (aa *AIAdjuster) SetTargetMode(mode string) {
	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()
	aa.targetMode = mode
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

// SetPoolStatsFunc sets a callback that returns live source and target pool stats.
func (aa *AIAdjuster) SetPoolStatsFunc(fn func() (stats.PoolStats, stats.PoolStats)) {
	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()
	aa.poolStatsFunc = fn
}

// SetProgressTracker sets the progress tracker for table-level completion stats.
func (aa *AIAdjuster) SetProgressTracker(tracker *progress.Tracker) {
	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()
	aa.progressTracker = tracker
}

// SetTableSummary sets static table metadata computed from the tables list.
func (aa *AIAdjuster) SetTableSummary(summary TableSummary) {
	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()
	aa.tableSummary = summary
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

	// Check circuit breaker (API failures)
	if aa.circuitOpen {
		return nil, fmt.Errorf("circuit breaker OPEN - AI adjustments temporarily disabled")
	}

	// Check effectiveness circuit breaker (consecutive bad adjustments)
	if aa.consecutiveNegative >= negativeEffectThreshold {
		// Auto-reset after timeout to allow retrying
		if !aa.effectivenessOpenedAt.IsZero() && time.Since(aa.effectivenessOpenedAt) >= aa.effectivenessResetAfter {
			logging.Debug("AI adjuster effectiveness breaker reset after %v", aa.effectivenessResetAfter)
			aa.consecutiveNegative = 0
			aa.effectivenessOpenedAt = time.Time{}
		} else {
			if aa.effectivenessOpenedAt.IsZero() {
				aa.effectivenessOpenedAt = time.Now()
			}
			logging.Warn("AI adjuster paused: %d consecutive adjustments had negative effect (resets in %v)",
				aa.consecutiveNegative, aa.effectivenessResetAfter-time.Since(aa.effectivenessOpenedAt))
			return &AdjustmentDecision{
				Action:      "continue",
				Reasoning:   fmt.Sprintf("Pausing adjustments after %d consecutive negative effects", aa.consecutiveNegative),
				Confidence:  "high",
				Adjustments: make(map[string]int),
			}, nil
		}
	}

	// Check post-adjustment cooldown: wait for metrics to stabilize after
	// the last adjustment before evaluating again
	if !aa.lastAdjustment.IsZero() && time.Since(aa.lastAdjustment) < adjustmentCooldown {
		remaining := adjustmentCooldown - time.Since(aa.lastAdjustment)
		logging.Debug("AI adjuster cooldown: %.0fs remaining after last adjustment", remaining.Seconds())
		return &AdjustmentDecision{
			Action:      "continue",
			Reasoning:   fmt.Sprintf("Cooling down after last adjustment (%.0fs remaining)", remaining.Seconds()),
			Confidence:  "high",
			Adjustments: make(map[string]int),
		}, nil
	}

	// Skip adjustments when transfer is nearly complete (>90%)
	// Use live row count (not snapshot) to avoid 30s stale data gap
	if aa.totalRows > 0 {
		currentRows := aa.collector.GetCurrentRowCount()
		if currentRows > 0 {
			pct := float64(currentRows) / float64(aa.totalRows) * 100
			if pct >= 90 {
				logging.Debug("AI adjuster skipping: transfer %.1f%% complete", pct)
				return &AdjustmentDecision{
					Action:      "continue",
					Reasoning:   fmt.Sprintf("Transfer %.1f%% complete, no adjustments needed", pct),
					Confidence:  "high",
					Adjustments: make(map[string]int),
				}, nil
			}
		}
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
	config := aa.tuner.Snapshot()

	var sb strings.Builder

	sb.WriteString("Real-time database migration parameter adjustment.\n\n")

	// System Resources (refresh available memory)
	sb.WriteString("## System Resources\n")
	sb.WriteString(fmt.Sprintf("- CPU cores: %d\n", aa.systemResources.CPUCores))
	sb.WriteString(fmt.Sprintf("- Total RAM: %d MB\n", aa.systemResources.MemoryTotalMB))
	if memInfo, err := mem.VirtualMemory(); err == nil {
		availMB := int64(memInfo.Available / 1024 / 1024)
		usedMB := int64(memInfo.Used / 1024 / 1024)
		totalMB := int64(memInfo.Total / 1024 / 1024)
		if totalMB > 0 {
			sb.WriteString(fmt.Sprintf("- Available RAM: %d MB (%.1f%% free)\n", availMB, float64(availMB)/float64(totalMB)*100))
			sb.WriteString(fmt.Sprintf("- Used RAM: %d MB (%.1f%% used)\n", usedMB, float64(usedMB)/float64(totalMB)*100))
		} else {
			sb.WriteString(fmt.Sprintf("- Available RAM: %d MB\n", availMB))
			sb.WriteString(fmt.Sprintf("- Used RAM: %d MB\n", usedMB))
		}
	} else {
		sb.WriteString(fmt.Sprintf("- Available RAM: %d MB (at startup)\n", aa.systemResources.MemoryAvailableMB))
	}
	if aa.systemResources.MaxSourceConnections > 0 {
		sb.WriteString(fmt.Sprintf("- Max source connections: %d\n", aa.systemResources.MaxSourceConnections))
	}
	if aa.systemResources.MaxTargetConnections > 0 {
		sb.WriteString(fmt.Sprintf("- Max target connections: %d\n", aa.systemResources.MaxTargetConnections))
	}
	sb.WriteString("\n")

	// Connection pool stats (live)
	if aa.poolStatsFunc != nil {
		src, tgt := aa.poolStatsFunc()
		sb.WriteString("## Connection Pool Stats\n")
		sb.WriteString(fmt.Sprintf("- Source (%s): %d/%d active, %d idle", src.DBType, src.ActiveConns, src.MaxConns, src.IdleConns))
		if src.WaitCount > 0 {
			sb.WriteString(fmt.Sprintf(", %d waits (%.1fms avg)", src.WaitCount, float64(src.WaitTimeMs)/float64(src.WaitCount)))
		}
		sb.WriteString("\n")
		sb.WriteString(fmt.Sprintf("- Target (%s): %d/%d active, %d idle", tgt.DBType, tgt.ActiveConns, tgt.MaxConns, tgt.IdleConns))
		if tgt.WaitCount > 0 {
			sb.WriteString(fmt.Sprintf(", %d waits (%.1fms avg)", tgt.WaitCount, float64(tgt.WaitTimeMs)/float64(tgt.WaitCount)))
		}
		sb.WriteString("\n\n")
	}

	// Baseline metrics
	if aa.baselineMetrics != nil {
		sb.WriteString("## Baseline Performance\n")
		sb.WriteString(fmt.Sprintf("- Baseline throughput: %.0f rows/sec\n", aa.baselineMetrics.Throughput))
		sb.WriteString(fmt.Sprintf("- Baseline CPU: %.1f%%\n", aa.baselineMetrics.CPUPercent))
		sb.WriteString(fmt.Sprintf("- Baseline memory: %.1f%%\n", aa.baselineMetrics.MemoryPercent))
		sb.WriteString("\n")
	}

	// Migration mode
	if aa.targetMode != "" {
		sb.WriteString("## Migration Mode\n")
		sb.WriteString(fmt.Sprintf("- Target mode: %s\n", aa.targetMode))
		sb.WriteString("\n")
	}

	// Data profile (static)
	if aa.tableSummary.TotalTables > 0 {
		sb.WriteString("## Data Profile\n")
		sb.WriteString(fmt.Sprintf("- Total tables: %d\n", aa.tableSummary.TotalTables))
		sb.WriteString(fmt.Sprintf("- Total rows: %d\n", aa.tableSummary.TotalRows))
		if aa.tableSummary.AvgRowBytes > 0 {
			sb.WriteString(fmt.Sprintf("- Avg row size: %d bytes\n", aa.tableSummary.AvgRowBytes))
			estMB := int64(config.WriteAheadWriters) * int64(config.ReadAheadBuffers) *
				int64(config.ChunkSize) * aa.tableSummary.AvgRowBytes / 1024 / 1024
			sb.WriteString(fmt.Sprintf("- Est. pipeline memory (workers×buffers×chunk×row): %d MB\n", estMB))
		}
		sb.WriteString("\n")
	}

	// Table-level progress (live)
	if aa.progressTracker != nil {
		total := aa.progressTracker.TablesTotal()
		complete := aa.progressTracker.TablesComplete()
		failed := aa.progressTracker.TablesFailed()
		if total > 0 {
			sb.WriteString("## Table Progress\n")
			sb.WriteString(fmt.Sprintf("- Tables complete: %d/%d\n", complete, total))
			if failed > 0 {
				sb.WriteString(fmt.Sprintf("- Tables failed: %d\n", failed))
			}
			sb.WriteString(fmt.Sprintf("- Tables remaining: %d\n", total-complete-failed))
			sb.WriteString("\n")
		}
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
		sb.WriteString(fmt.Sprintf("- Active jobs: %d (concurrent table transfers)\n", latest.ActiveWorkers))
		sb.WriteString(fmt.Sprintf("- Queue depth: %d (chunks buffered in read-ahead pipeline)\n", latest.QueueDepth))
		sb.WriteString(fmt.Sprintf("- Error count: %d (failed tables)\n", latest.ErrorCount))
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

	// Transfer time breakdown (from latest snapshot)
	if len(metrics) > 0 {
		latest := metrics[len(metrics)-1]
		if latest.QueryTimePercent > 0 || latest.WriteTimePercent > 0 {
			sb.WriteString("## Transfer Time Breakdown (since last sample)\n")
			sb.WriteString(fmt.Sprintf("- Source query + row scanning: %.0f%%\n", latest.QueryTimePercent))
			sb.WriteString(fmt.Sprintf("- Target write: %.0f%%\n", latest.WriteTimePercent))
			sb.WriteString("\n")
		}
	}

	// Current config
	sb.WriteString("## Current Configuration\n")
	sb.WriteString(fmt.Sprintf("- workers: %d\n", config.WriteAheadWriters))
	sb.WriteString(fmt.Sprintf("- chunk_size: %d\n", config.ChunkSize))
	sb.WriteString(fmt.Sprintf("- parallel_readers: %d\n", config.ParallelReaders))
	sb.WriteString(fmt.Sprintf("- read_ahead_buffers: %d\n", config.ReadAheadBuffers))
	sb.WriteString(fmt.Sprintf("- checkpoint_frequency: %d\n", config.CheckpointFrequency))
	sb.WriteString(fmt.Sprintf("- upsert_merge_chunk_size: %d\n", config.UpsertMergeChunkSize))
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
				count       int
				effective   int
				avgEffect   float64
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
Consider system resources when choosing parameter values:
- workers: Consider CPU cores and max DB connections. More workers than cores may cause contention. More workers than max connections will fail.
- parallel_readers: Consider max source connections. Each reader uses a connection.
- chunk_size: Rows per source query (reader side). Larger = better throughput, but each chunk is held in memory. Memory per chunk = chunk_size × avg_row_bytes.
- batch_size: Rows per INSERT statement (writer side). Controls how many rows are sent in a single write to the target DB. Must respect target DB placeholder limits (e.g. MySQL: rows × columns < 65,535). If 0, the writer uses its configured default.
- read_ahead_buffers: Total buffered memory = workers × read_ahead_buffers × chunk_size × avg_row_bytes.
- checkpoint_frequency: Higher = fewer checkpoints = better throughput; Lower = more safety on failure
- upsert_merge_chunk_size: Smaller = less memory pressure on target DB; Only relevant in upsert mode

Bottleneck diagnosis:
- Queue depth HIGH (many chunks buffered) → writers can't keep up (write-bound). Consider reducing chunk_size or workers to reduce write contention.
- Queue depth LOW/ZERO → readers can't keep up (read-bound). Consider increasing parallel_readers or read_ahead_buffers.
- Active jobs shows how many tables are being transferred concurrently. If active_jobs < configured workers, some jobs finished early.
- Error count tracks failed tables. Rising errors suggest reducing pressure (fewer workers, smaller chunks).
- Write time >60% → target is the bottleneck. Consider smaller batch_size or fewer workers to reduce write contention.
- Query+scan time >60% → source is the bottleneck. Consider more parallel_readers to increase read throughput.
- Balanced time split → pipeline is well-tuned, prefer "continue".
- Pool active connections near max → connection pool is saturated. Reducing workers or parallel_readers will free connections.
- Pool wait count rising → connections are being queued, a strong signal the pool is the bottleneck.
- Use avg row size from Data Profile to calculate memory impact: pipeline memory ≈ workers × read_ahead_buffers × chunk_size × avg_row_bytes.

There are no hard limits — you are free to try any value. If a change hurts performance, the effectiveness tracker will detect it and pause adjustments.

Decision rules:
1. **If within ±10% of baseline** → "continue" (stable, no changes needed)
2. **If >20% below baseline + CPU/memory available** → consider "scale_up"
3. **If memory >75%** → "reduce_chunk" or reduce upsert_merge_chunk_size
4. **If CPU >90% sustained** → consider "scale_down"
5. **If past adjustment didn't help** → don't repeat same action
6. **If stable and low failure risk** → consider increasing checkpoint_frequency for throughput
7. **If error count is rising** → consider reducing workers, chunk_size, or batch_size to reduce pressure
8. **If write errors due to placeholder limits** → reduce batch_size for affected tables

Important: Only adjust if there's a significant problem. Stability is preferred.

Return ONLY valid JSON:
{
  "action": "continue|scale_up|scale_down|reduce_chunk|reduce_batch|adjust_checkpoint|adjust_upsert_chunk",
  "adjustments": {
    "workers": <new value or omit>,
    "chunk_size": <new value or omit>,
    "batch_size": <new value or omit>,
    "checkpoint_frequency": <new value or omit>,
    "upsert_merge_chunk_size": <new value or omit>
  },
  "reasoning": "<2-3 sentences explaining decision based on data>",
  "confidence": "high|medium|low"
}`)

	return sb.String()
}

// extractJSON finds the outermost JSON object in a string by matching braces.
// Handles responses wrapped in markdown fences, with trailing prose, etc.
func extractJSON(s string) string {
	first := strings.Index(s, "{")
	if first == -1 {
		return ""
	}
	last := strings.LastIndex(s, "}")
	if last == -1 || last <= first {
		return ""
	}
	return s[first : last+1]
}

// parseDecision parses the AI response into an AdjustmentDecision.
func (aa *AIAdjuster) parseDecision(response string) (*AdjustmentDecision, error) {
	// Extract JSON by finding the outermost { ... } pair.
	// This handles markdown fences, trailing prose, and other wrapper text.
	jsonStr := extractJSON(response)
	if jsonStr == "" {
		return nil, fmt.Errorf("no JSON object found in response: %s", response)
	}

	var decision AdjustmentDecision
	if err := json.Unmarshal([]byte(jsonStr), &decision); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w\nExtracted: %s", err, jsonStr)
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

// ApplyDecision applies an adjustment decision via the runtime tuner.
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

	// Clamp adjustments to per-parameter minimums (let AI learn from effectiveness tracking)
	for param, value := range decision.Adjustments {
		minVal := 1
		if param == "read_ahead_buffers" {
			minVal = 0 // read_ahead_buffers=0 is valid (disables buffering)
		}
		if value < minVal {
			logging.Debug("AI adjustment clamped: %s=%d → %d (minimum)", param, value, minVal)
			decision.Adjustments[param] = minVal
		}
	}

	// Build runtime update
	update := transfer.RuntimeUpdate{}

	for param, value := range decision.Adjustments {
		v := value // capture loop variable
		switch param {
		case "chunk_size":
			update.ChunkSize = &v
		case "workers":
			update.WriteAheadWriters = &v
		case "parallel_readers":
			update.ParallelReaders = &v
		case "read_ahead_buffers":
			update.ReadAheadBuffers = &v
		case "checkpoint_frequency":
			update.CheckpointFrequency = &v
		case "upsert_merge_chunk_size":
			update.UpsertMergeChunkSize = &v
		case "batch_size":
			// batch_size is per-table, not a global tuner parameter.
			// It's handled via SetTableBatchSize in EvaluateWriteError.
			// If AI suggests it in periodic tuning, log but skip — it requires table context.
			logging.Debug("AIAdjuster: batch_size=%d suggested in periodic tuning (per-table only, skipping global apply)", value)
			continue
		default:
			logging.Debug("AIAdjuster: unknown adjustment parameter %q (value=%d); ignoring", param, value)
		}
	}

	// Apply to tuner (takes effect immediately)
	if err := aa.tuner.Update(update); err != nil {
		return fmt.Errorf("failed to apply runtime update: %w", err)
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

		// Track consecutive negative effects for effectiveness circuit breaker
		if record.EffectPercent < -5.0 {
			aa.consecutiveNegative++
			logging.Debug("AI adjuster consecutive negative effects: %d/%d",
				aa.consecutiveNegative, negativeEffectThreshold)
		} else if record.EffectPercent > 0 {
			aa.consecutiveNegative = 0
		}
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
	config := aa.tuner.Snapshot()

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

// EvaluateWriteError asks the AI to recommend a new chunk_size after a write error.
// Returns the recommended chunk size, or 0 if the AI cannot help (error should be fatal).
// Implements transfer.WriteErrorAdjuster.
func (aa *AIAdjuster) EvaluateWriteError(ctx context.Context, errCtx transfer.WriteErrorContext) int {
	aa.adjustmentsMu.Lock()
	defer aa.adjustmentsMu.Unlock()

	// Build a focused prompt for the error
	prompt := aa.buildWriteErrorPrompt(errCtx)

	// Call AI (bypass cooldown/circuit breaker — this is error recovery, not periodic tuning)
	response, err := aa.aiMapper.CallAI(ctx, prompt)
	if err != nil {
		logging.Warn("AI error diagnosis failed: %v, using fallback", err)
		return aa.fallbackChunkSize(errCtx)
	}

	decision, err := aa.parseDecision(response)
	if err != nil {
		logging.Warn("Failed to parse AI error response: %v, using fallback", err)
		return aa.fallbackChunkSize(errCtx)
	}

	// Check for batch_size (preferred) or chunk_size (backward compat) in response
	if newSize, ok := decision.Adjustments["batch_size"]; ok && newSize > 0 {
		logging.Info("AI recommended batch_size=%d for table %s: %s", newSize, errCtx.TableName, decision.Reasoning)
		return newSize
	}
	if newSize, ok := decision.Adjustments["chunk_size"]; ok && newSize > 0 {
		logging.Info("AI recommended batch_size=%d (via chunk_size) for table %s: %s", newSize, errCtx.TableName, decision.Reasoning)
		return newSize
	}

	// AI didn't recommend a batch size change — error is not batch-size related
	return 0
}

// buildWriteErrorPrompt constructs a prompt for AI to diagnose a write error and recommend batch_size.
func (aa *AIAdjuster) buildWriteErrorPrompt(errCtx transfer.WriteErrorContext) string {
	var sb strings.Builder

	sb.WriteString("A database write operation failed during migration. Analyze the error and recommend a new batch_size if the error is related to the number of rows per INSERT statement.\n\n")

	sb.WriteString("## Error Context\n")
	sb.WriteString(fmt.Sprintf("- Table: %s\n", errCtx.TableName))
	sb.WriteString(fmt.Sprintf("- Target database: %s\n", errCtx.TargetDBType))
	sb.WriteString(fmt.Sprintf("- Column count: %d\n", errCtx.ColumnCount))
	sb.WriteString(fmt.Sprintf("- Current batch_size: %d (rows per INSERT)\n", errCtx.ChunkSize))
	sb.WriteString(fmt.Sprintf("- Rows in failed batch: %d\n", errCtx.RowCount))
	sb.WriteString(fmt.Sprintf("- Total placeholders: %d (rows × columns)\n", errCtx.RowCount*errCtx.ColumnCount))
	sb.WriteString(fmt.Sprintf("- Error: %s\n\n", errCtx.ErrorMessage))

	sb.WriteString("## Current Configuration\n")
	config := aa.tuner.Snapshot()
	sb.WriteString(fmt.Sprintf("- Global chunk_size (reader): %d\n", config.ChunkSize))
	sb.WriteString(fmt.Sprintf("- workers: %d\n", config.WriteAheadWriters))
	sb.WriteString("\n")

	sb.WriteString(`## Instructions
Analyze the error and determine if it can be fixed by reducing batch_size (rows per INSERT statement).

Note: batch_size controls the writer side (rows per INSERT). chunk_size controls the reader side (rows per source query). They are independent parameters.

Known database limits:
- MySQL: max 65,535 prepared statement placeholders (batch_size × columns must be < 65,535)
- MySQL: max_allowed_packet limits total query size
- PostgreSQL: max 65,535 parameters per query
- SQL Server: max 2,100 parameters per query (but uses bulk copy, so this rarely applies)

If the error IS related to batch size:
- Calculate the optimal batch_size that avoids the error while maximizing throughput
- Apply a 10% safety margin below the hard limit
- Consider that this batch_size will apply to future batches for this table

If the error is NOT related to batch size:
- Set batch_size to 0 to indicate this error cannot be fixed by batch_size adjustment

Return ONLY valid JSON:
{
  "action": "adjust_batch_size",
  "adjustments": {
    "batch_size": <recommended value, or 0 if not a batch_size issue>
  },
  "reasoning": "<explain the root cause and how the new batch_size fixes it>",
  "confidence": "high|medium|low"
}`)

	return sb.String()
}

// fallbackChunkSize computes a safe batch_size when the AI is unavailable.
func (aa *AIAdjuster) fallbackChunkSize(errCtx transfer.WriteErrorContext) int {
	if errCtx.ColumnCount <= 0 {
		return 0
	}

	// Check for known placeholder limits by target DB type
	var maxPlaceholders int
	switch errCtx.TargetDBType {
	case "mysql":
		maxPlaceholders = 65535
	case "postgres":
		maxPlaceholders = 65535
	case "mssql":
		maxPlaceholders = 2100
	default:
		return 0
	}

	// Calculate with 10% safety margin
	safeChunkSize := int(float64(maxPlaceholders) / float64(errCtx.ColumnCount) * 0.9)
	if safeChunkSize < 1 {
		safeChunkSize = 1
	}

	// Only return if it's actually smaller than what failed
	if safeChunkSize < errCtx.RowCount {
		logging.Info("Fallback chunk_size=%d for table %s (%d columns, %d max placeholders)",
			safeChunkSize, errCtx.TableName, errCtx.ColumnCount, maxPlaceholders)
		return safeChunkSize
	}

	return 0
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
