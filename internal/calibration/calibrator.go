package calibration

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
)

const (
	// minRowsForCalibration is the minimum row count for a table to be useful for calibration.
	minRowsForCalibration = 1000

	// calibrationRunTimeout is the maximum duration for a single calibration run.
	calibrationRunTimeout = 5 * time.Minute
)

// Calibrator runs calibration tests to find optimal configuration.
type Calibrator struct {
	config        *config.Config
	sourcePool    pool.SourcePool
	targetPool    pool.TargetPool
	sourceDriver  driver.Driver
	targetDriver  driver.Driver
	aiAnalyzer    *AIAnalyzer
	aiGuided      *AIGuidedCalibrator
	schemaManager *SchemaManager
	sampleSize    int
	tables        []*driver.Table
	depth         CalibrationDepth
	aiGuidedMode  bool
	maxRuns       int
}

// CalibratorOptions configures the calibrator.
type CalibratorOptions struct {
	// SampleSize is the number of rows to transfer per table (default 10000).
	SampleSize int

	// Tables specifies specific tables to use (empty for auto-select).
	Tables []string

	// Depth controls how many configurations to test (default DepthQuick).
	Depth CalibrationDepth

	// AIMapper is the AI type mapper for calibration analysis.
	AIMapper *driver.AITypeMapper

	// AIGuided enables full AI-guided calibration where AI decides parameters.
	AIGuided bool

	// MaxRuns limits the number of calibration runs in AI-guided mode (default 7).
	MaxRuns int
}

// NewCalibrator creates a new calibrator.
func NewCalibrator(
	cfg *config.Config,
	sourcePool pool.SourcePool,
	targetPool pool.TargetPool,
	sourceDriver driver.Driver,
	targetDriver driver.Driver,
	opts CalibratorOptions,
) *Calibrator {
	if opts.SampleSize <= 0 {
		opts.SampleSize = 10000
	}
	if opts.Depth == "" {
		opts.Depth = DepthQuick
	}
	if opts.MaxRuns <= 0 {
		opts.MaxRuns = 7
	}

	return &Calibrator{
		config:       cfg,
		sourcePool:   sourcePool,
		targetPool:   targetPool,
		sourceDriver: sourceDriver,
		targetDriver: targetDriver,
		aiAnalyzer:   NewAIAnalyzer(opts.AIMapper),
		sampleSize:   opts.SampleSize,
		depth:        opts.Depth,
		aiGuidedMode: opts.AIGuided,
		maxRuns:      opts.MaxRuns,
	}
}

// Run executes the calibration process.
// Returns the calibration result with recommended configuration.
func (c *Calibrator) Run(ctx context.Context) (*CalibrationResult, error) {
	// Create cancelable context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Register signal handler for crash-safe cleanup
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Ensure cleanup on any exit
	cleanupDone := make(chan struct{})
	go func() {
		select {
		case <-sigChan:
			logging.Warn("Received interrupt signal, cleaning up...")
			cancel()
		case <-cleanupDone:
		}
	}()
	defer func() {
		signal.Stop(sigChan)
		close(cleanupDone)
	}()

	result := &CalibrationResult{
		Runs: make([]RunMetrics, 0),
	}

	// Step 1: Measure network latency
	logging.Debug("Measuring network latency...")
	sourceLatency, err := c.measureSourceLatency(ctx)
	if err != nil {
		logging.Warn("Failed to measure source latency: %v", err)
	}
	targetLatency, err := c.measureTargetLatency(ctx)
	if err != nil {
		logging.Warn("Failed to measure target latency: %v", err)
	}
	result.SourceLatencyMs = float64(sourceLatency.Milliseconds())
	result.TargetLatencyMs = float64(targetLatency.Milliseconds())
	logging.Debug("Network latency: source %.1fms, target %.1fms", result.SourceLatencyMs, result.TargetLatencyMs)

	// Step 2: Select tables for calibration
	logging.Debug("Selecting tables for calibration...")
	if err := c.selectTables(ctx); err != nil {
		return nil, fmt.Errorf("failed to select tables: %w", err)
	}
	if len(c.tables) == 0 {
		return nil, fmt.Errorf("no suitable tables found for calibration (need tables with primary keys)")
	}
	for _, t := range c.tables {
		result.TablesUsed = append(result.TablesUsed, t.Name)
		result.TotalSampleRows += int64(c.sampleSize)
	}
	logging.Debug("Selected %d table(s): %v", len(c.tables), result.TablesUsed)

	// Step 3: Create calibration schema
	c.schemaManager = NewSchemaManager(c.targetPool, c.targetPool.DBType())
	if err := c.schemaManager.CreateSchema(ctx); err != nil {
		return nil, fmt.Errorf("failed to create calibration schema: %w", err)
	}

	// Ensure cleanup happens
	defer func() {
		if err := c.schemaManager.DropSchema(context.Background()); err != nil {
			logging.Error("Failed to cleanup calibration schema: %v", err)
			logging.Error("Manual cleanup SQL:\n%s", c.schemaManager.ManualCleanupInstructions())
		}
	}()

	// Step 4: Create tables in calibration schema
	for _, table := range c.tables {
		_, err := c.schemaManager.CreateTable(ctx, table)
		if err != nil {
			return nil, fmt.Errorf("failed to create calibration table %s: %w", table.Name, err)
		}
	}

	// Step 5: Run calibration configurations
	configs := GetConfigs(c.depth)
	logging.Debug("Running %d calibration configurations...", len(configs))

	for i, cfg := range configs {
		logging.Debug("[%d/%d] Testing: %s (chunk=%d, workers=%d, buffers=%d)",
			i+1, len(configs), cfg.Name, cfg.ChunkSize, cfg.Workers, cfg.ReadAheadBuffers)

		// Check if context was canceled
		if ctx.Err() != nil {
			logging.Warn("Calibration interrupted")
			break
		}

		// Truncate tables before each run
		if err := c.schemaManager.TruncateAllTables(ctx); err != nil {
			logging.Warn("Failed to truncate tables: %v", err)
		}

		// Execute the calibration run
		metrics := c.executeRun(ctx, &cfg)
		result.Runs = append(result.Runs, metrics)

		// Log result
		if metrics.Status == StatusSuccess {
			logging.Debug("  Result: %.0f rows/sec (query=%.0f%%, write=%.0f%%)",
				metrics.RowsPerSecond, metrics.QueryTimePercent, metrics.WriteTimePercent)
		} else {
			logging.Debug("  Result: FAILED (%s) - %s", metrics.ErrorCategory, metrics.Error)
		}
	}

	// Step 6: AI analysis (if available)
	if c.aiAnalyzer != nil {
		logging.Debug("Analyzing results with AI...")
		sourceDefaults := c.sourceDriver.Defaults()
		targetDefaults := c.targetDriver.Defaults()

		aiInput := &AnalyzeInput{
			Result:               result,
			SourceDBType:         c.sourcePool.DBType(),
			TargetDBType:         c.targetPool.DBType(),
			SourceDriverDefaults: &sourceDefaults,
			TargetDriverDefaults: &targetDefaults,
		}

		rec, err := c.aiAnalyzer.Analyze(ctx, aiInput)
		if err != nil {
			logging.Warn("AI analysis failed: %v", err)
			logging.Info("Falling back to best observed configuration")
		} else {
			result.Recommendation = &RecommendedConfig{
				ChunkSize:            rec.RecommendedConfig.ChunkSize,
				Workers:              rec.RecommendedConfig.Workers,
				ReadAheadBuffers:     rec.RecommendedConfig.ReadAheadBuffers,
				ParallelReaders:      rec.RecommendedConfig.ParallelReaders,
				WriteAheadWriters:    rec.RecommendedConfig.WriteAheadWriters,
				PacketSize:           rec.RecommendedConfig.PacketSize,
				EstimatedRowsPerSec:  rec.EstimatedRowsPerSec,
				Confidence:           rec.Confidence,
				MaxPartitions:        rec.RecommendedConfig.MaxPartitions,
				LargeTableThreshold:  rec.RecommendedConfig.LargeTableThreshold,
				SourceChunkSize:      rec.RecommendedConfig.SourceChunkSize,
				TargetChunkSize:      rec.RecommendedConfig.TargetChunkSize,
				UpsertMergeChunkSize: rec.RecommendedConfig.UpsertMergeChunkSize,
				MaxSourceConnections: rec.RecommendedConfig.MaxSourceConnections,
				MaxTargetConnections: rec.RecommendedConfig.MaxTargetConnections,
			}
			result.AIReasoning = rec.Reasoning
			result.PatternsDetected = rec.PatternsDetected
			result.Warnings = rec.Warnings
			result.AIUsed = true
		}
	}

	// Fallback if no AI or AI failed
	if result.Recommendation == nil {
		result.Recommendation = FallbackRecommendation(result)
		if result.Recommendation == nil {
			result.Warnings = append(result.Warnings, "All calibration runs failed - check database connectivity and permissions")
		}
	}

	return result, nil
}

// measureSourceLatency measures round-trip time to the source database.
func (c *Calibrator) measureSourceLatency(ctx context.Context) (time.Duration, error) {
	const iterations = 5
	var totalLatency time.Duration

	db := c.sourcePool.DB()
	for i := 0; i < iterations; i++ {
		start := time.Now()
		err := db.PingContext(ctx)
		if err != nil {
			return 0, err
		}
		totalLatency += time.Since(start)
	}

	return totalLatency / iterations, nil
}

// measureTargetLatency measures round-trip time to the target database.
func (c *Calibrator) measureTargetLatency(ctx context.Context) (time.Duration, error) {
	const iterations = 5
	var totalLatency time.Duration

	for i := 0; i < iterations; i++ {
		start := time.Now()
		err := c.targetPool.Ping(ctx)
		if err != nil {
			return 0, err
		}
		totalLatency += time.Since(start)
	}

	return totalLatency / iterations, nil
}

// selectTables selects tables for calibration based on catalog statistics.
func (c *Calibrator) selectTables(ctx context.Context) error {
	// Extract schema to get table list with row counts
	tables, err := c.sourcePool.ExtractSchema(ctx, c.config.Source.Schema)
	if err != nil {
		return fmt.Errorf("failed to extract schema: %w", err)
	}

	// Filter tables: need PK and some rows
	var candidates []*driver.Table
	for i := range tables {
		t := &tables[i]
		if !t.HasSinglePK() {
			continue // Need single-column PK for efficient pagination
		}
		if !t.SupportsKeysetPagination() {
			continue // Need integer PK
		}
		if t.RowCount < minRowsForCalibration {
			continue // Need enough rows for meaningful test
		}
		candidates = append(candidates, t)
	}

	// Sort by row count (prefer larger tables)
	for i := 0; i < len(candidates)-1; i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].RowCount > candidates[i].RowCount {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	// Take top 3 tables
	maxTables := 3
	if len(candidates) < maxTables {
		maxTables = len(candidates)
	}
	c.tables = candidates[:maxTables]

	return nil
}

// executeRun executes a single calibration run with the given configuration.
func (c *Calibrator) executeRun(ctx context.Context, cfg *CalibrationConfig) RunMetrics {
	metrics := RunMetrics{
		ConfigName: cfg.Name,
		Config:     *cfg,
		Status:     StatusSuccess,
	}

	// Create timeout context for this run
	runCtx, cancel := context.WithTimeout(ctx, calibrationRunTimeout)
	defer cancel()

	startTime := time.Now()
	var totalRows int64
	var queryTime, writeTime time.Duration

	// Transfer data for each table
	for _, table := range c.tables {
		rows, qt, wt, err := c.transferTableSample(runCtx, table, cfg)
		if err != nil {
			metrics.Status = StatusFailed
			metrics.ErrorCategory = CategorizeError(err)
			metrics.Error = err.Error()
			metrics.Duration = time.Since(startTime)
			return metrics
		}
		totalRows += rows
		queryTime += qt
		writeTime += wt
	}

	metrics.Duration = time.Since(startTime)
	metrics.RowsTransferred = totalRows

	if metrics.Duration.Seconds() > 0 {
		metrics.RowsPerSecond = float64(totalRows) / metrics.Duration.Seconds()
	}

	// Calculate time percentages
	totalTime := float64(metrics.Duration)
	if totalTime > 0 {
		metrics.QueryTimePercent = float64(queryTime) / totalTime * 100
		metrics.WriteTimePercent = float64(writeTime) / totalTime * 100
	}

	return metrics
}

// transferTableSample transfers a sample of rows from a table.
func (c *Calibrator) transferTableSample(ctx context.Context, table *driver.Table, cfg *CalibrationConfig) (rows int64, queryTime, writeTime time.Duration, err error) {
	// Build column lists
	cols := make([]string, len(table.Columns))
	colTypes := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		cols[i] = col.Name
		colTypes[i] = col.DataType
	}

	// Create partition to limit sample size
	// We'll read from the beginning of the table up to sampleSize rows
	partition := &driver.Partition{
		TableName:   table.Name,
		PartitionID: 0,
		MinPK:       nil, // Start from beginning
		MaxPK:       nil, // No upper bound (we'll stop after sampleSize)
		StartRow:    0,
		EndRow:      int64(c.sampleSize),
		RowCount:    int64(c.sampleSize),
	}

	// Read sample rows from source
	queryStart := time.Now()
	readOpts := driver.ReadOptions{
		Table:       *table,
		Columns:     cols,
		ColumnTypes: colTypes,
		ChunkSize:   cfg.ChunkSize,
		Partition:   partition,
	}

	batchCh, err := c.sourcePool.ReadTable(ctx, readOpts)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to read from source: %w", err)
	}

	// Write to target
	for batch := range batchCh {
		queryTime += time.Since(queryStart)

		if batch.Error != nil {
			return rows, queryTime, writeTime, fmt.Errorf("batch read error: %w", batch.Error)
		}

		// Stop if we've read enough
		if rows >= int64(c.sampleSize) {
			break
		}

		// Limit batch if it would exceed sample size
		batchRows := batch.Rows
		remaining := int64(c.sampleSize) - rows
		if int64(len(batchRows)) > remaining {
			batchRows = batchRows[:remaining]
		}

		writeStart := time.Now()
		err := c.targetPool.WriteBatch(ctx, driver.WriteBatchOptions{
			Schema:  c.schemaManager.SchemaName(),
			Table:   table.Name,
			Columns: cols,
			Rows:    batchRows,
		})
		writeTime += time.Since(writeStart)

		if err != nil {
			return rows, queryTime, writeTime, fmt.Errorf("write error: %w", err)
		}

		rows += int64(len(batchRows))
		queryStart = time.Now()
	}

	return rows, queryTime, writeTime, nil
}

// gatherSystemContext collects comprehensive system information for AI.
func (c *Calibrator) gatherSystemContext(ctx context.Context) *SystemContext {
	logging.Debug("Gathering system context for AI analysis...")

	sysCtx := &SystemContext{
		Host: GatherHostInfo(),
	}

	// Source database info
	if c.sourcePool.DBType() == "mssql" {
		sysCtx.Source = GatherMSSQLInfo(ctx, c.sourcePool.DB(), c.config.Source.Host, c.config.Source.Port)
	} else {
		sysCtx.Source = GatherPostgresInfo(ctx, c.sourcePool.DB(), c.config.Source.Host, c.config.Source.Port)
	}

	// Source table statistics
	tableCount, totalRows, largestRows, largestName, avgWidth := GatherTableStats(
		ctx, c.sourcePool.DB(), c.sourcePool.DBType(), c.config.Source.Schema)
	sysCtx.Source.TableCount = tableCount
	sysCtx.Source.TotalRows = totalRows
	sysCtx.Source.LargestTableRows = largestRows
	sysCtx.Source.LargestTableName = largestName
	sysCtx.Source.AvgRowWidthBytes = avgWidth

	// Target database info (use Writer interface)
	sysCtx.Target = GatherTargetDatabaseInfo(ctx, c.targetPool, c.config.Target.Host, c.config.Target.Port)

	// Network latency with percentiles
	sysCtx.Network.SourceLatencyMs, sysCtx.Network.SourceLatencyP95Ms = MeasureLatencyDetailed(
		ctx, func(ctx context.Context) error { return c.sourcePool.DB().PingContext(ctx) })
	sysCtx.Network.TargetLatencyMs, sysCtx.Network.TargetLatencyP95Ms = MeasureLatencyDetailed(
		ctx, func(ctx context.Context) error { return c.targetPool.Ping(ctx) })

	sysCtx.Source.LatencyMs = sysCtx.Network.SourceLatencyMs
	sysCtx.Target.LatencyMs = sysCtx.Network.TargetLatencyMs

	return sysCtx
}

// RunAIGuided executes AI-guided calibration where AI decides what to test.
func (c *Calibrator) RunAIGuided(ctx context.Context) (*CalibrationResult, error) {
	// Create cancelable context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Register signal handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	cleanupDone := make(chan struct{})
	go func() {
		select {
		case <-sigChan:
			logging.Warn("Received interrupt signal, cleaning up...")
			cancel()
		case <-cleanupDone:
		}
	}()
	defer func() {
		signal.Stop(sigChan)
		close(cleanupDone)
	}()

	result := &CalibrationResult{
		Runs: make([]RunMetrics, 0),
	}

	// Step 1: Gather comprehensive system context
	sysCtx := c.gatherSystemContext(ctx)
	logging.Debug("\n%s", sysCtx.FormatSummary())

	result.SourceLatencyMs = sysCtx.Network.SourceLatencyMs
	result.TargetLatencyMs = sysCtx.Network.TargetLatencyMs

	// Step 2: Select tables for calibration
	logging.Debug("Selecting tables for calibration...")
	if err := c.selectTables(ctx); err != nil {
		return nil, fmt.Errorf("failed to select tables: %w", err)
	}
	if len(c.tables) == 0 {
		return nil, fmt.Errorf("no suitable tables found for calibration")
	}
	for _, t := range c.tables {
		result.TablesUsed = append(result.TablesUsed, t.Name)
		result.TotalSampleRows += int64(c.sampleSize)
	}
	logging.Debug("Selected %d table(s): %v", len(c.tables), result.TablesUsed)

	// Step 3: Create calibration schema
	c.schemaManager = NewSchemaManager(c.targetPool, c.targetPool.DBType())
	if err := c.schemaManager.CreateSchema(ctx); err != nil {
		return nil, fmt.Errorf("failed to create calibration schema: %w", err)
	}
	defer func() {
		if err := c.schemaManager.DropSchema(context.Background()); err != nil {
			logging.Error("Failed to cleanup calibration schema: %v", err)
		}
	}()

	// Create tables in calibration schema
	for _, table := range c.tables {
		_, err := c.schemaManager.CreateTable(ctx, table)
		if err != nil {
			return nil, fmt.Errorf("failed to create calibration table %s: %w", table.Name, err)
		}
	}

	// Step 4: Initialize AI-guided calibrator
	aiMapper, _ := driver.NewAITypeMapperFromSecrets()
	if aiMapper == nil {
		return nil, fmt.Errorf("AI-guided calibration requires AI to be configured")
	}
	c.aiGuided = NewAIGuidedCalibrator(aiMapper, sysCtx)

	// Step 5: Get initial configuration from AI
	logging.Debug("Asking AI for initial calibration parameters...")
	initialSuggestion, err := c.aiGuided.GetInitialConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("AI initial analysis failed: %w", err)
	}

	logging.Debug("AI Initial Suggestion:")
	logging.Debug("  Confidence: %s", initialSuggestion.Confidence)
	logging.Debug("  Reasoning: %s", initialSuggestion.Reasoning)
	if len(initialSuggestion.Warnings) > 0 {
		for _, w := range initialSuggestion.Warnings {
			logging.Debug("  Warning: %s", w)
		}
	}

	// Step 6: Run adaptive calibration
	currentConfig := initialSuggestion.Config
	currentConfig.Name = "ai_initial"

	for runNum := 1; runNum <= c.maxRuns; runNum++ {
		if ctx.Err() != nil {
			logging.Warn("Calibration interrupted")
			break
		}

		logging.Debug("[Run %d/%d] Testing: %s (chunk=%d, workers=%d, buffers=%d)",
			runNum, c.maxRuns, currentConfig.Name, currentConfig.ChunkSize,
			currentConfig.Workers, currentConfig.ReadAheadBuffers)

		// Truncate tables before each run
		if err := c.schemaManager.TruncateAllTables(ctx); err != nil {
			logging.Warn("Failed to truncate tables: %v", err)
		}

		// Execute the run
		metrics := c.executeRun(ctx, &currentConfig)
		result.Runs = append(result.Runs, metrics)

		// Log result
		if metrics.Status == StatusSuccess {
			logging.Debug("  Result: %.0f rows/sec (query=%.0f%%, write=%.0f%%)",
				metrics.RowsPerSecond, metrics.QueryTimePercent, metrics.WriteTimePercent)
		} else {
			logging.Debug("  Result: FAILED (%s) - %s", metrics.ErrorCategory, metrics.Error)
		}

		// Ask AI what to do next
		if runNum < c.maxRuns {
			logging.Debug("Asking AI for next configuration...")
			nextSuggestion, err := c.aiGuided.GetNextConfig(ctx, &metrics)
			if err != nil {
				logging.Warn("AI next config failed: %v, continuing with preset", err)
				// Fall back to preset configs
				presetConfigs := GetConfigs(c.depth)
				if runNum < len(presetConfigs) {
					currentConfig = presetConfigs[runNum]
				}
				continue
			}

			logging.Debug("  AI Decision: %s", nextSuggestion.Reasoning)

			if nextSuggestion.ShouldStop {
				logging.Debug("AI recommends stopping early - sufficient data collected")
				break
			}

			currentConfig = nextSuggestion.Config
			currentConfig.Name = fmt.Sprintf("ai_adaptive_%d", runNum)
		}
	}

	// Step 7: Get final recommendation
	logging.Debug("Analyzing all results with AI...")
	finalRec, err := c.aiGuided.GetFinalRecommendation(ctx)
	if err != nil {
		logging.Warn("AI final analysis failed: %v", err)
		logging.Info("Falling back to best observed configuration")
		result.Recommendation = FallbackRecommendation(result)
	} else {
		result.Recommendation = &RecommendedConfig{
			ChunkSize:            finalRec.RecommendedConfig.ChunkSize,
			Workers:              finalRec.RecommendedConfig.Workers,
			ReadAheadBuffers:     finalRec.RecommendedConfig.ReadAheadBuffers,
			ParallelReaders:      finalRec.RecommendedConfig.ParallelReaders,
			WriteAheadWriters:    finalRec.RecommendedConfig.WriteAheadWriters,
			PacketSize:           finalRec.RecommendedConfig.PacketSize,
			EstimatedRowsPerSec:  finalRec.EstimatedRowsPerSec,
			Confidence:           finalRec.Confidence,
			MaxPartitions:        finalRec.RecommendedConfig.MaxPartitions,
			LargeTableThreshold:  finalRec.RecommendedConfig.LargeTableThreshold,
			SourceChunkSize:      finalRec.RecommendedConfig.SourceChunkSize,
			TargetChunkSize:      finalRec.RecommendedConfig.TargetChunkSize,
			UpsertMergeChunkSize: finalRec.RecommendedConfig.UpsertMergeChunkSize,
			MaxSourceConnections: finalRec.RecommendedConfig.MaxSourceConnections,
			MaxTargetConnections: finalRec.RecommendedConfig.MaxTargetConnections,
		}
		result.AIReasoning = finalRec.Reasoning
		result.PatternsDetected = finalRec.PatternsDetected
		result.Warnings = finalRec.Warnings
		result.AIUsed = true
	}

	return result, nil
}
