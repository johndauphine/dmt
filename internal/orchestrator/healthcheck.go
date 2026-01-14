package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/calibration"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

// HealthCheck tests connectivity to source and target databases.
// Runs source and target checks in parallel with independent timeouts to prevent
// one slow connection from causing the other to fail with "context deadline exceeded".
func (o *Orchestrator) HealthCheck(ctx context.Context) (*HealthCheckResult, error) {
	result := &HealthCheckResult{
		Timestamp:    time.Now().Format(time.RFC3339),
		SourceDBType: o.sourcePool.DBType(),
		TargetDBType: o.targetPool.DBType(),
	}

	// Use a per-check timeout of 30 seconds.
	const checkTimeout = 30 * time.Second

	// Run source and target checks in parallel to:
	// 1. Give each check its own 30-second budget
	// 2. Preserve context cancellation (SIGINT, etc.)
	// 3. Complete in max(source, target) time instead of source + target
	var wg sync.WaitGroup
	wg.Add(2)

	// Source check goroutine
	go func() {
		defer wg.Done()
		sourceStart := time.Now()
		sourceCtx, sourceCancel := context.WithTimeout(ctx, checkTimeout)
		defer sourceCancel()

		if db := o.sourcePool.DB(); db != nil {
			if err := db.PingContext(sourceCtx); err != nil {
				result.SourceError = err.Error()
			} else {
				result.SourceConnected = true
				// Get table count
				tables, err := o.sourcePool.ExtractSchema(sourceCtx, o.config.Source.Schema)
				if err == nil {
					result.SourceTableCount = len(tables)
				}
			}
		}
		result.SourceLatencyMs = time.Since(sourceStart).Milliseconds()
	}()

	// Target check goroutine
	go func() {
		defer wg.Done()
		targetStart := time.Now()
		targetCtx, targetCancel := context.WithTimeout(ctx, checkTimeout)
		defer targetCancel()

		if err := o.targetPool.Ping(targetCtx); err != nil {
			result.TargetError = err.Error()
		} else {
			result.TargetConnected = true
		}
		result.TargetLatencyMs = time.Since(targetStart).Milliseconds()
	}()

	wg.Wait()

	result.Healthy = result.SourceConnected && result.TargetConnected
	return result, nil
}

// DryRun performs a migration preview without transferring data.
func (o *Orchestrator) DryRun(ctx context.Context) (*DryRunResult, error) {
	logging.Info("Performing dry run (no data will be transferred)...")

	// Extract schema
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		return nil, fmt.Errorf("extracting schema: %w", err)
	}

	// Apply table filters
	tables = o.filterTables(tables)

	result := &DryRunResult{
		SourceType:   o.config.Source.Type,
		TargetType:   o.config.Target.Type,
		SourceSchema: o.config.Source.Schema,
		TargetSchema: o.config.Target.Schema,
		Workers:      o.config.Migration.Workers,
		ChunkSize:    o.config.Migration.ChunkSize,
		TargetMode:   o.config.Migration.TargetMode,
		TotalTables:  len(tables),
	}

	// Calculate estimated memory
	bufferMem := int64(o.config.Migration.Workers) *
		int64(o.config.Migration.ReadAheadBuffers) *
		int64(o.config.Migration.ChunkSize) *
		500 // bytes per row estimate
	result.EstimatedMemMB = bufferMem / (1024 * 1024)

	// Analyze each table
	for _, t := range tables {
		rowCount, err := o.sourcePool.GetRowCount(ctx, o.config.Source.Schema, t.Name)
		if err != nil {
			logging.Warn("Failed to get row count for %s.%s: %v (assuming 0)", o.config.Source.Schema, t.Name, err)
			rowCount = 0
		}
		result.TotalRows += rowCount

		// Determine pagination method
		paginationMethod := "full_table"
		partitions := 1
		hasPK := len(t.PKColumns) > 0

		if hasPK {
			if len(t.PKColumns) == 1 && isIntegerType(t.PKColumns[0].DataType) {
				paginationMethod = "keyset"
			} else {
				paginationMethod = "row_number"
			}

			// Estimate partitions for large tables
			if rowCount > int64(o.config.Migration.LargeTableThreshold) {
				partitions = o.config.Migration.MaxPartitions
			}
		}

		result.Tables = append(result.Tables, DryRunTable{
			Name:             t.Name,
			RowCount:         rowCount,
			PaginationMethod: paginationMethod,
			Partitions:       partitions,
			HasPK:            hasPK,
			Columns:          len(t.Columns),
		})
	}

	return result, nil
}

// isIntegerType checks if a data type is an integer type.
func isIntegerType(dataType string) bool {
	dataType = strings.ToLower(dataType)
	intTypes := []string{"int", "bigint", "smallint", "tinyint", "integer", "int4", "int8", "int2"}
	for _, t := range intTypes {
		if strings.Contains(dataType, t) {
			return true
		}
	}
	return false
}

// AnalyzeConfig uses AI to analyze the source database and suggest optimal configuration.
func (o *Orchestrator) AnalyzeConfig(ctx context.Context, schema string) (*driver.SmartConfigSuggestions, error) {
	logging.Info("Analyzing source database for configuration suggestions...")

	// Get AI mapper from secrets
	aiMapper, err := driver.NewAITypeMapperFromSecrets()
	if err != nil {
		return nil, fmt.Errorf("loading AI type mapper: %w", err)
	}

	// Create the smart config analyzer
	analyzer := driver.NewSmartConfigAnalyzer(o.sourcePool.DB(), o.sourcePool.DBType(), aiMapper)

	// Run analysis
	suggestions, err := analyzer.Analyze(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("analyzing config: %w", err)
	}

	return suggestions, nil
}

// Calibrate runs calibration tests to find optimal migration configuration.
// It executes mini-migrations with different parameter combinations and uses AI
// to analyze results and recommend optimal settings.
func (o *Orchestrator) Calibrate(ctx context.Context, sampleSize int) (*calibration.CalibrationResult, error) {
	logging.Info("Starting calibration with sample size %d...", sampleSize)

	// Get AI mapper from secrets (optional - calibration works without AI too)
	aiMapper, err := driver.NewAITypeMapperFromSecrets()
	if err != nil {
		logging.Warn("AI not configured, calibration will use fallback recommendations: %v", err)
		aiMapper = nil
	}

	// Get drivers for driver-specific defaults
	sourceDriver, err := driver.Get(o.sourcePool.DBType())
	if err != nil {
		return nil, fmt.Errorf("getting source driver: %w", err)
	}
	targetDriver, err := driver.Get(o.targetPool.DBType())
	if err != nil {
		return nil, fmt.Errorf("getting target driver: %w", err)
	}

	// Create calibrator
	cal := calibration.NewCalibrator(
		o.config,
		o.sourcePool,
		o.targetPool,
		sourceDriver,
		targetDriver,
		calibration.CalibratorOptions{
			SampleSize: sampleSize,
			Depth:      calibration.DepthQuick,
			AIMapper:   aiMapper,
		},
	)

	// Run calibration
	result, err := cal.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("calibration failed: %w", err)
	}

	return result, nil
}
