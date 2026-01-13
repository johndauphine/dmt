package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/data-transfer-tool/internal/driver"
	"github.com/johndauphine/data-transfer-tool/internal/logging"
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

	// Create AI mapper if configured
	var aiMapper *driver.AITypeMapper
	if o.config.AI != nil && o.config.AI.APIKey != "" {
		aiConfig := &driver.AITypeMappingConfig{
			Enabled:        true,
			Provider:       o.config.AI.Provider,
			APIKey:         o.config.AI.APIKey,
			Model:          o.config.AI.Model,
			TimeoutSeconds: o.config.AI.TimeoutSeconds,
		}
		var err error
		aiMapper, err = driver.NewAITypeMapper(*aiConfig, nil)
		if err != nil {
			logging.Warn("Failed to create AI mapper for analysis: %v", err)
			// Continue without AI
		}
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
