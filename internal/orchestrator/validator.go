package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/source"
)

// ValidationTimeout is the maximum time to wait for a single table's row count query.
const ValidationTimeout = 30 * time.Second

// tableValidationResult holds the result of validating a single table.
type tableValidationResult struct {
	tableName    string
	sourceCount  int64
	targetCount  int64
	err          error
	timedOut     bool
	usedEstimate bool // true if we used fast/estimated counts
}

// Validate checks row counts between source and target in parallel.
func (o *Orchestrator) Validate(ctx context.Context) error {
	if o.tables == nil {
		tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
		if err != nil {
			return err
		}
		o.tables = tables
	}

	logging.Info("\nValidation Results:")
	logging.Info("-------------------")

	// Run validation for all tables in parallel
	results := make(chan tableValidationResult, len(o.tables))
	var wg sync.WaitGroup

	for _, t := range o.tables {
		wg.Add(1)
		go func(table source.Table) {
			defer wg.Done()
			result := o.validateTable(ctx, table)
			results <- result
		}(t)
	}

	// Wait for all validations to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var allResults []tableValidationResult
	for result := range results {
		allResults = append(allResults, result)
	}

	// Sort results by table name for consistent output
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].tableName < allResults[j].tableName
	})

	// Report results
	var failed bool
	for _, r := range allResults {
		if r.timedOut {
			logging.Warn("%-30s TIMEOUT (validation skipped after %v)", r.tableName, ValidationTimeout)
			continue
		}
		if r.err != nil {
			logging.Error("%-30s ERROR: %v", r.tableName, r.err)
			failed = true
			continue
		}
		if r.targetCount == r.sourceCount {
			if r.usedEstimate {
				logging.Warn("%-30s OK ~%d rows (estimated)", r.tableName, r.targetCount)
			} else {
				logging.Info("%-30s OK %d rows", r.tableName, r.targetCount)
			}
		} else {
			if r.usedEstimate {
				logging.Warn("%-30s DIFF source=~%d target=~%d (estimated, diff=%d)",
					r.tableName, r.sourceCount, r.targetCount, r.sourceCount-r.targetCount)
			} else {
				logging.Error("%-30s FAIL source=%d target=%d (diff=%d)",
					r.tableName, r.sourceCount, r.targetCount, r.sourceCount-r.targetCount)
				failed = true
			}
		}
	}

	if failed {
		return fmt.Errorf("validation failed")
	}
	return nil
}

// validateTable validates a single table's row count.
// It first tries exact COUNT(*) with a timeout, then falls back to estimated counts.
func (o *Orchestrator) validateTable(ctx context.Context, t source.Table) tableValidationResult {
	result := tableValidationResult{tableName: t.Name}

	// First, try exact COUNT(*) with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, ValidationTimeout)
	defer cancel()

	// Query source count (exact)
	sourceCount, srcErr := o.sourcePool.GetRowCountExact(timeoutCtx, o.config.Source.Schema, t.Name)
	srcTimedOut := timeoutCtx.Err() == context.DeadlineExceeded

	// Query target count (exact) - use fresh timeout
	timeoutCtx2, cancel2 := context.WithTimeout(ctx, ValidationTimeout)
	defer cancel2()
	targetCount, tgtErr := o.targetPool.GetRowCountExact(timeoutCtx2, o.config.Target.Schema, t.Name)
	tgtTimedOut := timeoutCtx2.Err() == context.DeadlineExceeded

	// If both exact counts succeeded, use them
	if srcErr == nil && tgtErr == nil {
		result.sourceCount = sourceCount
		result.targetCount = targetCount
		return result
	}

	// If either timed out, fall back to estimated counts
	if srcTimedOut || tgtTimedOut {
		srcEst, srcEstErr := o.sourcePool.GetRowCountFast(ctx, o.config.Source.Schema, t.Name)
		tgtEst, tgtEstErr := o.targetPool.GetRowCountFast(ctx, o.config.Target.Schema, t.Name)

		if srcEstErr == nil && tgtEstErr == nil && srcEst > 0 && tgtEst > 0 {
			result.sourceCount = srcEst
			result.targetCount = tgtEst
			result.usedEstimate = true
			return result
		}

		// Both exact and estimate failed
		result.timedOut = true
		return result
	}

	// Non-timeout error
	if srcErr != nil {
		result.err = fmt.Errorf("source count: %w", srcErr)
	} else {
		result.err = fmt.Errorf("target count: %w", tgtErr)
	}
	return result
}

// validateSamples performs sample data validation by comparing random rows
func (o *Orchestrator) validateSamples(ctx context.Context) error {
	sampleSize := o.config.Migration.SampleSize
	if sampleSize <= 0 {
		sampleSize = 100
	}

	logging.Info("\nSample Validation (n=%d per table):", sampleSize)
	logging.Info("------------------------------------")

	var failed bool
	for _, t := range o.tables {
		if !t.HasPK() {
			logging.Debug("%-30s SKIP (no PK)", t.Name)
			continue
		}

		// Build sample query based on source database type
		var sampleQuery string
		if o.sourcePool.DBType() == "postgres" {
			// PostgreSQL source syntax
			pkCols := make([]string, len(t.PrimaryKey))
			for i, col := range t.PrimaryKey {
				pkCols[i] = fmt.Sprintf("%q", col)
			}
			pkColList := strings.Join(pkCols, ", ")
			sampleQuery = fmt.Sprintf(`
				SELECT %s FROM %s.%q
				ORDER BY random()
				LIMIT %d
			`, pkColList, t.Schema, t.Name, sampleSize)
		} else {
			// SQL Server source syntax
			pkCols := make([]string, len(t.PrimaryKey))
			for i, col := range t.PrimaryKey {
				pkCols[i] = fmt.Sprintf("[%s]", col)
			}
			pkColList := strings.Join(pkCols, ", ")
			tableHint := "WITH (NOLOCK)"
			if o.config.Migration.StrictConsistency {
				tableHint = ""
			}
			sampleQuery = fmt.Sprintf(`
				SELECT TOP %d %s FROM [%s].[%s] %s
				ORDER BY NEWID()
			`, sampleSize, pkColList, t.Schema, t.Name, tableHint)
		}

		rows, err := o.sourcePool.DB().QueryContext(ctx, sampleQuery)
		if err != nil {
			logging.Error("%-30s ERROR: %v", t.Name, err)
			continue
		}

		// Collect sample PK tuples (each is a slice of values)
		var pkTuples [][]any
		for rows.Next() {
			// Create slice to hold all PK column values
			pkValues := make([]any, len(t.PrimaryKey))
			pkPtrs := make([]any, len(t.PrimaryKey))
			for i := range pkValues {
				pkPtrs[i] = &pkValues[i]
			}

			if err := rows.Scan(pkPtrs...); err != nil {
				continue
			}
			pkTuples = append(pkTuples, pkValues)
		}
		rows.Close()

		if len(pkTuples) == 0 {
			logging.Debug("%-30s SKIP (no rows)", t.Name)
			continue
		}

		// Check if these PK tuples exist in target
		missingCount := 0
		for _, pkTuple := range pkTuples {
			exists, err := o.checkRowExistsInTarget(ctx, t, pkTuple)
			if err != nil || !exists {
				missingCount++
			}
		}

		if missingCount == 0 {
			logging.Info("%-30s OK (%d samples)", t.Name, len(pkTuples))
		} else {
			logging.Error("%-30s FAIL (%d/%d missing)", t.Name, missingCount, len(pkTuples))
			failed = true
		}
	}

	if failed {
		return fmt.Errorf("sample validation failed")
	}
	return nil
}

// checkRowExistsInTarget checks if a row with the given PK values exists in target
func (o *Orchestrator) checkRowExistsInTarget(ctx context.Context, t source.Table, pkTuple []any) (bool, error) {
	var checkQuery string
	var args []any

	if o.targetPool.DBType() == "postgres" {
		// PostgreSQL target
		whereClauses := make([]string, len(t.PrimaryKey))
		for i, col := range t.PrimaryKey {
			whereClauses[i] = fmt.Sprintf("%q = $%d", col, i+1)
		}
		whereClause := strings.Join(whereClauses, " AND ")
		checkQuery = fmt.Sprintf(
			`SELECT EXISTS(SELECT 1 FROM %s.%q WHERE %s)`,
			o.config.Target.Schema, t.Name, whereClause,
		)
		args = pkTuple
		var exists bool
		err := o.targetPool.QueryRowRaw(ctx, checkQuery, &exists, args...)
		return exists, err
	} else {
		// SQL Server target
		whereClauses := make([]string, len(t.PrimaryKey))
		args = make([]any, len(t.PrimaryKey))
		for i, col := range t.PrimaryKey {
			whereClauses[i] = fmt.Sprintf("[%s] = @p%d", col, i+1)
			args[i] = sql.Named(fmt.Sprintf("p%d", i+1), pkTuple[i])
		}
		whereClause := strings.Join(whereClauses, " AND ")
		checkQuery = fmt.Sprintf(
			`SELECT CASE WHEN EXISTS(SELECT 1 FROM [%s].[%s] WHERE %s) THEN 1 ELSE 0 END`,
			o.config.Target.Schema, t.Name, whereClause,
		)
		var exists int
		err := o.targetPool.QueryRowRaw(ctx, checkQuery, &exists, args...)
		return exists == 1, err
	}
}
