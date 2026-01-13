package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/data-transfer-tool/internal/logging"
	"github.com/johndauphine/data-transfer-tool/internal/source"
)

// Validate checks row counts between source and target
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

	var failed bool
	for _, t := range o.tables {
		// Query fresh counts from both source and target (don't use cached t.RowCount)
		sourceCount, err := o.sourcePool.GetRowCount(ctx, o.config.Source.Schema, t.Name)
		if err != nil {
			logging.Error("%-30s ERROR getting source count: %v", t.Name, err)
			failed = true
			continue
		}

		targetCount, err := o.targetPool.GetRowCount(ctx, o.config.Target.Schema, t.Name)
		if err != nil {
			logging.Error("%-30s ERROR getting target count: %v", t.Name, err)
			failed = true
			continue
		}

		if targetCount == sourceCount {
			logging.Info("%-30s OK %d rows", t.Name, targetCount)
		} else {
			logging.Error("%-30s FAIL source=%d target=%d (diff=%d)",
				t.Name, sourceCount, targetCount, sourceCount-targetCount)
			failed = true
		}
	}

	if failed {
		return fmt.Errorf("validation failed")
	}
	return nil
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
