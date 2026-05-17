package orchestrator

import (
	"context"
	"errors"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/transfer"
)

// checkGeographyError logs a hint for geography/geometry errors.
func (r *TransferRunner) checkGeographyError(tableName string, err error) {
	errStr := err.Error()
	if strings.Contains(errStr, "Invalid operator for data type") &&
		(strings.Contains(errStr, "geography") || strings.Contains(errStr, "geometry")) {
		logging.Warn("HINT: Table %s contains geography/geometry columns which cannot be compared in MERGE statements.", tableName)
		logging.Warn("      Use 'target_mode: drop_recreate' or exclude this table with 'exclude_tables'.")
	}
}

// diagnoseError analyzes a transfer error and emits a diagnosis through
// the deterministic catalog (#173). Pattern-matched diagnoses are
// suggestions, not corrections — emitting one never changes control
// flow; the underlying error continues to propagate to the caller.
//
// The caller's ctx is forwarded so a canceled/timed-out transfer stays
// silent rather than emitting a misleading "no diagnosis available"
// box (driver.DiagnoseError returns nil when ctx is already done).
func (r *TransferRunner) diagnoseError(ctx context.Context, j transfer.Job, err error) {
	errCtx := &driver.ErrorContext{
		ErrorMessage: err.Error(),
		TableName:    j.Table.Name,
		TableSchema:  j.Table.Schema,
		SourceDBType: r.config.Source.Type,
		TargetDBType: r.config.Target.Type,
		TargetMode:   r.config.Migration.TargetMode,
	}

	if j.Table.Columns != nil {
		errCtx.Columns = make([]driver.Column, len(j.Table.Columns))
		for i, col := range j.Table.Columns {
			errCtx.Columns[i] = driver.Column{
				Name:       col.Name,
				DataType:   col.DataType,
				MaxLength:  col.MaxLength,
				Precision:  col.Precision,
				Scale:      col.Scale,
				IsNullable: col.IsNullable,
				IsIdentity: col.IsIdentity,
			}
		}
	}

	if diag := driver.DiagnoseError(ctx, errCtx); diag != nil {
		driver.EmitDiagnosis(diag)
	}
}

// collectFailures gathers and deduplicates table failures.
func (r *TransferRunner) collectFailures(errCh <-chan tableError) ([]TableFailure, error) {
	failedTables := make(map[string]error)

	for te := range errCh {
		if errors.Is(te.err, context.Canceled) || errors.Is(te.err, context.DeadlineExceeded) {
			return nil, context.Canceled
		}
		if _, exists := failedTables[te.tableName]; !exists {
			failedTables[te.tableName] = te.err
			r.progress.TableFailed()
		}
	}

	var failures []TableFailure
	for tableName, err := range failedTables {
		failures = append(failures, TableFailure{TableName: tableName, Error: err})
	}

	return failures, nil
}
