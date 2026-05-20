package mssql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	mssql "github.com/microsoft/go-mssqldb"
)

// UpsertBatch performs upsert using staging table + MERGE.
func (w *Writer) UpsertBatch(ctx context.Context, opts driver.UpsertBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("upsert requires primary key columns")
	}

	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Close()

	// Generate staging table name
	stagingTable := w.safeStagingName(opts.Table, opts.WriterID, nil)

	// Create temp table
	targetTable := w.dialect.QualifyTable(opts.Schema, opts.Table)
	createSQL := fmt.Sprintf(`SELECT TOP 0 * INTO %s FROM %s`, stagingTable, targetTable)
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("creating staging table: %w", err)
	}

	// Detect spatial columns
	isCrossEngine := w.sourceType == "postgres"
	spatialCols, err := w.getSpatialColumns(ctx, conn, stagingTable)
	if err != nil {
		return fmt.Errorf("detecting spatial columns: %w", err)
	}

	// Populate SRIDs
	if len(spatialCols) > 0 && len(opts.ColumnSRIDs) == len(opts.Columns) {
		sridMap := make(map[string]int, len(opts.Columns))
		for i, col := range opts.Columns {
			sridMap[strings.ToLower(col)] = opts.ColumnSRIDs[i]
		}
		for i := range spatialCols {
			if srid, ok := sridMap[strings.ToLower(spatialCols[i].Name)]; ok && srid > 0 {
				spatialCols[i].SRID = srid
			}
		}
	}

	// Alter spatial columns for cross-engine
	if isCrossEngine && len(spatialCols) > 0 {
		if err := w.alterSpatialColumnsToText(ctx, conn, stagingTable, spatialCols); err != nil {
			return fmt.Errorf("altering spatial columns: %w", err)
		}
	}

	// Get actual column names (case handling)
	actualCols, err := w.getStagingTableColumns(ctx, conn, stagingTable)
	if err != nil {
		return fmt.Errorf("getting staging table columns: %w", err)
	}

	colMapping := make(map[string]string, len(actualCols))
	for _, ac := range actualCols {
		colMapping[strings.ToLower(ac)] = ac
	}

	mappedCols := make([]string, len(opts.Columns))
	for i, c := range opts.Columns {
		if actual, ok := colMapping[strings.ToLower(c)]; ok {
			mappedCols[i] = actual
		} else {
			mappedCols[i] = c
		}
	}

	mappedPKCols := make([]string, len(opts.PKColumns))
	for i, pk := range opts.PKColumns {
		if actual, ok := colMapping[strings.ToLower(pk)]; ok {
			mappedPKCols[i] = actual
		} else {
			mappedPKCols[i] = pk
		}
	}

	// Check for identity columns
	var hasIdentity bool
	identitySQL := `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM sys.columns c
			JOIN sys.tables t ON c.object_id = t.object_id
			JOIN sys.schemas s ON t.schema_id = s.schema_id
			WHERE s.name = @p1 AND t.name = @p2 AND c.is_identity = 1
		) THEN 1 ELSE 0 END`
	if err := conn.QueryRowContext(ctx, identitySQL, opts.Schema, opts.Table).Scan(&hasIdentity); err != nil {
		hasIdentity = false
	}

	// Bulk insert to staging
	if err := w.bulkInsertToTemp(ctx, conn, stagingTable, mappedCols, opts.ColumnTypes, opts.Rows); err != nil {
		return fmt.Errorf("bulk insert to staging: %w", err)
	}

	// Execute MERGE
	mergeSQL := w.buildMerge(targetTable, stagingTable, mappedCols, mappedPKCols, spatialCols, isCrossEngine, false)
	if err := w.executeMergeWithRetry(ctx, conn, targetTable, mergeSQL, hasIdentity, 5); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	return nil
}

// writeBatchIdempotent is the IdempotentOnDup path for WriteBatch (#227).
// Rows are bulk-loaded into a per-writer-per-partition temp staging table and
// merged into the target via an insert-only MERGE — replayed rows already
// present become silent no-ops without overwriting existing values.
//
// It reuses the staging/bulk-load/MERGE machinery from UpsertBatch but
// supplies its own spatial metadata path because WriteBatch does not pass
// ColumnSRIDs (UpsertBatch does). Spatial columns are still detected from
// the staging table once it exists.
func (w *Writer) writeBatchIdempotent(ctx context.Context, opts driver.WriteBatchOptions) error {
	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("IdempotentOnDup requires PKColumns to be set")
	}

	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Close()

	// Per-writer + per-partition staging name keeps concurrent writers and
	// partitions on the same connection pool from colliding.
	stagingTable := w.safeStagingName(opts.Table, opts.WriterID, opts.PartitionID)
	targetTable := w.dialect.QualifyTable(opts.Schema, opts.Table)

	createSQL := fmt.Sprintf(`SELECT TOP 0 * INTO %s FROM %s`, stagingTable, targetTable)
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("creating idempotent staging table: %w", err)
	}

	isCrossEngine := w.sourceType == "postgres"
	spatialCols, err := w.getSpatialColumns(ctx, conn, stagingTable)
	if err != nil {
		return fmt.Errorf("detecting spatial columns: %w", err)
	}
	if isCrossEngine && len(spatialCols) > 0 {
		if err := w.alterSpatialColumnsToText(ctx, conn, stagingTable, spatialCols); err != nil {
			return fmt.Errorf("altering spatial columns: %w", err)
		}
	}

	actualCols, err := w.getStagingTableColumns(ctx, conn, stagingTable)
	if err != nil {
		return fmt.Errorf("getting staging table columns: %w", err)
	}
	colMapping := make(map[string]string, len(actualCols))
	for _, ac := range actualCols {
		colMapping[strings.ToLower(ac)] = ac
	}
	mappedCols := make([]string, len(opts.Columns))
	for i, c := range opts.Columns {
		if actual, ok := colMapping[strings.ToLower(c)]; ok {
			mappedCols[i] = actual
		} else {
			mappedCols[i] = c
		}
	}
	mappedPKCols := make([]string, len(opts.PKColumns))
	for i, pk := range opts.PKColumns {
		if actual, ok := colMapping[strings.ToLower(pk)]; ok {
			mappedPKCols[i] = actual
		} else {
			mappedPKCols[i] = pk
		}
	}

	var hasIdentity bool
	identitySQL := `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM sys.columns c
			JOIN sys.tables t ON c.object_id = t.object_id
			JOIN sys.schemas s ON t.schema_id = s.schema_id
			WHERE s.name = @p1 AND t.name = @p2 AND c.is_identity = 1
		) THEN 1 ELSE 0 END`
	if err := conn.QueryRowContext(ctx, identitySQL, opts.Schema, opts.Table).Scan(&hasIdentity); err != nil {
		hasIdentity = false
	}

	if err := w.bulkInsertToTemp(ctx, conn, stagingTable, mappedCols, opts.ColumnTypes, opts.Rows); err != nil {
		return fmt.Errorf("bulk insert to idempotent staging: %w", err)
	}

	mergeSQL := w.buildMerge(targetTable, stagingTable, mappedCols, mappedPKCols, spatialCols, isCrossEngine, true)
	if err := w.executeMergeWithRetry(ctx, conn, targetTable, mergeSQL, hasIdentity, 5); err != nil {
		return fmt.Errorf("idempotent merge failed: %w", err)
	}
	return nil
}

func (w *Writer) safeStagingName(table string, writerID int, partitionID *int) string {
	suffix := fmt.Sprintf("_w%d", writerID)
	if partitionID != nil {
		suffix = fmt.Sprintf("_p%d%s", *partitionID, suffix)
	}
	base := fmt.Sprintf("#stg_%s", table)
	maxLen := 116

	if len(base)+len(suffix) > maxLen {
		hash := sha256.Sum256([]byte(table))
		base = fmt.Sprintf("#stg_%x", hash[:8])
	}
	return base + suffix
}

func (w *Writer) getStagingTableColumns(ctx context.Context, conn *sql.Conn, stagingTable string) ([]string, error) {
	query := `
		SELECT c.name
		FROM tempdb.sys.columns c
		WHERE c.object_id = OBJECT_ID('tempdb..' + @p1)
		ORDER BY c.column_id`

	rows, err := conn.QueryContext(ctx, query, stagingTable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		cols = append(cols, colName)
	}

	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns found for staging table %s", stagingTable)
	}

	return cols, nil
}

type spatialColumn struct {
	Name     string
	TypeName string
	SRID     int
}

func (w *Writer) getSpatialColumns(ctx context.Context, conn *sql.Conn, stagingTable string) ([]spatialColumn, error) {
	query := `
		SELECT c.name, t.name AS type_name
		FROM tempdb.sys.columns c
		JOIN tempdb.sys.types t ON c.user_type_id = t.user_type_id
		WHERE c.object_id = OBJECT_ID('tempdb..' + @p1)
		AND t.name IN ('geography', 'geometry')
		ORDER BY c.column_id`

	rows, err := conn.QueryContext(ctx, query, stagingTable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var spatialCols []spatialColumn
	for rows.Next() {
		var col spatialColumn
		if err := rows.Scan(&col.Name, &col.TypeName); err != nil {
			return nil, err
		}
		spatialCols = append(spatialCols, col)
	}

	return spatialCols, nil
}

func (w *Writer) alterSpatialColumnsToText(ctx context.Context, conn *sql.Conn, stagingTable string, spatialCols []spatialColumn) error {
	// SQL Server doesn't allow ALTER COLUMN from geography/geometry to nvarchar(max)
	// (implicit conversion not allowed). Instead, we DROP and ADD the column.
	for _, col := range spatialCols {
		quotedCol := w.dialect.QuoteIdentifier(col.Name)

		// Drop the geography/geometry column
		dropSQL := fmt.Sprintf(`ALTER TABLE %s DROP COLUMN %s`, stagingTable, quotedCol)
		if _, err := conn.ExecContext(ctx, dropSQL); err != nil {
			return fmt.Errorf("dropping column %s: %w", col.Name, err)
		}

		// Add it back as nvarchar(max) for WKT text data
		addSQL := fmt.Sprintf(`ALTER TABLE %s ADD %s nvarchar(max)`, stagingTable, quotedCol)
		if _, err := conn.ExecContext(ctx, addSQL); err != nil {
			return fmt.Errorf("adding column %s as nvarchar: %w", col.Name, err)
		}
	}
	return nil
}

func (w *Writer) bulkInsertToTemp(ctx context.Context, conn *sql.Conn, tempTable string, cols, colTypes []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, mssql.CopyIn(tempTable, mssql.BulkOptions{
		RowsPerBatch: w.defaultBatchSize,
	}, cols...))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, convertRowForBulkCopy(row, colTypes)...); err != nil {
			return err
		}
	}

	if _, err := stmt.ExecContext(ctx); err != nil {
		return err
	}

	return tx.Commit()
}

// buildMerge constructs the MERGE statement that drives both upsert and the
// resume-safe insert-only path (#227). When insertOnly is true, the
// WHEN MATCHED ... THEN UPDATE branch is omitted entirely — replayed rows
// already in the target become silent no-ops rather than being overwritten
// with potentially-changed source values.
func (w *Writer) buildMerge(targetTable, stagingTable string, cols, pkCols []string, spatialCols []spatialColumn, isCrossEngine bool, insertOnly bool) string {
	spatialMap := make(map[string]spatialColumn, len(spatialCols))
	for _, col := range spatialCols {
		spatialMap[strings.ToLower(col.Name)] = col
	}

	var onClauses []string
	for _, pk := range pkCols {
		onClauses = append(onClauses, fmt.Sprintf("target.%s = source.%s",
			w.dialect.QuoteIdentifier(pk), w.dialect.QuoteIdentifier(pk)))
	}

	pkSet := make(map[string]bool)
	for _, pk := range pkCols {
		pkSet[pk] = true
	}

	var setClauses []string
	var changeDetection []string
	for _, col := range cols {
		if !pkSet[col] {
			quotedCol := w.dialect.QuoteIdentifier(col)
			sourceExpr := fmt.Sprintf("source.%s", quotedCol)

			spatialCol, isSpatial := spatialMap[strings.ToLower(col)]
			if isSpatial && isCrossEngine {
				srid := spatialCol.SRID
				if srid == 0 {
					srid = 4326
				}
				sourceExpr = fmt.Sprintf("%s::STGeomFromText(source.%s, %d)", spatialCol.TypeName, quotedCol, srid)
			}

			setClauses = append(setClauses, fmt.Sprintf("%s = %s", quotedCol, sourceExpr))

			if isSpatial {
				continue
			}

			changeDetection = append(changeDetection, fmt.Sprintf(
				"(target.%s <> source.%s OR "+
					"(target.%s IS NULL AND source.%s IS NOT NULL) OR "+
					"(target.%s IS NOT NULL AND source.%s IS NULL))",
				quotedCol, quotedCol, quotedCol, quotedCol, quotedCol, quotedCol))
		}
	}

	quotedCols := make([]string, len(cols))
	sourceCols := make([]string, len(cols))
	for i, col := range cols {
		quotedCol := w.dialect.QuoteIdentifier(col)
		quotedCols[i] = quotedCol

		spatialCol, isSpatial := spatialMap[strings.ToLower(col)]
		if isSpatial && isCrossEngine {
			srid := spatialCol.SRID
			if srid == 0 {
				srid = 4326
			}
			sourceCols[i] = fmt.Sprintf("%s::STGeomFromText(source.%s, %d)", spatialCol.TypeName, quotedCol, srid)
		} else {
			sourceCols[i] = fmt.Sprintf("source.%s", quotedCol)
		}
	}

	var sb strings.Builder

	fmt.Fprintf(&sb, "MERGE INTO %s WITH (TABLOCK) AS target\n", targetTable)
	fmt.Fprintf(&sb, "USING %s AS source\n", stagingTable)
	fmt.Fprintf(&sb, "ON %s\n", strings.Join(onClauses, " AND "))

	if !insertOnly && len(setClauses) > 0 {
		fmt.Fprintf(&sb, "WHEN MATCHED AND (%s) THEN UPDATE SET %s\n",
			strings.Join(changeDetection, " OR "),
			strings.Join(setClauses, ", "))

	}

	fmt.Fprintf(&sb, "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
		strings.Join(quotedCols, ", "),
		strings.Join(sourceCols, ", "))

	return sb.String()
}

func (w *Writer) executeMergeWithRetry(ctx context.Context, conn *sql.Conn, targetTable, mergeSQL string, hasIdentity bool, maxRetries int) error {
	const baseDelayMs = 200

	for attempt := 1; attempt <= maxRetries; attempt++ {
		var err error

		if hasIdentity {
			if _, err = conn.ExecContext(ctx, fmt.Sprintf("SET IDENTITY_INSERT %s ON", targetTable)); err != nil {
				return fmt.Errorf("enabling identity insert: %w", err)
			}
			_, err = conn.ExecContext(ctx, mergeSQL)
			if _, disableErr := conn.ExecContext(ctx, fmt.Sprintf("SET IDENTITY_INSERT %s OFF", targetTable)); disableErr != nil {
				logging.Warn("Failed to disable IDENTITY_INSERT on %s: %v", targetTable, disableErr)
			}
		} else {
			_, err = conn.ExecContext(ctx, mergeSQL)
		}

		if err == nil {
			return nil
		}

		if !isDeadlockError(err) || attempt == maxRetries {
			return err
		}

		logging.Warn("Deadlock on %s, retry %d/%d", targetTable, attempt, maxRetries)
		time.Sleep(time.Duration(baseDelayMs*attempt) * time.Millisecond)
	}

	return fmt.Errorf("merge failed after %d retries", maxRetries)
}
