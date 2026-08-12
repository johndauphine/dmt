package generic

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/logging"
)

// SQL Server strategies (#509), extracted from the hand-written mssql
// writer. WriteBatch uses the TDS bulk-copy protocol through the native
// *mssql.Conn (no TABLOCK — parallel BCP writers per table); upsert and
// idempotent replay stage into a #temp table and MERGE with deadlock
// retry, IDENTITY_INSERT handling, and cross-engine spatial conversion.

func init() {
	bulkStrategies["mssql_bcp"] = mssqlBulkWrite
	upsertStrategies["mssql_merge"] = mssqlMergeUpsert
	sequenceStrategies["mssql_checkident"] = mssqlCheckidentReset
}

// mssqlCheckidentReset reseeds the first identity column to MAX(col)
// via DBCC CHECKIDENT (moved verbatim from the hand-written writer).
func mssqlCheckidentReset(ctx context.Context, db *sql.DB, dialect *Dialect, schema string, t *driver.Table) error {
	var identityCol string
	for _, c := range t.Columns {
		if c.IsIdentity {
			identityCol = c.Name
			break
		}
	}
	if identityCol == "" {
		return nil
	}

	var maxVal int64
	err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s",
			dialect.QuoteIdentifier(identityCol), dialect.QualifyTable(schema, t.Name))).Scan(&maxVal)
	if err != nil {
		return fmt.Errorf("getting max value for %s.%s: %w", t.Name, identityCol, err)
	}
	if maxVal == 0 {
		return nil
	}

	_, err = db.ExecContext(ctx, mssqlCheckidentStatement(dialect, schema, t.Name, maxVal))
	return err
}

// mssqlCheckidentStatement builds the DBCC CHECKIDENT reseed statement. The
// table name is passed to DBCC as a single-quoted *string literal*, so the
// bracket-quoted identifier must additionally have its single quotes doubled.
// Without this, a legal SQL Server name containing an apostrophe (e.g.
// [It's Data]) breaks TaskResetSequences, and a hostile source table name
// could terminate the literal and inject SQL into the privileged target
// session (#548).
func mssqlCheckidentStatement(dialect *Dialect, schema, table string, maxVal int64) string {
	literal := strings.ReplaceAll(dialect.QualifyTable(schema, table), "'", "''")
	return fmt.Sprintf("DBCC CHECKIDENT ('%s', RESEED, %d)", literal, maxVal)
}

// convertRowForBulkCopy normalizes []byte values for the TDS protocol:
// text-typed columns and ASCII-numeric byte strings become Go strings.
func convertRowForBulkCopy(row []any, columnTypes []string) []any {
	result := make([]any, len(row))
	for i, v := range row {
		if b, ok := v.([]byte); ok {
			if isTextColumn(columnTypes, i) && utf8.Valid(b) {
				result[i] = string(b)
			} else if !isBinaryColumn(columnTypes, i) && isASCIINumeric(b) {
				// Numeric values arrive as []byte from some source
				// drivers; binary columns must stay []byte — go-mssqldb
				// bulk copy rejects strings there, and a varbinary
				// payload of ASCII digits is still binary (codex).
				result[i] = string(b)
			} else {
				result[i] = v
			}
		} else {
			result[i] = v
		}
	}
	return result
}

func isBinaryColumn(columnTypes []string, index int) bool {
	if index >= len(columnTypes) {
		return false
	}
	switch normalizeColumnType(columnTypes[index]) {
	case "binary", "varbinary", "image", "blob", "tinyblob", "mediumblob",
		"longblob", "bytea", "rowversion", "timestamp":
		return true
	default:
		return false
	}
}

func isTextColumn(columnTypes []string, index int) bool {
	if index >= len(columnTypes) {
		return false
	}
	switch normalizeColumnType(columnTypes[index]) {
	case "char", "varchar", "text", "tinytext", "mediumtext", "longtext",
		"nchar", "nvarchar", "ntext", "string", "uniqueidentifier", "uuid",
		"json", "jsonb", "xml", "enum", "set":
		return true
	default:
		return false
	}
}

func normalizeColumnType(columnType string) string {
	columnType = strings.ToLower(strings.TrimSpace(columnType))
	if idx := strings.IndexAny(columnType, "( \t\r\n"); idx >= 0 {
		columnType = columnType[:idx]
	}
	return columnType
}

func isASCIINumeric(b []byte) bool {
	if len(b) == 0 {
		return false
	}

	hasDigit := false
	hasDot := false
	hasE := false
	i := 0

	if b[i] == '+' || b[i] == '-' {
		i++
		if i >= len(b) {
			return false
		}
	}

	for i < len(b) {
		c := b[i]
		switch {
		case c >= '0' && c <= '9':
			hasDigit = true
		case c == '.':
			if hasDot || hasE {
				return false
			}
			hasDot = true
		case c == 'E' || c == 'e':
			if hasE || !hasDigit {
				return false
			}
			hasE = true
			i++
			if i < len(b) && (b[i] == '+' || b[i] == '-') {
				i++
			}
			if i >= len(b) || b[i] < '0' || b[i] > '9' {
				return false
			}
			continue
		default:
			return false
		}
		i++
	}

	return hasDigit
}

func isDeadlockError(err error) bool {
	if err == nil {
		return false
	}
	if mssqlErr, ok := err.(interface{ SQLErrorNumber() int32 }); ok {
		return mssqlErr.SQLErrorNumber() == 1205
	}
	errStr := err.Error()
	return strings.Contains(errStr, "deadlock") || strings.Contains(errStr, "1205")
}

// mssqlBulkWrite is the mssql_bcp bulk strategy: TDS bulk copy inside
// one transaction, sub-batched so the session buffer stays bounded.
// IdempotentOnDup routes to the staging + insert-only MERGE variant.
func mssqlBulkWrite(ctx context.Context, env bulkEnv, opts driver.WriteBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}
	if opts.IdempotentOnDup {
		return mssqlWriteBatchIdempotent(ctx, env, opts)
	}

	fullTableName := env.dialect.QualifyTable(opts.Schema, opts.Table)

	conn, err := env.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("getting connection: %w", err)
	}
	defer conn.Close()

	// Bounded waits on row/page locks during parallel bulk inserts —
	// without this, concurrent writers can block each other
	// indefinitely. 5 minutes is generous for bulk operations.
	if _, err := conn.ExecContext(ctx, "SET LOCK_TIMEOUT 300000"); err != nil {
		return fmt.Errorf("setting lock timeout: %w", err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	err = conn.Raw(func(driverConn any) error {
		mssqlConn, ok := driverConn.(*mssql.Conn)
		if !ok {
			return fmt.Errorf("mssql_bcp requires the sqlserver backend (got %T)", driverConn)
		}

		// Sub-batch rows to avoid accumulating too much data in the TDS
		// session buffer between CreateBulkContext and Done().
		batchRows := opts.BatchSize
		if batchRows <= 0 {
			batchRows = env.batchSize
		}
		if batchRows <= 0 {
			return fmt.Errorf("batch size not configured: set chunk_size in config or enable AI tuning")
		}
		for start := 0; start < len(opts.Rows); start += batchRows {
			end := start + batchRows
			if end > len(opts.Rows) {
				end = len(opts.Rows)
			}
			subBatch := opts.Rows[start:end]

			bulk := mssqlConn.CreateBulkContext(ctx, fullTableName, opts.Columns)
			// No TABLOCK — enables parallel BCP writers per table.
			// TABLOCK serializes writes but enables minimal logging.
			bulk.Options.Tablock = false
			// KEEP_NULLS: without it SQL Server replaces source NULLs
			// with the target column's DEFAULT during bulk load. The
			// hand-written writer omitted this — a silent data change
			// on defaulted columns (codex on #509).
			bulk.Options.KeepNulls = true
			bulk.Options.RowsPerBatch = len(subBatch)
			if len(opts.OrderColumns) > 0 {
				orderHints := make([]string, len(opts.OrderColumns))
				for i, col := range opts.OrderColumns {
					orderHints[i] = fmt.Sprintf("%s ASC", env.dialect.QuoteIdentifier(col))
				}
				bulk.Options.Order = orderHints
			}

			for _, row := range subBatch {
				if err := bulk.AddRow(convertRowForBulkCopy(row, opts.ColumnTypes)); err != nil {
					return fmt.Errorf("adding row: %w", err)
				}
			}

			rowsAffected, err := bulk.Done()
			if err != nil {
				return fmt.Errorf("finalizing bulk insert: %w", err)
			}
			if rowsAffected != int64(len(subBatch)) {
				return fmt.Errorf("bulk insert: expected %d rows, got %d", len(subBatch), rowsAffected)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("bulk copy: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("committing bulk copy: %w", err)
	}
	return nil
}

// mssqlMergeUpsert is the mssql_merge upsert strategy: stage into a
// #temp table via BCP, then MERGE with change detection.
func mssqlMergeUpsert(ctx context.Context, env bulkEnv, opts driver.UpsertBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}
	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("upsert requires primary key columns")
	}

	conn, err := env.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Close()

	stagingTable := mssqlSafeStagingName(opts.Table, opts.WriterID, nil)
	targetTable := env.dialect.QualifyTable(opts.Schema, opts.Table)

	createSQL := fmt.Sprintf(`SELECT TOP 0 * INTO %s FROM %s`, stagingTable, targetTable)
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("creating staging table: %w", err)
	}

	// Every non-mssql source emits spatial values as WKT text
	// (spatial_select wrapping) — the hand-written writer only
	// recognized postgres, breaking mysql→mssql spatial upserts
	// (codex on #509).
	isCrossEngine := env.sourceType != "mssql"
	spatialCols, err := mssqlGetSpatialColumns(ctx, conn, stagingTable)
	if err != nil {
		return fmt.Errorf("detecting spatial columns: %w", err)
	}

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

	if isCrossEngine && len(spatialCols) > 0 {
		if err := mssqlAlterSpatialColumnsToText(ctx, conn, env.dialect, stagingTable, spatialCols); err != nil {
			return fmt.Errorf("altering spatial columns: %w", err)
		}
	}

	mappedCols, mappedPKCols, err := mssqlMapStagingColumns(ctx, conn, stagingTable, opts.Columns, opts.PKColumns)
	if err != nil {
		return err
	}

	hasIdentity := mssqlHasIdentity(ctx, conn, opts.Schema, opts.Table)

	stagingRows := opts.Rows
	if isCrossEngine && len(spatialCols) > 0 {
		stagingRows = mssqlConvertSpatialWKT(opts.Rows, mappedCols, spatialCols)
	}
	if err := mssqlBulkInsertToTemp(ctx, conn, stagingTable, mappedCols, opts.ColumnTypes, stagingRows, env.batchSize); err != nil {
		return fmt.Errorf("bulk insert to staging: %w", err)
	}

	mergeSQL := mssqlBuildMerge(env.dialect, targetTable, stagingTable, mappedCols, mappedPKCols, spatialCols, isCrossEngine, false)
	if err := mssqlExecuteMergeWithRetry(ctx, conn, targetTable, mergeSQL, hasIdentity, 5); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}
	return nil
}

// mssqlWriteBatchIdempotent is the IdempotentOnDup path (#227): rows
// stage into a per-writer-per-partition #temp table and merge via an
// insert-only MERGE — replayed rows already present become silent
// no-ops without overwriting existing values.
func mssqlWriteBatchIdempotent(ctx context.Context, env bulkEnv, opts driver.WriteBatchOptions) error {
	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("IdempotentOnDup requires PKColumns to be set")
	}

	conn, err := env.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Close()

	stagingTable := mssqlSafeStagingName(opts.Table, opts.WriterID, opts.PartitionID)
	targetTable := env.dialect.QualifyTable(opts.Schema, opts.Table)

	createSQL := fmt.Sprintf(`SELECT TOP 0 * INTO %s FROM %s`, stagingTable, targetTable)
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("creating idempotent staging table: %w", err)
	}

	isCrossEngine := env.sourceType != "mssql"
	spatialCols, err := mssqlGetSpatialColumns(ctx, conn, stagingTable)
	if err != nil {
		return fmt.Errorf("detecting spatial columns: %w", err)
	}
	if isCrossEngine && len(spatialCols) > 0 {
		if err := mssqlAlterSpatialColumnsToText(ctx, conn, env.dialect, stagingTable, spatialCols); err != nil {
			return fmt.Errorf("altering spatial columns: %w", err)
		}
	}

	mappedCols, mappedPKCols, err := mssqlMapStagingColumns(ctx, conn, stagingTable, opts.Columns, opts.PKColumns)
	if err != nil {
		return err
	}

	hasIdentity := mssqlHasIdentity(ctx, conn, opts.Schema, opts.Table)

	stagingRows := opts.Rows
	if isCrossEngine && len(spatialCols) > 0 {
		stagingRows = mssqlConvertSpatialWKT(opts.Rows, mappedCols, spatialCols)
	}
	if err := mssqlBulkInsertToTemp(ctx, conn, stagingTable, mappedCols, opts.ColumnTypes, stagingRows, env.batchSize); err != nil {
		return fmt.Errorf("bulk insert to idempotent staging: %w", err)
	}

	mergeSQL := mssqlBuildMerge(env.dialect, targetTable, stagingTable, mappedCols, mappedPKCols, spatialCols, isCrossEngine, true)
	if err := mssqlExecuteMergeWithRetry(ctx, conn, targetTable, mergeSQL, hasIdentity, 5); err != nil {
		return fmt.Errorf("idempotent merge failed: %w", err)
	}
	return nil
}

// mssqlSafeStagingName builds the per-writer (and per-partition) #temp
// staging name, hashing long table names under the 116-char tempdb cap.
func mssqlSafeStagingName(table string, writerID int, partitionID *int) string {
	suffix := fmt.Sprintf("_w%d", writerID)
	if partitionID != nil {
		suffix = fmt.Sprintf("_p%d%s", *partitionID, suffix)
	}
	// The staging name is interpolated unquoted into SELECT INTO /
	// ALTER TABLE / CopyIn, so it must never need quoting: anything
	// outside [A-Za-z0-9_] becomes _ ("Order Details" → Order_Details).
	// The hand-written writer embedded the raw name, which broke
	// upserts for quoted-identifier tables (codex on #509).
	sanitized := make([]byte, 0, len(table))
	for i := 0; i < len(table); i++ {
		c := table[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_':
			sanitized = append(sanitized, c)
		default:
			sanitized = append(sanitized, '_')
		}
	}
	base := fmt.Sprintf("#stg_%s", sanitized)
	maxLen := 116

	if len(base)+len(suffix) > maxLen {
		hash := sha256.Sum256([]byte(table))
		base = fmt.Sprintf("#stg_%x", hash[:8])
	}
	return base + suffix
}

// mssqlConvertSpatialWKT returns rows with []byte values in spatial
// columns converted to strings: the cross-engine staging path re-types
// those columns to nvarchar(max) and source WKT commonly scans as
// []byte, which go-mssqldb would write as raw bytes into the NVARCHAR
// buffer (codex on #509). Native mssql→mssql copies never call this —
// their spatial values must stay binary.
func mssqlConvertSpatialWKT(rows [][]any, cols []string, spatialCols []spatialColumn) [][]any {
	sp := make(map[string]bool, len(spatialCols))
	for _, c := range spatialCols {
		sp[strings.ToLower(c.Name)] = true
	}
	var idx []int
	for i, c := range cols {
		if sp[strings.ToLower(c)] {
			idx = append(idx, i)
		}
	}
	if len(idx) == 0 {
		return rows
	}
	out := make([][]any, len(rows))
	for r, row := range rows {
		nr := make([]any, len(row))
		copy(nr, row)
		for _, i := range idx {
			if i < len(nr) {
				if b, ok := nr[i].([]byte); ok && utf8.Valid(b) {
					nr[i] = string(b)
				}
			}
		}
		out[r] = nr
	}
	return out
}

// mssqlMapStagingColumns resolves the requested column names against
// the staging table's actual (case-exact) names.
func mssqlMapStagingColumns(ctx context.Context, conn *sql.Conn, stagingTable string, cols, pkCols []string) ([]string, []string, error) {
	actualCols, err := mssqlGetStagingTableColumns(ctx, conn, stagingTable)
	if err != nil {
		return nil, nil, fmt.Errorf("getting staging table columns: %w", err)
	}
	colMapping := make(map[string]string, len(actualCols))
	for _, ac := range actualCols {
		colMapping[strings.ToLower(ac)] = ac
	}
	mapName := func(c string) string {
		if actual, ok := colMapping[strings.ToLower(c)]; ok {
			return actual
		}
		return c
	}
	mappedCols := make([]string, len(cols))
	for i, c := range cols {
		mappedCols[i] = mapName(c)
	}
	mappedPKCols := make([]string, len(pkCols))
	for i, pk := range pkCols {
		mappedPKCols[i] = mapName(pk)
	}
	return mappedCols, mappedPKCols, nil
}

func mssqlGetStagingTableColumns(ctx context.Context, conn *sql.Conn, stagingTable string) ([]string, error) {
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
	// A result set can fail after Next() returns false (mid-set driver/network
	// error or context cancellation); without this the loop ends early and
	// silently returns a truncated column list (#567).
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns found for staging table %s", stagingTable)
	}
	return cols, nil
}

func mssqlHasIdentity(ctx context.Context, conn *sql.Conn, schema, table string) bool {
	var hasIdentity bool
	identitySQL := `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM sys.columns c
			JOIN sys.tables t ON c.object_id = t.object_id
			JOIN sys.schemas s ON t.schema_id = s.schema_id
			WHERE s.name = @p1 AND t.name = @p2 AND c.is_identity = 1
		) THEN 1 ELSE 0 END`
	if err := conn.QueryRowContext(ctx, identitySQL, schema, table).Scan(&hasIdentity); err != nil {
		return false
	}
	return hasIdentity
}

type spatialColumn struct {
	Name     string
	TypeName string
	SRID     int
}

func mssqlGetSpatialColumns(ctx context.Context, conn *sql.Conn, stagingTable string) ([]spatialColumn, error) {
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
	// Surface a mid-set read error instead of returning a partial list — a
	// missed spatial column would never be re-typed to nvarchar(max) before
	// staging, misattributing the later failure (#567).
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return spatialCols, nil
}

// mssqlAlterSpatialColumnsToText converts staging spatial columns to
// nvarchar(max) for cross-engine WKT data. SQL Server doesn't allow
// ALTER COLUMN from geography/geometry (implicit conversion), so the
// column is dropped and re-added.
func mssqlAlterSpatialColumnsToText(ctx context.Context, conn *sql.Conn, dialect *Dialect, stagingTable string, spatialCols []spatialColumn) error {
	for _, col := range spatialCols {
		quotedCol := dialect.QuoteIdentifier(col.Name)

		dropSQL := fmt.Sprintf(`ALTER TABLE %s DROP COLUMN %s`, stagingTable, quotedCol)
		if _, err := conn.ExecContext(ctx, dropSQL); err != nil {
			return fmt.Errorf("dropping column %s: %w", col.Name, err)
		}

		addSQL := fmt.Sprintf(`ALTER TABLE %s ADD %s nvarchar(max)`, stagingTable, quotedCol)
		if _, err := conn.ExecContext(ctx, addSQL); err != nil {
			return fmt.Errorf("adding column %s as nvarchar: %w", col.Name, err)
		}
	}
	return nil
}

func mssqlBulkInsertToTemp(ctx context.Context, conn *sql.Conn, tempTable string, cols, colTypes []string, rows [][]any, batchSize int) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, mssql.CopyIn(tempTable, mssql.BulkOptions{
		RowsPerBatch: batchSize,
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

// mssqlBuildMerge constructs the MERGE driving both upsert and the
// resume-safe insert-only path (#227). When insertOnly is true the
// WHEN MATCHED branch is omitted entirely — replayed rows already in
// the target become silent no-ops rather than absorbing changed source
// values.
func mssqlBuildMerge(dialect *Dialect, targetTable, stagingTable string, cols, pkCols []string, spatialCols []spatialColumn, isCrossEngine bool, insertOnly bool) string {
	spatialMap := make(map[string]spatialColumn, len(spatialCols))
	for _, col := range spatialCols {
		spatialMap[strings.ToLower(col.Name)] = col
	}

	var onClauses []string
	for _, pk := range pkCols {
		onClauses = append(onClauses, fmt.Sprintf("target.%s = source.%s",
			dialect.QuoteIdentifier(pk), dialect.QuoteIdentifier(pk)))
	}

	pkSet := make(map[string]bool)
	for _, pk := range pkCols {
		pkSet[pk] = true
	}

	var setClauses []string
	var changeDetection []string
	for _, col := range cols {
		if !pkSet[col] {
			quotedCol := dialect.QuoteIdentifier(col)
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

			// Compare byte-exact via VARBINARY. A plain `<>` uses the target
			// column's collation, so under SQL Server's default
			// case-insensitive, ANSI-padded collation ('smith' = 'Smith',
			// 'a' = 'a ') a case-only or trailing-space change is seen as "no
			// change" and WHEN MATCHED is skipped — the target silently keeps
			// the stale value. CONVERT(VARBINARY(MAX), ...) is collation- and
			// padding-independent and valid for every scalar type `<>` already
			// supported (spatial columns are skipped above). NULL transitions
			// are still handled by the IS NULL / IS NOT NULL disjuncts (#547).
			changeDetection = append(changeDetection, fmt.Sprintf(
				"(CONVERT(VARBINARY(MAX), target.%s) <> CONVERT(VARBINARY(MAX), source.%s) OR "+
					"(target.%s IS NULL AND source.%s IS NOT NULL) OR "+
					"(target.%s IS NOT NULL AND source.%s IS NULL))",
				quotedCol, quotedCol, quotedCol, quotedCol, quotedCol, quotedCol))
		}
	}

	quotedCols := make([]string, len(cols))
	sourceCols := make([]string, len(cols))
	for i, col := range cols {
		quotedCol := dialect.QuoteIdentifier(col)
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
		// The change-detection predicate cannot cover spatial columns
		// (no = operator), so it is only safe when every mutable
		// column participates — otherwise a spatial-only change would
		// skip the WHEN MATCHED branch and leave the value stale. The
		// hand-written writer also rendered an empty "AND ()" syntax
		// error for all-spatial tables (codex on #509).
		if len(spatialCols) == 0 && len(changeDetection) > 0 {
			fmt.Fprintf(&sb, "WHEN MATCHED AND (%s) THEN UPDATE SET %s\n",
				strings.Join(changeDetection, " OR "),
				strings.Join(setClauses, ", "))
		} else {
			fmt.Fprintf(&sb, "WHEN MATCHED THEN UPDATE SET %s\n",
				strings.Join(setClauses, ", "))
		}
	}

	fmt.Fprintf(&sb, "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
		strings.Join(quotedCols, ", "),
		strings.Join(sourceCols, ", "))

	return sb.String()
}

func mssqlExecuteMergeWithRetry(ctx context.Context, conn *sql.Conn, targetTable, mergeSQL string, hasIdentity bool, maxRetries int) error {
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
