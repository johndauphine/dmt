package generic

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
)

// The bulk-strategy library (#191): named Go implementations of the
// genuinely imperative write paths, extracted from the hand-written
// writers. Catalogs select by name; phase-3 conversions add copy_from
// (postgres), bcp (mssql), and load_data (mysql) here so the fast
// paths stay shared Go, not templated SQL.

// bulkEnv is what a strategy needs from the writer: the open pool, the
// rendered identifiers, and the catalog's bind-variable budget.
type bulkEnv struct {
	db             *sql.DB
	dialect        *Dialect
	batchSize      int // resolved default; opts.BatchSize overrides
	maxVars        int // engine bind-variable ceiling; 0 = unlimited
	convert        func([]any) []any
	idempotentVerb string
	engine         string
}

type bulkWriteFunc func(ctx context.Context, env bulkEnv, opts driver.WriteBatchOptions) error

type upsertFunc func(ctx context.Context, env bulkEnv, opts driver.UpsertBatchOptions) error

var bulkStrategies = map[string]bulkWriteFunc{
	"batched_insert": batchedInsert,
}

var upsertStrategies = map[string]upsertFunc{
	"insert_on_conflict": insertOnConflict,
}

// rowConverters normalize Go values for the engine's bind layer
// (audit: strategy-selected). bool_to_int is SQLite's convention.
var rowConverters = map[string]func([]any) []any{
	"bool_to_int": boolToInt,
}

func boolToInt(row []any) []any {
	result := make([]any, len(row))
	for i, v := range row {
		switch val := v.(type) {
		case bool:
			if val {
				result[i] = 1
			} else {
				result[i] = 0
			}
		default:
			result[i] = v
		}
	}
	return result
}

// resolveBatchSize applies the per-call override, the writer default,
// and the engine's bind-variable ceiling — extracted verbatim from the
// hand-written sqlite writer.
func resolveBatchSize(env bulkEnv, requested, numCols int) int {
	batchSize := requested
	if batchSize <= 0 {
		batchSize = env.batchSize
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	if env.maxVars > 0 && numCols > 0 {
		maxRowsByVars := env.maxVars / numCols
		if maxRowsByVars < 1 {
			maxRowsByVars = 1
		}
		if batchSize > maxRowsByVars {
			batchSize = maxRowsByVars
		}
	}
	return batchSize
}

// batchedInsert writes rows with multi-row INSERTs inside one
// transaction. IdempotentOnDup uses INSERT OR IGNORE (#227 resume
// semantics: an already-present row is a silent no-op, not an upsert).
func batchedInsert(ctx context.Context, env bulkEnv, opts driver.WriteBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	colList := env.dialect.ColumnList(opts.Columns)
	fullTable := env.dialect.QualifyTable(opts.Schema, opts.Table)

	verb := "INSERT INTO"
	if opts.IdempotentOnDup {
		if len(opts.PKColumns) == 0 {
			return fmt.Errorf("IdempotentOnDup requires PKColumns to be set")
		}
		if env.idempotentVerb == "" {
			// Refuse rather than emit another engine's syntax (codex
			// on #507): a ROW_NUMBER resume that can't dedupe must
			// fail loudly so the operator restarts the table.
			return fmt.Errorf("%s has no duplicate-skipping INSERT verb; idempotent replay is not supported — restart the table instead of resuming", env.engine)
		}
		verb = env.idempotentVerb
	}

	batchSize := resolveBatchSize(env, opts.BatchSize, len(opts.Columns))

	tx, err := env.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		batch := opts.Rows[start:end]
		query := fmt.Sprintf("%s %s (%s) VALUES %s",
			verb, fullTable, colList, valuesPlaceholders(len(opts.Columns), len(batch)))
		args := make([]any, 0, len(batch)*len(opts.Columns))
		for _, row := range batch {
			args = append(args, env.convert(row)...)
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("inserting batch into %s: %w", opts.Table, err)
		}
	}

	return tx.Commit()
}

// insertOnConflict upserts via INSERT ... ON CONFLICT (pk) DO UPDATE
// SET col = excluded.col — the SQLite/PostgreSQL-shared UPSERT shape.
// A PK-only table degrades to DO NOTHING.
func insertOnConflict(ctx context.Context, env bulkEnv, opts driver.UpsertBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}
	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("upsert requires primary key columns")
	}

	colList := env.dialect.ColumnList(opts.Columns)
	fullTable := env.dialect.QualifyTable(opts.Schema, opts.Table)

	pkSet := make(map[string]bool)
	pkQuoted := make([]string, len(opts.PKColumns))
	for i, pk := range opts.PKColumns {
		pkSet[strings.ToLower(pk)] = true
		pkQuoted[i] = env.dialect.QuoteIdentifier(pk)
	}

	var updateClauses []string
	for _, col := range opts.Columns {
		if pkSet[strings.ToLower(col)] {
			continue
		}
		qCol := env.dialect.QuoteIdentifier(col)
		updateClauses = append(updateClauses, fmt.Sprintf("%s = excluded.%s", qCol, qCol))
	}

	var upsertSuffix string
	if len(updateClauses) > 0 {
		upsertSuffix = fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s",
			strings.Join(pkQuoted, ", "), strings.Join(updateClauses, ", "))
	} else {
		upsertSuffix = fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", strings.Join(pkQuoted, ", "))
	}

	batchSize := resolveBatchSize(env, opts.BatchSize, len(opts.Columns))

	tx, err := env.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		batch := opts.Rows[start:end]
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s%s",
			fullTable, colList, valuesPlaceholders(len(opts.Columns), len(batch)), upsertSuffix)
		args := make([]any, 0, len(batch)*len(opts.Columns))
		for _, row := range batch {
			args = append(args, env.convert(row)...)
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("upserting batch into %s: %w", opts.Table, err)
		}
	}

	return tx.Commit()
}

func valuesPlaceholders(numCols, numRows int) string {
	placeholders := make([]string, numCols)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	rowPlaceholder := "(" + strings.Join(placeholders, ", ") + ")"
	rows := make([]string, numRows)
	for i := range rows {
		rows[i] = rowPlaceholder
	}
	return strings.Join(rows, ", ")
}

// sequenceFunc resets the target's identity counter after transfer
// (imperative per engine; capability SequenceResetter).
type sequenceFunc func(ctx context.Context, db *sql.DB, dialect *Dialect, schema string, t *driver.Table) error

var sequenceStrategies = map[string]sequenceFunc{
	"sqlite_sequence": sqliteSequenceReset,
}

// sqliteSequenceReset mirrors the hand-written writer: sqlite_sequence
// is created lazily, so update-then-insert.
func sqliteSequenceReset(ctx context.Context, db *sql.DB, dialect *Dialect, schema string, t *driver.Table) error {
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

	var maxVal sql.NullInt64
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT MAX(%s) FROM %s",
			dialect.QuoteIdentifier(identityCol),
			dialect.QualifyTable(schema, t.Name))).Scan(&maxVal); err != nil {
		return fmt.Errorf("getting max %s.%s: %w", t.Name, identityCol, err)
	}
	if !maxVal.Valid || maxVal.Int64 == 0 {
		return nil
	}

	res, err := db.ExecContext(ctx,
		`UPDATE sqlite_sequence SET seq = ? WHERE name = ?`, maxVal.Int64, t.Name)
	if err != nil {
		return fmt.Errorf("resetting sequence for %s: %w", t.Name, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		_, err = db.ExecContext(ctx,
			`INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)`, t.Name, maxVal.Int64)
		if err != nil {
			return fmt.Errorf("seeding sequence for %s: %w", t.Name, err)
		}
	}
	return nil
}
