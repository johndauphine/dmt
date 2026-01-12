package source

import (
	"context"
	"database/sql"
	"strings"

	"github.com/johndauphine/mssql-pg-migrate/internal/util"
)

// SchemaLoader provides unified schema loading functionality using SQL strategies.
// It abstracts the common schema extraction patterns shared between MSSQL and PostgreSQL pools.
type SchemaLoader struct {
	db       *sql.DB
	strategy SQLStrategy
}

// NewSchemaLoader creates a new SchemaLoader with the given database connection and strategy.
func NewSchemaLoader(db *sql.DB, strategy SQLStrategy) *SchemaLoader {
	return &SchemaLoader{
		db:       db,
		strategy: strategy,
	}
}

// LoadColumns loads column metadata for a table.
func (l *SchemaLoader) LoadColumns(ctx context.Context, t *Table) error {
	rows, err := l.db.QueryContext(ctx, l.strategy.GetColumnsQuery(), l.strategy.BindColumnParams(t.Schema, t.Name)...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var c Column
		if err := rows.Scan(&c.Name, &c.DataType, &c.MaxLength, &c.Precision,
			&c.Scale, &c.IsNullable, &c.IsIdentity, &c.OrdinalPos); err != nil {
			return err
		}
		t.Columns = append(t.Columns, c)
	}

	return rows.Err()
}

// LoadPrimaryKey loads primary key columns for a table.
func (l *SchemaLoader) LoadPrimaryKey(ctx context.Context, t *Table) error {
	rows, err := l.db.QueryContext(ctx, l.strategy.GetPrimaryKeyQuery(), l.strategy.BindPKParams(t.Schema, t.Name)...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return err
		}
		t.PrimaryKey = append(t.PrimaryKey, col)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Populate PKColumns with full column metadata
	t.PopulatePKColumns()

	return nil
}

// LoadForeignKeys loads foreign key constraints for a table.
func (l *SchemaLoader) LoadForeignKeys(ctx context.Context, t *Table) error {
	rows, err := l.db.QueryContext(ctx, l.strategy.GetForeignKeysQuery(), l.strategy.BindFKParams(t.Schema, t.Name)...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var fk ForeignKey
		var colsStr, refColsStr string
		if err := rows.Scan(&fk.Name, &colsStr, &fk.RefSchema, &fk.RefTable, &refColsStr, &fk.OnDelete, &fk.OnUpdate); err != nil {
			return err
		}
		fk.Columns = util.SplitCSV(colsStr)
		fk.RefColumns = util.SplitCSV(refColsStr)
		t.ForeignKeys = append(t.ForeignKeys, fk)
	}

	return rows.Err()
}

// LoadCheckConstraints loads check constraints for a table.
func (l *SchemaLoader) LoadCheckConstraints(ctx context.Context, t *Table) error {
	rows, err := l.db.QueryContext(ctx, l.strategy.GetCheckConstraintsQuery(), l.strategy.BindCheckParams(t.Schema, t.Name)...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var chk CheckConstraint
		if err := rows.Scan(&chk.Name, &chk.Definition); err != nil {
			return err
		}
		t.CheckConstraints = append(t.CheckConstraints, chk)
	}

	return rows.Err()
}

// GetDateColumnInfo checks if any candidate columns exist as a temporal type.
// Returns the first matching column name, its data type, and whether a match was found.
func (l *SchemaLoader) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	if len(candidates) == 0 {
		return "", "", false
	}

	for _, candidate := range candidates {
		var dt string
		err := l.db.QueryRowContext(ctx, l.strategy.GetDateColumnQuery(),
			l.strategy.BindDateColumnParams(schema, table, candidate)...).Scan(&dt)

		if err == nil && l.strategy.IsValidDateType(strings.ToLower(dt)) {
			return candidate, dt, true
		}
	}

	return "", "", false
}
