package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/util"
)

func (r *Reader) LoadIndexes(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			i.name AS index_name,
			i.is_unique,
			i.type_desc,
			STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns,
			ISNULL(STRING_AGG(CASE WHEN ic.is_included_column = 1 THEN c.name END, ',')
				WITHIN GROUP (ORDER BY ic.key_ordinal), '') AS include_columns
		FROM sys.indexes i
		JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		JOIN sys.tables tb ON i.object_id = tb.object_id
		JOIN sys.schemas s ON tb.schema_id = s.schema_id
		WHERE s.name = @schema
		  AND tb.name = @table
		  AND i.is_primary_key = 0
		  AND i.type > 0
		GROUP BY i.name, i.is_unique, i.type_desc
		ORDER BY i.name
	`, sql.Named("schema", t.Schema), sql.Named("table", t.Name))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var idx driver.Index
		var typeDesc, colsStr, includeStr string
		if err := rows.Scan(&idx.Name, &idx.IsUnique, &typeDesc, &colsStr, &includeStr); err != nil {
			return err
		}
		idx.IsClustered = typeDesc == "CLUSTERED"
		idx.Columns = util.SplitCSV(colsStr)
		if includeStr != "" {
			idx.IncludeCols = util.SplitCSV(includeStr)
		}
		t.Indexes = append(t.Indexes, idx)
	}

	return nil
}

// fkColumnDelimiter is used to separate column names in STRING_AGG.
// Using CHAR(1) (SOH) as it cannot appear in valid SQL Server identifiers.
const fkColumnDelimiter = "\x01"

// LoadForeignKeys loads all foreign keys for a table.
func (r *Reader) LoadForeignKeys(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			fk.name AS fk_name,
			STRING_AGG(c.name, CHAR(1)) WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS columns,
			rs.name AS ref_schema,
			rt.name AS ref_table,
			STRING_AGG(rc.name, CHAR(1)) WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ref_columns,
			CASE fk.delete_referential_action
				WHEN 0 THEN 'NO ACTION'
				WHEN 1 THEN 'CASCADE'
				WHEN 2 THEN 'SET NULL'
				WHEN 3 THEN 'SET DEFAULT'
			END AS delete_rule,
			CASE fk.update_referential_action
				WHEN 0 THEN 'NO ACTION'
				WHEN 1 THEN 'CASCADE'
				WHEN 2 THEN 'SET NULL'
				WHEN 3 THEN 'SET DEFAULT'
			END AS update_rule
		FROM sys.foreign_keys fk
		JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
		JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
		JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
		JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
		JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
		JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
		JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
		WHERE ps.name = @schema AND pt.name = @table
		GROUP BY fk.name, rs.name, rt.name, fk.delete_referential_action, fk.update_referential_action
		ORDER BY fk.name
	`, sql.Named("schema", t.Schema), sql.Named("table", t.Name))
	if err != nil {
		return fmt.Errorf("querying foreign keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fk driver.ForeignKey
		var columns, refColumns string
		if err := rows.Scan(&fk.Name, &columns, &fk.RefSchema, &fk.RefTable, &refColumns,
			&fk.OnDelete, &fk.OnUpdate); err != nil {
			return fmt.Errorf("scanning FK for %s.%s: %w", t.Schema, t.Name, err)
		}
		fk.Columns = strings.Split(columns, fkColumnDelimiter)
		fk.RefColumns = strings.Split(refColumns, fkColumnDelimiter)
		t.ForeignKeys = append(t.ForeignKeys, fk)
	}
	return rows.Err()
}

// LoadCheckConstraints loads all check constraints for a table.
func (r *Reader) LoadCheckConstraints(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			cc.name AS constraint_name,
			cc.definition
		FROM sys.check_constraints cc
		JOIN sys.tables t ON cc.parent_object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND t.name = @table
		ORDER BY cc.name
	`, sql.Named("schema", t.Schema), sql.Named("table", t.Name))
	if err != nil {
		return fmt.Errorf("querying check constraints: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var chk driver.CheckConstraint
		if err := rows.Scan(&chk.Name, &chk.Definition); err != nil {
			return fmt.Errorf("scanning check constraint for %s.%s: %w", t.Schema, t.Name, err)
		}
		t.CheckConstraints = append(t.CheckConstraints, chk)
	}
	return rows.Err()
}

// GetRowCount returns the row count for a table.
