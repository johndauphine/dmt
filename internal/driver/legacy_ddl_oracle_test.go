package driver

import "github.com/johndauphine/dmt/internal/typemap/ddl"

// These adapters exist only for the pre-SMT parity oracle. Production target
// schema DDL does not import internal/typemap/ddl.
func driverColumnToDDL(col Column, targetDialect string) ddl.Column {
	fullType := col.FullDataType
	if fullType == "" {
		fullType = col.DataType
	}
	return ddl.Column{
		Name:                   sanitizeForTarget(col.Name, targetDialect),
		UDTName:                col.DataType,
		DataType:               fullType,
		CharacterMaximumLength: nullableInt(col.MaxLength),
		NumericPrecision:       nullableInt(col.Precision),
		NumericScale:           nullableInt(col.Scale),
		IsNullable:             col.IsNullable,
		IsIdentity:             col.IsIdentity,
	}
}

func driverTableToDDL(t *Table, targetSchema, targetDialect string) ddl.TableInfo {
	columns := make([]ddl.Column, len(t.Columns))
	for i, column := range t.Columns {
		columns[i] = driverColumnToDDL(column, targetDialect)
	}

	tableName := sanitizeForTarget(t.Name, targetDialect)
	var constraints []ddl.Constraint
	if len(t.PrimaryKey) > 0 {
		primaryKey := make([]string, len(t.PrimaryKey))
		for i, column := range t.PrimaryKey {
			primaryKey[i] = sanitizeForTarget(column, targetDialect)
		}
		constraints = append(constraints, ddl.Constraint{
			Name:    "pk_" + tableName,
			Type:    ddl.ConstraintPrimaryKey,
			Columns: primaryKey,
		})
	}

	return ddl.TableInfo{
		Schema:      sanitizeForTarget(targetSchema, targetDialect),
		Name:        tableName,
		Columns:     columns,
		Constraints: constraints,
	}
}
