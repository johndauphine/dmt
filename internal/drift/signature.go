package drift

import (
	"fmt"
	"strings"
)

func sameType(previous, current ColumnSnapshot) bool {
	return canonicalType(previous.DataType) == canonicalType(current.DataType) &&
		previous.MaxLength == current.MaxLength &&
		previous.Precision == current.Precision &&
		previous.Scale == current.Scale &&
		previous.SRID == current.SRID
}

func classifyTypeChange(previous, current ColumnSnapshot) ChangeKind {
	prevType := canonicalType(previous.DataType)
	currType := canonicalType(current.DataType)

	if prevRank, ok := integerRank(prevType); ok {
		if currRank, ok := integerRank(currType); ok {
			switch {
			case currRank > prevRank:
				return TypeWidened
			case currRank < prevRank:
				return TypeNarrowed
			default:
				return TypeChangedLossy
			}
		}
	}

	if sameFamily(prevType, currType, isStringType) || sameFamily(prevType, currType, isBinaryType) {
		return classifyLengthChange(previous.MaxLength, current.MaxLength)
	}
	if prevRank, ok := floatRank(prevType); ok {
		if currRank, ok := floatRank(currType); ok {
			switch {
			case currRank > prevRank:
				return TypeWidened
			case currRank < prevRank:
				return TypeNarrowed
			default:
				return TypeChangedLossy
			}
		}
	}

	if isDecimalType(prevType) && isDecimalType(currType) {
		prevIntegerDigits := previous.Precision - previous.Scale
		currIntegerDigits := current.Precision - current.Scale
		switch {
		case currIntegerDigits >= prevIntegerDigits && current.Scale >= previous.Scale:
			return TypeWidened
		case currIntegerDigits <= prevIntegerDigits && current.Scale <= previous.Scale:
			return TypeNarrowed
		default:
			return TypeChangedLossy
		}
	}

	if prevType == currType {
		switch {
		case current.Precision > previous.Precision || current.Scale > previous.Scale:
			return TypeWidened
		case current.Precision < previous.Precision || current.Scale < previous.Scale:
			return TypeNarrowed
		default:
			return TypeChangedLossy
		}
	}

	return TypeChangedLossy
}

func classifyLengthChange(previous, current int) ChangeKind {
	prevLen := lengthRank(previous)
	currLen := lengthRank(current)
	switch {
	case currLen > prevLen:
		return TypeWidened
	case currLen < prevLen:
		return TypeNarrowed
	default:
		return TypeChangedLossy
	}
}

func typeSignature(column ColumnSnapshot) string {
	base := strings.TrimSpace(column.DataType)
	if base == "" {
		base = "unknown"
	}
	switch {
	case column.Precision > 0:
		if column.Scale > 0 {
			return fmt.Sprintf("%s(%d,%d)", base, column.Precision, column.Scale)
		}
		return fmt.Sprintf("%s(%d)", base, column.Precision)
	case column.MaxLength != 0 && (isStringType(canonicalType(base)) || isBinaryType(canonicalType(base))):
		if column.MaxLength < 0 {
			return fmt.Sprintf("%s(max)", base)
		}
		return fmt.Sprintf("%s(%d)", base, column.MaxLength)
	default:
		return base
	}
}

func columnSignature(column ColumnSnapshot) string {
	parts := []string{typeSignature(column), nullability(column)}
	if column.IsIdentity {
		parts = append(parts, "IDENTITY")
	}
	return strings.Join(parts, " ")
}

func nullability(column ColumnSnapshot) string {
	if column.IsNullable {
		return "NULL"
	}
	return "NOT NULL"
}

func indexSignature(index IndexSnapshot) string {
	parts := []string{strings.Join(index.Columns, ",")}
	if index.IsUnique {
		parts = append(parts, "unique")
	}
	if index.IsClustered {
		parts = append(parts, "clustered")
	}
	if len(index.IncludeColumns) > 0 {
		parts = append(parts, "include "+strings.Join(index.IncludeColumns, ","))
	}
	if index.Filter != "" {
		parts = append(parts, "where "+index.Filter)
	}
	return strings.Join(parts, " ")
}

func fkSignature(fk ForeignKeySnapshot) string {
	ref := fk.RefTable
	if fk.RefSchema != "" {
		ref = fk.RefSchema + "." + ref
	}
	parts := []string{
		strings.Join(fk.Columns, ","),
		"->",
		ref + "(" + strings.Join(fk.RefColumns, ",") + ")",
	}
	if fk.OnDelete != "" {
		parts = append(parts, "on_delete="+fk.OnDelete)
	}
	if fk.OnUpdate != "" {
		parts = append(parts, "on_update="+fk.OnUpdate)
	}
	return strings.Join(parts, " ")
}

func canonicalType(dataType string) string {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "int", "int4", "serial", "mediumint":
		return "integer"
	case "int8", "bigserial":
		return "bigint"
	case "int2", "smallserial":
		return "smallint"
	case "bool":
		return "boolean"
	case "decimal", "number":
		return "numeric"
	case "character varying":
		return "varchar"
	case "character":
		return "char"
	case "double precision":
		return "double"
	default:
		return dt
	}
}

func integerRank(dataType string) (int, bool) {
	switch dataType {
	case "tinyint":
		return 1, true
	case "smallint":
		return 2, true
	case "integer":
		return 3, true
	case "bigint":
		return 4, true
	default:
		return 0, false
	}
}

func floatRank(dataType string) (int, bool) {
	switch dataType {
	case "real", "float4":
		return 1, true
	case "float", "double", "float8":
		return 2, true
	default:
		return 0, false
	}
}

func sameFamily(left, right string, pred func(string) bool) bool {
	return pred(left) && pred(right)
}

func isStringType(dataType string) bool {
	switch dataType {
	case "char", "varchar", "nchar", "nvarchar", "text", "ntext", "mediumtext", "longtext":
		return true
	default:
		return false
	}
}

func isBinaryType(dataType string) bool {
	switch dataType {
	case "binary", "varbinary", "blob", "bytea", "image", "tinyblob", "mediumblob", "longblob":
		return true
	default:
		return false
	}
}

func isDecimalType(dataType string) bool {
	switch dataType {
	case "numeric", "money", "smallmoney":
		return true
	default:
		return false
	}
}

func lengthRank(length int) int {
	if length <= 0 {
		return int(^uint(0) >> 1)
	}
	return length
}
