package oracle

import (
	"strings"
	"testing"
)

// BenchmarkBuildMergeFromStaging benchmarks the new staging table MERGE SQL generation
func BenchmarkBuildMergeFromStaging(b *testing.B) {
	w := &Writer{dialect: &Dialect{}}
	columns := []string{"ID", "NAME", "EMAIL", "CREATED_AT", "UPDATED_AT", "STATUS", "AMOUNT", "DESCRIPTION"}
	pkColumns := []string{"ID"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.buildMergeFromStaging("SCHEMA.USERS", "DMT_STG_12345", columns, pkColumns)
	}
}

// BenchmarkBuildMergeUnionAll benchmarks the old UNION ALL approach for comparison
// This simulates the SQL generation overhead of the old approach
func BenchmarkBuildMergeUnionAll(b *testing.B) {
	dialect := &Dialect{}
	columns := []string{"ID", "NAME", "EMAIL", "CREATED_AT", "UPDATED_AT", "STATUS", "AMOUNT", "DESCRIPTION"}
	pkColumns := []string{"ID"}

	// Simulate 200 rows (the old default batch size)
	rows := make([][]any, 200)
	for i := range rows {
		rows[i] = make([]any, len(columns))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buildMergeUnionAllSQL(dialect, "SCHEMA.USERS", columns, pkColumns, rows)
	}
}

// BenchmarkBuildMergeUnionAll_5000 benchmarks UNION ALL with 5000 rows (new batch size)
func BenchmarkBuildMergeUnionAll_5000(b *testing.B) {
	dialect := &Dialect{}
	columns := []string{"ID", "NAME", "EMAIL", "CREATED_AT", "UPDATED_AT", "STATUS", "AMOUNT", "DESCRIPTION"}
	pkColumns := []string{"ID"}

	// Simulate 5000 rows (new batch size - would be used with staging table)
	rows := make([][]any, 5000)
	for i := range rows {
		rows[i] = make([]any, len(columns))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buildMergeUnionAllSQL(dialect, "SCHEMA.USERS", columns, pkColumns, rows)
	}
}

// buildMergeUnionAllSQL simulates the old UNION ALL SQL generation approach
// This is the OLD approach - kept here for benchmark comparison only
func buildMergeUnionAllSQL(dialect *Dialect, fullTableName string, columns, pkColumns []string, rows [][]any) string {
	var sb strings.Builder
	sb.WriteString("MERGE INTO ")
	sb.WriteString(fullTableName)
	sb.WriteString(" tgt\nUSING (\n")

	// Build inline view with UNION ALL - this is the slow part
	paramIdx := 1
	for i := range rows {
		if i > 0 {
			sb.WriteString("    UNION ALL\n")
		}
		sb.WriteString("    SELECT ")
		placeholders := make([]string, len(columns))
		for j, col := range columns {
			placeholders[j] = ":" + string(rune('0'+paramIdx)) + " AS " + dialect.QuoteIdentifier(col)
			paramIdx++
		}
		sb.WriteString(strings.Join(placeholders, ", "))
		sb.WriteString(" FROM DUAL\n")
	}

	sb.WriteString(") src\n")

	// ON clause
	onClauses := make([]string, len(pkColumns))
	for i, pk := range pkColumns {
		qPK := dialect.QuoteIdentifier(pk)
		onClauses[i] = "tgt." + qPK + " = src." + qPK
	}
	sb.WriteString("ON (")
	sb.WriteString(strings.Join(onClauses, " AND "))
	sb.WriteString(")\n")

	// UPDATE clause
	pkSet := make(map[string]bool)
	for _, pk := range pkColumns {
		pkSet[strings.ToUpper(pk)] = true
	}

	var updateClauses []string
	for _, col := range columns {
		if !pkSet[strings.ToUpper(col)] {
			qCol := dialect.QuoteIdentifier(col)
			updateClauses = append(updateClauses, "tgt."+qCol+" = src."+qCol)
		}
	}

	if len(updateClauses) > 0 {
		sb.WriteString("WHEN MATCHED THEN\n    UPDATE SET ")
		sb.WriteString(strings.Join(updateClauses, ", "))
		sb.WriteString("\n")
	}

	// INSERT clause
	quotedCols := make([]string, len(columns))
	srcCols := make([]string, len(columns))
	for i, col := range columns {
		qCol := dialect.QuoteIdentifier(col)
		quotedCols[i] = qCol
		srcCols[i] = "src." + qCol
	}

	sb.WriteString("WHEN NOT MATCHED THEN\n    INSERT (")
	sb.WriteString(strings.Join(quotedCols, ", "))
	sb.WriteString(")\n    VALUES (")
	sb.WriteString(strings.Join(srcCols, ", "))
	sb.WriteString(")")

	return sb.String()
}

// BenchmarkConvertRowValues benchmarks row value conversion
func BenchmarkConvertRowValues(b *testing.B) {
	row := []any{
		int64(12345),
		"test string",
		true,
		false,
		nil,
		[]byte("binary data"),
		float64(123.45),
		"another string",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = convertRowValues(row)
	}
}

// BenchmarkConvertRowValues_Large benchmarks conversion of larger rows
func BenchmarkConvertRowValues_Large(b *testing.B) {
	// 50 columns typical of a large table
	row := make([]any, 50)
	for i := range row {
		switch i % 5 {
		case 0:
			row[i] = int64(i)
		case 1:
			row[i] = "string value"
		case 2:
			row[i] = true
		case 3:
			row[i] = nil
		case 4:
			row[i] = []byte("binary")
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = convertRowValues(row)
	}
}
