package driver

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/v5/internal/ident"
)

func (m *AITypeMapper) writeContextDetails(sb *strings.Builder, ctx *DatabaseContext, label string) {
	if ctx.Version != "" {
		fmt.Fprintf(sb, "Version: %s\n", ctx.Version)
	}
	if ctx.DatabaseName != "" {
		fmt.Fprintf(sb, "Database: %s\n", ctx.DatabaseName)
	}

	// Character encoding section
	sb.WriteString("Character Encoding:\n")
	if ctx.Charset != "" {
		fmt.Fprintf(sb, "  Charset: %s\n", ctx.Charset)
	}
	if ctx.NationalCharset != "" {
		fmt.Fprintf(sb, "  National Charset: %s\n", ctx.NationalCharset)
	}
	if ctx.Encoding != "" {
		fmt.Fprintf(sb, "  Encoding: %s\n", ctx.Encoding)
	}
	if ctx.CodePage > 0 {
		fmt.Fprintf(sb, "  Code Page: %d\n", ctx.CodePage)
	}
	if ctx.Collation != "" {
		fmt.Fprintf(sb, "  Collation: %s\n", ctx.Collation)
	}
	if ctx.BytesPerChar > 0 {
		fmt.Fprintf(sb, "  Max Bytes Per Char: %d\n", ctx.BytesPerChar)
	}

	// Case sensitivity section
	sb.WriteString("Case Sensitivity:\n")
	if ctx.IdentifierCase != "" {
		fmt.Fprintf(sb, "  Identifier Case: %s\n", ctx.IdentifierCase)
	}
	if ctx.CaseSensitiveIdentifiers {
		sb.WriteString("  Identifiers: case-sensitive\n")
	} else {
		sb.WriteString("  Identifiers: case-insensitive\n")
	}
	if ctx.CaseSensitiveData {
		sb.WriteString("  String Comparisons: case-sensitive\n")
	} else {
		sb.WriteString("  String Comparisons: case-insensitive (collation-dependent)\n")
	}

	// Limits section
	sb.WriteString("Limits:\n")
	if ctx.MaxIdentifierLength > 0 {
		fmt.Fprintf(sb, "  Max Identifier Length: %d\n", ctx.MaxIdentifierLength)
	}
	if ctx.MaxVarcharLength > 0 {
		fmt.Fprintf(sb, "  Max VARCHAR Length: %d\n", ctx.MaxVarcharLength)
	}
	if ctx.MaxNVarcharLength > 0 {
		fmt.Fprintf(sb, "  Max NVARCHAR Length: %d characters\n", ctx.MaxNVarcharLength)
	}
	if ctx.VarcharSemantics != "" {
		fmt.Fprintf(sb, "  VARCHAR Semantics: %s (lengths are in %ss)\n", ctx.VarcharSemantics, ctx.VarcharSemantics)
	}

	// Features section
	if ctx.StorageEngine != "" {
		fmt.Fprintf(sb, "Storage Engine: %s\n", ctx.StorageEngine)
	}
	if len(ctx.Features) > 0 {
		fmt.Fprintf(sb, "Features: %s\n", strings.Join(ctx.Features, ", "))
	}
	if ctx.Notes != "" {
		fmt.Fprintf(sb, "Notes: %s\n", ctx.Notes)
	}
}

// writeMigrationRules writes migration guidance derived dynamically from database context.
// All rules are generated from runtime metadata - no hardcoded database-specific rules.
func (m *AITypeMapper) writeMigrationRules(sb *strings.Builder, req TableDDLRequest) {
	// Source database characteristics - derived from SourceContext
	sb.WriteString("Source database characteristics:\n")
	if req.SourceContext != nil {
		m.writeVarcharGuidance(sb, req.SourceContext, "source")
		m.writeEncodingGuidance(sb, req.SourceContext, "source")
	} else {
		sb.WriteString("- No source context available, using standard type semantics\n")
	}

	sb.WriteString("\n")

	// Target database rules - derived from TargetContext
	sb.WriteString("Target database rules:\n")
	if req.TargetContext != nil {
		m.writeVarcharGuidance(sb, req.TargetContext, "target")
		m.writeEncodingGuidance(sb, req.TargetContext, "target")
		m.writeIdentifierGuidance(sb, req.TargetContext, req.SourceDBType, req.TargetDBType)
		m.writeLimitsGuidance(sb, req.TargetContext)
	} else {
		sb.WriteString("- No target context available, use standard type mappings\n")
	}

	// Cross-database conversion guidance
	sb.WriteString("\nConversion guidance:\n")
	m.writeConversionGuidance(sb, req.SourceContext, req.TargetContext)

	// NVARCHAR guidance based on DB types â€” fires even when SourceContext is nil
	srcType := Canonicalize(req.SourceDBType)
	tgtType := Canonicalize(req.TargetDBType)
	if (srcType == "postgres" || srcType == "mysql") && tgtType == "mssql" {
		sb.WriteString("- MANDATORY: Every VARCHAR column MUST be NVARCHAR, every CHAR column MUST be NCHAR â€” using VARCHAR will corrupt multi-byte data because VARCHAR uses byte lengths while the source uses character lengths\n")
	}

	// Reserved words note
	sb.WriteString("\nReserved words: If any column name is a SQL reserved word, quote it appropriately for the target database.\n")
}

// capitalizeFirst returns the string with its first character uppercased.
// This replaces the deprecated strings.Title function.
func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// writeVarcharGuidance writes VARCHAR semantics guidance based on context.
func (m *AITypeMapper) writeVarcharGuidance(sb *strings.Builder, ctx *DatabaseContext, role string) {
	if ctx.VarcharSemantics == "" {
		return
	}

	switch ctx.VarcharSemantics {
	case "char":
		fmt.Fprintf(sb, "- %s VARCHAR lengths are in CHARACTERS\n", capitalizeFirst(role))
	case "byte":
		fmt.Fprintf(sb, "- %s VARCHAR lengths are in BYTES\n", capitalizeFirst(role))
		if ctx.BytesPerChar > 1 {
			fmt.Fprintf(sb, "- Each character may take up to %d bytes\n", ctx.BytesPerChar)
		}
	}
}

// writeEncodingGuidance writes character encoding guidance based on context.
func (m *AITypeMapper) writeEncodingGuidance(sb *strings.Builder, ctx *DatabaseContext, role string) {
	if ctx.Charset != "" {
		fmt.Fprintf(sb, "- Character set: %s\n", ctx.Charset)
	}
	if ctx.BytesPerChar > 0 {
		fmt.Fprintf(sb, "- Max bytes per character: %d\n", ctx.BytesPerChar)
	}
	if ctx.Encoding != "" && ctx.Encoding != ctx.Charset {
		fmt.Fprintf(sb, "- Encoding: %s\n", ctx.Encoding)
	}
}

// writeIdentifierGuidance writes identifier handling guidance based on context.
func (m *AITypeMapper) writeIdentifierGuidance(sb *strings.Builder, ctx *DatabaseContext, sourceDBType, targetDBType string) {
	if ctx.IdentifierCase != "" {
		switch strings.ToLower(ctx.IdentifierCase) {
		case "upper":
			sb.WriteString("- CRITICAL: Unquoted identifiers are folded to UPPERCASE\n")
			sb.WriteString("- Use UPPERCASE for all unquoted table and column names\n")
			sb.WriteString("- Only quote identifiers that are reserved words\n")
		case "lower":
			if Canonicalize(sourceDBType) == Canonicalize(targetDBType) {
				sb.WriteString("- CRITICAL: Source and target are the same database engine\n")
				sb.WriteString("- Preserve ALL source column and table names EXACTLY as-is, including underscores\n")
				sb.WriteString("- Do NOT remove, add, or modify any characters in identifier names\n")
				sb.WriteString("- Example: user_id -> user_id (NOT userid)\n")
				sb.WriteString("- Example: created_at -> created_at (NOT createdat)\n")
			} else {
				sb.WriteString("- CRITICAL: Unquoted identifiers are folded to lowercase\n")
				sb.WriteString("- Use lowercase for all table and column names (e.g., UserId -> userid, not user_id)\n")
				sb.WriteString("- Do NOT convert to snake_case - just lowercase the original name directly\n")
			}
		case "preserve":
			sb.WriteString("- Identifier case is preserved as written\n")
		}
	}

	if ctx.CaseSensitiveIdentifiers {
		sb.WriteString("- Identifiers are case-sensitive when quoted\n")
	}
}

// writeLimitsGuidance writes database limits guidance based on context.
func (m *AITypeMapper) writeLimitsGuidance(sb *strings.Builder, ctx *DatabaseContext) {
	if ctx.MaxIdentifierLength > 0 {
		fmt.Fprintf(sb, "- Maximum identifier length: %d characters\n", ctx.MaxIdentifierLength)
	}
	if ctx.MaxVarcharLength > 0 {
		fmt.Fprintf(sb, "- Maximum VARCHAR length: %d\n", ctx.MaxVarcharLength)
		if ctx.VarcharSemantics == "byte" {
			sb.WriteString("- Use CLOB/TEXT equivalent for content exceeding max VARCHAR\n")
		}
	}
}

// writeConversionGuidance writes guidance for cross-database type conversion.
func (m *AITypeMapper) writeConversionGuidance(sb *strings.Builder, srcCtx, tgtCtx *DatabaseContext) {
	if srcCtx == nil || tgtCtx == nil {
		sb.WriteString("- Map types based on semantic equivalence\n")
		return
	}

	// VARCHAR semantics conversion
	if srcCtx.VarcharSemantics == "char" && tgtCtx.VarcharSemantics == "byte" {
		sb.WriteString("- Source VARCHAR/CHAR lengths are in CHARACTERS\n")
		sb.WriteString("- Target VARCHAR lengths are in BYTES (not characters)\n")
	} else if srcCtx.VarcharSemantics == "byte" && tgtCtx.VarcharSemantics == "char" {
		sb.WriteString("- Source uses BYTE lengths, target uses CHARACTER lengths\n")
		if srcCtx.BytesPerChar > 1 {
			fmt.Fprintf(sb, "- Source VARCHAR(n) with %d bytes/char = approximately n/%d characters\n", srcCtx.BytesPerChar, srcCtx.BytesPerChar)
		}
	} else if srcCtx.VarcharSemantics == "char" && tgtCtx.VarcharSemantics == "char" {
		sb.WriteString("- Both source and target use CHARACTER lengths - preserve lengths directly\n")
	}

	// Case handling guidance
	if srcCtx.IdentifierCase != tgtCtx.IdentifierCase && tgtCtx.IdentifierCase != "" {
		switch strings.ToLower(tgtCtx.IdentifierCase) {
		case "upper":
			sb.WriteString("- Convert all identifiers to UPPERCASE for target\n")
		case "lower":
			sb.WriteString("- Convert all identifiers to lowercase for target\n")
		}
	}
}

// findReservedWords checks source table columns for SQL reserved words.
func (m *AITypeMapper) findReservedWords(t *Table, targetDBType string) []string {
	// Common SQL reserved words that cause issues
	reservedWords := map[string]bool{
		"date": true, "time": true, "timestamp": true, "year": true, "month": true, "day": true,
		"user": true, "order": true, "group": true, "table": true, "index": true, "key": true,
		"type": true, "name": true, "value": true, "size": true, "number": true, "level": true,
		"comment": true, "desc": true, "asc": true, "limit": true, "offset": true,
		"select": true, "insert": true, "update": true, "delete": true, "from": true, "where": true,
		"and": true, "or": true, "not": true, "null": true, "true": true, "false": true,
		"primary": true, "foreign": true, "references": true, "constraint": true,
		"create": true, "alter": true, "drop": true, "truncate": true,
		"row": true, "rows": true, "column": true, "schema": true, "database": true,
		"function": true, "procedure": true, "trigger": true, "view": true,
		"id": false, // not reserved in most DBs
	}

	var found []string
	for _, col := range t.Columns {
		colLower := strings.ToLower(col.Name)
		if reservedWords[colLower] {
			found = append(found, col.Name)
		}
	}
	return found
}

// targetIdentifier returns the exact column/table name the transfer phase will
// use for the target database. Uses the shared ident.SanitizePG implementation
// so prompt-generated names always match what WriteBatch/CopyFrom expects.
func targetIdentifier(name, targetDBType string) string {
	if targetDBType != "postgres" {
		return name
	}
	return ident.SanitizePG(name)
}

// buildSourceDDL creates a DDL-like representation of the source table.
func (m *AITypeMapper) buildSourceDDL(t *Table, sourceDBType string) string {
	return m.buildSourceDDLWithTarget(t, sourceDBType, "")
}

// buildSourceDDLWithTarget creates a DDL-like representation of the source table
// with required target column names annotated inline.
func (m *AITypeMapper) buildSourceDDLWithTarget(t *Table, sourceDBType, targetDBType string) string {
	var sb strings.Builder

	tableName := t.Name
	if t.Schema != "" {
		tableName = t.Schema + "." + t.Name
	}

	fmt.Fprintf(&sb, "CREATE TABLE %s (\n", tableName)

	for i, col := range t.Columns {
		sb.WriteString("    ")
		sb.WriteString(col.Name)
		sb.WriteString(" ")

		// Build type with length/precision
		typeStr := col.DataType
		if col.MaxLength > 0 {
			typeStr = fmt.Sprintf("%s(%d)", col.DataType, col.MaxLength)
		} else if col.MaxLength == -1 {
			typeStr = fmt.Sprintf("%s(MAX)", col.DataType)
		} else if col.Precision > 0 {
			if col.Scale > 0 {
				typeStr = fmt.Sprintf("%s(%d,%d)", col.DataType, col.Precision, col.Scale)
			} else {
				typeStr = fmt.Sprintf("%s(%d)", col.DataType, col.Precision)
			}
		}
		sb.WriteString(typeStr)

		// NULL constraint
		if !col.IsNullable {
			sb.WriteString(" NOT NULL")
		}

		// Identity
		if col.IsIdentity {
			switch sourceDBType {
			case "postgres":
				sb.WriteString(" GENERATED BY DEFAULT AS IDENTITY")
			case "mssql":
				sb.WriteString(" IDENTITY")
			case "mysql":
				sb.WriteString(" AUTO_INCREMENT")
			}
		}

		if i < len(t.Columns)-1 {
			sb.WriteString(",")
		}

		// Annotate with required target column name
		if targetDBType != "" {
			tgt := targetIdentifier(col.Name, targetDBType)
			if tgt != col.Name {
				fmt.Fprintf(&sb, "  -- target column: %s", tgt)
			}
		}

		sb.WriteString("\n")
	}

	// Primary key
	if len(t.PrimaryKey) > 0 {
		fmt.Fprintf(&sb, "    ,PRIMARY KEY (%s)\n", strings.Join(t.PrimaryKey, ", "))
	}

	sb.WriteString(");")

	return sb.String()
}
