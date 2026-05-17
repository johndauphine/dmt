package driver

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
)

// GenerateFinalizationDDL generates DDL for indexes, foreign keys, or check constraints using AI.
func (m *AITypeMapper) GenerateFinalizationDDL(ctx context.Context, req FinalizationDDLRequest) (string, error) {
	if req.Table == nil {
		return "", fmt.Errorf("Table is required")
	}
	if req.TargetDBType == "" {
		return "", fmt.Errorf("TargetDBType is required")
	}

	var prompt string
	var entityName string
	var validatePrefix string

	switch req.Type {
	case DDLTypeIndex:
		if req.Index == nil {
			return "", fmt.Errorf("Index is required for DDLTypeIndex")
		}
		prompt = m.buildIndexDDLPrompt(req)
		entityName = req.Index.Name
		// validatePrefix not used for index - has custom validation below
		logging.Debug("AI index DDL generation: %s on %s.%s (%s)",
			req.Index.Name, req.TargetSchema, req.Table.Name, req.TargetDBType)

	case DDLTypeForeignKey:
		if req.ForeignKey == nil {
			return "", fmt.Errorf("ForeignKey is required for DDLTypeForeignKey")
		}
		prompt = m.buildForeignKeyDDLPrompt(req)
		entityName = req.ForeignKey.Name
		validatePrefix = "ALTER TABLE"
		logging.Debug("AI FK DDL generation: %s on %s.%s (%s)",
			req.ForeignKey.Name, req.TargetSchema, req.Table.Name, req.TargetDBType)

	case DDLTypeCheckConstraint:
		if req.CheckConstraint == nil {
			return "", fmt.Errorf("CheckConstraint is required for DDLTypeCheckConstraint")
		}
		prompt = m.buildCheckConstraintDDLPrompt(req)
		entityName = req.CheckConstraint.Name
		validatePrefix = "ALTER TABLE"
		logging.Debug("AI check constraint DDL generation: %s on %s.%s (%s)",
			req.CheckConstraint.Name, req.TargetSchema, req.Table.Name, req.TargetDBType)

	default:
		return "", fmt.Errorf("unknown DDL type: %s", req.Type)
	}

	result, err := m.CallAI(ctx, prompt)
	if err != nil {
		return "", fmt.Errorf("AI DDL generation failed for %s.%s: %w",
			req.Table.Name, entityName, err)
	}

	ddl := strings.TrimSpace(result)

	// Validate response starts with expected prefix
	upperDDL := strings.ToUpper(ddl)
	if req.Type == DDLTypeIndex {
		if !strings.HasPrefix(upperDDL, "CREATE") || !strings.Contains(upperDDL, "INDEX") {
			return "", fmt.Errorf("response does not contain valid CREATE INDEX statement: %s", truncateString(ddl, 100))
		}
	} else if !strings.HasPrefix(upperDDL, validatePrefix) {
		return "", fmt.Errorf("response does not contain valid %s statement: %s", validatePrefix, truncateString(ddl, 100))
	}

	logging.Debug("AI generated DDL:\n%s", ddl)

	return ddl, nil
}

// buildIndexDDLPrompt creates the AI prompt for index DDL generation.
func (m *AITypeMapper) buildIndexDDLPrompt(req FinalizationDDLRequest) string {
	var sb strings.Builder

	sb.WriteString("You are a database migration expert. Generate a CREATE INDEX statement.\n\n")

	// Target database context
	sb.WriteString("=== TARGET DATABASE ===\n")
	sb.WriteString(fmt.Sprintf("Type: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("Schema: %s\n", req.TargetSchema))
	}
	if req.TargetContext != nil {
		sb.WriteString(fmt.Sprintf("Max Identifier Length: %d\n", req.TargetContext.MaxIdentifierLength))
		if req.TargetContext.IdentifierCase != "" {
			sb.WriteString(fmt.Sprintf("Identifier Case: %s\n", req.TargetContext.IdentifierCase))
		}
	}
	sb.WriteString("\n")

	// Target table DDL for context
	if req.TargetTableDDL != "" {
		sb.WriteString("=== TARGET TABLE DDL ===\n")
		sb.WriteString(req.TargetTableDDL)
		sb.WriteString("\n\n")
	}

	// Index details
	sb.WriteString("=== INDEX TO CREATE ===\n")
	sb.WriteString(fmt.Sprintf("Table: %s\n", req.Table.Name))
	sb.WriteString(fmt.Sprintf("Index Name: %s\n", req.Index.Name))
	sb.WriteString(fmt.Sprintf("Columns: %s\n", strings.Join(req.Index.Columns, ", ")))
	sb.WriteString(fmt.Sprintf("Is Unique: %v\n", req.Index.IsUnique))
	if len(req.Index.IncludeCols) > 0 {
		sb.WriteString(fmt.Sprintf("Include Columns: %s\n", strings.Join(req.Index.IncludeCols, ", ")))
	}
	if req.Index.Filter != "" {
		sb.WriteString(fmt.Sprintf("Filter (WHERE clause): %s\n", req.Index.Filter))
	}
	sb.WriteString("\n")

	// Output requirements
	sb.WriteString("=== OUTPUT REQUIREMENTS ===\n")
	sb.WriteString("Generate the complete CREATE INDEX statement for the target database.\n")
	sb.WriteString("- Use appropriate index name (prefix with idx_ if needed, respect max identifier length)\n")
	sb.WriteString("- Include UNIQUE keyword if IsUnique is true\n")
	sb.WriteString("- Include INCLUDE clause if target supports it (SQL Server, PostgreSQL 11+)\n")
	sb.WriteString("- Include WHERE clause for filtered indexes if target supports it\n")
	sb.WriteString("- Quote identifiers appropriately for the target database\n")
	sb.WriteString("- Return ONLY the raw CREATE INDEX SQL statement as plain text\n")
	sb.WriteString("- Do NOT wrap the response in JSON, markdown code blocks, or any other format\n")

	// Database-specific identifier requirements from the target dialect
	if dialect := GetDialect(req.TargetDBType); dialect != nil {
		if aug := dialect.AIPromptAugmentation(); aug != "" {
			sb.WriteString(aug)
		}
	}

	return sb.String()
}

// buildForeignKeyDDLPrompt creates the AI prompt for foreign key DDL generation.
func (m *AITypeMapper) buildForeignKeyDDLPrompt(req FinalizationDDLRequest) string {
	var sb strings.Builder

	sb.WriteString("You are a database migration expert. Generate an ALTER TABLE statement to add a foreign key constraint.\n\n")

	// Target database context
	sb.WriteString("=== TARGET DATABASE ===\n")
	sb.WriteString(fmt.Sprintf("Type: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("Schema: %s\n", req.TargetSchema))
	}
	if req.TargetContext != nil {
		sb.WriteString(fmt.Sprintf("Max Identifier Length: %d\n", req.TargetContext.MaxIdentifierLength))
		if req.TargetContext.IdentifierCase != "" {
			sb.WriteString(fmt.Sprintf("Identifier Case: %s\n", req.TargetContext.IdentifierCase))
		}
	}
	sb.WriteString("\n")

	// Target table DDL for context
	if req.TargetTableDDL != "" {
		sb.WriteString("=== TARGET TABLE DDL ===\n")
		sb.WriteString(req.TargetTableDDL)
		sb.WriteString("\n\n")
	}

	// Foreign key details
	sb.WriteString("=== FOREIGN KEY TO CREATE ===\n")
	sb.WriteString(fmt.Sprintf("Table: %s\n", req.Table.Name))
	sb.WriteString(fmt.Sprintf("FK Name: %s\n", req.ForeignKey.Name))
	sb.WriteString(fmt.Sprintf("Columns: %s\n", strings.Join(req.ForeignKey.Columns, ", ")))
	refTable := req.ForeignKey.RefTable
	if req.ForeignKey.RefSchema != "" && req.ForeignKey.RefSchema != req.TargetSchema {
		refTable = req.ForeignKey.RefSchema + "." + req.ForeignKey.RefTable
	}
	sb.WriteString(fmt.Sprintf("References Table: %s\n", refTable))
	sb.WriteString(fmt.Sprintf("References Columns: %s\n", strings.Join(req.ForeignKey.RefColumns, ", ")))
	if req.ForeignKey.OnDelete != "" {
		sb.WriteString(fmt.Sprintf("ON DELETE: %s\n", req.ForeignKey.OnDelete))
	}
	if req.ForeignKey.OnUpdate != "" {
		sb.WriteString(fmt.Sprintf("ON UPDATE: %s\n", req.ForeignKey.OnUpdate))
	}
	sb.WriteString("\n")

	// Output requirements
	sb.WriteString("=== OUTPUT REQUIREMENTS ===\n")
	sb.WriteString("Generate the complete ALTER TABLE ... ADD CONSTRAINT statement for the foreign key.\n")
	sb.WriteString("- Use appropriate constraint name (prefix with fk_ if needed, respect max identifier length)\n")
	sb.WriteString("- Include ON DELETE and ON UPDATE actions if specified\n")
	sb.WriteString("- Map referential actions to target database syntax (NO ACTION, CASCADE, SET NULL, etc.)\n")
	sb.WriteString("- Quote identifiers appropriately for the target database\n")
	sb.WriteString("- Return ONLY the raw ALTER TABLE SQL statement as plain text\n")
	sb.WriteString("- Do NOT wrap the response in JSON, markdown code blocks, or any other format\n")

	// Database-specific identifier requirements from the target dialect
	if dialect := GetDialect(req.TargetDBType); dialect != nil {
		if aug := dialect.AIPromptAugmentation(); aug != "" {
			sb.WriteString(aug)
		}
	}

	return sb.String()
}

// buildCheckConstraintDDLPrompt creates the AI prompt for check constraint DDL generation.
func (m *AITypeMapper) buildCheckConstraintDDLPrompt(req FinalizationDDLRequest) string {
	var sb strings.Builder

	sb.WriteString("You are a database migration expert. Generate an ALTER TABLE statement to add a check constraint.\n\n")

	// Source database context (for translating expressions)
	if req.SourceDBType != "" {
		sb.WriteString("=== SOURCE DATABASE ===\n")
		sb.WriteString(fmt.Sprintf("Type: %s\n", req.SourceDBType))
		sb.WriteString("\n")
	}

	// Target database context
	sb.WriteString("=== TARGET DATABASE ===\n")
	sb.WriteString(fmt.Sprintf("Type: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("Schema: %s\n", req.TargetSchema))
	}
	if req.TargetContext != nil {
		sb.WriteString(fmt.Sprintf("Max Identifier Length: %d\n", req.TargetContext.MaxIdentifierLength))
		if req.TargetContext.IdentifierCase != "" {
			sb.WriteString(fmt.Sprintf("Identifier Case: %s\n", req.TargetContext.IdentifierCase))
		}
	}
	sb.WriteString("\n")

	// Target table DDL for context
	if req.TargetTableDDL != "" {
		sb.WriteString("=== TARGET TABLE DDL ===\n")
		sb.WriteString(req.TargetTableDDL)
		sb.WriteString("\n\n")
	}

	// Check constraint details
	sb.WriteString("=== CHECK CONSTRAINT TO CREATE ===\n")
	sb.WriteString(fmt.Sprintf("Table: %s\n", req.Table.Name))
	sb.WriteString(fmt.Sprintf("Constraint Name: %s\n", req.CheckConstraint.Name))
	sb.WriteString(fmt.Sprintf("Definition: %s\n", req.CheckConstraint.Definition))
	sb.WriteString("\n")

	// Output requirements
	sb.WriteString("=== OUTPUT REQUIREMENTS ===\n")
	sb.WriteString("Generate the complete ALTER TABLE ... ADD CONSTRAINT statement for the check constraint.\n")
	sb.WriteString("- Use appropriate constraint name (prefix with chk_ if needed, respect max identifier length)\n")
	sb.WriteString("- Convert the check expression syntax from source database to target database\n")
	sb.WriteString("- Convert functions appropriately (e.g., GETDATE() -> NOW(), SYSDATE, CURRENT_TIMESTAMP)\n")
	sb.WriteString("- Quote identifiers appropriately for the target database\n")
	sb.WriteString("- Return ONLY the raw ALTER TABLE SQL statement as plain text\n")
	sb.WriteString("- Do NOT wrap the response in JSON, markdown code blocks, or any other format\n")

	// Database-specific identifier requirements from the target dialect
	if dialect := GetDialect(req.TargetDBType); dialect != nil {
		if aug := dialect.AIPromptAugmentation(); aug != "" {
			sb.WriteString(aug)
		}
	}

	return sb.String()
}
