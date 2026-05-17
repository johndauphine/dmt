package driver

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
)

// GenerateDropTableDDL generates DDL statement(s) for dropping a table.
// The AI generates database-specific syntax that properly handles foreign key constraints.
func (m *AITypeMapper) GenerateDropTableDDL(ctx context.Context, req DropTableDDLRequest) (string, error) {
	if req.TableName == "" {
		return "", fmt.Errorf("TableName is required")
	}
	if req.TargetDBType == "" {
		return "", fmt.Errorf("TargetDBType is required")
	}

	// Build cache key
	cacheKey := fmt.Sprintf("drop:%s:%s.%s", req.TargetDBType, req.TargetSchema, req.TableName)

	// Check cache first
	m.cacheMu.RLock()
	if cached, ok := m.cache.Get(cacheKey); ok {
		m.cacheMu.RUnlock()
		return cached, nil
	}
	m.cacheMu.RUnlock()

	logging.Debug("AI drop table DDL generation: %s.%s (%s)", req.TargetSchema, req.TableName, req.TargetDBType)

	// Build prompt
	prompt := m.buildDropTableDDLPrompt(req)

	// Call AI API
	result, err := m.CallAI(ctx, prompt)
	if err != nil {
		return "", fmt.Errorf("AI drop table DDL generation failed for %s.%s: %w",
			req.TargetSchema, req.TableName, err)
	}

	ddl := strings.TrimSpace(result)

	// Basic validation - should contain DROP
	upperDDL := strings.ToUpper(ddl)
	if !strings.Contains(upperDDL, "DROP") {
		return "", fmt.Errorf("response does not contain valid DROP statement: %s", truncateString(ddl, 100))
	}

	// Cache the result with AI provenance (#177).
	m.cacheMu.Lock()
	m.cache.SetAI(cacheKey, ddl, m.Model())
	m.cacheMu.Unlock()

	// Persist cache
	if err := m.saveCache(); err != nil {
		logging.Warn("Failed to save AI drop table DDL cache: %v", err)
	}

	logging.Debug("AI generated DROP DDL:\n%s", ddl)

	return ddl, nil
}

// buildDropTableDDLPrompt creates the AI prompt for DROP TABLE DDL generation.
func (m *AITypeMapper) buildDropTableDDLPrompt(req DropTableDDLRequest) string {
	var sb strings.Builder

	sb.WriteString("You are a database migration expert. Generate a DROP TABLE statement.\n\n")

	// Target database context
	sb.WriteString("=== TARGET DATABASE ===\n")
	sb.WriteString(fmt.Sprintf("Type: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("Schema: %s\n", req.TargetSchema))
	}
	if req.TargetContext != nil {
		sb.WriteString(fmt.Sprintf("Version: %s\n", req.TargetContext.Version))
		if req.TargetContext.MaxIdentifierLength > 0 {
			sb.WriteString(fmt.Sprintf("Max Identifier Length: %d\n", req.TargetContext.MaxIdentifierLength))
		}
	}
	sb.WriteString("\n")

	// Table to drop
	sb.WriteString("=== TABLE TO DROP ===\n")
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("Schema: %s\n", req.TargetSchema))
	}
	sb.WriteString(fmt.Sprintf("Table: %s\n", req.TableName))
	sb.WriteString("\n")

	// Output requirements
	sb.WriteString("=== OUTPUT REQUIREMENTS ===\n")
	sb.WriteString("Generate the complete statement(s) to drop the table, ensuring foreign key constraints do not block the drop.\n")
	sb.WriteString("Return ONLY the raw SQL statement(s) as plain text.\n")
	sb.WriteString("Do NOT wrap the response in JSON, markdown code blocks, or any other format.\n\n")

	// Database-specific instructions from dialect
	if dialect := GetDialect(req.TargetDBType); dialect != nil {
		if aug := dialect.AIDropTablePromptAugmentation(); aug != "" {
			sb.WriteString(aug)
		}
	} else {
		sb.WriteString("- Use DROP TABLE IF EXISTS with appropriate syntax for the target database\n")
		sb.WriteString("- Handle foreign key constraints appropriately\n")
	}

	return sb.String()
}
