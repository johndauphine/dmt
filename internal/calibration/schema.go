package calibration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

const (
	// SchemaPrefix is the prefix for calibration schemas.
	SchemaPrefix = "_dmt_cal_"
)

// SchemaManager handles creation and cleanup of the calibration schema.
type SchemaManager struct {
	targetPool driver.Writer
	schemaName string
	dbType     string
	tables     []string
}

// NewSchemaManager creates a new schema manager with a unique schema name.
func NewSchemaManager(targetPool driver.Writer, dbType string) *SchemaManager {
	return &SchemaManager{
		targetPool: targetPool,
		schemaName: generateSchemaName(),
		dbType:     dbType,
		tables:     make([]string, 0),
	}
}

// generateSchemaName creates a unique schema name with a random suffix.
func generateSchemaName() string {
	b := make([]byte, 4)
	rand.Read(b)
	return SchemaPrefix + hex.EncodeToString(b)
}

// SchemaName returns the generated schema name.
func (sm *SchemaManager) SchemaName() string {
	return sm.schemaName
}

// CreateSchema creates the calibration schema in the target database.
func (sm *SchemaManager) CreateSchema(ctx context.Context) error {
	logging.Debug("Creating calibration schema: %s", sm.schemaName)

	err := sm.targetPool.CreateSchema(ctx, sm.schemaName)
	if err != nil {
		return fmt.Errorf("failed to create calibration schema: %w", err)
	}

	return nil
}

// CreateTable creates a table in the calibration schema.
// It returns the full qualified name (schema.table).
func (sm *SchemaManager) CreateTable(ctx context.Context, t *driver.Table) (string, error) {
	// Create table without indexes or constraints for speed (unlogged for PostgreSQL)
	opts := driver.TableOptions{
		Unlogged: true, // Faster writes for calibration
	}

	err := sm.targetPool.CreateTableWithOptions(ctx, t, sm.schemaName, opts)
	if err != nil {
		return "", fmt.Errorf("failed to create calibration table %s: %w", t.Name, err)
	}

	sm.tables = append(sm.tables, t.Name)

	fullName := fmt.Sprintf("%s.%s", sm.schemaName, t.Name)
	logging.Debug("Created calibration table: %s", fullName)

	return fullName, nil
}

// TruncateAllTables truncates all tables in the calibration schema.
func (sm *SchemaManager) TruncateAllTables(ctx context.Context) error {
	for _, tableName := range sm.tables {
		err := sm.targetPool.TruncateTable(ctx, sm.schemaName, tableName)
		if err != nil {
			return fmt.Errorf("failed to truncate %s.%s: %w", sm.schemaName, tableName, err)
		}
	}
	return nil
}

// DropSchema drops the calibration schema and all its tables.
// This is safe to call multiple times (idempotent).
func (sm *SchemaManager) DropSchema(ctx context.Context) error {
	logging.Debug("Cleaning up calibration schema: %s", sm.schemaName)

	var query string
	switch strings.ToLower(sm.dbType) {
	case "postgres", "postgresql":
		query = fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", sm.schemaName)
	case "mssql", "sqlserver":
		// MSSQL requires dropping tables first, then schema
		// First drop all tables
		for _, tableName := range sm.tables {
			dropTableQuery := fmt.Sprintf("DROP TABLE IF EXISTS [%s].[%s]", sm.schemaName, tableName)
			if _, err := sm.targetPool.ExecRaw(ctx, dropTableQuery); err != nil {
				logging.Warn("Failed to drop table %s.%s: %v", sm.schemaName, tableName, err)
			}
		}
		// Then drop schema (escape single quotes for safety)
		escapedName := strings.ReplaceAll(sm.schemaName, "'", "''")
		query = fmt.Sprintf(`
			IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'%s')
			BEGIN
				DROP SCHEMA [%s]
			END`, escapedName, sm.schemaName)
	default:
		return fmt.Errorf("unsupported database type for schema cleanup: %s", sm.dbType)
	}

	_, err := sm.targetPool.ExecRaw(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop calibration schema: %w", err)
	}

	logging.Debug("Dropped calibration schema: %s", sm.schemaName)
	return nil
}

// ManualCleanupInstructions returns SQL for manual cleanup if automatic fails.
func (sm *SchemaManager) ManualCleanupInstructions() string {
	switch strings.ToLower(sm.dbType) {
	case "postgres", "postgresql":
		return fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE;", sm.schemaName)
	case "mssql", "sqlserver":
		var sb strings.Builder
		for _, tableName := range sm.tables {
			sb.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS [%s].[%s];\n", sm.schemaName, tableName))
		}
		sb.WriteString(fmt.Sprintf("DROP SCHEMA [%s];", sm.schemaName))
		return sb.String()
	default:
		return fmt.Sprintf("-- Manual cleanup for schema: %s", sm.schemaName)
	}
}
