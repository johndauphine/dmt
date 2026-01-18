//go:build integration && cgo

package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/godror/godror"
)

// getTestDSN returns the Oracle DSN for testing.
// Set ORACLE_TEST_DSN environment variable or use defaults.
func getTestDSN() string {
	if dsn := os.Getenv("ORACLE_TEST_DSN"); dsn != "" {
		return dsn
	}
	// Default: oracle-target container
	return "so2010/oracle123@localhost:1522/FREEPDB1"
}

func TestResetExplicitSequence_Integration(t *testing.T) {
	dsn := getTestDSN()
	db, err := sql.Open("godror", dsn)
	if err != nil {
		t.Skipf("Cannot connect to Oracle: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("Cannot ping Oracle: %v", err)
	}

	ctx := context.Background()
	dialect := &Dialect{}
	w := &Writer{
		db:      db,
		dialect: dialect,
	}

	// Test sequence name - use unique name to avoid conflicts
	seqName := "TEST_SEQ_RESET_123"
	schema := "SO2010"

	// Cleanup any existing test sequence
	db.ExecContext(ctx, fmt.Sprintf("DROP SEQUENCE %s.%s", schema, seqName))

	// Create test sequence starting at 1
	_, err = db.ExecContext(ctx, fmt.Sprintf(
		"CREATE SEQUENCE %s.%s START WITH 1 INCREMENT BY 1 NOCACHE", schema, seqName))
	if err != nil {
		t.Fatalf("Failed to create test sequence: %v", err)
	}
	defer db.ExecContext(ctx, fmt.Sprintf("DROP SEQUENCE %s.%s", schema, seqName))

	tests := []struct {
		name       string
		targetVal  int64
		wantResult bool
	}{
		{
			name:       "reset to higher value",
			targetVal:  100,
			wantResult: true,
		},
		{
			name:       "reset to even higher value",
			targetVal:  500,
			wantResult: true,
		},
		{
			name:       "reset to lower value (negative increment)",
			targetVal:  50,
			wantResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := w.resetExplicitSequence(ctx, schema, seqName, tt.targetVal)
			if result != tt.wantResult {
				t.Errorf("resetExplicitSequence() = %v, want %v", result, tt.wantResult)
			}

			// Verify the sequence is at the expected value
			var nextVal int64
			err := db.QueryRowContext(ctx,
				fmt.Sprintf("SELECT %s.%s.NEXTVAL FROM DUAL", schema, seqName)).Scan(&nextVal)
			if err != nil {
				t.Errorf("Failed to get NEXTVAL: %v", err)
				return
			}

			// The sequence should be at targetVal (or targetVal+1 after NEXTVAL call)
			// Since we just called NEXTVAL, it should return targetVal
			if nextVal != tt.targetVal {
				t.Errorf("Sequence NEXTVAL = %d, want %d", nextVal, tt.targetVal)
			}
		})
	}
}

func TestResetExplicitSequence_NonExistent(t *testing.T) {
	dsn := getTestDSN()
	db, err := sql.Open("godror", dsn)
	if err != nil {
		t.Skipf("Cannot connect to Oracle: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("Cannot ping Oracle: %v", err)
	}

	ctx := context.Background()
	dialect := &Dialect{}
	w := &Writer{
		db:      db,
		dialect: dialect,
	}

	// Test with non-existent sequence
	result := w.resetExplicitSequence(ctx, "SO2010", "NONEXISTENT_SEQ_XYZ", 100)
	if result != false {
		t.Errorf("resetExplicitSequence() for non-existent sequence = %v, want false", result)
	}
}

func TestResetExplicitSequence_AlreadyAtValue(t *testing.T) {
	dsn := getTestDSN()
	db, err := sql.Open("godror", dsn)
	if err != nil {
		t.Skipf("Cannot connect to Oracle: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("Cannot ping Oracle: %v", err)
	}

	ctx := context.Background()
	dialect := &Dialect{}
	w := &Writer{
		db:      db,
		dialect: dialect,
	}

	seqName := "TEST_SEQ_ALREADY_AT"
	schema := "SO2010"

	// Cleanup and create sequence starting at 99
	db.ExecContext(ctx, fmt.Sprintf("DROP SEQUENCE %s.%s", schema, seqName))
	_, err = db.ExecContext(ctx, fmt.Sprintf(
		"CREATE SEQUENCE %s.%s START WITH 99 INCREMENT BY 1 NOCACHE", schema, seqName))
	if err != nil {
		t.Fatalf("Failed to create test sequence: %v", err)
	}
	defer db.ExecContext(ctx, fmt.Sprintf("DROP SEQUENCE %s.%s", schema, seqName))

	// Reset to 100 - sequence is at 99, NEXTVAL will be 99, so we want 100
	// The increment will be: 100 - 99 - 1 = 0, so it should return true immediately
	result := w.resetExplicitSequence(ctx, schema, seqName, 100)
	if result != true {
		t.Errorf("resetExplicitSequence() = %v, want true", result)
	}
}
