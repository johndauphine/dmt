package target

import (
	"strings"
	"testing"
)

func TestDetectPGIdentifierCollisions_NoCollisions(t *testing.T) {
	tables := []TableInfo{
		&mockTable{name: "Users", columns: []string{"UserID", "FirstName"}},
		&mockTable{name: "Orders", columns: []string{"id", "Order-Date"}},
	}
	if err := DetectPGIdentifierCollisions(tables); err != nil {
		t.Fatalf("expected no collision error, got: %v", err)
	}
}

func TestDetectPGIdentifierCollisions_TableNames(t *testing.T) {
	// Regression for #553: three distinct source tables sanitize to the same
	// PostgreSQL name; in drop_recreate two of them would be silently
	// destroyed. Detection must fail before any DDL.
	tables := []TableInfo{
		&mockTable{name: "Order Items", columns: []string{"id"}},
		&mockTable{name: "Order-Items", columns: []string{"id"}},
		&mockTable{name: "Order_Items", columns: []string{"id"}},
	}
	err := DetectPGIdentifierCollisions(tables)
	if err == nil {
		t.Fatal("expected a collision error for colliding table names, got nil")
	}
	msg := err.Error()
	for _, want := range []string{"Order Items", "Order-Items", "Order_Items", "order_items"} {
		if !strings.Contains(msg, want) {
			t.Errorf("collision error missing %q; got: %s", want, msg)
		}
	}
}

func TestDetectPGIdentifierCollisions_CaseOnlyTableNames(t *testing.T) {
	// Under a case-sensitive source collation, Users and USERS are distinct
	// tables but both sanitize to "users".
	tables := []TableInfo{
		&mockTable{name: "Users", columns: []string{"id"}},
		&mockTable{name: "USERS", columns: []string{"id"}},
	}
	if err := DetectPGIdentifierCollisions(tables); err == nil {
		t.Fatal("expected a collision error for case-only-distinct table names, got nil")
	}
}

func TestDetectPGIdentifierCollisions_Columns(t *testing.T) {
	// Colliding columns within one table would emit a CREATE TABLE with
	// duplicate columns and fail confusingly; detect it up front.
	tables := []TableInfo{
		&mockTable{name: "widgets", columns: []string{"Col A", "Col-A", "other"}},
	}
	err := DetectPGIdentifierCollisions(tables)
	if err == nil {
		t.Fatal("expected a collision error for colliding column names, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "widgets") || !strings.Contains(msg, "col_a") {
		t.Errorf("column collision error missing table/sanitized name; got: %s", msg)
	}
	// A non-colliding column in the same table must not be reported.
	if strings.Contains(msg, `"other"`) {
		t.Errorf("non-colliding column should not appear in error; got: %s", msg)
	}
}

func TestDetectPGIdentifierCollisions_SameColumnNameAcrossTablesIsFine(t *testing.T) {
	// "id" in two different tables is not a collision — columns are scoped
	// per table.
	tables := []TableInfo{
		&mockTable{name: "a", columns: []string{"id", "name"}},
		&mockTable{name: "b", columns: []string{"id", "name"}},
	}
	if err := DetectPGIdentifierCollisions(tables); err != nil {
		t.Fatalf("same column names across tables should not collide, got: %v", err)
	}
}
