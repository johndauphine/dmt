package typemap

import (
	"reflect"
	"testing"
)

// TestRegistryBuiltins verifies the four built-in mappers are registered
// and dispatch identically to the pre-#479 hardcoded switch.
func TestRegistryBuiltins(t *testing.T) {
	want := []string{DialectMSSQL, DialectMySQL, DialectPostgres, DialectSQLite}
	if got := SupportedDialects(); !reflect.DeepEqual(got, want) {
		t.Fatalf("SupportedDialects() = %v, want %v", got, want)
	}
	col := ColumnInfo{Name: "id", UDTName: "int"}
	if ct := ToCanonical(col, DialectMSSQL); ct.Kind != KindInteger {
		t.Errorf("mssql int -> %v, want KindInteger", ct.Kind)
	}
	if ddl := FromCanonical(CanonicalType{Kind: KindInteger}, DialectPostgres); ddl.SQLType != "INTEGER" {
		t.Errorf("canonical Integer -> pg %q, want INTEGER", ddl.SQLType)
	}
}

// TestRegisterCatalogMapper is the #479/#191 contract: a catalog engine
// registers by name and immediately participates in ToCanonical /
// FromCanonical / Supported with no package changes.
func TestRegisterCatalogMapper(t *testing.T) {
	// The registry is package-global: restore it so repeated runs
	// (-count=2) and shuffled test order stay hermetic (codex review).
	t.Cleanup(func() {
		registryMu.Lock()
		delete(dialectMappers, "testdb479")
		registryMu.Unlock()
	})
	Register("testdb479", DialectMapper{
		ToCanonical: func(col ColumnInfo) CanonicalType {
			return CanonicalType{Kind: KindText}
		},
		FromCanonical: func(ct CanonicalType) DdlType {
			return DdlType{SQLType: "TESTTEXT"}
		},
	})
	if !Supported("testdb479") {
		t.Fatal("registered dialect not Supported")
	}
	if ct := ToCanonical(ColumnInfo{DataType: "whatever"}, "testdb479"); ct.Kind != KindText {
		t.Errorf("registered ToCanonical not dispatched: %v", ct.Kind)
	}
	if ddl := FromCanonical(CanonicalType{Kind: KindInteger}, "testdb479"); ddl.SQLType != "TESTTEXT" {
		t.Errorf("registered FromCanonical not dispatched: %q", ddl.SQLType)
	}
}

func TestRegisterRejectsDuplicatesAndPartial(t *testing.T) {
	mustPanic := func(name string, m DialectMapper) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("Register(%q) did not panic", name)
			}
		}()
		Register(name, m)
	}
	mustPanic(DialectPostgres, DialectMapper{
		ToCanonical:   func(ColumnInfo) CanonicalType { return CanonicalType{} },
		FromCanonical: func(CanonicalType) DdlType { return DdlType{} },
	})
	mustPanic("partial479", DialectMapper{})
}
