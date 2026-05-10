// Tests for the FallbackChain decorator and the GetTypeMapper /
// GetAIMapper factory functions.

package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/typemap"
)

// stubMapper is a minimal TypeMapper used to stand in for the AI
// mapper in tests. Records call counts so tests can verify routing.
type stubMapper struct {
	mapTypeReturns          string
	mapTypeCalled           int
	tableDDLReturns         *TableDDLResponse
	tableDDLErr             error
	tableDDLCalled          int
	finalizationDDLReturns  string
	finalizationDDLErr      error
	finalizationDDLCalled   int
	supportedTargetsReturns []string
}

func (s *stubMapper) MapType(info TypeInfo) string {
	s.mapTypeCalled++
	return s.mapTypeReturns
}
func (s *stubMapper) CanMap(_, _ string) bool { return true }
func (s *stubMapper) SupportedTargets() []string {
	if s.supportedTargetsReturns == nil {
		return []string{"oracle"} // distinct from the deterministic list
	}
	return s.supportedTargetsReturns
}
func (s *stubMapper) GenerateTableDDL(_ context.Context, _ TableDDLRequest) (*TableDDLResponse, error) {
	s.tableDDLCalled++
	return s.tableDDLReturns, s.tableDDLErr
}
func (s *stubMapper) GenerateFinalizationDDL(_ context.Context, _ FinalizationDDLRequest) (string, error) {
	s.finalizationDDLCalled++
	return s.finalizationDDLReturns, s.finalizationDDLErr
}

// ---------- MapType routing ----------

func TestFallbackChain_MapType_KnownType_NoAIRouting(t *testing.T) {
	stub := &stubMapper{mapTypeReturns: "FROM_AI"}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail)

	got := chain.MapType(TypeInfo{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		DataType:     "int4",
	})

	if got != "INT" {
		t.Errorf("got %q, want INT (deterministic)", got)
	}
	if stub.mapTypeCalled != 0 {
		t.Errorf("AI fallback should NOT fire for known type; got %d calls", stub.mapTypeCalled)
	}
}

func TestFallbackChain_MapType_RawType_RoutesToAI(t *testing.T) {
	stub := &stubMapper{mapTypeReturns: "AI_HANDLED"}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail)

	got := chain.MapType(TypeInfo{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		DataType:     "hierarchyid", // not in canonical PG catalog → KindRaw
	})

	if got != "AI_HANDLED" {
		t.Errorf("got %q, want AI_HANDLED (Raw type should route)", got)
	}
	if stub.mapTypeCalled != 1 {
		t.Errorf("AI fallback should fire exactly once for Raw type; got %d calls", stub.mapTypeCalled)
	}
}

func TestFallbackChain_MapType_RawType_NoAI_FailAction(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail)

	got := chain.MapType(TypeInfo{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		DataType:     "hierarchyid",
	})

	if got != "" {
		t.Errorf("Fail action should emit empty SQLType (downstream fails visibly); got %q", got)
	}
}

func TestFallbackChain_MapType_RawType_NoAI_ConservativeText(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionConservativeText)

	tests := []struct {
		target, want string
	}{
		{typemap.DialectMSSQL, "NVARCHAR(MAX)"},
		{typemap.DialectMySQL, "LONGTEXT"},
		{typemap.DialectPostgres, "TEXT"},
	}
	for _, tc := range tests {
		t.Run(tc.target, func(t *testing.T) {
			got := chain.MapType(TypeInfo{
				SourceDBType: typemap.DialectPostgres,
				TargetDBType: tc.target,
				DataType:     "hierarchyid",
			})
			if got != tc.want {
				t.Errorf("conservative-text on %s: got %q, want %q", tc.target, got, tc.want)
			}
		})
	}
}

func TestFallbackChain_MapType_DefaultsToFailAction(t *testing.T) {
	// Empty action string should default to "fail" — not silently
	// degrade to skip or conservative-text.
	chain := NewFallbackChain(NewDeterministicMapper(), nil, "")
	if chain.action != UnmappedActionFail {
		t.Errorf("empty action should default to UnmappedActionFail; got %q", chain.action)
	}
}

// ---------- GenerateTableDDL routing ----------

func TestFallbackChain_GenerateTableDDL_DeterministicSuccess(t *testing.T) {
	stub := &stubMapper{}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail)

	resp, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectPostgres,
		SourceTable: &Table{
			Name:       "users",
			Columns:    []Column{{Name: "id", DataType: "int4", IsNullable: false}},
			PrimaryKey: []string{"id"},
		},
		TargetSchema: "public",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(resp.CreateTableDDL, "CREATE TABLE") {
		t.Errorf("expected CREATE TABLE in DDL; got %q", resp.CreateTableDDL)
	}
	if stub.tableDDLCalled != 0 {
		t.Errorf("AI fallback should NOT fire when deterministic succeeds; got %d calls", stub.tableDDLCalled)
	}
}

func TestFallbackChain_GenerateTableDDL_DeterministicFailure_RoutesToAI(t *testing.T) {
	expectedResp := &TableDDLResponse{CreateTableDDL: "AI_GENERATED_DDL"}
	stub := &stubMapper{tableDDLReturns: expectedResp}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail)

	// Unsupported source dialect → deterministic returns error → AI fires
	resp, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: "oracle",
		TargetDBType: typemap.DialectPostgres,
		SourceTable:  &Table{Name: "users", Columns: []Column{{Name: "id"}}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.CreateTableDDL != "AI_GENERATED_DDL" {
		t.Errorf("expected AI fallback to provide the response; got %q", resp.CreateTableDDL)
	}
	if stub.tableDDLCalled != 1 {
		t.Errorf("AI fallback should fire exactly once; got %d calls", stub.tableDDLCalled)
	}
}

// TestFallbackChain_GenerateTableDDL_RawColumn_RoutesToAI — Codex
// review on PR #170. The deterministic mapper successfully emits
// CREATE TABLE for tables with Raw columns by passing the source UDT
// name through verbatim — e.g., a PG `inet` column targeting MSSQL
// would emit `[ip] INET ...` (invalid MSSQL). The chain must detect
// Raw columns up front and route the entire table to AI when
// configured, rather than letting deterministic silently produce
// invalid cross-dialect DDL.
func TestFallbackChain_GenerateTableDDL_RawColumn_RoutesToAI(t *testing.T) {
	expectedResp := &TableDDLResponse{CreateTableDDL: "AI_TABLE_DDL"}
	stub := &stubMapper{tableDDLReturns: expectedResp}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail)

	resp, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		SourceTable: &Table{
			Name: "logs",
			Columns: []Column{
				{Name: "id", DataType: "int4", IsNullable: false},
				{Name: "client_ip", DataType: "inet", IsNullable: false}, // Raw on PG
			},
			PrimaryKey: []string{"id"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.CreateTableDDL != "AI_TABLE_DDL" {
		t.Errorf("expected AI fallback to handle table with Raw column; got DDL=%q (#170 regression — would have emitted invalid INET cross-dialect)",
			resp.CreateTableDDL)
	}
	if stub.tableDDLCalled != 1 {
		t.Errorf("expected exactly one AI call; got %d", stub.tableDDLCalled)
	}
}

func TestFallbackChain_GenerateTableDDL_RawColumn_NoAI_ErrorsClearly(t *testing.T) {
	// Same scenario but no AI configured — the chain must error with
	// a clear message naming the offending columns rather than letting
	// deterministic emit invalid DDL.
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail)

	_, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		SourceTable: &Table{
			Name: "logs",
			Columns: []Column{
				{Name: "id", DataType: "int4"},
				{Name: "client_ip", DataType: "inet"},
				{Name: "subnet", DataType: "cidr"},
			},
			PrimaryKey: []string{"id"},
		},
	})
	if err == nil {
		t.Fatal("expected error for table with Raw columns + no AI fallback (#170 regression)")
	}
	// Error message should name both Raw columns
	for _, col := range []string{"client_ip", "subnet"} {
		if !strings.Contains(err.Error(), col) {
			t.Errorf("error should name unmapped column %q; got %q", col, err.Error())
		}
	}
}

func TestFallbackChain_GenerateTableDDL_NoRawColumns_DeterministicPath(t *testing.T) {
	// Sanity check: a fully-mapped table doesn't trigger the Raw-column
	// gate; deterministic handles it and AI is never called.
	stub := &stubMapper{}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail)

	resp, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		SourceTable: &Table{
			Name: "users",
			Columns: []Column{
				{Name: "id", DataType: "int4"},
				{Name: "email", DataType: "varchar", MaxLength: 255},
			},
			PrimaryKey: []string{"id"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(resp.CreateTableDDL, "CREATE TABLE") {
		t.Errorf("expected deterministic CREATE TABLE; got %q", resp.CreateTableDDL)
	}
	if stub.tableDDLCalled != 0 {
		t.Errorf("AI must NOT be called for tables with no Raw columns; got %d calls", stub.tableDDLCalled)
	}
}

func TestFallbackChain_GenerateTableDDL_NoFallback_ErrorPropagates(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail)
	_, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: "oracle",
		TargetDBType: typemap.DialectPostgres,
		SourceTable:  &Table{Name: "x"},
	})
	if err == nil {
		t.Error("expected error when no AI fallback configured and deterministic fails")
	}
}

// ---------- GenerateFinalizationDDL routing ----------

func TestFallbackChain_GenerateFinalizationDDL_RoutesOnErrUnsupportedDDL(t *testing.T) {
	stub := &stubMapper{finalizationDDLReturns: "AI_INDEX_DDL"}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail)

	// Clustered index → deterministic returns ErrUnsupportedDDL → AI fires
	got, err := chain.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeIndex,
		SourceDBType: typemap.DialectMSSQL,
		TargetDBType: typemap.DialectMSSQL,
		Table:        &Table{Name: "t"},
		Index:        &Index{Name: "ci", Columns: []string{"id"}, IsClustered: true},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "AI_INDEX_DDL" {
		t.Errorf("expected AI fallback to provide the DDL; got %q", got)
	}
	if stub.finalizationDDLCalled != 1 {
		t.Errorf("AI fallback should fire once; got %d calls", stub.finalizationDDLCalled)
	}
}

func TestFallbackChain_GenerateFinalizationDDL_OtherErrors_DoNotRoute(t *testing.T) {
	// Errors that aren't ErrUnsupportedDDL — like "unsupported dialect"
	// or "Index field required" — propagate without AI involvement.
	// AI can't recover from them either; shouldn't waste a round-trip.
	stub := &stubMapper{finalizationDDLReturns: "AI_DDL"}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail)

	_, err := chain.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeIndex,
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectPostgres,
		Table:        &Table{Name: "t"},
		// Index: nil — deterministic returns "DDLTypeIndex requires Index field"
	})
	if err == nil {
		t.Fatal("expected error to propagate")
	}
	if errors.Is(err, ErrUnsupportedDDL) {
		t.Error("the propagated error should NOT be ErrUnsupportedDDL")
	}
	if stub.finalizationDDLCalled != 0 {
		t.Errorf("AI fallback should NOT fire for non-Unsupported errors; got %d calls", stub.finalizationDDLCalled)
	}
}

func TestFallbackChain_GenerateFinalizationDDL_RoutesIndex_NoFallback_ErrorPropagates(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail)
	_, err := chain.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeIndex,
		SourceDBType: typemap.DialectMSSQL,
		TargetDBType: typemap.DialectMSSQL,
		Table:        &Table{Name: "t"},
		Index:        &Index{Name: "ci", Columns: []string{"id"}, IsClustered: true},
	})
	if !errors.Is(err, ErrUnsupportedDDL) {
		t.Errorf("expected ErrUnsupportedDDL to propagate when no fallback; got %v", err)
	}
}

// ---------- GenerateDropTableDDL — never routes ----------

func TestFallbackChain_GenerateDropTableDDL_AlwaysDeterministic(t *testing.T) {
	stub := &stubMapper{}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail)

	got, err := chain.GenerateDropTableDDL(context.Background(), DropTableDDLRequest{
		TargetSchema: "public",
		TableName:    "users",
		TargetDBType: typemap.DialectPostgres,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != `DROP TABLE IF EXISTS "users";` {
		t.Errorf("got %q, want DROP TABLE IF EXISTS \"users\";", got)
	}
	// stub is a stubMapper not a TableDropDDLMapper — but even if it
	// were, the chain should never call it for drops.
}

// ---------- CanMap / SupportedTargets union ----------

func TestFallbackChain_CanMap_UnionWithFallback(t *testing.T) {
	stub := &stubMapper{} // CanMap returns true for anything
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail)

	if !chain.CanMap("oracle", "snowflake") {
		t.Error("union with always-true fallback should allow any pair")
	}
}

func TestFallbackChain_CanMap_NoFallback_DeterministicOnly(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail)

	if !chain.CanMap(typemap.DialectPostgres, typemap.DialectMSSQL) {
		t.Error("deterministic should allow PG → MSSQL")
	}
	if chain.CanMap("oracle", typemap.DialectMSSQL) {
		t.Error("oracle source should not be supported when no fallback")
	}
}

func TestFallbackChain_SupportedTargets_UnionDedupes(t *testing.T) {
	stub := &stubMapper{
		supportedTargetsReturns: []string{typemap.DialectMSSQL, "oracle"},
	}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail)
	got := chain.SupportedTargets()

	want := map[string]bool{
		typemap.DialectPostgres: true,
		typemap.DialectMSSQL:    true,
		typemap.DialectMySQL:    true,
		"oracle":                true,
	}
	if len(got) != len(want) {
		t.Errorf("got %d targets, want %d (duplicates dropped); list=%v", len(got), len(want), got)
	}
	for _, d := range got {
		if !want[d] {
			t.Errorf("unexpected target %q in union", d)
		}
	}
}

// ---------- conservativeTextType ----------

func TestConservativeTextType_PerDialect(t *testing.T) {
	tests := []struct {
		dialect, want string
	}{
		{typemap.DialectMSSQL, "NVARCHAR(MAX)"},
		{typemap.DialectMySQL, "LONGTEXT"},
		{typemap.DialectPostgres, "TEXT"},
		{"unknown", "TEXT"}, // safe default
	}
	for _, tc := range tests {
		t.Run(tc.dialect, func(t *testing.T) {
			if got := conservativeTextType(tc.dialect); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------- GetTypeMapper / GetAIMapper smoke tests ----------

func TestGetTypeMapper_NoAI_ReturnsChainWithDeterministicOnly(t *testing.T) {
	// In a test context with no AI secrets configured, GetTypeMapper
	// should still return a non-nil chain — just with nil fallback.
	// The chain delegates everything to the deterministic mapper.
	m, err := GetTypeMapper(UnmappedActionFail)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	chain, ok := m.(*FallbackChain)
	if !ok {
		t.Fatalf("expected *FallbackChain; got %T", m)
	}
	if chain.primary == nil {
		t.Error("primary (deterministic) mapper must be non-nil")
	}
	// fallback may or may not be nil depending on test environment;
	// don't assert.
}

func TestGetAIMapper_NoCrash(t *testing.T) {
	// Just verifies the function doesn't panic — actual nil/non-nil
	// depends on whether ~/.secrets/dmt-config.yaml has AI configured
	// in the test environment.
	_ = GetAIMapper()
}
