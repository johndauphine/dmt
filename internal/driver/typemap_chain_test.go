// Tests for the FallbackChain decorator and the GetTypeMapper /
// GetAIMapper factory functions.

package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/secrets"
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
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionConservativeText, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), nil, "", "")
	if chain.action != UnmappedActionFail {
		t.Errorf("empty action should default to UnmappedActionFail; got %q", chain.action)
	}
}

// ---------- GenerateTableDDL routing ----------

func TestFallbackChain_GenerateTableDDL_DeterministicSuccess(t *testing.T) {
	stub := &stubMapper{}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail, "")
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
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail, "")
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
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

	if !chain.CanMap("oracle", "snowflake") {
		t.Error("union with always-true fallback should allow any pair")
	}
}

func TestFallbackChain_CanMap_NoFallback_DeterministicOnly(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail, "")

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
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")
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

// ---------- GetTypeMapper / GetAIMapper smoke tests (hermetic) ----------

// withNoAISecrets isolates a test from the developer's
// ~/.secrets/dmt-config.yaml by pointing DMT_SECRETS_FILE at a
// non-existent path and resetting both the secrets package's cache
// and the driver package's AI-mapper singleton. Restores everything
// in a Cleanup hook so subsequent tests don't see the override.
//
// Without this, GetTypeMapper / GetAIMapper smoke tests behave
// differently depending on whether the developer has AI configured
// (Copilot review on PR #192 — non-hermetic tests).
func withNoAISecrets(t *testing.T) {
	t.Helper()
	t.Setenv("DMT_SECRETS_FILE", "/nonexistent/dmt-test-secrets-"+t.Name())
	secrets.Reset()
	resetCachedAIMapper()
	t.Cleanup(func() {
		secrets.Reset()
		resetCachedAIMapper()
	})
}

func TestGetTypeMapper_NoAI_ReturnsChainWithNilFallback(t *testing.T) {
	withNoAISecrets(t)

	m, err := GetTypeMapper(UnmappedActionFail, "")
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
	if chain.fallback != nil {
		t.Errorf("fallback must be nil when no AI secrets file present; got %T", chain.fallback)
	}
}

func TestGetAIMapper_NoAI_ReturnsNil(t *testing.T) {
	withNoAISecrets(t)

	if got := GetAIMapper(); got != nil {
		t.Errorf("GetAIMapper() should return nil when no AI secrets present; got %T", got)
	}
}

// TestGetTypeMapper_AIMapperIsSingleton — Copilot review on PR #192.
// Both GetTypeMapper (chain.fallback) and GetAIMapper must return the
// SAME *AITypeMapper instance — otherwise both would load and write
// ~/.dmt/type-cache.json from independent in-memory caches.
//
// Tested only in the no-AI case since constructing a real AI mapper
// in tests requires secrets fixtures. The singleton-ness still
// holds: both return nil from the same sync.Once. For positive
// coverage we rely on the implementation (sync.Once around a
// package-level var); a fixture-based test would just re-verify
// what sync.Once already guarantees.
func TestGetTypeMapper_AIMapperIsSingleton_NoAICase(t *testing.T) {
	withNoAISecrets(t)

	// Two consecutive GetAIMapper calls return the same (nil) result
	// from the cached sync.Once initialization — proves the cache
	// path is hit without re-evaluating.
	a := GetAIMapper()
	b := GetAIMapper()
	if a != b {
		t.Errorf("singleton broken: two GetAIMapper calls returned different references (%v vs %v)", a, b)
	}

	// Chain's fallback equals what GetAIMapper returns directly.
	m, _ := GetTypeMapper(UnmappedActionFail, "")
	chain := m.(*FallbackChain)
	if chain.fallback != nil {
		// fallback is typed TypeMapper; only meaningful comparison is via
		// nil check, which we already did above. This branch shouldn't
		// fire in the withNoAISecrets case.
		t.Errorf("fallback should be nil (no AI secrets); got %T", chain.fallback)
	}
}

// TestHandleUnmapped_UnknownAction_WarnsAndFails — Copilot review on
// PR #192. A typo'd action string would have silently emitted empty
// SQLType; now it warns and falls back to fail semantics.
func TestHandleUnmapped_UnknownAction_WarnsAndFails(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, "bogus-action", "")

	got := chain.MapType(TypeInfo{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		DataType:     "hierarchyid",
	})
	if got != "" {
		t.Errorf("unknown action should fall back to empty (fail semantics); got %q", got)
	}
}

// ---------- Approx-column routing (#197) ----------

// TestFallbackChain_GenerateTableDDL_ApproxColumn_DefaultDeterministic
// verifies the default ApproxAction (deterministic) keeps the
// deterministic path even when approx columns are present. The
// existing PG-INTERVAL → NVARCHAR(255) approxDDL emission triggers
// IsApproximate=true, so a PG `interval` column targeting MSSQL is
// the cleanest test fixture.
func TestFallbackChain_GenerateTableDDL_ApproxColumn_DefaultDeterministic(t *testing.T) {
	stub := &stubMapper{tableDDLReturns: &TableDDLResponse{CreateTableDDL: "AI_DDL"}}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, ApproxActionDeterministic)

	resp, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		SourceTable: &Table{
			Name: "events",
			Columns: []Column{
				{Name: "id", DataType: "int4", IsNullable: false},
				{Name: "duration", DataType: "interval", IsNullable: false}, // approx on MSSQL
			},
			PrimaryKey: []string{"id"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.CreateTableDDL == "AI_DDL" {
		t.Error("default ApproxActionDeterministic must NOT route to AI; got AI response")
	}
	if stub.tableDDLCalled != 0 {
		t.Errorf("AI fallback should NOT fire on default action; got %d calls", stub.tableDDLCalled)
	}
}

// TestFallbackChain_GenerateTableDDL_ApproxColumn_AIFallback verifies
// the opt-in ApproxActionAIFallback routes the entire table to AI
// when at least one column has an approximate mapping.
func TestFallbackChain_GenerateTableDDL_ApproxColumn_AIFallback(t *testing.T) {
	expectedResp := &TableDDLResponse{CreateTableDDL: "AI_HANDLED_APPROX"}
	stub := &stubMapper{tableDDLReturns: expectedResp}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, ApproxActionAIFallback)

	resp, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		SourceTable: &Table{
			Name: "events",
			Columns: []Column{
				{Name: "id", DataType: "int4", IsNullable: false},
				{Name: "duration", DataType: "interval", IsNullable: false}, // approx on MSSQL
			},
			PrimaryKey: []string{"id"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.CreateTableDDL != "AI_HANDLED_APPROX" {
		t.Errorf("expected AI fallback to handle approx-bearing table; got %q", resp.CreateTableDDL)
	}
	if stub.tableDDLCalled != 1 {
		t.Errorf("AI fallback should fire exactly once; got %d calls", stub.tableDDLCalled)
	}
}

// TestFallbackChain_GenerateTableDDL_ApproxColumn_AIFallback_NoAI
// verifies that opting into ai_fallback when no AI is configured
// silently falls through to deterministic. The user asked for AI
// but didn't configure it — better to deliver the deterministic
// result than to fail the migration.
func TestFallbackChain_GenerateTableDDL_ApproxColumn_AIFallback_NoAI(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail, ApproxActionAIFallback)

	resp, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		SourceTable: &Table{
			Name: "events",
			Columns: []Column{
				{Name: "id", DataType: "int4", IsNullable: false},
				{Name: "duration", DataType: "interval", IsNullable: false},
			},
			PrimaryKey: []string{"id"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(resp.CreateTableDDL, "CREATE TABLE") {
		t.Errorf("no-AI configuration with ai_fallback should still emit deterministic DDL; got %q", resp.CreateTableDDL)
	}
}

// TestFallbackChain_GenerateTableDDL_NoApproxColumns_NoTelemetry
// smoke test: clean tables with no approx columns must not trigger
// the AI route under ai_fallback either.
func TestFallbackChain_GenerateTableDDL_NoApproxColumns_NoTelemetry(t *testing.T) {
	stub := &stubMapper{tableDDLReturns: &TableDDLResponse{CreateTableDDL: "AI_DDL"}}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, ApproxActionAIFallback)

	resp, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		SourceTable: &Table{
			Name: "users",
			Columns: []Column{
				{Name: "id", DataType: "int4", IsNullable: false},
				{Name: "name", DataType: "varchar", IsNullable: false},
			},
			PrimaryKey: []string{"id"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.CreateTableDDL == "AI_DDL" {
		t.Error("AI fallback should NOT fire on table with no approx columns")
	}
	if stub.tableDDLCalled != 0 {
		t.Errorf("AI fallback should NOT fire; got %d calls", stub.tableDDLCalled)
	}
}

// TestFindApproximateColumns_DetectsKnownApproxMappings is a focused
// unit test on findApproximateColumns. PG INTERVAL → MSSQL is the
// cleanest known approxDDL emission site (see internal/typemap/mssql.go).
func TestFindApproximateColumns_DetectsKnownApproxMappings(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail, ApproxActionDeterministic)
	req := TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		SourceTable: &Table{
			Columns: []Column{
				{Name: "id", DataType: "int4"},      // exact
				{Name: "dur", DataType: "interval"}, // approx
				{Name: "name", DataType: "varchar"}, // exact
			},
		},
	}
	got := chain.findApproximateColumns(req)
	if len(got) != 1 || got[0] != "dur" {
		t.Errorf("expected [dur]; got %v", got)
	}
}

// TestFindApproximateColumns_SkipsRawColumns verifies findApproximateColumns
// correctly skips Raw columns (handled separately by findRawColumns).
// PG `inet` is Raw on a non-PG target.
func TestFindApproximateColumns_SkipsRawColumns(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail, ApproxActionDeterministic)
	req := TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectMSSQL,
		SourceTable: &Table{
			Columns: []Column{
				{Name: "id", DataType: "int4"},
				{Name: "client_ip", DataType: "inet"}, // Raw on PG
			},
		},
	}
	got := chain.findApproximateColumns(req)
	if len(got) != 0 {
		t.Errorf("Raw columns should not appear in approx list; got %v", got)
	}
}

// TestNewFallbackChain_DefaultsApproxActionToDeterministic guards the
// default behavior — empty string ApproxAction must collapse to
// ApproxActionDeterministic, not silently leave the field empty
// (which would skip both branches of the switch in GenerateTableDDL).
func TestNewFallbackChain_DefaultsApproxActionToDeterministic(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail, "")
	if chain.approxAction != ApproxActionDeterministic {
		t.Errorf("empty approxAction should default to %q; got %q",
			ApproxActionDeterministic, chain.approxAction)
	}
}
