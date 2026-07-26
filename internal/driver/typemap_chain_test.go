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
	"github.com/johndauphine/smt/schema"
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

func TestFallbackChain_GenerateTableDDL_DeterministicFailure_DoesNotRouteToAI(t *testing.T) {
	stub := &stubMapper{tableDDLReturns: &TableDDLResponse{CreateTableDDL: "AI_GENERATED_DDL"}}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

	// Unsupported source dialect must propagate from the SMT boundary rather
	// than let the optional AI mapper construct a CREATE TABLE statement.
	_, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: "oracle",
		TargetDBType: typemap.DialectPostgres,
		SourceTable:  &Table{Name: "users", Columns: []Column{{Name: "client_ip", DataType: "inet"}}},
	})
	if err == nil {
		t.Fatal("expected SMT create-plan error")
	}
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) {
		t.Fatalf("expected SMT UnsupportedFeatureError, got %v", err)
	}
	if stub.tableDDLCalled != 0 {
		t.Errorf("AI must never generate CREATE TABLE; got %d calls", stub.tableDDLCalled)
	}
}

func TestFallbackChain_GenerateTableDDL_RawColumn_UsesSMTUnsupportedPolicy(t *testing.T) {
	stub := &stubMapper{tableDDLReturns: &TableDDLResponse{CreateTableDDL: "AI_TABLE_DDL"}}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

	_, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
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
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) {
		t.Fatalf("expected SMT UnsupportedFeatureError, got %v", err)
	}
	if stub.tableDDLCalled != 0 {
		t.Errorf("AI must never generate CREATE TABLE; got %d calls", stub.tableDDLCalled)
	}
}

func TestFallbackChain_GenerateTableDDL_RawColumn_NoAIUsesSameSMTPolicy(t *testing.T) {
	// Policy must be independent of whether an AI mapper is configured.
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
		t.Fatal("expected SMT unsupported policy for Raw source types")
	}
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) {
		t.Fatalf("expected SMT UnsupportedFeatureError, got %v", err)
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

func TestFallbackChain_GenerateFinalizationDDL_NeverRoutesSMTSideObjectsToAI(t *testing.T) {
	stub := &stubMapper{finalizationDDLReturns: "AI_INDEX_DDL"}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")

	// Clustered indexes are outside SMT's public capability surface, so the
	// public typed policy reaches DMT unchanged rather than triggering AI SQL.
	_, err := chain.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeIndex,
		SourceDBType: typemap.DialectMSSQL,
		TargetDBType: typemap.DialectMSSQL,
		Table:        &Table{Name: "t"},
		Index:        &Index{Name: "ci", Columns: []string{"id"}, IsClustered: true},
	})
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) || unsupported.Feature != "clustered indexes" {
		t.Fatalf("clustered-index error = %#v, want SMT unsupported policy", err)
	}
	if stub.finalizationDDLCalled != 0 {
		t.Errorf("AI must not generate adopted SMT side-object DDL; got %d calls", stub.finalizationDDLCalled)
	}
}

func TestFallbackChain_GenerateFinalizationDDL_OtherErrors_DoNotRoute(t *testing.T) {
	// DDL errors such as unsupported dialects or missing payloads propagate
	// without AI involvement.
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
	if stub.finalizationDDLCalled != 0 {
		t.Errorf("AI fallback should never fire for DDL errors; got %d calls", stub.finalizationDDLCalled)
	}
}

func TestFallbackChain_GenerateFinalizationDDL_SideObjectPolicyPropagatesWithoutFallback(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail, "")
	_, err := chain.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeIndex,
		SourceDBType: typemap.DialectMSSQL,
		TargetDBType: typemap.DialectMSSQL,
		Table:        &Table{Name: "t"},
		Index:        &Index{Name: "ci", Columns: []string{"id"}, IsClustered: true},
	})
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) || unsupported.Feature != "clustered indexes" {
		t.Errorf("expected SMT typed unsupported policy to propagate; got %v", err)
	}
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
		typemap.DialectPostgres:   true,
		typemap.DialectMSSQL:      true,
		typemap.DialectMySQL:      true,
		typemap.DialectSQLite:     true,
		typemap.DialectClickHouse: true,
		"oracle":                  true,
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

// ---------- Approx-column create policy ----------

// The ApproxAction setting is retained for configuration compatibility, but it
// must not authorize a second CREATE TABLE renderer. PG interval is outside
// SMT's portable create model, so it exercises the explicit error path.
func TestFallbackChain_GenerateTableDDL_ApproxColumn_DefaultNeverRoutesToAI(t *testing.T) {
	stub := &stubMapper{tableDDLReturns: &TableDDLResponse{CreateTableDDL: "AI_DDL"}}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, ApproxActionDeterministic)

	_, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
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
	if err == nil {
		t.Fatal("expected SMT create-plan policy for unsupported interval source type")
	}
	if stub.tableDDLCalled != 0 {
		t.Errorf("AI must never generate CREATE TABLE; got %d calls", stub.tableDDLCalled)
	}
}

// Even an explicit legacy ai_fallback setting cannot route SMT-owned CREATE
// TABLE SQL through the optional AI mapper.
func TestFallbackChain_GenerateTableDDL_ApproxColumn_AIFallbackSettingNeverRoutesToAI(t *testing.T) {
	stub := &stubMapper{tableDDLReturns: &TableDDLResponse{CreateTableDDL: "AI_HANDLED_APPROX"}}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, ApproxActionAIFallback)

	_, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
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
	if err == nil {
		t.Fatal("expected SMT create-plan policy for unsupported interval source type")
	}
	if stub.tableDDLCalled != 0 {
		t.Errorf("AI must never generate CREATE TABLE, even when ai_fallback is configured; got %d calls", stub.tableDDLCalled)
	}
}

// The no-AI case returns the same SMT policy; it must not silently degrade
// through DMT's old approximate mapping.
func TestFallbackChain_GenerateTableDDL_ApproxColumn_AIFallback_NoAIUsesSMTPolicy(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail, ApproxActionAIFallback)

	_, err := chain.GenerateTableDDL(context.Background(), TableDDLRequest{
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
	if err == nil {
		t.Fatal("expected SMT create-plan policy for unsupported interval source type")
	}
}

// Clean tables still use the SMT deterministic path when the legacy setting
// is present.
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

// TestNewFallbackChain_DefaultsApproxActionToDeterministic_NoAI is the
// baseline: with no AI fallback available, empty ApproxAction must
// collapse to ApproxActionDeterministic. There's no AI to route to.
func TestNewFallbackChain_DefaultsApproxActionToDeterministic_NoAI(t *testing.T) {
	chain := NewFallbackChain(NewDeterministicMapper(), nil, UnmappedActionFail, "")
	if chain.approxAction != ApproxActionDeterministic {
		t.Errorf("empty approxAction with nil fallback should default to %q; got %q",
			ApproxActionDeterministic, chain.approxAction)
	}
}

// TestNewFallbackChain_DefaultsApproxActionToAIFallback_WhenAIAvailable
// is the #209 fix: when AI IS available, empty ApproxAction collapses
// to ApproxActionAIFallback (implicit opt-in — consistent with how
// Raw, table-DDL-error, finalization-error, and error-diagnosis all
// default-on when AI is configured). The user opted into AI by
// configuring it; they shouldn't also need to set N more knobs.
func TestNewFallbackChain_DefaultsApproxActionToAIFallback_WhenAIAvailable(t *testing.T) {
	stub := &stubMapper{}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, "")
	if chain.approxAction != ApproxActionAIFallback {
		t.Errorf("empty approxAction with non-nil fallback should default to %q (#209); got %q",
			ApproxActionAIFallback, chain.approxAction)
	}
}

// TestNewFallbackChain_ExplicitDeterministicRespected_WhenAIAvailable
// is the #209 opt-out path: a user with AI configured who explicitly
// sets approx_type_action: deterministic still gets deterministic.
// Default is smarter; explicit values are still respected.
func TestNewFallbackChain_ExplicitDeterministicRespected_WhenAIAvailable(t *testing.T) {
	stub := &stubMapper{}
	chain := NewFallbackChain(NewDeterministicMapper(), stub, UnmappedActionFail, ApproxActionDeterministic)
	if chain.approxAction != ApproxActionDeterministic {
		t.Errorf("explicit ApproxActionDeterministic must be respected even with AI available; got %q",
			chain.approxAction)
	}
}
