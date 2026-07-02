package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/observability"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/source"
	_ "modernc.org/sqlite"
)

func TestPreviewDeleteReconciliationDisabled(t *testing.T) {
	state := &deletePreviewState{}
	orch := &Orchestrator{
		config: deletePreviewConfig(false),
		state:  state,
	}

	got, err := orch.previewDeleteReconciliation(nil, time.Now())
	if err != nil {
		t.Fatalf("previewDeleteReconciliation() error: %v", err)
	}
	if got != nil {
		t.Fatalf("previewDeleteReconciliation() = %#v, want nil", got)
	}
	if state.calls != 0 {
		t.Fatalf("state calls = %d, want 0", state.calls)
	}
}

func TestPreviewDeleteReconciliationDueWithoutPriorSuccess(t *testing.T) {
	orch := &Orchestrator{
		config: deletePreviewConfig(true),
		state:  &deletePreviewState{},
	}

	got, err := orch.previewDeleteReconciliation(deletePreviewTables(), time.Now())
	if err != nil {
		t.Fatalf("previewDeleteReconciliation() error: %v", err)
	}
	if got == nil {
		t.Fatal("previewDeleteReconciliation() = nil, want preview")
	}
	if !got.Due {
		t.Fatal("Due = false, want true")
	}
	if got.Reason != "no previous successful reconciliation" {
		t.Fatalf("Reason = %q", got.Reason)
	}
	if got.EligibleTables != 2 || got.SkippedNoPKTables != 1 {
		t.Fatalf("eligible/skipped = %d/%d, want 2/1", got.EligibleTables, got.SkippedNoPKTables)
	}
}

func TestPreviewDeleteReconciliationUsesInterval(t *testing.T) {
	last := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		now  time.Time
		due  bool
		want string
	}{
		{
			name: "not due",
			now:  last.Add(23 * time.Hour),
			want: "interval has not elapsed",
		},
		{
			name: "due",
			now:  last.Add(24 * time.Hour),
			due:  true,
			want: "interval elapsed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orch := &Orchestrator{
				config: deletePreviewConfig(true),
				state: &deletePreviewState{
					state: &checkpoint.DeleteReconciliationState{
						LastSuccessAt: last,
					},
				},
			}

			got, err := orch.previewDeleteReconciliation(deletePreviewTables(), tt.now)
			if err != nil {
				t.Fatalf("previewDeleteReconciliation() error: %v", err)
			}
			if got.Due != tt.due {
				t.Fatalf("Due = %t, want %t", got.Due, tt.due)
			}
			if got.Reason != tt.want {
				t.Fatalf("Reason = %q, want %q", got.Reason, tt.want)
			}
			if got.LastSuccessAt == nil || !got.LastSuccessAt.Equal(last) {
				t.Fatalf("LastSuccessAt = %v, want %s", got.LastSuccessAt, last)
			}
			wantNext := last.Add(24 * time.Hour)
			if got.NextDueAt == nil || !got.NextDueAt.Equal(wantNext) {
				t.Fatalf("NextDueAt = %v, want %s", got.NextDueAt, wantNext)
			}
		})
	}
}

func TestPreviewDeleteReconciliationNoEligibleTables(t *testing.T) {
	orch := &Orchestrator{
		config: deletePreviewConfig(true),
		state:  &deletePreviewState{},
	}

	got, err := orch.previewDeleteReconciliation([]source.Table{{Name: "logs"}}, time.Now())
	if err != nil {
		t.Fatalf("previewDeleteReconciliation() error: %v", err)
	}
	if got.Due {
		t.Fatal("Due = true, want false")
	}
	if got.Reason != "no eligible primary-key tables" {
		t.Fatalf("Reason = %q", got.Reason)
	}
}

func TestPreviewDeleteReconciliationPropagatesStateErrors(t *testing.T) {
	orch := &Orchestrator{
		config: deletePreviewConfig(true),
		state:  &deletePreviewState{err: errors.New("boom")},
	}

	_, err := orch.previewDeleteReconciliation(deletePreviewTables(), time.Now())
	if err == nil {
		t.Fatal("previewDeleteReconciliation() error = nil, want error")
	}
}

func TestRunDeleteReconciliationDeletesTargetOnlyRows(t *testing.T) {
	sourceDB := openDeleteRuntimeDB(t)
	targetDB := openDeleteRuntimeDB(t)
	execDeleteRuntimeSQL(t, sourceDB, `
		CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
		INSERT INTO items (id, name) VALUES (1, 'one'), (3, 'three');
	`)
	execDeleteRuntimeSQL(t, targetDB, `
		CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
		INSERT INTO items (id, name) VALUES
			(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four');
	`)

	state := &deletePreviewState{}
	orch := deleteRuntimeOrchestrator(sourceDB, targetDB, state)
	result, err := orch.runDeleteReconciliation(
		context.Background(),
		"run-delete",
		[]source.Table{{Name: "items", PrimaryKey: []string{"id"}}},
	)
	if err != nil {
		t.Fatalf("runDeleteReconciliation() error: %v", err)
	}
	if result.CandidateRows != 2 {
		t.Fatalf("CandidateRows = %d, want 2", result.CandidateRows)
	}
	if result.DeletedRows != 2 {
		t.Fatalf("DeletedRows = %d, want 2", result.DeletedRows)
	}
	if state.recordCalls != 1 || state.recordRunID != "run-delete" {
		t.Fatalf("record success calls/run = %d/%q, want 1/run-delete",
			state.recordCalls, state.recordRunID)
	}
	if len(state.savedTables) != 1 {
		t.Fatalf("saved table records = %d, want 1", len(state.savedTables))
	}
	if state.savedTables[0].CandidateRows != 2 || state.savedTables[0].DeletedRows != 2 {
		t.Fatalf("saved candidate/deleted = %d/%d, want 2/2",
			state.savedTables[0].CandidateRows, state.savedTables[0].DeletedRows)
	}
	if got := countDeleteRuntimeRows(t, targetDB); got != 2 {
		t.Fatalf("target row count = %d, want 2", got)
	}
}

func TestCountDeleteReconciliationCandidates(t *testing.T) {
	sourceDB := openDeleteRuntimeDB(t)
	defer sourceDB.Close()
	targetDB := openDeleteRuntimeDB(t)
	defer targetDB.Close()
	execDeleteRuntimeSQL(t, sourceDB, `
		CREATE TABLE items (id INTEGER PRIMARY KEY);
		INSERT INTO items (id) VALUES (1), (3);
	`)
	execDeleteRuntimeSQL(t, targetDB, `
		CREATE TABLE items (id INTEGER PRIMARY KEY);
		INSERT INTO items (id) VALUES (1), (2), (3), (4);
	`)

	orch := deleteRuntimeOrchestrator(sourceDB, targetDB, &deletePreviewState{})
	preview := &DeleteReconciliationPreview{Due: true}
	if err := orch.countDeleteReconciliationCandidates(
		context.Background(),
		[]source.Table{
			{Name: "items", PrimaryKey: []string{"id"}},
			{Name: "logs"},
		},
		preview,
	); err != nil {
		t.Fatalf("countDeleteReconciliationCandidates() error: %v", err)
	}
	if preview.CandidateRows == nil || *preview.CandidateRows != 2 {
		t.Fatalf("CandidateRows = %v, want 2", preview.CandidateRows)
	}
	if len(preview.Tables) != 2 {
		t.Fatalf("table previews = %d, want 2", len(preview.Tables))
	}
	if preview.Tables[0].Table != ".items" || preview.Tables[0].CandidateRows != 2 {
		t.Fatalf("items preview = %#v, want 2 candidates", preview.Tables[0])
	}
	if preview.Tables[1].Table != ".logs" || !preview.Tables[1].Skipped {
		t.Fatalf("logs preview = %#v, want skipped no-PK table", preview.Tables[1])
	}
}

func TestRunDeleteReconciliationSkipsWhenIntervalNotDue(t *testing.T) {
	sourceDB := openDeleteRuntimeDB(t)
	targetDB := openDeleteRuntimeDB(t)
	execDeleteRuntimeSQL(t, sourceDB, `
		CREATE TABLE items (id INTEGER PRIMARY KEY);
		INSERT INTO items (id) VALUES (1);
	`)
	execDeleteRuntimeSQL(t, targetDB, `
		CREATE TABLE items (id INTEGER PRIMARY KEY);
		INSERT INTO items (id) VALUES (1), (2);
	`)

	state := &deletePreviewState{
		state: &checkpoint.DeleteReconciliationState{
			LastSuccessAt: time.Now().UTC(),
		},
	}
	orch := deleteRuntimeOrchestrator(sourceDB, targetDB, state)
	result, err := orch.runDeleteReconciliation(
		context.Background(),
		"run-not-due",
		[]source.Table{{Name: "items", PrimaryKey: []string{"id"}}},
	)
	if err != nil {
		t.Fatalf("runDeleteReconciliation() error: %v", err)
	}
	if result.Preview == nil || result.Preview.Due {
		t.Fatalf("Preview.Due = %v, want false", result.Preview)
	}
	if result.DeletedRows != 0 {
		t.Fatalf("DeletedRows = %d, want 0", result.DeletedRows)
	}
	if state.recordCalls != 0 {
		t.Fatalf("record success calls = %d, want 0", state.recordCalls)
	}
	if got := countDeleteRuntimeRows(t, targetDB); got != 2 {
		t.Fatalf("target row count = %d, want unchanged 2", got)
	}
}

func TestRunDeleteReconciliationSummarizesAllNoPKTables(t *testing.T) {
	sourceDB := openDeleteRuntimeDB(t)
	defer sourceDB.Close()
	targetDB := openDeleteRuntimeDB(t)
	defer targetDB.Close()

	state := &deletePreviewState{}
	orch := deleteRuntimeOrchestrator(sourceDB, targetDB, state)
	result, err := orch.runDeleteReconciliation(
		context.Background(),
		"run-delete",
		[]source.Table{{Name: "logs"}},
	)
	if err != nil {
		t.Fatalf("runDeleteReconciliation() error: %v", err)
	}
	if result.Preview == nil || result.Preview.Due {
		t.Fatalf("Preview.Due = %v, want false for no eligible tables", result.Preview)
	}
	if len(result.TableResults) != 1 || !result.TableResults[0].Skipped {
		t.Fatalf("table results = %#v, want one skipped result", result.TableResults)
	}
	if len(state.savedTables) != 1 {
		t.Fatalf("saved table records = %d, want 1", len(state.savedTables))
	}
	if state.savedTables[0].TableName != ".logs" ||
		!state.savedTables[0].Skipped ||
		state.savedTables[0].SkipReason != "no primary key" {
		t.Fatalf("saved no-PK record = %#v, want skipped .logs", state.savedTables[0])
	}
	if state.recordCalls != 0 {
		t.Fatalf("record success calls = %d, want 0", state.recordCalls)
	}
}

func TestReconcileDeletesIfDueSetsStrictValidation(t *testing.T) {
	sourceDB := openDeleteRuntimeDB(t)
	defer sourceDB.Close()
	targetDB := openDeleteRuntimeDB(t)
	defer targetDB.Close()
	execDeleteRuntimeSQL(t, sourceDB, `
		CREATE TABLE items (id INTEGER PRIMARY KEY);
		INSERT INTO items (id) VALUES (1);
	`)
	execDeleteRuntimeSQL(t, targetDB, `
		CREATE TABLE items (id INTEGER PRIMARY KEY);
		INSERT INTO items (id) VALUES (1), (2);
	`)

	state := &deletePreviewState{}
	orch := deleteRuntimeOrchestrator(sourceDB, targetDB, state)
	if err := orch.reconcileDeletesIfDue(
		context.Background(),
		"run-delete",
		[]source.Table{{Name: "items", PrimaryKey: []string{"id"}}},
	); err != nil {
		t.Fatalf("reconcileDeletesIfDue() error: %v", err)
	}
	if !orch.deleteReconciliationStrictValidation {
		t.Fatal("deleteReconciliationStrictValidation = false, want true after successful reconciliation")
	}
}

func TestReconcileDeletesIfDueAbortsNearTotalTargetOnlyKeys(t *testing.T) {
	sourceDB := openDeleteRuntimeDB(t)
	defer sourceDB.Close()
	targetDB := openDeleteRuntimeDB(t)
	defer targetDB.Close()
	execDeleteRuntimeSQL(t, sourceDB, `CREATE TABLE items (id INTEGER PRIMARY KEY);`)
	execDeleteRuntimeSQL(t, targetDB, `
		CREATE TABLE items (id INTEGER PRIMARY KEY);
		INSERT INTO items (id) VALUES (1), (2), (3);
	`)

	state := &deletePreviewState{}
	orch := deleteRuntimeOrchestrator(sourceDB, targetDB, state)
	err := orch.reconcileDeletesIfDue(
		context.Background(),
		"run-delete",
		[]source.Table{{Name: "items", PrimaryKey: []string{"id"}}},
	)
	if err == nil {
		t.Fatal("reconcileDeletesIfDue() error = nil, want near-total delete guard")
	}
	if !strings.Contains(err.Error(), "would delete 3 of 3 target row") {
		t.Fatalf("error = %q, want guard details", err)
	}
	if got := countDeleteRuntimeRows(t, targetDB); got != 3 {
		t.Fatalf("target rows after aborted reconciliation = %d, want 3", got)
	}
	if state.recordCalls != 0 {
		t.Fatalf("record success calls = %d, want 0", state.recordCalls)
	}
}

func TestReconcileDeletesIfDueDoesNotSetStrictValidationWhenNotDue(t *testing.T) {
	sourceDB := openDeleteRuntimeDB(t)
	defer sourceDB.Close()
	targetDB := openDeleteRuntimeDB(t)
	defer targetDB.Close()
	state := &deletePreviewState{
		state: &checkpoint.DeleteReconciliationState{
			LastRunID:     "prior-run",
			LastSuccessAt: time.Now().UTC(),
		},
	}
	orch := deleteRuntimeOrchestrator(sourceDB, targetDB, state)
	if err := orch.reconcileDeletesIfDue(
		context.Background(),
		"run-not-due",
		[]source.Table{{Name: "items", PrimaryKey: []string{"id"}}},
	); err != nil {
		t.Fatalf("reconcileDeletesIfDue() error: %v", err)
	}
	if orch.deleteReconciliationStrictValidation {
		t.Fatal("deleteReconciliationStrictValidation = true, want false when current run did not reconcile")
	}
}

func deletePreviewConfig(enabled bool) *config.Config {
	cfg := &config.Config{}
	cfg.Source.Schema = "dbo"
	cfg.Target.Schema = "public"
	if enabled {
		cfg.Migration.Deletes = &config.DeleteConfig{
			Mode: config.DeleteModeReconcile,
			Reconcile: config.DeleteReconcileConfig{
				Interval: "24h",
			},
		}
	}
	return cfg
}

func deletePreviewTables() []source.Table {
	return []source.Table{
		{Name: "users", PrimaryKey: []string{"id"}},
		{Name: "orders", PrimaryKey: []string{"id"}},
		{Name: "logs"},
	}
}

type deletePreviewState struct {
	checkpoint.StateBackend
	state       *checkpoint.DeleteReconciliationState
	err         error
	recordErr   error
	calls       int
	recordCalls int
	recordRunID string
	savedTables []checkpoint.DeleteReconciliationTableRecord
}

func (s *deletePreviewState) GetDeleteReconciliationState(
	string,
	string,
) (*checkpoint.DeleteReconciliationState, error) {
	s.calls++
	return s.state, s.err
}

func (s *deletePreviewState) RecordDeleteReconciliationSuccess(
	runID string,
	sourceSchema string,
	targetSchema string,
	completedAt time.Time,
) error {
	s.recordCalls++
	s.recordRunID = runID
	s.state = &checkpoint.DeleteReconciliationState{
		SourceSchema:  sourceSchema,
		TargetSchema:  targetSchema,
		LastRunID:     runID,
		LastSuccessAt: completedAt,
		UpdatedAt:     completedAt,
	}
	_, _, _ = sourceSchema, targetSchema, completedAt
	return s.recordErr
}

func (s *deletePreviewState) UpdatePhase(string, string) error {
	return nil
}

func (s *deletePreviewState) SaveDeleteReconciliationTable(
	runID string,
	record checkpoint.DeleteReconciliationTableRecord,
) error {
	record.RunID = runID
	s.savedTables = append(s.savedTables, record)
	return nil
}

func (s *deletePreviewState) GetDeleteReconciliationTables(
	string,
) ([]checkpoint.DeleteReconciliationTableRecord, error) {
	return s.savedTables, nil
}

type deleteRuntimeSourcePool struct {
	pool.SourcePool
	db *sql.DB
}

func (p *deleteRuntimeSourcePool) DB() *sql.DB { return p.db }
func (p *deleteRuntimeSourcePool) DBType() string {
	return "sqlite"
}

type deleteRuntimeTargetPool struct {
	pool.TargetPool
	db *sql.DB
}

func (p *deleteRuntimeTargetPool) DB() *sql.DB { return p.db }
func (p *deleteRuntimeTargetPool) DBType() string {
	return "sqlite"
}
func (p *deleteRuntimeTargetPool) GetRowCountExact(context.Context, string, string, bool) (int64, error) {
	var count int64
	if err := p.db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func deleteRuntimeOrchestrator(
	sourceDB,
	targetDB *sql.DB,
	state checkpoint.StateBackend,
) *Orchestrator {
	cfg := &config.Config{}
	cfg.Migration.Deletes = &config.DeleteConfig{
		Mode: config.DeleteModeReconcile,
		Reconcile: config.DeleteReconcileConfig{
			Interval:  "24h",
			BatchSize: 1,
		},
	}
	return &Orchestrator{
		config:     cfg,
		sourcePool: &deleteRuntimeSourcePool{db: sourceDB},
		targetPool: &deleteRuntimeTargetPool{db: targetDB},
		state:      state,
		progress:   progress.New(),
		metrics:    observability.Noop(),
	}
}

func openDeleteRuntimeDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func execDeleteRuntimeSQL(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec SQL: %v", err)
	}
}

func countDeleteRuntimeRows(t *testing.T, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return count
}
