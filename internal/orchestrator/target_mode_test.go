package orchestrator

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/source"
)

func TestDropRecreatePrepareTablesCreateFailureProvidesRerunGuidance(t *testing.T) {
	createErr := errors.New("generated DDL is invalid")
	pool := &targetModeTestPool{
		createErrs: map[string]error{"orders": createErr},
	}
	strategy := &dropRecreateStrategy{
		targetPool:   pool,
		targetSchema: "public",
		sourceType:   "postgres",
		targetType:   "postgres",
	}

	err := strategy.PrepareTables(context.Background(), []source.Table{
		{Schema: "public", Name: "orders"},
	})
	if err == nil {
		t.Fatal("PrepareTables() error = nil, want create failure")
	}
	for _, want := range []string{
		"drop_recreate prepare failed after dropping existing target tables",
		"rerun drop_recreate",
		"creating table public.orders",
		"generated DDL is invalid",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("PrepareTables() error = %q, want substring %q", err.Error(), want)
		}
	}

	if !reflect.DeepEqual(pool.droppedTables(), []string{"orders"}) {
		t.Fatalf("dropped tables = %v, want [orders]", pool.droppedTables())
	}
	if !reflect.DeepEqual(pool.createdTables(), []string{"orders"}) {
		t.Fatalf("created tables = %v, want [orders]", pool.createdTables())
	}
	if got := pool.primaryKeyTables(); len(got) != 0 {
		t.Fatalf("primary key tables = %v, want none after create failure", got)
	}
}

func TestDropRecreateFinalizeResetsSequencesInTableOrder(t *testing.T) {
	pool := &targetModeTestPool{}
	strategy := &dropRecreateStrategy{
		targetPool:   pool,
		targetSchema: "public",
	}
	tables := []source.Table{
		{Name: "accounts"},
		{Name: "orders"},
		{Name: "line_items"},
	}

	if err := strategy.Finalize(context.Background(), tables); err != nil {
		t.Fatalf("Finalize() error: %v", err)
	}

	want := []string{"accounts", "orders", "line_items"}
	if got := pool.resetTables(); !reflect.DeepEqual(got, want) {
		t.Fatalf("reset sequence order = %v, want %v", got, want)
	}
}

func TestFinalizableTablesExcludeFailedTablesAndTheirForeignKeys(t *testing.T) {
	tables := []source.Table{
		{
			Schema: "public",
			Name:   "customers",
		},
		{
			Schema: "public",
			Name:   "orders",
			ForeignKeys: []source.ForeignKey{
				{Name: "fk_orders_customers", RefTable: "customers"},
			},
		},
		{
			Schema: "public",
			Name:   "line_items",
			ForeignKeys: []source.ForeignKey{
				{Name: "fk_line_items_orders", RefTable: "orders"},
			},
		},
	}

	got := finalizableTables(tables, map[string]bool{"customers": true})

	if len(got) != 2 {
		t.Fatalf("len(finalizableTables) = %d, want 2", len(got))
	}
	if got[0].Name != "orders" || got[1].Name != "line_items" {
		t.Fatalf("finalizable table order = %v, want [orders line_items]", []string{got[0].Name, got[1].Name})
	}
	if len(got[0].ForeignKeys) != 0 {
		t.Fatalf("orders FKs = %#v, want skipped reference to failed customers", got[0].ForeignKeys)
	}
	if len(got[1].ForeignKeys) != 1 || got[1].ForeignKeys[0].Name != "fk_line_items_orders" {
		t.Fatalf("line_items FKs = %#v, want fk_line_items_orders", got[1].ForeignKeys)
	}
}

type targetModeTestPool struct {
	driver.Writer

	mu         sync.Mutex
	createErrs map[string]error
	dropped    []string
	created    []string
	primaryKey []string
	resets     []string
}

func (p *targetModeTestPool) DropTable(ctx context.Context, _, table string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropped = append(p.dropped, table)
	return nil
}

func (p *targetModeTestPool) CreateTableWithOptions(ctx context.Context, table *driver.Table, _ string, _ driver.TableOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.created = append(p.created, table.Name)
	return p.createErrs[table.Name]
}

func (p *targetModeTestPool) CreatePrimaryKey(ctx context.Context, table *driver.Table, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.primaryKey = append(p.primaryKey, table.Name)
	return nil
}

func (p *targetModeTestPool) ResetSequence(ctx context.Context, _ string, table *driver.Table) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resets = append(p.resets, table.Name)
	return nil
}

func (p *targetModeTestPool) droppedTables() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.dropped...)
}

func (p *targetModeTestPool) createdTables() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.created...)
}

func (p *targetModeTestPool) primaryKeyTables() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.primaryKey...)
}

func (p *targetModeTestPool) resetTables() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.resets...)
}
