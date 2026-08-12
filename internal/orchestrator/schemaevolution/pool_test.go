package schemaevolution

import (
	"context"
	"sync"

	"github.com/johndauphine/dmt/v5/internal/driver"
)

// fakeTargetPool is a copy of the orchestrator package's targetModeTestPool
// (target_mode_test.go) trimmed for this package's tests after the #456
// extraction. Test fakes are duplicated rather than shared through a
// non-test package.
type fakeTargetPool struct {
	driver.Writer

	mu         sync.Mutex
	createErrs map[string]error
	existing   map[string]bool
	dropped    []string
	created    []string
	primaryKey []string
	resets     []string
	indexes    []string
	fks        []string
	checks     []string
}

func (p *fakeTargetPool) DropTable(ctx context.Context, _, table string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropped = append(p.dropped, table)
	return nil
}

func (p *fakeTargetPool) CreateTableWithOptions(ctx context.Context, table *driver.Table, _ string, _ driver.TableOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.created = append(p.created, table.Name)
	if err := p.createErrs[table.Name]; err != nil {
		return err
	}
	if p.existing == nil {
		p.existing = make(map[string]bool)
	}
	p.existing[table.Name] = true
	return nil
}

func (p *fakeTargetPool) TableExists(ctx context.Context, _, table string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.existing[table], nil
}

func (p *fakeTargetPool) CreatePrimaryKey(ctx context.Context, table *driver.Table, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.primaryKey = append(p.primaryKey, table.Name)
	return nil
}

func (p *fakeTargetPool) ResetSequence(ctx context.Context, _ string, table *driver.Table) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resets = append(p.resets, table.Name)
	return nil
}

func (p *fakeTargetPool) CreateIndex(ctx context.Context, table *driver.Table, index *driver.Index, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.indexes = append(p.indexes, table.Name+"."+index.Name)
	return nil
}

func (p *fakeTargetPool) CreateForeignKey(ctx context.Context, table *driver.Table, fk *driver.ForeignKey, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fks = append(p.fks, table.Name+"."+fk.Name)
	return nil
}

func (p *fakeTargetPool) CreateCheckConstraint(ctx context.Context, table *driver.Table, check *driver.CheckConstraint, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.checks = append(p.checks, table.Name+"."+check.Name)
	return nil
}

func (p *fakeTargetPool) droppedTables() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.dropped...)
}

func (p *fakeTargetPool) createdTables() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.created...)
}

func (p *fakeTargetPool) primaryKeyTables() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.primaryKey...)
}

func (p *fakeTargetPool) resetTables() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.resets...)
}

func (p *fakeTargetPool) createdIndexes() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.indexes...)
}

func (p *fakeTargetPool) createdForeignKeys() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.fks...)
}

func (p *fakeTargetPool) createdChecks() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.checks...)
}
