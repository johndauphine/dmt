package orchestrator

import (
	"context"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/source"
)

// loadSchemaMetadata intentionally ignores the target DDL creation flags.
// create_indexes/create_foreign_keys/create_check_constraints decide what dmt
// creates on the target; schema drift detection needs the complete source shape
// so toggling those flags does not manufacture false index/FK/check drift.
func (o *Orchestrator) loadSchemaMetadata(ctx context.Context, tables []source.Table) {
	for i := range tables {
		t := &tables[i]

		if err := o.sourcePool.LoadIndexes(ctx, t); err != nil {
			logging.Warn("Warning: loading indexes for %s: %v", t.Name, err)
		}

		if err := o.sourcePool.LoadForeignKeys(ctx, t); err != nil {
			logging.Warn("Warning: loading FKs for %s: %v", t.Name, err)
		}

		if err := o.sourcePool.LoadCheckConstraints(ctx, t); err != nil {
			logging.Warn("Warning: loading check constraints for %s: %v", t.Name, err)
		}
	}
}
