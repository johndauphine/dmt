package orchestrator

import (
	"context"
	"fmt"
	"sync"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/transfer"
)

// preTruncateIfNeeded truncates partitioned tables before transfer.
func (r *TransferRunner) preTruncateIfNeeded(ctx context.Context, jobs []transfer.Job, resume bool) error {
	if resume || !r.targetMode.ShouldTruncateBeforeTransfer() {
		return nil
	}

	// Collect unique table names that need truncation
	tablesToTruncate := make(map[string]bool)
	for _, j := range jobs {
		if j.Partition != nil {
			tablesToTruncate[j.Table.Name] = true
		}
	}

	if len(tablesToTruncate) == 0 {
		return nil
	}

	logging.Debug("Pre-truncating %d partitioned tables in parallel...", len(tablesToTruncate))
	var truncWg sync.WaitGroup
	truncErrs := make(chan error, len(tablesToTruncate))

	for tableName := range tablesToTruncate {
		truncWg.Add(1)
		go func(tname string) {
			defer truncWg.Done()
			if err := r.targetPool.TruncateTable(ctx, r.config.Target.Schema, tname); err != nil {
				truncErrs <- fmt.Errorf("pre-truncating table %s: %w", tname, err)
			}
		}(tableName)
	}

	truncWg.Wait()
	close(truncErrs)

	if err := <-truncErrs; err != nil {
		return err
	}

	return nil
}
