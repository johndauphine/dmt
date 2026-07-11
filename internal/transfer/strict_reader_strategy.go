package transfer

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/source"
)

const (
	strictParallelNone             = "none"
	strictParallelExportedSnapshot = "exported_snapshot"
	strictParallelLockWindow       = "lock_window_sessions"
	strictParallelTableSharedLock  = "table_shared_lock"
	strictParallelMigrationEpoch   = "migration_epoch"
)

// strictReaderView is the source view acquired by a strict reader strategy.
// The coordinator queryer supplies boundary/count reads; workerFactory mints
// the per-reader sessions that observe that same view.
type strictReaderView struct {
	queryer       sourceQueryer
	workerFactory sourceQueryerFactory
	release       func()
}

// strictReaderStrategy owns the engine-native mechanism for sharing a stable
// source view across strict readers. The reader count passed to begin is the
// already-clamped number of worker sessions to mint; zero means the
// coordinator queryer will read serially. joinBudget is the number of pooled
// connections reserved in addition to those worker sessions.
type strictReaderStrategy interface {
	begin(context.Context, pool.SourcePool, source.Table, int) (strictReaderView, error)
	joinBudget() int
	perJobParallel() bool
	sharedViewAcrossJobs(scope string) bool
}

type exportedSnapshotStrategy struct{}

// migrationEpochReaderStrategy describes an already-frozen external source
// view (currently a SQL Server database snapshot). It reserves no additional
// connection beyond the worker reads themselves and is never acquired here.
type migrationEpochReaderStrategy struct{}

func (migrationEpochReaderStrategy) begin(context.Context, pool.SourcePool, source.Table, int) (strictReaderView, error) {
	return strictReaderView{}, fmt.Errorf("migration epoch is acquired by the orchestrator")
}

func (migrationEpochReaderStrategy) joinBudget() int                  { return 0 }
func (migrationEpochReaderStrategy) perJobParallel() bool             { return true }
func (migrationEpochReaderStrategy) sharedViewAcrossJobs(string) bool { return true }

func (exportedSnapshotStrategy) begin(ctx context.Context, srcPool pool.SourcePool, _ source.Table, _ int) (strictReaderView, error) {
	snapshot, err := beginPostgresExportedSnapshot(ctx, srcPool.DB())
	if err != nil {
		return strictReaderView{}, err
	}
	return strictReaderView{
		queryer: snapshot.lead,
		workerFactory: func(workerCtx context.Context, _ int) (sourceQueryer, func(), error) {
			return snapshot.joinReader(workerCtx)
		},
		release: snapshot.release,
	}, nil
}

func (exportedSnapshotStrategy) joinBudget() int                        { return 1 }
func (exportedSnapshotStrategy) perJobParallel() bool                   { return true }
func (exportedSnapshotStrategy) sharedViewAcrossJobs(scope string) bool { return scope == "migration" }

var strictReaderStrategies = map[string]strictReaderStrategy{
	strictParallelExportedSnapshot: exportedSnapshotStrategy{},
	strictParallelLockWindow:       mysqlLockWindowStrategy{},
	strictParallelTableSharedLock:  mssqlTableSharedLockStrategy{},
}

func resolveStrictReaderStrategy(dialect driver.Dialect) (string, strictReaderStrategy, error) {
	capability, ok := dialect.(driver.StrictParallelCapability)
	if !ok {
		return strictParallelNone, nil, nil
	}
	name := capability.StrictParallelStrategy()
	if name == strictParallelNone {
		return name, nil, nil
	}
	strategy, ok := strictReaderStrategies[name]
	if !ok {
		return name, nil, fmt.Errorf("strict parallel strategy %q is not registered", name)
	}
	return name, strategy, nil
}

func resolveStrictReaderStrategyForDBType(dbType string) (string, strictReaderStrategy, error) {
	dialect := driver.GetDialect(dbType)
	if dialect == nil {
		return strictParallelNone, nil, nil
	}
	return resolveStrictReaderStrategy(dialect)
}

func resolveStrictReaderStrategyForScope(dbType, scope string) (string, strictReaderStrategy, error) {
	if driver.Canonicalize(dbType) == "mssql" && scope == "migration" {
		return strictParallelMigrationEpoch, migrationEpochReaderStrategy{}, nil
	}
	return resolveStrictReaderStrategyForDBType(dbType)
}

// StrictStrategyAllowsPartitioning reports whether independently scheduled
// strict jobs can share one stable source view for the requested scope.
func StrictStrategyAllowsPartitioning(dbType, scope string) (bool, string, error) {
	name, strategy, err := resolveStrictReaderStrategyForScope(dbType, scope)
	if err != nil || strategy == nil {
		return false, name, err
	}
	return strategy.sharedViewAcrossJobs(scope), name, nil
}

func strictStrategyWorkerCountForTable(dbType string, table source.Table, requested, maxSourceConnections int) (int, error) {
	dialect := driver.GetDialect(dbType)
	compositeEligible := dialect != nil && dialect.SupportsCompositeRangeKeyset() &&
		driver.TupleKeysetRoutable(&table, dbType) && compositeRangeLeadingTypeEligible(table)
	if !table.SupportsKeysetPagination() && !compositeEligible {
		return 0, nil
	}
	strategyName, strategy, err := resolveStrictReaderStrategyForDBType(dbType)
	if err != nil {
		return 0, err
	}
	// MySQL's lock window is solely a parallelism upgrade. Preserve the
	// established single-reader strict transaction when there is nothing to
	// parallelize, matching the preflight gate and avoiding needless write
	// blocking / LOCK TABLES privilege requirements.
	if strategyName == strictParallelLockWindow && requested <= 1 {
		return 0, nil
	}
	readers, joins, _ := strictKeysetReaderPlan(true, strategy, requested, maxSourceConnections)
	if !joins {
		return 0, nil
	}
	return readers, nil
}

func strictReaderContext(ctx context.Context, view strictReaderView) context.Context {
	strictCtx := context.WithValue(ctx, sourceQueryerContextKey{}, view.queryer)
	return context.WithValue(strictCtx, sourceQueryerFactoryContextKey{}, view.workerFactory)
}
