package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/source"
)

// TargetModeStrategy handles mode-specific table preparation and finalization.
// This interface encapsulates the differences between drop_recreate and upsert modes.
type TargetModeStrategy interface {
	// PrepareTables prepares target tables before data transfer.
	// For drop_recreate: drops and recreates tables with PKs.
	// For upsert: validates existing tables have PKs.
	PrepareTables(ctx context.Context, tables []source.Table) error

	// ShouldTruncateBeforeTransfer returns whether partitioned tables should be
	// truncated before transfer (to prevent race conditions with parallel inserts).
	ShouldTruncateBeforeTransfer() bool

	// Finalize performs post-transfer operations.
	// For drop_recreate: resets sequences, creates indexes/FKs/constraints.
	// For upsert: no-op (constraints already exist).
	Finalize(ctx context.Context, tables []source.Table) error

	// ModeName returns the name of this mode for logging.
	ModeName() string
}

// NewTargetModeStrategy creates the appropriate strategy based on config.
func NewTargetModeStrategy(targetMode string, targetPool pool.TargetPool, targetSchema string, createIndexes, createFKs, createChecks bool) TargetModeStrategy {
	if targetMode == "upsert" {
		return &upsertStrategy{
			targetPool:   targetPool,
			targetSchema: targetSchema,
		}
	}
	return &dropRecreateStrategy{
		targetPool:    targetPool,
		targetSchema:  targetSchema,
		createIndexes: createIndexes,
		createFKs:     createFKs,
		createChecks:  createChecks,
	}
}

// dropRecreateStrategy implements TargetModeStrategy for drop_recreate mode.
// This mode drops existing tables and recreates them from scratch.
type dropRecreateStrategy struct {
	targetPool    pool.TargetPool
	targetSchema  string
	createIndexes bool
	createFKs     bool
	createChecks  bool
}

func (s *dropRecreateStrategy) ModeName() string {
	return "drop_recreate"
}

func (s *dropRecreateStrategy) ShouldTruncateBeforeTransfer() bool {
	return true
}

func (s *dropRecreateStrategy) PrepareTables(ctx context.Context, tables []source.Table) error {
	logging.Info("Creating target tables (drop and recreate)...")

	// Phase 1: Drop all tables in parallel (no FK dependencies since we're dropping)
	logging.Debug("  Dropping %d tables in parallel...", len(tables))
	var dropWg sync.WaitGroup
	dropErrs := make(chan error, len(tables))
	for _, t := range tables {
		dropWg.Add(1)
		go func(table source.Table) {
			defer dropWg.Done()
			if err := s.targetPool.DropTable(ctx, s.targetSchema, table.Name); err != nil {
				dropErrs <- fmt.Errorf("dropping table %s: %w", table.Name, err)
			}
		}(t)
	}
	dropWg.Wait()
	close(dropErrs)
	if err := <-dropErrs; err != nil {
		return err
	}

	// Phase 2: Create all tables with PKs in parallel
	logging.Debug("  Creating %d tables in parallel...", len(tables))
	var createWg sync.WaitGroup
	createErrs := make(chan error, len(tables))
	for _, t := range tables {
		createWg.Add(1)
		go func(table source.Table) {
			defer createWg.Done()
			if err := s.targetPool.CreateTable(ctx, &table, s.targetSchema); err != nil {
				createErrs <- fmt.Errorf("creating table %s: %w", table.FullName(), err)
				return
			}
			// Create PK immediately after table (PKs are part of table structure)
			if err := s.targetPool.CreatePrimaryKey(ctx, &table, s.targetSchema); err != nil {
				createErrs <- fmt.Errorf("creating PK for %s: %w", table.FullName(), err)
			}
		}(t)
	}
	createWg.Wait()
	close(createErrs)
	if err := <-createErrs; err != nil {
		return err
	}

	return nil
}

func (s *dropRecreateStrategy) Finalize(ctx context.Context, tables []source.Table) error {
	// Phase 1: Reset sequences (parallel - no dependencies between tables)
	logging.Debug("  Resetting sequences...")
	var seqWg sync.WaitGroup
	for _, t := range tables {
		seqWg.Add(1)
		go func(table source.Table) {
			defer seqWg.Done()
			if err := s.targetPool.ResetSequence(ctx, s.targetSchema, &table); err != nil {
				logging.Warn("Warning: resetting sequence for %s: %v", table.Name, err)
			}
		}(t)
	}
	seqWg.Wait()

	// Phase 2: Create indexes (if enabled) - parallel per table
	if s.createIndexes {
		totalIndexes := 0
		for _, t := range tables {
			totalIndexes += len(t.Indexes)
		}
		if totalIndexes > 0 {
			logging.Debug("  Creating %d indexes in parallel...", totalIndexes)
		}

		var idxWg sync.WaitGroup
		for _, t := range tables {
			for _, idx := range t.Indexes {
				idxWg.Add(1)
				go func(table source.Table, index source.Index) {
					defer idxWg.Done()
					if err := s.targetPool.CreateIndex(ctx, &table, &index, s.targetSchema); err != nil {
						logging.Warn("Warning: creating index %s on %s: %v", index.Name, table.Name, err)
					}
				}(t, idx)
			}
		}
		idxWg.Wait()
	}

	// Phase 3: Create foreign keys (if enabled) - parallel per table
	if s.createFKs {
		totalFKs := 0
		for _, t := range tables {
			totalFKs += len(t.ForeignKeys)
		}
		if totalFKs > 0 {
			logging.Debug("  Creating %d foreign keys in parallel...", totalFKs)
		}

		var fkWg sync.WaitGroup
		for _, t := range tables {
			for _, fk := range t.ForeignKeys {
				fkWg.Add(1)
				go func(table source.Table, foreignKey source.ForeignKey) {
					defer fkWg.Done()
					if err := s.targetPool.CreateForeignKey(ctx, &table, &foreignKey, s.targetSchema); err != nil {
						logging.Warn("Warning: creating FK %s on %s: %v", foreignKey.Name, table.Name, err)
					}
				}(t, fk)
			}
		}
		fkWg.Wait()
	}

	// Phase 4: Create check constraints (if enabled) - parallel per table
	if s.createChecks {
		totalChecks := 0
		for _, t := range tables {
			totalChecks += len(t.CheckConstraints)
		}
		if totalChecks > 0 {
			logging.Debug("  Creating %d check constraints in parallel...", totalChecks)
		}

		var chkWg sync.WaitGroup
		for _, t := range tables {
			for _, chk := range t.CheckConstraints {
				chkWg.Add(1)
				go func(table source.Table, check source.CheckConstraint) {
					defer chkWg.Done()
					if err := s.targetPool.CreateCheckConstraint(ctx, &table, &check, s.targetSchema); err != nil {
						logging.Warn("Warning: creating CHECK %s on %s: %v", check.Name, table.Name, err)
					}
				}(t, chk)
			}
		}
		chkWg.Wait()
	}

	return nil
}

// upsertStrategy implements TargetModeStrategy for upsert mode.
// This mode requires tables to already exist and performs incremental sync.
type upsertStrategy struct {
	targetPool   pool.TargetPool
	targetSchema string
}

func (s *upsertStrategy) ModeName() string {
	return "upsert"
}

func (s *upsertStrategy) ShouldTruncateBeforeTransfer() bool {
	return false // Upserts are idempotent, no truncation needed
}

func (s *upsertStrategy) PrepareTables(ctx context.Context, tables []source.Table) error {
	logging.Info("Validating target tables (upsert mode)...")

	var sourceMissingPK []string
	var missingTables []string
	var targetMissingPK []string

	for i, t := range tables {
		logging.Debug("  [%d/%d] %s", i+1, len(tables), t.Name)

		// Validate source table has PK for upsert (required for ON CONFLICT / MERGE)
		if !t.HasPK() {
			sourceMissingPK = append(sourceMissingPK, t.Name)
			continue // Skip target checks if source has no PK
		}

		exists, err := s.targetPool.TableExists(ctx, s.targetSchema, t.Name)
		if err != nil {
			return fmt.Errorf("checking if table %s exists: %w", t.Name, err)
		}
		if !exists {
			missingTables = append(missingTables, t.Name)
		} else {
			// Table exists - verify it has a primary key (required for ON CONFLICT / MERGE)
			hasPK, err := s.targetPool.HasPrimaryKey(ctx, s.targetSchema, t.Name)
			if err != nil {
				return fmt.Errorf("checking primary key for %s: %w", t.Name, err)
			}
			if !hasPK {
				targetMissingPK = append(targetMissingPK, t.Name)
			}
		}
	}

	// Report all validation errors at once for better UX
	var validationErrors []string
	if len(sourceMissingPK) > 0 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("source tables missing primary keys: %v", sourceMissingPK))
	}
	if len(missingTables) > 0 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("target tables do not exist: %v", missingTables))
	}
	if len(targetMissingPK) > 0 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("target tables missing primary keys: %v", targetMissingPK))
	}

	if len(validationErrors) > 0 {
		return fmt.Errorf("upsert mode validation failed - %s. "+
			"Run with target_mode: drop_recreate first to create tables, then use upsert for incremental syncs",
			strings.Join(validationErrors, "; "))
	}

	return nil
}

func (s *upsertStrategy) Finalize(ctx context.Context, tables []source.Table) error {
	// In upsert mode, tables already exist with PKs, sequences, indexes, FKs, and constraints.
	// Nothing to finalize.
	logging.Debug("  Skipping finalize (upsert mode - tables already have constraints)")
	return nil
}
