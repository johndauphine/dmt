package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/orchestrator/validation"
	"github.com/johndauphine/dmt/v5/internal/source"
	"github.com/johndauphine/dmt/v5/internal/target"
)

// validationPolicy carries the resolved fail-or-warn flags for the
// row-count validation phase. Default true on each flag (#253);
// users opt out via validation.fail_on_timeout: false /
// fail_on_estimate_mismatch: false to restore pre-#253 log-only
// behavior.
type validationPolicy struct {
	FailOnTimeout          bool
	FailOnEstimateMismatch bool
	AllowTargetSuperset    bool
}

func newValidationPolicy(
	cfg config.ValidationConfig,
	targetMode string,
	deleteReconciliationStrict bool,
) validationPolicy {
	p := validationPolicy{FailOnTimeout: true, FailOnEstimateMismatch: true}
	if v := cfg.FailOnTimeout; v != nil {
		p.FailOnTimeout = *v
	}
	if v := cfg.FailOnEstimateMismatch; v != nil {
		p.FailOnEstimateMismatch = *v
	}
	p.AllowTargetSuperset = strings.EqualFold(strings.TrimSpace(targetMode), "upsert") &&
		!deleteReconciliationStrict
	return p
}

// evaluate logs the per-table result line and returns true if the
// table should count as a validation failure under the configured
// policy. The logging stays here so the wall of "%-30s ..." lines
// remains aligned and uniform; the boolean is what the outer loop
// in Validate consumes.
func (p validationPolicy) evaluate(r tableValidationResult) bool {
	switch {
	case r.timedOut:
		if p.FailOnTimeout {
			logging.Error("%-30s TIMEOUT (failed; both exact and estimated counts unavailable after %v)", r.tableName, ValidationTimeout)
			return true
		}
		logging.Warn("%-30s TIMEOUT (validation skipped after %v; fail_on_timeout disabled)", r.tableName, ValidationTimeout)
		return false
	case r.err != nil:
		logging.Error("%-30s ERROR: %v", r.tableName, r.err)
		return true
	case r.exactTimedOut && p.FailOnTimeout:
		// Exact COUNT(*) timed out and we filled in with estimates.
		// fail_on_timeout: true → fail regardless of whether the
		// estimates happened to match; the operator asked to be
		// notified when real validation didn't complete. (Codex
		// review on #253 PR.)
		if r.snapshotRowCount != nil {
			logging.Error("%-30s EXACT TARGET TIMEOUT (snapshot=%d target=~%d; exact validation incomplete after %v)",
				r.tableName, r.sourceCount, r.targetCount, ValidationTimeout)
			return true
		}
		logging.Error("%-30s EXACT TIMEOUT (estimated source=~%d target=~%d; exact validation incomplete after %v)",
			r.tableName, r.sourceCount, r.targetCount, ValidationTimeout)
		return true
	case r.targetCount == r.sourceCount:
		if r.snapshotRowCount != nil {
			if r.usedEstimate {
				logging.Warn("%-30s OK snapshot=%d target=~%d (estimated; fail_on_timeout disabled%s)",
					r.tableName, r.sourceCount, r.targetCount, r.snapshotDriftDetail())
				return false
			}
			logging.Info("%-30s OK %d rows (strict snapshot%s)",
				r.tableName, r.targetCount, r.snapshotDriftDetail())
			return false
		}
		if r.usedEstimate {
			logging.Warn("%-30s OK ~%d rows (estimated; fail_on_timeout disabled)", r.tableName, r.targetCount)
		} else {
			logging.Info("%-30s OK %d rows", r.tableName, r.targetCount)
		}
		return false
	case p.AllowTargetSuperset && r.targetCount > r.sourceCount:
		extraRows := r.targetCount - r.sourceCount
		if r.snapshotRowCount != nil {
			if r.usedEstimate {
				logging.Warn("%-30s OK snapshot=%d target=~%d (%d extra target %s allowed in upsert mode; estimated%s)",
					r.tableName, r.sourceCount, r.targetCount, extraRows, rowNoun(extraRows), r.snapshotDriftDetail())
			} else {
				logging.Info("%-30s OK snapshot=%d target=%d (%d extra target %s allowed in upsert mode%s)",
					r.tableName, r.sourceCount, r.targetCount, extraRows, rowNoun(extraRows), r.snapshotDriftDetail())
			}
			return false
		}
		if r.usedEstimate {
			logging.Warn("%-30s OK source=~%d target=~%d (%d extra target %s allowed in upsert mode; estimated)",
				r.tableName, r.sourceCount, r.targetCount, extraRows, rowNoun(extraRows))
		} else {
			logging.Info("%-30s OK source=%d target=%d (%d extra target %s allowed in upsert mode)",
				r.tableName, r.sourceCount, r.targetCount, extraRows, rowNoun(extraRows))
		}
		return false
	case r.usedEstimate:
		if p.FailOnEstimateMismatch {
			if r.snapshotRowCount != nil {
				logging.Error("%-30s FAIL snapshot=%d target=~%d (estimated counts disagree, diff=%d%s)",
					r.tableName, r.sourceCount, r.targetCount, r.sourceCount-r.targetCount, r.snapshotDriftDetail())
				return true
			}
			logging.Error("%-30s FAIL source=~%d target=~%d (estimated counts disagree, diff=%d)",
				r.tableName, r.sourceCount, r.targetCount, r.sourceCount-r.targetCount)
			return true
		}
		if r.snapshotRowCount != nil {
			logging.Warn("%-30s DIFF snapshot=%d target=~%d (estimated, diff=%d; fail_on_estimate_mismatch disabled%s)",
				r.tableName, r.sourceCount, r.targetCount, r.sourceCount-r.targetCount, r.snapshotDriftDetail())
			return false
		}
		logging.Warn("%-30s DIFF source=~%d target=~%d (estimated, diff=%d; fail_on_estimate_mismatch disabled)",
			r.tableName, r.sourceCount, r.targetCount, r.sourceCount-r.targetCount)
		return false
	default:
		if r.snapshotRowCount != nil {
			logging.Error("%-30s FAIL snapshot=%d target=%d (diff=%d%s)",
				r.tableName, r.sourceCount, r.targetCount, r.sourceCount-r.targetCount, r.snapshotDriftDetail())
			return true
		}
		logging.Error("%-30s FAIL source=%d target=%d (diff=%d)",
			r.tableName, r.sourceCount, r.targetCount, r.sourceCount-r.targetCount)
		return true
	}
}

func rowNoun(count int64) string {
	if count == 1 {
		return "row"
	}
	return "rows"
}

// ValidationTimeout is the maximum time to wait for a single table's row count query.
const ValidationTimeout = 30 * time.Second

// tableValidationResult holds the result of validating a single table.
type tableValidationResult struct {
	tableName    string
	sourceCount  int64
	targetCount  int64
	err          error
	timedOut     bool // exact AND estimated both failed
	usedEstimate bool // true if we used fast/estimated counts
	// exactTimedOut is true whenever the exact COUNT(*) path
	// exceeded ValidationTimeout, even if the estimated-count
	// fallback subsequently succeeded. Distinct from `timedOut`,
	// which is only true if estimates also failed. The validator
	// policy uses this to honor fail_on_timeout for the
	// estimate-fallback success path that previously slipped
	// through as a green result (Codex review on #253 PR).
	exactTimedOut bool
	// snapshotRowCount is the exact full-table count captured by the strict
	// transfer's pinned transaction. sourceCount becomes this expectation when
	// present, while liveSourceCount is informational drift only (#664).
	snapshotRowCount *int64
	liveSourceCount  *int64
	liveSourceError  string
}

// ValidationRunResult is the structured deterministic validation outcome used
// by the opt-in AI triage path. It contains facts only; AI hypotheses are
// added later by internal/aicopilot.
type ValidationRunResult struct {
	Mode      string
	Rows      []ValidationRowCountResult
	Deep      validation.Result
	Failed    bool
	Error     string
	StartedAt time.Time
	EndedAt   time.Time
}

// ValidationRowCountResult describes one table's count validation outcome.
type ValidationRowCountResult struct {
	TableName        string
	SourceCount      int64
	TargetCount      int64
	Difference       int64
	CountsKnown      bool
	UsedEstimate     bool
	ExactTimedOut    bool
	TimedOut         bool
	Error            string
	Failed           bool
	SnapshotRowCount *int64 `json:"snapshot_row_count,omitempty"`
	LiveSourceCount  *int64 `json:"live_source_count,omitempty"`
	LiveSourceDrift  *int64 `json:"live_source_drift,omitempty"`
}

// Validate checks row counts between source and target in parallel.
func (o *Orchestrator) Validate(ctx context.Context) error {
	_, err := o.ValidateDetailed(ctx)
	return err
}

// ValidateDetailed checks row counts and configured deep-validation passes,
// returning the structured deterministic facts even when validation fails.
func (o *Orchestrator) ValidateDetailed(ctx context.Context) (*ValidationRunResult, error) {
	result := &ValidationRunResult{
		Mode:      validationModeForResult(o.config.Migration.Validation.Mode),
		StartedAt: time.Now(),
	}
	defer func() {
		result.EndedAt = time.Now()
	}()

	if o.tables == nil {
		tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
		if err != nil {
			result.Failed = true
			result.Error = err.Error()
			return result, err
		}
		o.tables = tables
	}

	logging.Info("\nValidation Results:")
	logging.Info("-------------------")

	// Run validation for all tables in parallel
	results := make(chan tableValidationResult, len(o.tables))
	var wg sync.WaitGroup

	for _, t := range o.tables {
		wg.Add(1)
		go func(table source.Table) {
			defer wg.Done()
			result := o.validateTable(ctx, table)
			results <- result
		}(t)
	}

	// Wait for all validations to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var allResults []tableValidationResult
	for result := range results {
		allResults = append(allResults, result)
	}

	// Sort results by table name for consistent output
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].tableName < allResults[j].tableName
	})

	// Pre-#253 timeouts and estimate-mismatches were warnings, not
	// failures, which combined with #248 (silent partial-success
	// exit) let a run be reported successful even when validation
	// never completed or compared approximate counts that disagreed.
	policy := newValidationPolicy(
		o.config.Migration.Validation,
		o.config.Migration.TargetMode,
		o.deleteReconciliationStrictValidation,
	)

	// Report results
	var failed bool
	for _, r := range allResults {
		rowFailed := policy.evaluate(r)
		result.Rows = append(result.Rows, validationRowCountResult(r, rowFailed))
		if rowFailed {
			failed = true
		}
	}

	if failed {
		err := fmt.Errorf("validation failed")
		result.Failed = true
		result.Error = err.Error()
		return result, err
	}

	// After row-count validation, run the deeper validation passes
	// configured by Migration.Validation.Mode (#226). Empty mode
	// or "count_only" → no additional passes; pre-#226 behavior.
	deep, err := o.runDeepValidationResult(ctx)
	result.Deep = deep
	if err != nil {
		result.Failed = true
		result.Error = err.Error()
		return result, err
	}

	return result, nil
}

// runDeepValidation runs the configured #226 passes (null_parity,
// sample) on top of the legacy row-count check. Skipped when
// validation.mode is empty or "count_only". The "full" mode is
// reserved for a separate in-DB row-hashing follow-up (this PR's
// Pass A would have pulled every row across the wire twice — see
// the PR discussion); we reject it here with a clear pointer
// rather than silently degrading to a weaker check.
func (o *Orchestrator) runDeepValidation(ctx context.Context) error {
	_, err := o.runDeepValidationResult(ctx)
	return err
}

func (o *Orchestrator) runDeepValidationResult(ctx context.Context) (validation.Result, error) {
	mode := validation.Mode(o.config.Migration.Validation.Mode)
	if mode == "" || mode == validation.ModeCountOnly {
		return validation.Result{}, nil
	}
	if mode == validation.ModeFull {
		return validation.Result{}, fmt.Errorf("validation.mode: full is reserved for the in-DB row-hashing follow-up to #226. Use 'sample' for value-level checks on a row sample, or 'null_parity' for NULL-count parity")
	}
	if mode != validation.ModeNullParity && mode != validation.ModeSample {
		return validation.Result{}, fmt.Errorf("validation.mode %q is not recognized; valid values are count_only, null_parity, sample", mode)
	}

	cfg := validation.Config{
		Mode:              mode,
		SampleRows:        o.config.Migration.Validation.SampleRows,
		SampleRowsPercent: o.config.Migration.Validation.SampleRowsPercent,
		HashColumns:       o.config.Migration.Validation.HashColumns,
		FailOnMismatch:    o.config.Migration.Validation.FailOnMismatch,
		MaxParallel:       o.config.Migration.Validation.MaxParallel,
	}
	if s := o.config.Migration.Validation.Timeout; s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return validation.Result{}, fmt.Errorf("validation.timeout %q: %w (use Go-format duration like \"30s\", \"5m\", \"1h\")", s, err)
		}
		cfg.Timeout = d
	}

	src := validation.Endpoint{
		DB:     o.sourcePool.DB(),
		Driver: o.sourcePool.DBType(),
		Schema: o.config.Source.Schema,
		// Source identifiers are used verbatim — dmt doesn't
		// rewrite them when extracting the schema.
		IdentifierFor: nil,
	}
	tgt := validation.Endpoint{
		DB:     o.targetPool.DB(),
		Driver: o.targetPool.DBType(),
		Schema: o.config.Target.Schema,
		// Target identifiers are sanitized when the transfer phase
		// writes to PG (target.SanitizePGIdentifier lowercases —
		// see CLAUDE.md § "Identifier Sanitization"). Without the
		// same transform on the validation side, queries against
		// the target hit "relation does not exist" for tables that
		// DO exist under a sanitized name.
		IdentifierFor: targetIdentifierTransform(o.targetPool.DBType()),
	}

	logging.Info("\nDeep Validation (%s):", mode)
	runner := validation.NewRunner(cfg, src, tgt)
	result := runner.Run(ctx, o.tables)

	rendered := validation.FormatResult(result)
	if rendered != "" {
		// Strip the trailing newline so the logger's own newline
		// doesn't double-space the report. Format string is "%s"
		// (not the rendered text itself) to satisfy go vet's
		// non-constant-format-string check.
		logging.Info("%s", strings.TrimRight(rendered, "\n"))
	}

	if result.HasFailure() && cfg.FailOnMismatchOrDefault() {
		return result, fmt.Errorf("deep validation (%s) failed", mode)
	}
	if result.HasFailure() {
		logging.Warn("Deep validation failures detected; continuing because validation.fail_on_mismatch is false")
	}
	return result, nil
}

func validationRowCountResult(r tableValidationResult, failed bool) ValidationRowCountResult {
	out := ValidationRowCountResult{
		TableName:        r.tableName,
		SourceCount:      r.sourceCount,
		TargetCount:      r.targetCount,
		Difference:       r.sourceCount - r.targetCount,
		CountsKnown:      r.err == nil && !r.timedOut,
		UsedEstimate:     r.usedEstimate,
		ExactTimedOut:    r.exactTimedOut,
		TimedOut:         r.timedOut,
		Failed:           failed,
		SnapshotRowCount: r.snapshotRowCount,
		LiveSourceCount:  r.liveSourceCount,
	}
	if r.snapshotRowCount != nil && r.liveSourceCount != nil {
		drift := *r.liveSourceCount - *r.snapshotRowCount
		out.LiveSourceDrift = &drift
	}
	if r.err != nil {
		out.Error = r.err.Error()
	}
	return out
}

func validationModeForResult(mode string) string {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return string(validation.ModeCountOnly)
	}
	return mode
}

// targetIdentifierTransform returns the per-driver identifier
// transform applied to source identifiers when querying the
// target endpoint. PostgreSQL targets sanitize to lowercase via
// target.SanitizePGIdentifier; other targets pass identifiers
// through unchanged (the transfer phase doesn't rewrite them).
func targetIdentifierTransform(targetDriver string) func(string) string {
	switch targetDriver {
	case "postgres", "postgresql", "pg":
		return target.SanitizePGIdentifier
	default:
		return nil
	}
}

// validateTable validates a single table's row count.
// It first tries exact COUNT(*) with a timeout, then falls back to estimated counts.
func (o *Orchestrator) validateTable(ctx context.Context, t source.Table) tableValidationResult {
	result := tableValidationResult{tableName: t.Name}

	snapshotCount, err := o.strictSnapshotRowCount(t)
	if err != nil {
		result.err = err
		return result
	}
	if snapshotCount != nil {
		return o.validateStrictSnapshotTable(ctx, t, *snapshotCount)
	}

	// First, try exact COUNT(*) with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, ValidationTimeout)
	defer cancel()

	// Query source count (exact). The strict source snapshot protects the
	// transfer itself; validation runs afterward as a new observation, so a
	// concurrent source change can make this count fail rather than silently
	// claiming the transferred snapshot matches the current source (#640).
	// The flag still suppresses MSSQL's NOLOCK hint for this exact count.
	strict := o.config.Migration.StrictConsistency
	sourceCount, srcErr := o.sourcePool.GetRowCountExact(timeoutCtx, o.config.Source.Schema, t.Name, strict)
	srcTimedOut := timeoutCtx.Err() == context.DeadlineExceeded

	// Query target count (exact) - use fresh timeout
	timeoutCtx2, cancel2 := context.WithTimeout(ctx, ValidationTimeout)
	defer cancel2()
	targetCount, tgtErr := o.targetPool.GetRowCountExact(timeoutCtx2, o.config.Target.Schema, t.Name, strict)
	tgtTimedOut := timeoutCtx2.Err() == context.DeadlineExceeded

	// If both exact counts succeeded, use them
	if srcErr == nil && tgtErr == nil {
		result.sourceCount = sourceCount
		result.targetCount = targetCount
		return result
	}

	// If either timed out, fall back to estimated counts. Record
	// the exact timeout separately so fail_on_timeout can still
	// surface it even when estimates filled in — operators set
	// fail_on_timeout because they want to know real validation
	// completed, not because they're happy with an estimated
	// approximation (Codex review on #253 PR).
	if srcTimedOut || tgtTimedOut {
		result.exactTimedOut = true
		srcEst, srcEstErr := o.sourcePool.GetRowCountFast(ctx, o.config.Source.Schema, t.Name)
		tgtEst, tgtEstErr := o.targetPool.GetRowCountFast(ctx, o.config.Target.Schema, t.Name)

		if srcEstErr == nil && tgtEstErr == nil && srcEst > 0 && tgtEst > 0 {
			result.sourceCount = srcEst
			result.targetCount = tgtEst
			result.usedEstimate = true
			return result
		}

		// Both exact and estimate failed
		result.timedOut = true
		return result
	}

	// Non-timeout error
	if srcErr != nil {
		result.err = fmt.Errorf("source count: %w", srcErr)
	} else {
		result.err = fmt.Errorf("target count: %w", tgtErr)
	}
	return result
}

// strictSnapshotRowCount returns the exact count captured by the transfer for
// this run/table. A standalone `dmt validate` passes an empty validationRunID,
// which asks the backend for the latest matching run's evidence.
func (o *Orchestrator) strictSnapshotRowCount(t source.Table) (*int64, error) {
	strictState, ok := o.state.(checkpoint.StrictSnapshotState)
	if !ok {
		return nil, nil
	}
	return strictState.GetStrictSnapshotRowCount(
		o.validationRunID,
		o.config.Source.Schema,
		o.config.Target.Schema,
		t.Name,
	)
}

// validateStrictSnapshotTable compares the target with the count captured in
// the source transaction that supplied its rows. A later live-source count is
// deliberately observational only: a busy source changing after its snapshot
// must not invalidate a faithful copy (#664).
func (o *Orchestrator) validateStrictSnapshotTable(ctx context.Context, t source.Table, snapshotCount int64) tableValidationResult {
	result := tableValidationResult{
		tableName:        t.Name,
		sourceCount:      snapshotCount,
		snapshotRowCount: int64Pointer(snapshotCount),
	}

	targetCtx, cancelTarget := context.WithTimeout(ctx, ValidationTimeout)
	targetCount, targetErr := o.targetPool.GetRowCountExact(targetCtx, o.config.Target.Schema, t.Name, true)
	targetTimedOut := targetCtx.Err() == context.DeadlineExceeded
	cancelTarget()
	if targetErr != nil {
		if !targetTimedOut {
			result.err = fmt.Errorf("target count: %w", targetErr)
			return result
		}
		result.exactTimedOut = true
		targetEstimate, estimateErr := o.targetPool.GetRowCountFast(ctx, o.config.Target.Schema, t.Name)
		if estimateErr == nil && targetEstimate > 0 {
			result.targetCount = targetEstimate
			result.usedEstimate = true
			return result
		}
		result.timedOut = true
		return result
	}
	result.targetCount = targetCount

	liveCtx, cancelLive := context.WithTimeout(ctx, ValidationTimeout)
	liveCount, liveErr := o.sourcePool.GetRowCountExact(liveCtx, o.config.Source.Schema, t.Name, true)
	liveTimedOut := liveCtx.Err() == context.DeadlineExceeded
	cancelLive()
	if liveErr != nil {
		if liveTimedOut {
			result.liveSourceError = fmt.Sprintf("exact live count timed out after %v", ValidationTimeout)
		} else {
			result.liveSourceError = liveErr.Error()
		}
		return result
	}
	result.liveSourceCount = int64Pointer(liveCount)
	return result
}

func int64Pointer(value int64) *int64 { return &value }

func (r tableValidationResult) snapshotDriftDetail() string {
	if r.snapshotRowCount == nil {
		return ""
	}
	if r.liveSourceCount == nil {
		if r.liveSourceError != "" {
			return fmt.Sprintf("; live source drift unavailable: %s", r.liveSourceError)
		}
		return "; live source drift unavailable"
	}
	drift := *r.liveSourceCount - *r.snapshotRowCount
	if drift == 0 {
		return "; live source is unchanged since the snapshot"
	}
	return fmt.Sprintf("; live source has drifted %+d %s since the snapshot", drift, rowNoun(drift))
}

// validateSamples performs sample data validation by comparing random rows
func (o *Orchestrator) validateSamples(ctx context.Context) error {
	sampleSize := o.config.Migration.SampleSize
	if sampleSize <= 0 {
		sampleSize = 100
	}

	logging.Info("\nSample Validation (n=%d per table):", sampleSize)
	logging.Info("------------------------------------")

	var failed bool
	for _, t := range o.tables {
		if !t.HasPK() {
			logging.Debug("%-30s SKIP (no PK)", t.Name)
			continue
		}

		// Build sample query based on source database type
		var sampleQuery string
		if o.sourcePool.DBType() == "postgres" {
			// PostgreSQL source syntax
			pkCols := make([]string, len(t.PrimaryKey))
			for i, col := range t.PrimaryKey {
				pkCols[i] = fmt.Sprintf("%q", col)
			}
			pkColList := strings.Join(pkCols, ", ")
			sampleQuery = fmt.Sprintf(`
				SELECT %s FROM %s.%q
				ORDER BY random()
				LIMIT %d
			`, pkColList, t.Schema, t.Name, sampleSize)
		} else {
			// SQL Server source syntax
			pkCols := make([]string, len(t.PrimaryKey))
			for i, col := range t.PrimaryKey {
				pkCols[i] = fmt.Sprintf("[%s]", col)
			}
			pkColList := strings.Join(pkCols, ", ")
			tableHint := "WITH (NOLOCK)"
			if o.config.Migration.StrictConsistency {
				tableHint = ""
			}
			sampleQuery = fmt.Sprintf(`
				SELECT TOP %d %s FROM [%s].[%s] %s
				ORDER BY NEWID()
			`, sampleSize, pkColList, t.Schema, t.Name, tableHint)
		}

		rows, err := o.sourcePool.DB().QueryContext(ctx, sampleQuery)
		if err != nil {
			logging.Error("%-30s ERROR: %v", t.Name, err)
			continue
		}

		// Collect sample PK tuples (each is a slice of values)
		var pkTuples [][]any
		for rows.Next() {
			// Create slice to hold all PK column values
			pkValues := make([]any, len(t.PrimaryKey))
			pkPtrs := make([]any, len(t.PrimaryKey))
			for i := range pkValues {
				pkPtrs[i] = &pkValues[i]
			}

			if err := rows.Scan(pkPtrs...); err != nil {
				continue
			}
			pkTuples = append(pkTuples, pkValues)
		}
		rows.Close()

		if len(pkTuples) == 0 {
			logging.Debug("%-30s SKIP (no rows)", t.Name)
			continue
		}

		// Check if these PK tuples exist in target
		missingCount := 0
		for _, pkTuple := range pkTuples {
			exists, err := o.checkRowExistsInTarget(ctx, t, pkTuple)
			if err != nil || !exists {
				missingCount++
			}
		}

		if missingCount == 0 {
			logging.Info("%-30s OK (%d samples)", t.Name, len(pkTuples))
		} else {
			logging.Error("%-30s FAIL (%d/%d missing)", t.Name, missingCount, len(pkTuples))
			failed = true
		}
	}

	if failed {
		return fmt.Errorf("sample validation failed")
	}
	return nil
}

// checkRowExistsInTarget checks if a row with the given PK values exists in target
func (o *Orchestrator) checkRowExistsInTarget(ctx context.Context, t source.Table, pkTuple []any) (bool, error) {
	var checkQuery string
	var args []any

	if o.targetPool.DBType() == "postgres" {
		// PostgreSQL target
		whereClauses := make([]string, len(t.PrimaryKey))
		for i, col := range t.PrimaryKey {
			whereClauses[i] = fmt.Sprintf("%q = $%d", col, i+1)
		}
		whereClause := strings.Join(whereClauses, " AND ")
		checkQuery = fmt.Sprintf(
			`SELECT EXISTS(SELECT 1 FROM %s.%q WHERE %s)`,
			o.config.Target.Schema, t.Name, whereClause,
		)
		args = pkTuple
		var exists bool
		err := o.targetPool.QueryRowRaw(ctx, checkQuery, &exists, args...)
		return exists, err
	} else {
		// SQL Server target
		whereClauses := make([]string, len(t.PrimaryKey))
		args = make([]any, len(t.PrimaryKey))
		for i, col := range t.PrimaryKey {
			whereClauses[i] = fmt.Sprintf("[%s] = @p%d", col, i+1)
			args[i] = sql.Named(fmt.Sprintf("p%d", i+1), pkTuple[i])
		}
		whereClause := strings.Join(whereClauses, " AND ")
		checkQuery = fmt.Sprintf(
			`SELECT CASE WHEN EXISTS(SELECT 1 FROM [%s].[%s] WHERE %s) THEN 1 ELSE 0 END`,
			o.config.Target.Schema, t.Name, whereClause,
		)
		var exists int
		err := o.targetPool.QueryRowRaw(ctx, checkQuery, &exists, args...)
		return exists == 1, err
	}
}
