package validation

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/source"
)

// Runner orchestrates the configured validation passes across a
// list of tables. Each table runs all passes sequentially (passes
// share the same DB connections; running them in parallel adds
// connection contention without speeding the wall clock); tables
// run concurrently up to the orchestrator's concurrency budget.
type Runner struct {
	Cfg    Config
	Src    Endpoint
	Tgt    Endpoint
	Passes []Pass
}

// NewRunner builds a Runner with the passes selected by cfg.Mode.
// Modes are inclusive: each higher mode runs everything below it.
// count_only is the legacy row-count check — it's still produced
// by the orchestrator's existing Validate(), not by this Runner.
// This Runner adds the new passes (null_parity, sample_row) on top
// of the legacy count check. ModeFull is reserved for a follow-up;
// callers should reject it at config-validation time before reaching
// NewRunner (see validator.go).
func NewRunner(cfg Config, src, tgt Endpoint) *Runner {
	r := &Runner{Cfg: cfg, Src: src, Tgt: tgt}
	switch cfg.Mode {
	case ModeSample:
		r.Passes = []Pass{
			NullParity{},
			SampleRow{N: cfg.SampleRows, PercentCap: cfg.SampleRowsPercent},
		}
	case ModeNullParity:
		r.Passes = []Pass{NullParity{}}
	case ModeCountOnly, "":
		// No passes from this package; the orchestrator's legacy
		// row-count Validate() runs separately.
		r.Passes = nil
	}
	return r
}

// Run executes the configured passes against every table. Tables
// run concurrently up to MaxParallel; each table runs its passes
// sequentially (passes share DB connections — running them in
// parallel adds contention without speeding wall clock).
//
// Returns a structured Result; the caller decides whether to
// error based on cfg.FailOnMismatch.
func (r *Runner) Run(ctx context.Context, tables []source.Table) Result {
	if len(r.Passes) == 0 {
		return Result{}
	}

	// Bound table-level concurrency. Default 4 — each parallel
	// table consumes one connection per side, plus one per pass.
	// Codex caught the original unbounded fan-out on #226: 100+
	// tables would spawn 100+ goroutines and exhaust connection
	// pools.
	maxPar := r.Cfg.MaxParallel
	if maxPar <= 0 {
		maxPar = 4
	}
	if maxPar > len(tables) {
		maxPar = len(tables)
	}

	results := make([]TableResult, len(tables))
	sem := make(chan struct{}, maxPar)
	var wg sync.WaitGroup
	for i, t := range tables {
		wg.Add(1)
		go func(idx int, table source.Table) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[idx] = r.runOneTable(ctx, table)
		}(i, t)
	}
	wg.Wait()

	// Stable order in output (alphabetical) so log diffs across
	// runs are easy to scan.
	sort.Slice(results, func(i, j int) bool {
		return results[i].TableName < results[j].TableName
	})
	return Result{Tables: results}
}

// runOneTable runs all configured passes against a single table.
// Honors the per-table timeout when set.
func (r *Runner) runOneTable(ctx context.Context, t source.Table) TableResult {
	tableCtx := ctx
	if r.Cfg.Timeout > 0 {
		var cancel context.CancelFunc
		tableCtx, cancel = context.WithTimeout(ctx, r.Cfg.Timeout)
		defer cancel()
	}

	res := TableResult{TableName: t.Name}
	for _, p := range r.Passes {
		start := time.Now()
		passRes := p.Run(tableCtx, r.Src, r.Tgt, t)
		passRes.Pass = p.Name() // ensure name is populated even if pass forgot
		elapsed := time.Since(start)
		logging.Debug("validation: %s.%s pass=%s status=%s in %s",
			t.Schema, t.Name, p.Name(), passRes.Status, elapsed)
		res.Passes = append(res.Passes, passRes)

		// If a pass fails outright and the table-level run is
		// configured to short-circuit, skip remaining passes for
		// this table. The fail-fast flag isn't a Config field
		// today — the implicit behavior is "continue even on
		// failure" so the operator sees ALL findings in one shot.
		// Easy to add later if a user asks.
	}
	return res
}

// FormatResult renders a Result as a multi-line log-friendly string.
// Used by the orchestrator's Validate() to report findings.
func FormatResult(r Result) string {
	if len(r.Tables) == 0 {
		return "(no validation passes ran)"
	}
	out := ""
	for _, t := range r.Tables {
		for _, p := range t.Passes {
			marker := "OK   "
			switch p.Status {
			case "fail":
				marker = "FAIL "
			case "skipped":
				marker = "SKIP "
			case "warn":
				marker = "WARN "
			}
			out += fmt.Sprintf("  %s%-30s %-12s %s\n", marker, t.TableName, p.Pass, p.Detail)
			for _, f := range p.Findings {
				prefix := "       "
				if f.Column != "" {
					out += fmt.Sprintf("%s  - column %q: %s\n", prefix, f.Column, f.Detail)
				} else {
					out += fmt.Sprintf("%s  - %s\n", prefix, f.Detail)
				}
			}
		}
	}
	return out
}
