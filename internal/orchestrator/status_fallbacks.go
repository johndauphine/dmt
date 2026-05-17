package orchestrator

import (
	"fmt"
	"sort"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
)

// fallbackCountsForRun returns per-surface fallback totals for a run
// from the checkpoint state. Cross-process visible — a separate
// `dmt status` invocation reads the running migration's writes
// because the orchestrator persists every RecordFallback call to the
// state file (#176, codex review). On state-read errors, returns nil
// and logs at debug; status output without an AI-fallback line is a
// reasonable degraded mode.
func (o *Orchestrator) fallbackCountsForRun(runID string) map[string]int64 {
	if o == nil || o.state == nil || runID == "" {
		return nil
	}
	events, err := o.state.GetFallbackEventsByRun(runID)
	if err != nil {
		logging.Debug("status: reading fallback events: %v", err)
		return nil
	}
	if len(events) == 0 {
		return nil
	}
	counts := map[string]int64{}
	for _, e := range events {
		counts[e.Surface] += e.Count
	}
	return counts
}

// fingerprintCount pairs a fingerprint string with its per-(run, surface,
// fingerprint) count so the detailed-status view can render
// "postgres:inet (47)" instead of just "postgres:inet" — a maintainer
// reviewing a catalog-growth signal needs to know whether each entry
// fired once or 10K times.
type fingerprintCount struct {
	Fingerprint string
	Count       int64
}

// fallbackBreakdownForRun returns per-surface counts plus the
// fingerprint+count list for each surface, ordered by count descending
// (heavy hitters first) then alphabetically for ties. Used by the
// detailed-status view to show maintainers which inputs triggered each
// fallback surface and how often, for catalog-growth review.
func (o *Orchestrator) fallbackBreakdownForRun(runID string) (map[string]int64, map[string][]fingerprintCount) {
	if o == nil || o.state == nil || runID == "" {
		return nil, nil
	}
	events, err := o.state.GetFallbackEventsByRun(runID)
	if err != nil {
		logging.Debug("status: reading fallback events: %v", err)
		return nil, nil
	}
	if len(events) == 0 {
		return nil, nil
	}
	counts := map[string]int64{}
	fingerprints := map[string][]fingerprintCount{}
	for _, e := range events {
		counts[e.Surface] += e.Count
		if e.Fingerprint == "" {
			continue
		}
		fingerprints[e.Surface] = append(fingerprints[e.Surface],
			fingerprintCount{Fingerprint: e.Fingerprint, Count: e.Count})
	}
	for surface := range fingerprints {
		fps := fingerprints[surface]
		sort.Slice(fps, func(i, j int) bool {
			if fps[i].Count != fps[j].Count {
				return fps[i].Count > fps[j].Count
			}
			return fps[i].Fingerprint < fps[j].Fingerprint
		})
		fingerprints[surface] = fps
	}
	return counts, fingerprints
}

// sortedKeys returns the map's keys in sorted order. Small helper so
// the AI-fallback summary prints deterministically regardless of map
// iteration order.
func sortedKeys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// fingerprintDisplayCap bounds the number of distinct fingerprints
// the detailed-status view prints inline for a single surface. The
// checkpoint table is unbounded by design (the source-schema vocabulary
// is finite but the prefix space is not), so without a cap a migration
// with 500 distinct unmapped types would emit one comma-joined line
// running into the kilobytes — unreadable in a terminal. 10 is enough
// to identify the heaviest hitters; the rest collapse into "and N more"
// and remain queryable via fallback_events directly for forensic work.
const fingerprintDisplayCap = 10

// formatFingerprintList renders a sorted (heavy-first) fingerprint
// slice as "fp1 (47), fp2 (12), … and N more", capped at
// fingerprintDisplayCap entries. Count is appended so maintainers can
// see which catalog gaps fired hardest without going to the SQL.
func formatFingerprintList(fps []fingerprintCount) string {
	if len(fps) == 0 {
		return ""
	}
	limit := len(fps)
	if limit > fingerprintDisplayCap {
		limit = fingerprintDisplayCap
	}
	parts := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		parts = append(parts, fmt.Sprintf("%s (%d)", fps[i].Fingerprint, fps[i].Count))
	}
	out := strings.Join(parts, ", ")
	if remaining := len(fps) - limit; remaining > 0 {
		out += fmt.Sprintf(", and %d more", remaining)
	}
	return out
}

// printFallbackCounts emits a one-line AI-fallback summary when any
// surface fired this process. Suppressed on the no-fallback path so
// the typical clean-deterministic run keeps its short output (#176).
func printFallbackCounts(counts map[string]int64) {
	if len(counts) == 0 {
		return
	}
	surfaces := make([]string, 0, len(counts))
	for s := range counts {
		surfaces = append(surfaces, s)
	}
	sort.Strings(surfaces)
	parts := make([]string, 0, len(surfaces))
	for _, s := range surfaces {
		parts = append(parts, fmt.Sprintf("%s=%d", s, counts[s]))
	}
	fmt.Printf("AI fallbacks: %s\n", strings.Join(parts, ", "))
}
