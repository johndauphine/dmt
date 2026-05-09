package tuning

// applyHistory layers history-aware selection on top of the baseline.
// PR1 (#175) implements smoothed-mean shrinkage + RULE 1 retry filter
// + outlier filter + regime filtering. PR2 (#179) adds quadratic
// regression on top.
//
// Stub for PR1's first commit — wired but no-op until the next commit
// in this PR fills it in. Keeps the build green while history.go grows.
func applyHistory(out *Output, in Input, profile DriverProfile, history HistoryProvider) {
	_ = out
	_ = in
	_ = profile
	_ = history
}
