package orchestrator

import (
	"errors"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
)

// boolPtr is a small helper because config.ValidationConfig uses
// `*bool` for tri-state semantics.
func boolPtr(b bool) *bool { return &b }

// TestValidationPolicy_Defaults guards the #253 default contract:
// validation timeouts and estimate-mismatches must be treated as
// failures unless the operator explicitly opts in to log-only via
// the two new fail_on_* flags.
func TestValidationPolicy_Defaults(t *testing.T) {
	p := newValidationPolicy(config.ValidationConfig{})
	if !p.FailOnTimeout {
		t.Error("default policy doesn't fail on timeout; #253 requires it does")
	}
	if !p.FailOnEstimateMismatch {
		t.Error("default policy doesn't fail on estimate mismatch; #253 requires it does")
	}
}

func TestValidationPolicy_ExplicitOptOut(t *testing.T) {
	p := newValidationPolicy(config.ValidationConfig{
		FailOnTimeout:          boolPtr(false),
		FailOnEstimateMismatch: boolPtr(false),
	})
	if p.FailOnTimeout {
		t.Error("FailOnTimeout: false should disable")
	}
	if p.FailOnEstimateMismatch {
		t.Error("FailOnEstimateMismatch: false should disable")
	}
}

// TestValidationPolicy_Evaluate is the per-table decision table.
// Each case captures one row-count validation outcome and asserts
// whether the policy reports it as a failure.
func TestValidationPolicy_Evaluate(t *testing.T) {
	cases := []struct {
		name       string
		policy     validationPolicy
		result     tableValidationResult
		wantFailed bool
	}{
		{
			name:       "exact_match_passes",
			policy:     validationPolicy{FailOnTimeout: true, FailOnEstimateMismatch: true},
			result:     tableValidationResult{tableName: "t", sourceCount: 100, targetCount: 100},
			wantFailed: false,
		},
		{
			name:       "exact_mismatch_fails",
			policy:     validationPolicy{FailOnTimeout: true, FailOnEstimateMismatch: true},
			result:     tableValidationResult{tableName: "t", sourceCount: 100, targetCount: 99},
			wantFailed: true,
		},
		{
			// Estimates can match for reasons unrelated to a timeout
			// (e.g. a future smartconfig that prefers estimates for
			// huge tables). When exactTimedOut is false, the
			// usedEstimate match path stays a pass.
			name:       "estimate_match_passes_when_not_timeout_driven",
			policy:     validationPolicy{FailOnTimeout: true, FailOnEstimateMismatch: true},
			result:     tableValidationResult{tableName: "t", sourceCount: 100, targetCount: 100, usedEstimate: true},
			wantFailed: false,
		},
		{
			// Codex-caught regression on the #253 PR: previously,
			// when the exact COUNT(*) timed out but estimates filled
			// in AND matched, the result was a green pass — silently
			// violating fail_on_timeout. Now the policy fails it.
			name:       "exact_timeout_with_matching_estimates_fails_under_fail_on_timeout",
			policy:     validationPolicy{FailOnTimeout: true, FailOnEstimateMismatch: true},
			result:     tableValidationResult{tableName: "t", sourceCount: 100, targetCount: 100, usedEstimate: true, exactTimedOut: true},
			wantFailed: true,
		},
		{
			// Same scenario with fail_on_timeout disabled: still
			// passes (operator explicitly accepted estimate-fallback
			// after exact timeout).
			name:       "exact_timeout_with_matching_estimates_passes_when_opted_out",
			policy:     validationPolicy{FailOnTimeout: false, FailOnEstimateMismatch: true},
			result:     tableValidationResult{tableName: "t", sourceCount: 100, targetCount: 100, usedEstimate: true, exactTimedOut: true},
			wantFailed: false,
		},
		{
			name:       "estimate_mismatch_fails_by_default_#253",
			policy:     validationPolicy{FailOnTimeout: true, FailOnEstimateMismatch: true},
			result:     tableValidationResult{tableName: "t", sourceCount: 100, targetCount: 50, usedEstimate: true},
			wantFailed: true,
		},
		{
			name:       "estimate_mismatch_warns_when_opted_out",
			policy:     validationPolicy{FailOnTimeout: true, FailOnEstimateMismatch: false},
			result:     tableValidationResult{tableName: "t", sourceCount: 100, targetCount: 50, usedEstimate: true},
			wantFailed: false,
		},
		{
			name:       "timeout_fails_by_default_#253",
			policy:     validationPolicy{FailOnTimeout: true, FailOnEstimateMismatch: true},
			result:     tableValidationResult{tableName: "t", timedOut: true},
			wantFailed: true,
		},
		{
			name:       "timeout_warns_when_opted_out",
			policy:     validationPolicy{FailOnTimeout: false, FailOnEstimateMismatch: true},
			result:     tableValidationResult{tableName: "t", timedOut: true},
			wantFailed: false,
		},
		{
			name:       "explicit_error_always_fails",
			policy:     validationPolicy{FailOnTimeout: false, FailOnEstimateMismatch: false},
			result:     tableValidationResult{tableName: "t", err: errors.New("count failed")},
			wantFailed: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.policy.evaluate(tc.result)
			if got != tc.wantFailed {
				t.Errorf("evaluate() = %v, want %v", got, tc.wantFailed)
			}
		})
	}
}
