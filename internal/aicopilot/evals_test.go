package aicopilot

import (
	"context"
	"strings"
	"testing"
)

type evalFakeClient struct {
	responses []string
	calls     int
}

func (c *evalFakeClient) CallAI(_ context.Context, prompt string) (string, error) {
	c.calls++
	if len(c.responses) > 0 {
		out := c.responses[0]
		c.responses = c.responses[1:]
		return out, nil
	}
	switch {
	case strings.Contains(prompt, "AI config reviewer"):
		return `{"summary":"Use conservative settings and validate before running.","patch_recommendations":[{"operation":"set","path":"migration.validation.mode","value":"sample","rationale":"Sample validation gives deterministic post-run evidence.","requires_confirmation":true}],"runbook":{"title":"Safe runbook","summary":"Run deterministic checks before migration.","before_run":["Run preflight."],"run":["Run with backup confirmation."],"validation":["Run validation."],"rollback":["Use documented restore procedures."]}}`, nil
	case strings.Contains(prompt, "AI schema evolution advisor"):
		return `{"summary":"The data_type drift remains blocked by deterministic policy.","deterministic_blockers":["data type drift is frozen by deterministic policy"],"recommendations":[{"drift_kind":"column_type_changed","classification":"lossy_conversion","risk":"blocked","schema":"public","table":"orders","column":"amount","reason":"The deterministic schema contract freezes this data type drift.","suggested_policy":"freeze","suggested_action":"Keep the deterministic block and plan a manual conversion.","deterministic_gate":{"allowed":false,"action":"block","policy":"schema_contract.data_type=freeze","reason":"data type drift is frozen by deterministic policy"}}]}`, nil
	case strings.Contains(prompt, "AI failure and validation triage reviewer"):
		return `{"impact":"attention","summary":"Validation facts show a null parity mismatch requiring inspection.","findings":[{"severity":"warn","category":"null_mismatch","affected":"orders.status_marker","affected_tables":["orders"],"deterministic_facts":["null parity mismatch"],"likely_cause":"A marker-column difference is possible but unconfirmed.","hypotheses":[{"confidence":"low","rationale":"Only aggregate null counts are available."}],"suggested_commands":["dmt validate --config config.yaml"],"manual_inspection":"Inspect validation sample evidence for orders.status_marker.","next_action":"Inspect deterministic validation output before any operator action."}]}`, nil
	case strings.Contains(prompt, "AI performance tuning explainer"):
		return `{"summary":"Deterministic tuning selected medium concurrency from memory and connection evidence.","findings":[{"knob":"workers","category":"connections","rationale":"The worker count is bounded by deterministic connection limits.","evidence":["deterministic_knobs.workers=8"],"next_action":"Keep the deterministic value unless future runs show retries."}]}`, nil
	default:
		return `{}`, nil
	}
}

func (c *evalFakeClient) ProviderName() string { return "fake" }
func (c *evalFakeClient) Model() string        { return "fake-model" }

func TestRunAdvisoryEvalsPassesHermetically(t *testing.T) {
	report, err := RunAdvisoryEvals(context.Background(), &evalFakeClient{}, AdvisoryEvalOptions{})
	if err != nil {
		t.Fatalf("RunAdvisoryEvals() error = %v", err)
	}
	if !report.Passed {
		t.Fatalf("report should pass: %+v", report.Results)
	}
	if len(report.Results) != 4 {
		t.Fatalf("result count = %d, want 4", len(report.Results))
	}
	for _, result := range report.Results {
		if result.PromptHash == "" {
			t.Fatalf("result %s missing prompt hash", result.ID)
		}
		if result.Flags.UnsafeCommandAdvice || result.Flags.OverconfidentCausality || result.Flags.MissingDeterministicGates || result.Flags.InvalidConfigAdvice {
			t.Fatalf("result %s has unexpected flags: %+v", result.ID, result.Flags)
		}
	}
}

func TestRunAdvisoryEvalsFlagsRawUnsafeAndOverconfidentAdvice(t *testing.T) {
	client := &evalFakeClient{responses: []string{`{"impact":"attention","summary":"The root cause is target drift.","findings":[{"severity":"warn","category":"row_count_mismatch","affected":"orders","likely_cause":"The root cause is target drift.","suggested_commands":["dmt run --config config.yaml --confirm-backup"],"next_action":"Drop the target rows and rerun."}]}`}}
	report, err := RunAdvisoryEvals(context.Background(), client, AdvisoryEvalOptions{ScenarioIDs: []string{"validation-triage-readonly-commands"}})
	if err != nil {
		t.Fatalf("RunAdvisoryEvals() error = %v", err)
	}
	if report.Passed {
		t.Fatal("report passed, want flagged failure")
	}
	result := report.Results[0]
	if !result.Flags.UnsafeCommandAdvice || !result.Flags.OverconfidentCausality {
		t.Fatalf("flags = %+v, want unsafe and overconfident", result.Flags)
	}
	if len(result.Evidence) == 0 {
		t.Fatal("expected evidence for failed eval")
	}
	if evidence := strings.Join(result.Evidence, "\n"); !strings.Contains(evidence, "dmt run") || !strings.Contains(evidence, "drop") {
		t.Fatalf("expected evidence to name unsafe triggers, got:\n%s", evidence)
	}
	if evidence := strings.Join(result.Evidence, "\n"); !strings.Contains(evidence, "root cause") {
		t.Fatalf("expected evidence to name overconfident trigger, got:\n%s", evidence)
	}
}

func TestUnsafeAdvisoryEvidenceIgnoresSafeIdentifierAndConfigWords(t *testing.T) {
	raw := `{"summary":"Review deleted_at null counts and migration.deletes mode before deciding next steps.","findings":[{"affected":"orders.deleted_at","next_action":"Inspect validation output only."}]}`
	if containsUnsafeAdvisory(raw) {
		t.Fatalf("containsUnsafeAdvisory() flagged safe identifier/config text: %s", raw)
	}
	if evidence := unsafeAdvisoryEvidence(raw); len(evidence) != 0 {
		t.Fatalf("unsafeAdvisoryEvidence() = %+v, want none", evidence)
	}
}

func TestRunAdvisoryEvalsFlagsMissingModelSuppliedEvidence(t *testing.T) {
	cases := []struct {
		name       string
		scenarioID string
		response   string
	}{
		{
			name:       "schema advisor missing deterministic gate",
			scenarioID: "schema-advisor-deterministic-blocker",
			response:   `{"summary":"The drift is blocked.","recommendations":[{"drift_kind":"column_type_changed","classification":"lossy_conversion","risk":"blocked","schema":"public","table":"orders","column":"amount","reason":"Policy blocks it.","suggested_policy":"freeze","suggested_action":"Keep the block."}]}`,
		},
		{
			name:       "triage missing finding deterministic facts",
			scenarioID: "validation-triage-readonly-commands",
			response:   `{"impact":"attention","summary":"Validation facts need inspection.","findings":[{"severity":"warn","category":"null_mismatch","affected":"orders","hypotheses":[{"confidence":"low","rationale":"Only aggregate null counts are available."}],"suggested_commands":["dmt validate --config config.yaml"],"next_action":"Inspect deterministic validation output."}]}`,
		},
		{
			name:       "performance missing cited evidence",
			scenarioID: "performance-explanation-deterministic-evidence",
			response:   `{"summary":"Deterministic tuning selected medium concurrency.","findings":[{"knob":"workers","category":"connections","rationale":"Connection limits bound workers.","next_action":"Keep the deterministic value."}]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &evalFakeClient{responses: []string{tc.response}}
			report, err := RunAdvisoryEvals(context.Background(), client, AdvisoryEvalOptions{ScenarioIDs: []string{tc.scenarioID}})
			if err != nil {
				t.Fatalf("RunAdvisoryEvals() error = %v", err)
			}
			if report.Passed {
				t.Fatal("report passed, want missing deterministic evidence failure")
			}
			if !report.Results[0].Flags.MissingDeterministicGates {
				t.Fatalf("flags = %+v, want missing deterministic gates", report.Results[0].Flags)
			}
			if len(report.Results[0].Evidence) == 0 {
				t.Fatal("expected evidence for missing deterministic gate failure")
			}
		})
	}
}

func TestRunAdvisoryEvalsRejectsUnknownScenario(t *testing.T) {
	_, err := RunAdvisoryEvals(context.Background(), &evalFakeClient{}, AdvisoryEvalOptions{ScenarioIDs: []string{"missing"}})
	if err == nil || !strings.Contains(err.Error(), "unknown advisory eval scenario") {
		t.Fatalf("error = %v, want unknown scenario", err)
	}
}
