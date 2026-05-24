package orchestrator

import (
	"context"
	"fmt"
	"testing"

	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
)

type fakeAIReviewClient struct {
	response string
	err      error
}

func (f fakeAIReviewClient) CallAI(context.Context, string) (string, error) {
	return f.response, f.err
}

func (f fakeAIReviewClient) ProviderName() string { return "fake" }
func (f fakeAIReviewClient) Model() string        { return "fake-model" }

func TestReviewPreflightWithAIUsesInjectedClient(t *testing.T) {
	orch := &Orchestrator{
		config: &config.Config{},
		opts: Options{AIReviewClientFactory: func() aicopilot.TextClient {
			return fakeAIReviewClient{response: `{"readiness":"ready","summary":"Looks good.","findings":[]}`}
		}},
	}

	review := orch.ReviewPreflightWithAI(context.Background(), &HealthCheckResult{
		SourceConnected: true,
		TargetConnected: true,
		Healthy:         true,
	})

	if review == nil {
		t.Fatal("ReviewPreflightWithAI() returned nil")
	}
	if review.Status != aicopilot.ReviewStatusOK || !review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if review.Provider != "fake" || review.Model != "fake-model" {
		t.Fatalf("provider/model = %q/%q", review.Provider, review.Model)
	}
	if review.Readiness != aicopilot.ReadinessReady {
		t.Fatalf("readiness = %q, want ready", review.Readiness)
	}
}

func TestReviewPreflightWithAIUnavailableFallback(t *testing.T) {
	orch := &Orchestrator{
		config: &config.Config{},
		opts: Options{AIReviewClientFactory: func() aicopilot.TextClient {
			return nil
		}},
	}

	review := orch.ReviewPreflightWithAI(context.Background(), &HealthCheckResult{
		SourceConnected: true,
		TargetConnected: true,
		Healthy:         true,
	})

	if review == nil {
		t.Fatal("ReviewPreflightWithAI() returned nil")
	}
	if review.Status != aicopilot.ReviewStatusUnavailable || review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if review.Readiness != aicopilot.ReadinessReady {
		t.Fatalf("readiness = %q, want ready", review.Readiness)
	}
}

func TestReviewPreflightWithAITypedNilClientFallback(t *testing.T) {
	orch := &Orchestrator{
		config: &config.Config{},
		opts: Options{AIReviewClientFactory: func() aicopilot.TextClient {
			var client *fakeAIReviewClient
			return client
		}},
	}

	review := orch.ReviewPreflightWithAI(context.Background(), &HealthCheckResult{
		SourceConnected: true,
		TargetConnected: true,
		Healthy:         true,
	})

	if review == nil {
		t.Fatal("ReviewPreflightWithAI() returned nil")
	}
	if review.Status != aicopilot.ReviewStatusUnavailable || review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if review.Readiness != aicopilot.ReadinessReady {
		t.Fatalf("readiness = %q, want ready", review.Readiness)
	}
}

func TestReviewPreflightWithAIDoesNotDowngradeDeterministicBlockers(t *testing.T) {
	orch := &Orchestrator{
		config: &config.Config{},
		opts: Options{AIReviewClientFactory: func() aicopilot.TextClient {
			return fakeAIReviewClient{response: `{"readiness":"ready","summary":"Looks good.","findings":[]}`}
		}},
	}

	review := orch.ReviewPreflightWithAI(context.Background(), &HealthCheckResult{
		SourceConnected: true,
		TargetConnected: true,
		Healthy:         false,
		PreFlightFindings: []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "backup.ack",
			Side:     driver.PreFlightSideTarget,
			Message:  "backup confirmation required",
		}},
	})

	if review == nil {
		t.Fatal("ReviewPreflightWithAI() returned nil")
	}
	if review.Readiness != aicopilot.ReadinessBlocked {
		t.Fatalf("readiness = %q, want blocked", review.Readiness)
	}
	if len(review.DeterministicBlockers) != 1 {
		t.Fatalf("deterministic blockers len = %d, want 1", len(review.DeterministicBlockers))
	}
}

func TestReviewPreflightWithAIProviderErrorFallback(t *testing.T) {
	orch := &Orchestrator{
		config: &config.Config{},
		opts: Options{AIReviewClientFactory: func() aicopilot.TextClient {
			return fakeAIReviewClient{err: fmt.Errorf("provider token=secret failed")}
		}},
	}

	review := orch.ReviewPreflightWithAI(context.Background(), &HealthCheckResult{
		SourceConnected: true,
		TargetConnected: true,
		Healthy:         true,
	})

	if review == nil {
		t.Fatal("ReviewPreflightWithAI() returned nil")
	}
	if review.Status != aicopilot.ReviewStatusError || !review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if review.Error == "" || review.Error == "provider token=secret failed" {
		t.Fatalf("error was not scrubbed: %q", review.Error)
	}
	if review.Readiness != aicopilot.ReadinessReady {
		t.Fatalf("readiness = %q, want ready", review.Readiness)
	}
}
