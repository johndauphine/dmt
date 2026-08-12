package orchestrator

import (
	"context"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/aicopilot"
	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/drift"
)

func TestReviewSchemaDriftWithAIUsesInjectedClient(t *testing.T) {
	orch := &Orchestrator{
		config: &config.Config{},
		opts: Options{AIReviewClientFactory: func() aicopilot.TextClient {
			return fakeAIReviewClient{response: `{"summary":"Review drift.","recommendations":[]}`}
		}},
	}

	review := orch.ReviewSchemaDriftWithAI(context.Background(), drift.Report{Changes: []drift.Change{{
		Kind:      drift.AddedColumn,
		Schema:    "dbo",
		TableName: "Users",
	}}}, nil, false)

	if review == nil {
		t.Fatal("ReviewSchemaDriftWithAI() returned nil")
	}
	if review.Status != aicopilot.ReviewStatusOK || !review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if review.Provider != "fake" || review.Model != "fake-model" {
		t.Fatalf("provider/model = %q/%q", review.Provider, review.Model)
	}
}

func TestReviewSchemaDriftWithAIUnavailableFallback(t *testing.T) {
	orch := &Orchestrator{
		config: &config.Config{},
		opts: Options{AIReviewClientFactory: func() aicopilot.TextClient {
			return nil
		}},
	}

	review := orch.ReviewSchemaDriftWithAI(context.Background(), drift.Report{Changes: []drift.Change{{
		Kind:      drift.TypeNarrowed,
		Schema:    "dbo",
		TableName: "Users",
	}}}, nil, false)

	if review == nil {
		t.Fatal("ReviewSchemaDriftWithAI() returned nil")
	}
	if review.Status != aicopilot.ReviewStatusUnavailable || review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if len(review.Recommendations) != 1 {
		t.Fatalf("recommendations len = %d, want 1", len(review.Recommendations))
	}
}
