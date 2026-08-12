package main

import (
	"encoding/json"
	"fmt"

	"github.com/johndauphine/dmt/v5/internal/aicopilot"
	"github.com/johndauphine/dmt/v5/internal/command"
)

func printTriageReviewJSON(review *aicopilot.TriageReview) error {
	data, err := json.MarshalIndent(review, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal AI triage review: %w", err)
	}
	fmt.Println(string(data))
	return nil
}

// printTriageReview renders via the shared CLI/TUI formatter (#441).
func printTriageReview(review *aicopilot.TriageReview) {
	fmt.Print(command.FormatTriageReview(review))
}
