package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/aicopilot"
)

func printTriageReviewJSON(review *aicopilot.TriageReview) error {
	data, err := json.MarshalIndent(review, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal AI triage review: %w", err)
	}
	fmt.Println(string(data))
	return nil
}

func printTriageReview(review *aicopilot.TriageReview) {
	if review == nil {
		return
	}
	fmt.Printf("\nAI triage: %s", review.Status)
	if review.Impact != "" {
		fmt.Printf(" (impact: %s)", review.Impact)
	}
	switch {
	case review.Provider != "" && review.Model != "":
		fmt.Printf(" via %s/%s", review.Provider, review.Model)
	case review.Provider != "":
		fmt.Printf(" via %s", review.Provider)
	case review.Model != "":
		fmt.Printf(" via %s", review.Model)
	}
	fmt.Println()
	if review.Summary != "" {
		fmt.Printf("Summary: %s\n", review.Summary)
	}
	if review.Error != "" {
		fmt.Printf("AI error: %s\n", review.Error)
	}
	if len(review.DeterministicFacts) > 0 {
		fmt.Println("Deterministic facts:")
		for _, fact := range review.DeterministicFacts {
			affected := ""
			if fact.Affected != "" {
				affected = " [" + fact.Affected + "]"
			}
			fmt.Printf("- %s%s: %s\n", fact.Category, affected, fact.Detail)
		}
	}
	if len(review.Findings) > 0 {
		fmt.Println("AI advisory findings:")
		for _, finding := range review.Findings {
			printTriageFinding(finding)
		}
	}
	if len(review.Notes) > 0 {
		fmt.Println("Notes:")
		for _, note := range review.Notes {
			fmt.Printf("- %s\n", note)
		}
	}
}

func printTriageFinding(f aicopilot.TriageFinding) {
	head := strings.TrimSpace(strings.Join([]string{f.Severity, f.Category}, " "))
	if f.Affected != "" {
		head += " [" + f.Affected + "]"
	}
	fmt.Printf("- %s\n", strings.TrimSpace(head))
	if len(f.AffectedTables) > 0 {
		fmt.Printf("  affected tables: %s\n", strings.Join(f.AffectedTables, ", "))
	}
	if f.LikelyCause != "" {
		fmt.Printf("  likely cause: %s\n", f.LikelyCause)
	}
	for _, h := range f.Hypotheses {
		fmt.Printf("  hypothesis (%s): %s\n", h.Confidence, h.Rationale)
	}
	if len(f.SuggestedCommands) > 0 {
		fmt.Printf("  suggested commands: %s\n", strings.Join(f.SuggestedCommands, "; "))
	}
	if len(f.SuggestedConfigChanges) > 0 {
		fmt.Printf("  config changes: %s\n", strings.Join(f.SuggestedConfigChanges, "; "))
	}
	if f.ManualInspection != "" {
		fmt.Printf("  inspect manually: %s\n", f.ManualInspection)
	}
	if f.NextAction != "" {
		fmt.Printf("  next action: %s\n", f.NextAction)
	}
}
