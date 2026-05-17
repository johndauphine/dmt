package orchestrator

import (
	"fmt"
	"time"
)

// ShowStatus displays status of current/last run
func (o *Orchestrator) ShowStatus() error {
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return err
	}
	if run == nil {
		fmt.Println("No active migration")
		return nil
	}

	// Check if a successful run supersedes this incomplete run
	superseded, err := o.state.HasSuccessfulRunAfter(run)
	if err != nil {
		return err
	}
	if superseded {
		fmt.Println("No active migration")
		return nil
	}

	total, pending, running, success, failed, err := o.state.GetRunStats(run.ID)
	if err != nil {
		return err
	}

	// Use phase to determine if migration is active vs interrupted
	// Phases: initializing -> transferring -> finalizing -> validating -> complete
	phase := run.Phase
	if phase == "" {
		phase = "initializing"
	}

	// Determine if migration is interrupted:
	// If in transferring phase with no running tasks and incomplete tasks remain
	// (pending or failed), the migration was cancelled or crashed.
	// Workers would be actively processing if the migration were truly running.
	if phase == "transferring" && running == 0 && (pending > 0 || failed > 0) {
		fmt.Printf("Run: %s\n", run.ID)
		fmt.Printf("Status: interrupted (%d/%d tasks completed)\n", success, total)
		fmt.Printf("Started: %s\n", run.StartedAt.Format(time.RFC3339))
		fmt.Printf("Tasks: %d total, %d pending, %d running, %d success, %d failed\n",
			total, pending, running, success, failed)
		printFallbackCounts(o.fallbackCountsForRun(run.ID))
		fmt.Println("Run 'resume' to continue.")
		return nil
	}

	fmt.Printf("Run: %s\n", run.ID)
	fmt.Printf("Status: %s (%s)\n", run.Status, phase)
	fmt.Printf("Started: %s\n", run.StartedAt.Format(time.RFC3339))
	if total > 0 {
		fmt.Printf("Tasks: %d total, %d pending, %d running, %d success, %d failed\n",
			total, pending, running, success, failed)
	}
	printFallbackCounts(o.fallbackCountsForRun(run.ID))

	return nil
}
