package orchestrator

import (
	"fmt"
	"strings"
	"time"
)

// ShowDetailedStatus displays detailed task-level status for a migration run
func (o *Orchestrator) ShowDetailedStatus() error {
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

	phase := run.Phase
	if phase == "" {
		phase = "initializing"
	}

	fmt.Printf("Run: %s\n", run.ID)
	fmt.Printf("Status: %s (%s)\n", run.Status, phase)
	fmt.Printf("Started: %s\n", run.StartedAt.Format(time.RFC3339))
	fmt.Printf("Tasks: %d total, %d pending, %d running, %d success, %d failed\n",
		total, pending, running, success, failed)
	if counts, fingerprints := o.fallbackBreakdownForRun(run.ID); len(counts) > 0 {
		printFallbackCounts(counts)
		for _, surface := range sortedKeys(counts) {
			if fps := fingerprints[surface]; len(fps) > 0 {
				fmt.Printf("  %s fingerprints: %s\n", surface, formatFingerprintList(fps))
			}
		}
	}
	fmt.Println()

	// Get all tasks with progress
	tasks, err := o.state.GetTasksWithProgress(run.ID)
	if err != nil {
		return err
	}

	if len(tasks) == 0 {
		fmt.Println("No tasks found")
		return nil
	}

	// Print task details
	fmt.Printf("%-30s %-10s %-15s %-20s %s\n", "Task", "Status", "Progress", "Rows", "Error")
	fmt.Println(strings.Repeat("-", 100))

	for _, t := range tasks {
		// Format progress
		progress := ""
		rows := ""
		if t.RowsTotal > 0 {
			pct := float64(t.RowsDone) / float64(t.RowsTotal) * 100
			progress = fmt.Sprintf("%.1f%%", pct)
			rows = fmt.Sprintf("%d/%d", t.RowsDone, t.RowsTotal)
		} else if t.RowsDone > 0 {
			rows = fmt.Sprintf("%d", t.RowsDone)
		}

		// Truncate task key if too long
		taskKey := t.TaskKey
		if len(taskKey) > 28 {
			taskKey = taskKey[:25] + "..."
		}

		// Truncate error if too long
		errorMsg := t.ErrorMessage
		if len(errorMsg) > 30 {
			errorMsg = errorMsg[:27] + "..."
		}

		// Status indicator
		statusIcon := ""
		switch t.Status {
		case "success":
			statusIcon = "✓"
		case "failed":
			statusIcon = "✗"
		case "running":
			statusIcon = "►"
		case "pending":
			statusIcon = "○"
		}

		fmt.Printf("%-30s %s %-8s %-15s %-20s %s\n",
			taskKey, statusIcon, t.Status, progress, rows, errorMsg)
	}

	return nil
}
