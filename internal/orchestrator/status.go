package orchestrator

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
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

	return nil
}

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
	fmt.Printf("Tasks: %d total, %d pending, %d running, %d success, %d failed\n\n",
		total, pending, running, success, failed)

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

// ShowHistory displays all migration runs
func (o *Orchestrator) ShowHistory() error {
	runs, err := o.state.GetAllRuns()
	if err != nil {
		return err
	}

	if len(runs) == 0 {
		fmt.Println("No migration history")
		return nil
	}

	fmt.Printf("%-10s %-20s %-20s %-10s %-30s\n", "ID", "Started", "Completed", "Status", "Origin")
	fmt.Println("--------------------------------------------------------------------------------------")

	for _, r := range runs {
		completed := "-"
		if r.CompletedAt != nil {
			completed = r.CompletedAt.Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%-10s %-20s %-20s %-10s %-30s\n",
			r.ID, r.StartedAt.Format("2006-01-02 15:04:05"), completed, r.Status, runOrigin(&r))
		if r.Error != "" {
			fmt.Printf("           Error: %s\n", r.Error)
		}
	}

	fmt.Println("\nUse 'history --run <ID>' to view run configuration")
	return nil
}

// ShowRunDetails displays detailed information for a specific run
func (o *Orchestrator) ShowRunDetails(runID string) error {
	run, err := o.state.GetRunByID(runID)
	if err != nil {
		return fmt.Errorf("getting run: %w", err)
	}
	if run == nil {
		return fmt.Errorf("run not found: %s", runID)
	}

	fmt.Printf("Run ID:        %s\n", run.ID)
	fmt.Printf("Status:        %s\n", run.Status)
	if run.Error != "" {
		fmt.Printf("Error:         %s\n", run.Error)
	}
	fmt.Printf("Started:       %s\n", run.StartedAt.Format("2006-01-02 15:04:05"))
	if run.CompletedAt != nil {
		fmt.Printf("Completed:     %s\n", run.CompletedAt.Format("2006-01-02 15:04:05"))
		duration := run.CompletedAt.Sub(run.StartedAt)
		fmt.Printf("Duration:      %s\n", duration.Round(time.Second))
	}
	fmt.Printf("Source Schema: %s\n", run.SourceSchema)
	fmt.Printf("Target Schema: %s\n", run.TargetSchema)
	if origin := runOrigin(run); origin != "" {
		fmt.Printf("Origin:        %s\n", origin)
	}

	// Task stats
	total, pending, running, success, failed, err := o.state.GetRunStats(run.ID)
	if err == nil && total > 0 {
		fmt.Printf("\nTasks: %d total, %d success, %d failed, %d pending, %d running\n",
			total, success, failed, pending, running)
	}

	// Config (if stored)
	if run.Config != "" {
		fmt.Println("\nConfiguration:")
		fmt.Println("--------------")
		// Pretty print the JSON config
		var cfg config.Config
		if err := json.Unmarshal([]byte(run.Config), &cfg); err == nil {
			prettyJSON, _ := json.MarshalIndent(cfg, "", "  ")
			fmt.Println(string(prettyJSON))
		} else {
			// Fall back to raw output if parsing fails
			fmt.Println(run.Config)
		}
	}

	return nil
}

func runOrigin(r *checkpoint.Run) string {
	if r == nil {
		return ""
	}
	if r.ProfileName != "" {
		return "profile:" + r.ProfileName
	}
	if r.ConfigPath != "" {
		return "config:" + r.ConfigPath
	}
	return ""
}

// GetLastRunResult builds a MigrationResult from the last run.
func (o *Orchestrator) GetLastRunResult() (*MigrationResult, error) {
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return nil, err
	}
	if run == nil {
		// Try to get the most recent run from history
		runs, err := o.state.GetAllRuns()
		if err != nil {
			return nil, err
		}
		if len(runs) == 0 {
			return nil, fmt.Errorf("no runs found")
		}
		run = &runs[0] // Most recent
	}
	return o.buildResultFromRun(run)
}

// GetRunResult builds a MigrationResult for a specific run ID.
func (o *Orchestrator) GetRunResult(runID string) (*MigrationResult, error) {
	run, err := o.state.GetRunByID(runID)
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, fmt.Errorf("run %s not found", runID)
	}
	return o.buildResultFromRun(run)
}

func (o *Orchestrator) buildResultFromRun(run *checkpoint.Run) (*MigrationResult, error) {
	tasks, err := o.state.GetTasksWithProgress(run.ID)
	if err != nil {
		return nil, err
	}

	result := &MigrationResult{
		RunID:      run.ID,
		Status:     run.Status,
		StartedAt:  run.StartedAt,
		TableStats: make([]TableResult, 0),
	}

	if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
		result.CompletedAt = *run.CompletedAt
		result.DurationSeconds = run.CompletedAt.Sub(run.StartedAt).Seconds()
	} else if run.Status == "running" {
		result.DurationSeconds = time.Since(run.StartedAt).Seconds()
	}

	if run.Error != "" {
		result.Error = run.Error
	}

	var totalRows int64
	tableMap := make(map[string]*TableResult)

	for _, t := range tasks {
		if !strings.HasPrefix(t.TaskKey, "transfer:") {
			continue
		}

		// Extract table name from task key (transfer:schema.table or transfer:schema.table:pN)
		tableName := strings.TrimPrefix(t.TaskKey, "transfer:")
		if idx := strings.LastIndex(tableName, ":p"); idx > 0 {
			tableName = tableName[:idx] // Remove partition suffix
		}

		if _, exists := tableMap[tableName]; !exists {
			tableMap[tableName] = &TableResult{
				Name:   tableName,
				Status: "pending",
			}
			result.TablesTotal++
		}

		tr := tableMap[tableName]
		tr.Rows += t.RowsDone
		totalRows += t.RowsDone

		// Update status (success only if all partitions succeed)
		switch t.Status {
		case "success":
			if tr.Status != "failed" {
				tr.Status = "success"
			}
		case "failed":
			tr.Status = "failed"
			tr.Error = t.ErrorMessage
		case "running":
			if tr.Status != "failed" {
				tr.Status = "running"
			}
		}
	}

	// Build table stats list, count successes/failures, and sort for deterministic output
	tableNames := make([]string, 0, len(tableMap))
	for name := range tableMap {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	for _, name := range tableNames {
		tr := tableMap[name]
		result.TableStats = append(result.TableStats, *tr)
		switch tr.Status {
		case "success":
			result.TablesSuccess++
		case "failed":
			result.TablesFailed++
			result.FailedTables = append(result.FailedTables, tr.Name)
		}
	}

	result.RowsTransferred = totalRows
	if result.DurationSeconds > 0 {
		result.RowsPerSecond = int64(float64(totalRows) / result.DurationSeconds)
	}

	if result.FailedTables == nil {
		result.FailedTables = []string{}
	}

	return result, nil
}

// GetStatusResult builds a StatusResult for the current/last run.
func (o *Orchestrator) GetStatusResult() (*StatusResult, error) {
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, fmt.Errorf("no active migration")
	}

	// Check if superseded
	superseded, err := o.state.HasSuccessfulRunAfter(run)
	if err != nil {
		return nil, err
	}
	if superseded {
		return nil, fmt.Errorf("no active migration")
	}

	total, pending, running, success, failed, err := o.state.GetRunStats(run.ID)
	if err != nil {
		return nil, err
	}

	tasks, err := o.state.GetTasksWithProgress(run.ID)
	if err != nil {
		return nil, err
	}

	var totalRows, totalRowsDone int64
	for _, t := range tasks {
		if strings.HasPrefix(t.TaskKey, "transfer:") {
			totalRows += t.RowsTotal
			totalRowsDone += t.RowsDone
		}
	}

	phase := run.Phase
	if phase == "" {
		phase = "initializing"
	}

	var progressPct float64
	if totalRows > 0 {
		progressPct = float64(totalRowsDone) / float64(totalRows) * 100
	}

	return &StatusResult{
		RunID:           run.ID,
		Status:          run.Status,
		Phase:           phase,
		StartedAt:       run.StartedAt,
		TablesTotal:     total,
		TablesComplete:  success,
		TablesRunning:   running,
		TablesPending:   pending,
		TablesFailed:    failed,
		RowsTransferred: totalRowsDone,
		ProgressPercent: progressPct,
	}, nil
}
