package orchestrator

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
)

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
