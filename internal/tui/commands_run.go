package tui

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/orchestrator"
)

// Migration commands

func (m Model) runMigrationCmd(configFile, profileName string, exploreOnce bool, exploreMode string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return MigrationDoneMsg{Status: "failed", Message: "Internal error: no program reference"}
		}

		label := configFile
		if profileName != "" {
			label = profileName
		}

		// Load config synchronously to catch errors before spawning goroutine
		cfg, err := loadConfigFromOrigin(configFile, profileName)
		if err != nil {
			return MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error loading config: %v", err)}
		}

		// Apply /explore session state (#182). Arm overrides cfg.Migration.Explore
		// for THIS run only; mode overrides cfg.Migration.ExploreMode for as long
		// as it's set in the session. Empty mode leaves the loaded value alone so
		// per-config / secrets-file values still take effect.
		if exploreOnce {
			cfg.Migration.Explore = true
		}
		if exploreMode != "" {
			cfg.Migration.ExploreMode = exploreMode
		}

		orch, err := orchestrator.New(cfg)
		if err != nil {
			return MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error initializing: %v", err)}
		}

		p.Send(OutputMsg(fmt.Sprintf("Starting migration with %s\n", label)))

		go func() {
			// Recover from panics and report as errors
			defer func() {
				if r := recover(); r != nil {
					p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Panic: %v", r)})
				}
			}()
			defer orch.Close()

			if profileName != "" {
				orch.SetRunContext(profileName, "")
			} else {
				orch.SetRunContext("", configFile)
			}

			ctx, cancel := context.WithCancel(context.Background())
			p.Send(migrationStartedMsg{cancel: cancel})

			// Redirect output
			r, w, pipeErr := os.Pipe()
			if pipeErr != nil {
				p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error creating pipe: %v", pipeErr)})
				return
			}
			origStdout := os.Stdout
			origStderr := os.Stderr
			os.Stdout = w
			os.Stderr = w
			logging.SetOutput(w)

			done := make(chan struct{})
			go func() {
				defer close(done)
				buf := make([]byte, 1024)
				for {
					n, err := r.Read(buf)
					if n > 0 {
						p.Send(OutputMsg(string(buf[:n])))
					}
					if err != nil {
						break
					}
				}
			}()

			runErr := orch.Run(ctx)

			w.Close()
			os.Stdout = origStdout
			os.Stderr = origStderr
			logging.SetOutput(origStdout)
			<-done

			if runErr != nil {
				if ctx.Err() == context.Canceled {
					p.Send(MigrationDoneMsg{Status: "cancelled", Message: "Migration cancelled"})
				} else {
					p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Migration failed: %v", runErr)})
				}
				return
			}
			p.Send(MigrationDoneMsg{Status: "completed", Message: "Migration completed successfully!"})
		}()

		return nil
	}
}

func (m Model) runResumeCmd(configFile, profileName string) tea.Cmd {
	return func() (result tea.Msg) {
		// Recover from any panics in this function
		defer func() {
			if r := recover(); r != nil {
				result = MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error: %v", r)}
			}
		}()

		p := GetProgramRef()
		if p == nil {
			return MigrationDoneMsg{Status: "failed", Message: "Internal error: no program reference"}
		}

		label := configFile
		if profileName != "" {
			label = profileName
		}

		// Load config synchronously to catch errors before spawning goroutine
		cfg, err := loadConfigFromOrigin(configFile, profileName)
		if err != nil {
			return MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error loading config: %v", err)}
		}

		orch, err := orchestrator.New(cfg)
		if err != nil {
			return MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error initializing: %v", err)}
		}

		p.Send(OutputMsg(fmt.Sprintf("Resuming migration with %s\n", label)))

		go func() {
			// Recover from panics and report as errors
			defer func() {
				if r := recover(); r != nil {
					p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Panic: %v", r)})
				}
			}()
			defer orch.Close()

			if profileName != "" {
				orch.SetRunContext(profileName, "")
			} else {
				orch.SetRunContext("", configFile)
			}

			ctx, cancel := context.WithCancel(context.Background())
			p.Send(migrationStartedMsg{cancel: cancel})

			r, w, pipeErr := os.Pipe()
			if pipeErr != nil {
				p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error creating pipe: %v", pipeErr)})
				return
			}
			origStdout := os.Stdout
			origStderr := os.Stderr
			os.Stdout = w
			os.Stderr = w
			logging.SetOutput(w)

			done := make(chan struct{})
			go func() {
				defer close(done)
				buf := make([]byte, 1024)
				for {
					n, err := r.Read(buf)
					if n > 0 {
						p.Send(OutputMsg(string(buf[:n])))
					}
					if err != nil {
						break
					}
				}
			}()

			runErr := orch.Resume(ctx)

			w.Close()
			os.Stdout = origStdout
			os.Stderr = origStderr
			logging.SetOutput(origStdout)
			<-done

			if runErr != nil {
				if ctx.Err() == context.Canceled {
					p.Send(MigrationDoneMsg{Status: "cancelled", Message: "Resume cancelled"})
				} else {
					p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Resume failed: %v", runErr)})
				}
				return
			}
			p.Send(MigrationDoneMsg{Status: "completed", Message: "Resume completed successfully!"})
		}()

		return nil
	}
}

func (m Model) runValidateCmd(configFile, profileName string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			origin := "config: " + configFile
			if profileName != "" {
				origin = "profile: " + profileName
			}
			p.Send(OutputMsg(fmt.Sprintf("Validating with %s\n", origin)))

			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			orch, err := orchestrator.New(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			defer orch.Close()

			if err := orch.Validate(context.Background()); err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Validation failed: %v\n", err)))
				return
			}
			p.Send(OutputMsg("Validation passed!\n"))
		}()

		return nil
	}
}

func (m Model) runConfigCmd(configFile, profileName string) tea.Cmd {
	return func() tea.Msg {
		origin := "config: " + configFile
		if profileName != "" {
			origin = "profile: " + profileName
		}

		cfg, err := loadConfigFromOrigin(configFile, profileName)
		if err != nil {
			return OutputMsg(fmt.Sprintf("Error loading %s: %v\n", origin, err))
		}

		return BoxedOutputMsg(cfg.DebugDump())
	}
}

func (m Model) runAnalyzeCmd(configFile, profileName string, apply bool) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			if apply && profileName != "" {
				p.Send(OutputMsg(fmt.Sprintf("❌ Error: analyze --apply requires a config file; profile %q cannot be updated in place\n", profileName)))
				return
			}

			origin := "config: " + configFile
			if profileName != "" {
				origin = "profile: " + profileName
			}
			p.Send(OutputMsg(fmt.Sprintf("Analyzing with %s\n", origin)))

			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}

			// Try full orchestrator first (with both source and target)
			orch, err := orchestrator.New(cfg)
			if err != nil {
				// Full connection failed - try source-only mode for analyze
				p.Send(OutputMsg(fmt.Sprintf("⚠️  Warning: %v\n", err)))
				p.Send(OutputMsg("   Attempting source-only analysis...\n"))

				orch, err = orchestrator.NewWithOptions(cfg, orchestrator.Options{SourceOnly: true})
				if err != nil {
					p.Send(OutputMsg(fmt.Sprintf("❌ Error: Cannot connect to source database\n")))
					p.Send(OutputMsg(fmt.Sprintf("   %v\n", err)))
					return
				}
				p.Send(OutputMsg("✓ Connected to source database\n"))
				p.Send(OutputMsg("⚠️  Target database unavailable - tuning recommendations may be less accurate\n\n"))
			}
			defer orch.Close()

			schema := cfg.Source.Schema
			if schema == "" {
				if cfg.Source.Type == "postgres" {
					schema = "public"
				} else {
					schema = "dbo"
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			suggestions, err := orch.AnalyzeConfig(ctx, schema)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}

			p.Send(BoxedOutputMsg(suggestions.FormatYAML()))

			// Apply AI-tuned parameters to the analyzed config file if requested.
			if apply {
				if err := config.ApplyTuningToConfigFile(configFile, suggestions); err != nil {
					p.Send(OutputMsg(fmt.Sprintf("\n❌ Failed to apply tuning: %v\n", err)))
				} else {
					p.Send(OutputMsg(fmt.Sprintf("\n✓ Applied AI-tuned parameters to %s\n", configFile)))
				}
			}
		}()

		return nil
	}
}

func (m Model) runStatusCmd(configFile, profileName string, detailed bool) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			orch, err := orchestrator.New(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			defer orch.Close()

			var output string
			if detailed {
				output, err = CaptureToString(orch.ShowDetailedStatus)
			} else {
				output, err = CaptureToString(orch.ShowStatus)
			}
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error showing status: %v\n", err)))
				return
			}
			p.Send(BoxedOutputMsg(output))
		}()

		return nil
	}
}

func (m Model) runHistoryCmd(configFile, profileName, runID string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			orch, err := orchestrator.New(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			defer orch.Close()

			var output string
			if runID != "" {
				output, err = CaptureToString(func() error { return orch.ShowRunDetails(runID) })
			} else {
				output, err = CaptureToString(orch.ShowHistory)
			}
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error showing history: %v\n", err)))
				return
			}
			p.Send(BoxedOutputMsg(output))
		}()

		return nil
	}
}

func (m Model) runShellCmd(shellCmd string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			cmd := exec.Command("sh", "-c", shellCmd)
			output, err := cmd.CombinedOutput()
			if err != nil {
				p.Send(BoxedOutputMsg(fmt.Sprintf("%s\nError: %v", string(output), err)))
				return
			}
			p.Send(BoxedOutputMsg(string(output)))
		}()

		return nil
	}
}
