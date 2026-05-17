package tui

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"gopkg.in/yaml.v3"
)

// Profile commands

func (m Model) handleProfileCommand(parts []string) tea.Cmd {
	if len(parts) < 2 {
		return func() tea.Msg { return OutputMsg("Usage: /profile save|list|delete|export\n") }
	}

	action := parts[1]
	switch action {
	case "list":
		return m.profileListCmd()
	case "save":
		name, configFile := parseProfileSaveArgs(parts)
		if name == "" {
			return func() tea.Msg { return OutputMsg("Usage: /profile save NAME [config_file]\n") }
		}
		return m.profileSaveCmd(name, configFile)
	case "delete":
		if len(parts) < 3 {
			return func() tea.Msg { return OutputMsg("Usage: /profile delete NAME\n") }
		}
		return m.profileDeleteCmd(parts[2])
	case "export":
		name, outFile := parseProfileExportArgs(parts)
		if name == "" {
			return func() tea.Msg { return OutputMsg("Usage: /profile export NAME [output_file]\n") }
		}
		return m.profileExportCmd(name, outFile)
	default:
		return func() tea.Msg { return OutputMsg("Unknown profile command: " + action + "\n") }
	}
}

func (m Model) profileSaveCmd(name, configFile string) tea.Cmd {
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

			cfg, err := config.Load(configFile)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error loading config: %v\n", err)))
				return
			}
			if name == "" {
				if cfg.Profile.Name != "" {
					name = cfg.Profile.Name
				} else {
					base := filepath.Base(configFile)
					name = strings.TrimSuffix(base, filepath.Ext(base))
				}
			}
			payload, err := yaml.Marshal(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error encoding config: %v\n", err)))
				return
			}

			dataDir, err := config.DefaultDataDir()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error resolving data dir: %v\n", err)))
				return
			}
			state, err := checkpoint.New(dataDir)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error opening profile store: %v\n", err)))
				return
			}
			defer state.Close()

			if err := state.SaveProfile(name, cfg.Profile.Description, payload); err != nil {
				if strings.Contains(err.Error(), "DMT_MASTER_KEY is not set") {
					p.Send(OutputMsg("Error saving profile: DMT_MASTER_KEY is not set. Start the TUI with the env var set.\n"))
					return
				}
				p.Send(OutputMsg(fmt.Sprintf("Error saving profile: %v\n", err)))
				return
			}
			p.Send(OutputMsg(fmt.Sprintf("Saved profile %q\n", name)))
		}()

		return nil
	}
}

func (m Model) profileListCmd() tea.Cmd {
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

			dataDir, err := config.DefaultDataDir()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error resolving data dir: %v\n", err)))
				return
			}
			state, err := checkpoint.New(dataDir)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error opening profile store: %v\n", err)))
				return
			}
			defer state.Close()

			profiles, err := state.ListProfiles()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error listing profiles: %v\n", err)))
				return
			}
			if len(profiles) == 0 {
				p.Send(BoxedOutputMsg("No profiles found"))
				return
			}

			var b strings.Builder
			fmt.Fprintf(&b, "%-20s %-40s %-20s %-20s\n", "Name", "Description", "Created", "Updated")
			for _, prof := range profiles {
				desc := strings.ReplaceAll(strings.TrimSpace(prof.Description), "\n", " ")
				fmt.Fprintf(&b, "%-20s %-40s %-20s %-20s\n",
					prof.Name,
					desc,
					prof.CreatedAt.Format("2006-01-02 15:04:05"),
					prof.UpdatedAt.Format("2006-01-02 15:04:05"))
			}
			p.Send(BoxedOutputMsg(b.String()))
		}()

		return nil
	}
}

func (m Model) profileDeleteCmd(name string) tea.Cmd {
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

			dataDir, err := config.DefaultDataDir()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error resolving data dir: %v\n", err)))
				return
			}
			state, err := checkpoint.New(dataDir)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error opening profile store: %v\n", err)))
				return
			}
			defer state.Close()

			if err := state.DeleteProfile(name); err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error deleting profile: %v\n", err)))
				return
			}
			p.Send(OutputMsg(fmt.Sprintf("Deleted profile %q\n", name)))
		}()

		return nil
	}
}

func (m Model) profileExportCmd(name, outFile string) tea.Cmd {
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

			dataDir, err := config.DefaultDataDir()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error resolving data dir: %v\n", err)))
				return
			}
			state, err := checkpoint.New(dataDir)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error opening profile store: %v\n", err)))
				return
			}
			defer state.Close()

			blob, err := state.GetProfile(name)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error loading profile: %v\n", err)))
				return
			}
			if err := os.WriteFile(outFile, blob, 0600); err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error exporting profile: %v\n", err)))
				return
			}
			p.Send(OutputMsg(fmt.Sprintf("Exported profile %q to %s\n", name, outFile)))
		}()

		return nil
	}
}
