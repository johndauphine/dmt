package tui

import (
	"context"
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/johndauphine/dmt/internal/secrets"
	"github.com/johndauphine/dmt/internal/setup"
)

// Setup wizard handling

func (m *Model) handleSetupStep(input string) tea.Cmd {
	info := m.setupState.Prompt()

	// Display input (masked or normal)
	if input != "" {
		if info.IsMasked {
			m.appendOutput(styleUserInput.Render("  ******") + "\n")
		} else {
			m.appendOutput(styleUserInput.Render("> "+input) + "\n")
		}
		m.textInput.Reset()
	} else {
		m.appendOutput(styleUserInput.Render("  (default)") + "\n")
	}

	// Reset echo mode
	m.textInput.EchoMode = textinput.EchoNormal

	if errMsg := m.setupState.Process(input); errMsg != "" {
		m.appendOutput(styleError.Render("  "+errMsg) + "\n")
		// Re-render prompt
		m.renderSetupPrompt()
		return nil
	}

	// Process any following auto steps
	return m.processSetupAutoSteps()
}

func (m *Model) processSetupAutoSteps() tea.Cmd {
	for {
		if m.setupState.CurrentStep == setup.StepDone {
			msg := fmt.Sprintf("Setup complete! Configuration saved to %s\nYou can now run the migration with /run @%s", m.setupState.ConfigPath, m.setupState.ConfigPath)
			wizardDone := func() tea.Msg {
				return WizardFinishedMsg{Message: msg}
			}
			if m.setupState.RunAnalysis {
				configPath := m.setupState.ConfigPath
				return tea.Batch(wizardDone, m.runAnalyzeCmd(configPath, "", false, false))
			}
			return wizardDone
		}

		info := m.setupState.Prompt()

		if !info.IsAutoAction {
			// Show section header and prompt
			m.renderSetupPrompt()
			return nil
		}

		// Handle auto steps
		switch m.setupState.CurrentStep {
		case setup.StepCheckSecrets:
			if info.SectionHeader != "" {
				m.appendOutput(fmt.Sprintf("\n=== %s ===\n", info.SectionHeader))
			}
			result := setup.CheckExistingSecrets()
			if result == "has_ai" {
				m.appendOutput(styleSuccess.Render("  AI provider already configured, skipping AI setup") + "\n")
			}
			m.setupState.Process(result)

		case setup.StepWriteSecrets:
			if err := m.setupState.WriteSecretsFile(); err != nil {
				errMsg := m.setupState.Process(err.Error())
				m.appendOutput(styleError.Render(fmt.Sprintf("  %s", errMsg)) + "\n")
				m.renderSetupPrompt()
				return nil
			}
			m.appendOutput(styleSuccess.Render(fmt.Sprintf("  Secrets saved to %s", secrets.GetSecretsPath())) + "\n")
			m.setupState.Process("")

		case setup.StepWriteSlackSecret:
			if err := m.setupState.WriteSlackSecret(); err != nil {
				errMsg := m.setupState.Process(err.Error())
				m.appendOutput(styleError.Render(fmt.Sprintf("  %s", errMsg)) + "\n")
				m.renderSetupPrompt()
				return nil
			}
			if m.setupState.SlackWebhook == "" {
				m.appendOutput(styleSuccess.Render("  Slack webhook cleared") + "\n")
			} else {
				m.appendOutput(styleSuccess.Render(fmt.Sprintf("  Slack webhook saved to %s", secrets.GetSecretsPath())) + "\n")
			}
			m.setupState.Process("")

		case setup.StepSourceConnTest, setup.StepTargetConnTest:
			if info.SectionHeader != "" {
				m.appendOutput(fmt.Sprintf("\n=== %s ===\n", info.SectionHeader))
			}
			m.appendOutput(styleSystemOutput.Render("  "+info.Text) + "\n")
			// Launch async connection test
			return m.runSetupConnTest(m.setupState.CurrentStep)

		case setup.StepWriteConfig:
			if err := m.setupState.WriteConfigFile(); err != nil {
				errMsg := m.setupState.Process(err.Error())
				m.appendOutput(styleError.Render(fmt.Sprintf("  %s", errMsg)) + "\n")
				m.renderSetupPrompt()
				return nil
			}
			m.appendOutput(styleSuccess.Render(fmt.Sprintf("  Configuration saved to %s", m.setupState.ConfigPath)) + "\n")
			m.setupState.Process("")

		default:
			// Unknown auto step, skip it
			m.setupState.Process("")
		}
	}
}

func (m *Model) renderSetupPrompt() {
	info := m.setupState.Prompt()

	if info.SectionHeader != "" {
		m.appendOutput(fmt.Sprintf("\n=== %s ===\n", info.SectionHeader))
	}

	prompt := info.Text
	if info.Default != "" {
		prompt += fmt.Sprintf(" [%s]", info.Default)
	}
	prompt += ": "
	m.appendOutput(prompt)

	if info.IsMasked {
		m.textInput.EchoMode = textinput.EchoPassword
	} else {
		m.textInput.EchoMode = textinput.EchoNormal
	}
}

func (m *Model) runSetupConnTest(step setup.Step) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// Resolve placeholders only for the side being tested — see comments
		// in cmd/migrate/main.go and internal/setup.SourceConnConfig.
		var result *setup.ConnTestResult
		if step == setup.StepSourceConnTest {
			conn := m.setupState.SourceConnConfig()
			result = setup.TestConnection(ctx,
				conn.Source.Type, conn.Source.Host,
				conn.Source.Port, conn.Source.Database,
				conn.Source.User, conn.Source.Password,
				conn.Source.DSNOptions())
		} else {
			conn := m.setupState.TargetConnConfig()
			result = setup.TestConnection(ctx,
				conn.Target.Type, conn.Target.Host,
				conn.Target.Port, conn.Target.Database,
				conn.Target.User, conn.Target.Password,
				conn.Target.DSNOptions())
		}

		return SetupConnTestMsg{Step: step, Result: result}
	}
}
