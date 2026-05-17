package tui

import (
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"gopkg.in/yaml.v3"
)

// Wizard handling

func (m *Model) handleWizardStep(input string) tea.Cmd {
	if input != "" {
		m.appendOutput(styleUserInput.Render("> "+input) + "\n")
		m.textInput.Reset()
	} else {
		m.appendOutput(styleUserInput.Render("  (default)") + "\n")
	}

	if cmd := m.processWizardInput(input); cmd != nil {
		return cmd
	}

	prompt := m.renderWizardPrompt()
	m.appendOutput(prompt)
	return nil
}

func (m *Model) processWizardInput(input string) tea.Cmd {
	switch m.wizardStep {
	case stepSourceType:
		if input != "" {
			m.wizardData.Source.Type = input
		}
		m.wizardStep = stepSourceHost
	case stepSourceHost:
		if input != "" {
			m.wizardData.Source.Host = input
		}
		m.wizardStep = stepSourcePort
	case stepSourcePort:
		if input != "" {
			fmt.Sscanf(input, "%d", &m.wizardData.Source.Port)
		}
		m.wizardStep = stepSourceDB
	case stepSourceDB:
		if input != "" {
			m.wizardData.Source.Database = input
		}
		m.wizardStep = stepSourceUser
	case stepSourceUser:
		if input != "" {
			m.wizardData.Source.User = input
		}
		m.wizardStep = stepSourcePass
	case stepSourcePass:
		if input != "" {
			m.wizardData.Source.Password = input
		}
		m.wizardStep = stepSourceSSL
		m.textInput.EchoMode = textinput.EchoNormal
	case stepSourceSSL:
		if input != "" {
			if m.wizardData.Source.Type == "postgres" {
				m.wizardData.Source.SSLMode = input
			} else {
				if strings.ToLower(input) == "y" || strings.ToLower(input) == "yes" || strings.ToLower(input) == "true" {
					m.wizardData.Source.TrustServerCert = true
				} else {
					m.wizardData.Source.TrustServerCert = false
				}
			}
		}
		m.wizardStep = stepTargetType
	case stepTargetType:
		if input != "" {
			m.wizardData.Target.Type = input
		}
		m.wizardStep = stepTargetHost
	case stepTargetHost:
		if input != "" {
			m.wizardData.Target.Host = input
		}
		m.wizardStep = stepTargetPort
	case stepTargetPort:
		if input != "" {
			fmt.Sscanf(input, "%d", &m.wizardData.Target.Port)
		}
		m.wizardStep = stepTargetDB
	case stepTargetDB:
		if input != "" {
			m.wizardData.Target.Database = input
		}
		m.wizardStep = stepTargetUser
	case stepTargetUser:
		if input != "" {
			m.wizardData.Target.User = input
		}
		m.wizardStep = stepTargetPass
	case stepTargetPass:
		if input != "" {
			m.wizardData.Target.Password = input
		}
		m.wizardStep = stepTargetSSL
		m.textInput.EchoMode = textinput.EchoNormal
	case stepTargetSSL:
		if input != "" {
			if m.wizardData.Target.Type == "postgres" {
				m.wizardData.Target.SSLMode = input
			} else {
				if strings.ToLower(input) == "y" || strings.ToLower(input) == "yes" || strings.ToLower(input) == "true" {
					m.wizardData.Target.TrustServerCert = true
				} else {
					m.wizardData.Target.TrustServerCert = false
				}
			}
		}
		m.wizardStep = stepWorkers
	case stepWorkers:
		if input != "" {
			fmt.Sscanf(input, "%d", &m.wizardData.Migration.Workers)
		}
		return m.finishWizard()
	}
	return nil
}

func (m *Model) renderWizardPrompt() string {
	var prompt string
	switch m.wizardStep {
	case stepSourceType:
		def := "mssql"
		if m.wizardData.Source.Type != "" {
			def = m.wizardData.Source.Type
		}
		prompt = fmt.Sprintf("Source Type (mssql/postgres) [%s]: ", def)
	case stepSourceHost:
		prompt = fmt.Sprintf("Source Host [%s]: ", m.wizardData.Source.Host)
	case stepSourcePort:
		def := 1433
		if m.wizardData.Source.Port != 0 {
			def = m.wizardData.Source.Port
		}
		prompt = fmt.Sprintf("Source Port [%d]: ", def)
	case stepSourceDB:
		prompt = fmt.Sprintf("Source Database [%s]: ", m.wizardData.Source.Database)
	case stepSourceUser:
		prompt = fmt.Sprintf("Source User [%s]: ", m.wizardData.Source.User)
	case stepSourcePass:
		prompt = "Source Password [******]: "
		m.textInput.EchoMode = textinput.EchoPassword
	case stepSourceSSL:
		if m.wizardData.Source.Type == "postgres" {
			def := "require"
			if m.wizardData.Source.SSLMode != "" {
				def = m.wizardData.Source.SSLMode
			}
			prompt = fmt.Sprintf("Source SSL Mode [%s]: ", def)
		} else {
			def := "n"
			if m.wizardData.Source.TrustServerCert {
				def = "y"
			}
			prompt = fmt.Sprintf("Trust Source Server Certificate? (y/n) [%s]: ", def)
		}
	case stepTargetType:
		def := "postgres"
		if m.wizardData.Target.Type != "" {
			def = m.wizardData.Target.Type
		}
		prompt = fmt.Sprintf("Target Type (postgres/mssql) [%s]: ", def)
	case stepTargetHost:
		prompt = fmt.Sprintf("Target Host [%s]: ", m.wizardData.Target.Host)
	case stepTargetPort:
		def := 5432
		if m.wizardData.Target.Port != 0 {
			def = m.wizardData.Target.Port
		}
		prompt = fmt.Sprintf("Target Port [%d]: ", def)
	case stepTargetDB:
		prompt = fmt.Sprintf("Target Database [%s]: ", m.wizardData.Target.Database)
	case stepTargetUser:
		prompt = fmt.Sprintf("Target User [%s]: ", m.wizardData.Target.User)
	case stepTargetPass:
		prompt = "Target Password [******]: "
		m.textInput.EchoMode = textinput.EchoPassword
	case stepTargetSSL:
		if m.wizardData.Target.Type == "postgres" {
			def := "require"
			if m.wizardData.Target.SSLMode != "" {
				def = m.wizardData.Target.SSLMode
			}
			prompt = fmt.Sprintf("Target SSL Mode [%s]: ", def)
		} else {
			def := "n"
			if m.wizardData.Target.TrustServerCert {
				def = "y"
			}
			prompt = fmt.Sprintf("Trust Target Server Certificate? (y/n) [%s]: ", def)
		}
	case stepWorkers:
		def := 8
		if m.wizardData.Migration.Workers != 0 {
			def = m.wizardData.Migration.Workers
		}
		prompt = fmt.Sprintf("Parallel Workers [%d]: ", def)
	}
	return prompt
}

func (m *Model) finishWizard() tea.Cmd {
	return func() tea.Msg {
		if m.wizardData.Source.Type == "" {
			m.wizardData.Source.Type = "mssql"
		}
		if m.wizardData.Target.Type == "" {
			m.wizardData.Target.Type = "postgres"
		}
		if m.wizardData.Migration.Workers == 0 {
			m.wizardData.Migration.Workers = 8
		}

		data, err := yaml.Marshal(m.wizardData)
		if err != nil {
			return WizardFinishedMsg{Err: fmt.Errorf("generating config: %w", err)}
		}

		filename := m.wizardFile
		if filename == "" {
			filename = "config.yaml"
		}

		if err := os.WriteFile(filename, data, 0600); err != nil {
			return WizardFinishedMsg{Err: fmt.Errorf("saving %s: %w", filename, err)}
		}

		return WizardFinishedMsg{Message: fmt.Sprintf("Configuration saved to %s!\nYou can now run the migration with /run @%s", filename, filename)}
	}
}
