package tui

import (
	"fmt"
	"os"
	"strings"
	"unicode"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
)

// Helper functions

// parseOriginArgs parses commands taking only the config origin (#444).
func (m *Model) parseOriginArgs(command string, parts []string) (configFile, profileName string, err error) {
	pa, err := parseSlashArgs(argSpec{command: command, strs: originFlags()}, parts)
	if err != nil {
		return "", "", err
	}
	configFile, profileName = m.resolveOrigin(pa)
	return configFile, profileName, nil
}

// parseHistoryArgs parses /history [--run ID] plus the config origin.
func (m *Model) parseHistoryArgs(parts []string) (configFile, profileName, runID string, err error) {
	strs := originFlags()
	strs["--run"] = "run"
	pa, err := parseSlashArgs(argSpec{command: "/history", strs: strs}, parts)
	if err != nil {
		return "", "", "", err
	}
	configFile, profileName = m.resolveOrigin(pa)
	return configFile, profileName, pa.strs["run"], nil
}

// parseStatusArgs parses /status [-d|--detailed] plus the config origin.
func (m *Model) parseStatusArgs(parts []string) (configFile, profileName string, detailed bool, err error) {
	pa, err := parseSlashArgs(argSpec{
		command: "/status",
		strs:    originFlags(),
		bools:   map[string]string{"-d": "detailed", "--detailed": "detailed"},
	}, parts)
	if err != nil {
		return "", "", false, err
	}
	configFile, profileName = m.resolveOrigin(pa)
	return configFile, profileName, pa.bools["detailed"], nil
}

// parseAnalyzeArgs parses /analyze [--apply] plus the config origin.
func (m *Model) parseAnalyzeArgs(parts []string) (configFile, profileName string, apply bool, err error) {
	pa, err := parseSlashArgs(argSpec{
		command: "/analyze",
		strs:    originFlags(),
		bools:   map[string]string{"-a": "apply", "--apply": "apply"},
	}, parts)
	if err != nil {
		return "", "", false, err
	}
	configFile, profileName = m.resolveOrigin(pa)
	return configFile, profileName, pa.bools["apply"], nil
}

func parseProfileSaveArgs(parts []string) (string, string) {
	if len(parts) < 3 {
		return "", "config.yaml"
	}

	name := ""
	configFile := "config.yaml"

	if strings.HasPrefix(parts[2], "@") {
		configFile = parts[2][1:]
	} else {
		name = parts[2]
	}

	if len(parts) > 3 {
		if strings.HasPrefix(parts[3], "@") {
			configFile = parts[3][1:]
		} else {
			configFile = parts[3]
		}
	}

	return name, configFile
}

func parseProfileExportArgs(parts []string) (string, string) {
	if len(parts) < 3 {
		return "", "config.yaml"
	}
	name := parts[2]
	outFile := "config.yaml"
	if len(parts) > 3 {
		if strings.HasPrefix(parts[3], "@") {
			outFile = parts[3][1:]
		} else {
			outFile = parts[3]
		}
	}
	return name, outFile
}

func loadConfigFromOrigin(configFile, profileName string) (*config.Config, error) {
	if profileName != "" {
		return loadProfileConfig(profileName)
	}
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s", configFile)
	}
	return config.Load(configFile)
}

func loadProfileConfig(name string) (*config.Config, error) {
	dataDir, err := config.DefaultDataDir()
	if err != nil {
		return nil, err
	}
	state, err := checkpoint.New(dataDir)
	if err != nil {
		return nil, err
	}
	defer state.Close()

	blob, err := state.GetProfile(name)
	if err != nil {
		return nil, err
	}
	return config.LoadBytes(blob)
}

// wrapLine wraps text to fit within width, preserving word boundaries where
// possible. Words longer than width are split at the boundary. Whitespace
// is preserved as separate tokens to maintain formatting.
func wrapLine(line string, width int) string {
	if width <= 0 || len(line) <= width {
		return line
	}

	var result strings.Builder
	currentLine := ""

	words := splitIntoWords(line)
	for _, word := range words {
		if len(currentLine)+len(word) > width {
			if currentLine != "" {
				result.WriteString(currentLine)
				result.WriteString("\n")
			}
			for len(word) > width {
				result.WriteString(word[:width])
				result.WriteString("\n")
				word = word[width:]
			}
			currentLine = word
		} else {
			currentLine += word
		}
	}

	if currentLine != "" {
		result.WriteString(currentLine)
	}

	return result.String()
}

func splitIntoWords(s string) []string {
	var words []string
	var current strings.Builder

	for _, r := range s {
		if unicode.IsSpace(r) {
			if current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
			}
			words = append(words, string(r))
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		words = append(words, current.String())
	}

	return words
}
