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

func parseConfigArgs(parts []string) (string, string) {
	configFile := "config.yaml"
	profileName := ""

	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		if arg == "--profile" && i+1 < len(parts) {
			profileName = parts[i+1]
			i++
			continue
		}
		if strings.HasPrefix(arg, "@") {
			configFile = arg[1:]
		} else {
			configFile = arg
		}
	}

	return configFile, profileName
}

func parseHistoryArgs(parts []string) (string, string, string) {
	configFile := "config.yaml"
	profileName := ""
	runID := ""

	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		switch arg {
		case "--run":
			if i+1 < len(parts) {
				runID = parts[i+1]
				i++
			}
		case "--profile":
			if i+1 < len(parts) {
				profileName = parts[i+1]
				i++
			}
		default:
			if strings.HasPrefix(arg, "@") {
				configFile = arg[1:]
			} else {
				configFile = arg
			}
		}
	}

	return configFile, profileName, runID
}

func parseStatusArgs(parts []string) (string, string, bool) {
	configFile := "config.yaml"
	profileName := ""
	detailed := false

	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		switch arg {
		case "--detailed", "-d":
			detailed = true
		case "--profile":
			if i+1 < len(parts) {
				profileName = parts[i+1]
				i++
			}
		default:
			if strings.HasPrefix(arg, "@") {
				configFile = arg[1:]
			} else {
				configFile = arg
			}
		}
	}

	return configFile, profileName, detailed
}

func parseAnalyzeArgs(parts []string) (string, string, bool) {
	configFile := "config.yaml"
	profileName := ""
	apply := false

	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		switch arg {
		case "--apply", "-a":
			apply = true
		case "--profile":
			if i+1 < len(parts) {
				profileName = parts[i+1]
				i++
			}
		default:
			if strings.HasPrefix(arg, "@") {
				configFile = arg[1:]
			} else {
				configFile = arg
			}
		}
	}

	return configFile, profileName, apply
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
