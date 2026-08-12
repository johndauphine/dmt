package tui

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/config"
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

// runOverrides carries /run's per-invocation flag overrides (#439).
// Zero values mean "not set" — the loaded config value stands, matching
// the CLI's c.IsSet gate. exploreOnce/exploreMode come from /explore
// session state rather than flags.
type runOverrides struct {
	sourceSchema    string
	targetSchema    string
	workers         int // 0 = unset
	skipPreflight   string
	dryRun          bool
	aiSchemaAdvisor bool
	confirmBackup   bool
	exploreOnce     bool
	exploreMode     string
}

// parseRunArgs parses /run's flags plus the config origin (#439).
func (m *Model) parseRunArgs(parts []string) (configFile, profileName string, ov runOverrides, err error) {
	strs := originFlags()
	strs["--source-schema"] = "source-schema"
	strs["--target-schema"] = "target-schema"
	strs["--workers"] = "workers"
	strs["--skip-preflight"] = "skip-preflight"
	pa, err := parseSlashArgs(argSpec{
		command: "/run",
		strs:    strs,
		bools: map[string]string{
			"--dry-run":           "dry-run",
			"--ai-schema-advisor": "ai-schema-advisor",
			"--confirm-backup":    "confirm-backup",
		},
	}, parts)
	if err != nil {
		return "", "", ov, err
	}
	if w, ok := pa.strs["workers"]; ok {
		n, convErr := strconv.Atoi(w)
		if convErr != nil || n < 1 {
			return "", "", ov, fmt.Errorf("/run: --workers requires a positive integer, got %q", w)
		}
		ov.workers = n
	}
	ov.sourceSchema = pa.strs["source-schema"]
	ov.targetSchema = pa.strs["target-schema"]
	ov.skipPreflight = pa.strs["skip-preflight"]
	ov.dryRun = pa.bools["dry-run"]
	ov.aiSchemaAdvisor = pa.bools["ai-schema-advisor"]
	ov.confirmBackup = pa.bools["confirm-backup"]
	configFile, profileName = m.resolveOrigin(pa)
	return configFile, profileName, ov, nil
}

// parseResumeArgs parses /resume [--force-resume] [--skip-preflight LIST]
// plus the config origin (#439).
func (m *Model) parseResumeArgs(parts []string) (configFile, profileName string, forceResume bool, skipPreflight string, err error) {
	strs := originFlags()
	strs["--skip-preflight"] = "skip-preflight"
	pa, err := parseSlashArgs(argSpec{
		command: "/resume",
		strs:    strs,
		bools:   map[string]string{"--force-resume": "force-resume"},
	}, parts)
	if err != nil {
		return "", "", false, "", err
	}
	configFile, profileName = m.resolveOrigin(pa)
	return configFile, profileName, pa.bools["force-resume"], pa.strs["skip-preflight"], nil
}

// parsePreflightArgs parses /preflight (alias /health-check) flags plus
// the config origin (#440).
func (m *Model) parsePreflightArgs(command string, parts []string) (configFile, profileName, skipPreflight string, aiReview bool, err error) {
	strs := originFlags()
	strs["--skip-preflight"] = "skip-preflight"
	pa, err := parseSlashArgs(argSpec{
		command: command,
		strs:    strs,
		bools:   map[string]string{"--ai-review": "ai-review"},
	}, parts)
	if err != nil {
		return "", "", "", false, err
	}
	configFile, profileName = m.resolveOrigin(pa)
	return configFile, profileName, pa.strs["skip-preflight"], pa.bools["ai-review"], nil
}

// parseTriageTimeout converts a --timeout value (Go duration syntax,
// e.g. "120s") with the CLI's <=0 → 90s default applied by callers.
func parseTriageTimeout(command, value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid --timeout %q (use Go duration syntax, e.g. 90s)", command, value)
	}
	return d, nil
}

// parseValidateArgs parses /validate [--ai-triage] [--timeout D] plus
// the config origin (#441).
func (m *Model) parseValidateArgs(parts []string) (configFile, profileName string, aiTriage bool, timeout time.Duration, err error) {
	strs := originFlags()
	strs["--timeout"] = "timeout"
	pa, err := parseSlashArgs(argSpec{
		command: "/validate",
		strs:    strs,
		bools:   map[string]string{"--ai-triage": "ai-triage"},
	}, parts)
	if err != nil {
		return "", "", false, 0, err
	}
	timeout, err = parseTriageTimeout("/validate", pa.strs["timeout"])
	if err != nil {
		return "", "", false, 0, err
	}
	configFile, profileName = m.resolveOrigin(pa)
	return configFile, profileName, pa.bools["ai-triage"], timeout, nil
}

// parseDiagnoseArgs parses /diagnose [--run ID] [--ai-triage]
// [--timeout D] plus the config origin (#441).
func (m *Model) parseDiagnoseArgs(parts []string) (configFile, profileName, runID string, aiTriage bool, timeout time.Duration, err error) {
	strs := originFlags()
	strs["--run"] = "run"
	strs["--timeout"] = "timeout"
	pa, err := parseSlashArgs(argSpec{
		command: "/diagnose",
		strs:    strs,
		bools:   map[string]string{"--ai-triage": "ai-triage"},
	}, parts)
	if err != nil {
		return "", "", "", false, 0, err
	}
	timeout, err = parseTriageTimeout("/diagnose", pa.strs["timeout"])
	if err != nil {
		return "", "", "", false, 0, err
	}
	configFile, profileName = m.resolveOrigin(pa)
	return configFile, profileName, pa.strs["run"], pa.bools["ai-triage"], timeout, nil
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

// parseAnalyzeArgs parses /analyze [--apply] [--ai-explain] plus the
// config origin.
func (m *Model) parseAnalyzeArgs(parts []string) (configFile, profileName string, apply, aiExplain bool, err error) {
	pa, err := parseSlashArgs(argSpec{
		command: "/analyze",
		strs:    originFlags(),
		bools: map[string]string{
			"-a": "apply", "--apply": "apply",
			"--ai-explain": "ai-explain",
		},
	}, parts)
	if err != nil {
		return "", "", false, false, err
	}
	configFile, profileName = m.resolveOrigin(pa)
	return configFile, profileName, pa.bools["apply"], pa.bools["ai-explain"], nil
}

// parseAIConfigReviewArgs parses /ai config-review (alias runbook)
// flags plus the config origin (#442). --request is free text, so it
// consumes the remainder of the line — put it last. parts[0] is the
// synthetic "/ai config-review" command token.
func parseAIConfigReviewArgs(m *Model, parts []string) (configFile, profileName, request string, timeout time.Duration, err error) {
	command := parts[0]
	for i, arg := range parts {
		if i == 0 {
			continue
		}
		if v, ok := strings.CutPrefix(arg, "--request="); ok {
			request = strings.Join(append([]string{v}, parts[i+1:]...), " ")
			parts = parts[:i]
			break
		}
		if arg == "--request" {
			if i+1 >= len(parts) {
				return "", "", "", 0, fmt.Errorf("%s: flag --request requires a value (see /help)", command)
			}
			request = strings.Join(parts[i+1:], " ")
			parts = parts[:i]
			break
		}
	}
	strs := originFlags()
	strs["--timeout"] = "timeout"
	pa, err := parseSlashArgs(argSpec{command: command, strs: strs}, parts)
	if err != nil {
		return "", "", "", 0, err
	}
	timeout, err = parseTriageTimeout(command, pa.strs["timeout"])
	if err != nil {
		return "", "", "", 0, err
	}
	configFile, profileName = m.resolveOrigin(pa)
	return configFile, profileName, request, timeout, nil
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
