package setup

import (
	"sort"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/secrets"
)

func (s *State) defaultPort(dbType string) int {
	d, err := driver.Get(dbType)
	if err == nil {
		return d.Defaults().Port
	}
	switch dbType {
	case "mssql":
		return 1433
	case "postgres":
		return 5432
	case "mysql":
		return 3306
	default:
		return 5432
	}
}

func (s *State) defaultSchema(dbType string) string {
	d, err := driver.Get(dbType)
	if err == nil && d.Defaults().Schema != "" {
		return d.Defaults().Schema
	}
	switch dbType {
	case "mssql":
		return "dbo"
	case "postgres":
		return "public"
	default:
		return ""
	}
}

func (s *State) defaultUser(dbType string) string {
	switch dbType {
	case "mssql":
		return "sa"
	case "postgres":
		return "postgres"
	case "mysql":
		return "root"
	default:
		return ""
	}
}

func (s *State) sslPrompt(dbType, sslMode string, trustCert bool) PromptInfo {
	switch dbType {
	case "postgres":
		def := "prefer"
		if sslMode != "" {
			def = sslMode
		}
		return PromptInfo{
			Text:    "SSL mode (disable/prefer/require/verify-ca/verify-full)",
			Default: def,
		}
	case "mssql":
		def := "n"
		if trustCert {
			def = "y"
		}
		return PromptInfo{
			Text:    "Trust server certificate? (y/n)",
			Default: def,
			Choices: []string{"y", "n"},
		}
	case "mysql":
		// Default is "require" rather than "preferred"; preferred
		// allows silent plaintext downgrade and isn't safe as a
		// default. (#252) Operators who explicitly want downgradeable
		// TLS can type "preferred" at the prompt. Form is the
		// Postgres-style canonical (disable/require/verify-ca/verify-full)
		// documented in dbconfig.SourceConfig.SSLMode; the dialect
		// accepts the MySQL-native variants as aliases.
		def := "require"
		if sslMode != "" {
			def = sslMode
		}
		return PromptInfo{
			Text:    "SSL mode (disable/require/verify-ca/verify-full, or 'preferred' for downgradeable TLS)",
			Default: def,
		}
	default:
		return PromptInfo{Text: "SSL mode", Default: "disable"}
	}
}

func (s *State) processSSL(input string, isSource bool) {
	if isSource {
		switch s.Config.Source.Type {
		case "postgres":
			if input == "" {
				if s.Config.Source.SSLMode == "" {
					s.Config.Source.SSLMode = "prefer"
				}
			} else {
				s.Config.Source.SSLMode = input
			}
		case "mssql":
			if input != "" {
				v := strings.ToLower(input)
				s.Config.Source.TrustServerCert = (v == "y" || v == "yes" || v == "true")
			}
		case "mysql":
			if input == "" {
				if s.Config.Source.SSLMode == "" {
					s.Config.Source.SSLMode = "require" // #252 safe default
				}
			} else {
				s.Config.Source.SSLMode = input
			}
		}
	} else {
		switch s.Config.Target.Type {
		case "postgres":
			if input == "" {
				if s.Config.Target.SSLMode == "" {
					s.Config.Target.SSLMode = "prefer"
				}
			} else {
				s.Config.Target.SSLMode = input
			}
		case "mssql":
			if input != "" {
				v := strings.ToLower(input)
				s.Config.Target.TrustServerCert = (v == "y" || v == "yes" || v == "true")
			}
		case "mysql":
			if input == "" {
				if s.Config.Target.SSLMode == "" {
					s.Config.Target.SSLMode = "require" // #252 safe default
				}
			} else {
				s.Config.Target.SSLMode = input
			}
		}
	}
}

func sortedProviderNames() []string {
	var names []string
	for name := range secrets.KnownProviders {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func defaultIfEmpty(val, def string) string {
	if val != "" {
		return val
	}
	return def
}

// boolDefault returns "y"/"n" for prompt display.
// In EditMode (config was loaded), an explicit loaded value wins: a user who
// set `create_indexes: false` must see "n" as the default and Enter must
// preserve it. If the field was omitted, fall back to the supplied dmt default.
func (s *State) boolDefault(loaded *bool, freshDefault string) string {
	if s.EditMode && loaded != nil {
		if *loaded {
			return "y"
		}
		return "n"
	}
	return freshDefault
}
