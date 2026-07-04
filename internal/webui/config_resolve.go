package webui

import (
	"fmt"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
)

// resolveConfig turns a request's origin into a loaded config plus the
// resolved config path and state file. Precedence is request-first: an
// explicit request field beats any session default. A profile (from the same
// tier) wins over a config path, matching the TUI. So the order is:
// request profile > request config > session profile > session config >
// launch config.
func (s *Server) resolveConfig(o originReq) (cfg *config.Config, path, stateFile string, err error) {
	stateFile = firstNonEmpty(o.StateFile, s.sessionDefaults.get("state-file"), s.opts.StateFile)

	// Explicit request origin wins outright.
	switch {
	case o.Profile != "":
		cfg, err = loadProfileConfig(o.Profile)
		return cfg, "", stateFile, err
	case o.Config != "":
		cfg, err = config.Load(o.Config)
		return cfg, o.Config, stateFile, err
	}

	// Then session defaults (profile beats config), then the launch config.
	if profile := s.sessionDefaults.get("profile"); profile != "" {
		cfg, err = loadProfileConfig(profile)
		return cfg, "", stateFile, err
	}
	path = firstNonEmpty(s.sessionDefaults.get("config"), s.opts.ConfigPath, "config.yaml")
	cfg, err = config.Load(path)
	if err != nil {
		return nil, "", "", err
	}
	return cfg, path, stateFile, nil
}

// loadProfileConfig decrypts a saved profile and parses it into a config,
// replicating the CLI's cmd/migrate/config_loader.go loadProfileConfig path:
// DefaultDataDir → checkpoint store → GetProfile (AES-GCM) → LoadBytes.
func loadProfileConfig(name string) (*config.Config, error) {
	dir, err := config.DefaultDataDir()
	if err != nil {
		return nil, err
	}
	state, err := checkpoint.New(dir)
	if err != nil {
		return nil, err
	}
	defer state.Close()
	blob, err := state.GetProfile(name)
	if err != nil {
		return nil, fmt.Errorf("loading profile %q: %w", name, err)
	}
	return config.LoadBytes(blob)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
