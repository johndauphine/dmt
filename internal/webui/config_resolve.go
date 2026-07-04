package webui

import (
	"fmt"

	"github.com/johndauphine/dmt/internal/config"
)

// resolveConfig turns a request's origin into a loaded config plus the
// resolved config path and state file, layering the request over the server's
// launch defaults (--config / --state-file). Profile-based resolution is
// deferred to #581, which owns the encrypted profile store; requesting a
// profile here fails with a clear message rather than silently ignoring it.
func (s *Server) resolveConfig(o originReq) (cfg *config.Config, path, stateFile string, err error) {
	if o.Profile != "" {
		return nil, "", "", &configErr{fmt.Errorf("profile-based config is not yet available in the WebUI (#581); pass a config path instead")}
	}

	path = firstNonEmpty(o.Config, s.opts.ConfigPath, "config.yaml")
	cfg, err = config.Load(path)
	if err != nil {
		return nil, "", "", err
	}

	stateFile = firstNonEmpty(o.StateFile, s.opts.StateFile)
	return cfg, path, stateFile, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
