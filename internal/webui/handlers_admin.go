package webui

import (
	"net/http"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/command"
	"github.com/johndauphine/dmt/internal/config"
)

// withProfileStore opens the checkpoint profile store and runs fn, always
// closing it. Profiles are AES-GCM encrypted with the master key from the
// secrets file or DMT_MASTER_KEY.
func withProfileStore(fn func(*checkpoint.State) error) error {
	dir, err := config.DefaultDataDir()
	if err != nil {
		return err
	}
	state, err := checkpoint.New(dir)
	if err != nil {
		return err
	}
	defer state.Close()
	return fn(state)
}

// profileDTO is one profile's public metadata. The encrypted config blob is
// never included — it can hold connection secrets.
type profileDTO struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// handleProfilesList returns saved profiles (names + metadata only).
func (s *Server) handleProfilesList(w http.ResponseWriter, r *http.Request) {
	var out []profileDTO
	err := withProfileStore(func(st *checkpoint.State) error {
		infos, err := st.ListProfiles()
		if err != nil {
			return err
		}
		out = make([]profileDTO, len(infos))
		for i, p := range infos {
			out[i] = profileDTO{
				Name:        p.Name,
				Description: p.Description,
				CreatedAt:   p.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
				UpdatedAt:   p.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"),
			}
		}
		return nil
	})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"profiles": out})
}

// handleProfileSave stores a profile from a config file, mirroring
// `dmt profile save`. The config is loaded (and validated) then re-marshaled
// and encrypted.
func (s *Server) handleProfileSave(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Config string `json:"config"`
		Name   string `json:"name"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	path := firstNonEmpty(req.Config, s.sessionDefaults.get("config"), s.opts.ConfigPath, "config.yaml")
	cfg, err := config.Load(path)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	name := firstNonEmpty(req.Name, cfg.Profile.Name)
	if name == "" {
		writeError(w, http.StatusBadRequest, "missing_name", "profile name required (set profile.name in config or pass name)")
		return
	}
	payload, err := yaml.Marshal(cfg)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if err := withProfileStore(func(st *checkpoint.State) error {
		return st.SaveProfile(name, cfg.Profile.Description, payload)
	}); err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "name": name})
}

// handleProfileDelete removes a saved profile.
func (s *Server) handleProfileDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := withProfileStore(func(st *checkpoint.State) error {
		return st.DeleteProfile(name)
	}); err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

// handleProfileExport writes a decrypted profile to a server-owned exports
// directory, mirroring `dmt profile export`. The decrypted config can contain
// resolved secrets, so (a) it is written to disk rather than returned to the
// browser, and (b) the destination is confined to <data-dir>/exports with a
// sanitized basename — a client-supplied path is NEVER used verbatim, which
// would be an arbitrary-file-write of secrets (especially on a remote bind).
func (s *Server) handleProfileExport(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req struct {
		Filename string `json:"filename"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	// filepath.Base strips any directory components (…/x, ../x → x); reject
	// the degenerate results too.
	fname := filepath.Base(firstNonEmpty(req.Filename, name+".yaml"))
	if fname == "." || fname == ".." || fname == string(filepath.Separator) {
		writeError(w, http.StatusBadRequest, "invalid_filename", "filename must be a simple file name")
		return
	}
	dir, err := config.DefaultDataDir()
	if err != nil {
		writeAPIError(w, err)
		return
	}
	exportsDir := filepath.Join(dir, "exports")
	if err := os.MkdirAll(exportsDir, 0700); err != nil {
		writeAPIError(w, err)
		return
	}
	out := filepath.Join(exportsDir, fname)
	if err := withProfileStore(func(st *checkpoint.State) error {
		blob, err := st.GetProfile(name)
		if err != nil {
			return err
		}
		return os.WriteFile(out, blob, 0600)
	}); err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "path": out})
}

// handleInitSecrets creates a secrets template file, mirroring
// `dmt init-secrets`. No secret values are returned to the browser.
func (s *Server) handleInitSecrets(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Force  bool `json:"force"`
		WithAI bool `json:"with_ai"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	msg, err := command.InitSecrets(req.Force, req.WithAI)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "message": msg})
}

// handleCacheClear clears the type-mapping cache, mirroring `dmt cache clear`.
func (s *Server) handleCacheClear(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AIOnly bool `json:"ai_only"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	msg, err := command.ClearTypeCache(req.AIOnly)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "message": msg})
}
