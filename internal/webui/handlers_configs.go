package webui

import (
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// configFileDTO is one selectable config file for the origin picker.
type configFileDTO struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// handleConfigsList lists candidate dmt config files the operator can pick in
// the WebUI origin dialog. It scans a fixed, non-client-controllable set of
// directories — the process working directory and the launch --config's
// directory — for *.yaml/*.yml files that look like dmt configs (contain
// top-level source: and target: keys). No client-supplied path is used, so
// there is no directory-traversal surface.
func (s *Server) handleConfigsList(w http.ResponseWriter, r *http.Request) {
	dirs := []string{"."}
	if s.opts.ConfigPath != "" {
		if d := filepath.Dir(s.opts.ConfigPath); d != "" && d != "." {
			dirs = append(dirs, d)
		}
	}

	seen := map[string]bool{}
	out := []configFileDTO{}
	for _, d := range dirs {
		entries, err := os.ReadDir(d)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			ext := strings.ToLower(filepath.Ext(e.Name()))
			if ext != ".yaml" && ext != ".yml" {
				continue
			}
			full := filepath.Join(d, e.Name())
			abs, err := filepath.Abs(full)
			if err != nil || seen[abs] {
				continue
			}
			if !looksLikeDMTConfig(full) {
				continue
			}
			seen[abs] = true
			out = append(out, configFileDTO{Name: e.Name(), Path: abs})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, http.StatusOK, map[string]any{"configs": out})
}

// looksLikeDMTConfig cheaply checks whether a YAML file is a dmt migration
// config (has top-level source: and target: sections), so the picker doesn't
// list unrelated YAML (compose files, CI workflows, etc.). Reads a bounded
// prefix — configs are small.
func looksLikeDMTConfig(path string) bool {
	// Only inspect regular files — never block the request goroutine reading a
	// FIFO/device someone placed as *.yaml in the scanned directory.
	if fi, err := os.Lstat(path); err != nil || !fi.Mode().IsRegular() {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	buf := make([]byte, 8192)
	n, _ := f.Read(buf)
	text := string(buf[:n])
	return strings.Contains(text, "source:") && strings.Contains(text, "target:")
}
