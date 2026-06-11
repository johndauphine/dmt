package tui

import (
	"strings"
	"testing"
)

// #439: /run flag parsing mirrors the CLI run command's flags.
func TestParseRunArgs(t *testing.T) {
	m := &Model{}

	t.Run("all flags parse", func(t *testing.T) {
		cf, pn, ov, err := m.parseRunArgs([]string{
			"/run", "@my.yaml", "--dry-run", "--ai-schema-advisor",
			"--source-schema", "sales", "--target-schema=analytics",
			"--workers", "12", "--skip-preflight", "privileges",
		})
		if err != nil {
			t.Fatal(err)
		}
		if cf != "my.yaml" || pn != "" {
			t.Fatalf("origin: %q %q", cf, pn)
		}
		want := runOverrides{
			sourceSchema:    "sales",
			targetSchema:    "analytics",
			workers:         12,
			skipPreflight:   "privileges",
			dryRun:          true,
			aiSchemaAdvisor: true,
		}
		if ov != want {
			t.Fatalf("overrides = %+v, want %+v", ov, want)
		}
	})

	t.Run("no flags leaves overrides zero", func(t *testing.T) {
		_, _, ov, err := m.parseRunArgs([]string{"/run"})
		if err != nil {
			t.Fatal(err)
		}
		if ov != (runOverrides{}) {
			t.Fatalf("overrides = %+v, want zero", ov)
		}
	})

	t.Run("workers must be a positive integer", func(t *testing.T) {
		for _, bad := range []string{"abc", "0", "-3"} {
			_, _, _, err := m.parseRunArgs([]string{"/run", "--workers", bad})
			if err == nil || !strings.Contains(err.Error(), "--workers requires a positive integer") {
				t.Fatalf("workers=%q: err = %v", bad, err)
			}
		}
	})

	t.Run("unknown flag errors", func(t *testing.T) {
		_, _, _, err := m.parseRunArgs([]string{"/run", "--bogus"})
		if err == nil || !strings.Contains(err.Error(), "/run: unknown flag --bogus") {
			t.Fatalf("err = %v", err)
		}
	})
}

// #439: /resume gains --force-resume and --skip-preflight.
func TestParseResumeArgs(t *testing.T) {
	m := &Model{}

	cf, pn, force, skip, err := m.parseResumeArgs([]string{
		"/resume", "--profile", "prod", "--force-resume", "--skip-preflight", "all",
	})
	if err != nil {
		t.Fatal(err)
	}
	// configFile keeps its "config.yaml" fallback even with a profile;
	// loadConfigFromOrigin prefers the profile when set.
	if cf != "config.yaml" || pn != "prod" {
		t.Fatalf("origin: %q %q", cf, pn)
	}
	if !force || skip != "all" {
		t.Fatalf("force=%v skip=%q", force, skip)
	}

	_, _, force, skip, err = m.parseResumeArgs([]string{"/resume"})
	if err != nil {
		t.Fatal(err)
	}
	if force || skip != "" {
		t.Fatalf("defaults: force=%v skip=%q", force, skip)
	}

	_, _, _, _, err = m.parseResumeArgs([]string{"/resume", "--dry-run"})
	if err == nil || !strings.Contains(err.Error(), "/resume: unknown flag --dry-run") {
		t.Fatalf("err = %v", err)
	}
}
