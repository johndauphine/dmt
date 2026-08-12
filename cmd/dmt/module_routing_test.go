package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/version"
)

const moduleRoot = "github.com/johndauphine/dmt"

func TestModulePathMatchesVersionMajor(t *testing.T) {
	goMod, err := os.ReadFile(filepath.Join(repoRoot(t), "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}

	major := strings.Split(strings.TrimPrefix(version.Version, "v"), ".")[0]
	want := "module " + moduleRoot + "/v" + major
	if !strings.HasPrefix(string(goMod), want+"\n") {
		t.Fatalf("go.mod module directive must be %q for version %q", want, version.Version)
	}
}

func TestCommandDirectoryMatchesApplicationName(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate module_routing_test.go")
	}
	if got := filepath.Base(filepath.Dir(file)); got != version.Name {
		t.Fatalf("command directory = %q, want application name %q", got, version.Name)
	}
}
