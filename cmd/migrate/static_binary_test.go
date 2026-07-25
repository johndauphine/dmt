package main

import (
	"debug/elf"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const smtModuleVersion = "github.com/johndauphine/smt v1.1.0"

func TestSMTDependencyIsVersioned(t *testing.T) {
	goMod, err := os.ReadFile(filepath.Join(repoRoot(t), "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	if !strings.Contains(string(goMod), smtModuleVersion) {
		t.Fatalf("go.mod must require %s", smtModuleVersion)
	}
}

func TestStaticBinaryBuildIncludesSMT(t *testing.T) {
	if testing.Short() {
		t.Skip("build integration test")
	}
	root := repoRoot(t)
	binary := filepath.Join(t.TempDir(), "dmt-linux-amd64")
	build := exec.Command("go", "build", "-trimpath", "-o", binary, "./cmd/migrate")
	build.Dir = root
	build.Env = append(os.Environ(),
		"CGO_ENABLED=0",
		"GOOS=linux",
		"GOARCH=amd64",
		"GOWORK=off",
		"GOCACHE="+filepath.Join(t.TempDir(), "gocache"),
	)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("static dmt build failed: %v\n%s", err, output)
	}

	file, err := elf.Open(binary)
	if err != nil {
		t.Fatalf("open Linux binary as ELF: %v", err)
	}
	defer file.Close()
	for _, program := range file.Progs {
		if program.Type == elf.PT_INTERP {
			t.Fatal("CGO_ENABLED=0 build has a dynamic ELF interpreter")
		}
	}

	version := exec.Command("go", "version", "-m", binary)
	if output, err := version.CombinedOutput(); err != nil {
		t.Fatalf("read binary build info: %v\n%s", err, output)
	} else if !strings.Contains(string(output), "github.com/johndauphine/smt\tv1.1.0") {
		t.Fatalf("binary build info does not consume SMT v1.1.0:\n%s", output)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate static_binary_test.go")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
