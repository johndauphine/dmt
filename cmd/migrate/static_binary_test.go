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

const (
	smtModulePath    = "github.com/johndauphine/smt"
	smtModuleVersion = smtModulePath + " v1.2.0"
)

func TestSMTDependencyIsVersioned(t *testing.T) {
	goMod, err := os.ReadFile(filepath.Join(repoRoot(t), "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	if !strings.Contains(string(goMod), smtModuleVersion) {
		t.Fatalf("go.mod must require %s", smtModuleVersion)
	}
	if hasModuleReplace(string(goMod), smtModulePath) {
		t.Fatalf("go.mod must consume %s as a versioned module, not a local replace", smtModulePath)
	}
}

func TestHasModuleReplace(t *testing.T) {
	tests := []struct {
		name  string
		goMod string
		want  bool
	}{
		{name: "no replace", goMod: "require " + smtModuleVersion, want: false},
		{name: "single line SMT replace", goMod: "replace " + smtModulePath + " => ../smt", want: true},
		{name: "single line unrelated replace", goMod: "replace example.com/other => ../other", want: false},
		{name: "block SMT replace", goMod: "replace (\n\t" + smtModulePath + " => ../smt\n)", want: true},
		{name: "block unrelated replace", goMod: "replace (\n\texample.com/other => ../other\n)", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasModuleReplace(tt.goMod, smtModulePath); got != tt.want {
				t.Errorf("hasModuleReplace() = %v, want %v", got, tt.want)
			}
		})
	}
}

func hasModuleReplace(goMod, modulePath string) bool {
	inBlock := false
	for _, line := range strings.Split(goMod, "\n") {
		line = strings.TrimSpace(strings.SplitN(line, "//", 2)[0])
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if inBlock {
			if line == ")" {
				inBlock = false
				continue
			}
			if len(fields) > 0 && fields[0] == modulePath {
				return true
			}
			continue
		}
		if len(fields) == 0 || fields[0] != "replace" {
			continue
		}
		if len(fields) == 2 && fields[1] == "(" {
			inBlock = true
			continue
		}
		if len(fields) > 1 && fields[1] == modulePath {
			return true
		}
	}
	return false
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
	} else if !strings.Contains(string(output), smtModulePath+"\tv1.2.0") {
		t.Fatalf("binary build info does not consume SMT v1.2.0:\n%s", output)
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
