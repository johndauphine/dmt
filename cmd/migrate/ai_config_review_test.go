package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/aicopilot"

	"github.com/urfave/cli/v2"
)

func TestOutputAIConfigReviewOutputFileAlsoPrintsHumanSummary(t *testing.T) {
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "review.json")

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "output-file"},
		},
		Action: func(c *cli.Context) error {
			return outputAIConfigReview(c, &aicopilot.ConfigReview{
				Status:        "unavailable",
				PromptVersion: aicopilot.ConfigReviewPromptVersion,
				Summary:       "fallback summary",
			})
		},
	}

	stdout := captureStdout(t, func() {
		if err := app.Run([]string{"app", "--output-file", outFile}); err != nil {
			t.Fatalf("app.Run() error = %v", err)
		}
	})
	if !strings.Contains(stdout, "fallback summary") || !strings.Contains(stdout, "Wrote AI config review JSON") {
		t.Fatalf("stdout should include human summary and write confirmation, got: %s", stdout)
	}
	if _, err := os.Stat(outFile); err != nil {
		t.Fatalf("expected output file: %v", err)
	}
}

func TestOutputAIConfigReviewJSONAndOutputFilePrintsConfirmationToStderr(t *testing.T) {
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "review.json")

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "json"},
			&cli.StringFlag{Name: "output-file"},
		},
		Action: func(c *cli.Context) error {
			return outputAIConfigReview(c, &aicopilot.ConfigReview{
				Status:        "ok",
				PromptVersion: aicopilot.ConfigReviewPromptVersion,
				Summary:       "json summary",
			})
		},
	}

	stdout, stderr := captureOutput(t, func() {
		if err := app.Run([]string{"app", "--json", "--output-file", outFile}); err != nil {
			t.Fatalf("app.Run() error = %v", err)
		}
	})
	if !strings.Contains(stderr, "Wrote AI config review JSON") {
		t.Fatalf("stderr should include write confirmation, got: %s", stderr)
	}
	var parsed aicopilot.ConfigReview
	if err := json.Unmarshal([]byte(stdout), &parsed); err != nil {
		t.Fatalf("stdout should remain JSON-only, got %q: %v", stdout, err)
	}
	if parsed.Summary != "json summary" {
		t.Fatalf("summary = %q, want json summary", parsed.Summary)
	}
	if _, err := os.Stat(outFile); err != nil {
		t.Fatalf("expected output file: %v", err)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() {
		os.Stdout = old
	}()

	fn()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func captureOutput(t *testing.T, fn func()) (string, string) {
	t.Helper()
	oldStdout := os.Stdout
	oldStderr := os.Stderr
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = stdoutW
	os.Stderr = stderrW
	defer func() {
		os.Stdout = oldStdout
		os.Stderr = oldStderr
	}()

	fn()
	if err := stdoutW.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stderrW.Close(); err != nil {
		t.Fatal(err)
	}
	stdout, err := io.ReadAll(stdoutR)
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := io.ReadAll(stderrR)
	if err != nil {
		t.Fatal(err)
	}
	return string(stdout), string(stderr)
}
