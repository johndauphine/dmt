package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/dmt/v5/internal/orchestrator"
	"github.com/urfave/cli/v2"
)

func TestGlobalStateFileFlag(t *testing.T) {
	tests := []struct {
		name           string
		globalFlag     string
		expectedResult string
	}{
		{
			name:           "no flag set",
			globalFlag:     "",
			expectedResult: "",
		},
		{
			name:           "global flag set",
			globalFlag:     "/tmp/global-state.yaml",
			expectedResult: "/tmp/global-state.yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &cli.App{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name: "state-file",
					},
				},
				Commands: []*cli.Command{
					{
						Name: "run",
						Action: func(c *cli.Context) error {
							result := c.String("state-file")
							if result != tt.expectedResult {
								t.Errorf("state-file = %q, want %q", result, tt.expectedResult)
							}
							return nil
						},
					},
				},
			}

			args := []string{"app"}
			if tt.globalFlag != "" {
				args = append(args, "--state-file", tt.globalFlag)
			}
			args = append(args, "run")

			if err := app.Run(args); err != nil {
				t.Fatalf("app.Run() error: %v", err)
			}
		})
	}
}

func TestGetConfigPath(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		expectedPath string
		expectedSet  bool
	}{
		{
			name:         "no flag set returns default",
			args:         []string{"app", "run"},
			expectedPath: "config.yaml",
			expectedSet:  false,
		},
		{
			name:         "config after subcommand",
			args:         []string{"app", "run", "-c", "myconfig.yaml"},
			expectedPath: "myconfig.yaml",
			expectedSet:  true,
		},
		{
			name:         "config before subcommand",
			args:         []string{"app", "-c", "myconfig.yaml", "run"},
			expectedPath: "myconfig.yaml",
			expectedSet:  true,
		},
		{
			name:         "long flag after subcommand",
			args:         []string{"app", "run", "--config", "/etc/dmt/prod.yaml"},
			expectedPath: "/etc/dmt/prod.yaml",
			expectedSet:  true,
		},
		{
			name:         "long flag before subcommand",
			args:         []string{"app", "--config", "/etc/dmt/prod.yaml", "run"},
			expectedPath: "/etc/dmt/prod.yaml",
			expectedSet:  true,
		},
		{
			name:         "command-level takes precedence over app-level",
			args:         []string{"app", "-c", "global.yaml", "run", "-c", "local.yaml"},
			expectedPath: "local.yaml",
			expectedSet:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &cli.App{
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "config",
						Aliases: []string{"c"},
						Value:   "config.yaml",
					},
				},
				Commands: []*cli.Command{
					{
						Name: "run",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:    "config",
								Aliases: []string{"c"},
								Value:   "config.yaml",
							},
						},
						Action: func(c *cli.Context) error {
							path, isSet := getConfigPath(c)
							if path != tt.expectedPath {
								t.Errorf("getConfigPath() path = %q, want %q", path, tt.expectedPath)
							}
							if isSet != tt.expectedSet {
								t.Errorf("getConfigPath() isSet = %v, want %v", isSet, tt.expectedSet)
							}
							return nil
						},
					},
				},
			}

			if err := app.Run(tt.args); err != nil {
				t.Fatalf("app.Run() error: %v", err)
			}
		})
	}
}

func TestOutputJSON(t *testing.T) {
	t.Run("output to stdout", func(t *testing.T) {
		result := &orchestrator.MigrationResult{
			RunID:           "test-run-123",
			Status:          "success",
			StartedAt:       time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
			CompletedAt:     time.Date(2025, 1, 15, 10, 5, 0, 0, time.UTC),
			DurationSeconds: 300,
			TablesTotal:     5,
			TablesSuccess:   5,
			TablesFailed:    0,
			RowsTransferred: 100000,
			RowsPerSecond:   333,
			FailedTables:    []string{},
			TableStats:      []orchestrator.TableResult{},
		}

		// Capture stdout
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		app := &cli.App{
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "output-json"},
				&cli.StringFlag{Name: "output-file"},
			},
			Action: func(c *cli.Context) error {
				return outputJSON(c, result)
			},
		}

		err := app.Run([]string{"app", "--output-json"})
		w.Close()
		os.Stdout = oldStdout

		if err != nil {
			t.Fatalf("outputJSON() error: %v", err)
		}

		var buf bytes.Buffer
		buf.ReadFrom(r)
		output := buf.String()

		// Verify JSON is valid
		var parsed orchestrator.MigrationResult
		if err := json.Unmarshal([]byte(output), &parsed); err != nil {
			t.Fatalf("invalid JSON output: %v\nOutput: %s", err, output)
		}

		if parsed.RunID != "test-run-123" {
			t.Errorf("parsed.RunID = %q, want %q", parsed.RunID, "test-run-123")
		}
	})

	t.Run("output to file", func(t *testing.T) {
		tmpDir := t.TempDir()
		outFile := filepath.Join(tmpDir, "result.json")

		result := &orchestrator.MigrationResult{
			RunID:       "test-run-456",
			Status:      "success",
			TablesTotal: 3,
		}

		app := &cli.App{
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "output-json"},
				&cli.StringFlag{Name: "output-file"},
			},
			Action: func(c *cli.Context) error {
				return outputJSON(c, result)
			},
		}

		err := app.Run([]string{"app", "--output-file", outFile})
		if err != nil {
			t.Fatalf("outputJSON() error: %v", err)
		}

		// Verify file was written
		data, err := os.ReadFile(outFile)
		if err != nil {
			t.Fatalf("failed to read output file: %v", err)
		}

		var parsed orchestrator.MigrationResult
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("invalid JSON in file: %v", err)
		}

		if parsed.RunID != "test-run-456" {
			t.Errorf("parsed.RunID = %q, want %q", parsed.RunID, "test-run-456")
		}
	})

	t.Run("output to both stdout and file", func(t *testing.T) {
		tmpDir := t.TempDir()
		outFile := filepath.Join(tmpDir, "result.json")

		result := &orchestrator.MigrationResult{
			RunID:  "test-run-789",
			Status: "success",
		}

		// Capture stdout
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		app := &cli.App{
			Flags: []cli.Flag{
				&cli.BoolFlag{Name: "output-json"},
				&cli.StringFlag{Name: "output-file"},
			},
			Action: func(c *cli.Context) error {
				return outputJSON(c, result)
			},
		}

		err := app.Run([]string{"app", "--output-json", "--output-file", outFile})
		w.Close()
		os.Stdout = oldStdout

		if err != nil {
			t.Fatalf("outputJSON() error: %v", err)
		}

		// Check stdout
		var buf bytes.Buffer
		buf.ReadFrom(r)
		if buf.Len() == 0 {
			t.Error("expected output to stdout")
		}

		// Check file
		if _, err := os.Stat(outFile); os.IsNotExist(err) {
			t.Error("expected output file to be created")
		}
	})
}

func TestCLIFlagParsing(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		validate func(c *cli.Context) error
	}{
		{
			name: "run-id flag",
			args: []string{"app", "--run-id", "my-custom-id", "run"},
			validate: func(c *cli.Context) error {
				if c.String("run-id") != "my-custom-id" {
					t.Errorf("run-id = %q, want %q", c.String("run-id"), "my-custom-id")
				}
				return nil
			},
		},
		{
			name: "log-format flag default",
			args: []string{"app", "run"},
			validate: func(c *cli.Context) error {
				if lf := c.String("log-format"); lf != "text" {
					t.Errorf("log-format = %q, want %q", lf, "text")
				}
				return nil
			},
		},
		{
			name: "log-format flag json",
			args: []string{"app", "--log-format", "json", "run"},
			validate: func(c *cli.Context) error {
				if lf := c.String("log-format"); lf != "json" {
					t.Errorf("log-format = %q, want %q", lf, "json")
				}
				return nil
			},
		},
		{
			name: "verbosity flag",
			args: []string{"app", "--verbosity", "debug", "run"},
			validate: func(c *cli.Context) error {
				if v := c.String("verbosity"); v != "debug" {
					t.Errorf("verbosity = %q, want %q", v, "debug")
				}
				return nil
			},
		},
		{
			name: "output-json flag",
			args: []string{"app", "--output-json", "run"},
			validate: func(c *cli.Context) error {
				if !c.Bool("output-json") {
					t.Error("expected output-json to be true")
				}
				return nil
			},
		},
		{
			name: "config flag before subcommand",
			args: []string{"app", "--config", "custom.yaml", "run"},
			validate: func(c *cli.Context) error {
				if cfg := c.String("config"); cfg != "custom.yaml" {
					t.Errorf("config = %q, want %q", cfg, "custom.yaml")
				}
				return nil
			},
		},
		{
			name: "profile flag before subcommand",
			args: []string{"app", "--profile", "myprofile", "run"},
			validate: func(c *cli.Context) error {
				if p := c.String("profile"); p != "myprofile" {
					t.Errorf("profile = %q, want %q", p, "myprofile")
				}
				return nil
			},
		},
		{
			name: "state-file flag before subcommand",
			args: []string{"app", "--state-file", "/tmp/state.yaml", "run"},
			validate: func(c *cli.Context) error {
				if sf := c.String("state-file"); sf != "/tmp/state.yaml" {
					t.Errorf("state-file = %q, want %q", sf, "/tmp/state.yaml")
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &cli.App{
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "config", Aliases: []string{"c"}, Value: "config.yaml"},
					&cli.StringFlag{Name: "state-file"},
					&cli.StringFlag{Name: "profile"},
					&cli.StringFlag{Name: "run-id"},
					&cli.BoolFlag{Name: "output-json"},
					&cli.StringFlag{Name: "output-file"},
					&cli.StringFlag{Name: "log-format", Value: "text"},
					&cli.StringFlag{Name: "verbosity", Value: "info"},
				},
				Commands: []*cli.Command{
					{
						Name: "run",
						Flags: []cli.Flag{
							&cli.StringFlag{Name: "source-schema", Value: "dbo"},
							&cli.StringFlag{Name: "target-schema", Value: "public"},
							&cli.IntFlag{Name: "workers", Value: 8},
						},
						Action: tt.validate,
					},
				},
			}

			if err := app.Run(tt.args); err != nil {
				t.Fatalf("app.Run() error: %v", err)
			}
		})
	}
}

func TestRunCommandDefaults(t *testing.T) {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config", Aliases: []string{"c"}, Value: "config.yaml"},
		},
		Commands: []*cli.Command{
			{
				Name: "run",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "source-schema", Value: "dbo"},
					&cli.StringFlag{Name: "target-schema", Value: "public"},
					&cli.IntFlag{Name: "workers", Value: 8},
				},
				Action: func(c *cli.Context) error {
					if c.String("source-schema") != "dbo" {
						t.Errorf("source-schema = %q, want %q", c.String("source-schema"), "dbo")
					}
					if c.String("target-schema") != "public" {
						t.Errorf("target-schema = %q, want %q", c.String("target-schema"), "public")
					}
					if c.Int("workers") != 8 {
						t.Errorf("workers = %d, want %d", c.Int("workers"), 8)
					}
					return nil
				},
			},
		},
	}

	if err := app.Run([]string{"app", "run"}); err != nil {
		t.Fatalf("app.Run() error: %v", err)
	}
}

func TestResumeCommandFlags(t *testing.T) {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config", Aliases: []string{"c"}, Value: "config.yaml"},
		},
		Commands: []*cli.Command{
			{
				Name: "resume",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "force-resume"},
				},
				Action: func(c *cli.Context) error {
					if !c.Bool("force-resume") {
						t.Error("expected force-resume to be true")
					}
					return nil
				},
			},
		},
	}

	if err := app.Run([]string{"app", "resume", "--force-resume"}); err != nil {
		t.Fatalf("app.Run() error: %v", err)
	}
}

func TestStatusCommandJSONFlag(t *testing.T) {
	app := &cli.App{
		Commands: []*cli.Command{
			{
				Name: "status",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "json"},
				},
				Action: func(c *cli.Context) error {
					if !c.Bool("json") {
						t.Error("expected json flag to be true")
					}
					return nil
				},
			},
		},
	}

	if err := app.Run([]string{"app", "status", "--json"}); err != nil {
		t.Fatalf("app.Run() error: %v", err)
	}
}

func TestAIConfigReviewCommandLocalOutputFlags(t *testing.T) {
	app := &cli.App{
		Commands: []*cli.Command{
			{
				Name: "ai",
				Subcommands: []*cli.Command{
					{
						Name: "config-review",
						Flags: []cli.Flag{
							&cli.StringFlag{Name: "state-file"},
							&cli.BoolFlag{Name: "output-json"},
							&cli.StringFlag{Name: "output-file"},
						},
						Action: func(c *cli.Context) error {
							if got := aiConfigReviewString(c, "state-file"); got != "state.yaml" {
								t.Errorf("state-file = %q, want state.yaml", got)
							}
							if !aiConfigReviewBool(c, "output-json") {
								t.Error("expected output-json to be true")
							}
							if got := aiConfigReviewOutputFile(c); got != "review.json" {
								t.Errorf("output-file = %q, want review.json", got)
							}
							return nil
						},
					},
				},
			},
		},
	}

	if err := app.Run([]string{"app", "ai", "config-review", "--state-file", "state.yaml", "--output-json", "--output-file", "review.json"}); err != nil {
		t.Fatalf("app.Run() error: %v", err)
	}
}

func TestAIConfigReviewCommandUsesGlobalOutputFlags(t *testing.T) {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "state-file"},
			&cli.BoolFlag{Name: "output-json"},
			&cli.StringFlag{Name: "output-file"},
		},
		Commands: []*cli.Command{
			{
				Name: "ai",
				Subcommands: []*cli.Command{
					{
						Name: "config-review",
						Flags: []cli.Flag{
							&cli.StringFlag{Name: "state-file"},
							&cli.BoolFlag{Name: "output-json"},
							&cli.StringFlag{Name: "output-file"},
						},
						Action: func(c *cli.Context) error {
							if got := aiConfigReviewString(c, "state-file"); got != "global-state.yaml" {
								t.Errorf("state-file = %q, want global-state.yaml", got)
							}
							if !aiConfigReviewJSONRequested(c) {
								t.Error("expected global output-json to be honored")
							}
							if got := aiConfigReviewOutputFile(c); got != "global-review.json" {
								t.Errorf("output-file = %q, want global-review.json", got)
							}
							return nil
						},
					},
				},
			},
		},
	}

	if err := app.Run([]string{"app", "--state-file", "global-state.yaml", "--output-json", "--output-file", "global-review.json", "ai", "config-review"}); err != nil {
		t.Fatalf("app.Run() error: %v", err)
	}
}

func TestProfileSubcommands(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		validate func(c *cli.Context) error
	}{
		{
			name: "profile save with name and global config",
			args: []string{"app", "--config", "test.yaml", "profile", "save", "--name", "myprofile"},
			validate: func(c *cli.Context) error {
				if c.String("name") != "myprofile" {
					t.Errorf("name = %q, want %q", c.String("name"), "myprofile")
				}
				if c.String("config") != "test.yaml" {
					t.Errorf("config = %q, want %q", c.String("config"), "test.yaml")
				}
				return nil
			},
		},
		{
			name: "profile delete requires name",
			args: []string{"app", "profile", "delete", "--name", "todelete"},
			validate: func(c *cli.Context) error {
				if c.String("name") != "todelete" {
					t.Errorf("name = %q, want %q", c.String("name"), "todelete")
				}
				return nil
			},
		},
		{
			name: "profile export with output",
			args: []string{"app", "profile", "export", "--name", "exportme", "--out", "/tmp/exported.yaml"},
			validate: func(c *cli.Context) error {
				if c.String("name") != "exportme" {
					t.Errorf("name = %q, want %q", c.String("name"), "exportme")
				}
				if c.String("out") != "/tmp/exported.yaml" {
					t.Errorf("out = %q, want %q", c.String("out"), "/tmp/exported.yaml")
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &cli.App{
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "config", Aliases: []string{"c"}, Value: "config.yaml"},
				},
				Commands: []*cli.Command{
					{
						Name: "profile",
						Subcommands: []*cli.Command{
							{
								Name:   "save",
								Action: tt.validate,
								Flags: []cli.Flag{
									&cli.StringFlag{Name: "name", Aliases: []string{"n"}},
								},
							},
							{
								Name:   "list",
								Action: tt.validate,
							},
							{
								Name:   "delete",
								Action: tt.validate,
								Flags: []cli.Flag{
									&cli.StringFlag{Name: "name", Aliases: []string{"n"}, Required: true},
								},
							},
							{
								Name:   "export",
								Action: tt.validate,
								Flags: []cli.Flag{
									&cli.StringFlag{Name: "name", Aliases: []string{"n"}, Required: true},
									&cli.StringFlag{Name: "out", Aliases: []string{"o"}, Value: "config.yaml"},
								},
							},
						},
					},
				},
			}

			if err := app.Run(tt.args); err != nil {
				t.Fatalf("app.Run() error: %v", err)
			}
		})
	}
}

// TestExitErrHandler_PropagatesPartialMigrationError guards the #248 fix:
// orchestrator.PartialMigrationError implements ExitCode() int which would
// otherwise satisfy urfave/cli/v2.ExitCoder and be intercepted by cli's
// HandleExitCoder (calling os.Exit directly), bypassing the centralized
// "Error:/Exit code N (description)" stderr formatting in main.go. The
// ExitErrHandler override on cli.App must keep that interception disabled
// so the error propagates back through app.Run.
func TestExitErrHandler_PropagatesPartialMigrationError(t *testing.T) {
	want := &orchestrator.PartialMigrationError{
		Failed: []orchestrator.TableFailure{{TableName: "dbo.Users"}},
	}

	app := &cli.App{
		ExitErrHandler: func(*cli.Context, error) {},
		Commands: []*cli.Command{
			{
				Name:   "boom",
				Action: func(*cli.Context) error { return want },
			},
		},
	}

	err := app.Run([]string{"dmt", "boom"})
	if err == nil {
		t.Fatal("app.Run returned nil; PartialMigrationError was intercepted by cli (override broken)")
	}
	var got *orchestrator.PartialMigrationError
	if !errors.As(err, &got) {
		t.Fatalf("app.Run returned %T (%v), want *orchestrator.PartialMigrationError", err, err)
	}
	if got.ExitCode() != 3 {
		t.Errorf("ExitCode() = %d, want 3", got.ExitCode())
	}
}
