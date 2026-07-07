package main

import (
	"testing"

	"github.com/urfave/cli/v2"
)

// findCommand returns the top-level command with the given name, or nil.
func findCommand(app *cli.App, name string) *cli.Command {
	for _, c := range app.Commands {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// commandDefinesFlag reports whether cmd defines a flag with the given name
// (checking every alias each flag registers).
func commandDefinesFlag(cmd *cli.Command, flag string) bool {
	for _, f := range cmd.Flags {
		for _, n := range f.Names() {
			if n == flag {
				return true
			}
		}
	}
	return false
}

// TestBackupAckFlagAsymmetry guards the #623 regression: the backup-ack
// preflight remedy names --confirm-backup, a flag that only `run` defines.
// `resume` must never surface that remedy — the fix skips the gate on resume
// (see internal/driver/shared.BackupAcknowledgmentRequired) — so `resume`
// intentionally does NOT define --confirm-backup. Its escape hatch is
// --skip-preflight. If a future change points a resume-reachable remedy at a
// flag `resume` lacks, this asymmetry is where it starts; pin it.
func TestBackupAckFlagAsymmetry(t *testing.T) {
	app := newApp()

	run := findCommand(app, "run")
	if run == nil {
		t.Fatal("run command not found")
	}
	if !commandDefinesFlag(run, "confirm-backup") {
		t.Error("run must define --confirm-backup (the backup-ack remedy names it)")
	}

	resume := findCommand(app, "resume")
	if resume == nil {
		t.Fatal("resume command not found")
	}
	if commandDefinesFlag(resume, "confirm-backup") {
		t.Error("resume must NOT define --confirm-backup: resume skips the backup-ack gate (#623), " +
			"so exposing the flag would be dead UX. If you add it, update the resume remedy path too.")
	}
	if !commandDefinesFlag(resume, "skip-preflight") {
		t.Error("resume must define --skip-preflight: it is the documented escape hatch for preflight gates on resume")
	}
}
