package tui

import "testing"

// TestRunConfirmBackupFlag pins #568: the TUI /run must actually parse
// --confirm-backup (the registry now claims it as a TUI surface, so it must
// exist) and set the override that acknowledges the drop_recreate backup gate.
func TestRunConfirmBackupFlag(t *testing.T) {
	m := &Model{}

	_, _, ov, err := m.parseRunArgs([]string{"/run", "--confirm-backup", "config.yaml"})
	if err != nil {
		t.Fatalf("/run --confirm-backup should parse without error, got: %v", err)
	}
	if !ov.confirmBackup {
		t.Fatal("/run --confirm-backup must set ov.confirmBackup")
	}

	// Without the flag it stays false (no accidental default acknowledgement).
	_, _, ov2, err := m.parseRunArgs([]string{"/run", "config.yaml"})
	if err != nil {
		t.Fatalf("/run should parse: %v", err)
	}
	if ov2.confirmBackup {
		t.Fatal("/run without --confirm-backup must leave ov.confirmBackup false")
	}
}
