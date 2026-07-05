package checkpoint

import "testing"

// seedRuns creates n runs with the given status, most-recent last (CreateRun
// stamps started_at = now, and GetRunsPage orders by started_at DESC, rowid
// DESC, so later inserts sort first).
func seedRuns(t *testing.T, s *State, status string, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := s.CreateRun(id, "dbo", "public", map[string]string{"run": id}, "", ""); err != nil {
			t.Fatalf("CreateRun(%s): %v", id, err)
		}
		if status != "running" {
			if err := s.CompleteRun(id, status, ""); err != nil {
				t.Fatalf("CompleteRun(%s): %v", id, err)
			}
		}
	}
}

func TestGetRunsPagePagination(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	defer s.Close()

	seedRuns(t, s, "success", "r1", "r2", "r3", "r4", "r5")

	page1, total, err := s.GetRunsPage("", 2, 0)
	if err != nil {
		t.Fatalf("GetRunsPage page1: %v", err)
	}
	if total != 5 {
		t.Errorf("total = %d, want 5", total)
	}
	if len(page1) != 2 {
		t.Fatalf("page1 len = %d, want 2", len(page1))
	}
	// Most-recent first: r5 was inserted last.
	if page1[0].ID != "r5" {
		t.Errorf("page1[0] = %s, want r5", page1[0].ID)
	}

	page3, total, err := s.GetRunsPage("", 2, 4)
	if err != nil {
		t.Fatalf("GetRunsPage page3: %v", err)
	}
	if total != 5 {
		t.Errorf("total = %d, want 5", total)
	}
	if len(page3) != 1 {
		t.Fatalf("page3 len = %d, want 1 (tail page)", len(page3))
	}

	// Offset past the end returns an empty page but the true total.
	empty, total, err := s.GetRunsPage("", 2, 99)
	if err != nil {
		t.Fatalf("GetRunsPage past-end: %v", err)
	}
	if len(empty) != 0 || total != 5 {
		t.Errorf("past-end: len=%d total=%d, want 0/5", len(empty), total)
	}
}

func TestGetRunsPageStatusFilter(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	defer s.Close()

	seedRuns(t, s, "success", "ok1", "ok2", "ok3")
	seedRuns(t, s, "failed", "bad1", "bad2")

	failed, total, err := s.GetRunsPage("failed", 20, 0)
	if err != nil {
		t.Fatalf("GetRunsPage(failed): %v", err)
	}
	if total != 2 {
		t.Errorf("failed total = %d, want 2", total)
	}
	if len(failed) != 2 {
		t.Fatalf("failed len = %d, want 2", len(failed))
	}
	for _, r := range failed {
		if r.Status != "failed" {
			t.Errorf("status = %s, want failed", r.Status)
		}
	}

	_, total, err = s.GetRunsPage("success", 20, 0)
	if err != nil {
		t.Fatalf("GetRunsPage(success): %v", err)
	}
	if total != 3 {
		t.Errorf("success total = %d, want 3", total)
	}
}
