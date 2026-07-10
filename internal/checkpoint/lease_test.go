package checkpoint

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type leaseBackendPair struct {
	first  StateBackend
	second StateBackend
}

func openLeaseBackendPairs(t *testing.T) map[string]leaseBackendPair {
	t.Helper()
	sqliteDir := filepath.Join(t.TempDir(), "sqlite")
	sqliteA, err := New(sqliteDir)
	if err != nil {
		t.Fatal(err)
	}
	sqliteB, err := New(sqliteDir)
	if err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(t.TempDir(), "state.yaml")
	fileA, err := NewFileState(filePath)
	if err != nil {
		t.Fatal(err)
	}
	fileB, err := NewFileState(filePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sqliteA.Close()
		_ = sqliteB.Close()
		_ = fileA.Close()
		_ = fileB.Close()
	})
	return map[string]leaseBackendPair{
		"sqlite": {first: sqliteA, second: sqliteB},
		"file":   {first: fileA, second: fileB},
	}
}

func TestProductionBackendsProvideMigrationLeases(t *testing.T) {
	tests := []struct {
		name    string
		factory func(t *testing.T) StateBackend
	}{
		{
			name: "sqlite",
			factory: func(t *testing.T) StateBackend {
				state, err := New(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				return state
			},
		},
		{
			name: "file",
			factory: func(t *testing.T) StateBackend {
				state, err := NewFileState(filepath.Join(t.TempDir(), "state.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				return state
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := tt.factory(t)
			defer backend.Close()
			if _, ok := backend.(MigrationLeaseBackend); !ok {
				t.Fatalf("%T does not implement MigrationLeaseBackend", backend)
			}
		})
	}
}

func TestMigrationTargetKeyIsStructuredAndCanonical(t *testing.T) {
	a := MigrationTarget{Driver: " POSTGRES ", Host: "DB.EXAMPLE", Port: 5432, Database: "a/b", Schema: "c"}
	b := MigrationTarget{Driver: "postgres", Host: "db.example", Port: 5432, Database: "a", Schema: "b/c"}
	if a.Key() == b.Key() {
		t.Fatalf("distinct target tuples collided: %q", a.Key())
	}
	canonical := MigrationTarget{Driver: "postgres", Host: "db.example", Port: 5432, Database: "a/b", Schema: "c"}
	if a.Key() != canonical.Key() {
		t.Fatalf("case-insensitive endpoint fields were not canonicalized: %q != %q", a.Key(), canonical.Key())
	}
	if got, want := (MigrationTarget{Driver: "sqlite", Database: "/tmp/target.db"}).Key(),
		(MigrationTarget{Driver: "sqlite", Database: "/tmp/target.db", Schema: "main"}).Key(); got != want {
		t.Fatalf("implicit SQLite main schema was not canonicalized: %q != %q", got, want)
	}
	if got, want := (MigrationTarget{Driver: "mysql", Host: "db", Database: "warehouse"}).Key(),
		(MigrationTarget{Driver: "mysql", Host: "db", Database: "warehouse", Schema: "warehouse"}).Key(); got != want {
		t.Fatalf("implicit MySQL database schema was not canonicalized: %q != %q", got, want)
	}
}

func TestMigrationLeaseConcurrentAcquireHasExactlyOneWinner(t *testing.T) {
	for name, pair := range openLeaseBackendPairs(t) {
		t.Run(name, func(t *testing.T) {
			first := pair.first.(MigrationLeaseBackend)
			second := pair.second.(MigrationLeaseBackend)
			start := make(chan struct{})
			errs := make(chan error, 2)
			var leases [2]MigrationLease
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				var err error
				leases[0], err = first.AcquireMigrationLease(testLeaseTarget(), "owner-a", testLeaseTime(), time.Minute)
				errs <- err
			}()
			go func() {
				defer wg.Done()
				<-start
				var err error
				leases[1], err = second.AcquireMigrationLease(testLeaseTarget(), "owner-b", testLeaseTime(), time.Minute)
				errs <- err
			}()
			close(start)
			wg.Wait()
			close(errs)

			winners, conflicts := 0, 0
			for err := range errs {
				switch {
				case err == nil:
					winners++
				case IsLeaseHeldError(err):
					conflicts++
				default:
					t.Fatalf("AcquireMigrationLease() unexpected error: %v", err)
				}
			}
			if winners != 1 || conflicts != 1 {
				t.Fatalf("acquire results: winners=%d conflicts=%d, want 1/1", winners, conflicts)
			}
			for i, lease := range leases {
				if lease.Generation > 0 {
					backend := []MigrationLeaseBackend{first, second}[i]
					if err := backend.ReleaseMigrationLease(lease); err != nil {
						t.Fatalf("release winner: %v", err)
					}
				}
			}
		})
	}
}

func TestMigrationLeaseStaleTakeoverIncrementsFenceAndRejectsFormerOwner(t *testing.T) {
	for name, pair := range openLeaseBackendPairs(t) {
		t.Run(name, func(t *testing.T) {
			first := pair.first.(MigrationLeaseBackend)
			second := pair.second.(MigrationLeaseBackend)
			now := testLeaseTime()
			leaseA, err := first.AcquireMigrationLease(testLeaseTarget(), "owner-a", now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			leaseB, err := second.AcquireMigrationLease(testLeaseTarget(), "owner-b", now.Add(2*time.Minute), time.Minute)
			if err != nil {
				t.Fatalf("stale takeover: %v", err)
			}
			if leaseB.Generation != leaseA.Generation+1 {
				t.Fatalf("takeover generation = %d, want %d", leaseB.Generation, leaseA.Generation+1)
			}
			if _, err := first.RenewMigrationLease(leaseA, now.Add(2*time.Minute), time.Minute); !IsLeaseLostError(err) {
				t.Fatalf("former owner renewal error = %v, want LeaseLostError", err)
			}
			if _, err := second.RenewMigrationLease(leaseB, now.Add(2*time.Minute+time.Second), time.Minute); err != nil {
				t.Fatalf("new owner renewal: %v", err)
			}
		})
	}
}

func TestMigrationLeaseStaleTakeoverRejectsFormerOwnerTaskAndProgressWrites(t *testing.T) {
	for name, pair := range openLeaseBackendPairs(t) {
		t.Run(name, func(t *testing.T) {
			first := pair.first.(MigrationLeaseBackend)
			second := pair.second.(MigrationLeaseBackend)
			now := time.Now().UTC()
			leaseA, err := first.AcquireMigrationLease(testLeaseTarget(), "owner-a", now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if err := pair.first.CreateRun("run-fenced", "source", "target", nil, "", ""); err != nil {
				t.Fatalf("create run: %v", err)
			}
			if err := first.BindRunLease("run-fenced", leaseA); err != nil {
				t.Fatalf("bind first owner: %v", err)
			}
			taskID, err := pair.first.CreateTask("run-fenced", "transfer", "task-a")
			if err != nil {
				t.Fatalf("create task: %v", err)
			}

			leaseB, err := second.AcquireMigrationLease(testLeaseTarget(), "owner-b", now.Add(2*time.Minute), time.Minute)
			if err != nil {
				t.Fatalf("stale takeover: %v", err)
			}
			if err := second.BindRunLease("run-fenced", leaseB); err != nil {
				t.Fatalf("bind new owner: %v", err)
			}

			if err := pair.first.UpdateRunHeartbeat("run-fenced", now.Add(2*time.Minute)); !IsLeaseLostError(err) {
				t.Fatalf("former owner run write error = %v, want LeaseLostError", err)
			}
			if err := pair.first.UpdateTaskStatus(taskID, "running", ""); !IsLeaseLostError(err) {
				t.Fatalf("former owner task write error = %v, want LeaseLostError", err)
			}
			if err := pair.first.SaveTransferProgress(taskID, "items", nil, 10, 10, 100, ""); !IsLeaseLostError(err) {
				t.Fatalf("former owner progress write error = %v, want LeaseLostError", err)
			}
			if err := pair.second.UpdateTaskStatus(taskID, "running", ""); err != nil {
				t.Fatalf("new owner task write: %v", err)
			}
			if err := pair.second.SaveTransferProgress(taskID, "items", nil, 10, 10, 100, ""); err != nil {
				t.Fatalf("new owner progress write: %v", err)
			}
		})
	}
}

func TestMigrationLeasesAllowDifferentTargetIdentities(t *testing.T) {
	for name, pair := range openLeaseBackendPairs(t) {
		t.Run(name, func(t *testing.T) {
			first := pair.first.(MigrationLeaseBackend)
			second := pair.second.(MigrationLeaseBackend)
			targetB := testLeaseTarget()
			targetB.Schema = "analytics"
			if _, err := first.AcquireMigrationLease(testLeaseTarget(), "owner-a", testLeaseTime(), time.Minute); err != nil {
				t.Fatalf("first target acquire: %v", err)
			}
			if _, err := second.AcquireMigrationLease(targetB, "owner-b", testLeaseTime(), time.Minute); err != nil {
				t.Fatalf("different target acquire: %v", err)
			}
		})
	}
}

func TestSQLiteSelectsIncompleteRunByCanonicalTarget(t *testing.T) {
	root := t.TempDir()
	first, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	now := time.Now().UTC()
	targetA := testLeaseTarget()
	targetB := testLeaseTarget()
	targetB.Schema = "analytics"
	leaseA, err := first.AcquireMigrationLease(targetA, "owner-a", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.CreateRun("run-a", "source", targetA.Schema, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := first.BindRunLease("run-a", leaseA); err != nil {
		t.Fatal(err)
	}
	leaseB, err := second.AcquireMigrationLease(targetB, "owner-b", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.CreateRun("run-b", "source", targetB.Schema, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := second.BindRunLease("run-b", leaseB); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		target MigrationTarget
		wantID string
	}{{targetA, "run-a"}, {targetB, "run-b"}} {
		run, err := first.GetLastIncompleteRunForTarget(tt.target)
		if err != nil {
			t.Fatal(err)
		}
		if run == nil || run.ID != tt.wantID {
			t.Fatalf("target %s selected run %#v, want %s", tt.target, run, tt.wantID)
		}
	}
}

func TestSuccessfulRunSupersedesOnlyTheSameCanonicalTarget(t *testing.T) {
	root := t.TempDir()
	currentState, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer currentState.Close()
	otherState, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer otherState.Close()

	now := time.Now().UTC()
	targetA := testLeaseTarget()
	targetB := targetA
	targetB.Database = "other"
	leaseA, err := currentState.AcquireMigrationLease(targetA, "owner-a", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := currentState.CreateRun("current", "source", targetA.Schema, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := currentState.BindRunLease("current", leaseA); err != nil {
		t.Fatal(err)
	}
	current, err := currentState.GetRunByID("current")
	if err != nil {
		t.Fatal(err)
	}

	leaseB, err := otherState.AcquireMigrationLease(targetB, "owner-b", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := otherState.CreateRun("other-success", "source", targetB.Schema, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := otherState.BindRunLease("other-success", leaseB); err != nil {
		t.Fatal(err)
	}
	if err := otherState.CompleteRun("other-success", "success", ""); err != nil {
		t.Fatal(err)
	}
	if err := otherState.ReleaseMigrationLease(leaseB); err != nil {
		t.Fatal(err)
	}
	superseded, err := currentState.HasSuccessfulRunAfter(current)
	if err != nil {
		t.Fatal(err)
	}
	if superseded {
		t.Fatal("success on a different canonical target superseded the current run")
	}

	if err := currentState.ReleaseMigrationLease(leaseA); err != nil {
		t.Fatal(err)
	}
	leaseA2, err := otherState.AcquireMigrationLease(targetA, "owner-a2", now.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := otherState.CreateRun("same-success", "source", targetA.Schema, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := otherState.BindRunLease("same-success", leaseA2); err != nil {
		t.Fatal(err)
	}
	if err := otherState.CompleteRun("same-success", "success", ""); err != nil {
		t.Fatal(err)
	}
	superseded, err = currentState.HasSuccessfulRunAfter(current)
	if err != nil {
		t.Fatal(err)
	}
	if !superseded {
		t.Fatal("later success on the same canonical target did not supersede the current run")
	}
}

func TestFileStateFiltersIncompleteRunByCanonicalTarget(t *testing.T) {
	state, err := NewFileState(filepath.Join(t.TempDir(), "state.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	now := time.Now().UTC()
	lease, err := state.AcquireMigrationLease(testLeaseTarget(), "owner", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.CreateRun("run-a", "source", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := state.BindRunLease("run-a", lease); err != nil {
		t.Fatal(err)
	}
	other := testLeaseTarget()
	other.Schema = "analytics"
	run, err := state.GetLastIncompleteRunForTarget(other)
	if err != nil {
		t.Fatal(err)
	}
	if run != nil {
		t.Fatalf("different target selected run %#v", run)
	}
}

func TestLegacySQLiteTargetWithImplicitMainSchemaRemainsResumable(t *testing.T) {
	tests := map[string]func(t *testing.T) StateBackend{
		"sqlite": func(t *testing.T) StateBackend {
			state, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return state
		},
		"file": func(t *testing.T) StateBackend {
			state, err := NewFileState(filepath.Join(t.TempDir(), "state.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			return state
		},
	}
	for name, factory := range tests {
		t.Run(name, func(t *testing.T) {
			state := factory(t)
			defer state.Close()
			if err := state.CreateRun("legacy-sqlite", "", "", nil, "", ""); err != nil {
				t.Fatal(err)
			}
			run, err := state.(MigrationLeaseBackend).GetLastIncompleteRunForTarget(MigrationTarget{
				Driver: "sqlite", Database: "/tmp/target.db",
			})
			if err != nil {
				t.Fatal(err)
			}
			if run == nil || run.ID != "legacy-sqlite" {
				t.Fatalf("implicit main target selected run %#v", run)
			}
		})
	}
}

func TestFileStateLeaseCreatesMissingParentDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "state.yaml")
	state, err := NewFileState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if _, err := state.AcquireMigrationLease(testLeaseTarget(), "owner", time.Now().UTC(), time.Minute); err != nil {
		t.Fatalf("acquire lease in missing parent: %v", err)
	}
}

func TestMigrationLeaseConcurrentAcquireAcrossProcesses(t *testing.T) {
	for _, backendName := range []string{"sqlite", "file"} {
		t.Run(backendName, func(t *testing.T) {
			root := t.TempDir()
			path := root
			if backendName == "file" {
				path = filepath.Join(root, "state.yaml")
			} else {
				state, err := New(path)
				if err != nil {
					t.Fatalf("initialize sqlite state: %v", err)
				}
				if err := state.Close(); err != nil {
					t.Fatalf("close sqlite state: %v", err)
				}
			}

			results := runLeaseAcquireProcesses(t, backendName, path)
			acquired, held := 0, 0
			for _, result := range results {
				fields := strings.Fields(result)
				for _, field := range fields {
					switch field {
					case "acquired":
						acquired++
					case "held":
						held++
					}
				}
				if strings.Contains(result, "lease-error:") {
					t.Fatalf("subprocess lease error: %s", result)
				}
			}
			if acquired != 1 || held != 1 {
				t.Fatalf("cross-process acquire results = %q; acquired=%d held=%d, want 1/1", results, acquired, held)
			}
		})
	}
}

func runLeaseAcquireProcesses(t *testing.T, backendName, path string) []string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	type process struct {
		cmd    *exec.Cmd
		stdin  io.WriteCloser
		output *bytes.Buffer
	}
	processes := make([]process, 2)
	for i := range processes {
		cmd := exec.Command(executable, "-test.run=^TestMigrationLeaseProcessHelper$", "-test.count=1")
		cmd.Env = append(os.Environ(),
			"DMT_LEASE_PROCESS_HELPER=1",
			"DMT_LEASE_BACKEND="+backendName,
			"DMT_LEASE_PATH="+path,
			fmt.Sprintf("DMT_LEASE_OWNER=owner-%d", i),
		)
		stdin, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		output := &bytes.Buffer{}
		cmd.Stdout = output
		cmd.Stderr = output
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		processes[i] = process{cmd: cmd, stdin: stdin, output: output}
	}
	for i := range processes {
		if _, err := io.WriteString(processes[i].stdin, "start\n"); err != nil {
			t.Fatal(err)
		}
		_ = processes[i].stdin.Close()
	}
	results := make([]string, len(processes))
	for i := range processes {
		if err := processes[i].cmd.Wait(); err != nil {
			t.Fatalf("lease helper failed: %v\n%s", err, processes[i].output.String())
		}
		results[i] = processes[i].output.String()
	}
	return results
}

func TestMigrationLeaseProcessHelper(t *testing.T) {
	if os.Getenv("DMT_LEASE_PROCESS_HELPER") != "1" {
		return
	}
	var backend StateBackend
	var err error
	switch os.Getenv("DMT_LEASE_BACKEND") {
	case "sqlite":
		backend, err = New(os.Getenv("DMT_LEASE_PATH"))
	case "file":
		backend, err = NewFileState(os.Getenv("DMT_LEASE_PATH"))
	default:
		err = fmt.Errorf("unknown backend")
	}
	if err != nil {
		fmt.Printf("lease-error:%v\n", err)
		return
	}
	defer backend.Close()
	var signal [1]byte
	if _, err := os.Stdin.Read(signal[:]); err != nil {
		fmt.Printf("lease-error:%v\n", err)
		return
	}
	_, err = backend.(MigrationLeaseBackend).AcquireMigrationLease(
		testLeaseTarget(), os.Getenv("DMT_LEASE_OWNER"), time.Now().UTC(), 5*time.Minute,
	)
	switch {
	case err == nil:
		fmt.Println("acquired")
	case IsLeaseHeldError(err):
		fmt.Println("held")
	default:
		fmt.Printf("lease-error:%v\n", err)
	}
}

func testLeaseTarget() MigrationTarget {
	return MigrationTarget{Driver: "postgres", Host: "db.example", Port: 5432, Database: "warehouse", Schema: "public"}
}

func testLeaseTime() time.Time {
	return time.Date(2026, 7, 9, 18, 0, 0, 0, time.UTC)
}
