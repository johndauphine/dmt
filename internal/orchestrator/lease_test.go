package orchestrator

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
)

func TestResumeRejectsFreshLiveLeaseOwner(t *testing.T) {
	for _, backendName := range []string{"sqlite", "file"} {
		t.Run(backendName, func(t *testing.T) {
			first, second := openOrchestratorLeaseBackends(t, backendName)
			cfg := orchestratorLeaseTestConfig()
			owner := &Orchestrator{config: cfg, state: first}
			leaseBackend, lease, err := owner.acquireMigrationLease(nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := first.CreateRun("run-live", "source", "public", nil, "", ""); err != nil {
				t.Fatal(err)
			}
			if err := bindMigrationLease(leaseBackend, "run-live", lease); err != nil {
				t.Fatal(err)
			}

			contender := &Orchestrator{config: cfg, state: second}
			err = contender.Resume(context.Background())
			if !checkpoint.IsLeaseHeldError(err) {
				t.Fatalf("Resume() error = %v, want LeaseHeldError", err)
			}
		})
	}
}

func TestResumeRejectsFreshLegacyOwnerEvenWithForce(t *testing.T) {
	for _, backendName := range []string{"sqlite", "file"} {
		t.Run(backendName, func(t *testing.T) {
			first, _ := openOrchestratorLeaseBackends(t, backendName)
			if err := first.CreateRun("legacy-live", "source", "public", nil, "", ""); err != nil {
				t.Fatal(err)
			}
			o := &Orchestrator{
				config: orchestratorLeaseTestConfig(),
				state:  first,
				opts: Options{
					ForceResume:     true,
					RunHeartbeatTTL: time.Minute,
				},
			}
			err := o.Resume(context.Background())
			if err == nil || !strings.Contains(err.Error(), "fresh heartbeat") {
				t.Fatalf("Resume() error = %v, want fresh legacy owner rejection", err)
			}
		})
	}
}

func TestMigrationLeaseSessionCancelsRunWhenFenceIsLost(t *testing.T) {
	for _, backendName := range []string{"sqlite", "file"} {
		t.Run(backendName, func(t *testing.T) {
			first, second := openOrchestratorLeaseBackends(t, backendName)
			cfg := orchestratorLeaseTestConfig()
			o := &Orchestrator{
				config: cfg,
				state:  first,
				opts: Options{
					RunHeartbeatTTL:      time.Minute,
					RunHeartbeatInterval: 5 * time.Millisecond,
				},
			}
			leaseBackend, leaseA, err := o.acquireMigrationLease(nil)
			if err != nil {
				t.Fatalf("acquire first owner: %v", err)
			}
			if err := first.CreateRun("run-lease-loss", "source", "target", nil, "", ""); err != nil {
				t.Fatalf("create run: %v", err)
			}
			if err := bindMigrationLease(leaseBackend, "run-lease-loss", leaseA); err != nil {
				t.Fatalf("bind first owner: %v", err)
			}
			ownedCtx, session, err := o.startMigrationLease(context.Background(), leaseBackend, leaseA, "run-lease-loss")
			if err != nil {
				t.Fatalf("start lease renewal: %v", err)
			}

			secondLeaseBackend := second.(checkpoint.MigrationLeaseBackend)
			leaseB, err := secondLeaseBackend.AcquireMigrationLease(
				o.migrationTarget(), "replacement-owner", time.Now().UTC().Add(2*time.Minute), time.Minute,
			)
			if err != nil {
				t.Fatalf("take over stale lease: %v", err)
			}
			defer secondLeaseBackend.ReleaseMigrationLease(leaseB)

			select {
			case <-ownedCtx.Done():
				if cause := context.Cause(ownedCtx); !checkpoint.IsLeaseLostError(cause) {
					t.Fatalf("lease cancellation cause = %v, want LeaseLostError", cause)
				}
			case <-time.After(time.Second):
				t.Fatal("run context was not canceled after lease takeover")
			}
			if err := session.Close(); !checkpoint.IsLeaseLostError(err) {
				t.Fatalf("lease session close error = %v, want LeaseLostError", err)
			}
		})
	}
}

func orchestratorLeaseTestConfig() *config.Config {
	return &config.Config{Target: config.TargetConfig{
		Type: "postgres", Host: "DB.EXAMPLE", Port: 5432,
		Database: "warehouse", Schema: "public",
	}}
}

func openOrchestratorLeaseBackends(t *testing.T, backendName string) (checkpoint.StateBackend, checkpoint.StateBackend) {
	t.Helper()
	var first, second checkpoint.StateBackend
	var err error
	switch backendName {
	case "sqlite":
		root := t.TempDir()
		first, err = checkpoint.New(root)
		if err == nil {
			second, err = checkpoint.New(root)
		}
	case "file":
		path := filepath.Join(t.TempDir(), "state.yaml")
		first, err = checkpoint.NewFileState(path)
		if err == nil {
			second, err = checkpoint.NewFileState(path)
		}
	default:
		t.Fatalf("unknown backend %q", backendName)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = first.Close()
		_ = second.Close()
	})
	return first, second
}
