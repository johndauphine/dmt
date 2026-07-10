package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/exitcodes"
)

// migrationLeaseSession renews the durable target lease and cancels the run
// context if ownership is lost. The checkpoint backend independently verifies
// the same fencing generation on every run/task/progress mutation.
type migrationLeaseSession struct {
	backend checkpoint.MigrationLeaseBackend
	state   checkpoint.StateBackend
	runID   string
	ttl     time.Duration

	mu    sync.Mutex
	lease checkpoint.MigrationLease
	err   error

	cancel context.CancelCauseFunc
	done   chan struct{}
	once   sync.Once
}

func (o *Orchestrator) migrationTarget() checkpoint.MigrationTarget {
	driverName := o.config.Target.Type
	if o.targetPool != nil {
		driverName = o.targetPool.DBType()
	}
	return checkpoint.MigrationTarget{
		Driver:   driverName,
		Host:     o.config.Target.Host,
		Port:     o.config.Target.Port,
		Database: o.config.Target.Database,
		Schema:   o.config.Target.Schema,
	}.Canonical()
}

func (o *Orchestrator) migrationLeaseBackend() (checkpoint.MigrationLeaseBackend, error) {
	backend, ok := o.state.(checkpoint.MigrationLeaseBackend)
	if !ok {
		return nil, exitcodes.NewExitError(
			fmt.Errorf("state backend %T does not support exclusive migration leases", o.state),
			exitcodes.StateError,
		)
	}
	return backend, nil
}

func (o *Orchestrator) acquireMigrationLease(run *checkpoint.Run) (checkpoint.MigrationLeaseBackend, checkpoint.MigrationLease, error) {
	backend, err := o.migrationLeaseBackend()
	if err != nil {
		return nil, checkpoint.MigrationLease{}, err
	}
	target := o.migrationTarget()
	if run != nil && run.LeaseTargetKey != "" && run.LeaseTargetKey != target.Key() {
		return nil, checkpoint.MigrationLease{}, exitcodes.NewExitError(
			fmt.Errorf(
				"run %s is fenced to target %s, but the current configuration identifies %s",
				run.ID, run.LeaseTargetKey, target.Key(),
			),
			exitcodes.StateError,
		)
	}
	lease, err := backend.AcquireMigrationLease(target, uuid.NewString(), time.Now().UTC(), o.runHeartbeatTTL())
	if err != nil {
		return nil, checkpoint.MigrationLease{}, err
	}
	return backend, lease, nil
}

func bindMigrationLease(backend checkpoint.MigrationLeaseBackend, runID string, lease checkpoint.MigrationLease) error {
	if err := backend.BindRunLease(runID, lease); err != nil {
		return checkpoint.RequiredWrite(fmt.Sprintf("binding run %s to migration lease", runID), err)
	}
	return nil
}

func releaseUnboundMigrationLease(backend checkpoint.MigrationLeaseBackend, lease checkpoint.MigrationLease, cause error) error {
	releaseErr := backend.ReleaseMigrationLease(lease)
	if releaseErr == nil {
		return cause
	}
	return errors.Join(cause, checkpoint.RequiredWrite("releasing migration lease", releaseErr))
}

func (o *Orchestrator) startMigrationLease(
	ctx context.Context,
	backend checkpoint.MigrationLeaseBackend,
	lease checkpoint.MigrationLease,
	runID string,
) (context.Context, *migrationLeaseSession, error) {
	ownedCtx, cancel := context.WithCancelCause(ctx)
	session := &migrationLeaseSession{
		backend: backend,
		state:   o.state,
		runID:   runID,
		ttl:     o.runHeartbeatTTL(),
		lease:   lease,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	if err := o.state.UpdateRunHeartbeat(runID, time.Now().UTC()); err != nil {
		cancel(err)
		close(session.done)
		return ownedCtx, session, checkpoint.RequiredWrite(fmt.Sprintf("updating run %s ownership heartbeat", runID), err)
	}

	interval := o.runHeartbeatInterval()
	renewalLimit := session.ttl / 3
	if renewalLimit <= 0 {
		renewalLimit = session.ttl
	}
	if interval > renewalLimit {
		interval = renewalLimit
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	go session.renewLoop(ownedCtx, interval)
	return ownedCtx, session, nil
}

func (s *migrationLeaseSession) renewLoop(ctx context.Context, interval time.Duration) {
	defer close(s.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.renew(); err != nil {
				s.fail(err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *migrationLeaseSession) renew() error {
	s.mu.Lock()
	lease := s.lease
	s.mu.Unlock()

	renewed, err := s.backend.RenewMigrationLease(lease, time.Now().UTC(), s.ttl)
	if err != nil {
		return checkpoint.RequiredWrite(fmt.Sprintf("renewing migration lease for run %s", s.runID), err)
	}
	s.mu.Lock()
	s.lease = renewed
	s.mu.Unlock()
	if err := s.state.UpdateRunHeartbeat(s.runID, time.Now().UTC()); err != nil {
		return checkpoint.RequiredWrite(fmt.Sprintf("updating run %s ownership heartbeat", s.runID), err)
	}
	return nil
}

func (s *migrationLeaseSession) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
	s.cancel(err)
}

func (s *migrationLeaseSession) Close() error {
	s.once.Do(func() {
		s.cancel(nil)
		<-s.done
		s.mu.Lock()
		lease := s.lease
		s.mu.Unlock()
		if err := s.backend.ReleaseMigrationLease(lease); err != nil {
			s.fail(checkpoint.RequiredWrite("releasing migration lease", err))
		}
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func mergeLeaseSessionError(runErr error, session *migrationLeaseSession) error {
	leaseErr := session.Close()
	if leaseErr == nil {
		return runErr
	}
	if runErr == nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
		return leaseErr
	}
	return errors.Join(runErr, leaseErr)
}
