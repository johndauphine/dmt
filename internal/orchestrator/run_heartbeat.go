package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/logging"
)

const (
	defaultRunHeartbeatInterval = 30 * time.Second
	defaultRunHeartbeatTTL      = 15 * time.Minute
)

func (o *Orchestrator) runHeartbeatInterval() time.Duration {
	if o.opts.RunHeartbeatInterval > 0 {
		return o.opts.RunHeartbeatInterval
	}
	return defaultRunHeartbeatInterval
}

func (o *Orchestrator) runHeartbeatTTL() time.Duration {
	if o.opts.RunHeartbeatTTL > 0 {
		return o.opts.RunHeartbeatTTL
	}
	return defaultRunHeartbeatTTL
}

func (o *Orchestrator) startRunHeartbeat(ctx context.Context, runID string) func() {
	if o.state == nil {
		return func() {}
	}

	heartbeatCtx, cancel := context.WithCancel(ctx)
	interval := o.runHeartbeatInterval()
	update := func() {
		if err := o.state.UpdateRunHeartbeat(runID, time.Now().UTC()); err != nil {
			logging.Warn("failed to update run heartbeat for %s: %v", runID, err)
		}
	}

	update()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				update()
			case <-heartbeatCtx.Done():
				return
			}
		}
	}()

	return cancel
}

func (o *Orchestrator) validateResumeHeartbeat(run *checkpoint.Run, now time.Time) error {
	if run == nil || o.opts.ForceResume {
		return nil
	}

	lastHeartbeat := run.LastHeartbeat
	if lastHeartbeat.IsZero() {
		lastHeartbeat = run.StartedAt
	}
	ttl := o.runHeartbeatTTL()
	age := now.Sub(lastHeartbeat)
	if age <= ttl {
		return nil
	}

	return fmt.Errorf("incomplete run %s has a stale heartbeat: last heartbeat %s (%s ago, TTL %s). Verify no migration process is still running, then use --force-resume to override",
		run.ID,
		lastHeartbeat.UTC().Format(time.RFC3339),
		age.Round(time.Second),
		ttl.Round(time.Second))
}
