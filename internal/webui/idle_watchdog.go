package webui

import (
	"context"
	"time"
)

// idleWatchdogPoll is how often --gui checks whether it should exit.
const idleWatchdogPoll = 2 * time.Second

// idleWatchdogGrace is how long the subscriber count must stay at zero before
// exiting. A page reload or window refresh briefly drops the count to zero as
// the old EventSource closes and the new one connects, so an immediate exit
// on the first zero reading would kill the server out from under a refresh.
const idleWatchdogGrace = 10 * time.Second

// runIdleWatchdog blocks until --gui should exit for inactivity, or ctx is
// done (server already shutting down for another reason).
//
// It arms only after the first SSE subscriber ever connects: until then it
// stays inert, so a browser that never launched (headless box, no display,
// launch failure) leaves the server running exactly like plain --webui — the
// same degrade-gracefully rule the browser launcher itself follows. Once
// armed, it exits only when the subscriber count has been zero for at least
// grace AND no migration is in flight — an active run always outlives the
// window that started it.
//
// Arming is read from hub.hasEverSubscribed(), a flag latched synchronously
// inside subscribe() — not sampled from subscriberCount() on each tick. A
// tick-sampled "armed" would race a subscriber that connects and disconnects
// within a single poll interval (routine for a --gui browser window, whose
// SSE connects almost immediately, well inside the multi-second poll period):
// the count could read zero on every tick that ever observes it, so the
// watchdog would never arm and idle-exit would never trigger for that run.
func runIdleWatchdog(ctx context.Context, hub *eventHub, runs *runManager, poll, grace time.Duration) {
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	var zeroSince time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if hub.subscriberCount() > 0 {
				zeroSince = time.Time{}
				continue
			}
			if !hub.hasEverSubscribed() {
				continue
			}
			if zeroSince.IsZero() {
				zeroSince = time.Now()
				continue
			}
			if time.Since(zeroSince) >= grace && !runs.isRunning() {
				return
			}
		}
	}
}
