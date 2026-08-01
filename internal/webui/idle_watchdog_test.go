package webui

import (
	"context"
	"testing"
	"time"
)

// Fast intervals so these tests run in milliseconds without being flaky.
const (
	testIdlePoll  = 2 * time.Millisecond
	testIdleGrace = 12 * time.Millisecond
)

func waitWatchdog(t *testing.T, hub *eventHub, runs *runManager) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		runIdleWatchdog(context.Background(), hub, runs, testIdlePoll, testIdleGrace)
		close(done)
	}()
	return done
}

func TestIdleWatchdogNeverFiresBeforeFirstSubscriber(t *testing.T) {
	hub := newEventHub()
	runs := newRunManager()
	done := waitWatchdog(t, hub, runs)

	select {
	case <-done:
		t.Fatal("watchdog fired with no browser ever connected — a failed launch must leave the server running")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestIdleWatchdogFiresAfterWindowClosesAndGraceElapses(t *testing.T) {
	hub := newEventHub()
	runs := newRunManager()

	id, _ := hub.subscribe()
	done := waitWatchdog(t, hub, runs)
	hub.unsubscribe(id)

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("watchdog did not fire after the only subscriber disconnected and idled out")
	}
}

// Regression test for a real race a review caught: arming used to be sampled
// from subscriberCount() on ticker ticks, so a subscriber that connected and
// disconnected again inside a single poll interval could be invisible to
// every tick that ever ran — the watchdog would then never arm, and --gui
// would never idle-exit for that run. subscribe()/unsubscribe() here happen
// with no synchronization delay at all, faster than any poll tick could
// possibly observe them, to prove arming no longer depends on tick timing.
func TestIdleWatchdogArmsOnFastConnectDisconnect(t *testing.T) {
	hub := newEventHub()
	runs := newRunManager()

	done := waitWatchdog(t, hub, runs)
	id, _ := hub.subscribe()
	hub.unsubscribe(id) // gone before the watchdog's goroutine could plausibly have ticked yet

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("watchdog never armed after a subscriber connected and disconnected within one poll interval")
	}
}

func TestIdleWatchdogSurvivesBriefZeroFromReload(t *testing.T) {
	hub := newEventHub()
	runs := newRunManager()

	id, _ := hub.subscribe()
	done := waitWatchdog(t, hub, runs)
	hub.unsubscribe(id)

	// Simulate a page reload: the new EventSource reconnects well within the
	// grace window, so the watchdog must not have fired.
	time.Sleep(testIdleGrace / 2)
	hub.subscribe()

	select {
	case <-done:
		t.Fatal("watchdog fired despite a reconnect inside the grace window")
	case <-time.After(3 * testIdleGrace):
	}
}

func TestIdleWatchdogWaitsForActiveMigration(t *testing.T) {
	hub := newEventHub()
	runs := newRunManager()
	runs.start(kindRun, "run-1", func() {})

	id, _ := hub.subscribe()
	done := waitWatchdog(t, hub, runs)
	hub.unsubscribe(id)

	select {
	case <-done:
		t.Fatal("watchdog fired while a migration was still running")
	case <-time.After(5 * testIdleGrace):
	}

	// Once the run finishes, the same idle condition should be free to fire.
	runs.finish("completed", "")
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("watchdog did not fire after the migration finished")
	}
}

func TestIdleWatchdogRespectsContextCancellation(t *testing.T) {
	hub := newEventHub()
	runs := newRunManager()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runIdleWatchdog(ctx, hub, runs, testIdlePoll, testIdleGrace)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("watchdog did not exit promptly on context cancellation")
	}
}
