package webui

import (
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/progress"
)

// eventType tags the kind of server-sent event pushed to browser clients.
type eventType string

const (
	eventProgress  eventType = "progress"
	eventStarted   eventType = "started"
	eventDone      eventType = "done"
	eventFailed    eventType = "failed"
	eventCancelled eventType = "cancelled"
)

// event is one server-sent event. Exactly one payload field is populated per
// event; the SSE `event:` line carries Type and the `data:` line the JSON.
type event struct {
	Type     eventType                `json:"type"`
	Time     string                   `json:"time"`
	Progress *progress.ProgressUpdate `json:"progress,omitempty"`
	Run      *runState                `json:"run,omitempty"`
}

// eventHub fans migration progress and lifecycle events out to every connected
// SSE client. It implements progress.Reporter so the orchestrator can push
// straight into it via SetProgressReporter. It is a long-lived, per-server
// object: it outlives any single run (Close is a no-op — see below) so a
// browser stays subscribed across multiple migrations.
type eventHub struct {
	mu     sync.Mutex
	subs   map[int]chan event
	nextID int
	last   *event // most recent progress event, replayed to late joiners
	live   bool   // true only while a migration is running; gates progress relay

	// everSubscribed latches true the moment the first SSE client ever
	// connects. It exists for the --gui idle-shutdown watchdog
	// (idle_watchdog.go): the watchdog polls on a timer, so sampling
	// subscriberCount() on each tick can miss a subscriber that connects and
	// disconnects again inside a single poll interval — a real risk in
	// practice, since a --gui browser window's SSE connection typically opens
	// well within the multi-second poll period. Setting this flag inside
	// subscribe() itself, synchronously at connection time, makes "has a
	// browser ever shown up" an event the watchdog can't miss regardless of
	// its poll cadence.
	everSubscribed bool
}

func newEventHub() *eventHub {
	return &eventHub{subs: make(map[int]chan event)}
}

// Report / ReportImmediate satisfy progress.Reporter. Both publish a progress
// event; the hub does not throttle (the orchestrator's tracker already emits on
// an interval, and the browser can absorb the rest).
func (h *eventHub) Report(u progress.ProgressUpdate) {
	h.publish(event{Type: eventProgress, Progress: &u})
}
func (h *eventHub) ReportImmediate(u progress.ProgressUpdate) {
	h.publish(event{Type: eventProgress, Progress: &u})
}

// Close is intentionally a no-op. The orchestrator's tracker calls Close() on
// its reporter at the end of every run, but this hub is shared across runs and
// SSE subscribers, so it must NOT tear down subscriber channels here. Those are
// closed per-connection in unsubscribe.
func (h *eventHub) Close() {}

// setLive marks whether a migration is currently running. Progress events are
// only relayed while live, so a late-stopping tracker (or any other stray
// emission) can't post-date the terminal done/failed/cancelled event. The
// retained snapshot is cleared when a run ends so a client connecting between
// runs relies on the run-state event, not a stale progress frame.
func (h *eventHub) setLive(v bool) {
	h.mu.Lock()
	h.live = v
	if !v {
		h.last = nil
	}
	h.mu.Unlock()
}

// publish delivers e to every subscriber. Delivery is non-blocking: a
// subscriber whose buffer is full drops the event (drop-slowest backpressure).
// This is safe for progress (the next update supersedes it) and for lifecycle
// events the client can re-derive authoritatively via GET /api/run.
func (h *eventHub) publish(e event) {
	if e.Time == "" {
		e.Time = time.Now().UTC().Format(time.RFC3339)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if e.Type == eventProgress {
		// Drop progress emitted outside an active run (e.g. a tracker tick
		// racing the run's end). Lifecycle events always pass.
		if !h.live {
			return
		}
		snap := e
		h.last = &snap
	}
	for _, ch := range h.subs {
		select {
		case ch <- e:
		default:
		}
	}
}

// subscribe registers a new SSE client and returns its id + receive channel.
func (h *eventHub) subscribe() (int, <-chan event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := h.nextID
	h.nextID++
	ch := make(chan event, 64)
	h.subs[id] = ch
	h.everSubscribed = true
	return id, ch
}

// unsubscribe removes and closes a client's channel. publish and unsubscribe
// are mutually excluded by h.mu, so a send can never race a close.
func (h *eventHub) unsubscribe(id int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ch, ok := h.subs[id]; ok {
		delete(h.subs, id)
		close(ch)
	}
}

// snapshot returns the last progress event, or nil if none has been published.
func (h *eventHub) snapshot() *event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.last
}

// subscriberCount reports the number of connected SSE clients. Used by the
// --gui idle-shutdown watchdog (idle_watchdog.go) as a proxy for "is a browser
// window still open" — each open tab/window holds exactly one long-lived
// EventSource connection for as long as it is mounted.
func (h *eventHub) subscriberCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// hasEverSubscribed reports whether any SSE client has connected since the
// hub was created. See the everSubscribed field comment for why the watchdog
// needs this instead of polling subscriberCount() to decide when to arm.
func (h *eventHub) hasEverSubscribed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.everSubscribed
}
