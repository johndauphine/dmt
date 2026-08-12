package tui

import (
	"fmt"
	"os"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/johndauphine/dmt/v5/internal/logging"
)

// CaptureOutput pipes stdout and stderr to a channel that feeds the TUI
func CaptureOutput(p *tea.Program) func() {
	r, w, err := os.Pipe()
	if err != nil {
		return func() {}
	}

	origStdout := os.Stdout
	origStderr := os.Stderr

	os.Stdout = w
	os.Stderr = w

	// Redirect logging to the pipe and enable simple mode (no timestamps in TUI)
	logging.SetOutput(w)
	logging.SetSimpleMode(true)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		buf := make([]byte, 1024)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				p.Send(OutputMsg(string(buf[:n])))
			}
			if err != nil {
				break
			}
		}
	}()

	return func() {
		w.Close()
		os.Stdout = origStdout
		os.Stderr = origStderr
		// Restore logging to stdout and disable simple mode
		logging.SetOutput(origStdout)
		logging.SetSimpleMode(false)
		// Wait a tiny bit to ensure last bytes are read
		time.Sleep(10 * time.Millisecond)
	}
}

// WriterAdapter implements io.Writer and sends messages to the program
type WriterAdapter struct {
	Program *tea.Program
}

func (w *WriterAdapter) Write(p []byte) (n int, err error) {
	if w.Program != nil {
		w.Program.Send(OutputMsg(string(p)))
	} else {
		fmt.Print(string(p))
	}
	return len(p), nil
}

// programRef holds a reference to the tea.Program for use by migration commands
var programRef *tea.Program

// SetProgramRef stores the program reference for migration commands
func SetProgramRef(p *tea.Program) {
	programRef = p
}

// GetProgramRef returns the stored program reference
func GetProgramRef() *tea.Program {
	return programRef
}

// stdoutMu serializes every global os.Stdout/os.Stderr redirect in the TUI —
// the migration's (commands_run.go) and each command capture's — so the two
// never race on the process-global or restore each other's now-closed pipe.
// The base session redirect (CaptureOutput) is installed once at startup before
// any of this runs and is not part of the mutual exclusion (#556).
var stdoutMu sync.Mutex

// CaptureToString captures stdout from a function and returns it as a string.
// Used for commands like /status and /history that print to stdout.
func CaptureToString(fn func() error) (string, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return "", fmt.Errorf("creating pipe: %w", err)
	}

	// Drain the read end concurrently while fn writes. Output larger than the
	// OS pipe buffer (~64KB; /status --detailed on 700+ tables exceeds it) would
	// otherwise block the writer forever with no reader — hanging the command
	// goroutine permanently, so its `defer orch.Close()` never runs and the
	// orchestrator's DB + checkpoint SQLite handles leak for the TUI's life
	// (#556).
	captured := make(chan []byte, 1)
	go func() {
		var buf []byte
		readBuf := make([]byte, 4096)
		for {
			n, readErr := r.Read(readBuf)
			if n > 0 {
				buf = append(buf, readBuf[:n]...)
			}
			if readErr != nil {
				break
			}
		}
		captured <- buf
	}()

	// Redirect under stdoutMu so this capture is mutually exclusive with the
	// migration's redirect: origStdout is always the stable base, never a peer's
	// transient pipe, so the restore can't clobber it. Every cleanup step is
	// deferred and a panicking fn is recovered, so a panic never leaves the
	// mutex wedged or os.Stdout pointing at this closed pipe (#556).
	fnErr := func() (err error) {
		stdoutMu.Lock()
		defer stdoutMu.Unlock()
		origStdout := os.Stdout
		os.Stdout = w
		defer func() {
			os.Stdout = origStdout
			w.Close()
			if r := recover(); r != nil {
				err = fmt.Errorf("panic while capturing output: %v", r)
			}
		}()
		return fn()
	}()

	out := <-captured
	r.Close()

	return string(out), fnErr
}
