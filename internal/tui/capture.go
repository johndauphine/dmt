package tui

import (
	"fmt"
	"os"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/johndauphine/dmt/internal/logging"
)

// CaptureOutput pipes stdout and stderr to a channel that feeds the TUI
func CaptureOutput(p *tea.Program, _ string) func() {
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

// CaptureToString captures stdout from a function and returns it as a string.
// Used for commands like /status and /history that print to stdout.
func CaptureToString(fn func() error) (string, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return "", fmt.Errorf("creating pipe: %w", err)
	}

	origStdout := os.Stdout
	os.Stdout = w

	// Run the function
	fnErr := fn()

	// Restore stdout and close writer
	w.Close()
	os.Stdout = origStdout

	// Read captured output
	var buf []byte
	readBuf := make([]byte, 1024)
	for {
		n, readErr := r.Read(readBuf)
		if n > 0 {
			buf = append(buf, readBuf[:n]...)
		}
		if readErr != nil {
			break
		}
	}
	r.Close()

	return string(buf), fnErr
}
