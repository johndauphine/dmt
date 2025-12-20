package tui

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// OutputMsg is sent when new stdout/stderr output is captured
type OutputMsg string

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

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			text := scanner.Text()
			p.Send(OutputMsg(text))
		}
	}()

	return func() {
		w.Close()
		os.Stdout = origStdout
		os.Stderr = origStderr
		// We don't wait for wg because scanner.Scan blocks until EOF
		// and we want to restore immediately.
		// However, to ensure last bytes are read, we could wait a tiny bit.
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
