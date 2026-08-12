package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/johndauphine/dmt/v5/internal/exitcodes"

	"github.com/urfave/cli/v2"
)

func setupSignalHandler(c *cli.Context, cancel context.CancelFunc, cleanup func()) {
	shutdownTimeout := c.Duration("shutdown-timeout")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Guard against concurrent cleanup from timer and second signal racing.
	// AfterFunc.Stop() doesn't wait for an in-flight callback, so both paths
	// could invoke forceCleanupAndExit simultaneously without this guard.
	var exitOnce sync.Once

	go func() {
		sig := <-sigCh
		sigName := "SIGINT"
		if sig == syscall.SIGTERM {
			sigName = "SIGTERM"
		}
		fmt.Fprintf(os.Stderr, "\nReceived %s. Shutting down gracefully (timeout: %s)...\n", sigName, shutdownTimeout)
		fmt.Fprintln(os.Stderr, "Saving checkpoint and allowing in-progress transfers to complete...")
		cancel()

		// Start shutdown timer
		shutdownTimer := time.AfterFunc(shutdownTimeout, func() {
			exitOnce.Do(func() {
				fmt.Fprintln(os.Stderr, "Shutdown timeout reached, forcing exit...")
				forceCleanupAndExit(cleanup)
			})
		})

		// Wait for second signal for immediate exit
		<-sigCh
		shutdownTimer.Stop()
		exitOnce.Do(func() {
			fmt.Fprintln(os.Stderr, "Second signal received, forcing immediate exit...")
			forceCleanupAndExit(cleanup)
		})
	}()
}

// forceCleanupAndExit closes database connections with a deadline and exits.
// Runs cleanup in a goroutine with a short timeout to avoid blocking on
// connections held by stalled operations (e.g. pgxpool.Close waits for
// acquired connections to be released, which never happens if a COPY is stalled).
// A brief sleep after cleanup allows the OS TCP stack to send FIN/RST packets
// so the database server can clean up its connection state.
func forceCleanupAndExit(cleanup func()) {
	fmt.Fprintln(os.Stderr, "Closing database connections...")
	if cleanup != nil {
		done := make(chan struct{})
		go func() {
			cleanup()
			close(done)
		}()
		select {
		case <-done:
			// Cleanup completed
		case <-time.After(5 * time.Second):
			fmt.Fprintln(os.Stderr, "Cleanup timed out, forcing exit...")
		}
	}
	// Brief pause to let OS flush TCP FIN/RST packets to database servers
	time.Sleep(100 * time.Millisecond)
	fmt.Fprintf(os.Stderr, "Exit code %d (%s) - safe to retry\n", exitcodes.Cancelled, exitcodes.Description(exitcodes.Cancelled))
	os.Exit(exitcodes.Cancelled)
}

// healthCheck implements `dmt preflight` (and its legacy `health-check`
// alias). Tests connections and runs the driver-side preflight checks
